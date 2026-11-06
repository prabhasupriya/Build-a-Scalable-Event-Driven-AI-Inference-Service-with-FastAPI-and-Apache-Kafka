"""End-to-end integration tests.

They exercise the REAL stack (API gateway -> Kafka -> consumer -> PostgreSQL)
and therefore need ``docker compose up --build -d`` to be running. Run them
inside the gateway container (service names resolve on the compose network):

    docker compose exec api_gateway pytest -m integration

Environment variables (all optional; defaults match the compose file):
    API_BASE_URL, KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC_REQUESTS,
    DB_HOST / DB_PORT / DB_USER / DB_PASSWORD / DB_NAME, E2E_TIMEOUT_SECONDS
"""
import asyncio
import json
import os
import time
import uuid

import asyncpg
import httpx
import pytest
from aiokafka import AIOKafkaProducer

pytestmark = pytest.mark.integration

API_BASE_URL = os.getenv("API_BASE_URL", f"http://localhost:{os.getenv('FASTAPI_PORT', '8000')}")
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
TOPIC = os.getenv("KAFKA_TOPIC_REQUESTS", "inference_requests")
E2E_TIMEOUT = float(os.getenv("E2E_TIMEOUT_SECONDS", "60"))
FAILURE_MARKER = os.getenv("INFERENCE_FAILURE_MARKER", "__fail__")


@pytest.fixture
async def http():
    async with httpx.AsyncClient(base_url=API_BASE_URL, timeout=10) as client:
        try:
            response = await client.get("/health")
        except httpx.HTTPError as exc:
            pytest.skip(f"Stack not reachable at {API_BASE_URL}: {exc!r}")
        if response.status_code != 200:
            pytest.skip(f"Stack unhealthy: {response.text}")
        yield client


async def wait_for_final_status(client: httpx.AsyncClient, request_id: str) -> dict:
    deadline = time.monotonic() + E2E_TIMEOUT
    last: dict = {}
    while time.monotonic() < deadline:
        response = await client.get(f"/inference/{request_id}")
        assert response.status_code == 200, response.text
        last = response.json()
        if last["status"] in ("COMPLETED", "FAILED"):
            return last
        await asyncio.sleep(0.5)
    pytest.fail(f"Request {request_id} did not finish within {E2E_TIMEOUT}s; last state: {last}")


async def test_full_flow_via_api(http):
    """POST -> Kafka -> consumer -> DB -> GET."""
    created = await http.post("/inference", json={"input_data": "integration test", "model_id": "m1"})
    assert created.status_code == 202
    request_id = created.json()["request_id"]

    immediate = (await http.get(f"/inference/{request_id}")).json()
    assert immediate["status"] in ("PENDING", "PROCESSING", "COMPLETED")

    final = await wait_for_final_status(http, request_id)
    assert final["status"] == "COMPLETED"
    assert final["input_data"] == "integration test"
    assert final["output_data"] == "Processed: integration test by m1"


async def test_full_flow_publishing_directly_to_kafka(http):
    """Publish straight to the topic; the consumer must create + complete the row."""
    request_id = str(uuid.uuid4())
    payload = {
        "request_id": request_id,
        "input_data": "published straight to kafka",
        "model_id": "direct",
        "timestamp": time.time(),
    }
    producer = AIOKafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP)
    await producer.start()
    try:
        await producer.send_and_wait(
            TOPIC, json.dumps(payload).encode(), key=request_id.encode()
        )
    finally:
        await producer.stop()

    # The API has never seen this id, so the row may not exist until the consumer inserts it.
    deadline = time.monotonic() + E2E_TIMEOUT
    final = None
    while time.monotonic() < deadline:
        response = await http.get(f"/inference/{request_id}")
        if response.status_code == 200 and response.json()["status"] == "COMPLETED":
            final = response.json()
            break
        await asyncio.sleep(0.5)
    assert final is not None, "consumer did not process the directly published message"
    assert final["output_data"] == "Processed: published straight to kafka by direct"


async def test_result_is_persisted_in_postgres(http):
    created = await http.post("/inference", json={"input_data": "check the database"})
    request_id = created.json()["request_id"]
    await wait_for_final_status(http, request_id)

    conn = await asyncpg.connect(
        host=os.getenv("DB_HOST", "postgres"),
        port=int(os.getenv("DB_PORT", "5432")),
        user=os.getenv("DB_USER", "user"),
        password=os.getenv("DB_PASSWORD", "password"),
        database=os.getenv("DB_NAME", "inference_db"),
    )
    try:
        row = await conn.fetchrow(
            "SELECT status, input_data, output_data FROM inference_requests WHERE request_id = $1",
            request_id,
        )
    finally:
        await conn.close()
    assert row is not None
    assert row["status"] == "COMPLETED"
    assert row["input_data"] == "check the database"
    assert row["output_data"] == "Processed: check the database by default_model"


async def test_duplicate_kafka_delivery_is_processed_once(http):
    """Re-publishing the same message must not change a COMPLETED result."""
    created = await http.post("/inference", json={"input_data": "dup test"})
    request_id = created.json()["request_id"]
    first = await wait_for_final_status(http, request_id)

    duplicate = {
        "request_id": request_id,
        "input_data": "dup test",
        "model_id": "default_model",
        "timestamp": time.time(),
    }
    producer = AIOKafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP)
    await producer.start()
    try:
        await producer.send_and_wait(TOPIC, json.dumps(duplicate).encode(), key=request_id.encode())
    finally:
        await producer.stop()

    await asyncio.sleep(3)
    second = (await http.get(f"/inference/{request_id}")).json()
    assert second["status"] == "COMPLETED"
    assert second["output_data"] == first["output_data"]
    assert second["updated_at"] == first["updated_at"]  # untouched by the duplicate


async def test_failed_inference_is_reported_as_failed(http):
    created = await http.post("/inference", json={"input_data": f"please {FAILURE_MARKER}"})
    final = await wait_for_final_status(http, created.json()["request_id"])
    assert final["status"] == "FAILED"
    assert final["error_message"]
    assert final["output_data"] is None


async def test_unknown_request_returns_404(http):
    response = await http.get(f"/inference/{uuid.uuid4()}")
    assert response.status_code == 404


async def test_invalid_payload_returns_422(http):
    response = await http.post("/inference", json={"model_id": "x"})
    assert response.status_code == 422

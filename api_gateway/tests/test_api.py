"""Unit tests for the API gateway (fake database + fake Kafka publisher)."""
import uuid

import pytest
from aiokafka.errors import KafkaConnectionError
from asyncpg.exceptions import PostgresConnectionError

from app.kafka_producer import build_message_payload
from app.main import generate_request_id
from app.models import InferenceRequest


# ------------------------------------------------------------- pure logic
def test_generate_request_id_is_unique_uuid4():
    ids = {generate_request_id() for _ in range(1000)}
    assert len(ids) == 1000
    for value in list(ids)[:10]:
        assert uuid.UUID(value).version == 4


def test_build_message_payload_contains_required_fields():
    payload = build_message_payload("abc", "hello", "resnet50", 123.5)
    assert payload == {
        "request_id": "abc",
        "input_data": "hello",
        "model_id": "resnet50",
        "timestamp": 123.5,
    }


def test_model_id_defaults_when_omitted_or_null():
    assert InferenceRequest(input_data="x").model_id == "default_model"
    assert InferenceRequest(input_data="x", model_id=None).model_id == "default_model"


# ------------------------------------------------------- POST /inference
async def test_post_inference_returns_202_and_publishes(client, fake_db, fake_publisher):
    response = await client.post(
        "/inference", json={"input_data": "a cat", "model_id": "resnet50"}
    )
    assert response.status_code == 202
    body = response.json()
    assert body["status"] == "PENDING"
    request_id = body["request_id"]
    uuid.UUID(request_id)

    assert len(fake_publisher.published) == 1
    sent = fake_publisher.published[0]
    assert sent["topic"] == "inference_requests"
    assert sent["key"] == request_id
    assert sent["payload"]["request_id"] == request_id
    assert sent["payload"]["input_data"] == "a cat"
    assert sent["payload"]["model_id"] == "resnet50"
    assert isinstance(sent["payload"]["timestamp"], float)

    # stored as PENDING before the consumer ever sees it
    assert fake_db.rows[request_id]["status"] == "PENDING"


async def test_post_inference_uses_default_model(client, fake_publisher):
    response = await client.post("/inference", json={"input_data": "hello"})
    assert response.status_code == 202
    assert fake_publisher.published[0]["payload"]["model_id"] == "default_model"


async def test_post_inference_propagates_correlation_id(client, fake_publisher):
    response = await client.post(
        "/inference", json={"input_data": "hello"}, headers={"X-Correlation-ID": "corr-42"}
    )
    assert response.headers["X-Correlation-ID"] == "corr-42"
    assert fake_publisher.published[0]["correlation_id"] == "corr-42"


async def test_response_always_carries_a_correlation_id(client):
    response = await client.get("/health")
    assert response.headers["X-Correlation-ID"]


@pytest.mark.parametrize(
    "body",
    [
        {},  # missing input_data
        {"input_data": ""},  # empty
        {"input_data": "   "},  # whitespace only (stripped -> empty)
        {"input_data": 123},  # wrong type
        {"input_data": "x" * 10_001},  # too long
        {"input_data": "ok", "model_id": "bad model id!"},  # invalid characters
        {"input_data": "ok", "model_id": "m" * 200},  # too long
    ],
)
async def test_post_inference_rejects_invalid_input_with_422(client, fake_publisher, body):
    response = await client.post("/inference", json=body)
    assert response.status_code == 422
    assert fake_publisher.published == []


async def test_post_inference_rejects_malformed_json(client):
    response = await client.post(
        "/inference", content=b"{not json", headers={"Content-Type": "application/json"}
    )
    assert response.status_code == 422


async def test_post_inference_returns_503_when_kafka_is_down(client, fake_db, fake_publisher):
    fake_publisher.fail_with = KafkaConnectionError("broker down")
    response = await client.post("/inference", json={"input_data": "hello"})
    assert response.status_code == 503
    assert "broker" in response.json()["detail"].lower()
    # the orphaned PENDING row is marked FAILED, never left dangling
    (row,) = fake_db.rows.values()
    assert row["status"] == "FAILED"


async def test_post_inference_returns_503_when_database_is_down(client, fake_db, fake_publisher):
    fake_db.fail_with = PostgresConnectionError("db down")
    response = await client.post("/inference", json={"input_data": "hello"})
    assert response.status_code == 503
    assert fake_publisher.published == []  # nothing is published if we could not persist


# --------------------------------------------- GET /inference/{request_id}
async def test_get_inference_returns_stored_record(client, fake_db):
    fake_db.rows["req-1"] = {
        "request_id": "req-1",
        "model_id": "resnet50",
        "status": "COMPLETED",
        "input_data": "a cat",
        "output_data": "Processed: a cat by resnet50",
        "error_message": None,
        "timestamp": 1700000000.0,
        "updated_at": 1700000005.0,
    }
    response = await client.get("/inference/req-1")
    assert response.status_code == 200
    body = response.json()
    assert body["request_id"] == "req-1"
    assert body["status"] == "COMPLETED"
    assert body["input_data"] == "a cat"
    assert body["output_data"] == "Processed: a cat by resnet50"


async def test_get_inference_pending_has_no_output(client, client_post=None):
    created = await client.post("/inference", json={"input_data": "hello"})
    request_id = created.json()["request_id"]
    response = await client.get(f"/inference/{request_id}")
    assert response.status_code == 200
    assert response.json()["status"] == "PENDING"
    assert response.json()["output_data"] is None


async def test_get_inference_unknown_id_returns_404(client):
    response = await client.get("/inference/does-not-exist")
    assert response.status_code == 404
    assert response.json() == {"detail": "Inference request not found"}


async def test_get_inference_returns_503_when_database_is_down(client, fake_db):
    fake_db.fail_with = PostgresConnectionError("db down")
    response = await client.get("/inference/anything")
    assert response.status_code == 503


# ------------------------------------------------------------------ health
async def test_health_ok(client):
    response = await client.get("/health")
    assert response.status_code == 200
    assert response.json() == {
        "status": "ok",
        "service": "api_gateway",
        "dependencies": {"database": "ok", "kafka": "ok"},
    }


async def test_health_degraded_when_dependency_down(client, fake_db, fake_publisher):
    fake_publisher.healthy = False
    response = await client.get("/health")
    assert response.status_code == 503
    assert response.json()["dependencies"]["kafka"] == "unavailable"
    assert response.json()["dependencies"]["database"] == "ok"

    fake_publisher.healthy = True
    fake_db.healthy = False
    response = await client.get("/health")
    assert response.status_code == 503
    assert response.json()["dependencies"]["database"] == "unavailable"

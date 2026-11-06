"""Integration tests that talk to the REAL PostgreSQL (and Kafka) of the compose stack.

Run them inside the consumer container while the stack is up:

    docker compose exec kafka_consumer pytest -m integration

* ``test_db_*``     need only PostgreSQL (the SQL that guarantees idempotency).
* ``test_kafka_*``  also need Kafka and the running consumer service.
"""
import asyncio
import json
import time
import uuid

import asyncpg
import pytest
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer

from app.config import Settings
from app.database import ClaimResult, ConsumerDatabase

pytestmark = pytest.mark.integration

SETTINGS = Settings.from_env()
E2E_TIMEOUT = 60


@pytest.fixture
async def real_db():
    database = ConsumerDatabase(SETTINGS)
    try:
        await asyncio.wait_for(database.connect(), timeout=15)
    except Exception as exc:  # noqa: BLE001
        pytest.skip(f"PostgreSQL not reachable: {exc!r}")
    yield database
    await database.close()


@pytest.fixture
async def conn():
    try:
        connection = await asyncpg.connect(
            host=SETTINGS.db_host,
            port=SETTINGS.db_port,
            user=SETTINGS.db_user,
            password=SETTINGS.db_password,
            database=SETTINGS.db_name,
            timeout=5,
        )
    except Exception as exc:  # noqa: BLE001
        pytest.skip(f"PostgreSQL not reachable: {exc!r}")
    yield connection
    await connection.close()


async def insert_pending(conn, request_id: str) -> None:
    await conn.execute(
        "INSERT INTO inference_requests (request_id, input_data, status) VALUES ($1, 'x', 'PENDING')",
        request_id,
    )


# ------------------------------------------------------- idempotency in SQL
async def test_db_claim_complete_and_duplicate_cycle(real_db, conn):
    request_id = str(uuid.uuid4())
    await insert_pending(conn, request_id)

    assert await real_db.claim_request(request_id, "x", "m", time.time(), 60) is ClaimResult.CLAIMED
    # A second delivery while the first is still running must not start a second run.
    assert await real_db.claim_request(request_id, "x", "m", time.time(), 60) is ClaimResult.IN_PROGRESS

    assert await real_db.mark_completed(request_id, "result") is True
    assert await real_db.claim_request(request_id, "x", "m", time.time(), 60) is ClaimResult.ALREADY_FINISHED
    assert await real_db.mark_completed(request_id, "other") is False  # no overwrite

    row = await conn.fetchrow("SELECT * FROM inference_requests WHERE request_id = $1", request_id)
    assert row["status"] == "COMPLETED"
    assert row["output_data"] == "result"
    assert row["attempts"] == 1
    assert row["completed_at"] is not None


async def test_db_claim_inserts_missing_row(real_db, conn):
    request_id = str(uuid.uuid4())
    assert await real_db.claim_request(request_id, "hello", "m", time.time(), 60) is ClaimResult.CLAIMED
    row = await conn.fetchrow("SELECT * FROM inference_requests WHERE request_id = $1", request_id)
    assert (row["status"], row["input_data"], row["model_id"]) == ("PROCESSING", "hello", "m")


async def test_db_only_one_of_many_concurrent_claims_wins(real_db, conn):
    request_id = str(uuid.uuid4())
    await insert_pending(conn, request_id)
    results = await asyncio.gather(
        *[real_db.claim_request(request_id, "x", "m", time.time(), 60) for _ in range(10)]
    )
    assert results.count(ClaimResult.CLAIMED) == 1
    assert results.count(ClaimResult.IN_PROGRESS) == 9


async def test_db_expired_lease_can_be_reclaimed(real_db, conn):
    request_id = str(uuid.uuid4())
    await insert_pending(conn, request_id)
    assert await real_db.claim_request(request_id, "x", "m", time.time(), 60) is ClaimResult.CLAIMED
    await asyncio.sleep(0.2)
    # lease of 0.1s has expired -> the crashed worker's request is picked up again
    assert await real_db.claim_request(request_id, "x", "m", time.time(), 0.1) is ClaimResult.CLAIMED
    attempts = await conn.fetchval(
        "SELECT attempts FROM inference_requests WHERE request_id = $1", request_id
    )
    assert attempts == 2


async def test_db_mark_failed(real_db, conn):
    request_id = str(uuid.uuid4())
    await insert_pending(conn, request_id)
    assert await real_db.mark_failed(request_id, "boom") is True
    assert await real_db.mark_failed(request_id, "again") is False  # already terminal
    row = await conn.fetchrow("SELECT * FROM inference_requests WHERE request_id = $1", request_id)
    assert (row["status"], row["error_message"]) == ("FAILED", "boom")


# ----------------------------------------------------- real Kafka, real worker
async def publish(value: bytes, key: str, topic: str = None) -> None:
    producer = AIOKafkaProducer(bootstrap_servers=SETTINGS.kafka_bootstrap_servers)
    try:
        await asyncio.wait_for(producer.start(), timeout=15)
    except Exception as exc:  # noqa: BLE001
        await producer.stop()
        pytest.skip(f"Kafka not reachable: {exc!r}")
    try:
        await producer.send_and_wait(topic or SETTINGS.kafka_topic_requests, value, key=key.encode())
    finally:
        await producer.stop()


async def wait_for_status(conn, request_id: str, wanted: str):
    deadline = time.monotonic() + E2E_TIMEOUT
    row = None
    while time.monotonic() < deadline:
        row = await conn.fetchrow(
            "SELECT * FROM inference_requests WHERE request_id = $1", request_id
        )
        if row and row["status"] == wanted:
            return row
        await asyncio.sleep(0.5)
    pytest.fail(f"{request_id} never reached {wanted}; last row: {dict(row) if row else None}")


async def test_kafka_message_is_consumed_and_stored(conn):
    request_id = str(uuid.uuid4())
    payload = {"request_id": request_id, "input_data": "via kafka", "model_id": "m", "timestamp": time.time()}
    await publish(json.dumps(payload).encode(), request_id)
    row = await wait_for_status(conn, request_id, "COMPLETED")
    assert row["output_data"] == "Processed: via kafka by m"


async def test_kafka_duplicate_messages_are_processed_once(conn):
    request_id = str(uuid.uuid4())
    payload = json.dumps(
        {"request_id": request_id, "input_data": "dup", "model_id": "m", "timestamp": time.time()}
    ).encode()
    await publish(payload, request_id)
    await publish(payload, request_id)
    await publish(payload, request_id)
    row = await wait_for_status(conn, request_id, "COMPLETED")
    await asyncio.sleep(SETTINGS.inference_simulated_seconds + 2)  # let the duplicates drain
    row = await conn.fetchrow("SELECT * FROM inference_requests WHERE request_id = $1", request_id)
    assert row["status"] == "COMPLETED"
    assert row["attempts"] == 1, "duplicates must not trigger another inference run"


async def test_kafka_poison_message_lands_in_dead_letter_topic():
    key = f"poison-{uuid.uuid4()}"
    await publish(b"{this is not json", key)

    consumer = AIOKafkaConsumer(
        SETTINGS.kafka_topic_dlq,
        bootstrap_servers=SETTINGS.kafka_bootstrap_servers,
        group_id=f"dlq-test-{uuid.uuid4()}",
        auto_offset_reset="earliest",
    )
    await consumer.start()
    try:
        deadline = time.monotonic() + E2E_TIMEOUT
        while time.monotonic() < deadline:
            batch = await consumer.getmany(timeout_ms=1000)
            for records in batch.values():
                for record in records:
                    if record.key == key.encode():
                        assert record.value == b"{this is not json"
                        reasons = dict(record.headers)
                        assert b"deserialization" in reasons["dead_letter_reason"]
                        return
        pytest.fail("poison message never reached the dead-letter topic")
    finally:
        await consumer.stop()

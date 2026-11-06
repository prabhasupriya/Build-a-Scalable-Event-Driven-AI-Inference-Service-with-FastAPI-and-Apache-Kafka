"""Integration tests for the gateway's SQL against the REAL PostgreSQL.

Only PostgreSQL is required (no Kafka, no running API). Inside the stack:

    docker compose exec api_gateway pytest -m integration tests/test_database_integration.py

The tests skip themselves when the database is not reachable.
"""
import asyncio
import time
import uuid

import pytest

from app.config import Settings
from app.database import Database

pytestmark = pytest.mark.integration

SETTINGS = Settings.from_env()


@pytest.fixture
async def database():
    db = Database(SETTINGS)
    try:
        await asyncio.wait_for(db.connect(), timeout=15)
    except Exception as exc:  # noqa: BLE001
        pytest.skip(f"PostgreSQL not reachable: {exc!r}")
    yield db
    await db.close()


async def test_create_pending_then_get_request(database):
    request_id = str(uuid.uuid4())
    created_at = time.time()

    await database.create_pending(request_id, "hello", "resnet50", created_at)
    record = await database.get_request(request_id)

    assert record is not None
    assert record["request_id"] == request_id
    assert record["status"] == "PENDING"
    assert record["input_data"] == "hello"
    assert record["model_id"] == "resnet50"
    assert record["output_data"] is None
    assert record["timestamp"] == pytest.approx(created_at, abs=1)


async def test_get_request_returns_none_for_unknown_id(database):
    assert await database.get_request("does-not-exist") is None


async def test_create_pending_is_idempotent(database):
    request_id = str(uuid.uuid4())
    await database.create_pending(request_id, "first", "m", time.time())
    await database.create_pending(request_id, "second", "m", time.time())  # no error, no overwrite

    assert (await database.get_request(request_id))["input_data"] == "first"


async def test_mark_failed_only_touches_pending_rows(database):
    pending = str(uuid.uuid4())
    await database.create_pending(pending, "x", "m", time.time())
    await database.mark_failed(pending, "broker unavailable")

    record = await database.get_request(pending)
    assert record["status"] == "FAILED"
    assert record["error_message"] == "broker unavailable"


async def test_sql_injection_attempt_is_stored_as_plain_text(database):
    request_id = str(uuid.uuid4())
    evil = "'; DROP TABLE inference_requests; --"
    await database.create_pending(request_id, evil, "m", time.time())

    assert (await database.get_request(request_id))["input_data"] == evil  # parameterised queries


async def test_ping_and_schema_bootstrap_are_repeatable(database):
    assert await database.ping() is True
    await database.close()
    await database.connect()  # runs the advisory-locked CREATE TABLE IF NOT EXISTS again
    assert await database.ping() is True

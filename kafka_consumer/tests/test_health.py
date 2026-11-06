"""Tests for the consumer's /health endpoint and the worker health report."""
import asyncio
import json
import time

import pytest

from app.consumer import InferenceWorker
from app.health import start_health_server


async def http_get(port: int, path: str = "/health", method: str = "GET"):
    reader, writer = await asyncio.open_connection("127.0.0.1", port)
    writer.write(f"{method} {path} HTTP/1.1\r\nHost: test\r\n\r\n".encode())
    await writer.drain()
    raw = await reader.read()
    writer.close()
    head, _, body = raw.partition(b"\r\n\r\n")
    status_code = int(head.split()[1])
    return status_code, json.loads(body)


@pytest.fixture
def worker(db, settings):
    instance = InferenceWorker(settings, db=db)
    instance._started = True
    instance._last_poll = time.monotonic()
    return instance


async def test_worker_health_is_ok_when_running_and_database_up(worker):
    healthy, details = await worker.health()
    assert healthy
    assert details["status"] == "ok"
    assert details["service"] == "kafka_consumer"
    assert details["dependencies"] == {"database": "ok", "kafka_consumer": "ok"}


async def test_worker_health_degrades_when_database_is_down(worker, db):
    db.healthy = False
    healthy, details = await worker.health()
    assert not healthy
    assert details["dependencies"]["database"] == "unavailable"


async def test_worker_health_degrades_before_start_and_after_stop(worker):
    worker._started = False
    assert (await worker.health())[0] is False

    worker._started = True
    worker.request_stop()
    assert (await worker.health())[0] is False


async def test_worker_health_degrades_when_poll_loop_is_stuck(worker):
    worker._last_poll = time.monotonic() - 10_000
    assert (await worker.health())[0] is False


async def test_health_endpoint_returns_200_then_503(worker, db):
    server = await start_health_server(worker.health, host="127.0.0.1", port=0)
    port = server.sockets[0].getsockname()[1]
    try:
        status_code, body = await http_get(port)
        assert status_code == 200
        assert body["status"] == "ok"

        db.healthy = False
        status_code, body = await http_get(port)
        assert status_code == 503
        assert body["status"] == "degraded"
    finally:
        server.close()
        await server.wait_closed()


async def test_health_server_rejects_unknown_paths_and_methods(worker):
    server = await start_health_server(worker.health, host="127.0.0.1", port=0)
    port = server.sockets[0].getsockname()[1]
    try:
        assert (await http_get(port, "/nope"))[0] == 404
        assert (await http_get(port, "/health", method="POST"))[0] == 405
    finally:
        server.close()
        await server.wait_closed()

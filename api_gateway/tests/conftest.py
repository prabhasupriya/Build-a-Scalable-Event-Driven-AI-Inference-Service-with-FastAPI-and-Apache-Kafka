"""Shared fixtures: the FastAPI app wired to in-memory fakes (no Kafka / Postgres)."""
from typing import Any, Dict, List, Optional

import httpx
import pytest
import pytest_asyncio

from app.config import Settings
from app.main import app


class FakeDatabase:
    def __init__(self) -> None:
        self.rows: Dict[str, Dict[str, Any]] = {}
        self.fail_with: Optional[BaseException] = None
        self.healthy = True

    async def create_pending(self, request_id, input_data, model_id, timestamp):
        if self.fail_with:
            raise self.fail_with
        self.rows[request_id] = {
            "request_id": request_id,
            "model_id": model_id,
            "status": "PENDING",
            "input_data": input_data,
            "output_data": None,
            "error_message": None,
            "timestamp": timestamp,
            "updated_at": timestamp,
        }

    async def get_request(self, request_id):
        if self.fail_with:
            raise self.fail_with
        return self.rows.get(request_id)

    async def mark_failed(self, request_id, error):
        if request_id in self.rows:
            self.rows[request_id].update(status="FAILED", error_message=error)

    async def ping(self):
        return self.healthy


class FakePublisher:
    def __init__(self) -> None:
        self.published: List[Dict[str, Any]] = []
        self.fail_with: Optional[BaseException] = None
        self.healthy = True

    async def publish(self, topic, payload, *, key, correlation_id=None):
        if self.fail_with:
            raise self.fail_with
        self.published.append(
            {"topic": topic, "payload": payload, "key": key, "correlation_id": correlation_id}
        )

    async def ping(self):
        return self.healthy


@pytest.fixture
def fake_db() -> FakeDatabase:
    return FakeDatabase()


@pytest.fixture
def fake_publisher() -> FakePublisher:
    return FakePublisher()


@pytest_asyncio.fixture
async def client(fake_db, fake_publisher):
    # ASGITransport does not run the lifespan, so we inject the dependencies directly.
    app.state.settings = Settings()
    app.state.db = fake_db
    app.state.producer = fake_publisher
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://testserver") as http_client:
        yield http_client

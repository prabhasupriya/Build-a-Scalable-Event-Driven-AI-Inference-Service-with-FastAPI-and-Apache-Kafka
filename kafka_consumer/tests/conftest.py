"""Shared fixtures: an in-memory stand-in for ConsumerDatabase and a fake DLQ."""
from typing import Dict, List, Optional, Tuple

import pytest

from app.config import Settings
from app.database import ClaimResult


class FakeDatabase:
    """Mirrors the state machine implemented by the SQL in ``app.database``.

    PENDING -> PROCESSING -> COMPLETED | FAILED, where finished rows are never
    claimed again. Failures can be injected per method via ``fail_<method>``.
    """

    def __init__(self) -> None:
        self.rows: Dict[str, Dict] = {}
        self.fail_claim: Optional[BaseException] = None
        self.fail_completed: Optional[BaseException] = None
        self.fail_failed: Optional[BaseException] = None
        self.claim_calls = 0
        self.healthy = True

    def seed(self, request_id: str, status: str, **extra) -> None:
        self.rows[request_id] = {
            "status": status,
            "attempts": 0,
            "output_data": None,
            "error_message": None,
            **extra,
        }

    async def claim_request(self, request_id, input_data, model_id, timestamp, lease_seconds):
        self.claim_calls += 1
        if self.fail_claim:
            raise self.fail_claim
        row = self.rows.get(request_id)
        if row is None:
            self.rows[request_id] = {
                "status": "PROCESSING",
                "attempts": 1,
                "input_data": input_data,
                "model_id": model_id,
                "output_data": None,
                "error_message": None,
            }
            return ClaimResult.CLAIMED
        if row["status"] == "PENDING":
            row.update(status="PROCESSING", attempts=row["attempts"] + 1)
            return ClaimResult.CLAIMED
        if row["status"] in ("COMPLETED", "FAILED"):
            return ClaimResult.ALREADY_FINISHED
        return ClaimResult.IN_PROGRESS

    async def mark_completed(self, request_id, output_data):
        if self.fail_completed:
            raise self.fail_completed
        row = self.rows.get(request_id)
        if not row or row["status"] != "PROCESSING":
            return False
        row.update(status="COMPLETED", output_data=output_data)
        return True

    async def mark_failed(self, request_id, error):
        if self.fail_failed:
            raise self.fail_failed
        row = self.rows.get(request_id)
        if not row or row["status"] not in ("PENDING", "PROCESSING"):
            return False
        row.update(status="FAILED", error_message=error)
        return True

    async def ping(self):
        return self.healthy

    async def connect(self):
        return None

    async def close(self):
        return None


class FakeDeadLetter:
    """Records what would have been published to the dead-letter topic."""

    def __init__(self) -> None:
        self.messages: List[Tuple[Optional[bytes], Optional[str], str]] = []
        self.fail_with: Optional[BaseException] = None

    async def __call__(self, raw_value, key, reason):
        if self.fail_with:
            raise self.fail_with
        self.messages.append((raw_value, key, reason))


@pytest.fixture
def settings() -> Settings:
    """Fast settings: no simulated delay, no backoff, two inference attempts."""
    return Settings(
        inference_simulated_seconds=0,
        inference_max_attempts=2,
        retry_max_attempts=2,
        retry_base_delay_seconds=0,
        retry_max_delay_seconds=0,
        processing_lease_seconds=60,
    )


@pytest.fixture
def db() -> FakeDatabase:
    return FakeDatabase()


@pytest.fixture
def dead_letter() -> FakeDeadLetter:
    return FakeDeadLetter()

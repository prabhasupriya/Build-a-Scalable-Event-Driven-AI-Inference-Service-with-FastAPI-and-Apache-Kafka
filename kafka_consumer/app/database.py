"""PostgreSQL access for the Kafka consumer.

The key piece is :meth:`ConsumerDatabase.claim_request`, a single atomic
``INSERT ... ON CONFLICT DO UPDATE ... WHERE`` statement that makes message
processing idempotent: only one worker can move a request into ``PROCESSING``,
and finished requests are never processed again.
"""
import asyncio
import logging
from enum import Enum
from typing import Awaitable, Callable, Optional, TypeVar

import asyncpg
from asyncpg.exceptions import (
    CannotConnectNowError,
    InterfaceError,
    PostgresConnectionError,
    PostgresError,
    TooManyConnectionsError,
)

from app.config import Settings
from app.logging_config import log_event
from app.retry import retry_async

logger = logging.getLogger(__name__)

T = TypeVar("T")

DB_TRANSIENT_ERRORS = (
    PostgresConnectionError,
    InterfaceError,
    CannotConnectNowError,
    TooManyConnectionsError,
    OSError,
    asyncio.TimeoutError,
)
DB_ERRORS = DB_TRANSIENT_ERRORS + (PostgresError,)

SCHEMA_LOCK_ID = 727274

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS inference_requests (
    request_id    TEXT PRIMARY KEY,
    model_id      TEXT NOT NULL DEFAULT 'default_model',
    input_data    TEXT NOT NULL,
    output_data   TEXT,
    status        TEXT NOT NULL DEFAULT 'PENDING'
                  CHECK (status IN ('PENDING', 'PROCESSING', 'COMPLETED', 'FAILED')),
    error_message TEXT,
    attempts      INTEGER NOT NULL DEFAULT 0,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    completed_at  TIMESTAMPTZ
);
CREATE INDEX IF NOT EXISTS idx_inference_requests_status
    ON inference_requests (status);
"""

# Atomically claim a request for processing.
#  * no row yet (API write lost / consumer ahead)  -> insert it as PROCESSING
#  * row is PENDING                                -> move it to PROCESSING
#  * row is PROCESSING but the lease has expired   -> re-claim (worker crashed)
#  * row is COMPLETED / FAILED / freshly PROCESSING -> no row returned
CLAIM_SQL = """
INSERT INTO inference_requests
    (request_id, model_id, input_data, status, attempts, created_at, updated_at)
VALUES ($1, $2, $3, 'PROCESSING', 1, to_timestamp($4), now())
ON CONFLICT (request_id) DO UPDATE
   SET status        = 'PROCESSING',
       attempts      = inference_requests.attempts + 1,
       error_message = NULL,
       updated_at    = now()
 WHERE inference_requests.status = 'PENDING'
    OR (inference_requests.status = 'PROCESSING'
        AND inference_requests.updated_at < now() - make_interval(secs => $5))
RETURNING request_id
"""

SELECT_STATUS_SQL = "SELECT status FROM inference_requests WHERE request_id = $1"

MARK_COMPLETED_SQL = """
UPDATE inference_requests
   SET status = 'COMPLETED', output_data = $2, error_message = NULL,
       updated_at = now(), completed_at = now()
 WHERE request_id = $1 AND status = 'PROCESSING'
"""

MARK_FAILED_SQL = """
UPDATE inference_requests
   SET status = 'FAILED', error_message = $2, updated_at = now(), completed_at = now()
 WHERE request_id = $1 AND status IN ('PENDING', 'PROCESSING')
"""


class ClaimResult(str, Enum):
    CLAIMED = "claimed"  # this worker now owns the request
    ALREADY_FINISHED = "already_finished"  # duplicate delivery: COMPLETED / FAILED
    IN_PROGRESS = "in_progress"  # another worker holds a valid lease


class ConsumerDatabase:
    def __init__(self, settings: Settings) -> None:
        self._settings = settings
        self._policy = settings.retry_policy
        self._pool: Optional[asyncpg.Pool] = None

    async def connect(self) -> None:
        async def _create_pool() -> asyncpg.Pool:
            return await asyncpg.create_pool(
                host=self._settings.db_host,
                port=self._settings.db_port,
                user=self._settings.db_user,
                password=self._settings.db_password,
                database=self._settings.db_name,
                min_size=1,
                max_size=5,
                timeout=10,
                command_timeout=10,
            )

        self._pool = await retry_async(
            _create_pool,
            operation="db_connect",
            policy=self._policy,
            retry_on=DB_TRANSIENT_ERRORS,
            logger=logger,
            db_host=self._settings.db_host,
        )
        await self._run("db_ensure_schema", self._ensure_schema)
        log_event(logger, logging.INFO, "database_connected", db_host=self._settings.db_host)

    async def close(self) -> None:
        if self._pool is not None:
            await self._pool.close()
            self._pool = None
            log_event(logger, logging.INFO, "database_closed")

    async def ping(self) -> bool:
        """Cheap liveness probe used by the health endpoint (never raises)."""
        if self._pool is None:
            return False
        try:
            async with self._pool.acquire(timeout=3) as conn:
                await asyncio.wait_for(conn.fetchval("SELECT 1"), timeout=3)
            return True
        except Exception as exc:  # noqa: BLE001 - health probe must never raise
            log_event(logger, logging.WARNING, "database_ping_failed", error=repr(exc))
            return False

    async def claim_request(
        self,
        request_id: str,
        input_data: str,
        model_id: str,
        timestamp: float,
        lease_seconds: float,
    ) -> ClaimResult:
        async def _claim(conn: asyncpg.Connection) -> ClaimResult:
            claimed = await conn.fetchrow(
                CLAIM_SQL, request_id, model_id, input_data, timestamp, float(lease_seconds)
            )
            if claimed is not None:
                return ClaimResult.CLAIMED
            status = await conn.fetchval(SELECT_STATUS_SQL, request_id)
            if status in ("COMPLETED", "FAILED"):
                return ClaimResult.ALREADY_FINISHED
            return ClaimResult.IN_PROGRESS

        return await self._run("db_claim_request", _claim, request_id=request_id)

    async def mark_completed(self, request_id: str, output_data: str) -> bool:
        """Store the result. Returns False if the row was not in PROCESSING."""

        async def _update(conn: asyncpg.Connection) -> bool:
            result = await conn.execute(MARK_COMPLETED_SQL, request_id, output_data)
            return result.endswith(" 1")

        return await self._run("db_mark_completed", _update, request_id=request_id)

    async def mark_failed(self, request_id: str, error: str) -> bool:
        async def _update(conn: asyncpg.Connection) -> bool:
            result = await conn.execute(MARK_FAILED_SQL, request_id, error)
            return result.endswith(" 1")

        return await self._run("db_mark_failed", _update, request_id=request_id)

    # ---------------------------------------------------------------- internals
    @staticmethod
    async def _ensure_schema(conn: asyncpg.Connection) -> None:
        async with conn.transaction():
            await conn.execute("SELECT pg_advisory_xact_lock($1)", SCHEMA_LOCK_ID)
            await conn.execute(SCHEMA_SQL)

    async def _run(
        self,
        operation: str,
        func: Callable[[asyncpg.Connection], Awaitable[T]],
        **context: object,
    ) -> T:
        if self._pool is None:
            raise RuntimeError("Database pool is not initialised")
        pool = self._pool

        async def _attempt() -> T:
            async with pool.acquire() as conn:
                return await func(conn)

        return await retry_async(
            _attempt,
            operation=operation,
            policy=self._policy,
            retry_on=DB_TRANSIENT_ERRORS,
            logger=logger,
            **context,
        )

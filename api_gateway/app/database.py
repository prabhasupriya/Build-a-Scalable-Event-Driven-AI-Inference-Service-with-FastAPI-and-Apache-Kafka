"""PostgreSQL access for the API gateway (asyncpg, parameterised queries only)."""
import asyncio
import logging
from typing import Any, Awaitable, Callable, Dict, Optional, TypeVar

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

# Errors worth retrying: the database may simply be restarting / saturated.
DB_TRANSIENT_ERRORS = (
    PostgresConnectionError,
    InterfaceError,
    CannotConnectNowError,
    TooManyConnectionsError,
    OSError,
    asyncio.TimeoutError,
)
# Everything the API translates into a 503 response.
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

INSERT_PENDING_SQL = """
INSERT INTO inference_requests (request_id, model_id, input_data, status, created_at, updated_at)
VALUES ($1, $2, $3, 'PENDING', to_timestamp($4), now())
ON CONFLICT (request_id) DO NOTHING
"""

SELECT_REQUEST_SQL = """
SELECT request_id,
       model_id,
       status,
       input_data,
       output_data,
       error_message,
       EXTRACT(EPOCH FROM created_at)::float8 AS "timestamp",
       EXTRACT(EPOCH FROM updated_at)::float8 AS updated_at
FROM inference_requests
WHERE request_id = $1
"""

MARK_FAILED_SQL = """
UPDATE inference_requests
SET status = 'FAILED', error_message = $2, updated_at = now(), completed_at = now()
WHERE request_id = $1 AND status = 'PENDING'
"""


class Database:
    """Thin async wrapper around an asyncpg pool with retry on transient errors."""

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
                max_size=10,
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
        """Cheap liveness probe used by ``/health`` (never raises)."""
        if self._pool is None:
            return False
        try:
            async with self._pool.acquire(timeout=3) as conn:
                await asyncio.wait_for(conn.fetchval("SELECT 1"), timeout=3)
            return True
        except Exception as exc:  # noqa: BLE001 - health probe must never raise
            log_event(logger, logging.WARNING, "database_ping_failed", error=repr(exc))
            return False

    # ------------------------------------------------------------------ queries
    async def create_pending(
        self, request_id: str, input_data: str, model_id: str, timestamp: float
    ) -> None:
        async def _insert(conn: asyncpg.Connection) -> None:
            await conn.execute(INSERT_PENDING_SQL, request_id, model_id, input_data, timestamp)

        await self._run("db_create_pending", _insert, request_id=request_id)

    async def get_request(self, request_id: str) -> Optional[Dict[str, Any]]:
        async def _select(conn: asyncpg.Connection) -> Optional[asyncpg.Record]:
            return await conn.fetchrow(SELECT_REQUEST_SQL, request_id)

        record = await self._run("db_get_request", _select, request_id=request_id)
        return dict(record) if record is not None else None

    async def mark_failed(self, request_id: str, error: str) -> None:
        async def _update(conn: asyncpg.Connection) -> None:
            await conn.execute(MARK_FAILED_SQL, request_id, error)

        await self._run("db_mark_failed", _update, request_id=request_id)

    # ---------------------------------------------------------------- internals
    @staticmethod
    async def _ensure_schema(conn: asyncpg.Connection) -> None:
        # The advisory lock serialises schema creation between the API and the
        # consumer, which both bootstrap the table on start-up.
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

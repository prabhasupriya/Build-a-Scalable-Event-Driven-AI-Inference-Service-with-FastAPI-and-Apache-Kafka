"""Kafka consumer worker: (simulated) AI inference + result storage.

Delivery semantics
------------------
* Offsets are committed **manually, after** a message has been fully handled, so
  a crash can only cause *re*-delivery, never loss (at-least-once).
* Duplicates are made harmless by :meth:`ConsumerDatabase.claim_request`
  (idempotent consumer): a request that is already COMPLETED / FAILED is skipped.
* Transient infrastructure errors (database down) never commit the offset: the
  worker seeks back to the failed message and tries again after a pause.
* Poison messages (invalid JSON, schema violations, permanent DB errors) and
  requests whose inference keeps failing are published to a dead-letter topic.
"""
import asyncio
import contextlib
import json
import logging
import math
import signal
import time
import uuid
from dataclasses import dataclass
from enum import Enum
from typing import Any, Awaitable, Callable, Dict, Optional, Tuple

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer, TopicPartition
from aiokafka.admin import AIOKafkaAdminClient, NewTopic
from aiokafka.errors import KafkaError
from asyncpg.exceptions import PostgresError

from app.config import Settings
from app.database import DB_ERRORS, DB_TRANSIENT_ERRORS, ClaimResult, ConsumerDatabase
from app.health import start_health_server
from app.logging_config import configure_logging, correlation_id_var, log_event
from app.retry import retry_async

logger = logging.getLogger(__name__)

DEFAULT_MODEL_ID = "default_model"
KAFKA_TRANSIENT_ERRORS = (KafkaError, OSError, asyncio.TimeoutError)
_MAX_TIMESTAMP = 4_102_444_800  # 2100-01-01, keeps to_timestamp() in range


# --------------------------------------------------------------------------- types
class MessageDeserializationError(ValueError):
    """The Kafka message could not be turned into a valid inference request."""

    def __init__(self, reason: str, request_id: Optional[str] = None) -> None:
        super().__init__(reason)
        self.reason = reason
        self.request_id = request_id


class InferenceError(Exception):
    """The (simulated) model failed to produce a result."""


class Outcome(str, Enum):
    COMPLETED = "completed"
    FAILED = "failed"  # inference failed permanently; stored as FAILED
    DUPLICATE_SKIPPED = "duplicate_skipped"
    DEAD_LETTERED = "dead_lettered"  # poison message sent to the DLQ
    RETRY_LATER = "retry_later"  # do not commit: redeliver later


@dataclass(frozen=True)
class InferenceMessage:
    request_id: str
    input_data: str
    model_id: str
    timestamp: float


# (raw message value, message key, reason) -> None
DeadLetterPublisher = Callable[[Optional[bytes], Optional[str], str], Awaitable[None]]


# ------------------------------------------------------------------ pure functions
def parse_message(raw: Optional[bytes]) -> InferenceMessage:
    """Decode and validate a Kafka message value."""
    if not raw:
        raise MessageDeserializationError("empty message body")
    try:
        payload = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise MessageDeserializationError(f"invalid JSON: {exc}") from exc
    if not isinstance(payload, dict):
        raise MessageDeserializationError("message body must be a JSON object")

    request_id = payload.get("request_id")
    if not isinstance(request_id, str) or not request_id.strip() or "\x00" in request_id:
        raise MessageDeserializationError("missing or invalid 'request_id'")

    input_data = payload.get("input_data")
    if not isinstance(input_data, str) or not input_data or "\x00" in input_data:
        raise MessageDeserializationError("missing or invalid 'input_data'", request_id)

    model_id = payload.get("model_id") or DEFAULT_MODEL_ID
    if not isinstance(model_id, str) or "\x00" in model_id:
        raise MessageDeserializationError("invalid 'model_id'", request_id)

    timestamp = payload.get("timestamp")
    if (
        isinstance(timestamp, bool)
        or not isinstance(timestamp, (int, float))
        or not math.isfinite(timestamp)
        or not 0 <= timestamp <= _MAX_TIMESTAMP
    ):
        timestamp = time.time()

    return InferenceMessage(request_id, input_data, model_id, float(timestamp))


async def run_inference(
    input_data: str,
    model_id: str,
    *,
    simulated_seconds: float,
    failure_marker: str = "",
) -> str:
    """Simulate a long-running model call (non-blocking equivalent of ``time.sleep``).

    Inputs containing ``failure_marker`` raise :class:`InferenceError`, which lets
    tests and demos exercise the failure path deterministically.
    """
    if failure_marker and failure_marker in input_data:
        raise InferenceError(f"Simulated inference failure for model {model_id!r}")
    await asyncio.sleep(simulated_seconds)
    return f"Processed: {input_data} by {model_id}"


async def _dead_letter(
    dead_letter: DeadLetterPublisher,
    raw_value: Optional[bytes],
    key: Optional[str],
    reason: str,
    request_id: Optional[str],
) -> bool:
    """Publish to the DLQ. Returns False when Kafka rejected it."""
    try:
        await dead_letter(raw_value, key, reason)
    except KAFKA_TRANSIENT_ERRORS as exc:
        log_event(
            logger,
            logging.ERROR,
            "dead_letter_publish_failed",
            exc_info=True,
            request_id=request_id,
            reason=reason,
            error=repr(exc),
        )
        return False
    log_event(
        logger, logging.WARNING, "message_dead_lettered", request_id=request_id, reason=reason
    )
    return True


async def process_message(
    raw_value: Optional[bytes],
    *,
    db: ConsumerDatabase,
    settings: Settings,
    dead_letter: DeadLetterPublisher,
    key: Optional[str] = None,
) -> Outcome:
    """Handle one Kafka message end to end and report what should happen next."""
    try:
        message = parse_message(raw_value)
    except MessageDeserializationError as exc:
        log_event(
            logger,
            logging.ERROR,
            "message_deserialization_failed",
            request_id=exc.request_id,
            reason=exc.reason,
        )
        if exc.request_id:  # best effort: do not leave the API row stuck in PENDING
            with contextlib.suppress(*DB_ERRORS):
                await db.mark_failed(exc.request_id, f"Invalid message: {exc.reason}")
        delivered = await _dead_letter(
            dead_letter, raw_value, key, f"deserialization: {exc.reason}", exc.request_id
        )
        return Outcome.DEAD_LETTERED if delivered else Outcome.RETRY_LATER

    try:
        return await _process_parsed(message, db=db, settings=settings, dead_letter=dead_letter, key=key)
    except DB_TRANSIENT_ERRORS as exc:
        log_event(
            logger,
            logging.ERROR,
            "database_unavailable_will_retry",
            request_id=message.request_id,
            error=repr(exc),
        )
        return Outcome.RETRY_LATER
    except PostgresError as exc:  # permanent database error, e.g. data exception
        log_event(
            logger,
            logging.ERROR,
            "database_permanent_error",
            exc_info=True,
            request_id=message.request_id,
            error=repr(exc),
        )
        with contextlib.suppress(*DB_ERRORS):
            await db.mark_failed(message.request_id, f"Database error: {exc!r}")
        delivered = await _dead_letter(
            dead_letter, raw_value, key, f"database error: {exc!r}", message.request_id
        )
        return Outcome.DEAD_LETTERED if delivered else Outcome.RETRY_LATER
    except Exception as exc:  # noqa: BLE001 - unexpected bug: isolate the poison message
        log_event(
            logger,
            logging.ERROR,
            "unexpected_processing_error",
            exc_info=True,
            request_id=message.request_id,
            error=repr(exc),
        )
        with contextlib.suppress(*DB_ERRORS):
            await db.mark_failed(message.request_id, f"Unexpected error: {exc!r}")
        delivered = await _dead_letter(
            dead_letter, raw_value, key, f"unexpected error: {exc!r}", message.request_id
        )
        return Outcome.DEAD_LETTERED if delivered else Outcome.RETRY_LATER


async def _process_parsed(
    message: InferenceMessage,
    *,
    db: ConsumerDatabase,
    settings: Settings,
    dead_letter: DeadLetterPublisher,
    key: Optional[str],
) -> Outcome:
    claim = await db.claim_request(
        message.request_id,
        message.input_data,
        message.model_id,
        message.timestamp,
        settings.processing_lease_seconds,
    )
    if claim is ClaimResult.ALREADY_FINISHED:
        log_event(
            logger, logging.INFO, "duplicate_message_skipped", request_id=message.request_id
        )
        return Outcome.DUPLICATE_SKIPPED
    if claim is ClaimResult.IN_PROGRESS:
        log_event(
            logger,
            logging.INFO,
            "request_in_progress_elsewhere",
            request_id=message.request_id,
        )
        return Outcome.RETRY_LATER

    log_event(
        logger,
        logging.INFO,
        "inference_started",
        request_id=message.request_id,
        model_id=message.model_id,
    )
    started = time.perf_counter()
    try:
        output = await retry_async(
            lambda: run_inference(
                message.input_data,
                message.model_id,
                simulated_seconds=settings.inference_simulated_seconds,
                failure_marker=settings.inference_failure_marker,
            ),
            operation="inference",
            policy=settings.inference_retry_policy,
            retry_on=(InferenceError,),
            logger=logger,
            request_id=message.request_id,
        )
    except InferenceError as exc:
        error = f"Inference failed after {settings.inference_max_attempts} attempts: {exc}"
        await db.mark_failed(message.request_id, error)
        log_event(
            logger, logging.ERROR, "inference_failed", request_id=message.request_id, error=error
        )
        await _dead_letter(dead_letter, None, message.request_id, error, message.request_id)
        return Outcome.FAILED

    duration_ms = round((time.perf_counter() - started) * 1000, 2)
    log_event(
        logger,
        logging.INFO,
        "inference_completed",
        request_id=message.request_id,
        duration_ms=duration_ms,
    )
    stored = await db.mark_completed(message.request_id, output)
    log_event(
        logger,
        logging.INFO,
        "result_stored" if stored else "result_not_stored_row_not_processing",
        request_id=message.request_id,
        status="COMPLETED",
    )
    return Outcome.COMPLETED


# ------------------------------------------------------------------------- worker
class InferenceWorker:
    """Owns the Kafka consumer, the dead-letter producer and the database."""

    def __init__(self, settings: Settings, db: Optional[ConsumerDatabase] = None) -> None:
        self._settings = settings
        self._db = db or ConsumerDatabase(settings)
        self._consumer: Optional[AIOKafkaConsumer] = None
        self._dlq_producer: Optional[AIOKafkaProducer] = None
        self._stopping = asyncio.Event()
        self._started = False
        self._last_poll = time.monotonic()
        self._stats: Dict[str, int] = {outcome.value: 0 for outcome in Outcome}

    # ---------------------------------------------------------------- lifecycle
    async def start(self) -> None:
        await self._db.connect()
        await self._ensure_topics()

        self._dlq_producer = await retry_async(
            self._start_dlq_producer,
            operation="kafka_dlq_producer_start",
            policy=self._settings.retry_policy,
            retry_on=KAFKA_TRANSIENT_ERRORS,
            logger=logger,
        )
        self._consumer = await retry_async(
            self._start_consumer,
            operation="kafka_consumer_start",
            policy=self._settings.retry_policy,
            retry_on=KAFKA_TRANSIENT_ERRORS,
            logger=logger,
            topic=self._settings.kafka_topic_requests,
        )
        self._started = True
        self._last_poll = time.monotonic()
        log_event(
            logger,
            logging.INFO,
            "consumer_started",
            topic=self._settings.kafka_topic_requests,
            group_id=self._settings.kafka_consumer_group_id,
            bootstrap_servers=self._settings.kafka_bootstrap_servers,
        )

    async def _start_consumer(self) -> AIOKafkaConsumer:
        consumer = AIOKafkaConsumer(
            self._settings.kafka_topic_requests,
            bootstrap_servers=self._settings.kafka_bootstrap_servers,
            group_id=self._settings.kafka_consumer_group_id,
            enable_auto_commit=False,  # commit only after successful processing
            auto_offset_reset="earliest",
        )
        try:
            await consumer.start()
        except BaseException:
            await consumer.stop()
            raise
        return consumer

    async def _start_dlq_producer(self) -> AIOKafkaProducer:
        producer = AIOKafkaProducer(
            bootstrap_servers=self._settings.kafka_bootstrap_servers,
            acks="all",
            enable_idempotence=True,
        )
        try:
            await producer.start()
        except BaseException:
            await producer.stop()
            raise
        return producer

    async def _ensure_topics(self) -> None:
        """Create the request and dead-letter topics (best effort)."""
        topics = [
            NewTopic(
                name=self._settings.kafka_topic_requests,
                num_partitions=self._settings.kafka_topic_partitions,
                replication_factor=self._settings.kafka_topic_replication_factor,
            ),
            NewTopic(
                name=self._settings.kafka_topic_dlq,
                num_partitions=1,
                replication_factor=self._settings.kafka_topic_replication_factor,
            ),
        ]
        admin = AIOKafkaAdminClient(bootstrap_servers=self._settings.kafka_bootstrap_servers)
        try:
            await admin.start()
            await admin.create_topics(topics)
            log_event(logger, logging.INFO, "kafka_topics_created", topics=[t.name for t in topics])
        except Exception as exc:  # noqa: BLE001 - "already exists" or broker still booting
            log_event(logger, logging.INFO, "kafka_topic_create_skipped", reason=repr(exc))
        finally:
            with contextlib.suppress(Exception):
                await admin.close()

    def request_stop(self) -> None:
        log_event(logger, logging.INFO, "shutdown_requested")
        self._stopping.set()

    async def shutdown(self) -> None:
        self._stopping.set()
        if self._consumer is not None:
            with contextlib.suppress(Exception):
                await self._consumer.stop()
            self._consumer = None
        if self._dlq_producer is not None:
            with contextlib.suppress(Exception):
                await self._dlq_producer.stop()
            self._dlq_producer = None
        await self._db.close()
        self._started = False
        log_event(logger, logging.INFO, "consumer_stopped", stats=self._stats)

    # ---------------------------------------------------------------- main loop
    async def run(self) -> None:
        if self._consumer is None:
            raise RuntimeError("Worker not started")
        while not self._stopping.is_set():
            try:
                batch = await self._consumer.getmany(timeout_ms=1000, max_records=1)
            except Exception as exc:  # noqa: BLE001 - keep the worker alive
                log_event(logger, logging.ERROR, "poll_failed", exc_info=True, error=repr(exc))
                await self._sleep_unless_stopping(self._settings.retry_policy.max_delay)
                continue
            self._last_poll = time.monotonic()
            for tp, records in batch.items():
                for record in records:
                    await self._handle_record(tp, record)

    async def _handle_record(self, tp: TopicPartition, record: Any) -> None:
        headers = {name: value for name, value in (record.headers or [])}
        key = record.key.decode("utf-8", errors="replace") if record.key else None
        raw_correlation = headers.get("correlation_id")
        correlation_id = (
            raw_correlation.decode("utf-8", errors="replace")
            if raw_correlation
            else (key or str(uuid.uuid4()))
        )
        token = correlation_id_var.set(correlation_id)
        try:
            log_event(
                logger,
                logging.INFO,
                "message_consumed",
                topic=tp.topic,
                partition=tp.partition,
                offset=record.offset,
                request_id=key,
            )
            try:
                outcome = await process_message(
                    record.value,
                    db=self._db,
                    settings=self._settings,
                    dead_letter=self._publish_dead_letter,
                    key=key,
                )
            except Exception as exc:  # noqa: BLE001 - never kill the loop
                log_event(logger, logging.ERROR, "handler_crashed", exc_info=True, error=repr(exc))
                outcome = Outcome.RETRY_LATER

            self._stats[outcome.value] += 1
            if outcome is Outcome.RETRY_LATER:
                # Rewind so the very same message is delivered again, then back off.
                self._consumer.seek(tp, record.offset)
                log_event(
                    logger,
                    logging.WARNING,
                    "message_will_be_redelivered",
                    partition=tp.partition,
                    offset=record.offset,
                )
                await self._sleep_unless_stopping(self._settings.retry_policy.max_delay)
                return

            try:
                await self._consumer.commit({tp: record.offset + 1})
            except KafkaError as exc:
                # Not fatal: the message is redelivered and skipped as a duplicate.
                log_event(
                    logger,
                    logging.WARNING,
                    "offset_commit_failed",
                    partition=tp.partition,
                    offset=record.offset,
                    error=repr(exc),
                )
        finally:
            correlation_id_var.reset(token)

    async def _publish_dead_letter(
        self, raw_value: Optional[bytes], key: Optional[str], reason: str
    ) -> None:
        producer = self._dlq_producer
        if producer is None:
            raise RuntimeError("Dead-letter producer is not started")
        headers = [("dead_letter_reason", reason[:1000].encode("utf-8"))]
        correlation_id = correlation_id_var.get()
        if correlation_id:
            headers.append(("correlation_id", correlation_id.encode("utf-8")))

        async def _send() -> Any:
            return await asyncio.wait_for(
                producer.send_and_wait(
                    self._settings.kafka_topic_dlq,
                    value=raw_value,
                    key=key.encode("utf-8") if key else None,
                    headers=headers,
                ),
                timeout=15,
            )

        await retry_async(
            _send,
            operation="kafka_dead_letter_publish",
            policy=self._settings.retry_policy,
            retry_on=KAFKA_TRANSIENT_ERRORS,
            logger=logger,
            request_id=key,
        )

    async def _sleep_unless_stopping(self, seconds: float) -> None:
        with contextlib.suppress(asyncio.TimeoutError):
            await asyncio.wait_for(self._stopping.wait(), timeout=seconds)

    # ------------------------------------------------------------------- health
    async def health(self) -> Tuple[bool, Dict[str, Any]]:
        database_ok = await self._db.ping()
        seconds_since_poll = round(time.monotonic() - self._last_poll, 1)
        consumer_ok = (
            self._started
            and not self._stopping.is_set()
            and seconds_since_poll < max(self._settings.processing_lease_seconds, 30.0)
        )
        healthy = database_ok and consumer_ok
        return healthy, {
            "status": "ok" if healthy else "degraded",
            "service": "kafka_consumer",
            "dependencies": {
                "database": "ok" if database_ok else "unavailable",
                "kafka_consumer": "ok" if consumer_ok else "unavailable",
            },
            "seconds_since_last_poll": seconds_since_poll,
            "stats": dict(self._stats),
        }


# --------------------------------------------------------------------- entrypoint
async def main() -> None:
    settings = Settings.from_env()
    configure_logging(settings.service_name, settings.log_level)

    worker = InferenceWorker(settings)
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, worker.request_stop)

    # Start the probe first: it reports "degraded" (503) until the worker is ready.
    health_server = await start_health_server(
        worker.health, host="0.0.0.0", port=settings.consumer_health_port  # nosec B104
    )
    try:
        await worker.start()
        await worker.run()
    except Exception as exc:  # noqa: BLE001 - log fatal error, exit non-zero, let Docker restart us
        log_event(logger, logging.CRITICAL, "consumer_fatal_error", exc_info=True, error=repr(exc))
        raise
    finally:
        health_server.close()
        await worker.shutdown()


if __name__ == "__main__":
    asyncio.run(main())

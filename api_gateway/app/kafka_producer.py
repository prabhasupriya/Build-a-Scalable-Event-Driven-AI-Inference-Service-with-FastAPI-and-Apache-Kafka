"""Kafka producer used by the API gateway (aiokafka)."""
import asyncio
import json
import logging
from typing import Any, Dict, Optional

from aiokafka import AIOKafkaProducer
from aiokafka.admin import AIOKafkaAdminClient, NewTopic
from aiokafka.errors import KafkaError

from app.config import Settings
from app.logging_config import log_event
from app.retry import retry_async

logger = logging.getLogger(__name__)

KAFKA_TRANSIENT_ERRORS = (KafkaError, OSError, asyncio.TimeoutError)
SEND_TIMEOUT_SECONDS = 15


def _is_permanent(exc: BaseException) -> bool:
    """Kafka errors flagged non-retriable (e.g. message too large) are not retried."""
    return isinstance(exc, KafkaError) and not getattr(exc, "retriable", False)


def build_message_payload(
    request_id: str, input_data: str, model_id: str, timestamp: float
) -> Dict[str, Any]:
    """Build the JSON document published to the ``inference_requests`` topic."""
    return {
        "request_id": request_id,
        "input_data": input_data,
        "model_id": model_id,
        "timestamp": timestamp,
    }


class KafkaPublisher:
    """Owns the AIOKafkaProducer and publishes with retry + exponential backoff."""

    def __init__(self, settings: Settings) -> None:
        self._settings = settings
        self._policy = settings.retry_policy
        self._producer: Optional[AIOKafkaProducer] = None

    async def start(self) -> None:
        await self._ensure_topic()

        async def _start() -> AIOKafkaProducer:
            producer = AIOKafkaProducer(
                bootstrap_servers=self._settings.kafka_bootstrap_servers,
                acks="all",  # wait for all in-sync replicas: no silent loss
                enable_idempotence=True,  # broker de-duplicates producer retries
                linger_ms=5,
                request_timeout_ms=10_000,
            )
            try:
                await producer.start()
            except BaseException:
                await producer.stop()
                raise
            return producer

        self._producer = await retry_async(
            _start,
            operation="kafka_producer_start",
            policy=self._policy,
            retry_on=KAFKA_TRANSIENT_ERRORS,
            giveup=_is_permanent,
            logger=logger,
            bootstrap_servers=self._settings.kafka_bootstrap_servers,
        )
        log_event(
            logger,
            logging.INFO,
            "kafka_producer_started",
            bootstrap_servers=self._settings.kafka_bootstrap_servers,
        )

    async def stop(self) -> None:
        if self._producer is not None:
            await self._producer.stop()
            self._producer = None
            log_event(logger, logging.INFO, "kafka_producer_stopped")

    async def publish(
        self,
        topic: str,
        payload: Dict[str, Any],
        *,
        key: str,
        correlation_id: Optional[str] = None,
    ) -> Any:
        """Publish ``payload`` and wait for the broker acknowledgement.

        The request id is used as the message key so every message for one
        request lands on the same partition (preserving per-request ordering).
        """
        if self._producer is None:
            raise RuntimeError("Kafka producer is not started")
        producer = self._producer

        value = json.dumps(payload).encode("utf-8")
        headers = [("correlation_id", correlation_id.encode("utf-8"))] if correlation_id else []

        async def _send() -> Any:
            return await asyncio.wait_for(
                producer.send_and_wait(topic, value=value, key=key.encode("utf-8"), headers=headers),
                timeout=SEND_TIMEOUT_SECONDS,
            )

        metadata = await retry_async(
            _send,
            operation="kafka_publish",
            policy=self._policy,
            retry_on=KAFKA_TRANSIENT_ERRORS,
            giveup=_is_permanent,
            logger=logger,
            topic=topic,
            request_id=key,
        )
        log_event(
            logger,
            logging.INFO,
            "message_published",
            topic=topic,
            request_id=key,
            partition=getattr(metadata, "partition", None),
            offset=getattr(metadata, "offset", None),
        )
        return metadata

    async def ping(self) -> bool:
        """Liveness probe for ``/health``: can we still fetch topic metadata?"""
        if self._producer is None:
            return False
        try:
            await asyncio.wait_for(
                self._producer.partitions_for(self._settings.kafka_topic_requests), timeout=5
            )
            return True
        except Exception as exc:  # noqa: BLE001 - health probe must never raise
            log_event(logger, logging.WARNING, "kafka_ping_failed", error=repr(exc))
            return False

    async def _ensure_topic(self) -> None:
        """Create the topic with the configured partition count (best effort)."""
        admin = AIOKafkaAdminClient(bootstrap_servers=self._settings.kafka_bootstrap_servers)
        try:
            await admin.start()
            await admin.create_topics(
                [
                    NewTopic(
                        name=self._settings.kafka_topic_requests,
                        num_partitions=self._settings.kafka_topic_partitions,
                        replication_factor=self._settings.kafka_topic_replication_factor,
                    )
                ]
            )
            log_event(
                logger,
                logging.INFO,
                "kafka_topic_created",
                topic=self._settings.kafka_topic_requests,
                partitions=self._settings.kafka_topic_partitions,
            )
        except Exception as exc:  # noqa: BLE001 - "already exists" or broker still booting
            log_event(
                logger,
                logging.INFO,
                "kafka_topic_create_skipped",
                topic=self._settings.kafka_topic_requests,
                reason=repr(exc),
            )
        finally:
            try:
                await admin.close()
            except Exception:  # noqa: BLE001
                pass

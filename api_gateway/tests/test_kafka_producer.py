"""Unit tests for the Kafka producer wrapper (no broker needed)."""
import json

import pytest
from aiokafka.errors import KafkaConnectionError, MessageSizeTooLargeError

from app.config import Settings
from app.kafka_producer import KafkaPublisher


class FakeAIOKafkaProducer:
    """Stands in for ``AIOKafkaProducer``; fails ``failures`` times, then succeeds."""

    def __init__(self, failures=0, error=None):
        self.failures = failures
        self.error = error or KafkaConnectionError("broker down")
        self.calls = []

    async def send_and_wait(self, topic, value=None, key=None, headers=None):
        self.calls.append({"topic": topic, "value": value, "key": key, "headers": headers})
        if len(self.calls) <= self.failures:
            raise self.error

        class _Metadata:
            partition = 2
            offset = 41

        return _Metadata()

    async def partitions_for(self, topic):
        if self.failures:
            raise KafkaConnectionError("broker down")
        return {0, 1, 2}


@pytest.fixture
def settings():
    return Settings(retry_max_attempts=3, retry_base_delay_seconds=0, retry_max_delay_seconds=0)


@pytest.fixture
def publisher(settings):
    return KafkaPublisher(settings)


async def test_publish_sends_json_value_with_request_id_as_key(publisher):
    fake = FakeAIOKafkaProducer()
    publisher._producer = fake
    payload = {"request_id": "abc", "input_data": "hi", "model_id": "m", "timestamp": 1.5}

    await publisher.publish("inference_requests", payload, key="abc", correlation_id="corr-1")

    (call,) = fake.calls
    assert call["topic"] == "inference_requests"
    assert call["key"] == b"abc"  # same key -> same partition -> per-request ordering
    assert json.loads(call["value"]) == payload
    assert call["headers"] == [("correlation_id", b"corr-1")]


async def test_publish_without_correlation_id_sends_no_headers(publisher):
    fake = FakeAIOKafkaProducer()
    publisher._producer = fake
    await publisher.publish("t", {"request_id": "abc"}, key="abc")
    assert fake.calls[0]["headers"] == []


async def test_publish_retries_transient_errors_with_backoff(publisher):
    fake = FakeAIOKafkaProducer(failures=2)
    publisher._producer = fake

    metadata = await publisher.publish("t", {"request_id": "abc"}, key="abc")

    assert len(fake.calls) == 3  # 2 failures + 1 success
    assert metadata.partition == 2


async def test_publish_raises_after_retries_are_exhausted(publisher):
    fake = FakeAIOKafkaProducer(failures=99)
    publisher._producer = fake

    with pytest.raises(KafkaConnectionError):
        await publisher.publish("t", {"request_id": "abc"}, key="abc")

    assert len(fake.calls) == 3  # RETRY_MAX_ATTEMPTS


async def test_publish_does_not_retry_permanent_errors(publisher):
    fake = FakeAIOKafkaProducer(failures=99, error=MessageSizeTooLargeError())
    publisher._producer = fake

    with pytest.raises(MessageSizeTooLargeError):
        await publisher.publish("t", {"request_id": "abc"}, key="abc")

    assert len(fake.calls) == 1


async def test_publish_before_start_fails_fast(publisher):
    with pytest.raises(RuntimeError):
        await publisher.publish("t", {}, key="abc")


async def test_ping_reflects_broker_availability(publisher):
    assert await publisher.ping() is False  # not started

    publisher._producer = FakeAIOKafkaProducer()
    assert await publisher.ping() is True

    publisher._producer = FakeAIOKafkaProducer(failures=1)
    assert await publisher.ping() is False

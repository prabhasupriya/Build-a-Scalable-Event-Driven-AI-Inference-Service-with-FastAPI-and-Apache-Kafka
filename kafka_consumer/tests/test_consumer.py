"""Unit tests for the consumer's message handling (no Kafka / Postgres needed)."""
import json
import time
from types import SimpleNamespace

import pytest
from aiokafka import TopicPartition
from aiokafka.errors import KafkaConnectionError
from asyncpg.exceptions import PostgresConnectionError

from app import consumer as consumer_module
from app.consumer import (
    InferenceError,
    InferenceWorker,
    MessageDeserializationError,
    Outcome,
    parse_message,
    process_message,
    run_inference,
)


def make_message(request_id="req-1", input_data="a cat", model_id="resnet50", **extra) -> bytes:
    payload = {
        "request_id": request_id,
        "input_data": input_data,
        "model_id": model_id,
        "timestamp": time.time(),
        **extra,
    }
    return json.dumps(payload).encode()


# ----------------------------------------------------------- message parsing
def test_parse_message_accepts_valid_payload():
    message = parse_message(make_message())
    assert message.request_id == "req-1"
    assert message.input_data == "a cat"
    assert message.model_id == "resnet50"
    assert isinstance(message.timestamp, float)


def test_parse_message_defaults_model_id_and_timestamp():
    raw = json.dumps({"request_id": "r", "input_data": "x"}).encode()
    message = parse_message(raw)
    assert message.model_id == "default_model"
    assert message.timestamp == pytest.approx(time.time(), abs=5)


@pytest.mark.parametrize(
    "raw",
    [
        None,
        b"",
        b"not json at all",
        b"\xff\xfe\x00",  # not valid UTF-8
        b"[1, 2, 3]",  # JSON but not an object
        json.dumps({"input_data": "x"}).encode(),  # no request_id
        json.dumps({"request_id": "  ", "input_data": "x"}).encode(),
        json.dumps({"request_id": 7, "input_data": "x"}).encode(),
        json.dumps({"request_id": "r"}).encode(),  # no input_data
        json.dumps({"request_id": "r", "input_data": ""}).encode(),
        json.dumps({"request_id": "r", "input_data": "x", "model_id": 5}).encode(),
        json.dumps({"request_id": "r", "input_data": "bad\u0000byte"}).encode(),
    ],
)
def test_parse_message_rejects_malformed_payloads(raw):
    with pytest.raises(MessageDeserializationError):
        parse_message(raw)


def test_parse_message_error_carries_request_id_when_known():
    raw = json.dumps({"request_id": "known", "input_data": ""}).encode()
    with pytest.raises(MessageDeserializationError) as info:
        parse_message(raw)
    assert info.value.request_id == "known"


@pytest.mark.parametrize("bad_timestamp", ["yesterday", True, -5, float("inf"), 10**12])
def test_parse_message_replaces_bad_timestamps(bad_timestamp):
    raw = json.dumps({"request_id": "r", "input_data": "x", "timestamp": bad_timestamp}).encode()
    # json.dumps writes Infinity for inf; json.loads accepts it, parse_message must cope.
    assert parse_message(raw).timestamp == pytest.approx(time.time(), abs=5)


# ------------------------------------------------------ inference simulation
async def test_run_inference_returns_deterministic_output():
    output = await run_inference("hello", "m1", simulated_seconds=0)
    assert output == "Processed: hello by m1"


async def test_run_inference_waits_for_the_simulated_duration(monkeypatch):
    slept = []

    async def fake_sleep(seconds):
        slept.append(seconds)

    monkeypatch.setattr(consumer_module.asyncio, "sleep", fake_sleep)
    await run_inference("hello", "m1", simulated_seconds=5)
    assert slept == [5]


async def test_run_inference_fails_for_marker():
    with pytest.raises(InferenceError):
        await run_inference("please __fail__", "m1", simulated_seconds=0, failure_marker="__fail__")


# ------------------------------------------------------- process_message
async def test_successful_message_is_stored_as_completed(db, settings, dead_letter):
    db.seed("req-1", "PENDING")  # the API stored it first
    outcome = await process_message(
        make_message(), db=db, settings=settings, dead_letter=dead_letter, key="req-1"
    )
    assert outcome is Outcome.COMPLETED
    assert db.rows["req-1"]["status"] == "COMPLETED"
    assert db.rows["req-1"]["output_data"] == "Processed: a cat by resnet50"
    assert db.rows["req-1"]["attempts"] == 1
    assert dead_letter.messages == []


async def test_consumer_creates_the_row_when_the_api_write_is_missing(db, settings, dead_letter):
    outcome = await process_message(
        make_message("ghost"), db=db, settings=settings, dead_letter=dead_letter
    )
    assert outcome is Outcome.COMPLETED
    assert db.rows["ghost"]["status"] == "COMPLETED"


async def test_duplicate_delivery_is_processed_only_once(db, settings, dead_letter, monkeypatch):
    runs = []
    real = consumer_module.run_inference

    async def counting(*args, **kwargs):
        runs.append(args)
        return await real(*args, **kwargs)

    monkeypatch.setattr(consumer_module, "run_inference", counting)
    db.seed("req-1", "PENDING")

    first = await process_message(make_message(), db=db, settings=settings, dead_letter=dead_letter)
    second = await process_message(make_message(), db=db, settings=settings, dead_letter=dead_letter)
    third = await process_message(make_message(), db=db, settings=settings, dead_letter=dead_letter)

    assert (first, second, third) == (
        Outcome.COMPLETED,
        Outcome.DUPLICATE_SKIPPED,
        Outcome.DUPLICATE_SKIPPED,
    )
    assert len(runs) == 1
    assert db.rows["req-1"]["attempts"] == 1


@pytest.mark.parametrize("status", ["COMPLETED", "FAILED"])
async def test_finished_requests_are_never_reprocessed(db, settings, dead_letter, status):
    db.seed("req-1", status, output_data="original")
    outcome = await process_message(make_message(), db=db, settings=settings, dead_letter=dead_letter)
    assert outcome is Outcome.DUPLICATE_SKIPPED
    assert db.rows["req-1"]["status"] == status
    assert db.rows["req-1"]["output_data"] == "original"


async def test_request_held_by_another_worker_is_retried_later(db, settings, dead_letter):
    db.seed("req-1", "PROCESSING")
    outcome = await process_message(make_message(), db=db, settings=settings, dead_letter=dead_letter)
    assert outcome is Outcome.RETRY_LATER  # offset must NOT be committed
    assert db.rows["req-1"]["status"] == "PROCESSING"


async def test_failing_inference_is_marked_failed_and_dead_lettered(db, settings, dead_letter):
    db.seed("req-1", "PENDING")
    outcome = await process_message(
        make_message(input_data="boom __fail__"),
        db=db,
        settings=settings,
        dead_letter=dead_letter,
        key="req-1",
    )
    assert outcome is Outcome.FAILED
    assert db.rows["req-1"]["status"] == "FAILED"
    assert "after 2 attempts" in db.rows["req-1"]["error_message"]
    assert len(dead_letter.messages) == 1
    assert dead_letter.messages[0][1] == "req-1"


async def test_transient_inference_errors_are_retried(db, settings, dead_letter, monkeypatch):
    calls = {"n": 0}

    async def flaky(input_data, model_id, **_):
        calls["n"] += 1
        if calls["n"] == 1:
            raise InferenceError("GPU hiccup")
        return "recovered"

    monkeypatch.setattr(consumer_module, "run_inference", flaky)
    db.seed("req-1", "PENDING")
    outcome = await process_message(make_message(), db=db, settings=settings, dead_letter=dead_letter)
    assert outcome is Outcome.COMPLETED
    assert calls["n"] == 2
    assert db.rows["req-1"]["output_data"] == "recovered"


async def test_malformed_message_goes_to_dead_letter_queue(db, settings, dead_letter):
    outcome = await process_message(
        b"definitely not json", db=db, settings=settings, dead_letter=dead_letter, key="k1"
    )
    assert outcome is Outcome.DEAD_LETTERED
    raw, key, reason = dead_letter.messages[0]
    assert raw == b"definitely not json"
    assert key == "k1"
    assert reason.startswith("deserialization:")
    assert db.claim_calls == 0


async def test_malformed_message_with_known_id_marks_the_row_failed(db, settings, dead_letter):
    db.seed("req-1", "PENDING")
    raw = json.dumps({"request_id": "req-1", "input_data": ""}).encode()
    outcome = await process_message(raw, db=db, settings=settings, dead_letter=dead_letter)
    assert outcome is Outcome.DEAD_LETTERED
    assert db.rows["req-1"]["status"] == "FAILED"
    assert "Invalid message" in db.rows["req-1"]["error_message"]


async def test_dead_letter_failure_means_retry_not_loss(db, settings, dead_letter):
    dead_letter.fail_with = KafkaConnectionError("dlq down")
    outcome = await process_message(b"garbage", db=db, settings=settings, dead_letter=dead_letter)
    assert outcome is Outcome.RETRY_LATER


async def test_database_outage_on_claim_is_retried_later(db, settings, dead_letter):
    db.fail_claim = PostgresConnectionError("db down")
    outcome = await process_message(make_message(), db=db, settings=settings, dead_letter=dead_letter)
    assert outcome is Outcome.RETRY_LATER
    assert dead_letter.messages == []  # an outage must never dead-letter good messages


async def test_database_outage_when_storing_result_is_retried_later(db, settings, dead_letter):
    db.seed("req-1", "PENDING")
    db.fail_completed = PostgresConnectionError("db down")
    outcome = await process_message(make_message(), db=db, settings=settings, dead_letter=dead_letter)
    assert outcome is Outcome.RETRY_LATER


async def test_unexpected_error_is_isolated_as_poison_message(db, settings, dead_letter, monkeypatch):
    async def broken(*_args, **_kwargs):
        raise RuntimeError("model blew up")

    monkeypatch.setattr(consumer_module, "run_inference", broken)
    db.seed("req-1", "PENDING")
    outcome = await process_message(make_message(), db=db, settings=settings, dead_letter=dead_letter)
    assert outcome is Outcome.DEAD_LETTERED
    assert db.rows["req-1"]["status"] == "FAILED"
    assert "model blew up" in db.rows["req-1"]["error_message"]


# ------------------------------------------- worker: offsets & redelivery
class FakeKafkaConsumer:
    def __init__(self) -> None:
        self.commits = []
        self.seeks = []

    async def commit(self, offsets):
        self.commits.append(offsets)

    def seek(self, tp, offset):
        self.seeks.append((tp, offset))


def make_record(value: bytes, offset=7, key=b"req-1", headers=None):
    return SimpleNamespace(value=value, offset=offset, key=key, headers=headers or [])


@pytest.fixture
def worker(db, settings):
    instance = InferenceWorker(settings, db=db)
    instance._consumer = FakeKafkaConsumer()
    instance._started = True
    return instance


TP = TopicPartition("inference_requests", 0)


async def test_offset_is_committed_only_after_successful_processing(worker, db):
    db.seed("req-1", "PENDING")
    await worker._handle_record(TP, make_record(make_message()))
    assert worker._consumer.commits == [{TP: 8}]  # offset + 1
    assert worker._consumer.seeks == []
    assert db.rows["req-1"]["status"] == "COMPLETED"


async def test_offset_is_committed_for_duplicates_and_poison_messages(worker, db):
    db.seed("req-1", "COMPLETED")
    worker._dlq_producer = object()
    worker._publish_dead_letter = _noop_dead_letter
    await worker._handle_record(TP, make_record(make_message(), offset=1))
    await worker._handle_record(TP, make_record(b"garbage", offset=2))
    assert worker._consumer.commits == [{TP: 2}, {TP: 3}]


async def test_offset_is_not_committed_and_message_is_redelivered_on_db_outage(worker, db):
    db.fail_claim = PostgresConnectionError("db down")

    slept = []

    async def fake_sleep(seconds):
        slept.append(seconds)

    worker._sleep_unless_stopping = fake_sleep
    await worker._handle_record(TP, make_record(make_message(), offset=7))

    assert worker._consumer.commits == []  # nothing committed -> nothing lost
    assert worker._consumer.seeks == [(TP, 7)]  # rewound to the failed message
    assert len(slept) == 1  # backed off before redelivery


async def test_correlation_id_from_kafka_header_is_used_for_logging(worker, db):
    from app.logging_config import correlation_id_var

    seen = []
    original = consumer_module.process_message

    async def spy(*args, **kwargs):
        seen.append(correlation_id_var.get())
        return await original(*args, **kwargs)

    consumer_module.process_message, backup = spy, consumer_module.process_message
    try:
        db.seed("req-1", "PENDING")
        record = make_record(make_message(), headers=[("correlation_id", b"corr-123")])
        await worker._handle_record(TP, record)
    finally:
        consumer_module.process_message = backup
    assert seen == ["corr-123"]
    assert correlation_id_var.get() is None  # context restored afterwards


async def _noop_dead_letter(raw_value, key, reason):
    return None

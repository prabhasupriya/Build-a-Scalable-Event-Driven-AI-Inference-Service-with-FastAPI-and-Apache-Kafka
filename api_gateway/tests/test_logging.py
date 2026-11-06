"""Unit tests for structured JSON logging."""
import json
import logging

from app.logging_config import JsonFormatter, correlation_id_var, log_event


def _record(message="hello", **fields):
    logger = logging.getLogger("test")
    record = logger.makeRecord("test", logging.INFO, __file__, 1, message, (), None)
    record.fields = fields
    return record


def test_formatter_emits_single_line_json_with_context():
    token = correlation_id_var.set("corr-1")
    try:
        line = JsonFormatter("svc").format(_record("evt", request_id="r1", attempt=2))
    finally:
        correlation_id_var.reset(token)
    data = json.loads(line)
    assert "\n" not in line
    assert data["message"] == "evt"
    assert data["service"] == "svc"
    assert data["level"] == "INFO"
    assert data["correlation_id"] == "corr-1"
    assert data["request_id"] == "r1"
    assert data["attempt"] == 2
    assert "timestamp" in data


def test_formatter_does_not_let_fields_overwrite_core_keys():
    data = json.loads(JsonFormatter("svc").format(_record("evt", level="HACKED", service="x")))
    assert data["level"] == "INFO"
    assert data["service"] == "svc"


def test_formatter_includes_exception_traceback():
    logger = logging.getLogger("test")
    try:
        raise RuntimeError("kaboom")
    except RuntimeError:
        import sys

        record = logger.makeRecord("test", logging.ERROR, __file__, 1, "failed", (), sys.exc_info())
    data = json.loads(JsonFormatter("svc").format(record))
    assert "RuntimeError: kaboom" in data["exception"]


def test_log_event_attaches_fields(caplog):
    with caplog.at_level(logging.INFO):
        log_event(logging.getLogger("x"), logging.INFO, "thing_happened", request_id="abc")
    assert caplog.records[0].fields == {"request_id": "abc"}

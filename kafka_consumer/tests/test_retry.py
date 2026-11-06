"""Unit tests for the exponential-backoff retry helper."""
import pytest

from app import retry as retry_module
from app.retry import RetryPolicy, retry_async


@pytest.fixture(autouse=True)
def no_sleep(monkeypatch):
    """Record the delays instead of really sleeping."""
    delays = []

    async def fake_sleep(seconds):
        delays.append(seconds)

    monkeypatch.setattr(retry_module.asyncio, "sleep", fake_sleep)
    return delays


def test_delay_grows_exponentially_and_is_capped():
    policy = RetryPolicy(max_attempts=10, base_delay=1.0, max_delay=5.0, jitter=0)
    assert [policy.delay_for(n) for n in (1, 2, 3, 4, 5)] == [1.0, 2.0, 4.0, 5.0, 5.0]


def test_jitter_never_exceeds_max_delay():
    policy = RetryPolicy(max_attempts=10, base_delay=1.0, max_delay=2.0, jitter=0.5)
    assert all(policy.delay_for(n) <= 2.0 for n in range(1, 10))


def test_invalid_policy_rejected():
    with pytest.raises(ValueError):
        RetryPolicy(max_attempts=0)


async def test_succeeds_after_transient_failures(no_sleep):
    calls = {"n": 0}

    async def flaky():
        calls["n"] += 1
        if calls["n"] < 3:
            raise ConnectionError("boom")
        return "ok"

    policy = RetryPolicy(max_attempts=5, base_delay=1.0, max_delay=10, jitter=0)
    result = await retry_async(flaky, operation="t", policy=policy, retry_on=(ConnectionError,))
    assert result == "ok"
    assert calls["n"] == 3
    assert no_sleep == [1.0, 2.0]  # exponential backoff between attempts


async def test_gives_up_after_max_attempts(no_sleep):
    calls = {"n": 0}

    async def always_fails():
        calls["n"] += 1
        raise ConnectionError("boom")

    policy = RetryPolicy(max_attempts=3, base_delay=0.1, max_delay=1, jitter=0)
    with pytest.raises(ConnectionError):
        await retry_async(always_fails, operation="t", policy=policy, retry_on=(ConnectionError,))
    assert calls["n"] == 3
    assert len(no_sleep) == 2


async def test_non_retryable_exception_is_raised_immediately(no_sleep):
    async def bad():
        raise ValueError("nope")

    with pytest.raises(ValueError):
        await retry_async(
            bad, operation="t", policy=RetryPolicy(max_attempts=5), retry_on=(ConnectionError,)
        )
    assert no_sleep == []


async def test_giveup_predicate_stops_retrying_permanent_errors(no_sleep):
    calls = {"n": 0}

    async def permanent():
        calls["n"] += 1
        raise ConnectionError("permanent")

    with pytest.raises(ConnectionError):
        await retry_async(
            permanent,
            operation="t",
            policy=RetryPolicy(max_attempts=5),
            retry_on=(ConnectionError,),
            giveup=lambda exc: True,
        )
    assert calls["n"] == 1

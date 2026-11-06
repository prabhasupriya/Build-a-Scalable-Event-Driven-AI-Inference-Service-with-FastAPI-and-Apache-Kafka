"""Async retry helper with exponential backoff and jitter."""
import asyncio
import logging
import random
from dataclasses import dataclass
from typing import Awaitable, Callable, Optional, Tuple, Type, TypeVar

from app.logging_config import log_event

T = TypeVar("T")

_default_logger = logging.getLogger(__name__)
# Jitter does not need to be unpredictable; SystemRandom keeps static analysers quiet.
_JITTER_RNG = random.SystemRandom()


@dataclass(frozen=True)
class RetryPolicy:
    """Exponential backoff: ``base_delay * 2**(attempt-1)`` capped at ``max_delay``."""

    max_attempts: int = 5
    base_delay: float = 0.5
    max_delay: float = 10.0
    jitter: float = 0.1  # fraction of the delay that may be added randomly

    def __post_init__(self) -> None:
        if self.max_attempts < 1:
            raise ValueError("max_attempts must be >= 1")

    def delay_for(self, attempt: int) -> float:
        """Delay to wait after the ``attempt``-th (1-based) failure."""
        delay = min(self.max_delay, self.base_delay * (2 ** (attempt - 1)))
        if self.jitter > 0:
            delay += _JITTER_RNG.uniform(0, delay * self.jitter)
        return min(delay, self.max_delay)


async def retry_async(
    fn: Callable[[], Awaitable[T]],
    *,
    operation: str,
    policy: RetryPolicy,
    retry_on: Tuple[Type[BaseException], ...],
    giveup: Optional[Callable[[BaseException], bool]] = None,
    logger: Optional[logging.Logger] = None,
    **context: object,
) -> T:
    """Run ``fn`` until it succeeds, retrying ``retry_on`` errors with backoff.

    ``giveup`` may return True for errors that are known to be permanent so they
    are raised immediately. Every retry and the final failure are logged with
    ``operation`` and any extra ``context`` fields.
    """
    log = logger or _default_logger
    attempt = 0
    while True:
        attempt += 1
        try:
            return await fn()
        except retry_on as exc:
            permanent = giveup is not None and giveup(exc)
            if permanent or attempt >= policy.max_attempts:
                log_event(
                    log,
                    logging.ERROR,
                    "operation_failed",
                    exc_info=True,
                    operation=operation,
                    attempt=attempt,
                    max_attempts=policy.max_attempts,
                    permanent_error=permanent,
                    error=repr(exc),
                    error_type=type(exc).__name__,
                    **context,
                )
                raise
            delay = policy.delay_for(attempt)
            log_event(
                log,
                logging.WARNING,
                "operation_retrying",
                operation=operation,
                attempt=attempt,
                max_attempts=policy.max_attempts,
                retry_in_seconds=round(delay, 3),
                error=repr(exc),
                error_type=type(exc).__name__,
                **context,
            )
            await asyncio.sleep(delay)

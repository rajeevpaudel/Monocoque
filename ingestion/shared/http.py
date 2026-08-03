"""Shared HTTP client with retry, rate limiting, and structured logging."""

import logging
import random
import threading
import time
from typing import Any

import httpx
import structlog
from tenacity import (
    before_sleep_log,
    retry,
    retry_if_exception,
    stop_after_attempt,
    wait_exponential,
)

log = structlog.get_logger()


class RateLimiter:
    """Thread-safe fixed-rate limiter with jitter, shared by every API client.

    Each client (Jolpica, OpenF1, ...) gets its own instance since each API has a
    different polite-usage ceiling; the wait/lock logic itself is identical, so it
    lives here once instead of being reimplemented per client.
    """

    def __init__(self, requests_per_second: float, jitter_max: float = 0.3) -> None:
        self._min_interval = 1.0 / requests_per_second
        self._jitter_max = jitter_max
        self._last_request_time: float = 0.0
        self._lock = threading.Lock()

    def wait(self) -> None:
        with self._lock:
            jitter = random.uniform(0, self._jitter_max)
            elapsed = time.monotonic() - self._last_request_time
            wait = max(0.0, self._min_interval + jitter - elapsed)
            if wait:
                time.sleep(wait)
            self._last_request_time = time.monotonic()


# Jolpica asks for polite usage: keep well under 4 req/s
_RATE_LIMIT_RPS = 2
_limiter = RateLimiter(_RATE_LIMIT_RPS)


def _rate_limit():
    _limiter.wait()


def _is_retryable(exc: BaseException) -> bool:
    """Retry on 429 (rate limit) and 5xx. Never retry on 4xx or non-HTTP errors."""
    if isinstance(exc, httpx.HTTPStatusError):
        return exc.response.status_code in (429,) or exc.response.status_code >= 500
    # Non-HTTP exceptions (JSON decode errors, connection errors) are not retried:
    # a JSON decode error on a well-formed request indicates a persistent upstream
    # bug, not a transient failure, and retrying wastes time.
    return isinstance(exc, httpx.TransportError)


@retry(
    retry=retry_if_exception(_is_retryable),
    stop=stop_after_attempt(6),
    wait=wait_exponential(multiplier=2, min=5, max=60),
    before_sleep=before_sleep_log(logging.getLogger(__name__), logging.WARNING),
)
def get_json(url: str, params: dict[str, Any] | None = None, rate_limit: bool = True) -> Any:
    """Synchronous GET that returns parsed JSON. Applies rate limiting + retry."""
    if rate_limit:
        _rate_limit()
    log.debug("GET", url=url, params=params)
    with httpx.Client(timeout=30) as client:
        resp = client.get(url, params=params)
        resp.raise_for_status()
    return resp.json()

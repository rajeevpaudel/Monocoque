"""Shared HTTP client with retry, rate limiting, and structured logging."""

import logging
import random
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

# Jolpica asks for polite usage: keep well under 4 req/s
_RATE_LIMIT_RPS = 2
_last_request_time: float = 0.0


def _rate_limit():
    global _last_request_time
    min_interval = 1.0 / _RATE_LIMIT_RPS
    jitter = random.uniform(0, 0.3)
    elapsed = time.monotonic() - _last_request_time
    wait = max(0.0, min_interval + jitter - elapsed)
    if wait:
        time.sleep(wait)
    _last_request_time = time.monotonic()


def _is_retryable(exc: BaseException) -> bool:
    """Retry on 429 (rate limit) and 5xx. Never retry on 404 or other 4xx."""
    if isinstance(exc, httpx.HTTPStatusError):
        return exc.response.status_code in (429,) or exc.response.status_code >= 500
    return True


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

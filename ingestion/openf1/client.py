"""OpenF1 API client (historical, no auth required)."""

import json
import time
import urllib.parse
from typing import Any

import httpx
import structlog

from ingestion.shared.http import RateLimiter

BASE_URL = "https://api.openf1.org/v1"
_MAX_RETRIES = 6
_RATE_LIMIT_RPS = 3
_MAX_429_WAIT = 120
_limiter = RateLimiter(_RATE_LIMIT_RPS, jitter_max=0.1)

log = structlog.get_logger()


def _rate_limit_wait(resp: httpx.Response, attempt: int) -> float:
    """Back-off for 429: honor Retry-After if present, else 10s, 20s, 40s... capped."""
    retry_after = resp.headers.get("Retry-After")
    if retry_after:
        try:
            return min(float(retry_after), _MAX_429_WAIT)
        except ValueError:
            pass
    return min(10 * 2**attempt, _MAX_429_WAIT)


def _rate_limit() -> None:
    _limiter.wait()


class TooLargeError(Exception):
    """Raised when OpenF1 returns 422 (request too large to serve at once)."""


class OpenF1APIError(Exception):
    """Raised when OpenF1 is unreachable after all retries.

    Commonly caused by the API being under load during a live session.
    """


def _build_url(base: str, params: dict[str, Any]) -> str:
    """Build URL keeping comparison operators (>=, <=, >, <) literal in param names.

    httpx URL-encodes dict keys, turning 'date>=' into 'date%3E%3D' which the
    OpenF1 API does not recognise. Params with operator chars are appended raw.
    """
    normal, raw_parts = {}, []
    for k, v in params.items():
        if ">" in k or "<" in k:
            # Keep the operator literal; colons in ISO timestamps are safe in query strings
            raw_parts.append(f"{k}={urllib.parse.quote(str(v), safe=':.-')}")
        else:
            normal[k] = v

    url = base
    if normal:
        url += "?" + urllib.parse.urlencode(normal)
    if raw_parts:
        sep = "&" if "?" in url else "?"
        url += sep + "&".join(raw_parts)
    return url


def get(endpoint: str, params: dict[str, Any] | None = None) -> list[dict]:
    """GET an OpenF1 endpoint, retrying up to _MAX_RETRIES times on transient errors.

    Raises TooLargeError on 422 — caller must split the request.
    Returns [] on 404.
    """
    url = _build_url(f"{BASE_URL}/{endpoint}", params or {})
    last_exc: Exception = RuntimeError("no attempts made")

    for attempt in range(_MAX_RETRIES):
        try:
            _rate_limit()
            with httpx.Client(timeout=60) as client:
                resp = client.get(url)
            if resp.status_code == 404:
                return []
            if resp.status_code == 422:
                raise TooLargeError(f"{url} params={params}")
            resp.raise_for_status()
            data = resp.json()
            return data if isinstance(data, list) else []
        except TooLargeError:
            raise  # never retry 422 — the same request will always fail
        except httpx.HTTPStatusError as e:
            if e.response.status_code == 401:
                # OpenF1 returns 401 with a JSON detail when a live session is in
                # progress — the entire API is locked until the session ends.
                # Retrying is pointless; surface the API's own message immediately.
                try:
                    detail = e.response.json().get("detail", "")
                except Exception:
                    detail = ""
                msg = f"OpenF1 API returned 401 for {url}"
                if detail:
                    msg += f": {detail}"
                log.error("openf1 access denied — live session in progress", url=url, detail=detail)
                raise OpenF1APIError(msg) from e
            last_exc = e
            if attempt < _MAX_RETRIES - 1:
                wait = (
                    _rate_limit_wait(e.response, attempt)
                    if e.response.status_code == 429
                    else 2**attempt
                )
                log.warning(
                    "openf1 request failed, retrying",
                    url=url,
                    attempt=attempt + 1,
                    status=e.response.status_code,
                    wait=wait,
                )
                time.sleep(wait)
        except Exception as e:
            last_exc = e
            if attempt < _MAX_RETRIES - 1:
                wait = 2**attempt
                log.warning(
                    "openf1 request failed, retrying",
                    url=url,
                    attempt=attempt + 1,
                    wait=wait,
                    error=str(e),
                )
                time.sleep(wait)

    # All retries exhausted — build a descriptive error with a live-session hint
    # for 5xx / timeout failures, which are the most common cause.
    status_code = None
    if isinstance(last_exc, httpx.HTTPStatusError):
        status_code = last_exc.response.status_code

    hint = (
        " The OpenF1 API is often slow or returns 5xx during a live session — "
        "retry once the session has finished."
        if (status_code is None or status_code >= 500)
        else ""
    )
    msg = (
        f"OpenF1 request failed after {_MAX_RETRIES} attempts: {url}"
        + (f" (HTTP {status_code})" if status_code else f" ({type(last_exc).__name__}: {last_exc})")
        + hint
    )
    log.error("openf1 api unavailable", url=url, attempts=_MAX_RETRIES, status=status_code)
    raise OpenF1APIError(msg) from last_exc


def raw(obj: Any) -> str:
    return json.dumps(obj, ensure_ascii=False)

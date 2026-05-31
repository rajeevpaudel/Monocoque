"""OpenF1 API client (historical, no auth required)."""

import json
import random
import time
import urllib.parse
from typing import Any

import httpx
import structlog

BASE_URL = "https://api.openf1.org/v1"
_MAX_RETRIES = 3
_RATE_LIMIT_RPS = 3
_last_request_time: float = 0.0

log = structlog.get_logger()


def _rate_limit() -> None:
    global _last_request_time
    min_interval = 1.0 / _RATE_LIMIT_RPS
    elapsed = time.monotonic() - _last_request_time
    wait = max(0.0, min_interval - elapsed) + random.uniform(0, 0.1)
    if wait:
        time.sleep(wait)
    _last_request_time = time.monotonic()


class TooLargeError(Exception):
    """Raised when OpenF1 returns 422 (request too large to serve at once)."""


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
    """GET an OpenF1 endpoint, retrying up to 3 times on transient errors.

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
            last_exc = e
            if attempt < _MAX_RETRIES - 1:
                # 429 needs a much longer back-off than other errors
                wait = 10 if e.response.status_code == 429 else 2**attempt
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

    raise last_exc


def raw(obj: Any) -> str:
    return json.dumps(obj, ensure_ascii=False)

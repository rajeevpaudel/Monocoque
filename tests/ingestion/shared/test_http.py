from unittest.mock import MagicMock, patch

import httpx
import pytest
import tenacity

from ingestion.shared.http import RateLimiter, _is_retryable, get_json


def _response(status_code, json_body=None):
    request = httpx.Request("GET", "https://example.test")
    resp = httpx.Response(status_code, request=request, json=json_body)
    return resp


def test_is_retryable_on_429():
    exc = httpx.HTTPStatusError("rate limited", request=None, response=_response(429))
    assert _is_retryable(exc) is True


def test_is_retryable_on_5xx():
    exc = httpx.HTTPStatusError("server error", request=None, response=_response(503))
    assert _is_retryable(exc) is True


def test_not_retryable_on_4xx_other_than_429():
    exc = httpx.HTTPStatusError("not found", request=None, response=_response(404))
    assert _is_retryable(exc) is False


def test_retryable_on_transport_error():
    assert _is_retryable(httpx.ConnectError("boom")) is True


def test_not_retryable_on_arbitrary_exception():
    """A JSON decode error or similar non-HTTP failure must not be retried —
    retrying a persistent upstream bug just wastes time."""
    assert _is_retryable(ValueError("bad json")) is False


def test_get_json_returns_parsed_body():
    mock_resp = MagicMock()
    mock_resp.json.return_value = {"ok": True}
    mock_resp.raise_for_status.return_value = None
    mock_client = MagicMock()
    mock_client.__enter__.return_value.get.return_value = mock_resp

    with (
        patch("ingestion.shared.http.httpx.Client", return_value=mock_client),
        patch("ingestion.shared.http._rate_limit") as mock_rate_limit,
    ):
        result = get_json("https://example.test/x", params={"a": 1})

    assert result == {"ok": True}
    mock_rate_limit.assert_called_once()


def test_get_json_skips_rate_limit_when_disabled():
    mock_resp = MagicMock()
    mock_resp.json.return_value = {}
    mock_client = MagicMock()
    mock_client.__enter__.return_value.get.return_value = mock_resp

    with (
        patch("ingestion.shared.http.httpx.Client", return_value=mock_client),
        patch("ingestion.shared.http._rate_limit") as mock_rate_limit,
    ):
        get_json("https://example.test/x", rate_limit=False)

    mock_rate_limit.assert_not_called()


def test_get_json_raises_after_exhausting_retries_on_persistent_5xx():
    mock_resp = MagicMock()
    mock_resp.raise_for_status.side_effect = httpx.HTTPStatusError(
        "server error", request=None, response=_response(500)
    )
    mock_client = MagicMock()
    mock_client.__enter__.return_value.get.return_value = mock_resp

    with (
        patch("ingestion.shared.http.httpx.Client", return_value=mock_client),
        patch("ingestion.shared.http._rate_limit"),
        patch("ingestion.shared.http.time.sleep"),
        pytest.raises(tenacity.RetryError),
    ):
        get_json("https://example.test/x")

    assert mock_client.__enter__.return_value.get.call_count == 6


def test_get_json_does_not_retry_on_404():
    mock_resp = MagicMock()
    mock_resp.raise_for_status.side_effect = httpx.HTTPStatusError(
        "not found", request=None, response=_response(404)
    )
    mock_client = MagicMock()
    mock_client.__enter__.return_value.get.return_value = mock_resp

    with (
        patch("ingestion.shared.http.httpx.Client", return_value=mock_client),
        patch("ingestion.shared.http._rate_limit"),
        pytest.raises(httpx.HTTPStatusError),
    ):
        get_json("https://example.test/x")

    assert mock_client.__enter__.return_value.get.call_count == 1


def test_rate_limiter_waits_for_minimum_interval():
    limiter = RateLimiter(requests_per_second=10, jitter_max=0)  # min_interval = 0.1s
    t = {"now": 0.0}

    def fake_monotonic():
        return t["now"]

    sleeps = []

    def fake_sleep(seconds):
        sleeps.append(seconds)
        t["now"] += seconds

    with (
        patch("ingestion.shared.http.time.monotonic", side_effect=fake_monotonic),
        patch("ingestion.shared.http.time.sleep", side_effect=fake_sleep),
    ):
        limiter.wait()  # first call: no prior request, no sleep needed
        t["now"] += 0.02  # only 20ms elapsed, need to wait ~80ms more
        limiter.wait()

    assert sleeps == [pytest.approx(0.08)]


def test_rate_limiter_no_wait_when_interval_already_elapsed():
    limiter = RateLimiter(requests_per_second=10, jitter_max=0)  # min_interval = 0.1s
    t = {"now": 0.0}

    with (
        patch("ingestion.shared.http.time.monotonic", side_effect=lambda: t["now"]),
        patch("ingestion.shared.http.time.sleep") as mock_sleep,
    ):
        limiter.wait()
        t["now"] += 1.0  # plenty of time has passed
        limiter.wait()

    mock_sleep.assert_not_called()

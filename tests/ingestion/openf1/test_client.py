from unittest.mock import MagicMock, patch

import httpx
import pytest

from ingestion.openf1.client import OpenF1APIError, TooLargeError, _build_url, get, raw

# ── _build_url ───────────────────────────────────────────────────────────────


def test_build_url_no_params():
    assert _build_url("https://api.openf1.org/v1/laps", {}) == "https://api.openf1.org/v1/laps"


def test_build_url_normal_params_urlencoded():
    url = _build_url("https://api.openf1.org/v1/laps", {"session_key": 9900})
    assert url == "https://api.openf1.org/v1/laps?session_key=9900"


def test_build_url_keeps_operator_chars_literal():
    """httpx would otherwise percent-encode '>' in 'date>' to '%3E', which OpenF1
    does not recognise as a comparison operator."""
    url = _build_url("https://api.openf1.org/v1/laps", {"date>": "2024-01-01T00:00:00"})
    assert url == "https://api.openf1.org/v1/laps?date>=2024-01-01T00:00:00"
    assert "%3E" not in url


def test_build_url_mixes_normal_and_operator_params():
    url = _build_url(
        "https://api.openf1.org/v1/laps",
        {"session_key": 9900, "date>": "2024-01-01T00:00:00", "date<": "2024-01-01T00:30:00"},
    )
    assert url.startswith("https://api.openf1.org/v1/laps?session_key=9900")
    assert "date>=2024-01-01T00:00:00" in url
    assert "date<=2024-01-01T00:30:00" in url


def test_build_url_only_operator_params_starts_with_question_mark():
    url = _build_url("https://api.openf1.org/v1/laps", {"date>": "2024-01-01T00:00:00"})
    assert url == "https://api.openf1.org/v1/laps?date>=2024-01-01T00:00:00"


# ── get() ──────────────────────────────────────────────────────────────────


def _client_returning(resp):
    mock_client = MagicMock()
    mock_client.__enter__.return_value.get.return_value = resp
    return mock_client


def _resp(status_code, json_body=None):
    """A real httpx.Response so status_code/json()/raise_for_status() all behave
    consistently — a MagicMock with independently-stubbed methods would let
    raise_for_status()'s exception carry a response.json() that doesn't match
    what get() itself intended to return."""
    request = httpx.Request("GET", "https://api.openf1.org/v1/x")
    return httpx.Response(status_code, request=request, json=json_body)


def test_get_returns_list_on_success():
    resp = _resp(200, [{"a": 1}])
    with patch("ingestion.openf1.client.httpx.Client", return_value=_client_returning(resp)), \
         patch("ingestion.openf1.client._rate_limit"):
        result = get("laps", {"session_key": 1})
    assert result == [{"a": 1}]


def test_get_returns_empty_list_when_body_is_not_a_list():
    resp = _resp(200, {"detail": "not a list"})
    with patch("ingestion.openf1.client.httpx.Client", return_value=_client_returning(resp)), \
         patch("ingestion.openf1.client._rate_limit"):
        result = get("laps")
    assert result == []


def test_get_returns_empty_list_on_404():
    resp = _resp(404)
    with patch("ingestion.openf1.client.httpx.Client", return_value=_client_returning(resp)), \
         patch("ingestion.openf1.client._rate_limit"):
        result = get("laps")
    assert result == []


def test_get_raises_too_large_on_422_without_retrying():
    resp = _resp(422)
    mock_client = _client_returning(resp)
    with patch("ingestion.openf1.client.httpx.Client", return_value=mock_client), \
         patch("ingestion.openf1.client._rate_limit"), pytest.raises(TooLargeError):
        get("car_data", {"session_key": 1})
    assert mock_client.__enter__.return_value.get.call_count == 1


def test_get_raises_api_error_immediately_on_401_without_retrying():
    """401 means a live session is locking the whole API — retrying is pointless."""
    resp = _resp(401, {"detail": "locked during live session"})
    mock_client = _client_returning(resp)
    with patch("ingestion.openf1.client.httpx.Client", return_value=mock_client), \
         patch("ingestion.openf1.client._rate_limit"), \
         pytest.raises(OpenF1APIError, match="locked during live session"):
        get("laps")
    assert mock_client.__enter__.return_value.get.call_count == 1


def test_get_retries_on_5xx_then_succeeds():
    fail_resp = _resp(503)
    ok_resp = _resp(200, [{"a": 1}])
    mock_client = MagicMock()
    mock_client.__enter__.return_value.get.side_effect = [fail_resp, ok_resp]
    with patch("ingestion.openf1.client.httpx.Client", return_value=mock_client), \
         patch("ingestion.openf1.client._rate_limit"), \
         patch("ingestion.openf1.client.time.sleep") as mock_sleep:
        result = get("laps")
    mock_sleep.assert_called_once()
    assert result == [{"a": 1}]
    assert mock_client.__enter__.return_value.get.call_count == 2


def test_get_raises_api_error_after_exhausting_retries():
    resp = _resp(503)
    mock_client = _client_returning(resp)
    with patch("ingestion.openf1.client.httpx.Client", return_value=mock_client), \
         patch("ingestion.openf1.client._rate_limit"), \
         patch("ingestion.openf1.client.time.sleep"), pytest.raises(OpenF1APIError):
        get("laps")
    assert mock_client.__enter__.return_value.get.call_count == 3


def test_get_retries_on_transport_error():
    mock_client = MagicMock()
    mock_client.__enter__.return_value.get.side_effect = httpx.ConnectError("boom")
    with patch("ingestion.openf1.client.httpx.Client", return_value=mock_client), \
         patch("ingestion.openf1.client._rate_limit"), \
         patch("ingestion.openf1.client.time.sleep"), pytest.raises(OpenF1APIError):
        get("laps")
    assert mock_client.__enter__.return_value.get.call_count == 3


def test_get_429_waits_longer_than_other_errors():
    resp = _resp(429)
    mock_client = _client_returning(resp)
    with patch("ingestion.openf1.client.httpx.Client", return_value=mock_client), \
         patch("ingestion.openf1.client._rate_limit"), \
         patch("ingestion.openf1.client.time.sleep") as mock_sleep, pytest.raises(OpenF1APIError):
        get("laps")
    # first retry wait for 429 must be the fixed 10s back-off, not 2**0
    assert mock_sleep.call_args_list[0].args[0] == 10


def test_raw_serializes_without_ascii_escaping():
    assert raw({"name": "Pérez"}) == '{"name": "Pérez"}'

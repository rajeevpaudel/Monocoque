import pytest
from unittest.mock import MagicMock, patch


def test_send_alert_posts_to_telegram():
    failures = [
        {"test_name": "assert_match_rate", "model": "mart_qualifying_summary",
         "season": 2024, "round": 22, "failures": 20},
    ]
    with patch("httpx.post") as mock_post:
        mock_post.return_value = MagicMock(status_code=200)
        from airflow.dags.dq_telegram import send_alert
        send_alert(failures, bot_token="test_token", chat_id="test_chat")
    mock_post.assert_called_once()
    call_args = mock_post.call_args
    assert "test_token" in call_args[0][0]
    payload = call_args[1]["json"]
    assert payload["chat_id"] == "test_chat"
    assert "🔴" in payload["text"]
    assert "Round 22" in payload["text"]


def test_send_daily_ok_posts_green_summary():
    stats = {"rounds": 24, "drivers": 480, "minutes_since_ingest": 14}
    with patch("httpx.post") as mock_post:
        mock_post.return_value = MagicMock(status_code=200)
        from airflow.dags.dq_telegram import send_daily_ok
        send_daily_ok(stats, bot_token="test_token", chat_id="test_chat")
    mock_post.assert_called_once()
    payload = mock_post.call_args[1]["json"]
    assert "✅" in payload["text"]
    assert "24 rounds" in payload["text"]


def test_send_alert_raises_on_http_error():
    with patch("httpx.post") as mock_post:
        mock_post.return_value = MagicMock(status_code=400, text="Bad Request")
        from airflow.dags.dq_telegram import send_alert
        with pytest.raises(RuntimeError, match="Telegram API error"):
            send_alert([], bot_token="bad", chat_id="bad")

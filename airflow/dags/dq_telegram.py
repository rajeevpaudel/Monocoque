import httpx


TELEGRAM_API = "https://api.telegram.org/bot{token}/sendMessage"


def send_alert(
    failures: list[dict],
    bot_token: str,
    chat_id: str,
) -> None:
    lines = ["🔴 *F1 Warehouse — Data Quality Alert*", ""]
    for f in failures:
        round_info = ""
        if f.get("season") and f.get("round"):
            round_info = f" (Round {f['round']})"
        lines.append(f"• `{f['test_name']}`{round_info}: {f.get('failures', '?')} row(s) failed")
    if not failures:
        lines.append("_No failure details available — check the elementary report._")
    text = "\n".join(lines)
    _post(bot_token, chat_id, text)


def send_daily_ok(stats: dict, bot_token: str, chat_id: str) -> None:
    text = (
        f"✅ *F1 Warehouse — All checks passed*\n"
        f"{stats.get('rounds', '?')} rounds · "
        f"{stats.get('drivers', '?')} drivers · "
        f"last ingestion {stats.get('minutes_since_ingest', '?')} min ago"
    )
    _post(bot_token, chat_id, text)


def _post(bot_token: str, chat_id: str, text: str) -> None:
    url = TELEGRAM_API.format(token=bot_token)
    response = httpx.post(url, json={"chat_id": chat_id, "text": text, "parse_mode": "Markdown"})
    if response.status_code != 200:
        raise RuntimeError(f"Telegram API error {response.status_code}: {response.text}")

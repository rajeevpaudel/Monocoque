from unittest.mock import MagicMock, patch


def _make_row(session_key=9900):
    m = MagicMock()
    m.model_dump.return_value = {
        "session_key": session_key,
        "lap_number": 1,
        "ingested_at": "2025-01-01",
        "raw_json": "{}",
    }
    return m


def test_ingest_session_skips_table_when_already_ok():
    """If log says ok/empty, we do not fetch from API or insert."""
    with patch("ingestion.openf1.backfill.log_util.needs_ingestion", return_value=False), \
         patch("ingestion.openf1.backfill.endpoints") as mock_ep, \
         patch("ingestion.openf1.backfill.ch"):
        from ingestion.openf1 import backfill
        backfill.ingest_session(9900, skip_telemetry=True)
    mock_ep.get_laps.assert_not_called()


def test_ingest_session_deletes_before_reinserting():
    """When log says needs work, delete existing rows before re-fetching."""
    with patch("ingestion.openf1.backfill.log_util.needs_ingestion", return_value=True), \
         patch("ingestion.openf1.backfill.log_util.record"), \
         patch("ingestion.openf1.backfill.endpoints") as mock_ep, \
         patch("ingestion.openf1.backfill.ch") as mock_ch:
        mock_ep.get_laps.return_value = [_make_row()]
        mock_ep.get_pit.return_value = []
        mock_ep.get_stints.return_value = []
        mock_ep.get_intervals.return_value = []
        mock_ep.get_weather.return_value = []
        mock_ep.get_race_control.return_value = []
        from ingestion.openf1 import backfill
        backfill.ingest_session(9900, skip_telemetry=True)
    mock_ch.delete_rows.assert_any_call("raw_openf1.laps", "session_key = 9900")


def test_ingest_session_records_failed_on_api_error():
    """If API raises, record 'failed' in log and return False."""
    with patch("ingestion.openf1.backfill.log_util.needs_ingestion", return_value=True), \
         patch("ingestion.openf1.backfill.log_util.record") as mock_record, \
         patch("ingestion.openf1.backfill.endpoints") as mock_ep, \
         patch("ingestion.openf1.backfill.ch"):
        mock_ep.get_laps.side_effect = RuntimeError("API timeout")
        mock_ep.get_pit.return_value = []
        mock_ep.get_stints.return_value = []
        mock_ep.get_intervals.return_value = []
        mock_ep.get_weather.return_value = []
        mock_ep.get_race_control.return_value = []
        from ingestion.openf1 import backfill
        result = backfill.ingest_session(9900, skip_telemetry=True)
    laps_calls = [c for c in mock_record.call_args_list if c[0][2] == "raw_openf1.laps"]
    assert len(laps_calls) == 1
    assert laps_calls[0][0][4] != ""  # error_msg is non-empty
    assert result is False

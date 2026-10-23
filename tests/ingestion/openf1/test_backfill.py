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


def _make_session(
    session_key=9900, session_type="Race", status="completed", date_end="2020-01-01T00:00:00Z"
):
    m = MagicMock()
    m.session_key = session_key
    m.session_type = session_type
    m.status = status
    m.date_end = date_end
    m.model_dump.return_value = {
        "session_key": session_key,
        "session_type": session_type,
        "status": status,
        "date_end": date_end,
        "year": 2020,
        "ingested_at": "2025-01-01",
        "raw_json": "{}",
    }
    return m


def test_ingest_session_skips_table_when_already_ok():
    """If log says ok/empty, we do not fetch from API or insert."""
    with (
        patch("ingestion.openf1.backfill.log_util.needs_ingestion", return_value=False),
        patch("ingestion.openf1.backfill.endpoints") as mock_ep,
        patch("ingestion.openf1.backfill.ch"),
    ):
        from ingestion.openf1 import backfill

        backfill.ingest_session(9900, skip_telemetry=True)
    mock_ep.get_laps.assert_not_called()


def test_ingest_session_deletes_before_reinserting():
    """When log says needs work, delete existing rows before re-fetching."""
    with (
        patch("ingestion.openf1.backfill.log_util.needs_ingestion", return_value=True),
        patch("ingestion.openf1.backfill.log_util.record"),
        patch("ingestion.openf1.backfill.endpoints") as mock_ep,
        patch("ingestion.openf1.backfill.ch") as mock_ch,
    ):
        mock_ep.get_drivers.return_value = []
        mock_ep.get_laps.return_value = [_make_row()]
        mock_ep.get_pit.return_value = []
        mock_ep.get_stints.return_value = []
        mock_ep.get_intervals.return_value = []
        mock_ep.get_position.return_value = []
        mock_ep.get_weather.return_value = []
        mock_ep.get_race_control.return_value = []
        from ingestion.openf1 import backfill

        backfill.ingest_session(9900, skip_telemetry=True)
    mock_ch.delete_rows.assert_any_call(
        "raw_openf1.laps", "session_key = {sk:UInt32}", parameters={"sk": 9900}
    )


def test_ingest_session_records_failed_on_api_error():
    """If API raises, record 'failed' in log and return False."""
    with (
        patch("ingestion.openf1.backfill.log_util.needs_ingestion", return_value=True),
        patch("ingestion.openf1.backfill.log_util.record") as mock_record,
        patch("ingestion.openf1.backfill.endpoints") as mock_ep,
        patch("ingestion.openf1.backfill.ch"),
    ):
        mock_ep.get_drivers.return_value = []
        mock_ep.get_laps.side_effect = RuntimeError("API timeout")
        mock_ep.get_pit.return_value = []
        mock_ep.get_stints.return_value = []
        mock_ep.get_intervals.return_value = []
        mock_ep.get_position.return_value = []
        mock_ep.get_weather.return_value = []
        mock_ep.get_race_control.return_value = []
        from ingestion.openf1 import backfill

        result = backfill.ingest_session(9900, skip_telemetry=True)
    laps_calls = [c for c in mock_record.call_args_list if c[0][2] == "raw_openf1.laps"]
    assert len(laps_calls) == 1
    assert laps_calls[0][0][4] != ""  # error_msg is non-empty
    assert result is False


def test_ingest_year_deletes_sessions_before_reinserting():
    """raw_openf1.sessions used to be appended on every ingest_year call with
    no delete-first, so re-running a year backfill duplicated every session
    row for that year. It must now delete-by-year first."""
    with (
        patch("ingestion.openf1.backfill.endpoints") as mock_ep,
        patch("ingestion.openf1.backfill.ch") as mock_ch,
        patch("ingestion.openf1.backfill.dm"),
        patch("ingestion.openf1.backfill.ingest_session"),
    ):
        mock_ep.get_sessions.return_value = [_make_session()]
        from ingestion.openf1 import backfill

        backfill.ingest_year(2020, skip_telemetry=True)

    mock_ch.delete_rows.assert_any_call(
        "raw_openf1.sessions", "year = {year:UInt16}", parameters={"year": 2020}
    )
    delete_pos = [i for i, c in enumerate(mock_ch.mock_calls) if c[0] == "delete_rows"][0]
    insert_pos = [
        i
        for i, c in enumerate(mock_ch.mock_calls)
        if c[0] == "insert_rows" and c[1][0] == "raw_openf1.sessions"
    ][0]
    assert delete_pos < insert_pos


def test_ingest_session_rejects_non_numeric_session_key():
    """session_key may arrive as an untyped string from an Airflow dag_run.conf
    (see airflow/dags/session_openf1.py) — it must be validated as numeric before
    it can reach any downstream SQL/query-parameter construction."""
    import pytest

    from ingestion.openf1 import backfill

    with pytest.raises(ValueError):
        backfill.ingest_session("9900 OR 1=1", skip_telemetry=True)


def test_ingest_session_accepts_numeric_string_session_key():
    """Airflow templates always render conf values as strings even when the
    caller passed a JSON int, so a numeric string must still work."""
    with (
        patch("ingestion.openf1.backfill.log_util.needs_ingestion", return_value=False),
        patch("ingestion.openf1.backfill.endpoints") as mock_ep,
        patch("ingestion.openf1.backfill.ch"),
    ):
        from ingestion.openf1 import backfill

        backfill.ingest_session("9900", skip_telemetry=True)
    mock_ep.get_laps.assert_not_called()


def test_ingest_year_returns_failed_session_keys():
    with (
        patch("ingestion.openf1.backfill.endpoints") as mock_ep,
        patch("ingestion.openf1.backfill.ch"),
        patch("ingestion.openf1.backfill.dm"),
        patch("ingestion.openf1.backfill.ingest_session", side_effect=lambda sk, **_: sk != 2),
    ):
        mock_ep.get_sessions.return_value = [_make_session(1), _make_session(2)]
        from ingestion.openf1 import backfill

        assert backfill.ingest_year(2020, skip_telemetry=True) == [2]

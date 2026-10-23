from unittest.mock import MagicMock, patch


def _make_race(round_num, season=2025):
    r = MagicMock()
    r.round = round_num
    r.model_dump.return_value = {
        "season": season,
        "round": round_num,
        "ingested_at": "2025-01-01",
        "raw_json": "{}",
    }
    return r


def _make_row(season=2025, round_num=1):
    r = MagicMock()
    r.model_dump.return_value = {
        "season": season,
        "round": round_num,
        "driver_id": "driver_a",
        "ingested_at": "2025-01-01",
        "raw_json": "{}",
    }
    return r


def test_ingest_season_skips_round_when_all_tables_ok():
    """Round with all tables 'ok' in log should not fetch from API."""
    with (
        patch("ingestion.jolpica.backfill.log_util.needs_ingestion", return_value=False),
        patch("ingestion.jolpica.backfill.endpoints") as mock_ep,
        patch("ingestion.jolpica.backfill.ch"),
    ):
        mock_ep.get_races.return_value = [_make_race(1)]
        from ingestion.jolpica import backfill

        backfill.ingest_season(2025)
    mock_ep.get_qualifying.assert_not_called()
    mock_ep.get_results.assert_not_called()


def test_ingest_season_retries_incomplete_round():
    """Round with 'incomplete' qualifying should re-fetch that table."""

    def needs_side_effect(source, entity_key, table):
        return table == "raw_jolpica.qualifying"

    with (
        patch("ingestion.jolpica.backfill.log_util.needs_ingestion", side_effect=needs_side_effect),
        patch("ingestion.jolpica.backfill.log_util.record"),
        patch("ingestion.jolpica.backfill.endpoints") as mock_ep,
        patch("ingestion.jolpica.backfill.ch"),
    ):
        mock_ep.get_races.return_value = [_make_race(4)]
        mock_ep.get_qualifying.return_value = [_make_row(round_num=4)]
        from ingestion.jolpica import backfill

        backfill.ingest_season(2025)
    mock_ep.get_qualifying.assert_called_once_with(2025, 4)
    mock_ep.get_results.assert_not_called()


def test_ingest_season_deletes_before_reinserting():
    """Re-ingesting a table deletes existing rows first."""
    with (
        patch("ingestion.jolpica.backfill.log_util.needs_ingestion", return_value=True),
        patch("ingestion.jolpica.backfill.log_util.record"),
        patch("ingestion.jolpica.backfill.endpoints") as mock_ep,
        patch("ingestion.jolpica.backfill.ch") as mock_ch,
    ):
        mock_ep.get_races.return_value = [_make_race(1)]
        mock_ep.get_results.return_value = [_make_row()]
        mock_ep.get_qualifying.return_value = []
        mock_ep.get_sprint.return_value = []
        mock_ep.get_laps.return_value = []
        mock_ep.get_pit_stops.return_value = []
        mock_ep.get_driver_standings.return_value = []
        mock_ep.get_constructor_standings.return_value = []
        from ingestion.jolpica import backfill

        backfill.ingest_season(2025)
    mock_ch.delete_rows.assert_any_call(
        "raw_jolpica.results",
        "season = {season:UInt16} AND round = {rnd:UInt16}",
        parameters={"season": 2025, "rnd": 1},
    )


def test_ingest_round_records_failed_and_returns_false():
    """If a table's fetch raises, ingest_round logs 'failed' and returns False
    instead of silently succeeding — this is what the weekly DAG's task result
    depends on to actually fail the Airflow task."""
    with (
        patch("ingestion.jolpica.backfill.log_util.needs_ingestion", return_value=True),
        patch("ingestion.jolpica.backfill.log_util.record") as mock_record,
        patch("ingestion.jolpica.backfill.endpoints") as mock_ep,
        patch("ingestion.jolpica.backfill.ch"),
    ):
        mock_ep.get_results.side_effect = RuntimeError("API timeout")
        mock_ep.get_qualifying.return_value = []
        mock_ep.get_sprint.return_value = []
        mock_ep.get_laps.return_value = []
        mock_ep.get_pit_stops.return_value = []
        mock_ep.get_driver_standings.return_value = []
        mock_ep.get_constructor_standings.return_value = []
        from ingestion.jolpica import backfill

        result = backfill.ingest_round(2025, 4)
    results_calls = [c for c in mock_record.call_args_list if c[0][2] == "raw_jolpica.results"]
    assert len(results_calls) == 1
    assert results_calls[0][0][4] != ""  # error_msg is non-empty
    assert result is False


def test_incremental_ingest_round_delegates_to_backfill():
    """ingestion.jolpica.incremental.ingest_round must be the same function as
    backfill.ingest_round — the weekly DAG's re-run duplication and lack of
    ingestion_log tracking was fixed by consolidating on one implementation."""
    from ingestion.jolpica import backfill, incremental

    assert incremental.ingest_round is backfill.ingest_round


def test_ingest_season_returns_failed_rounds():
    with (
        patch("ingestion.jolpica.backfill.endpoints") as mock_ep,
        patch("ingestion.jolpica.backfill.ch"),
        patch(
            "ingestion.jolpica.backfill.ingest_round", side_effect=lambda s, r, rlog=None: r != 2
        ),
    ):
        mock_ep.get_races.return_value = [_make_race(1), _make_race(2), _make_race(3)]
        from ingestion.jolpica import backfill

        assert backfill.ingest_season(2025) == [2]


def test_main_exits_non_zero_on_failed_rounds():
    import pytest

    from ingestion.jolpica import backfill

    with (
        patch("ingestion.jolpica.backfill.ingest_season", return_value=[4]),
        patch("sys.argv", ["backfill", "--start", "2025", "--end", "2025", "--skip-reference"]),
        pytest.raises(SystemExit) as exc,
    ):
        backfill.main()
    assert exc.value.code == 1

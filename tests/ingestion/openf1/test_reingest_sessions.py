from unittest.mock import MagicMock

from ingestion.openf1.reingest_sessions import resolve_target_sessions


def test_module_imports():
    """Guards against a repeat of the dead-import bug where reingest_sessions.py
    imported names (_load_checkpoint/_save_checkpoint) that only ever existed
    in ingestion.fastf1.backfill, not ingestion.openf1.backfill — the module
    failed at import time before argparse even ran."""
    import ingestion.openf1.reingest_sessions  # noqa: F401


def _session(session_key, session_type, meeting_key, date_start, location="Monza"):
    s = MagicMock()
    s.session_key = session_key
    s.session_type = session_type
    s.meeting_key = meeting_key
    s.date_start = date_start
    s.location = location
    return s


def test_resolve_target_sessions_maps_round_ordinal_by_race_date():
    """Round numbers are 1-based ordinals over Race sessions sorted by date,
    not raw meeting_key or session_key order."""
    sessions = [
        _session(1, "Race", meeting_key=200, date_start="2024-03-01"),  # round 1
        _session(2, "Qualifying", meeting_key=200, date_start="2024-02-29"),
        _session(3, "Race", meeting_key=100, date_start="2024-04-01"),  # round 2
        _session(4, "Qualifying", meeting_key=100, date_start="2024-03-30"),
    ]
    result = resolve_target_sessions(sessions, [1, 2])
    assert [s.session_key for s in result] == [2, 4]


def test_resolve_target_sessions_skips_sprint_qualifying_and_sprint_race():
    """A sprint weekend has two Race and two Qualifying sessions per meeting;
    the LATER one of each is always the grid-setting main session."""
    sessions = [
        _session(1, "Race", meeting_key=1, date_start="2024-03-02"),        # sprint race (earlier)
        _session(2, "Race", meeting_key=1, date_start="2024-03-03"),        # main race (later) -> round 1
        _session(3, "Qualifying", meeting_key=1, date_start="2024-03-01"),  # sprint qualifying (earlier)
        _session(4, "Qualifying", meeting_key=1, date_start="2024-03-02T12:00:00"),  # main qualifying (later)
    ]
    result = resolve_target_sessions(sessions, [1])
    assert [s.session_key for s in result] == [4]


def test_resolve_target_sessions_skips_unknown_round():
    sessions = [
        _session(1, "Race", meeting_key=1, date_start="2024-03-01"),
        _session(2, "Qualifying", meeting_key=1, date_start="2024-02-29"),
    ]
    result = resolve_target_sessions(sessions, [1, 99])
    assert [s.session_key for s in result] == [2]


def test_resolve_target_sessions_skips_round_with_no_qualifying_session():
    sessions = [
        _session(1, "Race", meeting_key=1, date_start="2024-03-01"),
        # no Qualifying session for meeting_key=1
    ]
    result = resolve_target_sessions(sessions, [1])
    assert result == []


def test_resolve_target_sessions_returns_empty_for_no_sessions():
    assert resolve_target_sessions([], [1, 2, 3]) == []

import json
import os
import tempfile
from unittest.mock import patch

from ingestion.fastf1.backfill import (
    _load_checkpoint,
    _save_checkpoint,
    _resolve_session_key,
)


def test_checkpoint_roundtrip():
    with tempfile.NamedTemporaryFile(suffix=".json", delete=False, mode="w") as f:
        f.write("[]")
        path = f.name
    try:
        with patch("ingestion.fastf1.backfill._CHECKPOINT_FILE", path):
            completed = {"2024-1-Q", "2024-1-R", "2024-2-FP1"}
            _save_checkpoint(completed)
            loaded = _load_checkpoint()
        assert loaded == completed
    finally:
        os.unlink(path)


def test_load_checkpoint_returns_empty_when_missing():
    with patch("ingestion.fastf1.backfill._CHECKPOINT_FILE", "/tmp/__ff1_no_such_file__.json"):
        result = _load_checkpoint()
    assert result == set()


def test_resolve_session_key_qualifying():
    index = {(5, "Qualifying"): 9501, (5, "Race"): 9502}
    assert _resolve_session_key(index, 5, "Q") == 9501


def test_resolve_session_key_practice_numbered():
    index = {(3, "Practice 1"): 9301, (3, "Practice 2"): 9302}
    assert _resolve_session_key(index, 3, "FP1") == 9301
    assert _resolve_session_key(index, 3, "FP2") == 9302


def test_resolve_session_key_practice_unnumbered_fallback():
    # Older OpenF1 seasons use 'Practice' without a number
    index = {(3, "Practice"): 9301}
    assert _resolve_session_key(index, 3, "FP1") == 9301


def test_resolve_session_key_sprint_qualifying_variants():
    index = {(7, "Sprint Shootout"): 9701}
    assert _resolve_session_key(index, 7, "SQ") == 9701


def test_resolve_session_key_returns_none_for_missing():
    index = {(5, "Race"): 9501}
    assert _resolve_session_key(index, 5, "FP3") is None


def test_build_session_index_deduplicates_sprint_race():
    """Sprint weekends have two Race sessions per meeting; only the later one counts."""
    from unittest.mock import MagicMock

    def _make_session(session_key, session_type, meeting_key, date_start):
        s = MagicMock()
        s.session_key = session_key
        s.session_type = session_type
        s.meeting_key = meeting_key
        s.date_start = date_start
        return s

    sessions = [
        # Meeting 1: normal weekend — one Race, one Qualifying
        _make_session(101, "Race",       1, "2024-03-02T15:00:00"),
        _make_session(102, "Qualifying", 1, "2024-03-01T15:00:00"),
        # Meeting 2: sprint weekend — two Race sessions, take the later one
        _make_session(201, "Race",       2, "2024-03-09T13:00:00"),  # sprint race
        _make_session(202, "Race",       2, "2024-03-10T15:00:00"),  # main race (later → round 2)
        _make_session(203, "Qualifying", 2, "2024-03-09T15:00:00"),
    ]

    with patch("ingestion.fastf1.backfill.openf1_endpoints.get_sessions", return_value=sessions):
        from ingestion.fastf1.backfill import _build_session_index
        index, rounds = _build_session_index(2024)

    assert rounds == [1, 2]
    # Round 1: Qualifying maps to session 102
    assert index[(1, "Qualifying")] == 102
    # Round 1: Race maps to session 101
    assert index[(1, "Race")] == 101
    # Round 2: Race maps to session 202 (the later one, not 201)
    assert index[(2, "Race")] == 202
    # Round 2: Qualifying maps to session 203
    assert index[(2, "Qualifying")] == 203

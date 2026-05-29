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

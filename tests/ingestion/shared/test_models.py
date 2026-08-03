from datetime import UTC, datetime

import pytest
from pydantic import ValidationError

from ingestion.shared.models import JolpicaSeason, OpenF1Interval, RawRecord, model_to_dict


def test_raw_record_requires_raw_json_alias():
    rec = RawRecord(_raw_json="{}")
    assert rec.raw_json == "{}"


def test_raw_record_auto_sets_ingested_at():
    before = datetime.now(UTC)
    rec = RawRecord(_raw_json="{}")
    after = datetime.now(UTC)
    assert before <= rec.ingested_at <= after


def test_raw_record_accepts_explicit_ingested_at_via_alias():
    ts = datetime(2020, 1, 1, tzinfo=UTC)
    rec = RawRecord(_ingested_at=ts, _raw_json="{}")
    assert rec.ingested_at == ts


def test_raw_record_populate_by_name_allows_field_names_too():
    """model_config populate_by_name=True means callers can pass either the
    alias (_raw_json) or the field name (raw_json)."""
    rec = RawRecord(raw_json="{}")
    assert rec.raw_json == "{}"


def test_raw_record_missing_raw_json_raises():
    with pytest.raises(ValidationError):
        RawRecord()


def test_jolpica_season_subclass_inherits_raw_record_fields():
    rec = JolpicaSeason(year=2024, url="https://x", _raw_json="{}")
    assert rec.year == 2024
    assert rec.raw_json == "{}"
    assert rec.ingested_at is not None


def test_openf1_interval_coerces_numeric_string():
    rec = OpenF1Interval(
        session_key=1, driver_number=1, date="2024-01-01",
        gap_to_leader="1.234", interval="0.5", _raw_json="{}",
    )
    assert rec.gap_to_leader == 1.234
    assert rec.interval == 0.5


def test_openf1_interval_coerces_lapped_marker_to_none():
    """OpenF1 marks lapped cars with a string like '+1 LAP' instead of a float —
    that must become None, not raise a validation error."""
    rec = OpenF1Interval(
        session_key=1, driver_number=1, date="2024-01-01",
        gap_to_leader="+1 LAP", interval="+2 LAPS", _raw_json="{}",
    )
    assert rec.gap_to_leader is None
    assert rec.interval is None


def test_openf1_interval_passes_through_numeric_types():
    rec = OpenF1Interval(
        session_key=1, driver_number=1, date="2024-01-01",
        gap_to_leader=1.5, interval=None, _raw_json="{}",
    )
    assert rec.gap_to_leader == 1.5
    assert rec.interval is None


def test_model_to_dict_renames_alias_fields():
    season = JolpicaSeason(year=2024, url="https://example.test", _raw_json="{}")
    d = model_to_dict(season)
    assert d["year"] == 2024
    assert "_ingested_at" in d and "ingested_at" not in d
    assert d["_raw_json"] == "{}" and "raw_json" not in d

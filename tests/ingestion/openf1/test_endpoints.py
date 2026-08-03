from unittest.mock import patch

from ingestion.openf1 import endpoints as ep
from ingestion.openf1.client import TooLargeError


def _patched_get(records):
    return patch("ingestion.openf1.endpoints.get", return_value=records)


# ── _time_chunks ─────────────────────────────────────────────────────────────


def test_time_chunks_splits_into_30_minute_windows():
    chunks = list(ep._time_chunks("2024-01-01T00:00:00Z", "2024-01-01T01:05:00Z"))
    assert chunks == [
        ("2024-01-01T00:00:00", "2024-01-01T00:30:00"),
        ("2024-01-01T00:30:00", "2024-01-01T01:00:00"),
        ("2024-01-01T01:00:00", "2024-01-01T01:05:00"),
    ]


def test_time_chunks_handles_sub_window_range():
    chunks = list(ep._time_chunks("2024-01-01T00:00:00Z", "2024-01-01T00:10:00Z"))
    assert chunks == [("2024-01-01T00:00:00", "2024-01-01T00:10:00")]


def test_time_chunks_empty_when_start_equals_end():
    assert list(ep._time_chunks("2024-01-01T00:00:00Z", "2024-01-01T00:00:00Z")) == []


def test_time_chunks_handles_timezone_offset_suffix():
    chunks = list(ep._time_chunks("2024-01-01T00:00:00+00:00", "2024-01-01T00:20:00+00:00"))
    assert chunks == [("2024-01-01T00:00:00", "2024-01-01T00:20:00")]


# ── get_sessions ─────────────────────────────────────────────────────────────


def test_get_sessions_maps_all_fields():
    record = {
        "session_key": 9900, "session_name": "Qualifying", "session_type": "Qualifying",
        "status": "completed", "gmt_offset": "05:00:00", "path": "2024/x/", "date_start": "2024-01-01",
        "date_end": "2024-01-01", "year": 2024, "circuit_key": 63, "circuit_short_name": "Jeddah",
        "country_key": 153, "country_code": "SAU", "country_name": "Saudi Arabia",
        "location": "Jeddah", "meeting_key": 1141,
    }
    with _patched_get([record]):
        out = ep.get_sessions(2024)
    assert out[0].session_key == 9900
    assert out[0].circuit_key == 63


# ── get_drivers ──────────────────────────────────────────────────────────────


def test_get_drivers_defaults_none_fields_to_empty_string():
    record = {"session_key": 9900, "driver_number": 1}
    with _patched_get([record]):
        out = ep.get_drivers(9900)
    d = out[0]
    assert d.broadcast_name == ""
    assert d.name_acronym == ""
    assert d.team_colour == ""
    assert d.headshot_url is None


def test_get_drivers_populates_provided_fields():
    record = {
        "session_key": 9900, "driver_number": 1, "broadcast_name": "M VERSTAPPEN",
        "full_name": "Max Verstappen", "name_acronym": "VER", "team_name": "Red Bull",
        "team_colour": "3671C6", "first_name": "Max", "last_name": "Verstappen",
    }
    with _patched_get([record]):
        out = ep.get_drivers(9900)
    assert out[0].name_acronym == "VER"


# ── get_laps ─────────────────────────────────────────────────────────────────


def test_get_laps_skips_rows_without_date_start():
    """A lap row with no date_start can't be anchored to a wall-clock time and
    is unusable downstream — it must be dropped, not passed through with a null."""
    records = [
        {"session_key": 1, "driver_number": 1, "lap_number": 1, "date_start": None},
        {"session_key": 1, "driver_number": 1, "lap_number": 2, "date_start": "2024-01-01T00:00:00"},
    ]
    with _patched_get(records):
        out = ep.get_laps(1)
    assert len(out) == 1
    assert out[0].lap_number == 2


def test_get_laps_filters_none_from_segment_lists():
    record = {
        "session_key": 1, "driver_number": 1, "lap_number": 1, "date_start": "2024-01-01T00:00:00",
        "segments_sector_1": [2048, None, 2049], "segments_sector_2": None,
    }
    with _patched_get([record]):
        out = ep.get_laps(1)
    assert out[0].segments_sector_1 == [2048, 2049]
    assert out[0].segments_sector_2 == []
    assert out[0].segments_sector_3 == []


# ── get_pit ──────────────────────────────────────────────────────────────────


def test_get_pit():
    record = {"session_key": 1, "driver_number": 1, "lap_number": 15, "date": "2024-01-01", "pit_duration": 2.3}
    with _patched_get([record]):
        out = ep.get_pit(1)
    assert out[0].pit_duration == 2.3


# ── get_stints ───────────────────────────────────────────────────────────────


def test_get_stints_skips_rows_without_lap_start():
    records = [
        {"session_key": 1, "driver_number": 1, "stint_number": 1, "lap_start": None, "compound": "SOFT"},
        {"session_key": 1, "driver_number": 1, "stint_number": 2, "lap_start": 10, "compound": "MEDIUM"},
    ]
    with _patched_get(records):
        out = ep.get_stints(1)
    assert len(out) == 1
    assert out[0].compound == "MEDIUM"


def test_get_stints_defaults_missing_compound_and_tyre_age():
    record = {"session_key": 1, "driver_number": 1, "stint_number": 1, "lap_start": 1}
    with _patched_get([record]):
        out = ep.get_stints(1)
    assert out[0].compound == ""
    assert out[0].tyre_age_at_start == 0


# ── get_intervals / get_weather / get_race_control ──────────────────────────


def test_get_intervals():
    record = {"session_key": 1, "driver_number": 1, "date": "2024-01-01", "gap_to_leader": 1.5, "interval": 0.3}
    with _patched_get([record]):
        out = ep.get_intervals(1)
    assert out[0].gap_to_leader == 1.5


def test_get_weather_defaults_missing_numeric_fields():
    record = {"session_key": 1, "date": "2024-01-01"}
    with _patched_get([record]):
        out = ep.get_weather(1)
    w = out[0]
    assert w.air_temperature == 0.0
    assert w.rainfall is False


def test_get_race_control_defaults_optional_fields():
    record = {"session_key": 1, "date": "2024-01-01"}
    with _patched_get([record]):
        out = ep.get_race_control(1)
    rc = out[0]
    assert rc.driver_number is None
    assert rc.category == ""
    assert rc.message == ""


# ── get_car_data / get_location: per-driver + 422 chunked fallback ──────────


def _driver(number=1):
    from ingestion.shared.models import OpenF1Driver
    return OpenF1Driver(session_key=1, driver_number=number, broadcast_name="", full_name="",
                         name_acronym="", team_name="", team_colour="", first_name="", last_name="",
                         _raw_json="{}")


def test_get_car_data_fetches_per_driver():
    with patch("ingestion.openf1.endpoints.get_drivers", return_value=[_driver(1), _driver(44)]), \
         patch("ingestion.openf1.endpoints.get") as mock_get:
        mock_get.return_value = [
            {"session_key": 1, "driver_number": 1, "date": "2024-01-01", "rpm": 1000, "speed": 200,
             "n_gear": 4, "throttle": 100, "brake": 0, "drs": 0}
        ]
        out = ep.get_car_data(1)
    assert mock_get.call_count == 2
    assert len(out) == 2  # one record per driver call


def test_get_car_data_falls_back_to_chunked_fetch_on_422():
    with patch("ingestion.openf1.endpoints.get_drivers", return_value=[_driver(1)]), \
         patch("ingestion.openf1.endpoints._session_dates", return_value=("2024-01-01T00:00:00", "2024-01-01T00:30:00")), \
         patch("ingestion.openf1.endpoints.get") as mock_get:
        mock_get.side_effect = [
            TooLargeError("too big"),
            [{"session_key": 1, "driver_number": 1, "date": "2024-01-01", "rpm": 1, "speed": 1,
              "n_gear": 1, "throttle": 1, "brake": 0, "drs": 0}],
        ]
        out = ep.get_car_data(1)
    assert len(out) == 1
    assert mock_get.call_count == 2


def test_get_location_falls_back_to_chunked_fetch_on_422():
    with patch("ingestion.openf1.endpoints.get_drivers", return_value=[_driver(1)]), \
         patch("ingestion.openf1.endpoints._session_dates", return_value=("2024-01-01T00:00:00", "2024-01-01T00:30:00")), \
         patch("ingestion.openf1.endpoints.get") as mock_get:
        mock_get.side_effect = [
            TooLargeError("too big"),
            [{"session_key": 1, "driver_number": 1, "date": "2024-01-01", "x": 1, "y": 2, "z": 3}],
        ]
        out = ep.get_location(1)
    assert len(out) == 1
    assert out[0].x == 1


def test_get_car_data_no_drivers_returns_empty_without_fetching():
    with patch("ingestion.openf1.endpoints.get_drivers", return_value=[]), \
         patch("ingestion.openf1.endpoints.get") as mock_get:
        out = ep.get_car_data(1)
    assert out == []
    mock_get.assert_not_called()


# ── _session_dates ───────────────────────────────────────────────────────────


def test_session_dates_raises_when_session_not_found():
    with patch("ingestion.openf1.endpoints.get", return_value=[]):
        try:
            ep._session_dates(9999)
            raised = False
        except ValueError:
            raised = True
    assert raised


def test_session_dates_falls_back_to_date_start_when_no_date_end():
    with patch("ingestion.openf1.endpoints.get", return_value=[{"date_start": "2024-01-01T00:00:00", "date_end": None}]):
        start, end = ep._session_dates(9900)
    assert start == end == "2024-01-01T00:00:00"

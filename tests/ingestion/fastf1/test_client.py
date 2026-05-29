import numpy as np
import pandas as pd
import pytest

from ingestion.fastf1.client import _process_lap_telemetry


def _make_tel(distances):
    dates = pd.date_range("2024-03-02 14:00:00", periods=len(distances), freq="270ms", tz="UTC")
    return pd.DataFrame({"Date": dates, "Distance": distances})


def test_returns_required_columns():
    result = _process_lap_telemetry(_make_tel([0.0, 10.5, 21.0]), session_key=9001, driver_number=1)
    assert list(result.columns) == ["session_key", "driver_number", "date", "distance_m"]


def test_filters_negative_distances():
    result = _process_lap_telemetry(_make_tel([-1.0, 0.0, 10.5, 21.0]), session_key=9001, driver_number=1)
    assert len(result) == 3
    assert (result["distance_m"] >= 0).all()


def test_filters_nan_distances():
    result = _process_lap_telemetry(_make_tel([0.0, np.nan, 21.0]), session_key=9001, driver_number=1)
    assert len(result) == 2


def test_stamps_session_key_and_driver():
    result = _process_lap_telemetry(_make_tel([0.0, 10.5]), session_key=9001, driver_number=44)
    assert (result["session_key"] == 9001).all()
    assert (result["driver_number"] == 44).all()


def test_empty_input_returns_empty_dataframe():
    result = _process_lap_telemetry(_make_tel([]), session_key=9001, driver_number=1)
    assert result.empty
    assert list(result.columns) == ["session_key", "driver_number", "date", "distance_m"]

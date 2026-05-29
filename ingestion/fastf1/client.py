"""FastF1 telemetry client — fetches Distance channel for any session type."""

import os

import fastf1
import pandas as pd
import pyarrow as pa
import structlog

log = structlog.get_logger()

_CACHE_DIR = os.environ.get("FASTF1_CACHE_DIR", "/tmp/fastf1_cache")
os.makedirs(_CACHE_DIR, exist_ok=True)
fastf1.Cache.enable_cache(_CACHE_DIR)

_EMPTY_SCHEMA = pa.schema([
    pa.field("session_key",   pa.int32()),
    pa.field("driver_number", pa.uint8()),
    pa.field("date",          pa.timestamp("ms", tz="UTC")),
    pa.field("distance_m",    pa.float32()),
])


def _process_lap_telemetry(
    tel_df: pd.DataFrame,
    session_key: int,
    driver_number: int,
) -> pd.DataFrame:
    """Filter and reshape one lap's telemetry to storage columns.

    Returns a DataFrame with columns [session_key, driver_number, date, distance_m].
    Filters out NaN and negative Distance values (FastF1 artefacts at lap boundaries).
    """
    tel = tel_df[["Date", "Distance"]].copy()
    tel = tel.dropna(subset=["Distance"])
    tel = tel[tel["Distance"] >= 0].copy()
    if tel.empty:
        return pd.DataFrame(columns=["session_key", "driver_number", "date", "distance_m"])
    tel["session_key"] = session_key
    tel["driver_number"] = driver_number
    return tel.rename(columns={"Date": "date", "Distance": "distance_m"})[
        ["session_key", "driver_number", "date", "distance_m"]
    ]


def get_session_telemetry(
    session_key: int,
    year: int,
    round_num: int,
    identifier: str,
) -> pa.Table:
    """Load distance telemetry for every driver in one session.

    Args:
        session_key: OpenF1 session_key (stored as-is for mart joins).
        year: F1 season year.
        round_num: Jolpica 1-based round number.
        identifier: FastF1 session code — 'FP1', 'FP2', 'FP3', 'Q', 'SQ', 'S', 'R'.

    Returns:
        PyArrow Table with schema (session_key, driver_number, date, distance_m).
        Empty table (zero rows, correct schema) if no telemetry is available.

    Raises:
        fastf1.core.SessionNotAvailableError: session does not exist in FastF1.
    """
    session = fastf1.get_session(year, round_num, identifier)
    session.load(laps=True, telemetry=True)

    frames: list[pd.DataFrame] = []
    for driver_str in session.drivers:
        driver_number = int(driver_str)
        driver_laps = session.laps.pick_drivers(driver_number)
        for _, lap in driver_laps.iterrows():
            try:
                tel = lap.get_telemetry()
            except Exception as exc:
                log.warning("lap_telemetry_load_failed", driver=driver_number, error=str(exc))
                continue
            if tel.empty or "Distance" not in tel.columns or "Date" not in tel.columns:
                continue
            processed = _process_lap_telemetry(tel, session_key, driver_number)
            if not processed.empty:
                frames.append(processed)

    if not frames:
        return pa.table({f.name: pa.array([], type=f.type) for f in _EMPTY_SCHEMA})

    combined = pd.concat(frames, ignore_index=True)
    combined["session_key"] = combined["session_key"].astype("int32")
    combined["driver_number"] = combined["driver_number"].astype("uint8")
    combined["distance_m"] = combined["distance_m"].astype("float32")
    return pa.Table.from_pandas(combined, schema=_EMPTY_SCHEMA, preserve_index=False)

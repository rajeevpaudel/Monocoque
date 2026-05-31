"""One function per OpenF1 endpoint. Each returns typed Pydantic models."""

from datetime import datetime, timedelta

from ingestion.openf1.client import TooLargeError, get, raw
from ingestion.shared.models import (
    OpenF1CarData,
    OpenF1Driver,
    OpenF1Interval,
    OpenF1Lap,
    OpenF1Location,
    OpenF1Pit,
    OpenF1RaceControl,
    OpenF1Session,
    OpenF1Stint,
    OpenF1Weather,
)

_CHUNK_MINUTES = 30


def _session_dates(session_key: int) -> tuple[str, str]:
    records = get("sessions", {"session_key": session_key})
    if not records:
        raise ValueError(f"session not found: {session_key}")
    r = records[0]
    return r["date_start"], r.get("date_end") or r["date_start"]


def _time_chunks(date_start: str, date_end: str):
    """Yield (start_iso, end_iso) pairs in 30-min windows covering the range."""

    def _parse(s: str) -> datetime:
        return datetime.fromisoformat(s.rstrip("Z").split("+")[0])

    start, end = _parse(date_start), _parse(date_end)
    delta = timedelta(minutes=_CHUNK_MINUTES)
    cur = start
    while cur < end:
        nxt = min(cur + delta, end)
        yield cur.strftime("%Y-%m-%dT%H:%M:%S"), nxt.strftime("%Y-%m-%dT%H:%M:%S")
        cur = nxt


def _fetch_chunked(endpoint: str, base_params: dict, date_start: str, date_end: str) -> list[dict]:
    """Fetch telemetry in 30-min windows, used as fallback after a 422."""
    out = []
    for cs, ce in _time_chunks(date_start, date_end):
        out.extend(get(endpoint, {**base_params, "date>": cs, "date<": ce}))
    return out


def get_sessions(year: int) -> list[OpenF1Session]:
    records = get("sessions", {"year": year})
    out = []
    for r in records:
        out.append(
            OpenF1Session(
                session_key=r["session_key"],
                session_name=r["session_name"],
                session_type=r["session_type"],
                status=r.get("status", ""),
                gmt_offset=r.get("gmt_offset", ""),
                path=r.get("path", ""),
                date_start=r["date_start"],
                date_end=r.get("date_end"),
                year=r["year"],
                circuit_key=r["circuit_key"],
                circuit_short_name=r["circuit_short_name"],
                country_key=r["country_key"],
                country_code=r["country_code"],
                country_name=r["country_name"],
                location=r["location"],
                meeting_key=r["meeting_key"],
                _raw_json=raw(r),
            )
        )
    return out


def get_drivers(session_key: int) -> list[OpenF1Driver]:
    records = get("drivers", {"session_key": session_key})
    out = []
    for r in records:
        out.append(
            OpenF1Driver(
                session_key=r["session_key"],
                driver_number=r["driver_number"],
                broadcast_name=r.get("broadcast_name") or "",
                full_name=r.get("full_name") or "",
                name_acronym=r.get("name_acronym") or "",
                team_name=r.get("team_name") or "",
                team_colour=r.get("team_colour") or "",
                first_name=r.get("first_name") or "",
                last_name=r.get("last_name") or "",
                headshot_url=r.get("headshot_url"),
                country_code=r.get("country_code"),
                _raw_json=raw(r),
            )
        )
    return out


def get_laps(session_key: int) -> list[OpenF1Lap]:
    records = get("laps", {"session_key": session_key})
    out = []
    for r in records:
        if not r.get("date_start"):
            continue  # row is unusable without a start timestamp
        out.append(
            OpenF1Lap(
                session_key=r["session_key"],
                driver_number=r["driver_number"],
                lap_number=r["lap_number"],
                date_start=r["date_start"],
                lap_duration=r.get("lap_duration"),
                is_pit_out_lap=bool(r.get("is_pit_out_lap", False)),
                duration_sector_1=r.get("duration_sector_1"),
                duration_sector_2=r.get("duration_sector_2"),
                duration_sector_3=r.get("duration_sector_3"),
                i1_speed=r.get("i1_speed"),
                i2_speed=r.get("i2_speed"),
                st_speed=r.get("st_speed"),
                segments_sector_1=[x for x in (r.get("segments_sector_1") or []) if x is not None],
                segments_sector_2=[x for x in (r.get("segments_sector_2") or []) if x is not None],
                segments_sector_3=[x for x in (r.get("segments_sector_3") or []) if x is not None],
                _raw_json=raw(r),
            )
        )
    return out


def get_pit(session_key: int) -> list[OpenF1Pit]:
    records = get("pit", {"session_key": session_key})
    return [
        OpenF1Pit(
            session_key=r["session_key"],
            driver_number=r["driver_number"],
            lap_number=r["lap_number"],
            date=r["date"],
            pit_duration=r.get("pit_duration"),
            _raw_json=raw(r),
        )
        for r in records
    ]


def get_stints(session_key: int) -> list[OpenF1Stint]:
    records = get("stints", {"session_key": session_key})
    out = []
    for r in records:
        if r.get("lap_start") is None:
            continue  # row is unusable without a starting lap
        out.append(
            OpenF1Stint(
                session_key=r["session_key"],
                driver_number=r["driver_number"],
                stint_number=r["stint_number"],
                lap_start=r["lap_start"],
                lap_end=r.get("lap_end"),
                compound=r.get("compound", ""),
                tyre_age_at_start=r.get("tyre_age_at_start", 0),
                _raw_json=raw(r),
            )
        )
    return out


def get_intervals(session_key: int) -> list[OpenF1Interval]:
    records = get("intervals", {"session_key": session_key})
    return [
        OpenF1Interval(
            session_key=r["session_key"],
            driver_number=r["driver_number"],
            date=r["date"],
            gap_to_leader=r.get("gap_to_leader"),
            interval=r.get("interval"),
            _raw_json=raw(r),
        )
        for r in records
    ]


def get_weather(session_key: int) -> list[OpenF1Weather]:
    records = get("weather", {"session_key": session_key})
    return [
        OpenF1Weather(
            session_key=r["session_key"],
            date=r["date"],
            air_temperature=r.get("air_temperature", 0.0),
            track_temperature=r.get("track_temperature", 0.0),
            humidity=r.get("humidity", 0.0),
            pressure=r.get("pressure", 0.0),
            wind_direction=r.get("wind_direction", 0),
            wind_speed=r.get("wind_speed", 0.0),
            rainfall=bool(r.get("rainfall", False)),
            _raw_json=raw(r),
        )
        for r in records
    ]


def get_race_control(session_key: int) -> list[OpenF1RaceControl]:
    records = get("race_control", {"session_key": session_key})
    return [
        OpenF1RaceControl(
            session_key=r["session_key"],
            date=r["date"],
            driver_number=r.get("driver_number"),
            lap_number=r.get("lap_number"),
            category=r.get("category", ""),
            flag=r.get("flag"),
            scope=r.get("scope"),
            sector=r.get("sector"),
            message=r.get("message", ""),
            _raw_json=raw(r),
        )
        for r in records
    ]


def get_car_data(session_key: int) -> list[OpenF1CarData]:
    """Fetches per-driver; falls back to 30-min time chunks if still too large."""
    drivers = get_drivers(session_key)
    out = []
    date_start = date_end = None  # lazily fetched on first 422
    for driver in drivers:
        params = {"session_key": session_key, "driver_number": driver.driver_number}
        try:
            records = get("car_data", params)
        except TooLargeError:
            if date_start is None:
                date_start, date_end = _session_dates(session_key)
            records = _fetch_chunked("car_data", params, date_start, date_end)
        for r in records:
            out.append(
                OpenF1CarData(
                    session_key=r["session_key"],
                    driver_number=r["driver_number"],
                    date=r["date"],
                    rpm=r.get("rpm", 0),
                    speed=r.get("speed", 0),
                    n_gear=r.get("n_gear", 0),
                    throttle=r.get("throttle", 0),
                    brake=r.get("brake", 0),
                    drs=r.get("drs", 0),
                    _raw_json=raw(r),
                )
            )
    return out


def get_location(session_key: int) -> list[OpenF1Location]:
    """Fetches per-driver; falls back to 30-min time chunks if still too large."""
    drivers = get_drivers(session_key)
    out = []
    date_start = date_end = None
    for driver in drivers:
        params = {"session_key": session_key, "driver_number": driver.driver_number}
        try:
            records = get("location", params)
        except TooLargeError:
            if date_start is None:
                date_start, date_end = _session_dates(session_key)
            records = _fetch_chunked("location", params, date_start, date_end)
        for r in records:
            out.append(
                OpenF1Location(
                    session_key=r["session_key"],
                    driver_number=r["driver_number"],
                    date=r["date"],
                    x=r.get("x", 0),
                    y=r.get("y", 0),
                    z=r.get("z", 0),
                    _raw_json=raw(r),
                )
            )
    return out

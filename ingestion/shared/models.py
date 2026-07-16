"""Pydantic v2 base models for raw ingestion records."""

from datetime import UTC, datetime

from pydantic import BaseModel, Field, field_validator


class RawRecord(BaseModel):
    """Base for every raw entity — auto-sets _ingested_at and requires _raw_json."""

    model_config = {"populate_by_name": True}

    ingested_at: datetime = Field(
        default_factory=lambda: datetime.now(UTC),
        alias="_ingested_at",
    )
    raw_json: str = Field(alias="_raw_json")


# ── Jolpica ──────────────────────────────────────────────────────────────────


class JolpicaSeason(RawRecord):
    year: int
    url: str


class JolpicaCircuit(RawRecord):
    circuit_id: str
    url: str
    circuit_name: str
    locality: str
    country: str
    lat: float
    lng: float
    alt: float | None = None
    length_km: float | None = None
    corners: int | None = None


class JolpicaDriver(RawRecord):
    driver_id: str
    url: str
    given_name: str
    family_name: str
    date_of_birth: str | None = None
    nationality: str
    permanent_number: int | None = None
    code: str | None = None


class JolpicaConstructor(RawRecord):
    constructor_id: str
    url: str
    name: str
    nationality: str


class JolpicaRace(RawRecord):
    season: int
    round: int
    race_name: str
    circuit_id: str
    date: str
    time: str | None = None
    url: str


class JolpicaResult(RawRecord):
    season: int
    round: int
    driver_id: str
    constructor_id: str
    grid: int
    position: int | None = None
    position_text: str
    position_order: int
    points: float
    laps: int
    status: str
    time_millis: int | None = None
    fastest_lap_rank: int | None = None
    fastest_lap_time: str | None = None
    fastest_lap_speed: float | None = None


class JolpicaQualifying(RawRecord):
    season: int
    round: int
    driver_id: str
    constructor_id: str
    position: int
    q1: str | None = None
    q2: str | None = None
    q3: str | None = None


class JolpicaSprintResult(RawRecord):
    season: int
    round: int
    driver_id: str
    constructor_id: str
    grid: int
    position: int | None = None
    position_text: str
    position_order: int
    points: float
    laps: int
    status: str
    time_millis: int | None = None


class JolpicaLapTime(RawRecord):
    season: int
    round: int
    driver_id: str
    lap: int
    position: int
    time: str
    time_millis: int


class JolpicaPitStop(RawRecord):
    season: int
    round: int
    driver_id: str
    stop: int
    lap: int
    time: str
    duration: str
    duration_millis: int | None = None


class JolpicaDriverStanding(RawRecord):
    season: int
    round: int
    driver_id: str
    constructor_id: str
    position: int
    position_text: str
    points: float
    wins: int


class JolpicaConstructorStanding(RawRecord):
    season: int
    round: int
    constructor_id: str
    position: int
    position_text: str
    points: float
    wins: int


# ── OpenF1 ───────────────────────────────────────────────────────────────────


class OpenF1Session(RawRecord):
    session_key: int
    session_name: str
    session_type: str
    status: str
    gmt_offset: str
    path: str
    date_start: str
    date_end: str | None = None
    year: int
    circuit_key: int
    circuit_short_name: str
    country_key: int
    country_code: str
    country_name: str
    location: str
    meeting_key: int


class OpenF1Driver(RawRecord):
    session_key: int
    driver_number: int
    broadcast_name: str
    full_name: str
    name_acronym: str
    team_name: str
    team_colour: str
    first_name: str
    last_name: str
    headshot_url: str | None = None
    country_code: str | None = None


class OpenF1Lap(RawRecord):
    session_key: int
    driver_number: int
    lap_number: int
    date_start: str
    lap_duration: float | None = None
    is_pit_out_lap: bool
    duration_sector_1: float | None = None
    duration_sector_2: float | None = None
    duration_sector_3: float | None = None
    i1_speed: int | None = None
    i2_speed: int | None = None
    st_speed: int | None = None
    segments_sector_1: list[int] = []
    segments_sector_2: list[int] = []
    segments_sector_3: list[int] = []


class OpenF1Pit(RawRecord):
    session_key: int
    driver_number: int
    lap_number: int
    date: str
    pit_duration: float | None = None


class OpenF1Stint(RawRecord):
    session_key: int
    driver_number: int
    stint_number: int
    lap_start: int
    lap_end: int | None = None
    compound: str
    tyre_age_at_start: int


class OpenF1Interval(RawRecord):
    session_key: int
    driver_number: int
    date: str
    gap_to_leader: float | None = None
    interval: float | None = None

    @field_validator("gap_to_leader", "interval", mode="before")
    @classmethod
    def coerce_lapped(cls, v):
        if isinstance(v, str):
            try:
                return float(v)
            except ValueError:
                return None  # e.g. '+1 LAP'
        return v


class OpenF1Weather(RawRecord):
    session_key: int
    date: str
    air_temperature: float
    track_temperature: float
    humidity: float
    pressure: float
    wind_direction: int
    wind_speed: float
    rainfall: bool


class OpenF1RaceControl(RawRecord):
    session_key: int
    date: str
    driver_number: int | None = None
    lap_number: int | None = None
    category: str
    flag: str | None = None
    scope: str | None = None
    sector: int | None = None
    message: str


class OpenF1CarData(RawRecord):
    session_key: int
    driver_number: int
    date: str
    rpm: int
    speed: int
    n_gear: int
    throttle: int
    brake: int
    drs: int


class OpenF1Location(RawRecord):
    session_key: int
    driver_number: int
    date: str
    x: int
    y: int
    z: int

-- Raw OpenF1 tables (2023-present only)
-- Telemetry tables (car_data, location) are partitioned by year+round due to high volume.

CREATE DATABASE IF NOT EXISTS raw_openf1;

CREATE TABLE IF NOT EXISTS raw_openf1.sessions (
    session_key      UInt32,
    session_name     String,
    session_type     String,
    status           String,
    gmt_offset       String,
    path             String,
    date_start       String,
    date_end         Nullable(String),
    year             UInt16,
    circuit_key      UInt16,
    circuit_short_name String,
    country_key      UInt16,
    country_code     String,
    country_name     String,
    location         String,
    meeting_key      UInt32,
    _raw_json        String,
    _ingested_at     DateTime DEFAULT now()
) ENGINE = MergeTree()
ORDER BY (year, session_key);

CREATE TABLE IF NOT EXISTS raw_openf1.drivers (
    session_key        UInt32,
    driver_number      UInt8,
    broadcast_name     String,
    full_name          String,
    name_acronym       String,
    team_name          String,
    team_colour        String,
    first_name         String,
    last_name          String,
    headshot_url       Nullable(String),
    country_code       Nullable(String),
    _raw_json          String,
    _ingested_at       DateTime DEFAULT now()
) ENGINE = MergeTree()
ORDER BY (session_key, driver_number);

CREATE TABLE IF NOT EXISTS raw_openf1.laps (
    session_key          UInt32,
    driver_number        UInt8,
    lap_number           UInt8,
    date_start           String,
    lap_duration         Nullable(Float32),
    is_pit_out_lap       UInt8,
    duration_sector_1    Nullable(Float32),
    duration_sector_2    Nullable(Float32),
    duration_sector_3    Nullable(Float32),
    i1_speed             Nullable(UInt16),
    i2_speed             Nullable(UInt16),
    st_speed             Nullable(UInt16),
    segments_sector_1    Array(UInt16),
    segments_sector_2    Array(UInt16),
    segments_sector_3    Array(UInt16),
    _raw_json            String,
    _ingested_at         DateTime DEFAULT now()
) ENGINE = MergeTree()
ORDER BY (session_key, driver_number, lap_number);

CREATE TABLE IF NOT EXISTS raw_openf1.pit (
    session_key      UInt32,
    driver_number    UInt8,
    lap_number       UInt8,
    date             String,
    pit_duration     Nullable(Float32),
    _raw_json        String,
    _ingested_at     DateTime DEFAULT now()
) ENGINE = MergeTree()
ORDER BY (session_key, driver_number, lap_number);

CREATE TABLE IF NOT EXISTS raw_openf1.stints (
    session_key      UInt32,
    driver_number    UInt8,
    stint_number     UInt8,
    lap_start        UInt8,
    lap_end          Nullable(UInt8),
    compound         String,
    tyre_age_at_start UInt8,
    _raw_json        String,
    _ingested_at     DateTime DEFAULT now()
) ENGINE = MergeTree()
ORDER BY (session_key, driver_number, stint_number);

CREATE TABLE IF NOT EXISTS raw_openf1.intervals (
    session_key      UInt32,
    driver_number    UInt8,
    date             String,
    gap_to_leader    Nullable(Float32),
    interval         Nullable(Float32),
    _raw_json        String,
    _ingested_at     DateTime DEFAULT now()
) ENGINE = MergeTree()
ORDER BY (session_key, driver_number, date);

CREATE TABLE IF NOT EXISTS raw_openf1.weather (
    session_key      UInt32,
    date             String,
    air_temperature  Float32,
    track_temperature Float32,
    humidity         Float32,
    pressure         Float32,
    wind_direction   UInt16,
    wind_speed       Float32,
    rainfall         UInt8,
    _raw_json        String,
    _ingested_at     DateTime DEFAULT now()
) ENGINE = MergeTree()
ORDER BY (session_key, date);

CREATE TABLE IF NOT EXISTS raw_openf1.race_control (
    session_key      UInt32,
    date             String,
    driver_number    Nullable(UInt8),
    lap_number       Nullable(UInt8),
    category         String,
    flag             Nullable(String),
    scope            Nullable(String),
    sector           Nullable(UInt8),
    message          String,
    _raw_json        String,
    _ingested_at     DateTime DEFAULT now()
) ENGINE = MergeTree()
ORDER BY (session_key, date);

CREATE TABLE IF NOT EXISTS raw_openf1.team_radio (
    session_key      UInt32,
    driver_number    UInt8,
    date             String,
    recording_url    String,
    _raw_json        String,
    _ingested_at     DateTime DEFAULT now()
) ENGINE = MergeTree()
ORDER BY (session_key, driver_number, date);

-- High-volume telemetry: partitioned by year and round (via session join)
CREATE TABLE IF NOT EXISTS raw_openf1.car_data (
    session_key      UInt32,
    driver_number    UInt8,
    date             DateTime64(3),  -- millisecond precision
    rpm              UInt16,
    speed            UInt16,
    n_gear           UInt8,
    throttle         UInt8,
    brake            UInt8,
    drs              UInt8,
    _raw_json        String,
    _ingested_at     DateTime DEFAULT now()
) ENGINE = MergeTree()
PARTITION BY (toYear(date), session_key)
ORDER BY (session_key, driver_number, date)
SETTINGS index_granularity = 8192;

CREATE TABLE IF NOT EXISTS raw_openf1.location (
    session_key      UInt32,
    driver_number    UInt8,
    date             DateTime64(3),
    x                Int32,
    y                Int32,
    z                Int32,
    _raw_json        String,
    _ingested_at     DateTime DEFAULT now()
) ENGINE = MergeTree()
PARTITION BY (toYear(date), session_key)
ORDER BY (session_key, driver_number, date)
SETTINGS index_granularity = 8192;

-- Raw Jolpica tables (Ergast-compatible, 1950-present)
-- All tables are append-only MergeTree with raw JSON preservation.

CREATE DATABASE IF NOT EXISTS raw_jolpica;

CREATE TABLE IF NOT EXISTS raw_jolpica.seasons (
    year         UInt16,
    url          String,
    _raw_json    String,
    _ingested_at DateTime DEFAULT now()
) ENGINE = MergeTree()
ORDER BY year;

CREATE TABLE IF NOT EXISTS raw_jolpica.circuits (
    circuit_id   String,
    url          String,
    circuit_name String,
    locality     String,
    country      String,
    lat          Float32,
    lng          Float32,
    alt          Nullable(Float32),
    _raw_json    String,
    _ingested_at DateTime DEFAULT now()
) ENGINE = MergeTree()
ORDER BY circuit_id;

CREATE TABLE IF NOT EXISTS raw_jolpica.constructors (
    constructor_id   String,
    url              String,
    name             String,
    nationality      String,
    _raw_json        String,
    _ingested_at     DateTime DEFAULT now()
) ENGINE = MergeTree()
ORDER BY constructor_id;

CREATE TABLE IF NOT EXISTS raw_jolpica.drivers (
    driver_id        String,
    url              String,
    given_name       String,
    family_name      String,
    date_of_birth    Nullable(String),
    nationality      String,
    permanent_number Nullable(UInt8),
    code             Nullable(String),
    _raw_json        String,
    _ingested_at     DateTime DEFAULT now()
) ENGINE = MergeTree()
ORDER BY driver_id;

CREATE TABLE IF NOT EXISTS raw_jolpica.races (
    season           UInt16,
    round            UInt8,
    race_name        String,
    circuit_id       String,
    date             String,
    time             Nullable(String),
    url              String,
    _raw_json        String,
    _ingested_at     DateTime DEFAULT now()
) ENGINE = MergeTree()
ORDER BY (season, round);

CREATE TABLE IF NOT EXISTS raw_jolpica.results (
    season           UInt16,
    round            UInt8,
    driver_id        String,
    constructor_id   String,
    grid             UInt8,
    position         Nullable(UInt8),
    position_text    String,
    position_order   UInt8,
    points           Float32,
    laps             UInt16,
    status           String,
    time_millis      Nullable(UInt32),
    fastest_lap_rank Nullable(UInt8),
    fastest_lap_time Nullable(String),
    fastest_lap_speed Nullable(Float32),
    _raw_json        String,
    _ingested_at     DateTime DEFAULT now()
) ENGINE = MergeTree()
ORDER BY (season, round, driver_id);

CREATE TABLE IF NOT EXISTS raw_jolpica.qualifying (
    season           UInt16,
    round            UInt8,
    driver_id        String,
    constructor_id   String,
    position         UInt8,
    q1               Nullable(String),
    q2               Nullable(String),
    q3               Nullable(String),
    _raw_json        String,
    _ingested_at     DateTime DEFAULT now()
) ENGINE = MergeTree()
ORDER BY (season, round, driver_id);

CREATE TABLE IF NOT EXISTS raw_jolpica.sprint_results (
    season           UInt16,
    round            UInt8,
    driver_id        String,
    constructor_id   String,
    grid             UInt8,
    position         Nullable(UInt8),
    position_text    String,
    position_order   UInt8,
    points           Float32,
    laps             UInt16,
    status           String,
    time_millis      Nullable(UInt32),
    _raw_json        String,
    _ingested_at     DateTime DEFAULT now()
) ENGINE = MergeTree()
ORDER BY (season, round, driver_id);

CREATE TABLE IF NOT EXISTS raw_jolpica.lap_times (
    season           UInt16,
    round            UInt8,
    driver_id        String,
    lap              UInt8,
    position         UInt8,
    time             String,
    time_millis      UInt32,
    _raw_json        String,
    _ingested_at     DateTime DEFAULT now()
) ENGINE = MergeTree()
ORDER BY (season, round, driver_id, lap);

CREATE TABLE IF NOT EXISTS raw_jolpica.pit_stops (
    season           UInt16,
    round            UInt8,
    driver_id        String,
    stop             UInt8,
    lap              UInt8,
    time             String,
    duration         String,
    duration_millis  Nullable(UInt32),
    _raw_json        String,
    _ingested_at     DateTime DEFAULT now()
) ENGINE = MergeTree()
ORDER BY (season, round, driver_id, stop);

CREATE TABLE IF NOT EXISTS raw_jolpica.driver_standings (
    season           UInt16,
    round            UInt8,
    driver_id        String,
    constructor_id   String,
    position         UInt8,
    position_text    String,
    points           Float32,
    wins             UInt8,
    updated_at       DateTime DEFAULT now(),
    _raw_json        String,
    _ingested_at     DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (season, round, driver_id);

CREATE TABLE IF NOT EXISTS raw_jolpica.constructor_standings (
    season           UInt16,
    round            UInt8,
    constructor_id   String,
    position         UInt8,
    position_text    String,
    points           Float32,
    wins             UInt8,
    updated_at       DateTime DEFAULT now(),
    _raw_json        String,
    _ingested_at     DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (season, round, constructor_id);

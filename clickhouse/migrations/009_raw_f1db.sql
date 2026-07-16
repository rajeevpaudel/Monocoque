-- Raw f1db tables — populated from the f1db community CSV dump
-- (https://github.com/f1db/f1db). Covers 1950–present.
--
-- NOTE: f1db uses full-name driver IDs (e.g. 'lewis-hamilton') while
-- Jolpica uses short slugs (e.g. 'hamilton'). These tables are kept
-- separate from raw_jolpica.* to avoid ID collisions.

CREATE DATABASE IF NOT EXISTS raw_f1db;

CREATE TABLE IF NOT EXISTS raw_f1db.seasons (
    year         UInt16,
    _ingested_at DateTime DEFAULT now()
) ENGINE = MergeTree() ORDER BY year;

CREATE TABLE IF NOT EXISTS raw_f1db.circuits (
    circuit_id   String,
    name         String,
    place_name   String,
    country_id   String,
    lat          Float64,
    lng          Float64,
    length_km    Nullable(Float32),
    turns        Nullable(UInt8),
    _ingested_at DateTime DEFAULT now()
) ENGINE = MergeTree() ORDER BY circuit_id;

CREATE TABLE IF NOT EXISTS raw_f1db.drivers (
    driver_id        String,
    first_name       String,
    last_name        String,
    abbreviation     Nullable(String),
    permanent_number Nullable(UInt8),
    date_of_birth    Nullable(String),
    nationality      String,
    _ingested_at     DateTime DEFAULT now()
) ENGINE = MergeTree() ORDER BY driver_id;

CREATE TABLE IF NOT EXISTS raw_f1db.constructors (
    constructor_id String,
    name           String,
    country_id     String,
    _ingested_at   DateTime DEFAULT now()
) ENGINE = MergeTree() ORDER BY constructor_id;

CREATE TABLE IF NOT EXISTS raw_f1db.races (
    year         UInt16,
    round        UInt8,
    race_name    String,
    circuit_id   String,
    date         String,
    time         Nullable(String),
    laps         Nullable(UInt8),
    _ingested_at DateTime DEFAULT now()
) ENGINE = MergeTree() ORDER BY (year, round);

CREATE TABLE IF NOT EXISTS raw_f1db.race_results (
    year           UInt16,
    round          UInt8,
    driver_id      String,
    constructor_id String,
    grid           Nullable(UInt8),
    position       Nullable(UInt8),
    position_text  String,
    position_order UInt8,
    points         Float32,
    laps           Nullable(UInt16),
    status         Nullable(String),
    time_millis    Nullable(UInt32),
    _ingested_at   DateTime DEFAULT now()
) ENGINE = MergeTree() ORDER BY (year, round, driver_id);

CREATE TABLE IF NOT EXISTS raw_f1db.qualifying_results (
    year           UInt16,
    round          UInt8,
    driver_id      String,
    constructor_id String,
    position       Nullable(UInt8),
    q1             Nullable(String),
    q1_millis      Nullable(UInt32),
    q2             Nullable(String),
    q2_millis      Nullable(UInt32),
    q3             Nullable(String),
    q3_millis      Nullable(UInt32),
    time_millis    Nullable(UInt32),
    _ingested_at   DateTime DEFAULT now()
) ENGINE = MergeTree() ORDER BY (year, round, driver_id);

CREATE TABLE IF NOT EXISTS raw_f1db.sprint_race_results (
    year           UInt16,
    round          UInt8,
    driver_id      String,
    constructor_id String,
    grid           Nullable(UInt8),
    position       Nullable(UInt8),
    position_text  String,
    position_order UInt8,
    points         Float32,
    laps           Nullable(UInt16),
    status         Nullable(String),
    time_millis    Nullable(UInt32),
    _ingested_at   DateTime DEFAULT now()
) ENGINE = MergeTree() ORDER BY (year, round, driver_id);

CREATE TABLE IF NOT EXISTS raw_f1db.pit_stops (
    year         UInt16,
    round        UInt8,
    driver_id    String,
    stop         UInt8,
    lap          UInt8,
    time         String,
    time_millis  Nullable(UInt32),
    _ingested_at DateTime DEFAULT now()
) ENGINE = MergeTree() ORDER BY (year, round, driver_id, stop);

CREATE TABLE IF NOT EXISTS raw_f1db.fastest_laps (
    year           UInt16,
    round          UInt8,
    driver_id      String,
    constructor_id String,
    position       Nullable(UInt8),
    lap            Nullable(UInt8),
    time           Nullable(String),
    time_millis    Nullable(UInt32),
    _ingested_at   DateTime DEFAULT now()
) ENGINE = MergeTree() ORDER BY (year, round, driver_id);

CREATE TABLE IF NOT EXISTS raw_f1db.driver_standings (
    year          UInt16,
    round         UInt8,
    driver_id     String,
    position      Nullable(UInt8),
    position_text String,
    points        Float32,
    _ingested_at  DateTime DEFAULT now()
) ENGINE = MergeTree() ORDER BY (year, round, driver_id);

CREATE TABLE IF NOT EXISTS raw_f1db.constructor_standings (
    year           UInt16,
    round          UInt8,
    constructor_id String,
    position       Nullable(UInt8),
    position_text  String,
    points         Float32,
    _ingested_at   DateTime DEFAULT now()
) ENGINE = MergeTree() ORDER BY (year, round, constructor_id);

CREATE DATABASE IF NOT EXISTS raw_fastf1;

CREATE TABLE IF NOT EXISTS raw_fastf1.car_telemetry
(
    session_key    Int32,
    driver_number  UInt8,
    date           DateTime64(3, 'UTC'),
    distance_m     Float32,
    _ingested_at   DateTime DEFAULT now()
)
ENGINE = MergeTree()
ORDER BY (session_key, driver_number, date);

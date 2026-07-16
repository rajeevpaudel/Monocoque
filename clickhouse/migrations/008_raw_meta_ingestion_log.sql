CREATE DATABASE IF NOT EXISTS raw_meta;

-- Design notes:
-- * Backfill records 'ok', 'incomplete', or 'empty' for sessions that already have raw data,
--   so the first pipeline run skips already-ingested sessions.
-- * Sessions with zero rows in a source table are NOT backfilled. Absence from this log is
--   treated as "needs ingestion" by needs_ingestion(), which returns True for missing entries.
--   Zero-row sessions will therefore be picked up and re-tried on the next pipeline run.
-- * Running this file more than once is safe: reads always use argMax(status, attempted_at)
--   to get the latest status per (source, entity_key, table_name), so duplicate backfill rows
--   are functionally harmless — they all carry the same status value.

CREATE TABLE IF NOT EXISTS raw_meta.ingestion_log (
    source        LowCardinality(String),   -- 'openf1' | 'jolpica'
    entity_key    String,                    -- str(session_key) or "season-round"
    table_name    LowCardinality(String),    -- 'raw_openf1.laps' etc.
    status        LowCardinality(String),    -- 'ok' | 'empty' | 'failed' | 'incomplete'
    row_count     UInt64,                    -- UInt64: count() returns UInt64 in ClickHouse
    min_expected  UInt32,
    error_msg     String DEFAULT '',
    attempted_at  DateTime
) ENGINE = MergeTree()
ORDER BY (source, entity_key, table_name, attempted_at);

-- Backfill from existing raw data so first run skips already-ingested sessions.
INSERT INTO raw_meta.ingestion_log
    (source, entity_key, table_name, status, row_count, min_expected, error_msg, attempted_at)
SELECT 'openf1', toString(session_key), 'raw_openf1.laps',
       if(count() >= 50, 'ok', 'incomplete'), count(), 50, '', now()
FROM raw_openf1.laps GROUP BY session_key;

INSERT INTO raw_meta.ingestion_log
    (source, entity_key, table_name, status, row_count, min_expected, error_msg, attempted_at)
SELECT 'openf1', toString(session_key), 'raw_openf1.car_data',
       if(count() >= 1000, 'ok', 'incomplete'), count(), 1000, '', now()
FROM raw_openf1.car_data GROUP BY session_key;

INSERT INTO raw_meta.ingestion_log
    (source, entity_key, table_name, status, row_count, min_expected, error_msg, attempted_at)
SELECT 'openf1', toString(session_key), 'raw_openf1.stints',
       if(count() >= 10, 'ok', 'incomplete'), count(), 10, '', now()
FROM raw_openf1.stints GROUP BY session_key;

INSERT INTO raw_meta.ingestion_log
    (source, entity_key, table_name, status, row_count, min_expected, error_msg, attempted_at)
SELECT 'openf1', toString(session_key), 'raw_openf1.weather',
       if(count() >= 5, 'ok', 'incomplete'), count(), 5, '', now()
FROM raw_openf1.weather GROUP BY session_key;

INSERT INTO raw_meta.ingestion_log
    (source, entity_key, table_name, status, row_count, min_expected, error_msg, attempted_at)
SELECT 'openf1', toString(session_key), 'raw_openf1.pit',
       if(count() > 0, 'ok', 'empty'), count(), 0, '', now()
FROM raw_openf1.pit GROUP BY session_key;

INSERT INTO raw_meta.ingestion_log
    (source, entity_key, table_name, status, row_count, min_expected, error_msg, attempted_at)
SELECT 'openf1', toString(session_key), 'raw_openf1.intervals',
       if(count() > 0, 'ok', 'empty'), count(), 0, '', now()
FROM raw_openf1.intervals GROUP BY session_key;

INSERT INTO raw_meta.ingestion_log
    (source, entity_key, table_name, status, row_count, min_expected, error_msg, attempted_at)
SELECT 'openf1', toString(session_key), 'raw_openf1.race_control',
       if(count() >= 3, 'ok', 'incomplete'), count(), 3, '', now()
FROM raw_openf1.race_control GROUP BY session_key;

INSERT INTO raw_meta.ingestion_log
    (source, entity_key, table_name, status, row_count, min_expected, error_msg, attempted_at)
SELECT 'openf1', toString(session_key), 'raw_openf1.location',
       if(count() >= 1000, 'ok', 'incomplete'), count(), 1000, '', now()
FROM raw_openf1.location GROUP BY session_key;

INSERT INTO raw_meta.ingestion_log
    (source, entity_key, table_name, status, row_count, min_expected, error_msg, attempted_at)
SELECT 'jolpica', concat(toString(season), '-', toString(round)), 'raw_jolpica.qualifying',
       if(count() >= 15, 'ok', 'incomplete'), count(), 15, '', now()
FROM raw_jolpica.qualifying GROUP BY season, round;

INSERT INTO raw_meta.ingestion_log
    (source, entity_key, table_name, status, row_count, min_expected, error_msg, attempted_at)
SELECT 'jolpica', concat(toString(season), '-', toString(round)), 'raw_jolpica.results',
       if(count() >= 10, 'ok', 'incomplete'), count(), 10, '', now()
FROM raw_jolpica.results GROUP BY season, round;

INSERT INTO raw_meta.ingestion_log
    (source, entity_key, table_name, status, row_count, min_expected, error_msg, attempted_at)
SELECT 'jolpica', concat(toString(season), '-', toString(round)), 'raw_jolpica.driver_standings',
       if(count() >= 1, 'ok', 'incomplete'), count(), 1, '', now()
FROM raw_jolpica.driver_standings GROUP BY season, round;

INSERT INTO raw_meta.ingestion_log
    (source, entity_key, table_name, status, row_count, min_expected, error_msg, attempted_at)
SELECT 'jolpica', concat(toString(season), '-', toString(round)), 'raw_jolpica.constructor_standings',
       if(count() >= 1, 'ok', 'incomplete'), count(), 1, '', now()
FROM raw_jolpica.constructor_standings GROUP BY season, round;

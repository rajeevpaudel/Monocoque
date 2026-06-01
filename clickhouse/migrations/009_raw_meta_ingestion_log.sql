CREATE DATABASE IF NOT EXISTS raw_meta;

CREATE TABLE IF NOT EXISTS raw_meta.ingestion_log (
    source        LowCardinality(String),   -- 'openf1' | 'jolpica'
    entity_key    String,                    -- str(session_key) or "season-round"
    table_name    LowCardinality(String),    -- 'raw_openf1.laps' etc.
    status        LowCardinality(String),    -- 'ok' | 'empty' | 'failed' | 'incomplete'
    row_count     UInt32,
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
       'ok', count(), 0, '', now()
FROM raw_openf1.pit GROUP BY session_key;

INSERT INTO raw_meta.ingestion_log
    (source, entity_key, table_name, status, row_count, min_expected, error_msg, attempted_at)
SELECT 'openf1', toString(session_key), 'raw_openf1.intervals',
       'ok', count(), 0, '', now()
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

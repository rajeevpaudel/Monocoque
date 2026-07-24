-- Deduplicate raw laps: the raw table is MergeTree (not ReplacingMergeTree) so
-- repeated ingestion runs can insert identical rows. Keep one row per natural key
-- (session_key, driver_number, lap_number) using argMax to pick the most-recent
-- ingestion while retaining all other columns deterministically.
-- Note: uses a subquery rather than CTE to avoid ClickHouse 24.3 CTE-flattening
-- bug that incorrectly flags max() as nested inside argMax().
SELECT
    session_key,
    driver_number,
    lap_number,
    parseDateTime64BestEffort(date_start) AS lap_start,
    lap_duration,
    is_pit_out_lap,
    duration_sector_1,
    duration_sector_2,
    duration_sector_3,
    i1_speed,
    i2_speed,
    st_speed,
    segments_sector_1,
    segments_sector_2,
    segments_sector_3,
    last_ingested_at AS _ingested_at,
    'openf1'         AS _source
FROM (
    SELECT
        session_key,
        driver_number,
        lap_number,
        argMax(date_start,          _ingested_at) AS date_start,
        argMax(lap_duration,        _ingested_at) AS lap_duration,
        argMax(is_pit_out_lap,      _ingested_at) AS is_pit_out_lap,
        argMax(duration_sector_1,   _ingested_at) AS duration_sector_1,
        argMax(duration_sector_2,   _ingested_at) AS duration_sector_2,
        argMax(duration_sector_3,   _ingested_at) AS duration_sector_3,
        argMax(i1_speed,            _ingested_at) AS i1_speed,
        argMax(i2_speed,            _ingested_at) AS i2_speed,
        argMax(st_speed,            _ingested_at) AS st_speed,
        argMax(segments_sector_1,   _ingested_at) AS segments_sector_1,
        argMax(segments_sector_2,   _ingested_at) AS segments_sector_2,
        argMax(segments_sector_3,   _ingested_at) AS segments_sector_3,
        max(_ingested_at)                         AS last_ingested_at
    FROM {{ source('raw_openf1', 'laps') }}
    GROUP BY session_key, driver_number, lap_number
)

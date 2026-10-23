-- Deduplicate by natural key — re-ingestion produces duplicate raw rows.
-- q{n}_ms computed in an outer SELECT to avoid ClickHouse 24.3's restriction on
-- scalar functions that contain aggregate function calls inside a CREATE VIEW body.
SELECT
    season,
    round,
    driver_id,
    constructor_id,
    qualifying_position,
    q1,
    q2,
    q3,
    {{ lap_time_to_ms('q1') }}   AS q1_ms,
    {{ lap_time_to_ms('q2') }}   AS q2_ms,
    {{ lap_time_to_ms('q3') }}   AS q3_ms,
    _ingested_at,
    _source
FROM (
    SELECT
        season,
        round,
        driver_id,
        any(constructor_id)       AS constructor_id,
        toUInt8(any(position))    AS qualifying_position,
        NULLIF(any(q1), '')       AS q1,
        NULLIF(any(q2), '')       AS q2,
        NULLIF(any(q3), '')       AS q3,
        max(_ingested_at)         AS _ingested_at,
        'jolpica'                 AS _source
    FROM {{ source('raw_jolpica', 'qualifying') }}
    GROUP BY season, round, driver_id

    UNION ALL

    -- f1db already has pre-computed millis; use them directly via the outer SELECT's macro.
    -- Deduplicate: raw_f1db.qualifying_results is MergeTree (not ReplacingMergeTree);
    -- repeated CSV ingestion produces duplicate rows. GROUP BY collapses them.
    SELECT
        year                      AS season,
        round,
        driver_id,
        any(constructor_id)       AS constructor_id,
        toUInt8(any(position))    AS qualifying_position,
        NULLIF(any(q1), '')       AS q1,
        NULLIF(any(q2), '')       AS q2,
        NULLIF(any(q3), '')       AS q3,
        max(_ingested_at)         AS _ingested_at,
        'f1db'                    AS _source
    FROM {{ f1db_source('qualifying_results', driver_col='driver_id', constructor_col='constructor_id') }}
    WHERE year NOT IN (SELECT DISTINCT season FROM {{ source('raw_jolpica', 'qualifying') }})
    GROUP BY year, round, driver_id
)

-- Deduplicate by natural key — re-ingestion produces duplicate raw rows.
-- q{n}_ms computed in an outer SELECT to avoid ClickHouse 24.3's restriction on
-- scalar functions that contain aggregate function calls inside a CREATE VIEW body.
-- (Plain SELECT ... GROUP BY accepts this, but VIEW creation does not.)
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
)

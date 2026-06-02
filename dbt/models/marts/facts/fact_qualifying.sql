{{
    config(
        materialized='incremental',
        incremental_strategy='append',
        engine='MergeTree()',
        order_by='(season, round, driver_id)',
    )
}}

-- ClickHouse 24.3 limitations:
-- (1) columns of a CTE cannot be referenced via alias qualification (cte_alias.column)
--     when that CTE is the primary FROM source.
-- (2) CREATE TABLE ... EMPTY AS fails to resolve columns from CTEs that reference a
--     view whose definition contains a subquery.
-- Workarounds:
-- (1) keep stg_jolpica__qualifying as the main FROM (alias q); use a separate jolpica_ms
--     CTE as a JOIN target inside matched_laps (where alias qualification works fine).
-- (2) jolpica_ms reads directly from the raw source table (not the view) so it remains
--     a plain aggregating subquery that ClickHouse can resolve during table creation.

WITH

jolpica_ms AS (
    SELECT
        season,
        round,
        driver_id,
        {{ lap_time_to_ms('NULLIF(any(q1), \'\')') }}               AS q1_ms,
        {{ lap_time_to_ms('NULLIF(any(q2), \'\')') }}               AS q2_ms,
        {{ lap_time_to_ms('NULLIF(any(q3), \'\')') }}               AS q3_ms
    FROM {{ source('raw_jolpica', 'qualifying') }}
    GROUP BY season, round, driver_id
),

openf1_best AS (
    SELECT
        session_key,
        driver_number,
        toInt32(min(lap_duration) * 1000)    AS openf1_best_time_ms
    FROM {{ ref('stg_openf1__laps') }}
    WHERE is_pit_out_lap = 0
      AND lap_duration IS NOT NULL
    GROUP BY session_key, driver_number
),

all_laps AS (
    SELECT
        session_key,
        driver_number,
        lap_number,
        toInt32(lap_duration * 1000)         AS lap_ms,
        lap_duration,
        duration_sector_1,
        duration_sector_2,
        duration_sector_3,
        i1_speed,
        i2_speed,
        st_speed,
        segments_sector_1,
        segments_sector_2,
        segments_sector_3
    FROM {{ ref('stg_openf1__laps') }}
    WHERE is_pit_out_lap = 0
      AND lap_duration IS NOT NULL
),

matched_laps AS (
    SELECT
        al.session_key     AS session_key,
        al.driver_number   AS driver_number,
        argMin(al.lap_number,        abs(al.lap_ms - COALESCE(jms.q3_ms, jms.q2_ms, jms.q1_ms))) AS best_lap_number,
        argMin(al.lap_duration,      abs(al.lap_ms - COALESCE(jms.q3_ms, jms.q2_ms, jms.q1_ms))) AS best_lap_duration,
        argMin(al.duration_sector_1, abs(al.lap_ms - COALESCE(jms.q3_ms, jms.q2_ms, jms.q1_ms))) AS best_s1,
        argMin(al.duration_sector_2, abs(al.lap_ms - COALESCE(jms.q3_ms, jms.q2_ms, jms.q1_ms))) AS best_s2,
        argMin(al.duration_sector_3, abs(al.lap_ms - COALESCE(jms.q3_ms, jms.q2_ms, jms.q1_ms))) AS best_s3,
        argMin(al.i1_speed,          abs(al.lap_ms - COALESCE(jms.q3_ms, jms.q2_ms, jms.q1_ms))) AS i1_speed,
        argMin(al.i2_speed,          abs(al.lap_ms - COALESCE(jms.q3_ms, jms.q2_ms, jms.q1_ms))) AS i2_speed,
        argMin(al.st_speed,          abs(al.lap_ms - COALESCE(jms.q3_ms, jms.q2_ms, jms.q1_ms))) AS st_speed,
        argMin(al.segments_sector_1, abs(al.lap_ms - COALESCE(jms.q3_ms, jms.q2_ms, jms.q1_ms))) AS segments_s1,
        argMin(al.segments_sector_2, abs(al.lap_ms - COALESCE(jms.q3_ms, jms.q2_ms, jms.q1_ms))) AS segments_s2,
        argMin(al.segments_sector_3, abs(al.lap_ms - COALESCE(jms.q3_ms, jms.q2_ms, jms.q1_ms))) AS segments_s3
    FROM all_laps al
    JOIN {{ ref('int_session_map') }}             sm ON sm.session_key = al.session_key
    JOIN {{ ref('driver_id_map') }}     m
        ON  m.openf1_driver_number = al.driver_number
        AND m.season               = sm.season
    JOIN jolpica_ms                              jms
        ON  jms.season    = sm.season
        AND jms.round     = sm.round
        AND jms.driver_id = m.jolpica_driver_id
    WHERE COALESCE(jms.q3_ms, jms.q2_ms, jms.q1_ms) IS NOT NULL
      AND abs(al.lap_ms - COALESCE(jms.q3_ms, jms.q2_ms, jms.q1_ms)) <= 50
    GROUP BY al.session_key, al.driver_number
)

SELECT
    q.season,
    q.round,
    q.driver_id,
    q.constructor_id,
    q.qualifying_position,
    q.q1,
    q.q2,
    q.q3,
    q.q1_ms,
    q.q2_ms,
    q.q3_ms,
    COALESCE(q.q3_ms, q.q2_ms, q.q1_ms)   AS jolpica_best_time_ms,
    m.openf1_driver_number,
    sm.session_key,
    ml.best_lap_number,
    ml.best_lap_duration,
    ml.best_s1,
    ml.best_s2,
    ml.best_s3,
    ml.i1_speed,
    ml.i2_speed,
    ml.st_speed,
    ml.segments_s1,
    ml.segments_s2,
    ml.segments_s3,
    ob.openf1_best_time_ms,
    multiIf(
        ml.best_lap_number IS NOT NULL,         'matched',
        sm.session_key     IS NULL,             'jolpica_only',
        COALESCE(q.q1_ms, q.q2_ms, q.q3_ms) IS NULL,    NULL,
                                                'mismatch'
    ) AS best_source_match
FROM {{ ref('stg_jolpica__qualifying') }}        q
LEFT JOIN {{ ref('driver_id_map') }}   m
    ON  m.jolpica_driver_id = q.driver_id
    AND m.season            = q.season
LEFT JOIN {{ ref('int_session_map') }}           sm
    ON  sm.season       = q.season
    AND sm.round        = q.round
    AND sm.session_type = 'Qualifying'
    AND sm.session_name = 'Qualifying'
LEFT JOIN matched_laps                           ml
    ON  ml.session_key   = sm.session_key
    AND ml.driver_number = m.openf1_driver_number
LEFT JOIN openf1_best                            ob
    ON  ob.session_key   = sm.session_key
    AND ob.driver_number = m.openf1_driver_number

{% if is_incremental() %}
WHERE (q.season, q.round) NOT IN (
    SELECT season, round FROM {{ this }}
)
{% endif %}

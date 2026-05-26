{{
    config(
        materialized='incremental',
        incremental_strategy='append',
        engine='MergeTree()',
        order_by='(season, round, driver_id)',
    )
}}

WITH best_laps AS (
    SELECT
        session_key,
        driver_number,
        argMin(lap_number,      lap_duration)   AS best_lap_number,
        min(lap_duration)                       AS best_lap_duration,
        argMin(duration_sector_1, lap_duration) AS best_s1,
        argMin(duration_sector_2, lap_duration) AS best_s2,
        argMin(duration_sector_3, lap_duration) AS best_s3,
        argMin(i1_speed,        lap_duration)   AS i1_speed,
        argMin(i2_speed,        lap_duration)   AS i2_speed,
        argMin(st_speed,        lap_duration)   AS st_speed,
        argMin(segments_sector_1, lap_duration) AS segments_s1,
        argMin(segments_sector_2, lap_duration) AS segments_s2,
        argMin(segments_sector_3, lap_duration) AS segments_s3
    FROM {{ ref('stg_openf1__laps') }}
    WHERE is_pit_out_lap = 0
      AND lap_duration IS NOT NULL
    GROUP BY session_key, driver_number
)

SELECT
    q.season                AS season,
    q.round                 AS round,
    q.driver_id             AS driver_id,
    q.constructor_id        AS constructor_id,
    q.qualifying_position   AS qualifying_position,
    q.q1                    AS q1,
    q.q2                    AS q2,
    q.q3                    AS q3,
    m.openf1_driver_number  AS openf1_driver_number,
    sm.session_key          AS session_key,
    -- OpenF1 best lap detail (NULL for pre-2023)
    bl.best_lap_number      AS best_lap_number,
    bl.best_lap_duration    AS best_lap_duration,
    bl.best_s1              AS best_s1,
    bl.best_s2              AS best_s2,
    bl.best_s3              AS best_s3,
    bl.i1_speed             AS i1_speed,
    bl.i2_speed             AS i2_speed,
    bl.st_speed             AS st_speed,
    bl.segments_s1          AS segments_s1,
    bl.segments_s2          AS segments_s2,
    bl.segments_s3          AS segments_s3
FROM {{ ref('stg_jolpica__qualifying') }}       q
LEFT JOIN {{ source('dim', 'driver_id_map') }}  m
    ON  m.jolpica_driver_id = q.driver_id
    AND m.season            = q.season
LEFT JOIN {{ ref('int_session_map') }}          sm
    ON  sm.season       = q.season
    AND sm.round        = q.round
    AND sm.session_type = 'Qualifying'
LEFT JOIN best_laps                             bl
    ON  bl.session_key   = sm.session_key
    AND bl.driver_number = m.openf1_driver_number

{% if is_incremental() %}
WHERE (q.season, q.round) NOT IN (
    SELECT season, round FROM {{ this }}
)
{% endif %}

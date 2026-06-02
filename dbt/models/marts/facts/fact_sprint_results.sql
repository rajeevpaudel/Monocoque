{{
    config(
        materialized='incremental',
        incremental_strategy='append',
        engine='MergeTree()',
        order_by='(season, round, driver_id)',
    )
}}

SELECT
    s.season                AS season,
    s.round                 AS round,
    s.driver_id             AS driver_id,
    s.constructor_id        AS constructor_id,
    s.grid_position         AS grid_position,
    s.finish_position       AS finish_position,
    s.position_text         AS position_text,
    s.points                AS points,
    s.laps_completed        AS laps_completed,
    s.status                AS status,
    s.time_millis           AS sprint_time_ms,
    m.openf1_driver_number  AS openf1_driver_number,
    sm.session_key          AS session_key
FROM {{ ref('stg_jolpica__sprint_results') }}   s
LEFT JOIN {{ ref('driver_id_map') }}  m
    ON  m.jolpica_driver_id = s.driver_id
    AND m.season            = s.season
LEFT JOIN {{ ref('int_session_map') }}          sm
    ON  sm.season       = s.season
    AND sm.round        = s.round
    AND sm.session_type = 'Sprint'

{% if is_incremental() %}
WHERE (s.season, s.round) NOT IN (
    SELECT season, round FROM {{ this }}
)
{% endif %}

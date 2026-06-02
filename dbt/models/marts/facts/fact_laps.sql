-- Per-lap timing. For 2023+, prefers OpenF1 detail. For pre-2023, uses Jolpica aggregate.
{{
    config(
        materialized='incremental',
        incremental_strategy='append',
        engine='MergeTree()',
        order_by='(season, round, driver_id, lap_number)',
    )
}}

SELECT
    jl.season               AS season,
    jl.round                AS round,
    jl.driver_id            AS driver_id,
    jl.lap_number           AS lap_number,
    jl.lap_position         AS lap_position,
    jl.lap_time             AS lap_time,
    jl.lap_time_ms          AS lap_time_ms,
    ol.lap_duration         AS of1_lap_duration,
    ol.duration_sector_1    AS of1_s1,
    ol.duration_sector_2    AS of1_s2,
    ol.duration_sector_3    AS of1_s3,
    ol.i1_speed             AS i1_speed,
    ol.i2_speed             AS i2_speed,
    ol.st_speed             AS st_speed,
    ol.is_pit_out_lap       AS is_pit_out_lap,
    m.openf1_driver_number  AS openf1_driver_number,
    sm.session_key          AS session_key
FROM {{ ref('stg_jolpica__laps') }}             jl
LEFT JOIN {{ ref('driver_id_map') }}  m
    ON  m.jolpica_driver_id = jl.driver_id
    AND m.season            = jl.season
LEFT JOIN {{ ref('int_session_map') }}          sm
    ON  sm.season       = jl.season
    AND sm.round        = jl.round
    AND sm.session_type = 'Race'
LEFT JOIN {{ ref('stg_openf1__laps') }}         ol
    ON  ol.session_key   = sm.session_key
    AND ol.driver_number = m.openf1_driver_number
    AND ol.lap_number    = jl.lap_number

{% if is_incremental() %}
WHERE (jl.season, jl.round) NOT IN (
    SELECT season, round FROM {{ this }}
)
{% endif %}

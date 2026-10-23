-- Per-lap timing. For 2023+, prefers OpenF1 detail. For pre-2023, uses Jolpica aggregate.
{{
    config(
        materialized='incremental',
        incremental_strategy='delete+insert',
        unique_key=['season', 'round', 'driver_id', 'lap_number'],
        engine='MergeTree()',
        order_by='(season, round, driver_id, lap_number)',
    )
}}

SELECT
    toUInt32(jl.season * 100 + jl.round) AS race_sk,
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
    m.driver_number  AS openf1_driver_number,
    sm.session_key          AS session_key,
    ts.track_status         AS track_status,
    greatest(jl._ingested_at, coalesce(ol._ingested_at, jl._ingested_at)) AS _ingested_at
FROM {{ ref('stg_jolpica__laps') }}             jl
LEFT JOIN {{ ref('int_session_map') }}          sm
    ON  sm.season        = jl.season
    AND sm.round         = jl.round
    AND sm.session_type  = 'Race'
    AND sm.session_name  = 'Race'
LEFT JOIN {{ ref('driver_session_map') }}     m
    ON  m.session_key       = sm.session_key
    AND m.jolpica_driver_id = jl.driver_id
LEFT JOIN {{ ref('stg_openf1__laps') }}         ol
    ON  ol.session_key   = sm.session_key
    AND ol.driver_number = m.driver_number
    AND ol.lap_number    = jl.lap_number
LEFT JOIN {{ ref('int_lap_track_status') }}     ts
    ON  ts.session_key   = sm.session_key
    AND ts.lap_number    = jl.lap_number

{% if is_incremental() %}
WHERE {{ changed_rounds('jl.season', 'jl.round', 'stg_jolpica__laps', session_name='Race') }}
{% endif %}

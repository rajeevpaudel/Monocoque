-- Enriches Jolpica race results with OpenF1 lap data via the driver ID bridge.
-- For seasons before 2023, openf1_* columns will be NULL.
SELECT
    r.season                AS season,
    r.round                 AS round,
    r.driver_id             AS driver_id,
    m.openf1_driver_number  AS openf1_driver_number,
    r.constructor_id        AS constructor_id,
    r.grid_position         AS grid_position,
    r.finish_position       AS finish_position,
    r.position_text         AS position_text,
    r.points                AS points,
    r.laps_completed        AS laps_completed,
    r.status                AS status,
    r.time_millis           AS race_time_ms,
    r.fastest_lap_rank      AS fastest_lap_rank,
    r.fastest_lap_time      AS fastest_lap_time,
    r.fastest_lap_speed     AS fastest_lap_speed,
    l.lap_duration          AS of1_fastest_lap_duration,
    l.duration_sector_1     AS of1_s1,
    l.duration_sector_2     AS of1_s2,
    l.duration_sector_3     AS of1_s3,
    sm.session_key          AS session_key
FROM {{ ref('stg_jolpica__results') }}          r
LEFT JOIN {{ ref('driver_id_map') }}  m
    ON  m.jolpica_driver_id = r.driver_id
    AND m.season            = r.season
LEFT JOIN {{ ref('int_session_map') }}          sm
    ON  sm.season       = r.season
    AND sm.round        = r.round
    AND sm.session_type = 'Race'
    AND sm.session_name = 'Race'
LEFT JOIN {{ ref('stg_openf1__laps') }}         l
    ON  l.session_key   = sm.session_key
    AND l.driver_number = m.openf1_driver_number
    AND l.lap_number    = r.laps_completed  -- fastest lap is typically the last completed

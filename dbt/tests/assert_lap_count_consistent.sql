-- Warn if OpenF1 and Jolpica race lap counts differ by more than 2 for any driver.
-- Both sides count all laps (including pit-out laps) to avoid systematic undercounting in OpenF1.
-- Severity is warn because OpenF1 occasionally serves partial lap data from stale replicas.
{{ config(severity='warn') }}
WITH openf1_laps AS (
    SELECT session_key, driver_number, count() AS lap_count
    FROM {{ ref('stg_openf1__laps') }}
    GROUP BY session_key, driver_number
),
jolpica_laps AS (
    SELECT season, round, driver_id, count() AS lap_count
    FROM {{ ref('stg_jolpica__laps') }}
    GROUP BY season, round, driver_id
)
SELECT
    sm.season,
    sm.round,
    jl.driver_id,
    jl.lap_count  AS jolpica_count,
    ol.lap_count  AS openf1_count
FROM jolpica_laps jl
JOIN {{ ref('int_session_map') }} sm
    ON  sm.season       = jl.season
    AND sm.round        = jl.round
    AND sm.session_type = 'Race'
    AND sm.session_name = 'Race'
JOIN {{ ref('driver_id_map') }} m
    ON  m.jolpica_driver_id = jl.driver_id
    AND m.season            = jl.season
JOIN openf1_laps ol
    ON  ol.session_key   = sm.session_key
    AND ol.driver_number = m.openf1_driver_number
WHERE abs(jl.lap_count - ol.lap_count) > 2

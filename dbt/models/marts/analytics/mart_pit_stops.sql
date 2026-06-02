-- Grain: one row per pit stop per driver per race.
-- Combines fact_pit_stops with race and driver context.
SELECT
    p.season,
    p.round,
    r.race_name,
    r.circuit_name,
    r.circuit_country,
    r.race_date,
    p.driver_id,
    p.stop_number,
    p.lap_number            AS pit_lap,
    p.pit_time_of_day,
    p.pit_duration,
    p.pit_duration_ms,
    p.openf1_driver_number,
    p.session_key,
    fr.constructor_id,
    d.full_name             AS driver_name,
    d.driver_code,
    d.nationality           AS driver_nationality
FROM {{ ref('fact_pit_stops') }}        p
LEFT JOIN {{ ref('dim_races') }}        r  ON r.race_sk = p.race_sk
LEFT JOIN {{ ref('fact_race_results') }} fr
    ON  fr.season    = p.season
    AND fr.round     = p.round
    AND fr.driver_id = p.driver_id
LEFT JOIN {{ ref('dim_drivers') }}      d
    ON  d.driver_id = p.driver_id
    AND d.season    = p.season

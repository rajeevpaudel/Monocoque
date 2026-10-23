-- Grain: one row per driver per tyre stint per race (2023+, OpenF1 stints).
-- deg_s_per_lap is the least-squares slope of fuel-corrected lap time vs tyre age over the
-- stint's clean laps (positive = getting slower). Requires ≥ 5 clean laps for a slope.
{{ config(materialized='table', order_by='(season, round, driver_id, stint_number)') }}

SELECT
    l.season                                       AS season,
    l.round                                        AS round,
    r.race_name                                    AS race_name,
    r.circuit_id                                   AS circuit_id,
    l.driver_id                                    AS driver_id,
    any(l.constructor_id)                          AS constructor_id,
    assumeNotNull(l.stint_number)                  AS stint_number,
    any(l.compound)                                AS compound,
    min(l.lap_number)                              AS first_lap,
    max(l.lap_number)                              AS last_lap,
    min(l.tyre_age)                                AS start_tyre_age,
    max(l.tyre_age)                                AS end_tyre_age,
    countIf(l.is_clean = 1)                        AS clean_laps,
    round(medianIf(l.lap_s, l.is_clean = 1), 3)    AS median_lap_s,
    round(minIf(l.lap_s, l.is_clean = 1), 3)       AS best_lap_s,
    round(if(clean_laps >= 5,
        simpleLinearRegressionIf(toFloat64(l.tyre_age), l.fuel_corrected_lap_s, l.is_clean = 1).1,
        NULL), 4)                                  AS deg_s_per_lap,
    round(if(clean_laps >= 5,
        simpleLinearRegressionIf(toFloat64(l.tyre_age), l.lap_s, l.is_clean = 1).1,
        NULL), 4)                                  AS raw_slope_s_per_lap
FROM {{ ref('int_race_laps_clean') }} l
LEFT JOIN {{ ref('dim_races') }} r ON r.season = l.season AND r.round = l.round
WHERE l.stint_number IS NOT NULL
GROUP BY l.season, l.round, r.race_name, r.circuit_id, l.driver_id, stint_number

-- Grain: one row per driver per race (2023+).
-- Race pace = median of clean laps (see int_race_laps_clean). gap_to_best_s compares to the
-- best driver median in the race; team_gap_s compares to the team-mate's median.
{{ config(materialized='table', order_by='(season, round, driver_id)') }}

WITH drv AS (
    SELECT
        season, round, driver_id,
        any(constructor_id)                        AS constructor_id,
        countIf(is_clean = 1)                      AS clean_laps,
        count()                                    AS total_laps,
        medianIf(lap_s, is_clean = 1)              AS median_lap_s,
        avgIf(lap_s, is_clean = 1)                 AS mean_lap_s,
        stddevSampIf(lap_s, is_clean = 1)          AS lap_stddev_s,
        medianIf(fuel_corrected_lap_s, is_clean = 1) AS median_fuel_corrected_s
    FROM {{ ref('int_race_laps_clean') }}
    GROUP BY season, round, driver_id
    HAVING clean_laps >= 5
)
SELECT
    d.season                                       AS season,
    d.round                                        AS round,
    r.race_name                                    AS race_name,
    d.driver_id                                    AS driver_id,
    d.constructor_id                               AS constructor_id,
    d.clean_laps                                   AS clean_laps,
    d.total_laps                                   AS total_laps,
    round(d.median_lap_s, 3)                       AS median_lap_s,
    round(d.mean_lap_s, 3)                         AS mean_lap_s,
    round(d.lap_stddev_s, 3)                       AS consistency_stddev_s,
    round(d.median_fuel_corrected_s, 3)            AS median_fuel_corrected_s,
    round(d.median_lap_s - min(d.median_lap_s) OVER (PARTITION BY d.season, d.round), 3) AS gap_to_best_s,
    round(100 * d.median_lap_s / min(d.median_lap_s) OVER (PARTITION BY d.season, d.round), 3) AS pct_of_best,
    rank() OVER (PARTITION BY d.season, d.round ORDER BY d.median_lap_s) AS pace_rank,
    round(d.median_lap_s - (sum(d.median_lap_s) OVER (PARTITION BY d.season, d.round, d.constructor_id) - d.median_lap_s)
        / nullIf(count() OVER (PARTITION BY d.season, d.round, d.constructor_id) - 1, 0), 3) AS team_gap_s
FROM drv d
LEFT JOIN {{ ref('dim_races') }} r ON r.season = d.season AND r.round = d.round

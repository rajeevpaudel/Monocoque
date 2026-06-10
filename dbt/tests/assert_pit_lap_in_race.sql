{{ config(tags=["marts"]) }}
-- Fail if any pit stop's lap number exceeds the race winner's total laps.
SELECT
    ps.season,
    ps.round,
    ps.driver_id,
    ps.lap_number,
    race_laps.total_laps
FROM {{ ref('stg_jolpica__pit_stops') }} ps
JOIN (
    SELECT season, round, max(laps_completed) AS total_laps
    FROM {{ ref('fact_race_results') }}
    GROUP BY season, round
) race_laps
    ON  race_laps.season = ps.season
    AND race_laps.round  = ps.round
WHERE ps.lap_number > race_laps.total_laps

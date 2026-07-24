{{ config(tags=["marts"]) }}
-- Fail if any 2011+ race result (>10 laps completed) has no pit stop records.
-- Pre-2011 pit stop coverage in Jolpica is incomplete so we exclude those years.
SELECT fr.season, fr.round, fr.driver_id
FROM {{ ref('fact_race_results') }} fr
LEFT JOIN {{ ref('stg_jolpica__pit_stops') }} ps
    ON  ps.season    = fr.season
    AND ps.round     = fr.round
    AND ps.driver_id = fr.driver_id
WHERE fr.season >= 2011
  AND fr.laps_completed > 10
GROUP BY fr.season, fr.round, fr.driver_id
HAVING count(ps.stop_number) = 0

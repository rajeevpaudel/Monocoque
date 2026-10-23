{{ config(tags=["marts"]) }}
-- Fail if any 2011+ race has no pit stop records at all.
-- Checked per race, not per driver: a driver can legitimately finish without stopping
-- (e.g. free tyre change under a red flag, Monaco 2024).
-- Pre-2011 pit stop coverage in Jolpica is incomplete so we exclude those years.
SELECT fr.season, fr.round
FROM {{ ref('fact_race_results') }} fr
LEFT JOIN {{ ref('stg_jolpica__pit_stops') }} ps
    ON  ps.season    = fr.season
    AND ps.round     = fr.round
    AND ps.driver_id = fr.driver_id
WHERE fr.season >= 2011
GROUP BY fr.season, fr.round
HAVING count(ps.stop_number) = 0
   AND max(fr.laps_completed) > 10  -- skip non-races, e.g. Spa 2021 (safety car only)

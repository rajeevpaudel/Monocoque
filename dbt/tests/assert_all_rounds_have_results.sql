-- Fail if any past race round has no rows in fact_race_results.
SELECT r.season, r.round
FROM {{ ref('stg_jolpica__races') }} r
LEFT JOIN {{ ref('fact_race_results') }} fr
    ON  fr.season = r.season
    AND fr.round  = r.round
WHERE r.race_date <= today()
  AND fr.driver_id IS NULL

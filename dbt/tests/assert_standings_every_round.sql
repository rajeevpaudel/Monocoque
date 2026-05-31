-- Fail if any completed race round (2000+) has no driver standings data.
SELECT r.season, r.round
FROM {{ ref('stg_jolpica__races') }} r
LEFT JOIN {{ ref('stg_jolpica__driver_standings') }} ds
    ON  ds.season = r.season
    AND ds.round  = r.round
WHERE r.race_date <= today()
  AND r.season >= 2000
  AND ds.driver_id IS NULL
GROUP BY r.season, r.round

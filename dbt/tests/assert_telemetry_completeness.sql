-- Fail if any matched driver in mart_qualifying_summary has no telemetry rows.
SELECT mqs.season, mqs.round, mqs.driver_id
FROM {{ ref('mart_qualifying_summary') }} mqs
LEFT JOIN {{ ref('mart_lap_telemetry') }} mlt
    ON  mlt.session_key   = mqs.session_key
    AND mlt.driver_number = mqs.openf1_driver_number
WHERE mqs.best_source_match = 'matched'
GROUP BY mqs.season, mqs.round, mqs.driver_id, mqs.session_key, mqs.openf1_driver_number
HAVING count(mlt.session_key) = 0

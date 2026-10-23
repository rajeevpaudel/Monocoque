{{ config(tags=["marts"]) }}
-- Fail if a matched driver has no telemetry in a qualifying session whose telemetry WAS
-- ingested. Telemetry is optional (--skip-telemetry), so sessions without any car_data
-- are out of scope; this catches drivers silently dropped within an ingested session.
SELECT mqs.season, mqs.round, mqs.driver_id
FROM {{ ref('mart_qualifying_summary') }} mqs
LEFT JOIN {{ ref('mart_lap_telemetry') }} mlt
    ON  mlt.session_key   = mqs.session_key
    AND mlt.driver_number = mqs.openf1_driver_number
WHERE mqs.best_source_match = 'matched'
  AND mqs.session_key IN (SELECT DISTINCT session_key FROM {{ ref('mart_lap_telemetry') }})
GROUP BY mqs.season, mqs.round, mqs.driver_id, mqs.session_key, mqs.openf1_driver_number
HAVING count(mlt.session_key) = 0

-- Fail if any Jolpica lap time is outside the physically plausible 60s–600s window.
-- 600s (10 min) accommodates red-flag restart laps where the clock keeps running;
-- <60s is impossible on any F1 circuit. Pre-2004 data is excluded due to known
-- timing anomalies in the Jolpica historical dataset for those seasons.
SELECT season, round, driver_id, lap_number, lap_time_ms
FROM {{ ref('stg_jolpica__laps') }}
WHERE season >= 2004
  AND lap_time_ms > 0
  AND (lap_time_ms < 60000 OR lap_time_ms > 600000)

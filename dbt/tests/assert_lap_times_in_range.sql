-- Fail if any Jolpica lap time is outside the physically plausible 60s–7200s window.
-- Upper bound (2 h) catches true data corruption (e.g. timestamps stored in wrong units)
-- while accommodating red-flag restart laps where the Jolpica clock keeps running.
-- Lower bound: <60s is impossible on any F1 circuit.
-- Pre-2004 data is excluded due to known timing anomalies in the Jolpica historical dataset.
SELECT season, round, driver_id, lap_number, lap_time_ms
FROM {{ ref('stg_jolpica__laps') }}
WHERE season >= 2004
  AND lap_time_ms > 0
  AND (lap_time_ms < 60000 OR lap_time_ms > 7200000)

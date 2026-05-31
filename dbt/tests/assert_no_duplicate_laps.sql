-- Fail if any (session_key, driver_number, lap_number) tuple appears more than once.
SELECT session_key, driver_number, lap_number, count() AS cnt
FROM {{ ref('stg_openf1__laps') }}
GROUP BY session_key, driver_number, lap_number
HAVING cnt > 1

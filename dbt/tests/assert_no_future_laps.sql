-- Fail if any OpenF1 lap has a start timestamp after today (ingestion timestamp corruption).
SELECT session_key, driver_number, lap_number, lap_start
FROM {{ ref('stg_openf1__laps') }}
WHERE lap_start > now()

{{ config(severity='warn') }}
-- Warn if any session's average car_data sample rate is outside 3.5–4.0 Hz.
-- OpenF1 advertises ~3.7 Hz; warn if a session deviates significantly.
WITH session_hz AS (
    SELECT
        session_key,
        driver_number,
        count()                                                             AS sample_count,
        dateDiff('second', min(date), max(date))                           AS duration_seconds,
        if(duration_seconds > 0, sample_count / duration_seconds, 0)      AS hz
    FROM {{ ref('stg_openf1__car_data') }}
    GROUP BY session_key, driver_number
    HAVING duration_seconds > 60  -- only measure sessions with enough data
)
SELECT session_key, driver_number, round(hz, 2) AS hz
FROM session_hz
WHERE hz < 3.5 OR hz > 4.0

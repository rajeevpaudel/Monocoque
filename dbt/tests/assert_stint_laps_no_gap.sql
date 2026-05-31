{{ config(severity='warn') }}
-- Fail if any driver's stints don't continuously cover all laps (gap or overlap detected).
-- A gap: sum of covered laps < range from first to last lap.
-- An overlap: sum of covered laps > range from first to last lap.
SELECT
    session_key,
    driver_number,
    min(lap_start)                           AS first_lap,
    max(lap_end)                             AS last_lap,
    sum(lap_end - lap_start + 1)             AS covered_laps,
    max(lap_end) - min(lap_start) + 1        AS expected_covered
FROM {{ ref('stg_openf1__stints') }}
WHERE lap_start IS NOT NULL
  AND lap_end   IS NOT NULL
GROUP BY session_key, driver_number
HAVING covered_laps != expected_covered

-- Fail if any driver's race stints have a gap (laps not covered between first and last lap).
-- Overlaps are expected: pit-in lap is lap_end of the old stint and the next lap starts the new stint.
-- Practice/Qualifying excluded because lap numbers reset per stint in those sessions.
SELECT
    s.session_key,
    s.driver_number,
    min(s.lap_start)                           AS first_lap,
    max(s.lap_end)                             AS last_lap,
    sum(s.lap_end - s.lap_start + 1)           AS covered_laps,
    max(s.lap_end) - min(s.lap_start) + 1      AS expected_covered
FROM {{ ref('stg_openf1__stints') }} s
JOIN {{ ref('stg_openf1__sessions') }} sess ON sess.session_key = s.session_key
WHERE s.lap_start IS NOT NULL
  AND s.lap_end   IS NOT NULL
  AND sess.session_type = 'Race'
GROUP BY s.session_key, s.driver_number
HAVING covered_laps < expected_covered

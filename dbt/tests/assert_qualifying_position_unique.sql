-- Fail if two drivers share the same qualifying position within a session.
-- Only Q3 finalists (drivers with a Q3 time set) are checked; tied Q1/Q2
-- eliminations are valid and expected in the Jolpica source data.
SELECT season, round, qualifying_position, count() AS driver_count
FROM {{ ref('fact_qualifying') }}
WHERE qualifying_position IS NOT NULL
  AND q3 IS NOT NULL
GROUP BY season, round, qualifying_position
HAVING driver_count > 1

-- Fail if two drivers share the same qualifying position within a session.
SELECT season, round, qualifying_position, count() AS driver_count
FROM {{ ref('fact_qualifying') }}
WHERE qualifying_position IS NOT NULL
GROUP BY season, round, qualifying_position
HAVING driver_count > 1

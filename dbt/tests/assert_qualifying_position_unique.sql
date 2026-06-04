{{ config(severity='warn') }}
-- Warn if two drivers share the same qualifying position within a session.
-- Only Q3 finalists (drivers with a Q3 time set) are checked; tied Q1/Q2
-- eliminations are valid and expected in the Jolpica source data.
-- Uses q3 != '' instead of IS NOT NULL because fact_qualifying.q3 is a
-- non-nullable String in ClickHouse — empty string is not NULL.
-- Known data: Monaco 2024 has Alonso/Hulkenberg both at P14 and
-- Magnussen/Sargeant at P15 due to FIA lap deletion collapsing positions.
SELECT season, round, qualifying_position, count() AS driver_count
FROM {{ ref('fact_qualifying') }}
WHERE qualifying_position IS NOT NULL
  AND q3 != ''
GROUP BY season, round, qualifying_position
HAVING driver_count > 1

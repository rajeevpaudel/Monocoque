{{ config(tags=["marts"]) }}
-- Fail if any driver scores more than 26 points in a race (max: 25 + 1 fastest lap)
-- or more than 8 points in a sprint (max: 8 for first place).
SELECT 'race' AS session_type, season, round, driver_id, points
FROM {{ ref('fact_race_results') }}
WHERE points > 26
UNION ALL
SELECT 'sprint', season, round, driver_id, points
FROM {{ ref('fact_sprint_results') }}
WHERE points > 8

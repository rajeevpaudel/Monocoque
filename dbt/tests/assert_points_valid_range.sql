{{ config(tags=["marts"]) }}
-- Fail if any driver scores more than 50 points in a race or more than 8 in a sprint.
-- Race upper bound of 50: the 2014 Abu Dhabi GP used double-points rules (1st = 50,
-- 2nd = 36, 3rd = 30); no other race in F1 history has exceeded 50.
-- Normal modern max is 26 (25 for win + 1 fastest lap).
-- Sprint upper bound of 8: max sprint points awarded to first place.
SELECT 'race' AS session_type, season, round, driver_id, points
FROM {{ ref('fact_race_results') }}
WHERE points > 50
UNION ALL
SELECT 'sprint', season, round, driver_id, points
FROM {{ ref('fact_sprint_results') }}
WHERE points > 8

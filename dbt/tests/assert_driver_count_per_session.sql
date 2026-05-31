-- Fail if any qualifying session (2010+) has fewer than 18 or more than 20 drivers.
-- 2010 is when the grid was standardised at 20 cars.
SELECT season, round, count() AS driver_count
FROM {{ ref('fact_qualifying') }}
WHERE season >= 2010
GROUP BY season, round
HAVING driver_count < 18 OR driver_count > 20

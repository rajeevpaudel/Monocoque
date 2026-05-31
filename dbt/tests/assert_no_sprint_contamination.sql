-- Fail if any qualifying round has more than 20 rows (sprint session contamination).
-- Scoped to 2010+ when the grid was standardised at 20 cars.
SELECT season, round, count() AS row_count
FROM {{ ref('mart_qualifying_summary') }}
WHERE season >= 2010
GROUP BY season, round
HAVING row_count > 20

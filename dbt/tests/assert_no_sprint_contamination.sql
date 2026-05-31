-- Fail if any qualifying round has more than 20 rows (sprint session contamination).
SELECT season, round, count() AS row_count
FROM {{ ref('mart_qualifying_summary') }}
GROUP BY season, round
HAVING row_count > 20

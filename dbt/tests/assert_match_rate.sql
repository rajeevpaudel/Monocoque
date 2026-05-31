-- Fail if any 2023+ qualifying round has more than 3 driver mismatches.
SELECT season, round, countIf(best_source_match = 'mismatch') AS mismatch_count
FROM {{ ref('mart_qualifying_summary') }}
WHERE season >= 2023
GROUP BY season, round
HAVING mismatch_count > 3

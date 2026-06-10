-- Warn if any 2023+ qualifying round has more than 3 driver mismatches.
-- Severity is warn because some rounds have complete OpenF1 lap data gaps (e.g. Baku 2025, Las Vegas 2024).
{{ config(tags=["marts"], severity='warn') }}
SELECT season, round, countIf(best_source_match = 'mismatch') AS mismatch_count
FROM {{ ref('mart_qualifying_summary') }}
WHERE season >= 2023
GROUP BY season, round
HAVING mismatch_count > 3

{{ config(tags=["marts"]) }}
-- Fail if sector times don't sum to within ±0.2s of the matched lap duration.
-- NULL sector times (pre-2023 or incomplete laps) are excluded.
SELECT season, round, driver_id,
    best_lap_duration,
    best_s1 + best_s2 + best_s3 AS sector_sum
FROM {{ ref('mart_qualifying_summary') }}
WHERE best_source_match = 'matched'
  AND best_s1          IS NOT NULL
  AND best_s2          IS NOT NULL
  AND best_s3          IS NOT NULL
  AND best_lap_duration IS NOT NULL
  AND abs((best_s1 + best_s2 + best_s3) - best_lap_duration) > 0.2

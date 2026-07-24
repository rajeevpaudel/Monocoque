{{ config(tags=["marts"]) }}
-- Fail if two drivers share the same classified finishing position in a race.
-- Only rows with a numeric position_text (classified finishers) are checked.
-- Scope: season >= 1979. Pre-1979 F1 permitted shared drives (two drivers sharing one car);
-- f1db correctly records both drivers at the same finishing position for such entries.
SELECT season, round, finish_position, count() AS driver_count
FROM {{ ref('fact_race_results') }}
WHERE toUInt8OrZero(position_text) > 0
  AND season >= 1979
GROUP BY season, round, finish_position
HAVING driver_count > 1

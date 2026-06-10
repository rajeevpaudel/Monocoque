{{ config(tags=["marts"]) }}
-- Fail if two drivers share the same classified finishing position in a race.
-- Only rows with a numeric position_text (classified finishers) are checked.
SELECT season, round, finish_position, count() AS driver_count
FROM {{ ref('fact_race_results') }}
WHERE toUInt8OrZero(position_text) > 0
GROUP BY season, round, finish_position
HAVING driver_count > 1

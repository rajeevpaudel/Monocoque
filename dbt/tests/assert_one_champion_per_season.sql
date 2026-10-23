{{ config(tags=["marts"]) }}
-- Fail if a completed season does not have exactly one drivers' champion
-- (guards against argMax skipping NULL final positions, e.g. Schumacher's 1997 DSQ).
SELECT season, countIf(is_champion = 1) AS champions
FROM {{ ref('mart_driver_season_standings') }}
WHERE is_season_complete = 1
GROUP BY season
HAVING champions != 1

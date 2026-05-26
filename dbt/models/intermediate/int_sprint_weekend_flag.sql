-- Flags rounds that include a sprint race.
SELECT
    season,
    round,
    1 AS has_sprint
FROM {{ ref('stg_jolpica__sprint_results') }}
GROUP BY season, round

{{ config(engine='MergeTree()', order_by=['season', 'session_type', 'position']) }}

-- Championship points per finishing position: one row per (season, session_type, position).
-- Expanded from the era-level points_systems / points_system_rules seeds. Open-ended eras
-- run to next season so upcoming races already resolve.
WITH
    toUInt16(toYear(today()) + 1) AS max_season,

points AS (
    SELECT
        arrayJoin(range(season_from, coalesce(season_to, max_season) + 1)) AS season,
        session_type,
        position,
        points,
        concat(toString(season_from), '-', ifNull(toString(season_to), 'present')) AS era
    FROM {{ ref('points_systems') }}
),

rules AS (
    SELECT
        arrayJoin(range(season_from, coalesce(season_to, max_season) + 1)) AS season,
        fastest_lap_points,
        fastest_lap_max_position,
        notes
    FROM {{ ref('points_system_rules') }}
)

SELECT
    toUInt16(p.season)              AS season,
    p.session_type                  AS session_type,
    p.position                      AS position,
    p.points                        AS points,
    p.era                           AS era,
    -- Fastest-lap bonus applies to the race only.
    if(p.session_type = 'race', r.fastest_lap_points, toUInt8(0)) AS fastest_lap_points,
    r.fastest_lap_max_position      AS fastest_lap_max_position,
    r.notes                         AS notes
FROM points p
LEFT JOIN rules r ON r.season = p.season

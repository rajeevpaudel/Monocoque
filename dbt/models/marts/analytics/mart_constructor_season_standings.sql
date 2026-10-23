-- is_champion is only set once every round of the season is completed.
-- Grain: one row per constructor per season (all eras; constructors' title from 1958).
{{ config(materialized='table', order_by='(season, constructor_id)') }}

WITH final_standings AS (
    SELECT
        season,
        constructor_id,
        -- tuple argMax keeps a NULL final position (e.g. McLaren 2007, excluded) — see drivers mart.
        argMax((standing_position, points, position_text), round) AS final_row,
        final_row.1                                AS championship_position,
        final_row.2                                AS championship_points,
        final_row.3                                AS championship_position_text
    FROM {{ ref('stg_jolpica__constructor_standings') }}
    GROUP BY season, constructor_id
),
races AS (
    SELECT
        season,
        constructor_id,
        uniqExact(round)                           AS races_entered,
        countIf(finish_position = 1)               AS wins,
        countIf(finish_position <= 3)              AS podiums,
        countIf(finish_position <= 2) - countIf(finish_position = 1) AS second_places,
        countIf(grid_position = 1)                 AS poles,
        countIf(position_text IN ('R', 'DNF'))     AS dnfs,
        sum(points)                                AS race_points,
        groupUniqArray(driver_id)                  AS drivers
    FROM {{ ref('fact_race_results') }}
    GROUP BY season, constructor_id
),
one_twos AS (
    SELECT season, constructor_id, count() AS one_two_finishes
    FROM (
        SELECT season, round, constructor_id
        FROM {{ ref('fact_race_results') }}
        WHERE finish_position IN (1, 2)
        GROUP BY season, round, constructor_id
        HAVING count() = 2
    )
    GROUP BY season, constructor_id
),
sprints AS (
    SELECT season, constructor_id, sum(points) AS sprint_points
    FROM {{ ref('fact_sprint_results') }}
    GROUP BY season, constructor_id
)
SELECT
    r.season                                       AS season,
    r.constructor_id                               AS constructor_id,
    c.constructor_name                             AS constructor_name,
    c.nationality                                  AS nationality,
    fs.championship_position                       AS championship_position,
    fs.championship_position_text                  AS championship_position_text,
    toUInt8(ifNull(cal.rounds_remaining, 0) = 0)   AS is_season_complete,
    coalesce(fs.championship_points, r.race_points + ifNull(s.sprint_points, 0)) AS points,
    r.races_entered                                AS races_entered,
    r.wins                                         AS wins,
    r.podiums                                      AS podiums,
    r.poles                                        AS poles,
    ifNull(o.one_two_finishes, 0)                  AS one_two_finishes,
    r.dnfs                                         AS dnfs,
    r.race_points                                  AS race_points,
    ifNull(s.sprint_points, 0)                     AS sprint_points,
    arraySort(r.drivers)                           AS drivers,
    length(r.drivers)                              AS driver_count,
    toUInt8(fs.championship_position = 1 AND is_season_complete = 1) AS is_champion
FROM races r
LEFT JOIN final_standings fs ON fs.season = r.season AND fs.constructor_id = r.constructor_id
LEFT JOIN sprints s          ON s.season = r.season  AND s.constructor_id = r.constructor_id
LEFT JOIN (
    SELECT season, countIf(status != 'completed') AS rounds_remaining
    FROM {{ ref('dim_calendar') }} GROUP BY season
) cal ON cal.season = r.season
LEFT JOIN one_twos o         ON o.season = r.season  AND o.constructor_id = r.constructor_id
LEFT JOIN (
    SELECT constructor_id, any(constructor_name) AS constructor_name, any(nationality) AS nationality
    FROM {{ ref('stg_jolpica__constructors') }} GROUP BY constructor_id
) c ON c.constructor_id = r.constructor_id

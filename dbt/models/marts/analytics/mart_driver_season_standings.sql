-- is_champion is only set once every round of the season is completed.
-- Grain: one row per driver per season (all eras).
-- Final championship position/points from the last standings round, plus season stats
-- derived from race and sprint results. constructors lists every team driven for, in order,
-- so mid-season swaps are visible.
{{ config(materialized='table', order_by='(season, driver_id)') }}

WITH final_standings AS (
    SELECT
        season,
        driver_id,
        -- tuple argMax: plain argMax skips NULLs, which would resurrect the previous round's
        -- position for drivers excluded from the final standings (Schumacher 1997 = DSQ).
        argMax((standing_position, points, wins, position_text), round) AS final_row,
        final_row.1                                AS championship_position,
        final_row.2                                AS championship_points,
        final_row.3                                AS championship_wins,
        final_row.4                                AS championship_position_text
    FROM {{ ref('stg_jolpica__driver_standings') }}
    GROUP BY season, driver_id
),
races AS (
    SELECT
        season,
        driver_id,
        countIf(position_text NOT IN ('W', 'F', 'DNQ', 'DNPQ', 'DNS', 'EX', 'E')) AS starts,
        countIf(finish_position = 1)               AS wins,
        countIf(finish_position <= 3)              AS podiums,
        countIf(grid_position = 1)                 AS poles,
        countIf(fastest_lap_rank = 1)              AS fastest_laps,
        countIf(position_text IN ('R', 'DNF'))     AS dnfs,
        countIf(position_text IN ('D', 'DSQ'))     AS disqualifications,
        countIf(points > 0)                        AS points_finishes,
        sum(points)                                AS race_points,
        avgIf(finish_position, match(position_text, '^[0-9]+$')) AS avg_finish_position,
        avgIf(grid_position, grid_position > 0)    AS avg_grid_position,
        sum(ifNull(laps_completed, 0))             AS laps_completed,
        arrayMap(x -> x.2, arraySort(groupArray((round, constructor_id)))) AS constructor_by_round
    FROM {{ ref('fact_race_results') }}
    GROUP BY season, driver_id
),
sprints AS (
    SELECT season, driver_id, count() AS sprint_starts, countIf(finish_position = 1) AS sprint_wins,
           sum(points) AS sprint_points
    FROM {{ ref('fact_sprint_results') }}
    GROUP BY season, driver_id
)
SELECT
    r.season                                       AS season,
    r.driver_id                                    AS driver_id,
    d.full_name                                    AS driver_name,
    d.nationality                                  AS nationality,
    arrayDistinct(r.constructor_by_round)          AS constructors,
    r.constructor_by_round[-1]                     AS final_constructor_id,
    fs.championship_position                       AS championship_position,
    fs.championship_position_text                  AS championship_position_text,
    toUInt8(ifNull(cal.rounds_remaining, 0) = 0)   AS is_season_complete,
    coalesce(fs.championship_points, r.race_points + ifNull(s.sprint_points, 0)) AS points,
    r.starts                                       AS starts,
    r.wins                                         AS wins,
    r.podiums                                      AS podiums,
    r.poles                                        AS poles,
    r.fastest_laps                                 AS fastest_laps,
    r.dnfs                                         AS dnfs,
    r.disqualifications                            AS disqualifications,
    r.points_finishes                              AS points_finishes,
    r.race_points                                  AS race_points,
    ifNull(s.sprint_points, 0)                     AS sprint_points,
    ifNull(s.sprint_starts, 0)                     AS sprint_starts,
    ifNull(s.sprint_wins, 0)                       AS sprint_wins,
    round(r.avg_finish_position, 2)                AS avg_finish_position,
    round(r.avg_grid_position, 2)                  AS avg_grid_position,
    r.laps_completed                               AS laps_completed,
    round(points / nullIf(r.starts, 0), 2)         AS points_per_start,
    toUInt8(ifNull(fs.championship_position = 1, 0) AND is_season_complete = 1) AS is_champion
FROM races r
LEFT JOIN final_standings fs ON fs.season = r.season AND fs.driver_id = r.driver_id
LEFT JOIN sprints s          ON s.season = r.season  AND s.driver_id = r.driver_id
LEFT JOIN (
    SELECT season, countIf(status != 'completed') AS rounds_remaining
    FROM {{ ref('dim_calendar') }} GROUP BY season
) cal ON cal.season = r.season
LEFT JOIN (
    SELECT driver_id, any(full_name) AS full_name, any(nationality) AS nationality
    FROM {{ ref('stg_jolpica__drivers') }} GROUP BY driver_id
) d ON d.driver_id = r.driver_id

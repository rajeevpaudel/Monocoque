-- Grain: one row per (season, constructor, driver, teammate) — both directions present.
-- Pairs are formed per race, so mid-season swaps (e.g. Lawson/Tsunoda 2025) produce
-- separate pairings counted only over the races they actually shared.
-- Qualifying H2H uses qualifying_position; race H2H counts only races where at least one
-- of the pair was classified (the classified driver wins if the other retired).
{{ config(materialized='table', order_by='(season, constructor_id, driver_id, teammate_id)') }}

WITH res AS (
    SELECT
        season, round, constructor_id, driver_id, finish_position, grid_position, points,
        match(position_text, '^[0-9]+$')           AS classified
    FROM {{ ref('fact_race_results') }}
),
quali AS (
    SELECT season, round, driver_id, qualifying_position
    FROM {{ ref('fact_qualifying') }}
),
pairs AS (
    SELECT
        a.season                                   AS season,
        a.round                                    AS round,
        a.constructor_id                           AS constructor_id,
        a.driver_id                                AS driver_id,
        b.driver_id                                AS teammate_id,
        coalesce(qa.qualifying_position, nullIf(a.grid_position, 0)) AS q_a,
        coalesce(qb.qualifying_position, nullIf(b.grid_position, 0)) AS q_b,
        a.classified                               AS cls_a,
        b.classified                               AS cls_b,
        a.finish_position                          AS f_a,
        b.finish_position                          AS f_b,
        a.points                                   AS pts_a,
        b.points                                   AS pts_b
    FROM res a
    JOIN res b
        ON  b.season = a.season AND b.round = a.round
        AND b.constructor_id = a.constructor_id
    LEFT JOIN quali qa ON qa.season = a.season AND qa.round = a.round AND qa.driver_id = a.driver_id
    LEFT JOIN quali qb ON qb.season = b.season AND qb.round = b.round AND qb.driver_id = b.driver_id
    WHERE a.driver_id != b.driver_id
)
SELECT
    season,
    constructor_id,
    driver_id,
    teammate_id,
    count()                                        AS races_together,
    min(round)                                     AS first_round,
    max(round)                                     AS last_round,
    countIf(q_a < q_b)                             AS quali_wins,
    countIf(q_a > q_b)                             AS quali_losses,
    countIf((cls_a AND NOT cls_b) OR (cls_a AND cls_b AND f_a < f_b)) AS race_wins,
    countIf((cls_b AND NOT cls_a) OR (cls_a AND cls_b AND f_a > f_b)) AS race_losses,
    sum(pts_a)                                     AS points,
    sum(pts_b)                                     AS teammate_points,
    round(avgIf(toInt16(q_a) - toInt16(q_b), q_a IS NOT NULL AND q_b IS NOT NULL), 2) AS avg_quali_position_delta,
    round(quali_wins / nullIf(quali_wins + quali_losses, 0), 3) AS quali_win_rate,
    round(race_wins / nullIf(race_wins + race_losses, 0), 3)   AS race_win_rate
FROM pairs
GROUP BY season, constructor_id, driver_id, teammate_id

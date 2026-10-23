{{ config(severity='warn') }}
-- Warn if a constructor's cumulative standings points differ from the cumulative sum of the
-- race + sprint result points scored in its cars, per round.
-- Results are attributed by each result row's constructor_id, so mid-season driver swaps
-- (e.g. Lawson/Tsunoda between rb and red_bull in 2025) are handled correctly; summing
-- driver standings would credit all of a driver's points to their latest team.
-- Before 1979 only a team's best-placed car scored constructors' points, so those seasons
-- are skipped. Known regulatory exceptions are listed below.
WITH result_points AS (
    SELECT season, round, constructor_id, sum(points) AS round_points
    FROM (
        SELECT season, round, constructor_id, points FROM {{ ref('fact_race_results') }}
        UNION ALL
        SELECT season, round, constructor_id, points FROM {{ ref('fact_sprint_results') }}
    )
    GROUP BY season, round, constructor_id
),
cumulative AS (
    SELECT
        season, round, constructor_id,
        sum(round_points) OVER (PARTITION BY season, constructor_id ORDER BY round) AS result_points
    FROM result_points
),
-- Cumulative comparison is only valid where every earlier round has results (partial loads,
-- e.g. the CI sample with only rounds 1 and 5, would otherwise false-alarm).
contiguous_rounds AS (
    SELECT season, round
    FROM (
        SELECT season, round, row_number() OVER (PARTITION BY season ORDER BY round) AS rn
        FROM (SELECT DISTINCT season, round FROM {{ ref('fact_race_results') }})
    )
    WHERE rn = round
),
exceptions AS (
    SELECT * FROM VALUES('season UInt16, constructor_id String',
        (1982, 'brabham'),       -- BMW- and Ford-engined cars scored as separate constructors
        (1983, 'team_lotus'),    -- Renault/Ford split
        (1983, 'williams'),      -- Ford/Honda split
        (1984, 'arrows'),        -- BMW/Ford split
        (1985, 'tyrrell'),       -- Renault/Ford split
        (1995, 'williams'),      -- Brazil fuel irregularity: drivers kept points, team did not
        (1995, 'benetton'),      -- Brazil fuel irregularity
        (2000, 'mclaren'),       -- Austria: missing FIA seal on Häkkinen's ECU
        (2007, 'mclaren'),       -- excluded from the constructors' championship
        (2018, 'force_india'),   -- Racing Point Force India re-entered from Belgium with zero points
        (2020, 'racing_point')   -- 15-point deduction (brake ducts)
    )
)
SELECT
    cs.season                                      AS season,
    cs.round                                       AS round,
    cs.constructor_id                              AS constructor_id,
    cs.points                                      AS standings_points,
    c.result_points                                AS result_points
FROM {{ ref('stg_jolpica__constructor_standings') }} cs
JOIN cumulative c
    ON c.season = cs.season AND c.round = cs.round AND c.constructor_id = cs.constructor_id
JOIN contiguous_rounds cr ON cr.season = cs.season AND cr.round = cs.round
WHERE cs.season >= 1979
  AND (cs.season, cs.constructor_id) NOT IN (SELECT season, constructor_id FROM exceptions)
  AND abs(cs.points - c.result_points) > 0.01

{{ config(engine='MergeTree()', order_by=['driver_id', 'stint_number']) }}

-- Driver–team stints ("contracts"): one row per unbroken run of race entries for the
-- same constructor, in chronological order across seasons. Mid-season swaps
-- (e.g. Lawson/Tsunoda 2025) and one-off stand-ins (Bearman, Ferrari 2024) become
-- their own stints instead of being collapsed to one team per season as in dim_drivers.
WITH entries AS (
    SELECT
        r.driver_id                                         AS driver_id,
        r.constructor_id                                    AS constructor_id,
        r.season                                            AS season,
        r.round                                             AS round,
        toUInt32(r.season * 100 + r.round)                  AS race_sk,
        ra.race_date                                        AS race_date,
        r.points                                            AS points
    FROM {{ ref('stg_jolpica__results') }} r
    INNER JOIN {{ ref('stg_jolpica__races') }} ra
        ON ra.season = r.season AND ra.round = r.round
),

flagged AS (
    SELECT
        *,
        if(constructor_id != lagInFrame(constructor_id, 1, '') OVER w, 1, 0) AS is_new_stint
    FROM entries
    WINDOW w AS (PARTITION BY driver_id ORDER BY race_sk ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),

numbered AS (
    SELECT
        *,
        sum(is_new_stint) OVER (
            PARTITION BY driver_id ORDER BY race_sk ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        )                                                   AS stint_number
    FROM flagged
),

stints AS (
    SELECT
        driver_id,
        toUInt16(stint_number)                              AS stint_number,
        any(constructor_id)                                 AS constructor_id,
        min(race_sk)                                        AS first_race_sk,
        max(race_sk)                                        AS last_race_sk,
        min(race_date)                                      AS start_date,
        max(race_date)                                      AS end_date,
        toUInt16(count())                                   AS races_entered,
        toUInt16(uniqExact(season))                         AS seasons_spanned,
        sum(points)                                         AS points_scored
    FROM numbered
    GROUP BY driver_id, stint_number
),

season_bounds AS (
    SELECT season, min(round) AS first_round, max(round) AS last_round
    FROM {{ ref('stg_jolpica__races') }}
    GROUP BY season
),

latest_race AS (
    SELECT max(toUInt32(season * 100 + round)) AS race_sk FROM {{ ref('stg_jolpica__results') }}
)

SELECT
    s.driver_id                                             AS driver_id,
    d.full_name                                             AS driver_name,
    s.stint_number                                          AS stint_number,
    s.constructor_id                                        AS constructor_id,
    c.constructor_name                                      AS constructor_name,
    toUInt16(intDiv(s.first_race_sk, 100))                  AS start_season,
    toUInt8(s.first_race_sk % 100)                          AS start_round,
    toUInt16(intDiv(s.last_race_sk, 100))                   AS end_season,
    toUInt8(s.last_race_sk % 100)                           AS end_round,
    s.start_date                                            AS start_date,
    s.end_date                                              AS end_date,
    s.races_entered                                         AS races_entered,
    s.seasons_spanned                                       AS seasons_spanned,
    s.points_scored                                         AS points_scored,
    toUInt8(toUInt8(s.first_race_sk % 100) > bs.first_round) AS started_mid_season,
    toUInt8(toUInt8(s.last_race_sk % 100) < be.last_round
            AND s.last_race_sk != lr.race_sk)               AS ended_mid_season,
    multiIf(
        s.races_entered = 1,                    'one_off',
        s.races_entered <= 5,                   'short_term',
        toUInt8(s.first_race_sk % 100) > bs.first_round
            OR (toUInt8(s.last_race_sk % 100) < be.last_round AND s.last_race_sk != lr.race_sk),
                                                'mid_season',
        'full_season'
    )                                                       AS stint_type,
    toUInt8(s.last_race_sk = lr.race_sk)                    AS is_current
FROM stints s
CROSS JOIN latest_race lr
LEFT JOIN season_bounds bs ON bs.season = intDiv(s.first_race_sk, 100)
LEFT JOIN season_bounds be ON be.season = intDiv(s.last_race_sk, 100)
LEFT JOIN (
    SELECT driver_id, any(full_name) AS full_name FROM {{ ref('dim_drivers') }} GROUP BY driver_id
) d ON d.driver_id = s.driver_id
LEFT JOIN (
    SELECT constructor_id, any(constructor_name) AS constructor_name FROM {{ ref('dim_constructors') }} GROUP BY constructor_id
) c ON c.constructor_id = s.constructor_id

-- Championship standings snapshot per round (latest version via FINAL on source).
-- constructor_id is the driver's race team as of that round (latest race entry up to the round).
-- Jolpica's standings Constructors list is not reliable for this: it can include a team before
-- the driver has raced for it (Bearman 2024 shows haas from r7, first Haas race was r17).
WITH race_team AS (
    SELECT
        ds.season                                   AS rt_season,
        ds.round                                    AS rt_round,
        ds.driver_id                                AS rt_driver_id,
        argMax(r.constructor_id, r.round)           AS rt_constructor_id
    FROM {{ ref('stg_jolpica__driver_standings') }} ds
    INNER JOIN {{ ref('stg_jolpica__results') }}    r
        ON  r.season    = ds.season
        AND r.driver_id = ds.driver_id
    WHERE r.round <= ds.round
      AND ds._source = 'jolpica'
    GROUP BY ds.season, ds.round, ds.driver_id
),

standings AS (
    SELECT
        ds.season                                                       AS season,
        ds.round                                                        AS round,
        ds.driver_id                                                    AS driver_id,
        -- f1db-sourced seasons carry no constructor; keep them blank (their constructor
        -- standings have several rows per constructor_id, e.g. engine variants, and would fan out).
        if(ds._source != 'jolpica' OR rt.rt_constructor_id IS NULL OR rt.rt_constructor_id = '',
           ds.constructor_id, rt.rt_constructor_id)                     AS constructor_id,
        ds.standing_position                                            AS standing_position,
        ds.points                                                       AS points,
        ds.wins                                                         AS wins
    FROM {{ ref('stg_jolpica__driver_standings') }} ds
    LEFT JOIN race_team rt
        ON  rt.rt_season    = ds.season
        AND rt.rt_round     = ds.round
        AND rt.rt_driver_id = ds.driver_id
)

SELECT
    s.season                AS season,
    s.round                 AS round,
    s.driver_id             AS driver_id,
    d.full_name             AS driver_name,
    s.constructor_id        AS constructor_id,
    c.constructor_name,
    s.standing_position     AS standing_position,
    s.points                AS points,
    s.wins                  AS wins,
    cs.standing_position    AS constructor_position,
    cs.points               AS constructor_points
FROM standings s
LEFT JOIN {{ ref('dim_drivers') }}          AS d FINAL
    ON  d.driver_id = s.driver_id
    AND d.season    = s.season
LEFT JOIN {{ ref('stg_jolpica__constructor_standings')}} cs
    ON  cs.season          = s.season
    AND cs.round           = s.round
    AND cs.constructor_id  = s.constructor_id
LEFT JOIN {{ ref('dim_constructors') }}     AS c FINAL
    ON  c.constructor_id = s.constructor_id
    AND c.season         = s.season

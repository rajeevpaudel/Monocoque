-- Championship standings snapshot per round (latest version via FINAL on source).
SELECT
    ds.season               AS season,
    ds.round                AS round,
    ds.driver_id            AS driver_id,
    d.full_name             AS driver_name,
    ds.constructor_id       AS constructor_id,
    c.constructor_name,
    ds.standing_position    AS standing_position,
    ds.points               AS points,
    ds.wins                 AS wins,
    cs.standing_position    AS constructor_position,
    cs.points               AS constructor_points
FROM {{ ref('stg_jolpica__driver_standings') }}         ds
LEFT JOIN {{ ref('dim_drivers') }}                      d
    ON  d.driver_id = ds.driver_id
    AND d.season    = ds.season
LEFT JOIN {{ ref('stg_jolpica__constructor_standings')}} cs
    ON  cs.season          = ds.season
    AND cs.round           = ds.round
    AND cs.constructor_id  = ds.constructor_id
LEFT JOIN {{ ref('dim_constructors') }}                 c
    ON  c.constructor_id = ds.constructor_id
    AND c.season         = ds.season

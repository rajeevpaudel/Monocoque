-- Championship standings snapshot per round (latest version via FINAL on source).
SELECT
    ds.season,
    ds.round,
    ds.driver_id,
    d.full_name             AS driver_name,
    ds.constructor_id,
    c.constructor_name,
    ds.standing_position,
    ds.points,
    ds.wins,
    cs.standing_position    AS constructor_position,
    cs.points               AS constructor_points
FROM {{ ref('stg_jolpica__driver_standings') }}         ds
LEFT JOIN {{ ref('dim_drivers') }}                      d  USING (driver_id)
LEFT JOIN {{ ref('stg_jolpica__constructor_standings')}} cs
    ON  cs.season          = ds.season
    AND cs.round           = ds.round
    AND cs.constructor_id  = ds.constructor_id
LEFT JOIN {{ ref('dim_constructors') }}                 c  ON c.constructor_id = ds.constructor_id

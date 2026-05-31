-- Fail if constructor standings points don't equal the sum of their drivers' points
-- for any given round (within 0.5 floating-point tolerance).
SELECT
    cs.season,
    cs.round,
    cs.constructor_id,
    cs.points               AS constructor_points,
    sum(ds.points)          AS driver_points_sum
FROM {{ ref('stg_jolpica__constructor_standings') }} cs
JOIN {{ ref('stg_jolpica__driver_standings') }} ds
    ON  ds.season         = cs.season
    AND ds.round          = cs.round
    AND ds.constructor_id = cs.constructor_id
GROUP BY cs.season, cs.round, cs.constructor_id, cs.points
HAVING abs(cs.points - driver_points_sum) > 0.5

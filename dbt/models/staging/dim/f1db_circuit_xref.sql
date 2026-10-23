{{ config(materialized='table', engine='MergeTree()', order_by='jolpica_circuit_id') }}

-- Jolpica circuit_id → f1db circuit_id.
-- 1. Races both sources hold for the same (season, round) give an exact pairing.
-- 2. Otherwise the nearest f1db circuit within 15 km. Coordinates alone can't
--    separate overlapping layouts (Las Vegas Strip vs Caesars Palace), hence step 1.
WITH jolpica AS (
    SELECT circuit_id, any(lat) AS lat, any(lng) AS lng
    FROM {{ source('raw_jolpica', 'circuits') }}
    GROUP BY circuit_id
),

by_race AS (
    SELECT
        j.circuit_id                    AS jolpica_circuit_id,
        topK(1)(f.circuit_id)[1]        AS f1db_circuit_id
    FROM (SELECT DISTINCT season, round, circuit_id FROM {{ source('raw_jolpica', 'races') }}) j
    INNER JOIN {{ source('raw_f1db', 'races') }} f
        ON f.year = j.season AND f.round = j.round
    GROUP BY j.circuit_id
),

by_geo AS (
    SELECT
        j.circuit_id                                                    AS jolpica_circuit_id,
        argMin(f.circuit_id, greatCircleDistance(j.lng, j.lat, f.lng, f.lat)) AS f1db_circuit_id,
        min(greatCircleDistance(j.lng, j.lat, f.lng, f.lat))            AS distance_m
    FROM jolpica j
    CROSS JOIN {{ source('raw_f1db', 'circuits') }} f
    GROUP BY j.circuit_id
)

SELECT
    g.jolpica_circuit_id                                        AS jolpica_circuit_id,
    coalesce(r.f1db_circuit_id, g.f1db_circuit_id) AS f1db_circuit_id,
    if(r.f1db_circuit_id IS NOT NULL, 'race', 'geo')                  AS match_method
FROM by_geo g
LEFT JOIN by_race r ON r.jolpica_circuit_id = g.jolpica_circuit_id
WHERE r.f1db_circuit_id IS NOT NULL OR g.distance_m < 15000

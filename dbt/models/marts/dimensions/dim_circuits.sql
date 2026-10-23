{{
    config(
        engine='ReplacingMergeTree()',
        order_by='circuit_id',
    )
}}

-- Jolpica doesn't publish length/turns. Take them from f1db (current layout) via the
-- circuit crosswalk, then the curated circuit_stats seed for anything f1db lacks.
SELECT
    c.circuit_id                                        AS circuit_id,
    c.circuit_name                                      AS circuit_name,
    c.locality                                          AS locality,
    c.country                                           AS country,
    c.latitude                                          AS latitude,
    c.longitude                                         AS longitude,
    c.altitude_m                                        AS altitude_m,
    c.url                                               AS url,
    coalesce(
        c.length_km,
        f.length_km,
        toFloat32(s.length_km)
    )                                                   AS length_km,
    coalesce(
        c.corners,
        f.turns,
        toUInt8(s.corners)
    )                                                   AS corners
FROM {{ ref('stg_jolpica__circuits') }}     c
LEFT JOIN {{ ref('f1db_circuit_xref') }}     x
    ON x.jolpica_circuit_id = c.circuit_id
LEFT JOIN {{ source('raw_f1db', 'circuits') }} f
    ON f.circuit_id = x.f1db_circuit_id
LEFT JOIN {{ ref('circuit_stats') }}        s
    ON s.circuit_id = c.circuit_id

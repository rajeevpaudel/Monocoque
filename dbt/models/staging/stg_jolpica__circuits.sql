SELECT
    circuit_id,
    circuit_name,
    locality,
    country,
    toFloat32(lat)              AS latitude,
    toFloat32(lng)              AS longitude,
    alt                         AS altitude_m,
    url,
    _ingested_at,
    'jolpica'                   AS _source
FROM {{ source('raw_jolpica', 'circuits') }}

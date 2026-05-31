SELECT
    circuit_id,
    circuit_name,
    locality,
    country,
    toFloat32(lat)              AS latitude,
    toFloat32(lng)              AS longitude,
    alt                         AS altitude_m,
    url,
    -- Prefer the explicit column (populated on new ingests after migration 004).
    -- Fall back to JSONExtract for rows ingested before the column was added.
    COALESCE(
        length_km,
        toFloat32OrNull(nullIf(JSONExtractString(_raw_json, 'length'), ''))
    )                           AS length_km,
    COALESCE(
        corners,
        toUInt8OrNull(nullIf(JSONExtractString(_raw_json, 'turns'), ''))
    )                           AS corners,
    _ingested_at,
    'jolpica'                   AS _source
FROM {{ source('raw_jolpica', 'circuits') }}

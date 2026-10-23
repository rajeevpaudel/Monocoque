-- Jolpica rows — deduplicated by circuit_id across re-ingests
SELECT
    circuit_id,
    any(circuit_name)   AS circuit_name,
    any(locality)       AS locality,
    any(country)        AS country,
    toFloat32(any(lat)) AS latitude,
    toFloat32(any(lng)) AS longitude,
    any(alt)            AS altitude_m,
    any(url)            AS url,
    MAX(COALESCE(
        length_km,
        toFloat32OrNull(nullIf(JSONExtractString(_raw_json, 'length'), ''))
    ))                  AS length_km,
    MAX(COALESCE(
        corners,
        toUInt8OrNull(nullIf(JSONExtractString(_raw_json, 'turns'), ''))
    ))                  AS corners,
    max(_ingested_at)   AS _ingested_at,
    'jolpica'           AS _source
FROM {{ source('raw_jolpica', 'circuits') }}
GROUP BY circuit_id

UNION ALL

-- f1db circuits with no Jolpica equivalent (ids mapped via f1db_source(), e.g. 'spielberg' → 'red_bull_ring').
SELECT
    circuit_id,
    name            AS circuit_name,
    place_name      AS locality,
    country_id      AS country,
    toFloat32(lat)  AS latitude,
    toFloat32(lng)  AS longitude,
    NULL            AS altitude_m,
    ''              AS url,
    length_km,
    turns           AS corners,
    _ingested_at,
    'f1db'          AS _source
FROM {{ f1db_source('circuits', circuit_col='circuit_id') }}
WHERE circuit_id NOT IN (SELECT DISTINCT circuit_id FROM {{ source('raw_jolpica', 'circuits') }})

-- Aggregate to one row per circuit_id: raw table is a plain MergeTree that accumulates
-- duplicate rows across re-ingests, so MAX() ensures non-null values win.
SELECT
    circuit_id,
    any(circuit_name)           AS circuit_name,
    any(locality)               AS locality,
    any(country)                AS country,
    toFloat32(any(lat))         AS latitude,
    toFloat32(any(lng))         AS longitude,
    any(alt)                    AS altitude_m,
    any(url)                    AS url,
    -- Prefer the explicit column (populated on new ingests after migration 004).
    -- Fall back to JSONExtract for rows ingested before the column was added.
    MAX(COALESCE(
        length_km,
        toFloat32OrNull(nullIf(JSONExtractString(_raw_json, 'length'), ''))
    ))                          AS length_km,
    MAX(COALESCE(
        corners,
        toUInt8OrNull(nullIf(JSONExtractString(_raw_json, 'turns'), ''))
    ))                          AS corners,
    max(_ingested_at)           AS _ingested_at,
    'jolpica'                   AS _source
FROM {{ source('raw_jolpica', 'circuits') }}
GROUP BY circuit_id

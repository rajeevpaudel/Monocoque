-- Jolpica's API never publishes circuit length/corner count (confirmed empty on every ingested
-- row's _raw_json). length_km/corners on raw_jolpica.circuits come from a one-off hardcoded
-- migration (clickhouse/migrations/007_populate_circuit_stats.sql) covering the 25 circuits used
-- in the 2024 season. f1db *does* publish real length_km/turns for all 78 circuits it has ever
-- seen, under its own circuit_id slugs (dashes instead of underscores, e.g. 'red-bull-ring' vs
-- Jolpica's 'red_bull_ring') — used here as a fallback so any circuit outside the hardcoded list
-- (including new ones added in future seasons) gets real stats without a new migration, as long
-- as the slug still normalizes to a match. A handful of circuits (older ones, plus a few where
-- Jolpica and f1db use unrelated slugs, e.g. 'americas' vs f1db's 'austin') fall through both
-- here; dim_circuits fills those via the f1db_circuit_xref crosswalk (+ circuit_stats seed).
WITH jolpica_circuits AS (
    SELECT
        circuit_id,
        any(circuit_name)   AS circuit_name,
        any(locality)       AS locality,
        any(country)        AS country,
        toFloat32(any(lat)) AS latitude,
        toFloat32(any(lng)) AS longitude,
        any(alt)            AS altitude_m,
        any(url)            AS url,
        MAX(length_km)      AS length_km_raw,
        MAX(corners)        AS corners_raw,
        max(_ingested_at)   AS _ingested_at
    FROM {{ source('raw_jolpica', 'circuits') }}
    GROUP BY circuit_id
),

f1db_stats AS (
    SELECT circuit_id, length_km, turns AS corners
    FROM {{ source('raw_f1db', 'circuits') }}
    WHERE length_km IS NOT NULL
)

SELECT
    jc.circuit_id,
    jc.circuit_name,
    jc.locality,
    jc.country,
    jc.latitude,
    jc.longitude,
    jc.altitude_m,
    jc.url,
    COALESCE(jc.length_km_raw, f1.length_km) AS length_km,
    COALESCE(jc.corners_raw, f1.corners)     AS corners,
    jc._ingested_at,
    'jolpica'           AS _source
FROM jolpica_circuits jc
LEFT JOIN f1db_stats f1
    ON  replaceAll(jc.circuit_id, '_', '-') = f1.circuit_id

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

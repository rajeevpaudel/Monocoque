SELECT
    constructor_id,
    name            AS constructor_name,
    nationality,
    url,
    _ingested_at,
    'jolpica'       AS _source
FROM {{ source('raw_jolpica', 'constructors') }}

UNION ALL

-- f1db constructors not already in jolpica (IDs differ: 'alfa-romeo' vs 'alfa').
SELECT
    constructor_id,
    name            AS constructor_name,
    country_id      AS nationality,
    ''              AS url,
    _ingested_at,
    'f1db'          AS _source
FROM {{ source('raw_f1db', 'constructors') }}
WHERE constructor_id NOT IN (SELECT DISTINCT constructor_id FROM {{ source('raw_jolpica', 'constructors') }})

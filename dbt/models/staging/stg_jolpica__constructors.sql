SELECT
    constructor_id,
    name            AS constructor_name,
    nationality,
    url,
    _ingested_at,
    'jolpica'       AS _source
FROM {{ source('raw_jolpica', 'constructors') }}

UNION ALL

-- f1db constructors with no Jolpica equivalent (ids mapped via f1db_source(), e.g. 'alfa-romeo' → 'alfa').
SELECT
    constructor_id,
    name            AS constructor_name,
    country_id      AS nationality,
    ''              AS url,
    _ingested_at,
    'f1db'          AS _source
FROM {{ f1db_source('constructors', constructor_col='constructor_id') }}
WHERE constructor_id NOT IN (SELECT DISTINCT constructor_id FROM {{ source('raw_jolpica', 'constructors') }})

SELECT
    constructor_id,
    name            AS constructor_name,
    nationality,
    url,
    _ingested_at,
    'jolpica'       AS _source
FROM {{ source('raw_jolpica', 'constructors') }}

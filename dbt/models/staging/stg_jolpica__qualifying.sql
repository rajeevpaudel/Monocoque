SELECT
    season,
    round,
    driver_id,
    constructor_id,
    toUInt8(position)   AS qualifying_position,
    q1,
    q2,
    q3,
    _ingested_at,
    'jolpica'           AS _source
FROM {{ source('raw_jolpica', 'qualifying') }}

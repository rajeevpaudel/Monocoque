SELECT
    season,
    round,
    constructor_id,
    toUInt8(position)       AS standing_position,
    position_text,
    toFloat32(points)       AS points,
    toUInt8(wins)           AS wins,
    _ingested_at,
    'jolpica'               AS _source
FROM {{ source('raw_jolpica', 'constructor_standings') }}
FINAL

SELECT
    season,
    round,
    driver_id,
    constructor_id,
    toUInt8(position)       AS standing_position,
    position_text,
    toFloat32(points)       AS points,
    toUInt8(wins)           AS wins,
    _ingested_at,
    'jolpica'               AS _source
FROM {{ source('raw_jolpica', 'driver_standings') }}
FINAL

UNION ALL

SELECT
    year            AS season,
    round,
    driver_id,
    ''              AS constructor_id,
    position AS standing_position,
    position_text,
    toFloat32(points)       AS points,
    0               AS wins,
    _ingested_at,
    'f1db'          AS _source
FROM {{ source('raw_f1db', 'driver_standings') }}
WHERE year NOT IN (SELECT DISTINCT season FROM {{ source('raw_jolpica', 'driver_standings') }})

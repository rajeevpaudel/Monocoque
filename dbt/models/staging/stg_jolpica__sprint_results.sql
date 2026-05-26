SELECT
    season,
    round,
    driver_id,
    constructor_id,
    toUInt8(grid)               AS grid_position,
    position                    AS finish_position,
    position_text,
    toUInt8(position_order)     AS position_order,
    toFloat32(points)           AS points,
    toUInt16(laps)              AS laps_completed,
    status,
    time_millis,
    _ingested_at,
    'jolpica'                   AS _source
FROM {{ source('raw_jolpica', 'sprint_results') }}

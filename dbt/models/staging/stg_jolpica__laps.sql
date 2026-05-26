SELECT
    season,
    round,
    driver_id,
    toUInt8(lap)            AS lap_number,
    toUInt8(position)       AS lap_position,
    time                    AS lap_time,
    toUInt32(time_millis)   AS lap_time_ms,
    _ingested_at,
    'jolpica'               AS _source
FROM {{ source('raw_jolpica', 'lap_times') }}

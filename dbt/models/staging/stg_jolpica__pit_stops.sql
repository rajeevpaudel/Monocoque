SELECT
    season,
    round,
    driver_id,
    toUInt8(stop)               AS stop_number,
    toUInt8(lap)                AS lap_number,
    time                        AS pit_time_of_day,
    duration                    AS pit_duration,
    duration_millis             AS pit_duration_ms,
    _ingested_at,
    'jolpica'                   AS _source
FROM {{ source('raw_jolpica', 'pit_stops') }}

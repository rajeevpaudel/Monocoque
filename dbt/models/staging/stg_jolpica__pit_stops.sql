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

UNION ALL

-- f1db pit stops: 'time' is the stop duration (not time-of-day)
SELECT
    year            AS season,
    round,
    driver_id,
    toUInt8(stop)   AS stop_number,
    toUInt8(lap)    AS lap_number,
    NULL            AS pit_time_of_day,
    time            AS pit_duration,
    time_millis     AS pit_duration_ms,
    _ingested_at,
    'f1db'          AS _source
FROM {{ source('raw_f1db', 'pit_stops') }}
WHERE year NOT IN (SELECT DISTINCT season FROM {{ source('raw_jolpica', 'pit_stops') }})

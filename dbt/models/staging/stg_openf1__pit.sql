SELECT
    session_key,
    driver_number,
    lap_number,
    parseDateTime64BestEffort(date)    AS pit_date,
    pit_duration,
    _ingested_at,
    'openf1'            AS _source
FROM {{ source('raw_openf1', 'pit') }}

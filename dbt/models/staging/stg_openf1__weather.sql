SELECT
    session_key,
    parseDateTime64BestEffort(date) AS weather_date,
    air_temperature,
    track_temperature,
    humidity,
    pressure,
    wind_direction,
    wind_speed,
    rainfall,
    _ingested_at,
    'openf1'                AS _source
FROM {{ source('raw_openf1', 'weather') }}

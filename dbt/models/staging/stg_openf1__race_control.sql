SELECT
    session_key,
    toDateTime(date)    AS message_date,
    driver_number,
    lap_number,
    category,
    flag,
    scope,
    sector,
    message,
    _ingested_at,
    'openf1'            AS _source
FROM {{ source('raw_openf1', 'race_control') }}

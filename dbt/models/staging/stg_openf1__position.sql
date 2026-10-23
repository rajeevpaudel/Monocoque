-- Running position changes per driver (one row per change event).
SELECT
    session_key,
    driver_number,
    parseDateTime64BestEffort(date, 3)  AS position_date,
    position,
    _ingested_at,
    'openf1'                            AS _source
FROM {{ source('raw_openf1', 'position') }}

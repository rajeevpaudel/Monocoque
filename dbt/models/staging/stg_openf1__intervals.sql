SELECT
    session_key,
    driver_number,
    parseDateTime64BestEffort(date)    AS interval_date,
    gap_to_leader,
    interval,
    _ingested_at,
    'openf1'            AS _source
FROM {{ source('raw_openf1', 'intervals') }}

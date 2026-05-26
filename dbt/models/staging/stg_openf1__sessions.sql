SELECT
    session_key,
    session_name,
    session_type,
    status,
    gmt_offset,
    year,
    circuit_key,
    circuit_short_name,
    country_code,
    country_name,
    location,
    meeting_key,
    parseDateTime64BestEffort(date_start)               AS date_start,
    parseDateTime64BestEffortOrNull(date_end)           AS date_end,
    _ingested_at,
    'openf1'                            AS _source
FROM {{ source('raw_openf1', 'sessions') }}

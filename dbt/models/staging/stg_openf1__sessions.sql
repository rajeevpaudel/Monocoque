-- Deduplicate by session_key — re-ingestion produces duplicate raw rows.
SELECT
    session_key,
    any(session_name)                                       AS session_name,
    any(session_type)                                       AS session_type,
    any(status)                                             AS status,
    any(gmt_offset)                                         AS gmt_offset,
    any(year)                                               AS year,
    any(circuit_key)                                        AS circuit_key,
    any(circuit_short_name)                                 AS circuit_short_name,
    any(country_code)                                       AS country_code,
    any(country_name)                                       AS country_name,
    any(location)                                           AS location,
    any(meeting_key)                                        AS meeting_key,
    parseDateTime64BestEffort(any(date_start))              AS date_start,
    parseDateTime64BestEffortOrNull(any(date_end))          AS date_end,
    max(_ingested_at)                                       AS _ingested_at,
    'openf1'                                                AS _source
FROM {{ source('raw_openf1', 'sessions') }}
GROUP BY session_key

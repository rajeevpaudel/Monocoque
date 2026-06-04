SELECT
    session_key,
    driver_number,
    stint_number,
    lap_start,
    lap_end,
    compound,
    tyre_age_at_start,
    _ingested_at,
    'openf1'            AS _source
FROM {{ source('raw_openf1', 'stints') }}

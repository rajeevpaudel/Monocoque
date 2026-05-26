SELECT
    session_key,
    driver_number,
    lap_number,
    parseDateTime64BestEffort(date_start) AS lap_start,
    lap_duration,
    is_pit_out_lap,
    duration_sector_1,
    duration_sector_2,
    duration_sector_3,
    i1_speed,
    i2_speed,
    st_speed,
    segments_sector_1,
    segments_sector_2,
    segments_sector_3,
    _ingested_at,
    'openf1'                        AS _source
FROM {{ source('raw_openf1', 'laps') }}

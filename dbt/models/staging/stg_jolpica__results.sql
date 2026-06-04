-- Deduplicate by natural key — re-ingestion produces duplicate raw rows.
SELECT
    season,
    round,
    driver_id,
    any(constructor_id)                         AS constructor_id,
    toUInt8(any(grid))                          AS grid_position,
    any(position)                               AS finish_position,
    any(position_text)                          AS position_text,
    toUInt8(any(position_order))                AS position_order,
    toFloat32(any(points))                      AS points,
    toUInt16(any(laps))                         AS laps_completed,
    any(status)                                 AS status,
    any(time_millis)                            AS time_millis,
    any(fastest_lap_rank)                       AS fastest_lap_rank,
    any(fastest_lap_time)                       AS fastest_lap_time,
    any(fastest_lap_speed)                      AS fastest_lap_speed,
    max(_ingested_at)                           AS _ingested_at,
    'jolpica'                                   AS _source
FROM {{ source('raw_jolpica', 'results') }}
GROUP BY season, round, driver_id

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

UNION ALL

-- Deduplicate: raw_f1db.race_results is MergeTree; repeated CSV ingestion produces duplicate rows.
SELECT
    year                         AS season,
    round,
    driver_id,
    any(constructor_id)          AS constructor_id,
    any(grid)                    AS grid_position,
    any(position)                AS finish_position,
    any(position_text)           AS position_text,
    toUInt8(any(position_order)) AS position_order,
    toFloat32(any(points))       AS points,
    any(laps)                    AS laps_completed,
    any(status)                  AS status,
    any(time_millis)             AS time_millis,
    NULL                         AS fastest_lap_rank,
    NULL                         AS fastest_lap_time,
    NULL                         AS fastest_lap_speed,
    max(_ingested_at)            AS _ingested_at,
    'f1db'                       AS _source
FROM {{ source('raw_f1db', 'race_results') }}
WHERE year NOT IN (SELECT DISTINCT season FROM {{ source('raw_jolpica', 'results') }})
GROUP BY year, round, driver_id

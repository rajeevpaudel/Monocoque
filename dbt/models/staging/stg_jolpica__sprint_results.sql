SELECT
    season,
    round,
    driver_id,
    constructor_id,
    toUInt8(grid)               AS grid_position,
    position                    AS finish_position,
    position_text,
    toUInt8(position_order)     AS position_order,
    toFloat32(points)           AS points,
    toUInt16(laps)              AS laps_completed,
    status,
    time_millis,
    _ingested_at,
    'jolpica'                   AS _source
FROM {{ source('raw_jolpica', 'sprint_results') }}

UNION ALL

-- Deduplicate: raw_f1db.sprint_race_results is MergeTree; repeated CSV ingestion produces duplicate rows.
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
    max(_ingested_at)            AS _ingested_at,
    'f1db'                       AS _source
FROM {{ f1db_source('sprint_race_results', driver_col='driver_id', constructor_col='constructor_id') }}
WHERE year NOT IN (SELECT DISTINCT season FROM {{ source('raw_jolpica', 'sprint_results') }})
GROUP BY year, round, driver_id

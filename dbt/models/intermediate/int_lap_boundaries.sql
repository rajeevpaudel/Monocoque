-- Per-lap time windows for any OpenF1 session.
-- lap_end is NULL for the final lap of each driver (handle with IS NULL check downstream).
-- Sorted by (session_key, driver_number, lap_start) to support ASOF JOIN in mart_lap_telemetry.
{{
    config(
        materialized='table',
        engine='MergeTree()',
        order_by='(session_key, driver_number, lap_start)',
    )
}}

SELECT
    session_key,
    driver_number,
    lap_number,
    lap_start,
    leadInFrame(lap_start) OVER (
        PARTITION BY session_key, driver_number
        ORDER BY lap_number
        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    )                           AS lap_end,
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
    segments_sector_3
FROM {{ ref('stg_openf1__laps') }}

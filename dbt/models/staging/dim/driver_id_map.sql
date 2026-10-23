{{
    config(
        materialized='table',
        engine='MergeTree()',
        order_by='(season, jolpica_driver_id, openf1_driver_number)',
    )
}}

-- Season-level summary of driver_session_map: every car number a driver used in a season.
-- NOT unique per (season, jolpica_driver_id) nor per (season, openf1_driver_number) — a driver
-- can change number (Bearman 2024: #38, #50) and FP1 drivers can reuse a number (2025 #89).
-- Join facts through driver_session_map on session_key instead; this table is for lookups only.
SELECT
    jolpica_driver_id,
    driver_number                       AS openf1_driver_number,
    season,
    toUInt32(count())                   AS session_count
FROM {{ ref('driver_session_map') }}
GROUP BY jolpica_driver_id, driver_number, season

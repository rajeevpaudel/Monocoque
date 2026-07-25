-- Lap time analysis: delta to fastest lap per race, distribution stats.
SELECT
    season,
    round,
    driver_id,
    lap_number,
    lap_time_ms,
    min(lap_time_ms) OVER (PARTITION BY season, round)          AS race_fastest_ms,
    lap_time_ms - min(lap_time_ms) OVER (PARTITION BY season, round) AS delta_to_fastest_ms,
    avg(lap_time_ms) OVER (PARTITION BY season, round, driver_id) AS driver_avg_lap_ms,
    -- OpenF1 sector detail (2023+ only)
    of1_s1,
    of1_s2,
    of1_s3,
    i1_speed,
    i2_speed,
    st_speed,
    -- 'sc' | 'vsc' | 'red' | NULL (green flag / no OpenF1 race control data, e.g. pre-2023)
    -- Exclude non-green laps from pace comparisons: filter WHERE track_status IS NULL.
    track_status
FROM {{ ref('fact_laps') }}
WHERE lap_time_ms > 0

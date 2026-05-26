-- One row per driver per race (1950-present). OpenF1 columns NULL for pre-2023.
{{
    config(
        materialized='incremental',
        incremental_strategy='append',
        engine='MergeTree()',
        order_by='(season, round, jolpica_driver_id)',
    )
}}

SELECT
    season,
    round,
    jolpica_driver_id,
    openf1_driver_number,
    constructor_id,
    grid_position,
    finish_position,
    position_text,
    points,
    laps_completed,
    status,
    race_time_ms,
    fastest_lap_rank,
    fastest_lap_time,
    fastest_lap_speed,
    of1_fastest_lap_duration,
    of1_s1,
    of1_s2,
    of1_s3,
    session_key
FROM {{ ref('int_driver_race_results') }}

{% if is_incremental() %}
WHERE (season, round) NOT IN (
    SELECT season, round FROM {{ this }}
)
{% endif %}

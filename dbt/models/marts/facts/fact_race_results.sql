-- depends_on: {{ ref('stg_jolpica__results') }}
-- depends_on: {{ ref('stg_openf1__laps') }}
-- depends_on: {{ ref('int_session_map') }}
-- One row per driver per race (1950-present). OpenF1 columns NULL for pre-2023.
{{
    config(
        materialized='incremental',
        incremental_strategy='delete+insert',
        unique_key=['season', 'round', 'driver_id'],
        engine='MergeTree()',
        order_by='(season, round, driver_id)',
    )
}}

SELECT
    toUInt32(season * 100 + round) AS race_sk,
    season,
    round,
    driver_id,
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
    session_key,
    _ingested_at
FROM {{ ref('int_driver_race_results') }}

{% if is_incremental() %}
WHERE {{ changed_rounds('season', 'round', 'stg_jolpica__results', session_name='Race') }}
{% endif %}

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
    toUInt32(r.season * 100 + r.round) AS race_sk,
    r.season,
    r.round,
    r.driver_id,
    r.openf1_driver_number,
    r.constructor_id,
    r.grid_position,
    r.finish_position,
    r.position_text,
    r.points,
    r.laps_completed,
    r.status,
    r.race_time_ms,
    r.fastest_lap_rank,
    r.fastest_lap_time,
    r.fastest_lap_speed,
    r.of1_fastest_lap_duration,
    r.of1_s1,
    r.of1_s2,
    r.of1_s3,
    r.session_key,
    -- grid_position vs qualifying_position diverge when a driver starts from anywhere other than
    -- their own qualifying result — grid drop penalties (gearbox/PU change, causing a collision,
    -- etc), promotion after another driver's penalty, or pit-lane starts. No source publishes the
    -- penalty *reason*, but this makes the fact of a non-native grid position visible without one.
    q.qualifying_position                                          AS qualifying_position,
    toInt16(r.grid_position) - toInt16(q.qualifying_position)      AS grid_delta,
    (q.qualifying_position IS NOT NULL
        AND r.grid_position != q.qualifying_position)               AS started_from_penalty_position,
    r._ingested_at
FROM {{ ref('int_driver_race_results') }}                       r
LEFT JOIN {{ ref('fact_qualifying') }}                           q
    ON  q.season    = r.season
    AND q.round     = r.round
    AND q.driver_id = r.driver_id

{% if is_incremental() %}
WHERE {{ changed_rounds('r.season', 'r.round', 'stg_jolpica__results', session_name='Race') }}
{% endif %}

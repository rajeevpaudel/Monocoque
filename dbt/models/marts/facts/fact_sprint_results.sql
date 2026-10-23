-- depends_on: {{ ref('stg_openf1__sessions') }}
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
    toUInt32(s.season * 100 + s.round) AS race_sk,
    s.season                AS season,
    s.round                 AS round,
    s.driver_id             AS driver_id,
    s.constructor_id        AS constructor_id,
    s.grid_position         AS grid_position,
    s.finish_position       AS finish_position,
    s.position_text         AS position_text,
    s.points                AS points,
    s.laps_completed        AS laps_completed,
    s.status                AS status,
    s.time_millis           AS sprint_time_ms,
    m.driver_number  AS openf1_driver_number,
    sm.session_key          AS session_key,
    -- Sprint weekend format has changed nearly every year it's existed:
    -- 2021: "Sprint Qualifying" — a real race whose result set Sunday's grid.
    -- 2022-2023: "Sprint"/"Sprint Shootout" — standalone, does NOT set Sunday's grid.
    -- 2024+: "Sprint" + "Sprint Qualifying" (a knockout session) — sprint result still
    --        does not set Sunday's grid.
    -- A naive query against sprint_results can't tell these apart without this column.
    multiIf(
        s.season = 2021, '2021_sets_grid',
        s.season IN (2022, 2023), '2022_2023_standalone',
        s.season >= 2024, '2024_plus_standalone',
        'unknown'
    )                        AS sprint_format_version,
    s.season = 2021          AS sprint_result_set_grid,
    s._ingested_at           AS _ingested_at
FROM {{ ref('stg_jolpica__sprint_results') }}   s
LEFT JOIN {{ ref('int_session_map') }}          sm
    ON  sm.season       = s.season
    AND sm.round        = s.round
    -- OpenF1 tags the sprint race as session_type='Race', session_name='Sprint' (confirmed
    -- against live data) — session_type='Sprint' never actually occurs and left this join dead.
    AND sm.session_name = 'Sprint'
LEFT JOIN {{ ref('driver_session_map') }}     m
    ON  m.session_key       = sm.session_key
    AND m.jolpica_driver_id = s.driver_id

{% if is_incremental() %}
WHERE {{ changed_rounds('s.season', 's.round', 'stg_jolpica__sprint_results', 'stg_openf1__sessions', session_name='Sprint') }}
{% endif %}

-- One row per pit stop. Grain: (season, round, driver_id, stop_number).
{{
    config(
        materialized='incremental',
        incremental_strategy='append',
        engine='MergeTree()',
        order_by='(season, round, driver_id, stop_number)',
    )
}}

SELECT
    toUInt32(p.season * 100 + p.round) AS race_sk,
    p.season,
    p.round,
    p.driver_id,
    p.stop_number,
    p.lap_number,
    p.pit_time_of_day,
    p.pit_duration,
    p.pit_duration_ms,
    m.openf1_driver_number,
    sm.session_key
FROM {{ ref('stg_jolpica__pit_stops') }}        p
LEFT JOIN {{ ref('driver_id_map') }}            m
    ON  m.jolpica_driver_id = p.driver_id
    AND m.season            = p.season
LEFT JOIN {{ ref('int_session_map') }}          sm
    ON  sm.season       = p.season
    AND sm.round        = p.round
    AND sm.session_type = 'Race'
    AND sm.session_name = 'Race'

{% if is_incremental() %}
WHERE (p.season, p.round) NOT IN (
    SELECT season, round FROM {{ this }}
)
{% endif %}

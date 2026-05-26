-- Per-sample telemetry joined with GPS, sliced by lap. 2023+ only.
-- Partition by (year, session_key) — query always filters on session_key for speed.
-- First ASOF JOIN assigns lap number from boundaries (nearest preceding lap_start).
-- Second ASOF JOIN aligns GPS timestamps to telemetry timestamps.
{{
    config(
        materialized='incremental',
        incremental_strategy='append',
        engine='MergeTree()',
        partition_by='(toYear(date), session_key)',
        order_by='(session_key, driver_number, date)',
    )
}}

SELECT
    cd.session_key                  AS session_key,
    cd.driver_number                AS driver_number,
    lb.lap_number,
    lb.is_pit_out_lap,
    cd.date                         AS date,
    cd.speed,
    cd.rpm,
    cd.n_gear,
    cd.throttle,
    cd.brake,
    cd.drs,
    loc.x,
    loc.y,
    loc.z,
    sm.season                       AS season,
    sm.round                        AS round,
    sm.session_type                 AS session_type,
    jmap.jolpica_driver_id          AS driver_id,
    d.name_acronym                  AS driver_code,
    d.team_name                     AS team_name,
    if(d.team_colour IS NOT NULL AND d.team_colour != '', concat('#', d.team_colour), NULL) AS team_colour,
    cd._ingested_at                 AS _ingested_at
FROM {{ ref('stg_openf1__car_data') }}      cd
ASOF LEFT JOIN {{ ref('int_lap_boundaries') }}  lb
    ON  lb.session_key   = cd.session_key
    AND lb.driver_number = cd.driver_number
    AND lb.lap_start    <= cd.date
ASOF LEFT JOIN {{ ref('stg_openf1__location') }} loc
    ON  loc.session_key   = cd.session_key
    AND loc.driver_number = cd.driver_number
    AND loc.date         <= cd.date
LEFT JOIN {{ ref('int_session_map') }}      sm
    ON  sm.session_key = cd.session_key
LEFT JOIN {{ ref('stg_openf1__drivers') }}  d
    ON  d.session_key   = cd.session_key
    AND d.driver_number = cd.driver_number
LEFT JOIN {{ source('dim', 'driver_id_map') }} jmap
    ON  jmap.openf1_driver_number = cd.driver_number
    AND jmap.season               = sm.season

{% if is_incremental() %}
WHERE cd._ingested_at > (SELECT max(_ingested_at) FROM {{ this }})
  AND lb.lap_number IS NOT NULL
  AND (lb.lap_end IS NULL OR cd.date < lb.lap_end)
{% else %}
WHERE lb.lap_number IS NOT NULL
  AND (lb.lap_end IS NULL OR cd.date < lb.lap_end)
{% endif %}

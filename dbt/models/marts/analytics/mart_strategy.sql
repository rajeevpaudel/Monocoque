-- Tire strategy per driver per race: stints, compounds, pit timing.
SELECT
    r.season                AS season,
    r.round                 AS round,
    r.driver_id     AS driver_id,
    r.constructor_id        AS constructor_id,
    -- Pit stop data from Jolpica
    p.stop_number           AS stop_number,
    p.lap_number            AS pit_lap,
    p.pit_duration_ms       AS pit_duration_ms,
    -- Stint data from OpenF1 (2023+ only)
    st.stint_number         AS stint_number,
    st.lap_start            AS lap_start,
    st.lap_end              AS lap_end,
    st.compound             AS compound,
    st.tyre_age_at_start    AS tyre_age_at_start
FROM {{ ref('fact_race_results') }}                 r
LEFT JOIN {{ ref('stg_jolpica__pit_stops') }}       p
    ON  p.season    = r.season
    AND p.round     = r.round
    AND p.driver_id = r.driver_id
LEFT JOIN {{ ref('stg_openf1__stints') }}           st
    ON  st.session_key   = r.session_key
    AND st.driver_number = r.openf1_driver_number

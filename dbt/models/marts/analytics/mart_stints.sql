-- Grain: one row per tyre stint per driver per race.
-- OpenF1 stints only; pre-2023 rows are not represented.
SELECT
    sm.season,
    sm.round,
    r.race_name,
    r.circuit_name,
    r.circuit_country,
    r.race_date,
    jmap.jolpica_driver_id      AS driver_id,
    st.driver_number            AS openf1_driver_number,
    st.stint_number,
    st.lap_start,
    st.lap_end,
    st.compound,
    st.tyre_age_at_start,
    fr.constructor_id,
    d.full_name                 AS driver_name,
    d.driver_code
FROM {{ ref('stg_openf1__stints') }}    st
JOIN {{ ref('int_session_map') }}       sm
    ON  sm.session_key  = st.session_key
    AND sm.session_type = 'Race'
    AND sm.session_name = 'Race'
LEFT JOIN {{ ref('dim_races') }}        r
    ON  r.season = sm.season AND r.round = sm.round
LEFT JOIN {{ ref('driver_id_map') }}   jmap
    ON  jmap.openf1_driver_number = st.driver_number
    AND jmap.season               = sm.season
LEFT JOIN {{ ref('fact_race_results') }} fr
    ON  fr.season    = sm.season
    AND fr.round     = sm.round
    AND fr.driver_id = jmap.jolpica_driver_id
LEFT JOIN {{ ref('dim_drivers') }}      d
    ON  d.driver_id = jmap.jolpica_driver_id
    AND d.season    = sm.season

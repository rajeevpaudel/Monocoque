{{ config(tags=["marts"]) }}
-- Fail if a driver's team in mart_standings is not the team of their most recent race entry
-- up to that round. Guards mid-season moves (Lawson/Tsunoda 2025, Bearman 2024), which used to
-- keep the first team of the season for every round. Jolpica-sourced seasons only.
WITH latest_team AS (
    SELECT
        st.season                                           AS season,
        st.round                                            AS round,
        st.driver_id                                        AS driver_id,
        st.constructor_id                                   AS standings_constructor_id,
        argMax(r.constructor_id, r.round)                   AS results_constructor_id
    FROM {{ ref('mart_standings') }} st
    INNER JOIN {{ ref('stg_jolpica__results') }} r
        ON  r.season    = st.season
        AND r.driver_id = st.driver_id
        AND r.round    <= st.round
    WHERE st.season IN (SELECT DISTINCT season FROM {{ source('raw_jolpica', 'driver_standings') }})
    GROUP BY st.season, st.round, st.driver_id, st.constructor_id
)
SELECT *
FROM latest_team
WHERE standings_constructor_id != results_constructor_id

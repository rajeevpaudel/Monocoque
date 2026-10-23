{{ config(tags=["marts"]) }}
-- Fail if a driver who took part in a championship Race/Qualifying/Sprint session has no
-- Jolpica driver_id. Regression guard for the season-level number map, which dropped
-- Bearman's 2024 Jeddah stand-in (#38 Ferrari) because he raced more often as #50 for Haas.
SELECT sm.season, sm.round, sm.session_name, d.session_key, d.driver_number, any(d.name_acronym) AS name_acronym
FROM {{ ref('stg_openf1__drivers') }} d
INNER JOIN {{ ref('int_session_map') }} sm ON sm.session_key = d.session_key
LEFT JOIN {{ ref('driver_session_map') }} m
    ON  m.session_key   = d.session_key
    AND m.driver_number = d.driver_number
WHERE sm.round IS NOT NULL
  AND sm.session_name IN ('Race', 'Qualifying', 'Sprint', 'Sprint Qualifying', 'Sprint Shootout')
  AND d.driver_number > 0
  AND m.jolpica_driver_id IS NULL
GROUP BY sm.season, sm.round, sm.session_name, d.session_key, d.driver_number

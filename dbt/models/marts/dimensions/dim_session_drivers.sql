-- One row per driver per session (OpenF1 2023+ only).
-- Carries display attributes that should not be denormalized into billion-row telemetry.
-- Note: CTE avoided due to ClickHouse 24.3 bug where ORDER BY columns cannot be resolved
-- through CTEs in CREATE TABLE ... EMPTY AS (...). Deduplication inlined as subquery instead.
{{
    config(
        engine='ReplacingMergeTree()',
        order_by=['session_key', 'driver_number'],
    )
}}

SELECT
    d.session_key                                                       AS session_key,
    d.driver_number                                                     AS driver_number,
    jmap.jolpica_driver_id                                              AS driver_id,
    d.driver_code                                                       AS driver_code,
    d.full_name                                                         AS full_name,
    d.team_name                                                         AS team_name,
    if(
        d.team_colour_raw IS NOT NULL AND d.team_colour_raw != '',
        concat('#', d.team_colour_raw),
        NULL
    )                                                                   AS team_colour,
    d.headshot_url                                                      AS headshot_url
FROM (
    SELECT
        session_key,
        driver_number,
        any(name_acronym)   AS driver_code,
        any(full_name)      AS full_name,
        any(team_name)      AS team_name,
        any(team_colour)    AS team_colour_raw,
        any(headshot_url)   AS headshot_url
    FROM {{ ref('stg_openf1__drivers') }}
    GROUP BY session_key, driver_number
)                                               d
LEFT JOIN {{ ref('int_session_map') }}          sm   ON sm.session_key = d.session_key
LEFT JOIN {{ ref('driver_id_map') }}            jmap
    ON  jmap.openf1_driver_number = d.driver_number
    AND jmap.season               = sm.season

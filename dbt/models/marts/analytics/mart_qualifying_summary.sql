-- Qualifying comparison mart. One row per driver per qualifying session.
-- All columns an app needs for a head-to-head comparison UI, no further joins required.
-- OpenF1 columns (sectors, speeds, segments, headshot, team_colour) are NULL pre-2023.
SELECT
    fq.season               AS season,
    fq.round                AS round,
    ds.race_name            AS race_name,
    ds.circuit_name         AS circuit_name,
    ds.circuit_country      AS circuit_country,
    ds.location             AS circuit_location,
    ds.date_start           AS qualifying_date,
    fq.driver_id            AS driver_id,
    fq.constructor_id       AS constructor_id,
    fq.qualifying_position  AS qualifying_position,
    -- Jolpica lap time strings
    fq.q1                   AS q1,
    fq.q2                   AS q2,
    fq.q3                   AS q3,
    COALESCE(fq.q3, fq.q2, fq.q1) AS best_time,
    -- OpenF1 best lap (2023+ only; matched to Jolpica time within ±50 ms)
    fq.session_key          AS session_key,
    fq.openf1_driver_number AS openf1_driver_number,
    fq.best_lap_number      AS best_lap_number,
    fq.best_lap_duration    AS best_lap_duration,
    -- Source-agreement diagnostics
    fq.jolpica_best_time_ms AS jolpica_best_time_ms,
    fq.openf1_best_time_ms  AS openf1_best_time_ms,
    fq.best_source_match    AS best_source_match,
    fq.best_s1              AS best_s1,
    fq.best_s2              AS best_s2,
    fq.best_s3              AS best_s3,
    fq.i1_speed             AS i1_speed,
    fq.i2_speed             AS i2_speed,
    fq.st_speed             AS st_speed,
    fq.segments_s1          AS segments_s1,
    fq.segments_s2          AS segments_s2,
    fq.segments_s3          AS segments_s3,
    -- Driver bio (Jolpica, all eras)
    d.full_name             AS driver_name,
    d.given_name,
    d.family_name,
    d.driver_code,
    d.permanent_number,
    d.nationality           AS driver_nationality,
    -- Driver appearance (OpenF1, 2023+ only)
    od.name_acronym         AS name_acronym,
    od.team_name            AS team_name,
    if(od.team_colour IS NOT NULL AND od.team_colour != '', concat('#', od.team_colour), NULL) AS team_colour,
    od.headshot_url         AS headshot_url,
    -- Constructor
    c.constructor_name      AS constructor_name
FROM {{ ref('fact_qualifying') }}               fq
LEFT JOIN {{ ref('dim_sessions') }}             ds
    ON  ds.session_key = fq.session_key
LEFT JOIN {{ ref('dim_drivers') }}              d
    ON  d.driver_id = fq.driver_id
LEFT JOIN (
    -- raw_openf1.drivers can have duplicate rows per (session_key, driver_number)
    -- due to repeated ingestion; deduplicate here before joining.
    SELECT
        session_key,
        driver_number,
        any(name_acronym)  AS name_acronym,
        any(team_name)     AS team_name,
        any(team_colour)   AS team_colour,
        any(headshot_url)  AS headshot_url
    FROM {{ ref('stg_openf1__drivers') }}
    GROUP BY session_key, driver_number
)                                               od
    ON  od.session_key   = fq.session_key
    AND od.driver_number = fq.openf1_driver_number
LEFT JOIN {{ ref('dim_constructors') }}         c
    ON  c.constructor_id = fq.constructor_id

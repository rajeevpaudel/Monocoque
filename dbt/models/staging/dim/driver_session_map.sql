{{
    config(
        materialized='table',
        engine='MergeTree()',
        order_by='(session_key, driver_number)',
    )
}}

-- Maps each OpenF1 (session_key, driver_number) to a Jolpica driver_id. Grain is the session,
-- not the season, because a car number does not identify a driver for a whole season:
--   * FP1 rookie/test drivers (team must run one) appear only in practice;
--   * reserves change number between teams (Bearman 2024: #38 Ferrari, #50 Haas);
--   * one number is reused by different FP1 drivers in a season (2025 #89: Dunne, O'Ward).
-- Match = OpenF1 name_acronym equals the driver's code AND the normalised surname agrees, so a
-- shared code (VER: Verstappen/Vergne) or a number collision can never misattribute a driver.
-- Remaining ties (MSC: Michael/Mick Schumacher) go to a given-name match, then to the driver whose
-- career (race seasons) is closest to the session. FP1-only drivers with no Jolpica/f1db record
-- (e.g. Iwasa) have no row here and keep driver_id NULL downstream.
WITH session_drivers AS (
    SELECT
        d.session_key                   AS session_key,
        d.driver_number                 AS driver_number,
        any(d.name_acronym)             AS name_acronym,
        any(d.first_name)               AS first_name,
        any(d.last_name)                AS last_name
    FROM {{ ref('stg_openf1__drivers') }} d
    WHERE d.driver_number > 0
    GROUP BY d.session_key, d.driver_number
),

jolpica_drivers AS (
    SELECT
        driver_id                       AS candidate_id,
        any(driver_code)                AS candidate_code,
        any(given_name)                 AS given_name,
        any(family_name)                AS family_name
    FROM {{ ref('stg_jolpica__drivers') }}
    WHERE driver_code IS NOT NULL AND driver_code != ''
    GROUP BY driver_id
),

careers AS (
    SELECT driver_id AS career_driver_id, min(season) AS first_season, max(season) AS last_season
    FROM {{ ref('stg_jolpica__results') }}
    GROUP BY driver_id
),

candidates AS (
    SELECT
        sd.session_key                  AS session_key,
        sd.driver_number                AS driver_number,
        sd.name_acronym                 AS name_acronym,
        jd.candidate_id                 AS candidate_id,
        {{ normalize_name('jd.family_name') }} = {{ normalize_name('sd.last_name') }}      AS surname_match,
        {{ normalize_name("splitByChar(' ', jd.given_name)[1]") }}
            = {{ normalize_name("splitByChar(' ', sd.first_name)[1]") }}                AS given_name_match,
        if(
            c.first_season IS NULL,
            9999,
            if(sm.season BETWEEN c.first_season AND c.last_season, 0,
               least(abs(toInt32(sm.season) - toInt32(c.first_season)),
                     abs(toInt32(sm.season) - toInt32(c.last_season))))
        )                               AS career_distance
    FROM session_drivers sd
    INNER JOIN {{ ref('int_session_map') }} sm ON sm.session_key = sd.session_key
    INNER JOIN jolpica_drivers jd ON jd.candidate_code = sd.name_acronym
    LEFT JOIN careers c ON c.career_driver_id = jd.candidate_id
)

SELECT
    c.session_key                                                       AS session_key,
    c.driver_number                                                     AS driver_number,
    any(sm.season)                                                      AS season,
    any(sm.round)                                                       AS round,
    any(sm.session_name)                                                AS session_name,
    any(c.name_acronym)                                                 AS name_acronym,
    argMax(c.candidate_id, (c.given_name_match, -c.career_distance))    AS jolpica_driver_id,
    toUInt8(count())                                                    AS candidate_count
FROM candidates c
INNER JOIN {{ ref('int_session_map') }} sm ON sm.session_key = c.session_key
WHERE c.surname_match
GROUP BY c.session_key, c.driver_number

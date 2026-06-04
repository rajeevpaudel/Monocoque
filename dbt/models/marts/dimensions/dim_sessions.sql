-- Clean session dimension. One row per OpenF1 session_key.
-- Pre-2023 races have no OpenF1 sessions and are not represented here.
-- Uses meeting_key to map all session types (Practice, Qualifying, Race) to Jolpica round.
-- The ±1 day tolerance handles late-night races (e.g. Las Vegas) where the UTC date of the
-- Race session is one day ahead of Jolpica's local race_date. ClickHouse 24.3 does not allow
-- OR in JOIN ON conditions referencing both sides, so the mapping is done via CROSS JOIN + WHERE.
{{
    config(
        engine='ReplacingMergeTree()',
        order_by='session_key',
    )
}}

WITH race_sessions AS (
    -- One row per Race session with its UTC date — used to resolve meeting_key → Jolpica round.
    SELECT
        meeting_key,
        year,
        toDate(date_start) AS race_date_utc
    FROM {{ ref('stg_openf1__sessions') }}
    WHERE session_type = 'Race'
),
meeting_rounds AS (
    -- Map each OpenF1 meeting to its Jolpica round. The OR in WHERE (not JOIN ON) is valid in
    -- ClickHouse for CROSS JOIN and handles the UTC-vs-local date offset for night races.
    SELECT rs.meeting_key, rs.year, any(jr.round) AS round, any(jr.race_name) AS race_name, any(jr.circuit_id) AS circuit_id
    FROM race_sessions rs, {{ ref('stg_jolpica__races') }} jr
    WHERE jr.season = rs.year
      AND (jr.race_date = rs.race_date_utc OR jr.race_date = addDays(rs.race_date_utc, -1))
    GROUP BY rs.meeting_key, rs.year
)

SELECT
    s.session_key,
    s.year                  AS season,
    mr.round                AS round,
    mr.race_name            AS race_name,
    mr.circuit_id           AS circuit_id,
    c.circuit_name,
    c.country               AS circuit_country,
    s.session_name,
    s.session_type,
    s.date_start,
    s.date_end,
    s.location,
    s.country_name,
    s.country_code,
    s.circuit_short_name
FROM {{ ref('stg_openf1__sessions') }}      s
LEFT JOIN meeting_rounds                    mr
    ON  mr.meeting_key = s.meeting_key
LEFT JOIN {{ ref('dim_circuits') }}         c
    ON  c.circuit_id = mr.circuit_id
-- Exclude sessions that couldn't be mapped to a Jolpica race round (e.g. pre-season testing).
WHERE mr.round IS NOT NULL

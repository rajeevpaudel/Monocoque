-- Maps (season, round, session_key, session_type) → for joining Jolpica to OpenF1.
-- OpenF1 only covers 2023+; rows for earlier seasons will simply have no match.
-- Uses meeting_key to correctly map all session types (Practice, Qualifying, Sprint) to round.
-- The ±1 day tolerance handles late-night races (e.g. Las Vegas) where the UTC date of the
-- Race session is one day ahead of Jolpica's local race_date. ClickHouse 24.3 does not allow
-- OR in JOIN ON conditions referencing both sides, so the mapping is done via CROSS JOIN + WHERE.
WITH race_sessions AS (
    -- One row per Race session with its UTC date — used to resolve meeting_key → Jolpica round.
    SELECT
        meeting_key,
        year,
        toDate(date_start) AS race_date_utc
    FROM {{ ref('stg_openf1__sessions') }}
    WHERE session_type = 'Race' AND session_name = 'Race'
),
meeting_rounds AS (
    -- Map each OpenF1 meeting to its Jolpica round. The OR in WHERE (not JOIN ON) is valid in
    -- ClickHouse for CROSS JOIN and handles the UTC-vs-local date offset for night races.
    SELECT rs.meeting_key, rs.year, any(jr.round) AS round
    FROM race_sessions rs, {{ ref('stg_jolpica__races') }} jr
    WHERE jr.season = rs.year
      AND (jr.race_date = rs.race_date_utc OR jr.race_date = addDays(rs.race_date_utc, -1))
    GROUP BY rs.meeting_key, rs.year
)

SELECT
    s.session_key,
    s.year                          AS season,
    mr.round                        AS round,
    s.session_name                  AS session_name,
    s.session_type                  AS session_type,
    s.date_start                    AS date_start,
    s.date_end                      AS date_end,
    s.circuit_short_name            AS circuit_short_name,
    s.country_name                  AS country_name
FROM {{ ref('stg_openf1__sessions') }}      s
LEFT JOIN meeting_rounds                    mr
    ON  mr.meeting_key = s.meeting_key

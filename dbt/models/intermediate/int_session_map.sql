-- Maps (season, round, session_key, session_type) → for joining Jolpica to OpenF1.
-- OpenF1 only covers 2023+; rows for earlier seasons will simply have no match.
-- Uses meeting_key to correctly map all session types (Practice, Qualifying, Sprint) to round.
WITH race_dates AS (
    -- Jolpica stores only the race date; meeting_key groups all sessions in a weekend,
    -- so we find the Race session's date to look up the Jolpica round for all session types.
    SELECT
        meeting_key,
        year,
        toDate(date_start) AS race_date
    FROM {{ ref('stg_openf1__sessions') }}
    WHERE session_type = 'Race'
)

SELECT
    s.session_key,
    s.year                          AS season,
    r.round,
    s.session_type,
    s.date_start,
    s.date_end,
    s.circuit_short_name,
    s.country_name
FROM {{ ref('stg_openf1__sessions') }}      s
LEFT JOIN race_dates                        rd
    ON  rd.meeting_key = s.meeting_key
LEFT JOIN {{ ref('stg_jolpica__races') }}   r
    ON  r.season    = rd.year
    AND r.race_date = rd.race_date

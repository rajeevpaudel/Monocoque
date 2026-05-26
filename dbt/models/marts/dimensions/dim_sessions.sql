-- Clean session dimension. One row per OpenF1 session_key.
-- Pre-2023 races have no OpenF1 sessions and are not represented here.
-- Uses meeting_key to map all session types (Practice, Qualifying, Race) to Jolpica round.
{{
    config(
        engine='ReplacingMergeTree()',
        order_by='session_key',
    )
}}

WITH race_dates AS (
    -- Jolpica stores only the race date; all sessions in a meeting share meeting_key,
    -- so we find the Race session's date to join all session types to Jolpica round.
    SELECT
        meeting_key,
        year,
        toDate(date_start) AS race_date
    FROM {{ ref('stg_openf1__sessions') }}
    WHERE session_type = 'Race'
)

SELECT
    s.session_key,
    s.year                  AS season,
    r.round,
    r.race_name,
    r.circuit_id,
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
LEFT JOIN race_dates                        rd
    ON  rd.meeting_key = s.meeting_key
LEFT JOIN {{ ref('stg_jolpica__races') }}   r
    ON  r.season    = rd.year
    AND r.race_date = rd.race_date
LEFT JOIN {{ ref('dim_circuits') }}         c
    ON  c.circuit_id = r.circuit_id

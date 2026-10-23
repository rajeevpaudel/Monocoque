-- Pairs safety-car / VSC / red-flag start-end events into [start_lap, end_lap] windows per session.
-- Pairing is positional (1st start <-> 1st end, 2nd <-> 2nd, ...) within each (session_key, family)
-- group ordered by message time. This assumes messages arrive in a well-formed start/end sequence,
-- which holds for the vast majority of sessions but is not guaranteed by the upstream API — a missing
-- "ending" message leaves end_lap NULL, which downstream models treat as "open through end of session".
{{
    config(
        materialized='table',
        engine='MergeTree()',
        order_by='(session_key, start_date)',
    )
}}

WITH events AS (
    SELECT
        session_key,
        message_date,
        lap_number,
        event_type,
        multiIf(
            event_type IN ('vsc_start', 'vsc_end'), 'vsc',
            event_type IN ('sc_start', 'sc_end'),    'sc',
            event_type IN ('red_flag_start', 'red_flag_end'), 'red',
                                                      'ignore'
        ) AS family,
        event_type IN ('vsc_start', 'sc_start', 'red_flag_start') AS is_start
    FROM {{ ref('int_race_control_events') }}
    WHERE event_type IN ('vsc_start', 'vsc_end', 'sc_start', 'sc_end', 'red_flag_start', 'red_flag_end')
),

starts AS (
    SELECT
        session_key,
        family,
        lap_number AS start_lap,
        message_date AS start_date,
        row_number() OVER (PARTITION BY session_key, family ORDER BY message_date) AS seq
    FROM events
    WHERE is_start
),

ends AS (
    SELECT
        session_key,
        family,
        lap_number AS end_lap,
        message_date AS end_date,
        row_number() OVER (PARTITION BY session_key, family ORDER BY message_date) AS seq
    FROM events
    WHERE NOT is_start
)

SELECT
    s.session_key       AS session_key,
    s.family             AS track_status,
    s.start_lap          AS start_lap,
    s.start_date          AS start_date,
    e.end_lap            AS end_lap,
    e.end_date            AS end_date
FROM starts s
LEFT JOIN ends e
    ON  e.session_key = s.session_key
    AND e.family      = s.family
    AND e.seq         = s.seq
WHERE s.start_lap IS NOT NULL

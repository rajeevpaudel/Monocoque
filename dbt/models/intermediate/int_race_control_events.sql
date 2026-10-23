-- Classifies raw OpenF1 race_control messages into structured event types.
-- OpenF1 does not publish a structured "track status" field — everything arrives as free-text
-- `message` with loosely structured `category`/`flag`/`scope` columns. This model is a best-effort
-- text classification of that free text; it is not a guaranteed-accurate source of truth, but it is
-- the only way to derive safety car / VSC / red flag / track-limits-deletion events from OpenF1 at all.
{{
    config(
        materialized='table',
        engine='MergeTree()',
        order_by='(session_key, message_date)',
    )
}}

SELECT
    session_key,
    message_date,
    driver_number,
    lap_number,
    category,
    flag,
    scope,
    sector,
    message,
    multiIf(
        category = 'SafetyCar' AND message ILIKE '%VIRTUAL SAFETY CAR%' AND message ILIKE '%DEPLOYED%',                           'vsc_start',
        category = 'SafetyCar' AND message ILIKE '%VIRTUAL SAFETY CAR%' AND (message ILIKE '%ENDING%' OR message ILIKE '%END%'),  'vsc_end',
        category = 'SafetyCar' AND message ILIKE '%SAFETY CAR%' AND message ILIKE '%DEPLOYED%',                                   'sc_start',
        category = 'SafetyCar' AND message ILIKE '%SAFETY CAR%' AND (message ILIKE '%IN THIS LAP%' OR message ILIKE '%ENDING%'),  'sc_end',
        flag = 'RED',                                                                                                             'red_flag_start',
        (flag = 'GREEN' OR message ILIKE '%RESUME%') AND (message ILIKE '%RED FLAG%' OR message ILIKE '%RESUME%'),                'red_flag_end',
        category = 'Other' AND message ILIKE '%TRACK LIMITS%' AND message ILIKE '%DELETED%',                                      'track_limits_deletion',
        flag IN ('YELLOW', 'DOUBLE YELLOW'),                                                                                      'yellow_flag',
                                                                                                                                    'other'
    ) AS event_type
FROM {{ ref('stg_openf1__race_control') }}

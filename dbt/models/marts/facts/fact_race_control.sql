-- One row per OpenF1 race control message (flags, safety car/VSC, track-limit deletions, etc), 2023+.
-- This is the mart-layer exposure of raw_openf1.race_control — previously ingested and staged but
-- never surfaced past staging. Join to dim_sessions on session_key for race/round context.
{{
    config(
        materialized='incremental',
        incremental_strategy='delete+insert',
        unique_key=['session_key', 'message_date', 'driver_number', 'category'],
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
    event_type
FROM {{ ref('int_race_control_events') }}

{% if is_incremental() %}
WHERE message_date > (SELECT coalesce(max(message_date), toDateTime('1970-01-01')) FROM {{ this }})
{% endif %}

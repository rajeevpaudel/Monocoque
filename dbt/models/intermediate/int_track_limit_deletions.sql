-- Track-limits lap-time deletion events, isolated from int_race_control_events.
-- Used to reconcile fact_qualifying's Jolpica-vs-OpenF1 best-lap matching: a lap deleted for
-- track limits can legitimately fail the ±50ms match (Jolpica drops the deleted time from the
-- driver's best; OpenF1's raw lap_duration has no concept of the deletion), which previously
-- surfaced as an unexplained 'mismatch' with no diagnostic trail.
{{
    config(
        materialized='table',
        engine='MergeTree()',
        order_by='(session_key, driver_number)',
    )
}}

SELECT
    session_key,
    assumeNotNull(driver_number) AS driver_number,
    lap_number,
    message_date,
    message
FROM {{ ref('int_race_control_events') }}
WHERE event_type = 'track_limits_deletion'
  AND driver_number IS NOT NULL

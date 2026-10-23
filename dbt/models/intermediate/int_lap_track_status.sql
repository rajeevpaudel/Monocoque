-- Flattens int_track_status_periods into one row per (session_key, lap_number), so it can be
-- joined 1:1 onto lap-level facts. An open period (end_lap IS NULL, e.g. a missing "ending"
-- message) is bounded by the session's last observed OpenF1 lap so arrayJoin(range(...)) stays finite.
-- Where periods for different families overlap the same lap, the most severe status wins:
-- red flag > safety car > virtual safety car.
{{
    config(
        materialized='table',
        engine='MergeTree()',
        order_by='(session_key, lap_number)',
    )
}}

WITH session_max_lap AS (
    SELECT session_key, max(lap_number) AS max_lap
    FROM {{ ref('stg_openf1__laps') }}
    GROUP BY session_key
),

bounded_periods AS (
    SELECT
        tsp.session_key                                    AS session_key,
        tsp.track_status                                    AS track_status,
        toUInt32(tsp.start_lap)                             AS start_lap,
        toUInt32(coalesce(tsp.end_lap, sml.max_lap, tsp.start_lap)) AS end_lap
    FROM {{ ref('int_track_status_periods') }} tsp
    LEFT JOIN session_max_lap sml
        ON sml.session_key = tsp.session_key
),

exploded AS (
    SELECT
        session_key,
        arrayJoin(range(start_lap, greatest(start_lap, end_lap) + 1)) AS lap_number,
        track_status,
        multiIf(track_status = 'red', 3, track_status = 'sc', 2, track_status = 'vsc', 1, 0) AS severity_rank
    FROM bounded_periods
)

SELECT
    session_key,
    lap_number,
    argMax(track_status, severity_rank) AS track_status
FROM exploded
GROUP BY session_key, lap_number

{{
    config(
        materialized='incremental',
        incremental_strategy='delete+insert',
        unique_key=['season', 'round', 'driver_id'],
        engine='MergeTree()',
        order_by='(season, round, driver_id)',
    )
}}

-- ClickHouse 24.3 limitations:
-- (1) columns of a CTE cannot be referenced via alias qualification (cte_alias.column)
--     when that CTE is the primary FROM source.
-- (2) CREATE TABLE ... EMPTY AS fails to resolve columns from CTEs that reference a
--     view whose definition contains a subquery.
-- Workarounds:
-- (1) keep stg_jolpica__qualifying as the main FROM (alias q); use a separate jolpica_ms
--     CTE as a JOIN target inside matched_laps (where alias qualification works fine).
-- (2) jolpica_ms reads directly from the raw source table (not the view) so it remains
--     a plain aggregating subquery that ClickHouse can resolve during table creation.

WITH

jolpica_ms AS (
    SELECT
        season,
        round,
        driver_id,
        {{ lap_time_to_ms('NULLIF(any(q1), \'\')') }}               AS q1_ms,
        {{ lap_time_to_ms('NULLIF(any(q2), \'\')') }}               AS q2_ms,
        {{ lap_time_to_ms('NULLIF(any(q3), \'\')') }}               AS q3_ms
    FROM {{ source('raw_jolpica', 'qualifying') }}
    GROUP BY season, round, driver_id
),

openf1_best AS (
    SELECT
        session_key,
        driver_number,
        toInt32(min(lap_duration) * 1000)    AS openf1_best_time_ms,
        max(_ingested_at)                    AS _ingested_at
    FROM {{ ref('stg_openf1__laps') }}
    WHERE is_pit_out_lap = 0
      AND lap_duration IS NOT NULL
    GROUP BY session_key, driver_number
),

all_laps AS (
    SELECT
        session_key,
        driver_number,
        lap_number,
        toInt32(lap_duration * 1000)         AS lap_ms,
        lap_duration,
        duration_sector_1,
        duration_sector_2,
        duration_sector_3,
        i1_speed,
        i2_speed,
        st_speed,
        segments_sector_1,
        segments_sector_2,
        segments_sector_3
    FROM {{ ref('stg_openf1__laps') }}
    WHERE is_pit_out_lap = 0
      AND lap_duration IS NOT NULL
),

matched_laps AS (
    -- Ties on lap time (OpenF1 occasionally repeats a lap_duration on the next lap) are broken by
    -- the lap whose sectors actually sum to its duration.
    SELECT
        al.session_key     AS session_key,
        al.driver_number   AS driver_number,
        argMin(al.lap_number,        (abs(al.lap_ms - COALESCE(jms.q3_ms, jms.q2_ms, jms.q1_ms)),
            ifNull(abs(al.duration_sector_1 + al.duration_sector_2 + al.duration_sector_3 - al.lap_duration), 1e9))) AS best_lap_number,
        argMin(al.lap_duration,      (abs(al.lap_ms - COALESCE(jms.q3_ms, jms.q2_ms, jms.q1_ms)),
            ifNull(abs(al.duration_sector_1 + al.duration_sector_2 + al.duration_sector_3 - al.lap_duration), 1e9))) AS best_lap_duration,
        argMin(al.duration_sector_1, (abs(al.lap_ms - COALESCE(jms.q3_ms, jms.q2_ms, jms.q1_ms)),
            ifNull(abs(al.duration_sector_1 + al.duration_sector_2 + al.duration_sector_3 - al.lap_duration), 1e9))) AS best_s1,
        argMin(al.duration_sector_2, (abs(al.lap_ms - COALESCE(jms.q3_ms, jms.q2_ms, jms.q1_ms)),
            ifNull(abs(al.duration_sector_1 + al.duration_sector_2 + al.duration_sector_3 - al.lap_duration), 1e9))) AS best_s2,
        argMin(al.duration_sector_3, (abs(al.lap_ms - COALESCE(jms.q3_ms, jms.q2_ms, jms.q1_ms)),
            ifNull(abs(al.duration_sector_1 + al.duration_sector_2 + al.duration_sector_3 - al.lap_duration), 1e9))) AS best_s3,
        argMin(al.i1_speed,          (abs(al.lap_ms - COALESCE(jms.q3_ms, jms.q2_ms, jms.q1_ms)),
            ifNull(abs(al.duration_sector_1 + al.duration_sector_2 + al.duration_sector_3 - al.lap_duration), 1e9))) AS i1_speed,
        argMin(al.i2_speed,          (abs(al.lap_ms - COALESCE(jms.q3_ms, jms.q2_ms, jms.q1_ms)),
            ifNull(abs(al.duration_sector_1 + al.duration_sector_2 + al.duration_sector_3 - al.lap_duration), 1e9))) AS i2_speed,
        argMin(al.st_speed,          (abs(al.lap_ms - COALESCE(jms.q3_ms, jms.q2_ms, jms.q1_ms)),
            ifNull(abs(al.duration_sector_1 + al.duration_sector_2 + al.duration_sector_3 - al.lap_duration), 1e9))) AS st_speed,
        argMin(al.segments_sector_1, (abs(al.lap_ms - COALESCE(jms.q3_ms, jms.q2_ms, jms.q1_ms)),
            ifNull(abs(al.duration_sector_1 + al.duration_sector_2 + al.duration_sector_3 - al.lap_duration), 1e9))) AS segments_s1,
        argMin(al.segments_sector_2, (abs(al.lap_ms - COALESCE(jms.q3_ms, jms.q2_ms, jms.q1_ms)),
            ifNull(abs(al.duration_sector_1 + al.duration_sector_2 + al.duration_sector_3 - al.lap_duration), 1e9))) AS segments_s2,
        argMin(al.segments_sector_3, (abs(al.lap_ms - COALESCE(jms.q3_ms, jms.q2_ms, jms.q1_ms)),
            ifNull(abs(al.duration_sector_1 + al.duration_sector_2 + al.duration_sector_3 - al.lap_duration), 1e9))) AS segments_s3
    FROM all_laps al
    JOIN {{ ref('int_session_map') }}             sm ON sm.session_key = al.session_key
    JOIN {{ ref('driver_session_map') }}         m
        ON  m.session_key   = al.session_key
        AND m.driver_number = al.driver_number
    JOIN jolpica_ms                              jms
        ON  jms.season    = sm.season
        AND jms.round     = sm.round
        AND jms.driver_id = m.jolpica_driver_id
    WHERE COALESCE(jms.q3_ms, jms.q2_ms, jms.q1_ms) IS NOT NULL
      AND abs(al.lap_ms - COALESCE(jms.q3_ms, jms.q2_ms, jms.q1_ms)) <= 50
    GROUP BY al.session_key, al.driver_number
),

-- A driver with a track-limits deletion event in this session is a plausible explanation for a
-- failed ±50ms match: Jolpica's official best time excludes the deleted lap, but OpenF1's raw
-- lap_duration doesn't know it was deleted, so the two sources can legitimately disagree.
track_limit_flags AS (
    SELECT DISTINCT session_key, driver_number
    FROM {{ ref('int_track_limit_deletions') }}
)

SELECT
    toUInt32(q.season * 100 + q.round) AS race_sk,
    q.season                            AS season,
    q.round                             AS round,
    q.driver_id                         AS driver_id,
    q.constructor_id                    AS constructor_id,
    q.qualifying_position               AS qualifying_position,
    q.q1                                AS q1,
    q.q2                                AS q2,
    q.q3                                AS q3,
    q.q1_ms                             AS q1_ms,
    q.q2_ms                             AS q2_ms,
    q.q3_ms                             AS q3_ms,
    COALESCE(q.q3_ms, q.q2_ms, q.q1_ms) AS jolpica_best_time_ms,
    m.driver_number              AS openf1_driver_number,
    sm.session_key                      AS session_key,
    ml.best_lap_number                  AS best_lap_number,
    ml.best_lap_duration                AS best_lap_duration,
    ml.best_s1                          AS best_s1,
    ml.best_s2                          AS best_s2,
    ml.best_s3                          AS best_s3,
    ml.i1_speed                         AS i1_speed,
    ml.i2_speed                         AS i2_speed,
    ml.st_speed                         AS st_speed,
    ml.segments_s1                      AS segments_s1,
    ml.segments_s2                      AS segments_s2,
    ml.segments_s3                      AS segments_s3,
    ob.openf1_best_time_ms              AS openf1_best_time_ms,
    tld.driver_number IS NOT NULL       AS deleted_for_track_limits,
    multiIf(
        ml.best_lap_number IS NOT NULL,         'matched',
        sm.session_key     IS NULL,             'jolpica_only',
        COALESCE(q.q1_ms, q.q2_ms, q.q3_ms) IS NULL,    NULL,
        tld.driver_number IS NOT NULL,          'mismatch_track_limits_deletion',
                                                'mismatch'
    ) AS best_source_match,
    greatest(q._ingested_at, coalesce(ob._ingested_at, q._ingested_at)) AS _ingested_at
FROM {{ ref('stg_jolpica__qualifying') }}        q
LEFT JOIN {{ ref('int_session_map') }}           sm
    ON  sm.season       = q.season
    AND sm.round        = q.round
    AND sm.session_type = 'Qualifying'
    AND sm.session_name = 'Qualifying'
LEFT JOIN {{ ref('driver_session_map') }}        m
    ON  m.session_key       = sm.session_key
    AND m.jolpica_driver_id = q.driver_id
LEFT JOIN matched_laps                           ml
    ON  ml.session_key   = sm.session_key
    AND ml.driver_number = m.driver_number
LEFT JOIN openf1_best                            ob
    ON  ob.session_key   = sm.session_key
    AND ob.driver_number = m.driver_number
LEFT JOIN track_limit_flags                      tld
    ON  tld.session_key   = sm.session_key
    AND tld.driver_number = m.driver_number

{% if is_incremental() %}
WHERE {{ changed_rounds('q.season', 'q.round', 'stg_jolpica__qualifying', session_name='Qualifying') }}
{% endif %}

-- join_use_nulls is also set warehouse-wide in profiles.yml; kept explicit here because every
-- `x IS NULL` "no match" check below (jolpica_only / track-limits reconciliation) depends on it.
SETTINGS join_use_nulls = 1

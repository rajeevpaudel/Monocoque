{{
    config(
        materialized='incremental',
        incremental_strategy='append',
        engine='MergeTree()',
        order_by='(season, round, driver_id)',
    )
}}

-- ClickHouse 24.3 limitation: columns of a CTE cannot be referenced via alias
-- qualification (cte_alias.column) when that CTE is the primary FROM source.
-- Workaround: keep stg_jolpica__qualifying as the main FROM (alias q), compute
-- jolpica_best_time_ms inline in the SELECT, and use a separate jolpica_ms CTE
-- only as a JOIN target inside matched_laps (where CTE alias qualification works).

-- Macro to parse 'M:SS.mmm' or 'SS.mmm' to milliseconds.
-- Repeated twice: once in jolpica_ms (for matched_laps), once inline in SELECT.
{% set parse_ms %}
multiIf(
    COALESCE(q3, q2, q1) IS NULL,
    NULL,
    position(COALESCE(q3, q2, q1), ':') > 0,
    toInt32OrZero(substring(COALESCE(q3, q2, q1),
                            1,
                            position(COALESCE(q3, q2, q1), ':') - 1)) * 60000
    + toInt32OrZero(substring(COALESCE(q3, q2, q1),
                              position(COALESCE(q3, q2, q1), ':') + 1,
                              position(COALESCE(q3, q2, q1), '.') - position(COALESCE(q3, q2, q1), ':') - 1)) * 1000
    + toInt32OrZero(substring(COALESCE(q3, q2, q1),
                              position(COALESCE(q3, q2, q1), '.') + 1)),
    toInt32OrZero(substring(COALESCE(q3, q2, q1),
                            1,
                            position(COALESCE(q3, q2, q1), '.') - 1)) * 1000
    + toInt32OrZero(substring(COALESCE(q3, q2, q1),
                              position(COALESCE(q3, q2, q1), '.') + 1))
)
{% endset %}

WITH

-- Jolpica times as a JOIN target only (used inside matched_laps).
-- `round` is renamed to `rnd` here so the name does not leak into the outer
-- query scope and shadow the `round` column on the main FROM alias `q`.
jolpica_ms AS (
    SELECT
        season,
        round    AS rnd,
        driver_id,
        {{ parse_ms }} AS jolpica_best_time_ms
    FROM {{ ref('stg_jolpica__qualifying') }}
),

-- OpenF1's own fastest lap per driver/session (for the diagnostic column only).
openf1_best AS (
    SELECT
        session_key,
        driver_number,
        toInt32(min(lap_duration) * 1000)         AS openf1_best_time_ms
    FROM {{ ref('stg_openf1__laps') }}
    WHERE is_pit_out_lap = 0
      AND lap_duration IS NOT NULL
    GROUP BY session_key, driver_number
),

-- All usable lap details — base for the tolerance match.
all_laps AS (
    SELECT
        session_key,
        driver_number,
        lap_number,
        lap_duration,
        toInt32(lap_duration * 1000)         AS lap_ms,
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

-- Match each driver's OpenF1 lap to the Jolpica official time within ±50 ms.
matched_laps AS (
    SELECT
        al.session_key     AS session_key,
        al.driver_number   AS driver_number,
        argMin(al.lap_number,          abs(al.lap_ms - jms.jolpica_best_time_ms)) AS best_lap_number,
        argMin(al.lap_duration,        abs(al.lap_ms - jms.jolpica_best_time_ms)) AS best_lap_duration,
        argMin(al.duration_sector_1,   abs(al.lap_ms - jms.jolpica_best_time_ms)) AS best_s1,
        argMin(al.duration_sector_2,   abs(al.lap_ms - jms.jolpica_best_time_ms)) AS best_s2,
        argMin(al.duration_sector_3,   abs(al.lap_ms - jms.jolpica_best_time_ms)) AS best_s3,
        argMin(al.i1_speed,            abs(al.lap_ms - jms.jolpica_best_time_ms)) AS i1_speed,
        argMin(al.i2_speed,            abs(al.lap_ms - jms.jolpica_best_time_ms)) AS i2_speed,
        argMin(al.st_speed,            abs(al.lap_ms - jms.jolpica_best_time_ms)) AS st_speed,
        argMin(al.segments_sector_1,   abs(al.lap_ms - jms.jolpica_best_time_ms)) AS segments_s1,
        argMin(al.segments_sector_2,   abs(al.lap_ms - jms.jolpica_best_time_ms)) AS segments_s2,
        argMin(al.segments_sector_3,   abs(al.lap_ms - jms.jolpica_best_time_ms)) AS segments_s3
    FROM all_laps al
    JOIN {{ ref('int_session_map') }}          sm
        ON  sm.session_key = al.session_key
    JOIN {{ source('dim', 'driver_id_map') }}  m
        ON  m.openf1_driver_number = al.driver_number
        AND m.season               = sm.season
    JOIN jolpica_ms                            jms
        ON  jms.season    = sm.season
        AND jms.rnd       = sm.round
        AND jms.driver_id = m.jolpica_driver_id
    WHERE jms.jolpica_best_time_ms IS NOT NULL
      AND abs(al.lap_ms - jms.jolpica_best_time_ms) <= 50
    GROUP BY al.session_key, al.driver_number
)

SELECT
    q.season                AS season,
    q.round                 AS round,
    q.driver_id             AS driver_id,
    q.constructor_id        AS constructor_id,
    q.qualifying_position   AS qualifying_position,
    q.q1                    AS q1,
    q.q2                    AS q2,
    q.q3                    AS q3,
    m.openf1_driver_number  AS openf1_driver_number,
    sm.session_key          AS session_key,
    -- Jolpica-aligned OpenF1 lap (NULL when no lap lands within ±50 ms)
    ml.best_lap_number      AS best_lap_number,
    ml.best_lap_duration    AS best_lap_duration,
    ml.best_s1              AS best_s1,
    ml.best_s2              AS best_s2,
    ml.best_s3              AS best_s3,
    ml.i1_speed             AS i1_speed,
    ml.i2_speed             AS i2_speed,
    ml.st_speed             AS st_speed,
    ml.segments_s1          AS segments_s1,
    ml.segments_s2          AS segments_s2,
    ml.segments_s3          AS segments_s3,
    -- Source-agreement diagnostics
    {{ parse_ms }}                                             AS jolpica_best_time_ms,
    ob.openf1_best_time_ms                                    AS openf1_best_time_ms,
    multiIf(
        ml.best_lap_number IS NOT NULL,         'matched',
        sm.session_key     IS NULL,             'jolpica_only',
        COALESCE(q.q3, q.q2, q.q1) IS NULL,    NULL,
                                                'mismatch'
    )                                                          AS best_source_match
FROM {{ ref('stg_jolpica__qualifying') }}       q
LEFT JOIN {{ source('dim', 'driver_id_map') }}  m
    ON  m.jolpica_driver_id = q.driver_id
    AND m.season            = q.season
LEFT JOIN {{ ref('int_session_map') }}          sm
    ON  sm.season       = q.season
    AND sm.round        = q.round
    AND sm.session_type = 'Qualifying'
    AND sm.session_name = 'Qualifying'
LEFT JOIN matched_laps                          ml
    ON  ml.session_key   = sm.session_key
    AND ml.driver_number = m.openf1_driver_number
LEFT JOIN openf1_best                           ob
    ON  ob.session_key   = sm.session_key
    AND ob.driver_number = m.openf1_driver_number

{% if is_incremental() %}
WHERE (q.season, q.round) NOT IN (
    SELECT season, round FROM {{ this }}
)
{% endif %}

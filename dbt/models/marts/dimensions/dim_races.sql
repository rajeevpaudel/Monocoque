-- dbt/models/marts/dimensions/dim_races.sql
-- One row per round per season. Covers all F1 races 1950–present.
-- race_sk is a deterministic surrogate: toUInt32(season * 100 + round).
--   Max value: 2099 * 100 + 99 = 209,999 — well within UInt32.
-- session_key is NULL pre-2023 (no OpenF1 Race session exists).
-- is_sprint_weekend = 1 when the round includes a sprint race.
{{
    config(
        engine='ReplacingMergeTree()',
        order_by=['season', 'round'],
    )
}}

-- Note: CTE avoided here due to ClickHouse 24.3 bug where ORDER BY columns
-- cannot be resolved through CTEs in CREATE TABLE ... EMPTY AS (...).
-- The race_session_keys aggregation is inlined as a subquery instead.
-- ClickHouse 24.3: all table-qualified columns need explicit AS aliases in EMPTY AS context,
-- otherwise the column name is stored as 'r.column_name' instead of 'column_name'.
SELECT
    toUInt32(r.season * 100 + r.round)          AS race_sk,
    r.season                                    AS season,
    r.round                                     AS round,
    r.race_name                                 AS race_name,
    r.circuit_id                                AS circuit_id,
    c.circuit_name                              AS circuit_name,
    c.country                                   AS circuit_country,
    c.locality                                  AS circuit_locality,
    r.race_date                                 AS race_date,
    r.race_time_utc                             AS race_time_utc,
    r.url                                       AS url,
    sk.session_key                              AS session_key,
    if(sp.has_sprint IS NOT NULL, toUInt8(1), toUInt8(0)) AS is_sprint_weekend
FROM {{ ref('stg_jolpica__races') }}            r
LEFT JOIN {{ ref('dim_circuits') }}             c  ON c.circuit_id = r.circuit_id
LEFT JOIN (
    SELECT season, round, any(session_key) AS session_key
    FROM {{ ref('int_session_map') }}
    WHERE session_type = 'Race' AND session_name = 'Race'
    GROUP BY season, round
)                                               sk ON sk.season = r.season AND sk.round = r.round
LEFT JOIN {{ ref('int_sprint_weekend_flag') }}  sp ON sp.season = r.season AND sp.round = r.round

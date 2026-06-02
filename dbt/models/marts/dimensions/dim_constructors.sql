-- Constructor dimension with SCD2 season-scoped validity.
-- One row per (constructor_id, season) for seasons with race results.
-- Constructors that never appear in results get one NULL-season row.
-- is_current = 1 for the most recent season each constructor was active.
{{
    config(
        engine='ReplacingMergeTree()',
        order_by='(constructor_id, valid_from)',
    )
}}

SELECT
    c.constructor_id                             AS constructor_id,
    c.constructor_name                           AS constructor_name,
    c.nationality                                AS nationality,
    c.url                                        AS url,
    ch.season                                    AS season,
    ch.valid_from                                AS valid_from,
    ch.valid_to                                  AS valid_to,
    ch.is_current                                AS is_current
FROM {{ ref('stg_jolpica__constructors') }}      c
LEFT JOIN (
    SELECT
        sc.constructor_id                        AS constructor_id,
        sc.season                                AS season,
        ss.first_race_date                       AS valid_from,
        leadInFrame(ss.first_race_date, 1, toDate('9999-12-31')) OVER (
            PARTITION BY sc.constructor_id
            ORDER BY sc.season
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        )                                        AS valid_to,
        if(
            ROW_NUMBER() OVER (
                PARTITION BY sc.constructor_id
                ORDER BY sc.season DESC
            ) = 1,
            toUInt8(1),
            toUInt8(0)
        )                                        AS is_current
    FROM (
        SELECT constructor_id, season
        FROM {{ ref('stg_jolpica__results') }}
        GROUP BY constructor_id, season
    ) sc
    JOIN (
        SELECT season, min(race_date) AS first_race_date
        FROM {{ ref('stg_jolpica__races') }}
        GROUP BY season
    ) ss ON ss.season = sc.season
) ch ON ch.constructor_id = c.constructor_id

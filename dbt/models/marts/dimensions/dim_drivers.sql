-- Canonical driver dimension with SCD2 constructor tracking.
-- One row per (driver_id, season) for seasons with race results.
-- Drivers with no results get one row with NULL season/constructor/valid_from/valid_to.
-- is_current = 1 marks the most recent season for each driver.
-- Join fact tables on driver_id; filter is_current = 1 for current team context.
{{
    config(
        engine='ReplacingMergeTree()',
        order_by='(driver_id, valid_from)',
    )
}}

SELECT
    d.driver_id                                  AS driver_id,
    d.given_name                                 AS given_name,
    d.family_name                                AS family_name,
    d.full_name                                  AS full_name,
    d.date_of_birth                              AS date_of_birth,
    d.nationality                                AS nationality,
    d.permanent_number                           AS permanent_number,
    d.driver_code                                AS driver_code,
    d.url                                        AS url,
    h.headshot_url                               AS headshot_url,
    dh.season                                    AS season,
    dh.constructor_id                            AS constructor_id,
    dh.valid_from                                AS valid_from,
    dh.valid_to                                  AS valid_to,
    dh.is_current                                AS is_current
FROM {{ ref('stg_jolpica__drivers') }}           d
LEFT JOIN (
    SELECT
        sc.driver_id                             AS driver_id,
        sc.season                                AS season,
        sc.constructor_id                        AS constructor_id,
        ss.first_race_date                       AS valid_from,
        leadInFrame(ss.first_race_date, 1, toDate('9999-12-31')) OVER (
            PARTITION BY sc.driver_id
            ORDER BY sc.season
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        )                                        AS valid_to,
        if(
            ROW_NUMBER() OVER (
                PARTITION BY sc.driver_id
                ORDER BY sc.season DESC
            ) = 1,
            toUInt8(1),
            toUInt8(0)
        )                                        AS is_current
    FROM (
        SELECT
            r.driver_id                          AS driver_id,
            r.season                             AS season,
            argMax(r.constructor_id, r.round)    AS constructor_id
        FROM {{ ref('stg_jolpica__results') }} r
        GROUP BY r.driver_id, r.season
    ) sc
    JOIN (
        SELECT
            season,
            min(race_date)                       AS first_race_date
        FROM {{ ref('stg_jolpica__races') }}
        GROUP BY season
    ) ss ON ss.season = sc.season
) dh ON dh.driver_id = d.driver_id
LEFT JOIN (
    SELECT
        jmap.jolpica_driver_id                   AS driver_id,
        argMax(od.headshot_url, od.session_key)  AS headshot_url
    FROM {{ ref('driver_id_map') }}              jmap
    JOIN {{ ref('int_session_map') }}            sm ON sm.season = jmap.season
    JOIN {{ ref('stg_openf1__drivers') }}        od
        ON  od.driver_number = jmap.openf1_driver_number
        AND od.session_key   = sm.session_key
    WHERE jmap.season >= 2023
      AND od.headshot_url != ''
    GROUP BY jmap.jolpica_driver_id
) h ON h.driver_id = d.driver_id

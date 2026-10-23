{{ config(materialized='table', engine='MergeTree()', order_by='f1db_driver_id') }}

-- f1db driver_id ('lewis-hamilton') → Jolpica driver_id ('hamilton'), so seasons sourced
-- from f1db join cleanly with Jolpica seasons. Match tiers, best first:
--   1. same (season, round, finishing order) in both sources' race results
--   2. date of birth + surname   3. date of birth + first name   4. full name (if unique)
-- The f1db_id_overrides seed takes precedence. Unmatched f1db drivers keep their f1db id.
WITH f AS (
    SELECT
        driver_id,
        date_of_birth                                           AS dob,
        {{ normalize_name('last_name') }}                       AS ln,
        {{ normalize_name('first_name') }}                      AS fn,
        {{ normalize_name("concat(first_name, last_name)") }}   AS full
    FROM {{ source('raw_f1db', 'drivers') }}
),

j AS (
    SELECT
        driver_id,
        any(date_of_birth)                                      AS dob,
        {{ normalize_name('any(family_name)') }}                AS ln,
        {{ normalize_name('any(given_name)') }}                 AS fn,
        {{ normalize_name("concat(any(given_name), any(family_name))") }} AS full
    FROM {{ source('raw_jolpica', 'drivers') }}
    GROUP BY driver_id
),

by_results AS (
    SELECT fr.driver_id AS f1db_driver_id, topK(1)(jr.driver_id)[1] AS jolpica_driver_id
    FROM {{ source('raw_f1db', 'race_results') }} fr
    INNER JOIN {{ source('raw_jolpica', 'results') }} jr
        ON jr.season = fr.year AND jr.round = fr.round AND jr.position_order = fr.position_order
    GROUP BY fr.driver_id
),

candidates AS (
    SELECT f1db_id AS f1db_driver_id, jolpica_id AS jolpica_driver_id, 0 AS tier
    FROM {{ ref('f1db_id_overrides') }} WHERE entity = 'driver'
    UNION ALL
    SELECT f1db_driver_id, jolpica_driver_id, 1 AS tier FROM by_results
    UNION ALL
    SELECT f.driver_id, j.driver_id, 2 FROM f INNER JOIN j ON j.dob = f.dob AND j.ln = f.ln WHERE f.dob != ''
    UNION ALL
    SELECT f.driver_id, j.driver_id, 3 FROM f INNER JOIN j ON j.dob = f.dob AND j.fn = f.fn WHERE f.dob != ''
    UNION ALL
    SELECT f.driver_id, any(j.driver_id), 4 FROM f INNER JOIN j ON j.full = f.full
    GROUP BY f.driver_id HAVING uniqExact(j.driver_id) = 1
)

SELECT
    f1db_driver_id,
    argMin(jolpica_driver_id, tier)                             AS jolpica_driver_id,
    multiIf(min(tier) = 0, 'override', min(tier) = 1, 'results', min(tier) = 2, 'dob_surname',
            min(tier) = 3, 'dob_first_name', 'full_name')       AS match_method
FROM candidates
GROUP BY f1db_driver_id

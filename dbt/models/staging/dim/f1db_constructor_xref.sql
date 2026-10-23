{{ config(materialized='table', engine='MergeTree()', order_by='f1db_constructor_id') }}

-- f1db constructor_id ('red-bull') → Jolpica constructor_id ('red_bull'). Match tiers:
--   1. same (season, round, finishing order) in both sources' race results
--   2. id with '-' → '_'
--   3. normalised team name, when it identifies a single Jolpica constructor
-- The f1db_id_overrides seed takes precedence over all tiers.
-- Unmatched f1db constructors keep their f1db id.
WITH j AS (
    SELECT constructor_id, {{ normalize_name('any(name)') }} AS nm
    FROM {{ source('raw_jolpica', 'constructors') }}
    GROUP BY constructor_id
),

by_results AS (
    SELECT fr.constructor_id AS f1db_constructor_id, topK(1)(jr.constructor_id)[1] AS jolpica_constructor_id
    FROM {{ source('raw_f1db', 'race_results') }} fr
    INNER JOIN {{ source('raw_jolpica', 'results') }} jr
        ON jr.season = fr.year AND jr.round = fr.round AND jr.position_order = fr.position_order
    GROUP BY fr.constructor_id
),

candidates AS (
    SELECT f1db_id AS f1db_constructor_id, jolpica_id AS jolpica_constructor_id, 0 AS tier
    FROM {{ ref('f1db_id_overrides') }} WHERE entity = 'constructor'
    UNION ALL
    SELECT f1db_constructor_id, jolpica_constructor_id, 1 AS tier FROM by_results
    UNION ALL
    SELECT f.constructor_id, any(j.constructor_id), 3
    FROM {{ source('raw_f1db', 'constructors') }} f
    INNER JOIN j ON j.nm = {{ normalize_name('f.name') }}
    GROUP BY f.constructor_id HAVING uniqExact(j.constructor_id) = 1
    UNION ALL
    SELECT f.constructor_id, j.constructor_id, 2
    FROM {{ source('raw_f1db', 'constructors') }} f
    INNER JOIN j ON j.constructor_id = replaceAll(f.constructor_id, '-', '_')
)

SELECT
    f1db_constructor_id,
    argMin(jolpica_constructor_id, tier)                        AS jolpica_constructor_id,
    multiIf(min(tier) = 0, 'override', min(tier) = 1, 'results', min(tier) = 2, 'id', 'name') AS match_method
FROM candidates
GROUP BY f1db_constructor_id

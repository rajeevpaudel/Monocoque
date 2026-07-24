{{ config(tags=["marts"]) }}
-- Fail if any race result references a constructor_id not present in dim_constructors.
SELECT DISTINCT fr.constructor_id
FROM {{ ref('fact_race_results') }} fr
LEFT JOIN {{ ref('dim_constructors') }} c
    ON  c.constructor_id = fr.constructor_id
WHERE c.constructor_id IS NULL

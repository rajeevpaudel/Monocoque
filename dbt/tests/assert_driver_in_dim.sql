-- Fail if any race result references a driver_id not present in dim_drivers.
SELECT DISTINCT fr.jolpica_driver_id
FROM {{ ref('fact_race_results') }} fr
LEFT JOIN {{ ref('dim_drivers') }} d
    ON  d.driver_id = fr.jolpica_driver_id
WHERE d.driver_id IS NULL

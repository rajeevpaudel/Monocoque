{{
    config(
        materialized='table',
        engine='MergeTree()',
        order_by='(season, jolpica_driver_id)',
    )
}}

-- Maps Jolpica slug IDs to OpenF1 driver numbers per season.
-- Primary match: first 3 chars of jolpica_driver_id == OpenF1 name_acronym.
-- Exceptions CTE handles drivers where the slug prefix doesn't match their acronym.
WITH

exceptions AS (
    SELECT jolpica_driver_id, openf1_acronym FROM (
        SELECT
            'max_verstappen'  AS jolpica_driver_id, 'VER' AS openf1_acronym
        UNION ALL SELECT 'kevin_magnussen',  'MAG'
        UNION ALL SELECT 'de_vries',         'DEV'
        UNION ALL SELECT 'ricciardo',        'RIC'
        UNION ALL SELECT 'lawson',           'LAW'
        UNION ALL SELECT 'drugovich',        'DRU'
        UNION ALL SELECT 'bearman',          'BEA'
        UNION ALL SELECT 'colapinto',        'COL'
        UNION ALL SELECT 'doohan',           'DOO'
        UNION ALL SELECT 'hadjar',           'HAD'
        UNION ALL SELECT 'bortoleto',        'BOR'
    )
),

jolpica_drivers AS (
    SELECT DISTINCT season, driver_id
    FROM {{ source('raw_jolpica', 'results') }}
),

openf1_drivers AS (
    SELECT DISTINCT
        s.year                          AS season,
        d.driver_number,
        d.name_acronym
    FROM {{ source('raw_openf1', 'drivers') }}  d
    JOIN {{ source('raw_openf1', 'sessions') }} s ON s.session_key = d.session_key
    WHERE s.session_type IN ('Race', 'Qualifying')
      AND d.driver_number > 0
)

SELECT
    j.driver_id     AS jolpica_driver_id,
    o.driver_number AS openf1_driver_number,
    j.season        AS season
FROM jolpica_drivers j
LEFT JOIN exceptions e ON e.jolpica_driver_id = j.driver_id
JOIN openf1_drivers o
    ON  o.season = j.season
    AND o.name_acronym = coalesce(nullIf(e.openf1_acronym, ''), upper(substring(j.driver_id, 1, 3)))

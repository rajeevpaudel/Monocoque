-- Canonical driver dimension. Jolpica is the system of record.
{{
    config(
        engine='ReplacingMergeTree()',
        order_by='driver_id',
    )
}}

SELECT
    driver_id,
    given_name,
    family_name,
    full_name,
    date_of_birth,
    nationality,
    permanent_number,
    driver_code,
    url
FROM {{ ref('stg_jolpica__drivers') }}

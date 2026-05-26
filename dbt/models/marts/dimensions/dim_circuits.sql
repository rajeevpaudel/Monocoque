{{
    config(
        engine='ReplacingMergeTree()',
        order_by='circuit_id',
    )
}}

SELECT
    circuit_id,
    circuit_name,
    locality,
    country,
    latitude,
    longitude,
    altitude_m,
    url
FROM {{ ref('stg_jolpica__circuits') }}

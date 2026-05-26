{{
    config(
        engine='ReplacingMergeTree()',
        order_by='constructor_id',
    )
}}

SELECT
    constructor_id,
    constructor_name,
    nationality,
    url
FROM {{ ref('stg_jolpica__constructors') }}

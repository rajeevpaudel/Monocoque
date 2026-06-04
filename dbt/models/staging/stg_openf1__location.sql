{{
    config(
        materialized='incremental',
        incremental_strategy='append',
        engine='MergeTree()',
        partition_by='(toYear(date), session_key)',
        order_by='(session_key, driver_number, date)',
    )
}}

SELECT
    session_key,
    driver_number,
    toDateTime64(date, 3)   AS date,
    x,
    y,
    z,
    _ingested_at
FROM {{ source('raw_openf1', 'location') }}

{% if is_incremental() %}
WHERE _ingested_at > (SELECT max(_ingested_at) FROM {{ this }})
{% endif %}

{{
    config(
        materialized='incremental',
        incremental_strategy='append',
        engine='MergeTree()',
        order_by='(session_key, driver_number, date)',
    )
}}

SELECT
    session_key,
    driver_number,
    toDateTime64(date, 3, 'UTC') AS date,
    distance_m,
    _ingested_at
FROM {{ source('raw_fastf1', 'car_telemetry') }}

{% if is_incremental() %}
WHERE _ingested_at > (SELECT max(_ingested_at) FROM {{ this }})
{% endif %}

-- Canonical driver dimension. Jolpica is the system of record.
-- headshot_url sourced from OpenF1 (most recent session per driver). NULL pre-2023.
{{
    config(
        engine='ReplacingMergeTree()',
        order_by='driver_id',
    )
}}

WITH latest_headshot AS (
    -- Only use 2023+ mappings — pre-2023 driver numbers are recycled across eras
    -- and would match the wrong modern driver's headshot.
    SELECT
        jmap.jolpica_driver_id                        AS driver_id,
        argMax(od.headshot_url, od.session_key)       AS headshot_url
    FROM {{ source('dim', 'driver_id_map') }}         jmap
    JOIN {{ ref('stg_openf1__drivers') }}             od
        ON  od.driver_number = jmap.openf1_driver_number
    WHERE jmap.season >= 2023
      AND od.headshot_url != ''
    GROUP BY jmap.jolpica_driver_id
)

SELECT
    d.driver_id,
    d.given_name,
    d.family_name,
    d.full_name,
    d.date_of_birth,
    d.nationality,
    d.permanent_number,
    d.driver_code,
    d.url,
    h.headshot_url
FROM {{ ref('stg_jolpica__drivers') }}  d
LEFT JOIN latest_headshot               h ON h.driver_id = d.driver_id

-- Canonical driver dimension. Jolpica is the system of record.
-- headshot_url sourced from OpenF1 (most recent session per driver). NULL pre-2023.
{{
    config(
        engine='ReplacingMergeTree()',
        order_by='driver_id',
    )
}}

WITH latest_headshot AS (
    -- Join through int_session_map to scope driver_number by season.
    -- Without this, a number reused by a different driver in a later season
    -- (e.g. #1 passing from one champion to the next) would match the wrong sessions.
    SELECT
        jmap.jolpica_driver_id                        AS driver_id,
        argMax(od.headshot_url, od.session_key)       AS headshot_url
    FROM {{ ref('driver_id_map') }}         jmap
    JOIN {{ ref('int_session_map') }}                 sm
        ON  sm.season = jmap.season
    JOIN {{ ref('stg_openf1__drivers') }}             od
        ON  od.driver_number = jmap.openf1_driver_number
        AND od.session_key   = sm.session_key
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

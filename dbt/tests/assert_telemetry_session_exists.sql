{{ config(tags=["marts"]) }}
-- Fail if any session_key in mart_lap_telemetry has no entry in dim_sessions.
SELECT DISTINCT mlt.session_key
FROM {{ ref('mart_lap_telemetry') }} mlt
LEFT JOIN {{ ref('dim_sessions') }} ds
    ON  ds.session_key = mlt.session_key
WHERE ds.session_key IS NULL

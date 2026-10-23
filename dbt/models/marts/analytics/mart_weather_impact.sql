-- Grain: one row per Race / Qualifying / Sprint session with weather data (2023+).
-- is_wet = rainfall in ≥5% of weather samples (ignores brief drizzle) or ≥10% of race laps on INTERMEDIATE/WET tyres.
-- pace_delta_vs_dry_pct compares this session's pace (race: field median clean lap;
-- qualifying: pole lap) with the dry-session average for the same circuit and session type.
{{ config(materialized='table', order_by='(season, session_key)') }}

WITH wx AS (
    SELECT
        session_key,
        count()                                    AS samples,
        round(avg(air_temperature), 1)             AS avg_air_temp_c,
        round(avg(track_temperature), 1)           AS avg_track_temp_c,
        round(min(track_temperature), 1)           AS min_track_temp_c,
        round(max(track_temperature), 1)           AS max_track_temp_c,
        round(avg(humidity), 1)                    AS avg_humidity_pct,
        round(avg(wind_speed), 2)                  AS avg_wind_speed_ms,
        round(avg(rainfall), 3)                    AS rain_sample_share
    FROM {{ ref('stg_openf1__weather') }}
    GROUP BY session_key
),
race_pace AS (
    SELECT
        session_key,
        medianIf(lap_s, is_clean = 1)              AS field_median_lap_s,
        round(countIf(compound IN ('INTERMEDIATE', 'WET')) / count(), 3) AS wet_tyre_lap_share
    FROM {{ ref('int_race_laps_clean') }}
    WHERE session_key IS NOT NULL
    GROUP BY session_key
),
quali_pace AS (
    SELECT session_key, min(best_lap_duration) AS pole_lap_s
    FROM {{ ref('fact_qualifying') }}
    WHERE session_key IS NOT NULL
    GROUP BY session_key
),
sessions AS (
    SELECT
        s.session_key                              AS session_key,
        s.season                                   AS season,
        s.round                                    AS round,
        s.race_name                                AS race_name,
        s.circuit_id                               AS circuit_id,
        s.session_type                             AS session_type,
        s.session_name                             AS session_name,
        s.date_start                               AS date_start,
        w.samples, w.avg_air_temp_c, w.avg_track_temp_c, w.min_track_temp_c, w.max_track_temp_c,
        w.avg_humidity_pct, w.avg_wind_speed_ms, w.rain_sample_share,
        rp.wet_tyre_lap_share                      AS wet_tyre_lap_share,
        toUInt8(w.rain_sample_share > 0)           AS any_rain,
        toUInt8(w.rain_sample_share >= 0.05 OR ifNull(rp.wet_tyre_lap_share, 0) >= 0.1) AS is_wet,
        coalesce(rp.field_median_lap_s, qp.pole_lap_s) AS pace_s
    FROM {{ ref('dim_sessions') }} s
    JOIN wx w               ON w.session_key = s.session_key
    LEFT JOIN race_pace rp  ON rp.session_key = s.session_key
    LEFT JOIN quali_pace qp ON qp.session_key = s.session_key
    WHERE s.session_type IN ('Race', 'Qualifying')
)
SELECT
    s.*,
    dry.dry_pace_s                                 AS circuit_dry_pace_s,
    round(100 * (s.pace_s - dry.dry_pace_s) / dry.dry_pace_s, 2) AS pace_delta_vs_dry_pct
FROM sessions s
LEFT JOIN (
    SELECT circuit_id, session_name, avg(pace_s) AS dry_pace_s
    FROM sessions
    WHERE is_wet = 0 AND pace_s > 0
    GROUP BY circuit_id, session_name
) dry ON dry.circuit_id = s.circuit_id AND dry.session_name = s.session_name

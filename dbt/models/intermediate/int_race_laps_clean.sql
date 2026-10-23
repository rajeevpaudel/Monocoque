-- Grain: one row per driver per race lap (OpenF1/Jolpica lap era, 2023+).
-- Flags "clean" racing laps for pace and tyre analysis. A lap is NOT clean if it is:
--   lap 1, a pit in-lap or out-lap, a neutralised lap (field median > 115% of the race's
--   typical lap — SC/VSC/red flag), or slower than 107% of the driver's own race median.
-- fuel_corrected_lap_s removes an assumed 0.03 s/lap fuel-burn effect so tyre degradation
-- slopes are not masked by the car getting lighter.
{{ config(materialized='table', order_by='(season, round, driver_id, lap_number)') }}

WITH laps AS (
    SELECT
        l.season                                   AS season,
        l.round                                    AS round,
        l.driver_id                                AS driver_id,
        l.lap_number                               AS lap_number,
        l.lap_position                             AS lap_position,
        l.session_key                              AS session_key,
        l.openf1_driver_number                     AS driver_number,
        coalesce(l.of1_lap_duration, l.lap_time_ms / 1000.0) AS lap_s,
        ifNull(l.is_pit_out_lap, 0)                AS of1_pit_out
    FROM {{ ref('fact_laps') }} l
    WHERE l.lap_time_ms > 0
),
pits AS (
    SELECT season, round, driver_id, groupUniqArray(lap_number) AS in_laps
    FROM {{ ref('fact_pit_stops') }}
    GROUP BY season, round, driver_id
),
stint_laps AS (
    -- OpenF1 stints share the pit lap (stint N lap_end = stint N+1 lap_start); that in-lap
    -- is driven on the old tyres, so it is assigned to the earlier stint.
    SELECT
        season, round, driver_id, lap_number,
        min(st_num)                                AS stint_number,
        argMin(st_compound, st_num)                AS compound,
        argMin(st_tyre_age, st_num)                AS tyre_age
    FROM (
        SELECT
            season                                 AS season,
            round                                  AS round,
            driver_id                              AS driver_id,
            stint_number                           AS st_num,
            compound                               AS st_compound,
            arrayJoin(range(toUInt16(lap_start), toUInt16(ifNull(lap_end, lap_start)) + 1)) AS lap_number,
            tyre_age_at_start + (lap_number - lap_start) AS st_tyre_age
        FROM {{ ref('mart_stints') }}
        WHERE driver_id IS NOT NULL
    )
    GROUP BY season, round, driver_id, lap_number
),
race_meta AS (
    SELECT season, round, max(lap_number) AS race_laps, median(lap_s) AS race_median_s
    FROM laps
    GROUP BY season, round
),
lap_field AS (
    SELECT season, round, lap_number, median(lap_s) AS field_median_s
    FROM laps
    GROUP BY season, round, lap_number
),
flagged AS (
    SELECT
        l.season AS season, l.round AS round, l.driver_id AS driver_id, l.lap_number AS lap_number,
        l.lap_position AS lap_position, l.session_key AS session_key,
        l.driver_number AS driver_number, l.lap_s AS lap_s,
        rm.race_laps                               AS race_laps,
        has(ifNull(p.in_laps, []), l.lap_number)   AS is_in_lap,
        (l.of1_pit_out = 1 OR has(ifNull(p.in_laps, []), toUInt8(l.lap_number - 1))) AS is_out_lap,
        lf.field_median_s > rm.race_median_s * 1.15 AS is_neutralised
    FROM laps l
    JOIN race_meta rm ON rm.season = l.season AND rm.round = l.round
    JOIN lap_field lf ON lf.season = l.season AND lf.round = l.round AND lf.lap_number = l.lap_number
    LEFT JOIN pits p  ON p.season = l.season AND p.round = l.round AND p.driver_id = l.driver_id
),
driver_median AS (
    SELECT season, round, driver_id, median(lap_s) AS drv_median_s
    FROM flagged
    WHERE lap_number > 1 AND NOT is_in_lap AND NOT is_out_lap AND NOT is_neutralised
    GROUP BY season, round, driver_id
)
SELECT
    f.season                                       AS season,
    f.round                                        AS round,
    f.session_key                                  AS session_key,
    f.driver_id                                    AS driver_id,
    f.driver_number                                AS driver_number,
    fr.constructor_id                              AS constructor_id,
    f.lap_number                                   AS lap_number,
    f.lap_position                                 AS lap_position,
    toFloat64(f.lap_s)                             AS lap_s,
    toFloat64(f.lap_s) - 0.03 * (f.race_laps - f.lap_number) AS fuel_corrected_lap_s,
    sl.stint_number                                AS stint_number,
    sl.compound                                    AS compound,
    sl.tyre_age                                    AS tyre_age,
    toUInt8(f.is_in_lap)                           AS is_in_lap,
    toUInt8(f.is_out_lap)                          AS is_out_lap,
    toUInt8(f.is_neutralised)                      AS is_neutralised,
    toUInt8(
        f.lap_number > 1
        AND NOT f.is_in_lap
        AND NOT f.is_out_lap
        AND NOT f.is_neutralised
        AND f.lap_s <= dm.drv_median_s * 1.07
    )                                              AS is_clean
FROM flagged f
LEFT JOIN driver_median dm
    ON dm.season = f.season AND dm.round = f.round AND dm.driver_id = f.driver_id
LEFT JOIN {{ ref('fact_race_results') }} fr
    ON fr.season = f.season AND fr.round = f.round AND fr.driver_id = f.driver_id
LEFT JOIN stint_laps sl
    ON sl.season = f.season AND sl.round = f.round AND sl.driver_id = f.driver_id
   AND sl.lap_number = f.lap_number

-- Grain: one row per driver per race (all eras for grid→finish; lap-chart stats 2023+;
-- OpenF1 position-feed stats where raw_openf1.position has been ingested).
-- Pit-lane starts (grid 0) are treated as starting last.
-- on_track_gains counts position improvements between consecutive laps excluding the
-- driver's own in/out laps — a proxy for overtakes made (also includes rivals pitting).
-- live_position_gains counts improvements in OpenF1's ~4 Hz position feed during the race.
{{ config(materialized='table', order_by='(season, round, driver_id)') }}

WITH res AS (
    SELECT
        season, round, driver_id, constructor_id, grid_position, finish_position, position_text,
        session_key,
        if(ifNull(grid_position, 0) = 0, count() OVER (PARTITION BY season, round), grid_position) AS start_pos
    FROM {{ ref('fact_race_results') }}
),
lap_chart AS (
    SELECT
        season, round, driver_id,
        countIf(lap_position = 1)                  AS laps_led,
        min(lap_position)                          AS best_running_position,
        max(lap_position)                          AS worst_running_position,
        sumIf(greatest(prev_pos - lap_position, 0), prev_pos > 0 AND is_in_lap = 0 AND is_out_lap = 0) AS on_track_gains,
        sumIf(greatest(lap_position - prev_pos, 0), prev_pos > 0 AND is_in_lap = 0 AND is_out_lap = 0) AS on_track_losses,
        argMinIf(lap_position, lap_number, lap_number = 1) AS lap1_position
    FROM (
        SELECT
            season, round, driver_id, lap_number, lap_position, is_in_lap, is_out_lap,
            lagInFrame(lap_position, 1, 0) OVER (
                PARTITION BY season, round, driver_id ORDER BY lap_number
                ROWS BETWEEN 1 PRECEDING AND CURRENT ROW
            ) AS prev_pos
        FROM {{ ref('int_race_laps_clean') }}
        WHERE lap_position > 0
    )
    GROUP BY season, round, driver_id
),
live AS (
    SELECT
        session_key, driver_number,
        countIf(prev > 0 AND position < prev)      AS live_position_gains,
        countIf(prev > 0 AND position > prev)      AS live_position_losses
    FROM (
        SELECT
            session_key, driver_number, position,
            lagInFrame(position, 1, 0) OVER (
                PARTITION BY session_key, driver_number ORDER BY position_date
                ROWS BETWEEN 1 PRECEDING AND CURRENT ROW
            ) AS prev
        FROM {{ ref('stg_openf1__position') }}
    )
    GROUP BY session_key, driver_number
)
SELECT
    r.season                                       AS season,
    r.round                                        AS round,
    r.driver_id                                    AS driver_id,
    r.constructor_id                               AS constructor_id,
    r.grid_position                                AS grid_position,
    r.finish_position                              AS finish_position,
    toUInt8(match(r.position_text, '^[0-9]+$'))    AS is_classified,
    if(is_classified = 1, toInt16(r.start_pos) - toInt16(r.finish_position), NULL) AS positions_gained,
    lc.lap1_position                               AS lap1_position,
    if(lc.lap1_position > 0, toInt16(r.start_pos) - toInt16(lc.lap1_position), NULL) AS lap1_positions_gained,
    lc.laps_led                                    AS laps_led,
    lc.best_running_position                       AS best_running_position,
    lc.worst_running_position                      AS worst_running_position,
    lc.on_track_gains                              AS on_track_gains,
    lc.on_track_losses                             AS on_track_losses,
    lv.live_position_gains                         AS live_position_gains,
    lv.live_position_losses                        AS live_position_losses
FROM res r
LEFT JOIN lap_chart lc ON lc.season = r.season AND lc.round = r.round AND lc.driver_id = r.driver_id
LEFT JOIN {{ ref('driver_id_map') }} m
    ON m.season = r.season AND m.jolpica_driver_id = r.driver_id
LEFT JOIN live lv ON lv.session_key = r.session_key AND lv.driver_number = m.openf1_driver_number

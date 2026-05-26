-- ============================================================
-- F1 Warehouse — Metabase Dashboard Queries
-- Use these as Native SQL questions in Metabase.
-- Add {{year}}, {{round}}, {{driver_1}}, {{driver_2}} as
-- Metabase variables (Text or Number type as noted).
-- ============================================================


-- ============================================================
-- DASHBOARD 1: Season Overview
-- ClickHouse connection: f1_mart
-- ============================================================

-- Card: Driver Championship Standings (final standings for the year)
-- Variable: {{year}} Number
SELECT
    standing_position,
    driver_id,
    driver_name,
    constructor_id,
    constructor_name,
    points,
    wins
FROM f1_mart.mart_standings
WHERE season = {{year}}
  AND round = (
      SELECT max(round) FROM f1_mart.mart_standings WHERE season = {{year}}
  )
ORDER BY standing_position;


-- Card: Points Progression by Round (top 5 drivers)
-- Variable: {{year}} Number
SELECT
    round,
    driver_name,
    points
FROM f1_mart.mart_standings
WHERE season = {{year}}
  AND driver_id IN (
      SELECT driver_id
      FROM f1_mart.mart_standings
      WHERE season = {{year}}
        AND round = (SELECT max(round) FROM f1_mart.mart_standings WHERE season = {{year}})
      ORDER BY standing_position
      LIMIT 5
  )
ORDER BY round, standing_position;


-- Card: Constructor Standings (final)
-- Variable: {{year}} Number
SELECT
    constructor_name,
    constructor_points AS points,
    constructor_position AS position
FROM f1_mart.mart_standings
WHERE season = {{year}}
  AND round = (
      SELECT max(round) FROM f1_mart.mart_standings WHERE season = {{year}}
  )
GROUP BY constructor_name, constructor_points, constructor_position
ORDER BY constructor_position;


-- Card: Podiums by Driver (finish position <= 3)
-- Variable: {{year}} Number
SELECT
    jolpica_driver_id AS driver,
    countIf(finish_position = 1) AS wins,
    countIf(finish_position <= 3) AS podiums
FROM f1_mart.fact_race_results
WHERE season = {{year}}
GROUP BY driver
ORDER BY wins DESC, podiums DESC;


-- ============================================================
-- DASHBOARD 2: Race Deep-Dive
-- ============================================================

-- Card: Race Result & Position Changes
-- Variables: {{year}} Number, {{round}} Number
SELECT
    jolpica_driver_id AS driver,
    grid_position,
    finish_position,
    toInt16(grid_position) - toInt16(finish_position) AS positions_gained,
    points,
    status
FROM f1_mart.fact_race_results
WHERE season = {{year}}
  AND round = {{round}}
ORDER BY finish_position;


-- Card: Lap Times by Driver (in seconds)
-- Variables: {{year}} Number, {{round}} Number
SELECT
    lap_number,
    driver_id,
    round(lap_time_ms / 1000.0, 3) AS lap_time_seconds
FROM f1_mart.mart_lap_analysis
WHERE season = {{year}}
  AND round = {{round}}
  AND lap_time_ms > 0
ORDER BY lap_number, driver_id;


-- Card: Pit Strategy
-- Variables: {{year}} Number, {{round}} Number
SELECT
    driver_id,
    stop_number,
    pit_lap,
    compound,
    stint_number,
    lap_start,
    lap_end,
    round(pit_duration_ms / 1000.0, 2) AS pit_duration_seconds
FROM f1_mart.mart_strategy
WHERE season = {{year}}
  AND round = {{round}}
ORDER BY driver_id, stop_number;


-- Card: Fastest Lap Sector Breakdown per Driver
-- Variables: {{year}} Number, {{round}} Number
SELECT
    driver_id,
    min(lap_time_ms) AS fastest_lap_ms,
    round(min(lap_time_ms) / 1000.0, 3) AS fastest_lap_seconds,
    any(of1_s1) AS s1,
    any(of1_s2) AS s2,
    any(of1_s3) AS s3
FROM f1_mart.mart_lap_analysis
WHERE season = {{year}}
  AND round = {{round}}
  AND lap_time_ms = (
      SELECT min(lap_time_ms)
      FROM f1_mart.mart_lap_analysis m2
      WHERE m2.season = mart_lap_analysis.season
        AND m2.round = mart_lap_analysis.round
        AND m2.driver_id = mart_lap_analysis.driver_id
  )
GROUP BY driver_id
ORDER BY fastest_lap_ms;


-- ============================================================
-- DASHBOARD 3: Driver Comparison
-- ============================================================

-- Card: Points Trajectory by Round
-- Variables: {{year}} Number, {{driver_1}} Text, {{driver_2}} Text
SELECT
    round,
    driver_name,
    points
FROM f1_mart.mart_standings
WHERE season = {{year}}
  AND driver_id IN ({{driver_1}}, {{driver_2}})
ORDER BY round, driver_id;


-- Card: Average Finish Position
-- Variables: {{year}} Number, {{driver_1}} Text, {{driver_2}} Text
SELECT
    jolpica_driver_id AS driver,
    round(avg(finish_position), 2) AS avg_finish,
    count() AS races
FROM f1_mart.fact_race_results
WHERE season = {{year}}
  AND jolpica_driver_id IN ({{driver_1}}, {{driver_2}})
  AND finish_position IS NOT NULL
GROUP BY driver
ORDER BY avg_finish;


-- Card: Qualifying vs Race Position
-- Variables: {{year}} Number, {{driver_1}} Text, {{driver_2}} Text
SELECT
    q.round,
    q.driver_id,
    q.qualifying_position,
    r.finish_position,
    toInt16(q.qualifying_position) - toInt16(r.finish_position) AS positions_gained_vs_qualifying
FROM f1_mart.fact_qualifying q
JOIN f1_mart.fact_race_results r
    ON r.season = q.season
    AND r.round = q.round
    AND r.jolpica_driver_id = q.driver_id
WHERE q.season = {{year}}
  AND q.driver_id IN ({{driver_1}}, {{driver_2}})
ORDER BY q.round, q.driver_id;


-- Card: Head-to-Head Race Record
-- Variables: {{year}} Number, {{driver_1}} Text, {{driver_2}} Text
SELECT
    r1.jolpica_driver_id AS driver,
    countIf(r1.finish_position < r2.finish_position) AS head_to_head_wins
FROM f1_mart.fact_race_results r1
JOIN f1_mart.fact_race_results r2
    ON r2.season = r1.season
    AND r2.round = r1.round
    AND r2.jolpica_driver_id != r1.jolpica_driver_id
    AND r2.jolpica_driver_id IN ({{driver_1}}, {{driver_2}})
WHERE r1.season = {{year}}
  AND r1.jolpica_driver_id IN ({{driver_1}}, {{driver_2}})
  AND r1.finish_position IS NOT NULL
  AND r2.finish_position IS NOT NULL
GROUP BY driver
ORDER BY head_to_head_wins DESC;

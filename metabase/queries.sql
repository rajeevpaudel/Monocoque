-- ============================================================
-- F1 Warehouse — Metabase Dashboard Queries
-- Use these as Native SQL questions in Metabase.
-- Variables: add as Field Filters or plain variables
--   {{year}}           Number
--   {{round}}          Number
--   {{driver_1}}       Text  (e.g. hamilton)
--   {{driver_2}}       Text  (e.g. max_verstappen)
--   {{constructor_id}} Text  (e.g. mercedes)
-- ============================================================


-- ============================================================
-- DASHBOARD 1: Season Overview
-- ============================================================

-- Card: Driver Championship Standings
-- Type: Table  |  Variable: {{year}}
SELECT
    standing_position   AS pos,
    driver_name,
    constructor_name,
    points,
    wins
FROM f1_mart.mart_standings
WHERE season = {{year}}
  AND round = (SELECT max(round) FROM f1_mart.mart_standings WHERE season = {{year}})
ORDER BY standing_position;


-- Card: Points Progression — Top 5 Drivers
-- Type: Line chart (x=round, y=points, series=driver_name)  |  Variable: {{year}}
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
ORDER BY round, standing_position NULLS LAST;


-- Card: Constructor Standings
-- Type: Bar chart (x=constructor_name, y=points)  |  Variable: {{year}}
SELECT
    constructor_name,
    constructor_points  AS points,
    constructor_position AS pos
FROM f1_mart.mart_standings
WHERE season = {{year}}
  AND round = (SELECT max(round) FROM f1_mart.mart_standings WHERE season = {{year}})
GROUP BY constructor_name, constructor_points, constructor_position
ORDER BY constructor_position;


-- Card: Wins & Podiums by Driver
-- Type: Bar chart (grouped)  |  Variable: {{year}}
SELECT
    jolpica_driver_id                           AS driver,
    countIf(finish_position = 1)                AS wins,
    countIf(finish_position <= 3)               AS podiums,
    countIf(finish_position <= 10)              AS points_finishes
FROM f1_mart.fact_race_results
WHERE season = {{year}}
GROUP BY driver
ORDER BY wins DESC, podiums DESC;


-- Card: Points Gap to Championship Leader by Round
-- Type: Line chart (x=round, y=gap, series=driver_name)  |  Variable: {{year}}
WITH leader AS (
    SELECT round, max(points) AS leader_points
    FROM f1_mart.mart_standings
    WHERE season = {{year}}
    GROUP BY round
)
SELECT
    s.round,
    s.driver_name,
    leader.leader_points - s.points AS gap_to_leader
FROM f1_mart.mart_standings s
JOIN leader ON leader.round = s.round
WHERE s.season = {{year}}
  AND s.standing_position <= 5
ORDER BY s.round, s.standing_position;


-- Card: DNF Rate by Constructor
-- Type: Bar chart  |  Variable: {{year}}
SELECT
    constructor_id,
    count()                                         AS starts,
    countIf(status NOT IN ('Finished', '+1 Lap', '+2 Laps', '+3 Laps', '+4 Laps', '+5 Laps', 'Lapped'))
                                                    AS dnfs,
    round(100.0 * countIf(status NOT IN ('Finished', '+1 Lap', '+2 Laps', '+3 Laps', '+4 Laps', '+5 Laps', 'Lapped')) / count(), 1)
                                                    AS dnf_pct
FROM f1_mart.fact_race_results
WHERE season = {{year}}
GROUP BY constructor_id
ORDER BY dnf_pct DESC;


-- ============================================================
-- DASHBOARD 2: Race Deep-Dive
-- ============================================================

-- Card: Race Result with Grid Changes
-- Type: Table  |  Variables: {{year}}, {{round}}
SELECT
    r.finish_position                               AS pos,
    r.jolpica_driver_id                             AS driver,
    r.constructor_id                                AS constructor,
    r.grid_position                                 AS grid,
    toInt16(r.grid_position) - toInt16(r.finish_position) AS gained,
    r.points,
    r.laps_completed                                AS laps,
    r.status
FROM f1_mart.fact_race_results r
WHERE r.season = {{year}}
  AND r.round  = {{round}}
ORDER BY r.finish_position;


-- Card: Lap Time Evolution — All Drivers
-- Type: Line chart (x=lap_number, y=lap_time_seconds, series=driver_id)
-- Variables: {{year}}, {{round}}
SELECT
    lap_number,
    driver_id,
    round(lap_time_ms / 1000.0, 3) AS lap_time_seconds
FROM f1_mart.fact_laps
WHERE season = {{year}}
  AND round  = {{round}}
  AND lap_time_ms BETWEEN 60000 AND 200000
ORDER BY lap_number, driver_id;


-- Card: Delta to Race Fastest Lap per Driver
-- Type: Bar chart  |  Variables: {{year}}, {{round}}
SELECT
    driver_id,
    round(min(lap_time_ms) / 1000.0, 3)                         AS personal_best_s,
    round((min(lap_time_ms) - min(min(lap_time_ms)) OVER ()) / 1000.0, 3)
                                                                 AS delta_to_fastest_s
FROM f1_mart.fact_laps
WHERE season = {{year}}
  AND round  = {{round}}
  AND lap_time_ms BETWEEN 60000 AND 200000
GROUP BY driver_id
ORDER BY personal_best_s;


-- Card: Pit Strategy — Stint Gantt
-- Type: Table  |  Variables: {{year}}, {{round}}
SELECT
    driver_id,
    stint_number,
    compound,
    lap_start,
    lap_end,
    lap_end - lap_start                             AS stint_laps,
    tyre_age_at_start,
    round(pit_duration_ms / 1000.0, 2)              AS pit_duration_s
FROM f1_mart.mart_strategy
WHERE season = {{year}}
  AND round  = {{round}}
ORDER BY driver_id, stint_number;


-- Card: Pit Stop Duration Ranking
-- Type: Bar chart  |  Variables: {{year}}, {{round}}
SELECT
    driver_id,
    stop_number,
    round(pit_duration_ms / 1000.0, 2) AS pit_duration_s
FROM f1_mart.mart_strategy
WHERE season = {{year}}
  AND round  = {{round}}
  AND pit_duration_ms IS NOT NULL
ORDER BY pit_duration_s;


-- Card: Fastest Lap Sector Breakdown
-- Type: Bar chart (grouped S1/S2/S3)  |  Variables: {{year}}, {{round}}
SELECT
    driver_id,
    round(min(lap_time_ms) / 1000.0, 3) AS fastest_lap_s,
    round(argMin(of1_s1, lap_time_ms), 3) AS s1,
    round(argMin(of1_s2, lap_time_ms), 3) AS s2,
    round(argMin(of1_s3, lap_time_ms), 3) AS s3
FROM f1_mart.mart_lap_analysis
WHERE season = {{year}}
  AND round  = {{round}}
  AND lap_time_ms BETWEEN 60000 AND 200000
  AND of1_s1 IS NOT NULL
GROUP BY driver_id
ORDER BY fastest_lap_s;


-- ============================================================
-- DASHBOARD 3: Qualifying Analysis
-- ============================================================

-- Card: Qualifying Results with Sector Times
-- Type: Table  |  Variables: {{year}}, {{round}}
SELECT
    qualifying_position AS pos,
    driver_name,
    team_name,
    best_time,
    round(best_s1, 3)   AS s1,
    round(best_s2, 3)   AS s2,
    round(best_s3, 3)   AS s3,
    i1_speed,
    st_speed,
    best_source_match
FROM f1_mart.mart_qualifying_summary
WHERE season = {{year}}
  AND round  = {{round}}
ORDER BY qualifying_position;


-- Card: Sector Time Rankings (S1, S2, S3 separately)
-- Type: Bar chart  |  Variables: {{year}}, {{round}}
SELECT
    driver_name,
    round(best_s1, 3) AS s1,
    round(best_s2, 3) AS s2,
    round(best_s3, 3) AS s3
FROM f1_mart.mart_qualifying_summary
WHERE season  = {{year}}
  AND round   = {{round}}
  AND best_s1 IS NOT NULL
ORDER BY (best_s1 + best_s2 + best_s3);


-- Card: Speed Trap Comparison
-- Type: Bar chart  |  Variables: {{year}}, {{round}}
SELECT
    driver_name,
    i1_speed,
    i2_speed,
    st_speed
FROM f1_mart.mart_qualifying_summary
WHERE season   = {{year}}
  AND round    = {{round}}
  AND st_speed IS NOT NULL
ORDER BY st_speed DESC;


-- Card: Pole Positions by Driver this Season
-- Type: Bar chart  |  Variable: {{year}}
SELECT
    driver_name,
    count() AS poles
FROM f1_mart.mart_qualifying_summary
WHERE season              = {{year}}
  AND qualifying_position = 1
GROUP BY driver_name
ORDER BY poles DESC;


-- Card: Pole Positions by Constructor this Season
-- Type: Bar chart  |  Variable: {{year}}
SELECT
    constructor_name,
    count() AS poles
FROM f1_mart.mart_qualifying_summary
WHERE season              = {{year}}
  AND qualifying_position = 1
GROUP BY constructor_name
ORDER BY poles DESC;


-- Card: Average Qualifying Position by Constructor
-- Type: Bar chart  |  Variable: {{year}}
SELECT
    constructor_name,
    round(avg(qualifying_position), 2) AS avg_qual_pos,
    count()                            AS entries
FROM f1_mart.mart_qualifying_summary
WHERE season = {{year}}
GROUP BY constructor_name
ORDER BY avg_qual_pos;


-- Card: Qualifying-to-Race Position Delta by Driver
-- Type: Bar chart (avg)  |  Variable: {{year}}
SELECT
    q.driver_name,
    round(avg(toInt16(q.qualifying_position) - toInt16(r.finish_position)), 2) AS avg_positions_gained,
    count() AS races
FROM f1_mart.mart_qualifying_summary q
JOIN f1_mart.fact_race_results r
    ON  r.season            = q.season
    AND r.round             = q.round
    AND r.jolpica_driver_id = q.driver_id
WHERE q.season = {{year}}
  AND r.finish_position IS NOT NULL
GROUP BY q.driver_name
ORDER BY avg_positions_gained DESC;


-- ============================================================
-- DASHBOARD 4: Constructor Battle
-- ============================================================

-- Card: Constructor Points Gap to Leader by Round
-- Type: Line chart (x=round, y=gap, series=constructor_name)  |  Variable: {{year}}
WITH leader AS (
    SELECT round, max(constructor_points) AS top
    FROM f1_mart.mart_standings
    WHERE season = {{year}}
    GROUP BY round
)
SELECT
    s.round,
    s.constructor_name,
    leader.top - s.constructor_points AS gap
FROM f1_mart.mart_standings s
JOIN leader ON leader.round = s.round
WHERE s.season = {{year}}
  AND s.constructor_position <= 5
GROUP BY s.round, s.constructor_name, s.constructor_position, s.constructor_points, leader.top
ORDER BY s.round, s.constructor_position;


-- Card: Race Wins by Constructor per Round (cumulative)
-- Type: Bar chart (stacked)  |  Variable: {{year}}
SELECT
    round,
    constructor_id,
    countIf(finish_position = 1) AS wins_this_round
FROM f1_mart.fact_race_results
WHERE season = {{year}}
GROUP BY round, constructor_id
ORDER BY round;


-- Card: Constructor Average Finish Position by Round
-- Type: Line chart  |  Variable: {{year}}
SELECT
    round,
    constructor_id,
    round(avg(finish_position), 2) AS avg_finish
FROM f1_mart.fact_race_results
WHERE season         = {{year}}
  AND finish_position IS NOT NULL
GROUP BY round, constructor_id
ORDER BY round, avg_finish;


-- Card: Both Drivers in Points — Constructor Reliability
-- Type: Bar chart  |  Variable: {{year}}
SELECT
    constructor_id,
    count()                                        AS race_entries,
    countIf(finish_position <= 10)                 AS points_finishes,
    countIf(status NOT IN ('Finished','+1 Lap','+2 Laps','+3 Laps','+4 Laps','+5 Laps'))
                                                   AS dnfs,
    round(avg(finish_position), 1)                 AS avg_finish
FROM f1_mart.fact_race_results
WHERE season = {{year}}
GROUP BY constructor_id
ORDER BY avg_finish;


-- Card: Constructor Qualifying 1-2 Lockouts
-- Type: Table  |  Variable: {{year}}
SELECT
    round,
    race_name,
    constructor_name,
    groupArray(driver_name)  AS drivers,
    groupArray(qualifying_position) AS positions
FROM f1_mart.mart_qualifying_summary
WHERE season              = {{year}}
  AND qualifying_position <= 2
GROUP BY round, race_name, constructor_name
HAVING count() = 2
ORDER BY round;


-- ============================================================
-- DASHBOARD 5: Driver Head-to-Head
-- ============================================================

-- Card: Points Trajectory
-- Type: Line chart  |  Variables: {{year}}, {{driver_1}}, {{driver_2}}
SELECT
    round,
    driver_name,
    points
FROM f1_mart.mart_standings
WHERE season    = {{year}}
  AND driver_id IN ({{driver_1}}, {{driver_2}})
ORDER BY round, driver_id;


-- Card: Race-by-Race Finishing Positions
-- Type: Line chart  |  Variables: {{year}}, {{driver_1}}, {{driver_2}}
SELECT
    r.round,
    r.jolpica_driver_id AS driver_id,
    r.finish_position
FROM f1_mart.fact_race_results r
WHERE r.season            = {{year}}
  AND r.jolpica_driver_id IN ({{driver_1}}, {{driver_2}})
  AND r.finish_position IS NOT NULL
ORDER BY r.round;


-- Card: Head-to-Head Race Record
-- Type: Number cards  |  Variables: {{year}}, {{driver_1}}, {{driver_2}}
SELECT
    r1.jolpica_driver_id                                    AS driver,
    countIf(r1.finish_position < r2.finish_position)        AS h2h_wins,
    round(avg(r1.finish_position), 2)                       AS avg_finish,
    countIf(r1.finish_position = 1)                         AS race_wins,
    sum(r1.points)                                          AS total_points
FROM f1_mart.fact_race_results r1
JOIN f1_mart.fact_race_results r2
    ON  r2.season             = r1.season
    AND r2.round              = r1.round
    AND r2.jolpica_driver_id != r1.jolpica_driver_id
    AND r2.jolpica_driver_id IN ({{driver_1}}, {{driver_2}})
WHERE r1.season             = {{year}}
  AND r1.jolpica_driver_id IN ({{driver_1}}, {{driver_2}})
  AND r1.finish_position   IS NOT NULL
  AND r2.finish_position   IS NOT NULL
GROUP BY driver
ORDER BY h2h_wins DESC;


-- Card: Qualifying Head-to-Head
-- Type: Table  |  Variables: {{year}}, {{driver_1}}, {{driver_2}}
SELECT
    q1.round,
    q1.race_name,
    q1.driver_name                          AS d1_name,
    q1.qualifying_position                  AS d1_pos,
    q2.qualifying_position                  AS d2_pos,
    q2.driver_name                          AS d2_name,
    if(q1.qualifying_position < q2.qualifying_position, q1.driver_name, q2.driver_name) AS faster
FROM f1_mart.mart_qualifying_summary q1
JOIN f1_mart.mart_qualifying_summary q2
    ON  q2.season    = q1.season
    AND q2.round     = q1.round
    AND q2.driver_id = {{driver_2}}
WHERE q1.season    = {{year}}
  AND q1.driver_id = {{driver_1}}
ORDER BY q1.round;


-- Card: Sector Time Comparison across Rounds (2023+ only)
-- Type: Table  |  Variables: {{year}}, {{driver_1}}, {{driver_2}}
SELECT
    q1.round,
    q1.race_name,
    round(q1.best_s1, 3) AS d1_s1, round(q2.best_s1, 3) AS d2_s1,
    round(q1.best_s2, 3) AS d1_s2, round(q2.best_s2, 3) AS d2_s2,
    round(q1.best_s3, 3) AS d1_s3, round(q2.best_s3, 3) AS d2_s3,
    round(q1.best_lap_duration, 3) AS d1_lap,
    round(q2.best_lap_duration, 3) AS d2_lap,
    round(q1.best_lap_duration - q2.best_lap_duration, 3) AS delta_s
FROM f1_mart.mart_qualifying_summary q1
JOIN f1_mart.mart_qualifying_summary q2
    ON  q2.season    = q1.season
    AND q2.round     = q1.round
    AND q2.driver_id = {{driver_2}}
WHERE q1.season        = {{year}}
  AND q1.driver_id     = {{driver_1}}
  AND q1.best_s1      IS NOT NULL
ORDER BY q1.round;


-- ============================================================
-- DASHBOARD 6: All-Time Records
-- ============================================================

-- Card: All-Time Race Wins Leaders
-- Type: Bar chart
SELECT
    d.full_name     AS driver,
    d.nationality,
    count()         AS wins
FROM f1_mart.fact_race_results r
JOIN f1_mart.dim_drivers d ON d.driver_id = r.jolpica_driver_id
WHERE r.finish_position = 1
GROUP BY driver, d.nationality
ORDER BY wins DESC
LIMIT 25;


-- Card: All-Time Pole Positions
-- Type: Bar chart
SELECT
    driver_name,
    count() AS poles
FROM f1_mart.mart_qualifying_summary
WHERE qualifying_position = 1
GROUP BY driver_name
ORDER BY poles DESC
LIMIT 25;


-- Card: Most Championship Points in a Single Season
-- Type: Table
SELECT
    season,
    driver_name,
    constructor_name,
    points
FROM f1_mart.mart_standings
WHERE round = (SELECT max(round) FROM f1_mart.mart_standings s2 WHERE s2.season = mart_standings.season)
ORDER BY points DESC
LIMIT 20;


-- Card: Circuit Lap Records (fastest lap ever set)
-- Type: Table
SELECT
    c.circuit_name,
    c.country,
    c.length_km,
    c.corners,
    round(min(r.lap_time_ms) / 1000.0, 3)          AS record_lap_s,
    argMin(r.jolpica_driver_id, r.lap_time_ms)      AS holder,
    argMin(r.season, r.lap_time_ms)                 AS year_set
FROM f1_mart.fact_race_results r
JOIN f1_mart.dim_circuits c
    ON  c.circuit_id = (
        SELECT circuit_id FROM f1_mart.dim_sessions
        WHERE season = r.season AND round = r.round AND session_type = 'Race'
        LIMIT 1
    )
WHERE r.fastest_lap_time IS NOT NULL
GROUP BY c.circuit_name, c.country, c.length_km, c.corners
ORDER BY c.circuit_name;


-- Card: Driver Career Stats Summary
-- Type: Table
SELECT
    d.full_name                         AS driver,
    d.nationality,
    min(r.season)                       AS first_season,
    max(r.season)                       AS last_season,
    count()                             AS race_starts,
    countIf(r.finish_position = 1)      AS wins,
    countIf(r.finish_position <= 3)     AS podiums,
    countIf(r.finish_position <= 10)    AS points_finishes,
    sum(r.points)                       AS career_points,
    round(avg(r.finish_position), 2)    AS avg_finish
FROM f1_mart.fact_race_results r
JOIN f1_mart.dim_drivers d ON d.driver_id = r.jolpica_driver_id
GROUP BY driver, d.nationality
HAVING race_starts >= 10
ORDER BY career_points DESC
LIMIT 50;


-- Card: Constructors with Most Race Wins (all-time)
-- Type: Bar chart
SELECT
    c.constructor_name,
    c.nationality,
    countIf(r.finish_position = 1)  AS wins,
    count()                         AS race_entries
FROM f1_mart.fact_race_results r
JOIN f1_mart.dim_constructors c ON c.constructor_id = r.constructor_id
GROUP BY c.constructor_name, c.nationality
ORDER BY wins DESC
LIMIT 15;


-- Card: Season Win Spread — How Many Drivers Won Races
-- Type: Bar chart (x=season, y=unique_winners)
SELECT
    season,
    uniq(jolpica_driver_id) AS unique_winners,
    count()                 AS total_races
FROM f1_mart.fact_race_results
WHERE finish_position = 1
GROUP BY season
ORDER BY season;


-- Card: Tire Compound Usage Across the Season
-- Type: Bar chart (stacked, x=round, y=stints, series=compound)  |  Variable: {{year}}
SELECT
    round,
    compound,
    count()     AS stints
FROM f1_mart.mart_strategy
WHERE season = {{year}}
  AND compound  != ''
GROUP BY round, compound
ORDER BY round, compound;

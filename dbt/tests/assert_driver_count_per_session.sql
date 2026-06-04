-- Fail if any qualifying session has a driver count outside the known grid size for that era.
-- Era reference (validated against actual raw_jolpica.qualifying counts):
--   1950-1959: early championship, highly variable (actual data starts 2000)
--   1960-1979: field grew/shrank with constructor entries
--   1980-1993: pre-qualifying era, up to 26 on grid
--   1994-2002: teams folding/joining; actual data shows 20-22
--   2003-2005: settling at 20; some rounds 18 (mid-season withdrawals)
--   2006-2008: Super Aguri joined → 11 teams, 22 cars; actual max=22
--   2009:       Super Aguri folded; back to 10 teams, 20 cars
--   2010-2012:  HRT/Virgin/Lotus joined, 24 cars
--   2013-2014:  HRT withdrew after 2012, 22 cars
--   2015:       Caterham/Marussia gone, 20 cars
--   2016:       Manor ran briefly, 22 cars
--   2017+:      10 teams × 2 drivers, standardised at 20
-- Edge case — Indianapolis Motor Speedway (circuit_id = 'indianapolis'), season <= 1960:
--   The Indianapolis 500 was a designated F1 championship round 1950-1960.
--   It ran under Indy rules with ~33 entries; most European F1 teams did not attend.
--   Max is raised to 33; min kept low to account for F1 non-attendance.
WITH race_meta AS (
    -- Deduplicate: stg_jolpica__races inherits duplicate rows from the unguarded raw ingest.
    -- circuit_id is stable per (season, round) so any() is deterministic here.
    SELECT season, round, any(circuit_id) AS circuit_id
    FROM {{ ref('stg_jolpica__races') }}
    GROUP BY season, round
),
expected AS (
    SELECT
        fq.season,
        fq.round,
        count()                 AS driver_count,
        rm.circuit_id,
        CASE
            WHEN rm.circuit_id = 'indianapolis' AND fq.season <= 1960 THEN 6
            WHEN fq.season BETWEEN 1950 AND 1979 THEN 8
            WHEN fq.season BETWEEN 1980 AND 1993 THEN 16
            WHEN fq.season BETWEEN 1994 AND 2002 THEN 18
            WHEN fq.season BETWEEN 2003 AND 2005 THEN 16
            WHEN fq.season BETWEEN 2006 AND 2008 THEN 18
            WHEN fq.season = 2009                THEN 18
            WHEN fq.season BETWEEN 2010 AND 2012 THEN 20
            WHEN fq.season BETWEEN 2013 AND 2014 THEN 20
            WHEN fq.season = 2015                THEN 18
            WHEN fq.season = 2016                THEN 18
            ELSE                                      18
        END AS min_drivers,
        CASE
            WHEN rm.circuit_id = 'indianapolis' AND fq.season <= 1960 THEN 33
            WHEN fq.season BETWEEN 1950 AND 1979 THEN 26
            WHEN fq.season BETWEEN 1980 AND 1993 THEN 26
            WHEN fq.season BETWEEN 1994 AND 2002 THEN 22
            WHEN fq.season BETWEEN 2003 AND 2005 THEN 20
            WHEN fq.season BETWEEN 2006 AND 2008 THEN 22
            WHEN fq.season = 2009                THEN 20
            WHEN fq.season BETWEEN 2010 AND 2012 THEN 24
            WHEN fq.season BETWEEN 2013 AND 2014 THEN 22
            WHEN fq.season = 2015                THEN 20
            WHEN fq.season = 2016                THEN 22
            ELSE                                      20
        END AS max_drivers
    FROM {{ ref('fact_qualifying') }} fq
    LEFT JOIN race_meta rm ON rm.season = fq.season AND rm.round = fq.round
    GROUP BY fq.season, fq.round, rm.circuit_id
)
SELECT season, round, circuit_id, driver_count, min_drivers, max_drivers
FROM expected
WHERE driver_count < min_drivers OR driver_count > max_drivers

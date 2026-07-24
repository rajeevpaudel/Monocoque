{{ config(tags=["marts"]) }}
-- Fail if any qualifying session has a driver count outside the known grid size for that era.
-- Scope: season >= 2000 (jolpica coverage). Pre-2000 data comes from f1db and had highly
-- variable field sizes (some championship rounds had as few as 6 drivers, some had 30+)
-- that cannot be asserted with the same era buckets used for modern F1.
-- Era reference (validated against actual data):
--   2000-2002: teams folding/joining; actual data shows 20-22
--   2003-2005: settling at 20; some rounds 18 (mid-season withdrawals)
--   2006-2008: Super Aguri joined → 11 teams, 22 cars; actual max=22
--   2009:       Super Aguri folded; back to 10 teams, 20 cars
--   2010-2012:  HRT/Virgin/Lotus joined, 24 cars
--   2013-2014:  HRT withdrew after 2012, 22 cars; min=16 because Caterham + Marussia
--               both missed the 2014 US and Brazilian GPs (18 cars entered those rounds)
--   2015:       Caterham/Marussia gone, 20 cars
--   2016:       Manor ran briefly, 22 cars
--   2017-2025:  10 teams × 2 drivers, standardised at 20
--   2026+:      Cadillac joined → 11 teams × 2 drivers = 22 cars
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
            WHEN fq.season BETWEEN 2000 AND 2002 THEN 18
            WHEN fq.season BETWEEN 2003 AND 2005 THEN 16
            WHEN fq.season BETWEEN 2006 AND 2008 THEN 18
            WHEN fq.season = 2009                THEN 18
            WHEN fq.season BETWEEN 2010 AND 2012 THEN 20
            WHEN fq.season BETWEEN 2013 AND 2014 THEN 16
            WHEN fq.season = 2015                THEN 18
            WHEN fq.season = 2016                THEN 18
            WHEN fq.season >= 2026               THEN 18
            ELSE                                      18
        END AS min_drivers,
        CASE
            WHEN fq.season BETWEEN 2000 AND 2002 THEN 22
            WHEN fq.season BETWEEN 2003 AND 2005 THEN 20
            WHEN fq.season BETWEEN 2006 AND 2008 THEN 22
            WHEN fq.season = 2009                THEN 20
            WHEN fq.season BETWEEN 2010 AND 2012 THEN 24
            WHEN fq.season BETWEEN 2013 AND 2014 THEN 22
            WHEN fq.season = 2015                THEN 20
            WHEN fq.season = 2016                THEN 22
            WHEN fq.season >= 2026               THEN 22
            ELSE                                      20
        END AS max_drivers
    FROM {{ ref('fact_qualifying') }} fq
    LEFT JOIN race_meta rm ON rm.season = fq.season AND rm.round = fq.round
    WHERE fq.season >= 2000
    GROUP BY fq.season, fq.round, rm.circuit_id
)
SELECT season, round, circuit_id, driver_count, min_drivers, max_drivers
FROM expected
WHERE driver_count < min_drivers OR driver_count > max_drivers

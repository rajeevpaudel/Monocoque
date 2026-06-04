-- Fail if any qualifying round has more rows than the era's maximum grid size.
-- Excess rows indicate sprint qualifying session contamination (2021+) or a general ingestion bug.
-- Era max mirrors assert_driver_count_per_session and assert_grid_position_valid.
-- Edge case — Indianapolis 500 (circuit_id = 'indianapolis', season <= 1960):
--   Ran under Indy rules with up to 33 starters; not an F1 road-circuit qualifying session.
WITH race_meta AS (
    -- Deduplicate: stg_jolpica__races inherits duplicate rows from the unguarded raw ingest.
    SELECT season, round, any(circuit_id) AS circuit_id
    FROM {{ ref('stg_jolpica__races') }}
    GROUP BY season, round
),
expected AS (
    SELECT
        mqs.season,
        mqs.round,
        count()        AS row_count,
        rm.circuit_id,
        CASE
            WHEN rm.circuit_id = 'indianapolis' AND mqs.season <= 1960 THEN 33
            WHEN mqs.season BETWEEN 1950 AND 1979 THEN 30
            WHEN mqs.season BETWEEN 1980 AND 1993 THEN 26
            WHEN mqs.season BETWEEN 1994 AND 2002 THEN 22
            WHEN mqs.season BETWEEN 2003 AND 2005 THEN 20
            WHEN mqs.season BETWEEN 2006 AND 2008 THEN 22
            WHEN mqs.season = 2009                THEN 20
            WHEN mqs.season BETWEEN 2010 AND 2012 THEN 24
            WHEN mqs.season BETWEEN 2013 AND 2014 THEN 22
            WHEN mqs.season = 2015                THEN 20
            WHEN mqs.season = 2016                THEN 22
            ELSE                                       20
        END AS max_drivers
    FROM {{ ref('mart_qualifying_summary') }} mqs
    LEFT JOIN race_meta rm ON rm.season = mqs.season AND rm.round = mqs.round
    GROUP BY mqs.season, mqs.round, rm.circuit_id
)
SELECT season, round, circuit_id, row_count, max_drivers
FROM expected
WHERE row_count > max_drivers

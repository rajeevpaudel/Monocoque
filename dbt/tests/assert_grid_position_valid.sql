{{ config(tags=["marts"]) }}
-- Fail if any grid position exceeds the maximum field size for that era.
-- Grid position 0 is valid (pit lane start due to grid penalty).
-- Era max-grid mirrors assert_driver_count_per_session.
-- Edge case — Indianapolis 500 (circuit_id = 'indianapolis', season <= 1960):
--   Ran under Indy rules with up to 33 starters; not an F1 road-circuit grid.
SELECT r.season, r.round, r.driver_id, r.grid_position, era.max_grid
FROM {{ ref('fact_race_results') }} r
JOIN (
    SELECT
        rr.season,
        rr.round,
        races.circuit_id,
        CASE
            WHEN races.circuit_id = 'indianapolis' AND rr.season <= 1960 THEN 33
            WHEN rr.season BETWEEN 1950 AND 1979 THEN 30
            WHEN rr.season BETWEEN 1980 AND 1993 THEN 26
            WHEN rr.season BETWEEN 1994 AND 2002 THEN 22
            WHEN rr.season BETWEEN 2003 AND 2005 THEN 20
            WHEN rr.season BETWEEN 2006 AND 2008 THEN 22
            WHEN rr.season = 2009                THEN 20
            WHEN rr.season BETWEEN 2010 AND 2012 THEN 24
            WHEN rr.season BETWEEN 2013 AND 2014 THEN 22
            WHEN rr.season = 2015                THEN 20
            WHEN rr.season = 2016                THEN 22
            ELSE                                      20
        END AS max_grid
    FROM (SELECT DISTINCT season, round FROM {{ ref('fact_race_results') }}) rr
    LEFT JOIN (
        -- Deduplicate: stg_jolpica__races inherits duplicate rows from the unguarded raw ingest.
        SELECT season, round, any(circuit_id) AS circuit_id
        FROM {{ ref('stg_jolpica__races') }}
        GROUP BY season, round
    ) races ON races.season = rr.season AND races.round = rr.round
) era ON era.season = r.season AND era.round = r.round
WHERE r.grid_position > era.max_grid

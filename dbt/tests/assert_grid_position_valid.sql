-- Fail if any 2010+ grid position is greater than 20.
-- Grid position 0 is valid (pit lane start due to grid penalty).
SELECT season, round, driver_id, grid_position
FROM {{ ref('fact_race_results') }}
WHERE season >= 2010
  AND grid_position > 20

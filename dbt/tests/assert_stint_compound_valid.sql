-- Fail if any stint uses a tyre compound outside the official set.
-- UNKNOWN and TEST_UNKNOWN are OpenF1 placeholders for genuinely unidentified compounds
-- and are excluded here as known non-corrupt values.
SELECT DISTINCT compound
FROM {{ ref('stg_openf1__stints') }}
WHERE compound NOT IN ('SOFT', 'MEDIUM', 'HARD', 'INTERMEDIATE', 'WET', 'UNKNOWN', 'TEST_UNKNOWN')
  AND compound IS NOT NULL
  AND compound  != ''

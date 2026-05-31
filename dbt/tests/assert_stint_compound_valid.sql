-- Fail if any stint uses a tyre compound outside the official set.
SELECT DISTINCT compound
FROM {{ ref('stg_openf1__stints') }}
WHERE compound NOT IN ('SOFT', 'MEDIUM', 'HARD', 'INTERMEDIATE', 'WET')
  AND compound IS NOT NULL
  AND compound  != ''

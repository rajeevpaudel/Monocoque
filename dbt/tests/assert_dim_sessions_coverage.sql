-- Fail if any OpenF1-era qualifying round has no matching row in dim_sessions.
SELECT DISTINCT fq.season, fq.round
FROM {{ ref('fact_qualifying') }} fq
LEFT JOIN {{ ref('dim_sessions') }} ds
    ON  ds.season       = fq.season
    AND ds.round        = fq.round
    AND ds.session_type = 'Qualifying'
    AND ds.session_name = 'Qualifying'
WHERE fq.session_key IS NOT NULL
  AND ds.session_key IS NULL

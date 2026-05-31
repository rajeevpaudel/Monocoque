-- Fail if any Race or Qualifying session (2023+) has no weather records.
SELECT ds.session_key, ds.season, ds.round, ds.session_type
FROM {{ ref('dim_sessions') }} ds
LEFT JOIN {{ ref('stg_openf1__weather') }} w
    ON  w.session_key = ds.session_key
WHERE ds.session_type IN ('Race', 'Qualifying')
  AND ds.season >= 2023
GROUP BY ds.session_key, ds.season, ds.round, ds.session_type
HAVING count(w.session_key) = 0

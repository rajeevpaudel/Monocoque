SELECT
    season,
    round,
    race_name,
    circuit_id,
    toDate(date)    AS race_date,
    time            AS race_time_utc,
    url,
    _ingested_at,
    'jolpica'       AS _source
FROM {{ source('raw_jolpica', 'races') }}

UNION ALL

SELECT
    year            AS season,
    round,
    race_name,
    circuit_id,
    toDate(date)    AS race_date,
    time            AS race_time_utc,
    ''              AS url,
    _ingested_at,
    'f1db'          AS _source
FROM {{ source('raw_f1db', 'races') }}
WHERE year NOT IN (SELECT DISTINCT season FROM {{ source('raw_jolpica', 'races') }})
  AND toDate(date) <= today()

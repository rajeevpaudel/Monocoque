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

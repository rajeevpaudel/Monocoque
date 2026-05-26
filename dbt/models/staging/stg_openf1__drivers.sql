SELECT
    session_key,
    driver_number,
    broadcast_name,
    full_name,
    name_acronym,
    team_name,
    team_colour,
    first_name,
    last_name,
    headshot_url,
    country_code,
    _ingested_at,
    'openf1'        AS _source
FROM {{ source('raw_openf1', 'drivers') }}

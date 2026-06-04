SELECT
    driver_id,
    given_name,
    family_name,
    concat(given_name, ' ', family_name) AS full_name,
    toDate(date_of_birth)                AS date_of_birth,
    nationality,
    permanent_number,
    code                                 AS driver_code,
    url,
    _ingested_at,
    'jolpica'                            AS _source
FROM {{ source('raw_jolpica', 'drivers') }}

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

UNION ALL

-- f1db drivers not already in jolpica (IDs differ: 'lewis-hamilton' vs 'hamilton').
-- Both kept so fact rows from each source can resolve their driver_id.
SELECT
    driver_id,
    first_name                           AS given_name,
    last_name                            AS family_name,
    concat(first_name, ' ', last_name)   AS full_name,
    toDate(date_of_birth)                AS date_of_birth,
    nationality,
    permanent_number,
    abbreviation                         AS driver_code,
    ''                                   AS url,
    _ingested_at,
    'f1db'                               AS _source
FROM {{ source('raw_f1db', 'drivers') }}
WHERE driver_id NOT IN (SELECT DISTINCT driver_id FROM {{ source('raw_jolpica', 'drivers') }})

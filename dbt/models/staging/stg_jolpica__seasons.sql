SELECT
    year,
    url,
    _ingested_at,
    'jolpica' AS _source
FROM {{ source('raw_jolpica', 'seasons') }}

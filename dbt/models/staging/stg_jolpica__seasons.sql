SELECT year, url, _ingested_at, 'jolpica' AS _source
FROM {{ source('raw_jolpica', 'seasons') }}

UNION ALL

SELECT year, '' AS url, _ingested_at, 'f1db' AS _source
FROM {{ source('raw_f1db', 'seasons') }}
WHERE year NOT IN (SELECT DISTINCT year FROM {{ source('raw_jolpica', 'seasons') }})

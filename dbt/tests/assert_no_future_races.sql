-- Fail if any race date is in the future (would indicate bad data or a timezone issue).
SELECT season, round, race_date
FROM {{ ref('stg_jolpica__races') }}
WHERE race_date > today()

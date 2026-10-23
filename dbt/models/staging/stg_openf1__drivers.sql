-- Applies seeds/openf1_driver_overrides.csv: known-bad upstream rows are corrected or dropped
-- here so every downstream model sees the same cleaned identity.
SELECT
    d.session_key                                               AS session_key,
    d.driver_number                                             AS driver_number,
    d.broadcast_name                                            AS broadcast_name,
    if(o.action = 'fix', o.full_name, d.full_name)              AS full_name,
    if(o.action = 'fix', o.name_acronym, d.name_acronym)        AS name_acronym,
    d.team_name                                                 AS team_name,
    d.team_colour                                               AS team_colour,
    d.first_name                                                AS first_name,
    d.last_name                                                 AS last_name,
    d.headshot_url                                              AS headshot_url,
    d.country_code                                              AS country_code,
    d._ingested_at                                              AS _ingested_at,
    'openf1'                                                    AS _source
FROM {{ source('raw_openf1', 'drivers') }}          d
LEFT JOIN {{ ref('openf1_driver_overrides') }}      o
    ON  o.session_key   = d.session_key
    AND o.driver_number = d.driver_number
WHERE ifNull(o.action, '') != 'exclude'

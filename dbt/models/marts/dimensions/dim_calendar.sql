{{ config(engine='ReplacingMergeTree()', order_by=['season', 'round']) }}

-- Race calendar: one row per round, including upcoming races, with every weekend
-- session's scheduled start (UTC). Session times come from Jolpica's schedule; seasons
-- only present in f1db carry the race date/time only.

WITH schedule AS (
    SELECT
        season,
        round,
        any(race_name)                                          AS race_name,
        any(circuit_id)                                         AS circuit_id,
        toDate(any(date))                                       AS race_date,
        any(parseDateTimeBestEffortOrNull(concat(date, ' ', replaceOne(ifNull(time, ''), 'Z', ''))))
                                                                AS race_start_utc,
        any({{ jolpica_session_ts('FirstPractice') }})                  AS fp1_start_utc,
        any({{ jolpica_session_ts('SecondPractice') }})                 AS fp2_start_utc,
        any({{ jolpica_session_ts('ThirdPractice') }})                  AS fp3_start_utc,
        any(coalesce({{ jolpica_session_ts('SprintQualifying') }}, {{ jolpica_session_ts('SprintShootout') }}))
                                                                AS sprint_qualifying_start_utc,
        any({{ jolpica_session_ts('Sprint') }})                         AS sprint_start_utc,
        any({{ jolpica_session_ts('Qualifying') }})                     AS qualifying_start_utc,
        'jolpica'                                               AS _source
    FROM {{ source('raw_jolpica', 'races') }}
    GROUP BY season, round

    UNION ALL

    SELECT
        year                                                    AS season,
        round,
        any(race_name),
        any(circuit_id),
        toDate(any(date)),
        any(parseDateTimeBestEffortOrNull(concat(date, ' ', ifNull(time, '')))),
        NULL, NULL, NULL, NULL, NULL, NULL,
        'f1db'
    FROM {{ f1db_source('races', circuit_col='circuit_id') }}
    WHERE year NOT IN (SELECT DISTINCT season FROM {{ source('raw_jolpica', 'races') }})
    GROUP BY year, round
)

SELECT
    toUInt32(s.season * 100 + s.round)                          AS race_sk,
    toUInt16(s.season)                                          AS season,
    toUInt8(s.round)                                            AS round,
    s.race_name                                                 AS race_name,
    s.circuit_id                                                AS circuit_id,
    c.circuit_name                                              AS circuit_name,
    c.locality                                                  AS locality,
    c.country                                                   AS country,
    least(
        s.race_date,
        ifNull(toDate(s.fp1_start_utc), s.race_date),
        ifNull(toDate(s.sprint_qualifying_start_utc), s.race_date)
    )                                                           AS weekend_start_date,
    s.race_date                                                 AS race_date,
    s.race_start_utc                                            AS race_start_utc,
    s.fp1_start_utc                                             AS fp1_start_utc,
    s.fp2_start_utc                                             AS fp2_start_utc,
    s.fp3_start_utc                                             AS fp3_start_utc,
    s.sprint_qualifying_start_utc                               AS sprint_qualifying_start_utc,
    s.sprint_start_utc                                          AS sprint_start_utc,
    s.qualifying_start_utc                                      AS qualifying_start_utc,
    toUInt8(s.sprint_start_utc IS NOT NULL OR sp.has_sprint IS NOT NULL) AS is_sprint_weekend,
    count() OVER (PARTITION BY s.season)                        AS season_round_count,
    toUInt8(s.round = max(s.round) OVER (PARTITION BY s.season)) AS is_season_finale,
    multiIf(
        s.race_date < today(), 'completed',
        s.race_date - 3 <= today(), 'race_week',
        'upcoming'
    )                                                           AS status,
    s._source                                                   AS _source
FROM schedule s
LEFT JOIN {{ ref('dim_circuits') }}              c  ON c.circuit_id = s.circuit_id
LEFT JOIN {{ ref('int_sprint_weekend_flag') }}   sp ON sp.season = s.season AND sp.round = s.round

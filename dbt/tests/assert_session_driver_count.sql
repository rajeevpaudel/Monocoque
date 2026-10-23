{{ config(tags=["marts"]) }}
-- Fail if an OpenF1 session lists more cars than an F1 grid can field (11 teams × 2 = 22,
-- plus headroom for testing days). 35 entries in Hungary 2023 FP1 exposed an F3 feed mix-up.
SELECT d.session_key, uniqExact(d.driver_number) AS drivers
FROM {{ ref('stg_openf1__drivers') }} d
WHERE d.driver_number > 0
GROUP BY d.session_key
HAVING drivers > 24

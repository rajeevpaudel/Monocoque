{{ config(tags=["marts"]) }}
-- Fail if one car number carries different drivers in different sessions of the same
-- race weekend. FP1 rookies run under their own number, so within a meeting a number
-- always identifies one driver. Catches corrupt feeds such as OpenF1 Hungary 2023 FP1
-- (session 9223), where F3 entries overwrote F1 car numbers (fixed via openf1_driver_overrides).
SELECT
    s.meeting_key                           AS meeting_key,
    d.driver_number                         AS driver_number,
    groupUniqArray(d.name_acronym)          AS acronyms
FROM {{ ref('stg_openf1__drivers') }} d
INNER JOIN {{ ref('stg_openf1__sessions') }} s ON s.session_key = d.session_key
WHERE d.driver_number > 0
GROUP BY s.meeting_key, d.driver_number
HAVING length(acronyms) > 1

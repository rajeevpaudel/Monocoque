{{ config(tags=["marts"]) }}
-- Fail if any circuit used in the current season has NULL length_km or corners.
SELECT c.circuit_id, c.length_km, c.corners
FROM {{ ref('dim_circuits') }} c
JOIN (
    SELECT DISTINCT circuit_id
    FROM {{ ref('dim_sessions') }}
    WHERE season = toYear(today())
      AND circuit_id IS NOT NULL
) used ON used.circuit_id = c.circuit_id
WHERE c.length_km IS NULL
   OR c.corners   IS NULL

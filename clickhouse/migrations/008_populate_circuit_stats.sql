-- Populate length_km and corners for all circuits used in the 2024 season.
-- Both `vegas` and `las_vegas` are present (Jolpica used different slugs in 2023 vs 2024).

ALTER TABLE raw_jolpica.circuits
UPDATE
    length_km = CASE circuit_id
        WHEN 'bahrain'       THEN 5.412
        WHEN 'jeddah'        THEN 6.174
        WHEN 'albert_park'   THEN 5.278
        WHEN 'suzuka'        THEN 5.807
        WHEN 'shanghai'      THEN 5.451
        WHEN 'miami'         THEN 5.412
        WHEN 'imola'         THEN 4.909
        WHEN 'monaco'        THEN 3.337
        WHEN 'villeneuve'    THEN 4.361
        WHEN 'catalunya'     THEN 4.657
        WHEN 'red_bull_ring' THEN 4.318
        WHEN 'silverstone'   THEN 5.891
        WHEN 'hungaroring'   THEN 4.381
        WHEN 'spa'           THEN 7.004
        WHEN 'zandvoort'     THEN 4.259
        WHEN 'monza'         THEN 5.793
        WHEN 'baku'          THEN 6.003
        WHEN 'marina_bay'    THEN 4.940
        WHEN 'americas'      THEN 5.513
        WHEN 'rodriguez'     THEN 4.304
        WHEN 'interlagos'    THEN 4.309
        WHEN 'vegas'         THEN 6.201
        WHEN 'las_vegas'     THEN 6.201
        WHEN 'losail'        THEN 5.380
        WHEN 'yas_marina'    THEN 5.281
        ELSE length_km
    END,
    corners = CASE circuit_id
        WHEN 'bahrain'       THEN 15
        WHEN 'jeddah'        THEN 27
        WHEN 'albert_park'   THEN 14
        WHEN 'suzuka'        THEN 18
        WHEN 'shanghai'      THEN 16
        WHEN 'miami'         THEN 19
        WHEN 'imola'         THEN 19
        WHEN 'monaco'        THEN 19
        WHEN 'villeneuve'    THEN 14
        WHEN 'catalunya'     THEN 16
        WHEN 'red_bull_ring' THEN 10
        WHEN 'silverstone'   THEN 18
        WHEN 'hungaroring'   THEN 14
        WHEN 'spa'           THEN 19
        WHEN 'zandvoort'     THEN 14
        WHEN 'monza'         THEN 11
        WHEN 'baku'          THEN 20
        WHEN 'marina_bay'    THEN 23
        WHEN 'americas'      THEN 20
        WHEN 'rodriguez'     THEN 17
        WHEN 'interlagos'    THEN 15
        WHEN 'vegas'         THEN 17
        WHEN 'las_vegas'     THEN 17
        WHEN 'losail'        THEN 16
        WHEN 'yas_marina'    THEN 16
        ELSE corners
    END
WHERE circuit_id IN (
    'bahrain', 'jeddah', 'albert_park', 'suzuka', 'shanghai', 'miami',
    'imola', 'monaco', 'villeneuve', 'catalunya', 'red_bull_ring', 'silverstone',
    'hungaroring', 'spa', 'zandvoort', 'monza', 'baku', 'marina_bay', 'americas',
    'rodriguez', 'interlagos', 'vegas', 'las_vegas', 'losail', 'yas_marina'
);

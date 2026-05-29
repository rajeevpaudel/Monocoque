ALTER TABLE f1_mart.mart_lap_telemetry
    ADD COLUMN IF NOT EXISTS distance_m Nullable(Float32);

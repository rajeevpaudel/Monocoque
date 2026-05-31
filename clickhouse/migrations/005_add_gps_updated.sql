-- Add gps_updated flag to mart_lap_telemetry.
-- Existing rows default to 0 until the mart is rebuilt for those sessions.

ALTER TABLE f1_mart.mart_lap_telemetry
    ADD COLUMN IF NOT EXISTS gps_updated UInt8 DEFAULT 0;

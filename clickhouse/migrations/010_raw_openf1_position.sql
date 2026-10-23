-- OpenF1 /position: one row each time a driver's running position changes.
-- Drives overtake / position-change analytics (mart_position_changes).
CREATE TABLE IF NOT EXISTS raw_openf1.position (
    session_key      UInt32,
    meeting_key      UInt32,
    driver_number    UInt8,
    date             String,
    position         UInt8,
    _raw_json        String,
    _ingested_at     DateTime DEFAULT now()
) ENGINE = MergeTree()
ORDER BY (session_key, driver_number, date);

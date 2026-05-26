-- Driver ID mapping table: bridges Jolpica string slugs to OpenF1 integer driver numbers.
-- Rebuilt at the start of each season via ingestion/openf1/driver_map.py.

CREATE DATABASE IF NOT EXISTS dim;

CREATE TABLE IF NOT EXISTS dim.driver_id_map (
    jolpica_driver_id     String,
    openf1_driver_number  UInt8,
    season                UInt16,
    updated_at            DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (season, jolpica_driver_id);

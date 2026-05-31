-- Add circuit physical characteristics to raw_jolpica.circuits.
-- Sourced from Jolpica/Ergast circuits endpoint ('length' and 'turns' fields).
-- Existing rows will have NULL until reference tables are re-ingested.

ALTER TABLE raw_jolpica.circuits
    ADD COLUMN IF NOT EXISTS length_km Nullable(Float32),
    ADD COLUMN IF NOT EXISTS corners   Nullable(UInt8);

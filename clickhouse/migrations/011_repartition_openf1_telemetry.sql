-- Repartition raw_openf1.car_data / location by year only.
-- 002 partitioned them by (year, session_key): one partition per session, which grows to
-- thousands of small partitions and works against ClickHouse's guidance of a few hundred at
-- most. Per-session deletes stay cheap because session_key leads the ORDER BY.
--
-- Each table is rebuilt via copy + EXCHANGE (atomic on the default Atomic database engine).
-- Every step is re-runnable, so a failure part-way can simply be retried.
-- Pause telemetry ingestion while this runs: rows inserted between the copy and the
-- exchange would be lost.

DROP TABLE IF EXISTS raw_openf1.car_data__repart;
CREATE TABLE raw_openf1.car_data__repart AS raw_openf1.car_data
ENGINE = MergeTree()
PARTITION BY toYear(date)
ORDER BY (session_key, driver_number, date)
SETTINGS index_granularity = 8192;
INSERT INTO raw_openf1.car_data__repart SELECT * FROM raw_openf1.car_data;
EXCHANGE TABLES raw_openf1.car_data AND raw_openf1.car_data__repart;
DROP TABLE raw_openf1.car_data__repart;

DROP TABLE IF EXISTS raw_openf1.location__repart;
CREATE TABLE raw_openf1.location__repart AS raw_openf1.location
ENGINE = MergeTree()
PARTITION BY toYear(date)
ORDER BY (session_key, driver_number, date)
SETTINGS index_granularity = 8192;
INSERT INTO raw_openf1.location__repart SELECT * FROM raw_openf1.location;
EXCHANGE TABLES raw_openf1.location AND raw_openf1.location__repart;
DROP TABLE raw_openf1.location__repart;

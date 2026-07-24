# Monocoque

A Formula 1 data warehouse built on ClickHouse and dbt. Ingests from three complementary sources — Jolpica (1950–present), OpenF1 (2023–present, sub-second telemetry), and FastF1 — stages raw data, transforms it through a layered dbt pipeline, and serves analytics-ready mart tables.

```
Jolpica API  (1950–present) ──┐
OpenF1 API   (2023–present) ──┼──► ClickHouse (raw_*) ──► dbt ──► f1_mart.*
FastF1       (2023–present) ──┘
```

---

## Stack

| Layer | Tool |
|---|---|
| Storage | ClickHouse 24.3 |
| Transformation | dbt-clickhouse 1.10 |
| Orchestration | Apache Airflow 2.9 |
| Ingestion | Python 3.11 + Pydantic v2 |
| Infrastructure | Docker Compose |

---

## Data Sources

**Jolpica** (`api.jolpi.ca/ergast/f1`) — the Ergast successor. Race results, qualifying, pit stops, lap times, driver/constructor standings from 1950 to present. No auth required. Updates Monday 06:00 UTC after each race weekend.

**OpenF1** (`api.openf1.org/v1`) — high-resolution telemetry from 2023 onward. Endpoints: `car_data` (~3.7 Hz per driver, ~2M rows per race), `location` (GPS), `laps`, `stints`, `pit`, `intervals`, `weather`, `race_control`, `sessions`, `drivers`.

**FastF1** — Python library providing distance-along-track data to complement OpenF1's GPS coordinates.

**Driver ID bridge** — Jolpica uses string slugs (`hamilton`); OpenF1 uses integers (`44`). `dim.driver_id_map` is the join spine between sources. Rebuilt at the start of each season.

---

## Repository Layout

```
├── clickhouse/
│   ├── migrate.py              # Ordered, idempotent migration runner
│   └── migrations/             # 008 SQL files — raw schema + alterations
│
├── ingestion/
│   ├── shared/                 # HTTP client, ClickHouse bulk insert, Pydantic models
│   ├── jolpica/                # backfill.py, incremental.py, endpoints.py
│   ├── openf1/                 # backfill.py, incremental.py, endpoints.py
│   └── fastf1/                 # Distance sampling
│
├── dbt/
│   └── models/
│       ├── staging/            # stg_jolpica__*, stg_openf1__*, stg_fastf1__* (views)
│       ├── intermediate/       # Cross-source joins and derived flags (tables)
│       └── marts/
│           ├── dimensions/     # dim_drivers, dim_circuits, dim_constructors, dim_sessions
│           ├── facts/          # fact_race_results, fact_laps, fact_qualifying, fact_sprint_results
│           ├── analytics/      # mart_qualifying_summary, mart_standings, mart_strategy, mart_lap_analysis
│           └── telemetry/      # mart_lap_telemetry (per-sample, 2023+)
│
├── airflow/
│   └── dags/                   # Backfill, incremental, and dbt DAGs
│
└── docker-compose.yml          # ClickHouse + PostgreSQL + Airflow + Metabase
```

---

## ClickHouse Schema

Three databases:

| Database | Purpose |
|---|---|
| `raw_jolpica` | Append-only raw data from Jolpica |
| `raw_openf1` | Append-only raw data from OpenF1 |
| `dim` | Driver ID bridge and reference tables |
| `f1_staging` | dbt staging views |
| `f1_intermediate` | dbt intermediate tables |
| `f1_mart` | Analytics-ready mart tables — query these |

All raw tables include `_raw_json String` and `_ingested_at DateTime` audit columns.

**Key sort keys:**
```sql
-- Jolpica fact tables
ORDER BY (season, round, driver_id)

-- OpenF1 timing
ORDER BY (session_key, driver_number, lap_number)

-- OpenF1 telemetry (car_data, location)
PARTITION BY (toYear(date), session_key)
ORDER BY (session_key, driver_number, date)
```

---

## dbt Model Layers

| Layer | Materialization | Naming convention | Purpose |
|---|---|---|---|
| Staging | View | `stg_<source>__<entity>` | 1:1 with raw tables; cast types, rename columns |
| Intermediate | Table | `int_<description>` | Cross-source joins, derived flags |
| Dimensions | Table (ReplacingMergeTree) | `dim_<entity>` | Canonical reference data |
| Facts | Incremental (append) | `fact_<entity>` | One row per event; never updated |
| Analytics | Table | `mart_<analysis>` | Pre-joined, query-ready for applications |

**Key models:**

- `int_session_map` — maps `(season, round, session_type)` → `session_key`. The join spine between Jolpica's race-centric model and OpenF1's session-key model. Handles the UTC-vs-local date offset for night races (e.g. Las Vegas).
- `dim_sessions` — one row per OpenF1 session with circuit, race name, and round resolved.
- `fact_qualifying` — Jolpica official times matched to OpenF1 laps within ±50 ms tolerance, with sector times and speed traps for 2023+.
- `mart_lap_telemetry` — per-sample telemetry joined with GPS and FastF1 distance. Partitioned by `(year, session_key)` for query performance.
- `mart_qualifying_summary` — fully denormalized qualifying table; everything a UI needs in one query.

---

## Quick Start

### Prerequisites

- Docker + Docker Compose v2
- Python 3.10+

### 1. Fix dbt directory permissions

The Airflow container runs as uid `50000` but the `dbt/` directory is owned by your host user.
dbt needs write access to `dbt/`, `dbt/logs/`, `dbt/dbt_packages/`, and `dbt/target/` to install
packages and write compiled output. Run once after cloning:

```bash
mkdir -p dbt/logs dbt/dbt_packages dbt/target
chmod 777 dbt
chmod -R 777 dbt/logs dbt/dbt_packages dbt/target
```

### 2. Build the Airflow image

The project uses a custom Airflow image (`airflow/Dockerfile`) that pre-installs the ingestion
dependencies (`structlog`, `clickhouse-connect`, `fastf1`, `dbt-clickhouse`, `elementary-data`,
etc.). Pull the base image and build once — and again whenever `airflow/requirements.txt` changes:

```bash
docker compose build
```

### 3. Start the stack

```bash
docker compose up -d
```

| Service | URL |
|---|---|
| ClickHouse HTTP | http://localhost:8123 |
| Airflow UI | http://localhost:8080 (admin / admin) |
| Metabase | http://localhost:3001 |

### 4. Apply the schema

```bash
python clickhouse/migrate.py
```

Safe to re-run. Applies all migrations in order, skipping any already applied.

### 5. Ingest data

```bash
# Jolpica — historical results, qualifying, standings (1950–present)
PYTHONPATH=. python ingestion/jolpica/backfill.py --start 2023 --end 2024

# OpenF1 — telemetry (2023+). Skip --skip-telemetry for a fast load without car_data/location
PYTHONPATH=. python ingestion/openf1/backfill.py --start 2023 --end 2024

# Or use make
make ingest-year YEAR=2024
make ingest-year YEAR=2024 SKIP_TELEMETRY=1
```

### 6. Run dbt

```bash
cd dbt
dbt deps
dbt run --profiles-dir .
dbt test --profiles-dir .
```

```bash
# Or
make dbt-run
make dbt-test
```

---

## Makefile Reference

```bash
make up                              # Start Docker services
make down                            # Stop Docker services
make migrate                         # Apply ClickHouse migrations
make reset-data                      # Drop all databases and re-apply migrations
make ingest-year YEAR=2024           # Ingest Jolpica + OpenF1 for a year, then run dbt
make ingest-year YEAR=2024 SKIP_TELEMETRY=1
make backfill-jolpica START=2018 END=2022
make backfill-openf1 START=2023 END=2024
make dbt-run
make dbt-test
make lint
make test
```

---

## Ingestion CLI Reference

```bash
# Jolpica — full historical load (slow, ~1 hour for full history)
PYTHONPATH=. python ingestion/jolpica/backfill.py --start 1950 --end 2024

# Jolpica — single race weekend
PYTHONPATH=. python ingestion/jolpica/incremental.py --season 2025 --round 8

# OpenF1 — without telemetry (fast, minutes per year)
PYTHONPATH=. python ingestion/openf1/backfill.py --start 2023 --end 2024 --skip-telemetry

# OpenF1 — full with telemetry (~1 hour per year, ~2M rows/session for car_data)
PYTHONPATH=. python ingestion/openf1/backfill.py --start 2023 --end 2024

# OpenF1 — single session
PYTHONPATH=. python ingestion/openf1/incremental.py --session-key 9158

# Rebuild driver ID map (run once per season)
PYTHONPATH=. python ingestion/openf1/driver_map.py --year 2024
```

---

## Querying the Warehouse

Only query `f1_mart.*`. Never read `raw_*` or staging tables from application code.

```sql
-- List qualifying sessions with telemetry (2023+)
SELECT session_key, season, round, race_name, circuit_name, qualifying_date
FROM f1_mart.dim_sessions
WHERE session_type = 'Qualifying' AND season >= 2023
ORDER BY season, round;

-- All drivers for a qualifying session
SELECT qualifying_position, driver_name, team_name, team_colour,
       best_time, best_s1, best_s2, best_s3, i1_speed, st_speed
FROM f1_mart.mart_qualifying_summary
WHERE season = 2024 AND round = 3
ORDER BY qualifying_position;

-- Full telemetry for a single lap
SELECT date, speed, throttle, brake, n_gear, drs, rpm, x, y, z, distance_m
FROM f1_mart.mart_lap_telemetry
WHERE session_key = 9484
  AND driver_number = 1
  AND lap_number = 20
ORDER BY date;
```

See [`docs/warehouse-access-guide.md`](docs/warehouse-access-guide.md) for a complete query reference including column definitions, DRS values, GPS coordinate conventions, and segment array encoding.

---

## Known Limitations

**Telemetry volume** — `car_data` produces ~2M rows per race session. A full 2023–2025 OpenF1 backfill with telemetry takes several hours. Use `--skip-telemetry` for development.

**Session key ≠ race round** — OpenF1 `session_key` is a globally unique integer unrelated to Jolpica's `(season, round)`. Always join through `int_session_map` or `dim_sessions`.

**Sprint weekends** — Sprint results live in `fact_sprint_results`, not `fact_race_results`. Sprint Qualifying sessions have `session_type = 'Qualifying'` in OpenF1 but `session_name = 'Sprint Qualifying'` — filter on `session_name` to exclude them.

**Pre-2023 coverage** — OpenF1 telemetry, sector times, speed traps, and GPS are unavailable before 2023. All OpenF1-sourced columns in mart tables are NULL for earlier seasons.

**Jolpica update lag** — Race data is published Monday 06:00 UTC. The weekly Airflow DAG is scheduled accordingly.

**dbt test failures block the pipeline** — `ingest_and_dbt` DAG fails if any dbt test returns `ERROR`. Warnings (e.g. `assert_telemetry_hz`) do not block. If you need to bypass a known data quality failure during development, mark the test as `severity: warn` in the schema YAML.

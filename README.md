# F1 Data Warehouse

A production-grade Formula 1 data warehouse built to demonstrate data engineering skills. The pipeline ingests from two complementary APIs, stages raw data in ClickHouse, transforms it with dbt, and serves analytics-ready tables — all orchestrated by Airflow.

```
Jolpica API (1950–present)  ──┐
                               ├──► ClickHouse (raw) ──► dbt ──► mart tables
OpenF1 API  (2023–present)  ──┘                                      │
                                                                       ▼
                            Airflow (orchestration)          Analytics queries
```

---

## Stack

| Layer | Tool | Why |
|---|---|---|
| Sources | Jolpica + OpenF1 APIs | Complementary coverage: Jolpica for history, OpenF1 for telemetry |
| Ingestion | Python (custom ingestors) | Pydantic-validated, idempotent, rate-limited |
| Raw storage | ClickHouse (`raw_jolpica.*`, `raw_openf1.*`) | Append-only, raw JSON preserved |
| Transformation | dbt-clickhouse | Staging → Intermediate → Mart layers |
| Orchestration | Apache Airflow | DAG-per-concern, retry logic, sensors |
| Local infra | Docker Compose | ClickHouse + PostgreSQL (Airflow meta) + Airflow |

---

## Data Sources

### Jolpica (`api.jolpi.ca/ergast/f1/`)
The Ergast successor API. Covers **1950 to present** for every race result, qualifying session, pit stop, lap time, driver/constructor standing, and championship season. This is the backbone — all historical data comes from here.

- No auth required
- Updates Monday after each race weekend (06:00 UTC)
- Rate limit: ~2 req/s with jitter

### OpenF1 (`api.openf1.org/v1/`)
High-resolution telemetry and session data. Covers **2023 to present** at sub-second granularity.

- No auth required for historical data
- Endpoints used: `laps`, `pit`, `stints`, `intervals`, `weather`, `race_control`, `car_data` (~2M rows/race), `location`
- `car_data` is the largest table — ~2M rows per race session

### Driver ID bridge
Jolpica uses string slugs (`"hamilton"`); OpenF1 uses integers (`44`). `dim.driver_id_map` is the only source of truth for cross-source joins. Rebuilt at the start of each season via `ingestion/openf1/driver_map.py`.

---

## Quick Start

### Prerequisites

- Docker + Docker Compose v2
- Python 3.10+

### 1. Clone and configure

```bash
git clone <this-repo>
cd f1-warehouse
cp .env.example .env
```

### 2. Start the stack

```bash
docker compose up -d
```

Services:
| Service | URL | Credentials |
|---|---|---|
| ClickHouse HTTP | http://localhost:8123 | no password |
| Airflow UI | http://localhost:8080 | admin / admin |

Wait ~30 seconds for Airflow to initialize. Check status with `docker compose ps`.

### 3. Apply ClickHouse schema

```bash
python3 clickhouse/migrate.py
```

This creates three databases (`raw_jolpica`, `raw_openf1`, `dim`) with 24 tables total. Safe to re-run.

### 4. Run a test backfill (2 seasons, ~5 min)

```bash
PYTHONPATH=. python3 ingestion/jolpica/backfill.py --start 2022 --end 2023
```

Or use the Makefile shortcut:

```bash
make backfill-jolpica START=2022 END=2023
```

### 5. Add OpenF1 data (2023+)

```bash
# Skip telemetry for a fast load (no car_data / location)
PYTHONPATH=. python3 ingestion/openf1/backfill.py --start 2023 --end 2023 --skip-telemetry

# Full load including telemetry (takes ~1 hour per year)
PYTHONPATH=. python3 ingestion/openf1/backfill.py --start 2023 --end 2023
```

### 6. Run dbt

```bash
cd dbt
dbt deps
dbt run
dbt test
```

Or:

```bash
make dbt-run
make dbt-test
```

---

## Repository Layout

```
f1-warehouse/
├── docker-compose.yml          ← Airflow + ClickHouse + Postgres
├── .env.example                ← Copy to .env before running
├── Makefile                    ← up / down / migrate / backfill-* / dbt-*
├── pyproject.toml              ← Python deps
│
├── clickhouse/
│   ├── migrate.py              ← Ordered migration runner (idempotent)
│   ├── config/
│   │   └── users_override.xml  ← Allows external connections to ClickHouse
│   └── migrations/
│       ├── 001_raw_jolpica.sql ← 12 raw tables for Jolpica data
│       ├── 002_raw_openf1.sql  ← 11 raw tables for OpenF1 data
│       └── 003_dim_driver_id_map.sql ← Driver ID bridge table
│
├── ingestion/
│   ├── shared/
│   │   ├── clickhouse.py       ← Bulk insert wrapper (clickhouse-connect)
│   │   ├── http.py             ← HTTP client with retry + rate limiting
│   │   └── models.py           ← Pydantic v2 models for all raw entities
│   ├── jolpica/
│   │   ├── client.py           ← Paginated GET, MRData envelope handling
│   │   ├── endpoints.py        ← One function per Jolpica endpoint
│   │   ├── backfill.py         ← CLI: full historical load
│   │   └── incremental.py      ← CLI: single race weekend load
│   └── openf1/
│       ├── client.py           ← Simple GET (no pagination)
│       ├── endpoints.py        ← One function per OpenF1 endpoint
│       ├── backfill.py         ← CLI: full year load
│       ├── incremental.py      ← CLI: single session load
│       └── driver_map.py       ← Builds dim.driver_id_map
│
├── dbt/
│   ├── dbt_project.yml         ← Materialization defaults per layer
│   ├── profiles.yml            ← ClickHouse connection (reads from env)
│   └── models/
│       ├── staging/            ← stg_jolpica__* and stg_openf1__* (views)
│       ├── intermediate/       ← int_* cross-source joins (tables)
│       └── marts/
│           ├── dimensions/     ← dim_drivers, dim_circuits, dim_constructors
│           ├── facts/          ← fact_race_results, fact_laps, fact_qualifying, fact_sprint_results
│           └── analytics/      ← mart_lap_analysis, mart_strategy, mart_standings
│
└── airflow/
    └── dags/
        ├── backfill_jolpica.py   ← One-time historical load (parameterised)
        ├── backfill_openf1.py    ← One-time OpenF1 load (parameterised)
        ├── weekly_jolpica.py     ← Runs Monday 06:00 UTC post-race
        ├── session_openf1.py     ← Triggered per session after it ends
        └── dbt_run.py            ← dbt staging → intermediate → marts pipeline
```

---

## ClickHouse Schema

### Databases

| Database | Purpose |
|---|---|
| `raw_jolpica` | Append-only raw data from Jolpica API |
| `raw_openf1` | Append-only raw data from OpenF1 API |
| `dim` | Bridge/dimension tables shared across sources |

### Key Tables

| Table | Engine | Notes |
|---|---|---|
| `raw_jolpica.results` | MergeTree | One row per driver per race, includes `_raw_json` |
| `raw_jolpica.lap_times` | MergeTree | Per-lap timing (aggregate) |
| `raw_openf1.car_data` | MergeTree (partitioned) | ~2M rows/race @ 3.7 Hz |
| `raw_openf1.laps` | MergeTree | Detailed sector times |
| `dim.driver_id_map` | ReplacingMergeTree | Jolpica slug ↔ OpenF1 number |

All raw tables include `_raw_json String` and `_ingested_at DateTime` columns.

### Sort Keys

```sql
-- Jolpica fact tables
ORDER BY (season, round, driver_id)

-- OpenF1 timing tables
ORDER BY (session_key, driver_number, lap_number)

-- OpenF1 telemetry (car_data, location)
PARTITION BY (toYear(date), session_key)
ORDER BY (session_key, driver_number, date)
```

---

## dbt Models

### Layer conventions

| Layer | Materialization | Naming | Purpose |
|---|---|---|---|
| Staging | View | `stg_<source>__<entity>` | 1:1 with raw tables; cast types, rename columns |
| Intermediate | Table | `int_<description>` | Cross-source joins, derived flags |
| Dimensions | Table (ReplacingMergeTree) | `dim_<entity>` | Canonical reference data |
| Facts | Incremental (append) | `fact_<entity>` | Historical records, never updated |
| Analytics | Table | `mart_<analysis>` | Pre-aggregated, query-ready |

### Key intermediate models

- **`int_session_map`** — maps `(season, round, session_type)` → `session_key`. The join spine between Jolpica and OpenF1. OpenF1 session keys don't correspond to race rounds.
- **`int_driver_race_results`** — joins Jolpica results + OpenF1 lap data via `dim.driver_id_map`. OpenF1 columns are NULL for seasons before 2023.

### Running specific models

```bash
cd dbt

# Single model
dbt run --select fact_race_results

# All marts
dbt run --select marts

# Staging only (fast, views)
dbt run --select staging

# With tests
dbt run && dbt test
```

---

## Airflow DAGs

| DAG | Schedule | Purpose |
|---|---|---|
| `backfill_jolpica` | Manual | Full historical load. Params: `start_season`, `end_season` |
| `backfill_openf1` | Manual | OpenF1 load for 2023+. Params: `start_year`, `end_year` |
| `weekly_jolpica` | `0 6 * * 1` | Monday post-race incremental load |
| `session_openf1` | Triggered | Per-session load after `date_end` passes |
| `dbt_run` | Triggered | Full dbt pipeline (staging → marts) |

Access the Airflow UI at **http://localhost:8080** (admin / admin). Trigger manual DAGs from the UI with the "Trigger DAG w/ config" button.

Example config for `backfill_jolpica`:
```json
{"start_season": 2020, "end_season": 2022}
```

---

## Ingestion CLI Reference

### Jolpica backfill

```bash
# Full pre-OpenF1 era (will take ~1 hour)
PYTHONPATH=. python3 ingestion/jolpica/backfill.py --start 1950 --end 2022

# Recent seasons only (good starting point, ~10 min)
PYTHONPATH=. python3 ingestion/jolpica/backfill.py --start 2018 --end 2022

# Skip reference tables (seasons/circuits/drivers — already loaded)
PYTHONPATH=. python3 ingestion/jolpica/backfill.py --start 2020 --end 2022 --skip-reference
```

### Jolpica single round

```bash
PYTHONPATH=. python3 ingestion/jolpica/incremental.py --season 2025 --round 8
```

### OpenF1 backfill

```bash
# Fast: no car_data / location (~minutes per year)
PYTHONPATH=. python3 ingestion/openf1/backfill.py --start 2023 --end 2025 --skip-telemetry

# Full: includes car_data + location (~1 hour per year, ~2M rows/session)
PYTHONPATH=. python3 ingestion/openf1/backfill.py --start 2023 --end 2025
```

### OpenF1 single session

```bash
PYTHONPATH=. python3 ingestion/openf1/incremental.py --session-key 9158
```

### Rebuild driver ID map

```bash
# Run once per season (or after a new team/driver appears)
PYTHONPATH=. python3 ingestion/openf1/driver_map.py --year 2024
```

---

## Verify Data is Loaded

```bash
# Row counts per table
curl "http://localhost:8123/?query=SELECT+table,sum(rows)+FROM+system.parts+WHERE+database='raw_jolpica'+AND+active=1+GROUP+BY+table+ORDER+BY+table"

# Spot check: 2024 British GP results
curl "http://localhost:8123/?query=SELECT+driver_id,+position,+points+FROM+raw_jolpica.results+WHERE+season=2024+AND+round=12+ORDER+BY+position"

# Driver ID map sample
curl "http://localhost:8123/?query=SELECT+*+FROM+dim.driver_id_map+WHERE+jolpica_driver_id='hamilton'+LIMIT+3"
```

---

## Known Limitations & Gotchas

**Driver ID mismatch**: Jolpica uses string slugs (`"max_verstappen"`), OpenF1 uses integers (`33`). Always join via `dim.driver_id_map`. Rebuild it at the start of each season.

**Jolpica update lag**: Race data is published Monday 06:00 UTC. Don't schedule the weekly DAG earlier.

**OpenF1 telemetry volume**: `car_data` produces ~2M rows per race session. The 2023–2025 full backfill takes hours. Use `--skip-telemetry` for development.

**Session key ≠ race round**: OpenF1 `session_key` is a globally unique integer. Practice 1, Qualifying, and Race for the same round are different session keys. Use `int_session_map` to join them to Jolpica's `(season, round)`.

**Sprint weekends**: Jolpica has a separate `sprint_results` endpoint. OpenF1 tags them `session_type = "Sprint"`. `fact_sprint_results` is separate from `fact_race_results` — do not conflate them.

**ClickHouse `Date` type**: Stores as uint16 (days since 1970). Pre-1970 dates (many F1 drivers' birthdates) can't be stored as `Date`. Raw tables use `String` for date fields; dbt staging casts them with `toDate()`.

**ClickHouse eventual consistency**: After bulk inserts, `SELECT count()` may lag. Staging models that read from `ReplacingMergeTree` tables include `FINAL` to force deduplication.

---

## Development Tips

```bash
# Tail ClickHouse logs
docker compose logs -f clickhouse

# Connect to ClickHouse via CLI
docker exec -it f1-clickhouse-1 clickhouse-client

# Run a quick query
curl "http://localhost:8123/?query=SELECT+count()+FROM+raw_jolpica.lap_times"

# Reset everything (drops all data)
docker exec f1-clickhouse-1 clickhouse-client --query "DROP DATABASE IF EXISTS raw_jolpica"
docker exec f1-clickhouse-1 clickhouse-client --query "DROP DATABASE IF EXISTS raw_openf1"
docker exec f1-clickhouse-1 clickhouse-client --query "DROP DATABASE IF EXISTS dim"
python3 clickhouse/migrate.py
```

---

## Project Goals / Showcase Checklist

- [x] Idempotent Python ingestors with Pydantic v2 validation
- [x] ClickHouse DDL with correct engines and partition keys
- [x] dbt project with staging / intermediate / mart layers
- [x] dbt tests covering nullability, uniqueness, and cross-source relationships
- [x] Airflow DAGs with proper dependencies and retry logic
- [x] Driver ID mapping table bridging the two sources
- [x] Incremental dbt models for fact tables (no full refresh on each run)
- [x] Docker Compose local dev environment
- [ ] dbt installed in docker-compose for `make dbt-run` to work without local dbt
- [ ] Full 1950–2025 backfill completed
- [ ] Grafana dashboard (optional)

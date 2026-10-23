# Monocoque

[![CI](https://github.com/rajeevpaudel/Monocoque/actions/workflows/ci.yml/badge.svg)](https://github.com/rajeevpaudel/Monocoque/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)
![Python](https://img.shields.io/badge/python-3.10%2B-blue.svg)

A Formula 1 data warehouse built on ClickHouse and dbt.

Monocoque pulls race data from four sources (Jolpica, OpenF1, f1db and FastF1), lands it in raw
ClickHouse tables, and transforms it through a layered dbt project into analytics-ready marts.
Results go back to 1950; lap timing and car telemetry are available from 2023.

```
Jolpica API  (1950–present) ──┐
OpenF1 API   (2023–present) ──┤
f1db release (1950–next)    ──┼──► ClickHouse (raw_*) ──► dbt ──► f1_mart.*
FastF1       (2023–present) ──┘
```

## Contents

- [Quick start](#quick-start)
- [Stack](#stack)
- [Data sources](#data-sources)
- [Repository layout](#repository-layout)
- [ClickHouse schema](#clickhouse-schema)
- [dbt models](#dbt-models)
- [Querying the warehouse](#querying-the-warehouse)
- [Metabase dashboards](#metabase-dashboards)
- [Reference](#reference)
- [Development](#development)
- [Known limitations](#known-limitations)
- [License](#license)

## Quick start

You need Docker with Compose v2 and Python 3.10 or newer.

```bash
make setup                    # .venv + pinned deps (incl. dbt) + pre-commit hooks
docker compose build          # custom Airflow image
make up                       # ClickHouse, Postgres, Airflow, Metabase
python clickhouse/migrate.py  # apply the raw schema
make ingest-year YEAR=2024 SKIP_TELEMETRY=1   # Jolpica + OpenF1, then dbt build
```

Once the stack is up:

| Service | URL |
|---|---|
| ClickHouse HTTP | http://localhost:8123 |
| Airflow UI | http://localhost:8080 (admin / admin) |
| Metabase | http://localhost:3001 |
| dbt docs | http://localhost:8082 (`docker compose up -d dbt-docs`) |

The steps below cover each stage in more detail.

### 1. Set up the environment

```bash
make setup
```

The Airflow containers bind-mount `dbt/` and `airflow/logs/`. `make up` writes `AIRFLOW_UID=$(id -u)`
to `.env` (creating it from `.env.example` if needed), so the containers run as your host user and can
write to those directories without `chmod 777` or `sudo`. If you pin a different `AIRFLOW_UID`,
`make fix-perms` opens up only the generated directories (`dbt/target`, `dbt/logs`,
`dbt/dbt_packages`, `airflow/logs`).

### 2. Build the Airflow image

The custom image (`airflow/Dockerfile`) pre-installs the ingestion dependencies (`structlog`,
`clickhouse-connect`, `fastf1`, `dbt-clickhouse`, `elementary-data`, etc.). Build it once, and again
whenever `airflow/requirements.txt` changes:

```bash
docker compose build
```

### 3. Start the stack

```bash
make up        # or: docker compose up -d
```

### 4. Apply the schema

```bash
python clickhouse/migrate.py
```

Applied migrations are tracked by filename in `raw_meta.schema_migrations`. Re-running the command
skips anything already applied and only runs new files.

### 5. Ingest data

```bash
# f1db: static dump with full history and the upcoming calendar (downloads the latest release).
# Fallback for seasons Jolpica hasn't ingested yet. Each table is truncated before load because
# every release is a full dump, not a delta.
PYTHONPATH=. python -m ingestion.f1db.loader

# Jolpica: historical results, qualifying, standings (1950–present)
PYTHONPATH=. python ingestion/jolpica/backfill.py --start 2023 --end 2024

# OpenF1: timing, stints, weather, position (2023+). Drop --skip-telemetry to also load
# car_data/location (~50M rows per season, rate-limited; prefer the targeted re-ingest in the reference)
PYTHONPATH=. python ingestion/openf1/backfill.py --start 2023 --end 2024 --skip-telemetry

# Or use make
make ingest-year YEAR=2024
make ingest-year YEAR=2024 SKIP_TELEMETRY=1
```

### 6. Run dbt

```bash
cd dbt
dbt deps
dbt seed --profiles-dir .   # circuit_stats, points systems, f1db id overrides, OpenF1 driver overrides
dbt run --profiles-dir .
dbt test --profiles-dir .
```

```bash
# Or
make dbt-build     # seed + run + test (incl. unit tests), excluding the elementary package
```

`dbt build` also runs the unit tests in `dbt/models/unit_tests.yml` (clean-lap flags, champion logic
incl. DSQ and incomplete seasons, points-system expansion) against mocked inputs.
`dbt/models/exposures.yml` declares the three Metabase dashboards as exposures, so lineage shows which
marts feed them: `dbt ls -s +exposure:metabase_championship`.

## Stack

| Layer | Tool |
|---|---|
| Storage | ClickHouse 26.3 |
| Transformation | dbt-clickhouse 1.10 |
| Orchestration | Apache Airflow 2.9 |
| Ingestion | Python 3.11 + Pydantic v2 |
| Dashboards | Metabase |
| Infrastructure | Docker Compose |

## Data sources

| Source | Coverage | What it provides |
|---|---|---|
| [Jolpica](https://api.jolpi.ca/ergast/f1) | 1950–present | Race results, qualifying, pit stops, lap times, driver and constructor standings. The Ergast successor. No auth; updates Monday 06:00 UTC after each race weekend. |
| [OpenF1](https://api.openf1.org/v1) | 2023–present | `car_data` (~3.7 Hz per driver, ~2M rows per race), `location` (GPS), `laps`, `stints`, `pit`, `intervals`, `position` (live running order), `weather`, `race_control`, `sessions`, `drivers`. |
| [f1db](https://github.com/f1db/f1db) | 1950–next season | Full history including the upcoming calendar and circuit lengths and turns. Used for seasons Jolpica doesn't have yet. |
| FastF1 | 2023–present | Python library; distance-along-track data to complement OpenF1's GPS coordinates. |

### Shared keys across sources

f1db ids (`lewis-hamilton`, `spielberg`) are rewritten to Jolpica ids (`hamilton`, `red_bull_ring`)
by the `f1db_{driver,constructor,circuit}_xref` crosswalks, so every season shares one set of keys.
The crosswalks pair on results first, then fall back to name, date of birth and geography, with manual
overrides in `seeds/f1db_id_overrides.csv`.

Jolpica identifies drivers by slug (`hamilton`) and OpenF1 by car number (`44`).
`f1_staging.driver_session_map` joins the two at session grain, `(session_key, driver_number)`. A
per-season map isn't enough because car numbers get reused within a season by FP1 rookies, reserve
drivers and mid-season substitutes. Matching uses the OpenF1 acronym and surname against Jolpica/f1db
driver codes. Corrupt upstream OpenF1 rows are fixed or excluded through the `openf1_driver_overrides`
seed. FP1-only drivers with no Jolpica/f1db record stay unmapped (NULL `driver_id`).

## Repository layout

```
├── clickhouse/
│   ├── migrate.py              # Ordered, idempotent migration runner
│   └── migrations/             # ordered SQL files — raw schema + alterations
│
├── ingestion/
│   ├── shared/                 # HTTP client, ClickHouse bulk insert, Pydantic models
│   ├── jolpica/                # backfill.py, incremental.py, endpoints.py
│   ├── openf1/                 # backfill.py, incremental.py, endpoints.py
│   ├── fastf1/                 # Distance sampling
│   └── f1db/                   # loader.py — f1db CSV release load, mapped to Jolpica IDs via crosswalks
│
├── dbt/
│   ├── seeds/                  # circuit_stats, points_systems, points_system_rules, f1db_id_overrides,
│   │                           # openf1_driver_overrides
│   └── models/
│       ├── staging/            # stg_jolpica__*, stg_openf1__*, stg_fastf1__* (views), f1db crosswalks,
│       │                       # driver_session_map
│       ├── intermediate/       # Cross-source joins and derived flags (tables), int_race_laps_clean
│       └── marts/
│           ├── dimensions/     # dim_drivers, dim_constructors, dim_circuits, dim_races, dim_sessions,
│           │                   # dim_calendar, dim_points_systems, dim_driver_team_stints
│           ├── facts/          # fact_race_results, fact_laps, fact_qualifying, fact_sprint_results, fact_pit_stops
│           ├── analytics/      # mart_standings, mart_{driver,constructor}_season_standings,
│           │                   # mart_teammate_head_to_head, mart_race_pace, mart_tyre_degradation,
│           │                   # mart_position_changes, mart_weather_impact, mart_qualifying_summary, ...
│           └── telemetry/      # mart_lap_telemetry (per-sample, 2023+)
│
├── metabase/                   # setup_dashboards.py (API provisioning), queries.sql, DASHBOARD.md
├── scripts/sample_data.py      # Export/load the CI sample dataset (tests/fixtures/warehouse_sample)
│
├── airflow/
│   └── dags/                   # Backfill, incremental, and dbt DAGs
│
└── docker-compose.yml          # ClickHouse + PostgreSQL + Airflow + Metabase
```

## ClickHouse schema

| Database | Purpose |
|---|---|
| `raw_jolpica` | Raw data from Jolpica (1950–present) |
| `raw_openf1` | Raw data from OpenF1 (2023–present) |
| `raw_f1db` | Raw data from the f1db CSV dump (1950 → upcoming season, mapped to Jolpica IDs via dbt crosswalks) |
| `raw_fastf1` | Raw car telemetry distance samples from FastF1 (2023+) |
| `raw_meta` | `ingestion_log` (per-entity/table outcome tracking) and `schema_migrations` |
| `dim` | Legacy driver ID map (`ingestion/openf1/driver_map.py`) and reference tables; dbt doesn't read the driver map |
| `f1_staging` | dbt staging views |
| `f1_intermediate` | dbt intermediate tables |
| `f1_mart` | Analytics-ready mart tables. Query these. |

Every raw table has `_raw_json String` and `_ingested_at DateTime` audit columns.

Main sort keys:

```sql
-- Jolpica fact tables
ORDER BY (season, round, driver_id)

-- OpenF1 timing
ORDER BY (session_key, driver_number, lap_number)

-- OpenF1 telemetry (car_data, location) — yearly partitions (migration 011)
PARTITION BY toYear(date)
ORDER BY (session_key, driver_number, date)
```

Migrations in `clickhouse/migrations/` are applied once each and tracked in
`raw_meta.schema_migrations`. Never edit an applied file; add a new numbered migration instead.

## dbt models

| Layer | Materialization | Naming | Purpose |
|---|---|---|---|
| Staging | View | `stg_<source>__<entity>` | 1:1 with raw tables; cast types, rename columns |
| Intermediate | Table | `int_<description>` | Cross-source joins, derived flags |
| Dimensions | Table (ReplacingMergeTree) | `dim_<entity>` | Canonical reference data |
| Facts | Incremental (delete+insert) | `fact_<entity>` | One row per event; rounds rebuilt when Jolpica or OpenF1 data for them changes |
| Analytics | Table | `mart_<analysis>` | Pre-joined, query-ready for applications |

### Main models

| Model | Description |
|---|---|
| `int_session_map` | Maps `(season, round, session_type)` to `session_key`, joining Jolpica's race-centric model to OpenF1's session keys. Handles the UTC-vs-local date offset for night races such as Las Vegas. |
| `driver_session_map` | Maps OpenF1 `(session_key, driver_number)` to the Jolpica `driver_id`. |
| `dim_sessions` | One row per OpenF1 session with circuit, race name and round resolved. |
| `dim_drivers` | One row per driver and season; `constructor_ids` lists every team the driver raced for that season, in order. |
| `dim_calendar` | Every round from 1950 to next season (including upcoming races from f1db) with FP, quali, sprint and race UTC start times, a sprint flag and `status` (completed / race_week / upcoming). |
| `dim_points_systems` | Points per position for every season and session type (race/sprint), including fastest-lap bonus eras. Driven by the `points_systems` seeds. |
| `dim_driver_team_stints` | Contiguous driver/team spells. Mid-season swaps and one-offs are separate stints (`stint_type`, `started_mid_season`, `is_current`). |
| `fact_qualifying` | Official Jolpica times matched to OpenF1 laps within ±50 ms, with sector times and speed traps for 2023+. |
| `mart_standings` | Championship standings per round. `constructor_id` is the team the driver last raced for up to that round. |
| `mart_driver_season_standings`, `mart_constructor_season_standings` | Final championship position plus wins, podiums, poles, DNFs, sprint points and 1-2s. `is_champion` is set only once a season is complete. |
| `mart_teammate_head_to_head` | Qualifying, race and points head-to-head, paired per race so line-up changes are handled. |
| `mart_race_pace`, `mart_tyre_degradation` | Clean-lap race pace (gap to fastest and to team-mate) and fuel-corrected degradation slope per stint (2023+). Clean-lap rules live in `int_race_laps_clean`. |
| `mart_position_changes` | Grid-to-finish gains (all eras), lap-1 gains, laps led, on-track gains (2023+) and OpenF1 live position-feed gains. |
| `mart_weather_impact` | Per-session weather, wet flag, and pace delta against dry sessions at the same circuit. |
| `mart_qualifying_summary` | Fully denormalized qualifying table; everything a UI needs in one query. |
| `mart_lap_telemetry` | Per-sample telemetry joined with GPS and FastF1 distance, partitioned by `(year, session_key)`. |

### JOIN semantics

`profiles.yml` sets `join_use_nulls: 1`, so unmatched `LEFT JOIN` columns are `NULL` (standard SQL)
rather than `0` or `''`. Wrap any column used in a model's `order_by` in `ifNull(...)`. If you upgrade
an existing warehouse across this change, run `dbt run --full-refresh` once so incremental tables pick
up the Nullable column types.

## Querying the warehouse

Application code should only read `f1_mart.*`, never `raw_*` or staging tables.

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

Column definitions are in the dbt docs (`docker compose up -d dbt-docs`, then http://localhost:8082).

## Metabase dashboards

`metabase/setup_dashboards.py` provisions the admin user, the ClickHouse connection (`f1_mart`) and
three dashboards:

- **F1 Championship**: standings, points progression, team-mate H2H, line-up changes
- **F1 Race Analysis**: race pace, tyre degradation, positions gained, weather
- **F1 Calendar & History**: upcoming races, world champions, wet sessions

```bash
MB_ADMIN_PASSWORD='choose-a-password' python metabase/setup_dashboards.py
```

The script is idempotent; re-run it after changing the card definitions. `metabase/queries.sql` has
further ad-hoc queries.

## Reference

### Makefile

```bash
make setup                           # .venv + deps + pre-commit hooks
make up / build-up / down            # Docker services (up also sets AIRFLOW_UID)
make migrate                         # Apply ClickHouse migrations
make reset-data                      # Drop all warehouse databases and re-apply migrations
make clear-raw                       # Truncate every raw_* table
make load-f1db                       # f1db history 1950 → upcoming season
make ingest-year YEAR=2024 [SKIP_TELEMETRY=1]   # Jolpica + OpenF1 for a year, then dbt-build
make backfill-jolpica START=2018 END=2022
make backfill-openf1 START=2023 END=2024 [SKIP_TELEMETRY=1]
make backfill-fastf1 START=2024 END=2024 [ROUNDS=1,5]   # needs `fastf1 auth f1tv` once
make dbt-deps / dbt-seed / dbt-run / dbt-test / dbt-build / dbt-docs
make metabase-dashboards             # needs MB_ADMIN_PASSWORD
make sample-export / sample-load     # CI warehouse sample
make lint / format / test / check    # ruff (check + format) and pytest
```

Ingestion CLIs exit non-zero when any table or session fails to load (it is retried on the next run),
and the Airflow tasks fail with them instead of reporting success on a partial load.

### Ingestion CLIs

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

# Telemetry for a sample of qualifying sessions (OpenF1 car_data/location)
PYTHONPATH=. python -m ingestion.openf1.reingest_sessions --year 2024 --rounds 1 5

# FastF1 distance telemetry for selected rounds only. F1's livetiming archive now returns
# 401 without auth — log in once with an F1TV account first (token is cached by FastF1):
python -m fastf1 auth f1tv
PYTHONPATH=. python ingestion/fastf1/backfill.py --start 2024 --end 2024 --rounds 1 5
```

## Development

```bash
make lint     # ruff check + ruff format --check
make test     # pytest
make check    # both
```

The same hooks run locally through pre-commit after `make setup`.

[CI](.github/workflows/ci.yml) runs `ruff check`, `ruff format --check` and pytest, then a `dbt-build`
job that applies the migrations to a ClickHouse service container, loads a small real sample (2024
Bahrain, the China sprint weekend and f1db 2026 R1) from `tests/fixtures/warehouse_sample/`, and runs
`dbt seed` and `dbt build` (all models, data tests and unit tests). dbt versions are pinned
(`pip install -e ".[dbt]"`, `dbt/packages.yml`). To regenerate the sample from a populated local
warehouse, run `python scripts/sample_data.py export`.

## Known limitations

- **Telemetry volume.** `car_data` produces ~2M rows per race session, and a full 2023–2025 OpenF1
  backfill with telemetry takes several hours. Use `--skip-telemetry` for development.
- **Session key ≠ race round.** OpenF1 `session_key` is a globally unique integer unrelated to
  Jolpica's `(season, round)`. Always join through `int_session_map` or `dim_sessions`.
- **Sprint weekends.** Sprint results live in `fact_sprint_results`, not `fact_race_results`. OpenF1
  gives Sprint Qualifying `session_type = 'Qualifying'` but `session_name = 'Sprint Qualifying'`, so
  filter on `session_name` to exclude it.
- **Pre-2023 coverage.** OpenF1 telemetry, sector times, speed traps and GPS don't exist before 2023.
  All OpenF1-sourced columns in mart tables are NULL for earlier seasons.
- **Jolpica update lag.** Race data is published Monday 06:00 UTC, and the weekly Airflow DAG is
  scheduled for that.
- **dbt test failures block the pipeline.** The `ingest_and_dbt` DAG fails if any dbt test returns
  `ERROR`; warnings such as `assert_telemetry_hz` don't block it. To bypass a known data quality
  failure during development, set the test to `severity: warn` in the schema YAML.
- **No penalties or stewards' decisions.** None of the sources publish structured penalty data
  (penalty type, seconds added, reason). `raw_jolpica.results.status` and f1db's `reasonRetired` can
  say `"Disqualified"`, and `raw_openf1.race_control` occasionally carries a stewards' note as free
  text, but neither is a penalties feed and neither is surfaced in any mart. No public source has this
  data, so treat it as a permanent gap rather than a pipeline bug.

See [`data-gaps.md`](data-gaps.md) for gaps found against the downstream viz app.

## License

[MIT](LICENSE)

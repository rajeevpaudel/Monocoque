# F1 Data Warehouse 

## What this project is

A production-grade Formula 1 data warehouse built to demonstrate data engineering skills.
The pipeline ingests from two complementary APIs, stages raw data in ClickHouse, transforms
it with dbt, and serves analytics-ready tables — all orchestrated by Airflow.

The project is intentionally over-engineered for a side project: the goal is to showcase
a real DE stack, not just get the data.

---

## Stack

| Layer | Tool | Notes |
|---|---|---|
| Sources | Jolpica API + OpenF1 API | See source strategy below |
| Ingestion | Python (custom ingestors) | One module per source |
| Staging | ClickHouse (raw_* tables) | Raw JSON preserved |
| Orchestration | Apache Airflow | DAG-per-concern pattern |
| Transformation | dbt-clickhouse | Staging → intermediate → mart |
| Warehouse | ClickHouse | MergeTree / ReplacingMergeTree |
| Testing | dbt tests + Great Expectations | Schema + data quality |
| Infra | Docker Compose (local) | Airflow + ClickHouse + dbt |

---

## Source strategy

### Jolpica (`api.jolpi.ca/ergast/f1/`)
- Ergast-compatible REST API — the canonical successor to Ergast (which shut down end-2024)
- Covers **1950 to present** for: seasons, circuits, constructors, drivers, races, results,
  qualifying, sprint results, lap times (aggregate), pit stops, driver standings,
  constructor standings
- No auth required. Updates Monday after each race weekend
- Rate limit: be polite — max 4 req/s, add jitter. Uses limit/offset pagination
- Database dumps available at `/ergast/f1/` — use these for the historical backfill
  instead of paginating 75 years of data via REST
- Key identifier: string `driverId` (e.g. `"hamilton"`)

### OpenF1 (`api.openf1.org/v1/`)
- Covers **2023 to present** with sub-second granularity
- Endpoints we care about:
  - `car_data` — throttle, brake, RPM, gear, DRS, speed @ 3.7 Hz → largest table
  - `location` — GPS x/y/z per car per session
  - `laps` — detailed per-lap timing
  - `pit` — pit stop durations, tire compounds
  - `intervals` — gap to leader every 4 seconds
  - `weather` — track/air temp, humidity, wind, rain
  - `race_control` — flags, safety car, session status
  - `stints` — tire strategy per driver
  - `drivers` — driver metadata per session (includes `driver_number` int)
  - `sessions` — session index (use `date_end` to know when a session is complete)
  - `team_radio` — audio metadata
- Historical data: no auth required, JSON or CSV
- Real-time data: requires paid subscription (out of scope for now)
- Key identifier: integer `driver_number` (e.g. `44`)

### Driver ID mapping (critical join key)
Jolpica uses string slugs; OpenF1 uses integers. Build and maintain a mapping table:
`dim.driver_id_map(jolpica_driver_id, openf1_driver_number, season)` — populated from
OpenF1's `/drivers` endpoint which returns both identifiers. This is the spine of
cross-source joins.

---

## Repository layout

```
f1-warehouse/
├── CLAUDE.md                    ← you are here
├── docker-compose.yml           ← Airflow + ClickHouse + dbt runner
├── .env.example
│
├── ingestion/
│   ├── jolpica/
│   │   ├── client.py            ← HTTP client, pagination, retry
│   │   ├── endpoints.py         ← one function per Jolpica endpoint
│   │   ├── backfill.py          ← full historical load (1950–present)
│   │   └── incremental.py       ← single-race-weekend load
│   ├── openf1/
│   │   ├── client.py
│   │   ├── endpoints.py
│   │   ├── backfill.py          ← load all sessions 2023-present
│   │   └── incremental.py       ← load a single session by session_key
│   └── shared/
│       ├── clickhouse.py        ← ClickHouse client wrapper (clickhouse-connect)
│       └── models.py            ← Pydantic models for each raw entity
│
├── dbt/
│   ├── dbt_project.yml
│   ├── profiles.yml             ← ClickHouse connection (reads from env)
│   ├── models/
│   │   ├── staging/             ← stg_jolpica__*, stg_openf1__* (1:1 with raw tables)
│   │   ├── intermediate/        ← int_* (cross-source joins, driver id mapping)
│   │   └── marts/
│   │       ├── dimensions/      ← dim_drivers, dim_circuits, dim_constructors
│   │       ├── facts/           ← fact_race_results, fact_laps, fact_telemetry
│   │       └── analytics/       ← mart_lap_analysis, mart_strategy, mart_standings
│   ├── tests/
│   └── macros/
│
├── airflow/
│   ├── dags/
│   │   ├── backfill_jolpica.py      ← one-time, parameterised by season range
│   │   ├── backfill_openf1.py       ← one-time, parameterised by season range
│   │   ├── weekly_jolpica.py        ← runs Monday 06:00 UTC post-race weekends
│   │   ├── session_openf1.py        ← triggered per session after date_end passes
│   │   └── dbt_run.py               ← dbt run + test after each ingest DAG
│   └── plugins/
│
└── clickhouse/
    └── migrations/              ← ordered .sql files for table DDL
        ├── 001_raw_jolpica.sql
        ├── 002_raw_openf1.sql
        └── 003_staging_views.sql
```

---

## ClickHouse table design

### Naming convention
- `raw_jolpica.<entity>` — raw ingest, append-only
- `raw_openf1.<entity>` — raw ingest, append-only
- `staging.<model>` — dbt staging (materialized as views or tables)
- `intermediate.<model>` — dbt intermediate
- `mart.<model>` — dbt mart (final analytics tables)

### Engine choices
| Table type | Engine | Rationale |
|---|---|---|
| Raw ingest | `MergeTree` | Append-only, no dedup needed |
| Standings snapshots | `ReplacingMergeTree(updated_at)` | Latest snapshot wins |
| Telemetry | `MergeTree PARTITION BY toYYYYMM(date)` | Prune by month on queries |
| Mart dimensions | `ReplacingMergeTree` | Handle re-runs safely |
| Mart facts | `MergeTree` | Immutable historical facts |

### Telemetry partition key (car_data, location)
These are the highest-volume tables — millions of rows per race weekend.
```sql
ENGINE = MergeTree()
PARTITION BY (toYear(date), round)
ORDER BY (session_key, driver_number, date)
SETTINGS index_granularity = 8192
```

### Primary sort key design
- Jolpica fact tables: `ORDER BY (season, round, driver_id)`
- OpenF1 timing tables: `ORDER BY (session_key, driver_number, lap_number)`
- OpenF1 telemetry: `ORDER BY (session_key, driver_number, date)`

---

## dbt model conventions

### Materialisation defaults
```yaml
# dbt_project.yml
models:
  f1_warehouse:
    staging:
      +materialized: view
    intermediate:
      +materialized: table
    marts:
      +materialized: table
      facts:
        +materialized: incremental
        +incremental_strategy: append
```

### Staging model pattern (stg_jolpica__results.sql)
```sql
-- Thin layer: cast types, rename to snake_case, add _source metadata
SELECT
    race_id,
    driver_id,
    constructor_id,
    toInt32(position)           AS finish_position,
    toFloat32(points)           AS points,
    toDateTime(updated_at)      AS _ingested_at,
    'jolpica'                   AS _source
FROM {{ source('raw_jolpica', 'results') }}
```

### Intermediate model pattern (int_driver_race_results.sql)
```sql
-- Join Jolpica results with OpenF1 lap data via the driver ID mapping table
SELECT
    r.season,
    r.round,
    r.driver_id        AS jolpica_driver_id,
    m.openf1_driver_number,
    r.finish_position,
    r.points,
    l.fastest_lap_time
FROM {{ ref('stg_jolpica__results') }}    r
JOIN {{ ref('dim_driver_id_map') }}       m USING (jolpica_driver_id, season)
LEFT JOIN {{ ref('stg_openf1__laps') }}   l
    ON l.driver_number = m.openf1_driver_number
    AND l.session_key  = r.session_key
```

### dbt tests to implement
- `not_null` on all primary keys and foreign keys
- `unique` on dimension natural keys
- `accepted_values` on `session_type` (Race, Qualifying, Sprint, Practice)
- `relationships` between fact tables and dim tables
- Custom test: `assert_telemetry_hz` — avg sample rate per session within 3.5–4.0 Hz
- Custom test: `assert_no_future_races` — `date < now()` on all loaded sessions

---

## Airflow DAG design

### DAG-per-concern pattern
Each DAG has a single responsibility. No mega-DAGs.

### `backfill_jolpica` (one-time)
```
Params: start_season (1950), end_season (2022)
Tasks: download_db_dump → upload_to_s3 → load_to_raw → validate
```

### `weekly_jolpica` (schedule: `0 6 * * 1`)
```
Tasks: check_race_weekend → ingest_results → ingest_qualifying →
       ingest_pit_stops → ingest_laps → trigger_dbt
```

### `session_openf1` (triggered by sensor)
```
Trigger: ExternalTaskSensor watching sessions.date_end < now()
Tasks: ingest_car_data → ingest_location → ingest_laps →
       ingest_pit → ingest_weather → ingest_race_control → trigger_dbt
Concurrency: limit to 1 session at a time (car_data is large)
```

### `dbt_run` (triggered by ingest DAGs)
```
Tasks: dbt_deps → dbt_run_staging → dbt_test_staging →
       dbt_run_intermediate → dbt_run_marts → dbt_test_marts
```

---

## Environment variables

```bash
# ClickHouse
CLICKHOUSE_HOST=localhost
CLICKHOUSE_PORT=8123
CLICKHOUSE_USER=default
CLICKHOUSE_PASSWORD=
CLICKHOUSE_DATABASE=f1

# Airflow
AIRFLOW__CORE__EXECUTOR=LocalExecutor
AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=postgresql+psycopg2://airflow:airflow@postgres/airflow

# Optional: OpenF1 real-time (paid)
OPENF1_ACCESS_TOKEN=
```

---

## Development workflow

1. **Spin up local stack**: `docker-compose up -d`
2. **Run ClickHouse migrations**: `python clickhouse/migrate.py`
3. **Backfill Jolpica** (2018–present is a good starting point):
   `python ingestion/jolpica/backfill.py --start 2018 --end 2024`
4. **Backfill OpenF1** (2023–present):
   `python ingestion/openf1/backfill.py`
5. **Run dbt**: `cd dbt && dbt run && dbt test`
6. **Trigger Airflow DAGs**: http://localhost:8080

---

## Code conventions

- Python 3.11+
- Use `clickhouse-connect` (not the legacy driver) for all ClickHouse I/O
- Pydantic v2 for all API response models — validate before insert
- Ingestors must be idempotent: re-running a backfill should not duplicate rows
  (use `INSERT INTO ... SELECT ... WHERE NOT EXISTS` pattern or ReplacingMergeTree)
- All timestamps stored as UTC
- No pandas in hot paths — use `pyarrow` for bulk inserts into ClickHouse
- Log to structured JSON (use `structlog`)
- Every ingestor writes a `_raw_json` column alongside typed columns — preserve the source

---

## Known gotchas

- **Driver ID mismatch**: Jolpica uses string slugs (`"max_verstappen"`), OpenF1 uses
  integers (`33`). The `dim_driver_id_map` table is the only source of truth for joins.
  Rebuild it at the start of each season.

- **Jolpica update lag**: Data is updated Mondays after race weekends. Don't schedule
  the weekly DAG before 06:00 UTC Monday.

- **OpenF1 telemetry volume**: `car_data` produces ~2M rows per race session. The backfill
  for 2023–2025 will take hours. Run it with `--session-concurrency 1` to avoid OOM.

- **Session key ≠ race round**: OpenF1 `session_key` is a globally unique integer per
  session (practice, quali, race are separate). Join to Jolpica via
  `(season, round, session_type)` not via `session_key` directly.

- **Sprint weekends**: Jolpica has a separate `sprint_results` endpoint. OpenF1 tags
  sprint sessions as `session_type = "Sprint"`. Handle them explicitly in intermediate
  models — do not conflate sprint results with race results.

- **ClickHouse eventual consistency**: After bulk inserts, `SELECT count()` may not
  immediately reflect the new rows. Add `FINAL` keyword in dbt models that read from
  ReplacingMergeTree tables, or use `optimize_on_insert = 1` setting.

---

## Project goals / showcase checklist

- [ ] Idempotent Python ingestors with Pydantic validation
- [ ] ClickHouse DDL with correct engines and partition keys
- [ ] dbt project with staging / intermediate / mart layers
- [ ] dbt tests covering nullability, uniqueness, and cross-source relationships
- [ ] Airflow DAGs with proper dependencies and retry logic
- [ ] Driver ID mapping table bridging the two sources
- [ ] Incremental dbt models for fact tables (no full refresh on each run)
- [ ] Docker Compose local dev environment
- [ ] README with architecture diagram and setup instructions



After any schema changes always update the warehouse access guide.md in docs


# Metabase Dashboards Design

**Date:** 2026-05-25
**Status:** Approved

## Goal

Add Metabase to the F1 warehouse Docker stack and build three dashboards covering season overview, race deep-dive, and driver comparison. Year is selectable from the UI on all dashboards.

---

## Infrastructure

### Metabase service

- Image: `metabase/metabase:latest`
- Port: `3000` (host) → `3000` (container)
- Metadata backend: existing `postgres` container, database `metabase` (separate from `airflow` DB)
- ClickHouse driver: official community JAR (`metabase-clickhouse-driver.jar`) downloaded and mounted at `/plugins/`
- Depends on: `postgres` (healthy), `clickhouse` (healthy)

### Postgres init

A `postgres-init/` directory with a SQL script that creates the `metabase` database on first container start. Mounted into the postgres container at `/docker-entrypoint-initdb.d/`.

### ClickHouse connection in Metabase

- Host: `clickhouse` (internal Docker network)
- Port: `8123`
- User: `default`
- Password: (empty)
- Schemas to expose: `f1_mart` only

---

## Dashboards

All three dashboards have a **Year** filter variable (integer, default: most recent year in data).

### 1. Season Overview

| Card | Type | Source |
|---|---|---|
| Driver championship standings | Table | `mart_standings` |
| Points progression by round | Line chart | `mart_standings` |
| Constructor standings | Bar chart | `mart_standings` |
| Wins & podiums by driver | Bar chart | `fact_race_results` |

Filters: `year`

### 2. Race Deep-Dive

| Card | Type | Source |
|---|---|---|
| Grid → finish position changes | Table | `fact_race_results` |
| Lap time distribution by driver | Bar/line chart | `mart_lap_analysis` |
| Pit strategy timeline | Table | `mart_strategy` |
| Fastest lap sector breakdown | Table | `mart_lap_analysis` |

Filters: `year`, `round`

### 3. Driver Comparison

| Card | Type | Source |
|---|---|---|
| Points trajectory by round | Dual line | `mart_standings` |
| Average finish position | Bar chart | `fact_race_results` |
| Qualifying vs finish position | Table | `fact_qualifying` + `fact_race_results` |
| Head-to-head race record | Summary | `fact_race_results` |

Filters: `year`, `driver_1`, `driver_2`

---

## Implementation Steps

1. Download the ClickHouse driver JAR and add it to the repo under `metabase/plugins/`
2. Add `postgres-init/01-metabase-db.sql` to create the `metabase` database
3. Update `docker-compose.yml` — add `metabase` service, update `postgres` to mount init script
4. Add `make metabase` target (alias for `docker-compose up -d metabase`)
5. First-run: open `localhost:3000`, complete setup wizard, add ClickHouse connection
6. Build dashboards manually in Metabase UI using the mart tables

## Out of Scope

- Metabase authentication / user management (personal project, no auth needed)
- Telemetry dashboards (car data / location not yet ingested)
- Automated dashboard provisioning (Metabase doesn't support declarative dashboard-as-code cleanly)

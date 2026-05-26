# Metabase Dashboards Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add Metabase to the Docker stack with a ClickHouse connection and set up three F1 dashboards (Season Overview, Race Deep-Dive, Driver Comparison).

**Architecture:** A custom Metabase Docker image downloads the ClickHouse community driver JAR at build time. Metabase stores its app metadata in a dedicated `metabase` database on the existing Postgres container. All services communicate over the existing Docker bridge network.

**Tech Stack:** Metabase OSS (latest), ClickHouse Metabase driver JAR, Docker Compose, Postgres 15, ClickHouse 24.3

---

## File Map

| Action | Path | Purpose |
|---|---|---|
| Create | `metabase/Dockerfile` | Custom Metabase image that bakes in the ClickHouse driver JAR |
| Create | `postgres-init/01-metabase-db.sql` | Creates the `metabase` database on first Postgres start |
| Modify | `docker-compose.yml` | Add `metabase` service; mount init script into `postgres` |
| Modify | `Makefile` | Add `metabase-open` and `metabase-logs` targets |

---

## Task 1: Postgres init script

**Files:**
- Create: `postgres-init/01-metabase-db.sql`

- [ ] **Step 1: Create the init directory and SQL script**

```bash
mkdir -p postgres-init
```

Create `postgres-init/01-metabase-db.sql`:

```sql
SELECT 'CREATE DATABASE metabase'
WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'metabase')\gexec
```

This uses `\gexec` to only create the DB if it doesn't already exist — safe to run on every container start.

- [ ] **Step 2: Commit**

```bash
git add postgres-init/01-metabase-db.sql
git commit -m "feat: add postgres init script to create metabase database"
```

---

## Task 2: Custom Metabase Dockerfile with ClickHouse driver

**Files:**
- Create: `metabase/Dockerfile`

The ClickHouse Metabase driver is maintained at https://github.com/ClickHouse/metabase-clickhouse-driver. Check the releases page for the latest JAR version before running. The plan uses `1.8.5` — update the version if a newer release is available.

- [ ] **Step 1: Create `metabase/Dockerfile`**

```dockerfile
FROM metabase/metabase:latest

USER root

RUN curl -sL \
    https://github.com/ClickHouse/metabase-clickhouse-driver/releases/download/1.8.5/clickhouse.metabase-driver.jar \
    -o /plugins/clickhouse.metabase-driver.jar && \
    chmod 644 /plugins/clickhouse.metabase-driver.jar

USER metabase
```

- [ ] **Step 2: Verify the Dockerfile builds**

```bash
docker build -t f1-metabase ./metabase/
```

Expected: build completes, no errors. The JAR download should succeed and produce output like:
```
Successfully tagged f1-metabase:latest
```

- [ ] **Step 3: Commit**

```bash
git add metabase/Dockerfile
git commit -m "feat: custom Metabase image with ClickHouse driver"
```

---

## Task 3: Add Metabase service to docker-compose

**Files:**
- Modify: `docker-compose.yml`

- [ ] **Step 1: Mount the Postgres init script into the `postgres` service**

In `docker-compose.yml`, update the `postgres` service volumes block to add the init script mount:

```yaml
  postgres:
    image: postgres:15
    environment:
      POSTGRES_USER: airflow
      POSTGRES_PASSWORD: airflow
      POSTGRES_DB: airflow
    volumes:
      - postgres-data:/var/lib/postgresql/data
      - ./postgres-init:/docker-entrypoint-initdb.d
    healthcheck:
      test: ["CMD", "pg_isready", "-U", "airflow"]
      interval: 10s
      timeout: 5s
      retries: 5
```

- [ ] **Step 2: Add the `metabase` service**

Add the following service block after the `airflow-scheduler` service in `docker-compose.yml`:

```yaml
  metabase:
    build: ./metabase
    ports:
      - "3000:3000"
    environment:
      MB_DB_TYPE: postgres
      MB_DB_DBNAME: metabase
      MB_DB_PORT: 5432
      MB_DB_USER: airflow
      MB_DB_PASS: airflow
      MB_DB_HOST: postgres
    depends_on:
      postgres:
        condition: service_healthy
      clickhouse:
        condition: service_healthy
    restart: unless-stopped
```

- [ ] **Step 3: Verify the compose file is valid**

```bash
docker-compose config --quiet && echo "OK"
```

Expected: `OK` (no YAML errors).

- [ ] **Step 4: Commit**

```bash
git add docker-compose.yml
git commit -m "feat: add Metabase service to docker-compose"
```

---

## Task 4: Makefile targets

**Files:**
- Modify: `Makefile`

- [ ] **Step 1: Add targets to `Makefile`**

Add `metabase-open` and `metabase-logs` to the `.PHONY` line and add the targets:

```makefile
.PHONY: up down migrate reset-data backfill-jolpica backfill-openf1 ingest-year dbt-run dbt-test lint test metabase-open metabase-logs
```

Add after the `ingest-year` target:

```makefile
metabase-open:
	@echo "Opening Metabase at http://localhost:3000"
	@xdg-open http://localhost:3000 2>/dev/null || open http://localhost:3000 2>/dev/null || echo "Visit http://localhost:3000"

metabase-logs:
	docker-compose logs -f metabase
```

- [ ] **Step 2: Commit**

```bash
git add Makefile
git commit -m "feat: add metabase-open and metabase-logs make targets"
```

---

## Task 5: Bring up the stack and configure Metabase

This task is manual. No code changes.

- [ ] **Step 1: Reset Postgres so the init script runs** (only needed if Postgres was already started before Task 1)

If the `postgres` container already exists, the init script won't re-run automatically. Reset the volume:

```bash
docker-compose down
docker volume rm f1_postgres-data
docker-compose up -d postgres
```

If this is a fresh setup, just run:

```bash
docker-compose up -d
```

- [ ] **Step 2: Start Metabase**

```bash
docker-compose up -d metabase
```

Watch logs until you see `Metabase Initialization COMPLETE`:

```bash
make metabase-logs
```

Expected (after ~60-90 seconds):
```
metabase  | Metabase Initialization COMPLETE
```

- [ ] **Step 3: Open Metabase and complete setup wizard**

Visit http://localhost:3000.

Fill in the setup wizard:
- **Language:** English
- **Your name / email / password:** anything (personal project)
- **Add your data:** click "I'll add my data later" — we'll add ClickHouse manually

- [ ] **Step 4: Add the ClickHouse database connection**

In Metabase: Settings (gear icon) → Admin → Databases → Add database

| Field | Value |
|---|---|
| Database type | ClickHouse |
| Display name | F1 Warehouse |
| Host | `clickhouse` |
| Port | `8123` |
| Database name | `f1_mart` |
| Username | `default` |
| Password | (leave empty) |

Click **Save**. Metabase will test the connection — expect a green "Connected" confirmation.

---

## Task 6: Build Season Overview dashboard

Manual steps in the Metabase UI. No code changes.

- [ ] **Step 1: Create a new dashboard called "Season Overview"**

Home → New → Dashboard → name it "Season Overview"

- [ ] **Step 2: Add a Year filter**

In the dashboard, click the filter icon → "Number" filter → label it `Year` → connect it to each card as you add them.

- [ ] **Step 3: Add Driver Standings table card**

New question → f1_mart → `mart_standings` → Summarize off → add columns:
`ds.standing_position`, `ds.driver_id`, `driver_name`, `ds.constructor_id`, `constructor_name`, `ds.points`, `ds.wins`

Filter: `ds.round` = (max round for the season — use a variable or filter by round = 22/23/24 based on year).

Save as "Driver Championship Standings".

- [ ] **Step 4: Add Points Progression line chart**

New question → `mart_standings` → x-axis: `ds.round`, y-axis: `ds.points`, group by `driver_name`.

Filter: `ds.season` = Year filter variable. Limit to top 5 drivers by final points for readability.

Save as "Points Progression by Round".

- [ ] **Step 5: Add Constructor Standings bar chart**

New question → `mart_standings` → group by `constructor_name`, sum `ds.points`, filter by year and max round.

Visualization: Bar chart, sorted descending. Save as "Constructor Standings".

- [ ] **Step 6: Add Wins & Podiums bar chart**

New question → `fact_race_results` → filter `season` = Year, filter `finish_position` <= 3 → group by `jolpica_driver_id`, count rows.

Save as "Podiums by Driver". Add to dashboard.

- [ ] **Step 7: Save and verify the dashboard**

Click Save. Apply the Year filter (e.g. 2023). All four cards should update and show data.

---

## Task 7: Build Race Deep-Dive dashboard

Manual steps in the Metabase UI.

- [ ] **Step 1: Create dashboard "Race Deep-Dive"**

New → Dashboard → "Race Deep-Dive"

Add two filters: **Year** (Number) and **Round** (Number).

- [ ] **Step 2: Grid → Finish position changes table**

New question → `fact_race_results` → columns: `jolpica_driver_id`, `grid_position`, `finish_position`, expression `grid_position - finish_position` as `positions_gained` → filter by `season` + `round` → sort by `finish_position`.

Save as "Race Result & Position Changes".

- [ ] **Step 3: Lap time distribution chart**

New question → `mart_lap_analysis` → x-axis: `lap_number`, y-axis: `lap_time_ms` (÷ 1000 for seconds), group by `driver_id` → filter by `season` + `round`.

Save as "Lap Times by Driver".

- [ ] **Step 4: Pit strategy table**

New question → `mart_strategy` → columns: `driver_id`, `stop_number`, `pit_lap`, `compound`, `stint_number`, `lap_start`, `lap_end` → filter by `r.season` + `r.round` → sort by `driver_id`, `stop_number`.

Save as "Pit Strategy".

- [ ] **Step 5: Fastest lap sector breakdown**

New question → `mart_lap_analysis` → filter by `season` + `round`, find the min `lap_time_ms` per driver → show `driver_id`, `of1_s1`, `of1_s2`, `of1_s3`.

Save as "Fastest Lap Sector Breakdown". Add all cards and save dashboard.

---

## Task 8: Build Driver Comparison dashboard

Manual steps in the Metabase UI.

- [ ] **Step 1: Create dashboard "Driver Comparison"**

New → Dashboard → "Driver Comparison"

Add filters: **Year** (Number), **Driver 1** (Text), **Driver 2** (Text).

- [ ] **Step 2: Points trajectory dual-line chart**

New question → `mart_standings` → filter `ds.season` = Year, filter `ds.driver_id` = Driver 1 OR Driver 2 → x: `ds.round`, y: `ds.points`, group by `driver_name`.

Save as "Points Trajectory".

- [ ] **Step 3: Average finish position bar**

New question → `fact_race_results` → filter `season` = Year, filter `jolpica_driver_id` = Driver 1 OR Driver 2 → group by `jolpica_driver_id`, average `finish_position`.

Save as "Average Finish Position".

- [ ] **Step 4: Qualifying vs finish position table**

New question → `fact_qualifying` → join isn't available in Metabase GUI for cross-table — use a Native SQL question:

```sql
SELECT
    q.round,
    q.driver_id,
    q.qualifying_position,
    r.finish_position,
    r.grid_position
FROM f1_mart.fact_qualifying q
JOIN f1_mart.fact_race_results r
    ON r.season = q.season
    AND r.round = q.round
    AND r.jolpica_driver_id = q.driver_id
WHERE q.season = {{year}}
  AND q.driver_id IN ({{driver_1}}, {{driver_2}})
ORDER BY q.round
```

Save as "Qualifying vs Race Position".

- [ ] **Step 5: Head-to-head record**

Native SQL question:

```sql
SELECT
    jolpica_driver_id AS driver,
    countIf(finish_position < (
        SELECT finish_position
        FROM f1_mart.fact_race_results r2
        WHERE r2.season = r1.season
          AND r2.round = r1.round
          AND r2.jolpica_driver_id != r1.jolpica_driver_id
          AND r2.jolpica_driver_id IN ({{driver_1}}, {{driver_2}})
        LIMIT 1
    )) AS head_to_head_wins
FROM f1_mart.fact_race_results r1
WHERE season = {{year}}
  AND jolpica_driver_id IN ({{driver_1}}, {{driver_2}})
GROUP BY jolpica_driver_id
```

Save as "Head-to-Head Record". Add all cards and save the dashboard.

---

## Verification Checklist

After completing all tasks:

- [ ] `docker-compose ps` shows `metabase` as `Up`
- [ ] http://localhost:3000 loads Metabase
- [ ] ClickHouse connection shows "Connected" in Admin → Databases
- [ ] Season Overview dashboard loads with Year = 2023 and shows standings data
- [ ] Race Deep-Dive dashboard loads with Year = 2023, Round = 1 and shows lap data
- [ ] Driver Comparison loads with Year = 2023, Driver 1 = `hamilton`, Driver 2 = `max_verstappen`

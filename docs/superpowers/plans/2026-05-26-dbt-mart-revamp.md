# dbt Mart Revamp Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Revamp the dbt layer so every table an application needs to power lap telemetry visualizations, qualifying comparisons, and session browsing is in a clean mart — no app ever touches a raw table.

**Architecture:** Add a `stg_openf1__location` staging model, an `int_lap_boundaries` intermediate that computes per-lap time windows via LEAD(), and two new mart tables: `mart_lap_telemetry` (telemetry+GPS per sample sliced by lap, the largest table) and `mart_qualifying_summary` (one row per driver per qualifying session, fully denormalized for comparison UIs). Also add `dim_sessions` for session browsing and enrich `fact_qualifying` with OpenF1 best-lap detail. Drop the dead-end `int_telemetry_enriched`.

**Tech Stack:** dbt-clickhouse, ClickHouse 24.x, ClickHouse ASOF JOIN, ClickHouse `argMin` aggregation, window functions (LEAD)

---

## File Map

| Action | File | Purpose |
|--------|------|---------|
| Modify | `dbt/models/staging/stg_openf1__laps.sql` | Add `segments_sector_1/2/3` arrays |
| Create | `dbt/models/staging/stg_openf1__location.sql` | Materialize GPS location data |
| Create | `dbt/models/intermediate/int_lap_boundaries.sql` | Per-lap time windows via LEAD() |
| Create | `dbt/models/marts/dimensions/dim_sessions.sql` | Clean session dimension for app browsing |
| Modify | `dbt/models/marts/facts/fact_qualifying.sql` | Add OpenF1 best-lap sectors/speeds/segments |
| Create | `dbt/models/marts/telemetry/mart_lap_telemetry.sql` | Per-sample telemetry+GPS tagged by lap |
| Create | `dbt/models/marts/analytics/mart_qualifying_summary.sql` | Denormalized qualifying comparison table |
| Delete | `dbt/models/intermediate/int_telemetry_enriched.sql` | Remove dead-end model |
| Modify | `dbt/models/marts/schema.yml` | Add tests for new models |

---

## Task 1: Add segment arrays to stg_openf1__laps

**Files:**
- Modify: `dbt/models/staging/stg_openf1__laps.sql`

The current staging model omits `segments_sector_1/2/3` (Array(UInt16) columns from the raw table). These are needed for mini-sector color visualization in qualifying comparison.

- [ ] **Step 1: Update stg_openf1__laps.sql**

Replace the entire file content:

```sql
SELECT
    session_key,
    driver_number,
    lap_number,
    toDateTime(date_start)          AS lap_start,
    lap_duration,
    is_pit_out_lap,
    duration_sector_1,
    duration_sector_2,
    duration_sector_3,
    i1_speed,
    i2_speed,
    st_speed,
    segments_sector_1,
    segments_sector_2,
    segments_sector_3,
    _ingested_at,
    'openf1'                        AS _source
FROM {{ source('raw_openf1', 'laps') }}
```

- [ ] **Step 2: Verify it compiles**

```bash
cd /home/rajeev/workspace/f1 && source venv/bin/activate && cd dbt && dbt compile --select stg_openf1__laps --profiles-dir .
```

Expected: `Compiled node 'stg_openf1__laps'` with no errors.

- [ ] **Step 3: Commit**

```bash
git add dbt/models/staging/stg_openf1__laps.sql
git commit -m "feat(dbt): add segment arrays to stg_openf1__laps"
```

---

## Task 2: Create stg_openf1__location

**Files:**
- Create: `dbt/models/staging/stg_openf1__location.sql`

Location data (~3.7Hz GPS, high volume) needs a dedicated materialized staging model with the same partition strategy as `stg_openf1__car_data`. Without this there is no mart-level GPS data.

- [ ] **Step 1: Create the file**

```sql
{{
    config(
        materialized='incremental',
        incremental_strategy='append',
        engine='MergeTree()',
        partition_by='(toYear(date), session_key)',
        order_by='(session_key, driver_number, date)',
    )
}}

SELECT
    session_key,
    driver_number,
    toDateTime64(date, 3)   AS date,
    x,
    y,
    z,
    _ingested_at
FROM {{ source('raw_openf1', 'location') }}

{% if is_incremental() %}
WHERE _ingested_at > (SELECT max(_ingested_at) FROM {{ this }})
{% endif %}
```

- [ ] **Step 2: Compile and run**

```bash
cd /home/rajeev/workspace/f1 && source venv/bin/activate && cd dbt && dbt run --select stg_openf1__location --profiles-dir .
```

Expected: `OK created incremental model f1_staging.stg_openf1__location`

- [ ] **Step 3: Verify row count matches raw**

```bash
docker exec f1-clickhouse-1 clickhouse-client --query "
SELECT
    (SELECT count() FROM raw_openf1.location) AS raw_count,
    (SELECT count() FROM f1_staging.stg_openf1__location) AS stg_count
"
```

Expected: both counts match.

- [ ] **Step 4: Commit**

```bash
git add dbt/models/staging/stg_openf1__location.sql
git commit -m "feat(dbt): add stg_openf1__location staging model"
```

---

## Task 3: Create int_lap_boundaries

**Files:**
- Create: `dbt/models/intermediate/int_lap_boundaries.sql`

For each lap, compute `lap_end` as the `lap_start` of the next lap (via LEAD). This is the key intermediate that lets `mart_lap_telemetry` slice raw telemetry into per-lap windows without touching raw tables.

- [ ] **Step 1: Create the file**

```sql
-- Per-lap time windows for any OpenF1 session.
-- lap_end is NULL for the final lap of each driver (handle with IS NULL check downstream).
SELECT
    session_key,
    driver_number,
    lap_number,
    lap_start,
    LEAD(lap_start) OVER (
        PARTITION BY session_key, driver_number
        ORDER BY lap_number
    )                           AS lap_end,
    lap_duration,
    is_pit_out_lap,
    duration_sector_1,
    duration_sector_2,
    duration_sector_3,
    i1_speed,
    i2_speed,
    st_speed,
    segments_sector_1,
    segments_sector_2,
    segments_sector_3
FROM {{ ref('stg_openf1__laps') }}
```

- [ ] **Step 2: Compile**

```bash
cd /home/rajeev/workspace/f1 && source venv/bin/activate && cd dbt && dbt compile --select int_lap_boundaries --profiles-dir .
```

Expected: compiles cleanly.

- [ ] **Step 3: Test the window logic manually**

```bash
docker exec f1-clickhouse-1 clickhouse-client --query "
SELECT driver_number, lap_number, lap_start,
       leadInFrame(lap_start) OVER (PARTITION BY session_key, driver_number ORDER BY lap_number) AS lap_end
FROM raw_openf1.laps
WHERE session_key = 9472 AND driver_number = 1
ORDER BY lap_number
LIMIT 5
"
```

Expected: `lap_end` of each row equals `lap_start` of the next row; last row has NULL `lap_end`.

- [ ] **Step 4: Commit**

```bash
git add dbt/models/intermediate/int_lap_boundaries.sql
git commit -m "feat(dbt): add int_lap_boundaries intermediate model"
```

---

## Task 4: Create dim_sessions

**Files:**
- Create: `dbt/models/marts/dimensions/dim_sessions.sql`
- Modify: `dbt/models/marts/schema.yml`

Apps need a single table to list sessions for a season, know whether telemetry exists, and get race/circuit context for a session_key. Currently this requires joining three tables manually.

- [ ] **Step 1: Create dim_sessions.sql**

```sql
-- Clean session dimension. One row per OpenF1 session_key.
-- Pre-2023 races have no OpenF1 sessions and are not represented here.
{{
    config(
        engine='ReplacingMergeTree()',
        order_by='session_key',
    )
}}

SELECT
    s.session_key,
    s.year                  AS season,
    r.round,
    r.race_name,
    r.circuit_id,
    c.circuit_name,
    c.country               AS circuit_country,
    s.session_name,
    s.session_type,
    s.date_start,
    s.date_end,
    s.location,
    s.country_name,
    s.country_code,
    s.circuit_short_name
FROM {{ ref('stg_openf1__sessions') }}      s
LEFT JOIN {{ ref('stg_jolpica__races') }}   r
    ON  r.season    = s.year
    AND r.race_date = toDate(s.date_start)
LEFT JOIN {{ ref('dim_circuits') }}         c
    ON  c.circuit_id = r.circuit_id
```

- [ ] **Step 2: Add test to schema.yml**

Add inside the `models:` list in `dbt/models/marts/schema.yml`:

```yaml
  - name: dim_sessions
    columns:
      - name: session_key
        tests:
          - not_null
          - unique
```

- [ ] **Step 3: Run and verify**

```bash
cd /home/rajeev/workspace/f1 && source venv/bin/activate && cd dbt && dbt run --select dim_sessions --profiles-dir . && dbt test --select dim_sessions --profiles-dir .
```

Expected: model runs, both tests pass.

- [ ] **Step 4: Spot-check Monaco 2024**

```bash
docker exec f1-clickhouse-1 clickhouse-client --query "
SELECT session_key, season, round, race_name, session_type, date_start
FROM f1_mart.dim_sessions
WHERE season = 2024 AND race_name LIKE '%Monaco%'
ORDER BY session_key
"
```

Expected: 5 rows (Practice ×3, Qualifying, Race) with `race_name = 'Monaco Grand Prix'` and correct `round`.

- [ ] **Step 5: Commit**

```bash
git add dbt/models/marts/dimensions/dim_sessions.sql dbt/models/marts/schema.yml
git commit -m "feat(dbt): add dim_sessions dimension"
```

---

## Task 5: Enrich fact_qualifying with OpenF1 best-lap detail

**Files:**
- Modify: `dbt/models/marts/facts/fact_qualifying.sql`

Currently `fact_qualifying` only has Jolpica Q1/Q2/Q3 time strings. For 2023+ races we can join the actual best OpenF1 lap to get sector times, speed traps, and mini-segment arrays — everything needed for the comparison UI.

- [ ] **Step 1: Replace fact_qualifying.sql**

```sql
{{
    config(
        materialized='incremental',
        incremental_strategy='append',
        engine='MergeTree()',
        order_by='(season, round, driver_id)',
    )
}}

WITH best_laps AS (
    SELECT
        session_key,
        driver_number,
        argMin(lap_number,      lap_duration)   AS best_lap_number,
        min(lap_duration)                       AS best_lap_duration,
        argMin(duration_sector_1, lap_duration) AS best_s1,
        argMin(duration_sector_2, lap_duration) AS best_s2,
        argMin(duration_sector_3, lap_duration) AS best_s3,
        argMin(i1_speed,        lap_duration)   AS i1_speed,
        argMin(i2_speed,        lap_duration)   AS i2_speed,
        argMin(st_speed,        lap_duration)   AS st_speed,
        argMin(segments_sector_1, lap_duration) AS segments_s1,
        argMin(segments_sector_2, lap_duration) AS segments_s2,
        argMin(segments_sector_3, lap_duration) AS segments_s3
    FROM {{ ref('stg_openf1__laps') }}
    WHERE is_pit_out_lap = 0
      AND lap_duration IS NOT NULL
    GROUP BY session_key, driver_number
)

SELECT
    q.season                AS season,
    q.round                 AS round,
    q.driver_id             AS driver_id,
    q.constructor_id        AS constructor_id,
    q.qualifying_position   AS qualifying_position,
    q.q1                    AS q1,
    q.q2                    AS q2,
    q.q3                    AS q3,
    m.openf1_driver_number  AS openf1_driver_number,
    sm.session_key          AS session_key,
    -- OpenF1 best lap detail (NULL for pre-2023)
    bl.best_lap_number      AS best_lap_number,
    bl.best_lap_duration    AS best_lap_duration,
    bl.best_s1              AS best_s1,
    bl.best_s2              AS best_s2,
    bl.best_s3              AS best_s3,
    bl.i1_speed             AS i1_speed,
    bl.i2_speed             AS i2_speed,
    bl.st_speed             AS st_speed,
    bl.segments_s1          AS segments_s1,
    bl.segments_s2          AS segments_s2,
    bl.segments_s3          AS segments_s3
FROM {{ ref('stg_jolpica__qualifying') }}       q
LEFT JOIN {{ source('dim', 'driver_id_map') }}  m
    ON  m.jolpica_driver_id = q.driver_id
    AND m.season            = q.season
LEFT JOIN {{ ref('int_session_map') }}          sm
    ON  sm.season       = q.season
    AND sm.round        = q.round
    AND sm.session_type = 'Qualifying'
LEFT JOIN best_laps                             bl
    ON  bl.session_key   = sm.session_key
    AND bl.driver_number = m.openf1_driver_number

{% if is_incremental() %}
WHERE (q.season, q.round) NOT IN (
    SELECT season, round FROM {{ this }}
)
{% endif %}
```

- [ ] **Step 2: Full-refresh fact_qualifying (schema changed)**

```bash
cd /home/rajeev/workspace/f1 && source venv/bin/activate && cd dbt && dbt run --full-refresh --select fact_qualifying --profiles-dir .
```

Expected: `OK created incremental model f1_mart.fact_qualifying`

- [ ] **Step 3: Verify new columns populated for 2023+**

```bash
docker exec f1-clickhouse-1 clickhouse-client --query "
SELECT driver_id, qualifying_position, q3, best_lap_duration, best_s1, best_s2, best_s3, i1_speed
FROM f1_mart.fact_qualifying
WHERE season = 2024 AND round = 1
ORDER BY qualifying_position
LIMIT 5
"
```

Expected: top-5 rows have non-NULL `best_lap_duration`, `best_s1/2/3`, `i1_speed`.

- [ ] **Step 4: Commit**

```bash
git add dbt/models/marts/facts/fact_qualifying.sql
git commit -m "feat(dbt): enrich fact_qualifying with OpenF1 best-lap sectors and segments"
```

---

## Task 6: Create mart_qualifying_summary

**Files:**
- Create: `dbt/models/marts/analytics/mart_qualifying_summary.sql`
- Modify: `dbt/models/marts/schema.yml`

This is the table the qualifying comparison website queries directly. One row per driver per qualifying session, fully denormalized with driver appearance info (headshot, team color), Jolpica bio data, and all lap timing detail. No raw table joins needed from the app.

- [ ] **Step 1: Create mart_qualifying_summary.sql**

```sql
-- Qualifying comparison mart. One row per driver per qualifying session.
-- All columns an app needs for a head-to-head comparison UI, no further joins required.
-- OpenF1 columns (sectors, speeds, segments, headshot, team_colour) are NULL pre-2023.
SELECT
    fq.season,
    fq.round,
    ds.race_name,
    ds.circuit_name,
    ds.circuit_country,
    ds.location             AS circuit_location,
    ds.date_start           AS qualifying_date,
    fq.driver_id,
    fq.constructor_id,
    fq.qualifying_position,
    -- Jolpica lap time strings
    fq.q1,
    fq.q2,
    fq.q3,
    COALESCE(fq.q3, fq.q2, fq.q1) AS best_time,
    -- OpenF1 best lap (2023+ only)
    fq.session_key,
    fq.openf1_driver_number,
    fq.best_lap_number,
    fq.best_lap_duration,
    fq.best_s1,
    fq.best_s2,
    fq.best_s3,
    fq.i1_speed,
    fq.i2_speed,
    fq.st_speed,
    fq.segments_s1,
    fq.segments_s2,
    fq.segments_s3,
    -- Driver bio (Jolpica, all eras)
    d.full_name             AS driver_name,
    d.given_name,
    d.family_name,
    d.driver_code,
    d.permanent_number,
    d.nationality           AS driver_nationality,
    -- Driver appearance (OpenF1, 2023+ only)
    od.name_acronym,
    od.team_name,
    concat('#', od.team_colour) AS team_colour,
    od.headshot_url,
    -- Constructor
    c.constructor_name
FROM {{ ref('fact_qualifying') }}               fq
LEFT JOIN {{ ref('dim_sessions') }}             ds
    ON  ds.session_key = fq.session_key
LEFT JOIN {{ ref('dim_drivers') }}              d
    ON  d.driver_id = fq.driver_id
LEFT JOIN {{ ref('stg_openf1__drivers') }}      od
    ON  od.session_key   = fq.session_key
    AND od.driver_number = fq.openf1_driver_number
LEFT JOIN {{ ref('dim_constructors') }}         c
    ON  c.constructor_id = fq.constructor_id
```

- [ ] **Step 2: Add test to schema.yml**

Add inside the `models:` list in `dbt/models/marts/schema.yml`:

```yaml
  - name: mart_qualifying_summary
    columns:
      - name: driver_id
        tests:
          - not_null
      - name: season
        tests:
          - not_null
      - name: round
        tests:
          - not_null
```

- [ ] **Step 3: Run and test**

```bash
cd /home/rajeev/workspace/f1 && source venv/bin/activate && cd dbt && dbt run --select mart_qualifying_summary --profiles-dir . && dbt test --select mart_qualifying_summary --profiles-dir .
```

Expected: model runs, tests pass.

- [ ] **Step 4: Spot-check Monaco 2024**

```bash
docker exec f1-clickhouse-1 clickhouse-client --query "
SELECT qualifying_position, driver_name, name_acronym, team_name, team_colour,
       best_time, best_lap_duration, best_s1, best_s2, best_s3
FROM f1_mart.mart_qualifying_summary
WHERE season = 2024 AND round = 8
ORDER BY qualifying_position
LIMIT 5
"
```

Expected: 5 rows with `driver_name`, `team_colour` as `#xxxxxx`, `best_time` as `1:xx.xxx`.

- [ ] **Step 5: Commit**

```bash
git add dbt/models/marts/analytics/mart_qualifying_summary.sql dbt/models/marts/schema.yml
git commit -m "feat(dbt): add mart_qualifying_summary for comparison UI"
```

---

## Task 7: Create mart_lap_telemetry

**Files:**
- Create: `dbt/models/marts/telemetry/mart_lap_telemetry.sql`
- Modify: `dbt/models/marts/schema.yml`

The centerpiece of the revamp. Joins car telemetry (speed, throttle, brake, gear, drs, rpm) with GPS position (x, y, z) at each sample point, tagged with lap number and context. Uses ClickHouse ASOF JOIN to align GPS timestamps to telemetry timestamps. Partitioned by year+session_key for fast per-lap queries.

ASOF JOIN semantics: for each car_data sample at time T, find the most recent location row at time ≤ T. This aligns the ~3.7Hz car_data and ~3.7Hz location streams which may not be perfectly synchronized.

- [ ] **Step 1: Create the telemetry directory**

```bash
mkdir -p /home/rajeev/workspace/f1/dbt/models/marts/telemetry
```

- [ ] **Step 2: Create mart_lap_telemetry.sql**

```sql
-- Per-sample telemetry joined with GPS, sliced by lap. 2023+ only.
-- Partition by (year, session_key) — query always filters on session_key for speed.
-- ASOF JOIN aligns car_data timestamps to nearest preceding location timestamp.
{{
    config(
        materialized='incremental',
        incremental_strategy='append',
        engine='MergeTree()',
        partition_by='(toYear(cd.date), cd.session_key)',
        order_by='(cd.session_key, cd.driver_number, cd.date)',
    )
}}

SELECT
    cd.session_key,
    cd.driver_number,
    lb.lap_number,
    lb.is_pit_out_lap,
    cd.date,
    cd.speed,
    cd.rpm,
    cd.n_gear,
    cd.throttle,
    cd.brake,
    cd.drs,
    loc.x,
    loc.y,
    loc.z,
    sm.season,
    sm.round,
    sm.session_type,
    jmap.jolpica_driver_id  AS driver_id,
    d.name_acronym          AS driver_code,
    d.team_name,
    concat('#', d.team_colour) AS team_colour
FROM {{ ref('stg_openf1__car_data') }}      cd
JOIN {{ ref('int_lap_boundaries') }}        lb
    ON  lb.session_key   = cd.session_key
    AND lb.driver_number = cd.driver_number
    AND cd.date >= lb.lap_start
    AND (lb.lap_end IS NULL OR cd.date < lb.lap_end)
ASOF LEFT JOIN {{ ref('stg_openf1__location') }} loc
    ON  loc.session_key   = cd.session_key
    AND loc.driver_number = cd.driver_number
    AND loc.date         <= cd.date
LEFT JOIN {{ ref('int_session_map') }}      sm
    ON  sm.session_key = cd.session_key
LEFT JOIN {{ ref('stg_openf1__drivers') }}  d
    ON  d.session_key   = cd.session_key
    AND d.driver_number = cd.driver_number
LEFT JOIN {{ source('dim', 'driver_id_map') }} jmap
    ON  jmap.openf1_driver_number = cd.driver_number
    AND jmap.season               = sm.season

{% if is_incremental() %}
WHERE cd._ingested_at > (SELECT max(_ingested_at) FROM {{ this }})
{% endif %}
```

- [ ] **Step 3: Add test to schema.yml**

Add inside the `models:` list in `dbt/models/marts/schema.yml`:

```yaml
  - name: mart_lap_telemetry
    columns:
      - name: session_key
        tests:
          - not_null
      - name: driver_number
        tests:
          - not_null
      - name: lap_number
        tests:
          - not_null
```

- [ ] **Step 4: Run (this will take several minutes — high volume)**

```bash
cd /home/rajeev/workspace/f1 && source venv/bin/activate && cd dbt && dbt run --select mart_lap_telemetry --profiles-dir .
```

Expected: `OK created incremental model f1_mart.mart_lap_telemetry` (runtime will be several minutes for first build).

- [ ] **Step 5: Verify row count and spot-check**

```bash
docker exec f1-clickhouse-1 clickhouse-client --query "SELECT count() FROM f1_mart.mart_lap_telemetry"
```

Expected: tens of millions of rows for full 2024 season.

```bash
docker exec f1-clickhouse-1 clickhouse-client --query "
SELECT lap_number, count() AS samples,
       round(min(speed),0) AS min_spd, round(max(speed),0) AS max_spd,
       countIf(brake > 0) AS brake_samples,
       round(avg(x),0) AS avg_x, round(avg(y),0) AS avg_y
FROM f1_mart.mart_lap_telemetry
WHERE session_key = 9472 AND driver_number = 1
GROUP BY lap_number
ORDER BY lap_number
LIMIT 5
"
```

Expected: rows with sensible speed ranges (0–350 km/h), non-zero brake samples, non-zero x/y GPS values.

- [ ] **Step 6: Commit**

```bash
git add dbt/models/marts/telemetry/mart_lap_telemetry.sql dbt/models/marts/schema.yml
git commit -m "feat(dbt): add mart_lap_telemetry — telemetry+GPS per lap"
```

---

## Task 8: Remove int_telemetry_enriched

**Files:**
- Delete: `dbt/models/intermediate/int_telemetry_enriched.sql`

`int_telemetry_enriched` had no consumers in the mart layer and is now superseded by `mart_lap_telemetry`. Keeping it wastes compute and storage on every dbt run.

- [ ] **Step 1: Check it has no dependents**

```bash
cd /home/rajeev/workspace/f1 && source venv/bin/activate && cd dbt && dbt ls --select int_telemetry_enriched+ --profiles-dir . 2>&1 | grep -v "^$"
```

Expected: only `int_telemetry_enriched` itself listed (no downstream models).

- [ ] **Step 2: Delete the file**

```bash
rm /home/rajeev/workspace/f1/dbt/models/intermediate/int_telemetry_enriched.sql
```

- [ ] **Step 3: Drop the ClickHouse table**

```bash
docker exec f1-clickhouse-1 clickhouse-client --query "DROP TABLE IF EXISTS f1_intermediate.int_telemetry_enriched"
```

- [ ] **Step 4: Verify dbt compiles without it**

```bash
cd /home/rajeev/workspace/f1 && source venv/bin/activate && cd dbt && dbt compile --profiles-dir .
```

Expected: no errors referencing `int_telemetry_enriched`.

- [ ] **Step 5: Commit**

```bash
git add -A dbt/models/intermediate/int_telemetry_enriched.sql
git commit -m "chore(dbt): remove dead-end int_telemetry_enriched"
```

---

## Task 9: Full dbt run and final validation

- [ ] **Step 1: Run all models**

```bash
cd /home/rajeev/workspace/f1 && source venv/bin/activate && cd dbt && dbt run --profiles-dir .
```

Expected: all 19+ models `OK`, no errors.

- [ ] **Step 2: Run all tests**

```bash
cd dbt && dbt test --profiles-dir .
```

Expected: all tests pass.

- [ ] **Step 3: Verify the full query an app would run**

This is the canonical app query — all data for a qualifying lap comparison between two drivers:

```bash
docker exec f1-clickhouse-1 clickhouse-client --query "
-- Compare best qualifying laps: two drivers in Monaco 2024
SELECT driver_name, name_acronym, team_colour, qualifying_position,
       best_time, best_lap_duration, best_s1, best_s2, best_s3,
       i1_speed, i2_speed, st_speed
FROM f1_mart.mart_qualifying_summary
WHERE season = 2024 AND round = 8
ORDER BY qualifying_position
"
```

Expected: one row per driver, enriched with appearance data and sector times.

```bash
docker exec f1-clickhouse-1 clickhouse-client --query "
-- Full telemetry for a specific driver's best qualifying lap
-- (replace session_key and driver_number once Monaco data is ingested)
SELECT date, lap_number, speed, throttle, brake, n_gear, drs, x, y, z
FROM f1_mart.mart_lap_telemetry
WHERE session_key = 9472
  AND driver_number = 1
  AND lap_number = 44
  AND is_pit_out_lap = 0
ORDER BY date
LIMIT 10
"
```

Expected: rows with all telemetry and GPS columns populated, ordered by timestamp.

- [ ] **Step 4: Commit**

```bash
git add -A
git commit -m "chore(dbt): full validation — mart revamp complete"
```

---

## App Query Reference

Once complete, these are the only queries an app ever needs:

**Browse qualifying sessions for a season:**
```sql
SELECT session_key, round, race_name, circuit_name, qualifying_date
FROM f1_mart.dim_sessions
WHERE season = ? AND session_type = 'Qualifying'
ORDER BY round
```

**List drivers in a qualifying session:**
```sql
SELECT qualifying_position, driver_name, name_acronym, team_colour,
       headshot_url, best_time, best_lap_duration
FROM f1_mart.mart_qualifying_summary
WHERE season = ? AND round = ?
ORDER BY qualifying_position
```

**Full telemetry for a driver's best qualifying lap:**
```sql
-- Step 1: get best_lap_number from mart_qualifying_summary
-- Step 2: fetch telemetry
SELECT date, speed, throttle, brake, n_gear, drs, rpm, x, y, z
FROM f1_mart.mart_lap_telemetry
WHERE session_key = ?
  AND driver_number = ?
  AND lap_number = ?
  AND is_pit_out_lap = 0
ORDER BY date
```

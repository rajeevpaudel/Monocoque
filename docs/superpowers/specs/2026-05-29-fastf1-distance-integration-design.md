# FastF1 Distance Integration Design

**Date:** 2026-05-29  
**Scope:** Add `distance_m` to `mart_lap_telemetry` via FastF1 as the authoritative source  
**Priority:** Low (nice-to-have per new-data-req.md §6)

---

## Problem

`mart_lap_telemetry` has no `distance_m` column. The front-end app computes cumulative arc-length from GPS `(x, y)` differences as a proxy. This works but is a secondary approximation: it is sensitive to GPS noise/outliers and means sector-break fractions in `trackPaths.json` are inferred rather than authoritative.

The F1 live telemetry stream carries a `Distance` channel (cumulative metres from the start/finish line, starting at 0.0 for each lap). FastF1 exposes this as `tel['Distance']`. OpenF1 ingests the same stream but does not re-expose Distance.

---

## Decision

Use **FastF1 (Option A — thin distance-only table)**. Store only the columns we don't already have from OpenF1. No duplication of Speed, RPM, GPS, etc.

Scope: **qualifying sessions only**, years **2023–present** (earlier years have no OpenF1 telemetry to join distance into).

---

## Architecture

```
F1 live telemetry stream
        │
    FastF1 library (Python)
        │
ingestion/fastf1/
  client.py      ← wraps fastf1.get_session(), manages local cache dir
  backfill.py    ← iterates year/rounds, writes to ClickHouse
        │
raw_fastf1.car_telemetry  (new ClickHouse table)
        │
stg_fastf1__distances     (new dbt staging model)
        │
mart_lap_telemetry        (updated — ASOF JOIN adds distance_m)
```

---

## ClickHouse Schema

### New database + table (migration 006)

```sql
CREATE DATABASE IF NOT EXISTS raw_fastf1;

CREATE TABLE IF NOT EXISTS raw_fastf1.car_telemetry (
    year            UInt16,
    round           UInt8,
    session_type    LowCardinality(String),  -- 'Qualifying'
    driver_number   UInt8,
    date            DateTime64(3, 'UTC'),
    distance_m      Float32,
    _ingested_at    DateTime DEFAULT now()
)
ENGINE = MergeTree()
PARTITION BY year
ORDER BY (year, round, driver_number, date);
```

No `session_key` at storage time — we bridge to OpenF1's `session_key` via `int_session_map` in dbt.

---

## Ingestion Layer

### `ingestion/fastf1/client.py`

Thin wrapper:
- Configures FastF1's local cache dir (`/tmp/fastf1_cache` or env `FASTF1_CACHE_DIR`)
- `get_qualifying_telemetry(year, round) -> list[dict]` — loads the session, iterates drivers, returns rows with `(year, round, session_type, driver_number, date, distance_m)`
- Filters to laps where `Distance` is not NaN and is monotonically increasing (FastF1 sometimes has reset artefacts at lap boundaries)

### `ingestion/fastf1/backfill.py`

```
usage: python -m ingestion.fastf1.backfill --start 2023 --end 2025

for each year:
    for each round (1..N):
        if (year, round) already in checkpoint: skip
        rows = client.get_qualifying_telemetry(year, round)
        clickhouse.insert('raw_fastf1.car_telemetry', rows)
        checkpoint.add((year, round))
```

Checkpoint file: `ingestion/fastf1/.backfill_checkpoint.json` (same pattern as OpenF1).  
Rate limiting: FastF1 fetches from F1's CDN — no strict rate limit, but cache after first fetch means re-runs are free.

### Pydantic model (`ingestion/shared/models.py` addition)

```python
class FastF1TelemetryRow(RawRecord):
    year: int
    round: int
    session_type: str
    driver_number: int
    date: str          # ISO datetime string
    distance_m: float
```

---

## dbt Staging Model

**`stg_fastf1__distances`**

```sql
SELECT
    year,
    round,
    session_type,
    driver_number,
    toDateTime64(date, 3, 'UTC') AS date,
    distance_m
FROM {{ source('raw_fastf1', 'car_telemetry') }}
```

Add `raw_fastf1` as a new dbt source in `sources.yml`.

---

## mart_lap_telemetry Update

Add a third ASOF JOIN after the existing two:

```sql
ASOF LEFT JOIN {{ ref('stg_fastf1__distances') }} f1d
    ON  f1d.year          = sm.season
    AND f1d.round         = sm.round
    AND f1d.session_type  = 'Qualifying'
    AND f1d.driver_number = cd.driver_number
    AND f1d.date         <= cd.date
```

Add to SELECT:

```sql
f1d.distance_m  AS distance_m
```

Add to ClickHouse schema (migration 007):

```sql
ALTER TABLE f1_mart.mart_lap_telemetry
    ADD COLUMN IF NOT EXISTS distance_m Nullable(Float32);
```

`distance_m` is `NULL` when no FastF1 data has been ingested for that session (pre-2023, or sessions not yet backfilled).

---

## Data Quality

FastF1's `Distance` channel can have artefacts at lap boundaries (resets to 0 mid-lap if the timing system drops a lap-start marker). The client filters these by:
1. Only emitting rows where `distance_m >= 0`
2. Resetting distance per lap — FastF1's `tel['Distance']` is already per-lap cumulative when accessed via `lap.get_telemetry()`

The ASOF JOIN naturally handles sub-millisecond timestamp misalignment between FastF1 and OpenF1 samples.

---

## Rollout

1. Run migration 006 (create `raw_fastf1.car_telemetry`)
2. Install `fastf1` Python package in the project environment
3. Run `python -m ingestion.fastf1.backfill --start 2023 --end 2025`
4. Run `dbt run --select stg_fastf1__distances`
5. Run migration 007 (add `distance_m` to mart table)
6. Run `dbt run --full-refresh --select mart_lap_telemetry` to rebuild with distance

---

## What This Does NOT Do

- Does not ingest other FastF1 channels (Speed, RPM, GPS) — OpenF1 already covers those
- Does not backfill pre-2023 (no OpenF1 telemetry to join into)
- Does not replace the GPS arc-length computation in `fetch-tracks-warehouse.mjs` — that script uses GPS for SVG path generation, not for lap distance

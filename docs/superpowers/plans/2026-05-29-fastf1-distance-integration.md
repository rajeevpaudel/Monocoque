# FastF1 Distance Integration Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add `distance_m` (cumulative lap distance in metres) to `mart_lap_telemetry` using FastF1 as the authoritative source for all session types (Practice, Qualifying, Sprint, Race) from 2023 onward.

**Architecture:** New `ingestion/fastf1/` module fetches FastF1 telemetry, resolves the OpenF1 `session_key` via the existing sessions API, and stores `(session_key, driver_number, date, distance_m)` in a new `raw_fastf1.car_telemetry` ClickHouse table. A dbt staging model over that table plugs into `mart_lap_telemetry` via a third ASOF JOIN, identical in structure to the existing location join.

**Tech Stack:** Python 3.11, FastF1 ≥3.4, PyArrow, ClickHouse, dbt-clickhouse

---

## File Map

| Action | Path | Responsibility |
|--------|------|----------------|
| Create | `clickhouse/migrations/006_raw_fastf1.sql` | New database + table |
| Create | `clickhouse/migrations/007_add_distance_m.sql` | Add column to mart table |
| Modify | `pyproject.toml` | Add `fastf1` dependency |
| Create | `ingestion/fastf1/__init__.py` | Package marker |
| Create | `ingestion/fastf1/client.py` | FastF1 session loader — pure functions, no I/O |
| Create | `ingestion/fastf1/backfill.py` | Orchestrate year/round/session loop, checkpoint |
| Create | `ingestion/fastf1/.backfill_checkpoint.json` | Persisted progress (created at first run) |
| Create | `tests/__init__.py` | Test package root (does not exist yet) |
| Create | `tests/ingestion/__init__.py` | Test sub-package |
| Create | `tests/ingestion/fastf1/__init__.py` | Test sub-package |
| Create | `tests/ingestion/fastf1/test_client.py` | Unit tests for `_process_lap_telemetry` |
| Create | `tests/ingestion/fastf1/test_backfill.py` | Unit tests for checkpoint + session resolution |
| Modify | `dbt/models/staging/sources.yml` | Add `raw_fastf1` source |
| Create | `dbt/models/staging/stg_fastf1__distances.sql` | Staging model over raw table |
| Modify | `dbt/models/marts/telemetry/mart_lap_telemetry.sql` | Add ASOF JOIN + `distance_m` column |

---

## Task 1: Dependencies and ClickHouse migrations

**Files:**
- Modify: `pyproject.toml`
- Create: `clickhouse/migrations/006_raw_fastf1.sql`
- Create: `clickhouse/migrations/007_add_distance_m.sql`

- [ ] **Step 1: Add fastf1 to pyproject.toml**

In `pyproject.toml`, add `fastf1>=3.4` to the `dependencies` list:

```toml
dependencies = [
    "clickhouse-connect>=0.7",
    "fastf1>=3.4",
    "pydantic>=2.0",
    "structlog>=24.0",
    "pyarrow>=15.0",
    "httpx>=0.27",
    "tenacity>=8.0",
]
```

- [ ] **Step 2: Install the new dependency**

```bash
pip install fastf1>=3.4
```

Expected: fastf1 and its dependencies (pandas, matplotlib, requests, etc.) install without error.

- [ ] **Step 3: Create migration 006**

Create `clickhouse/migrations/006_raw_fastf1.sql`:

```sql
CREATE DATABASE IF NOT EXISTS raw_fastf1;

CREATE TABLE IF NOT EXISTS raw_fastf1.car_telemetry
(
    session_key    Int32,
    driver_number  UInt8,
    date           DateTime64(3, 'UTC'),
    distance_m     Float32,
    _ingested_at   DateTime DEFAULT now()
)
ENGINE = MergeTree()
ORDER BY (session_key, driver_number, date);
```

- [ ] **Step 4: Create migration 007**

Create `clickhouse/migrations/007_add_distance_m.sql`:

```sql
ALTER TABLE f1_mart.mart_lap_telemetry
    ADD COLUMN IF NOT EXISTS distance_m Nullable(Float32);
```

- [ ] **Step 5: Run migrations**

```bash
python clickhouse/migrate.py
```

Expected output ends with: `Applied 7 migration(s) successfully.`

Verify:
```bash
curl -s "http://localhost:8123/?query=DESCRIBE+TABLE+raw_fastf1.car_telemetry"
```
Expected: four columns — `session_key`, `driver_number`, `date`, `distance_m`.

```bash
curl -s "http://localhost:8123/?query=SELECT+name+FROM+system.columns+WHERE+table%3D'mart_lap_telemetry'+AND+name%3D'distance_m'"
```
Expected: one row with `distance_m`.

- [ ] **Step 6: Commit**

```bash
git add pyproject.toml clickhouse/migrations/006_raw_fastf1.sql clickhouse/migrations/007_add_distance_m.sql
git commit -m "feat(migrations): add raw_fastf1 table and distance_m column to mart"
```

---

## Task 2: Unit tests for client (write first — TDD)

**Files:**
- Create: `tests/__init__.py`
- Create: `tests/ingestion/__init__.py`
- Create: `tests/ingestion/fastf1/__init__.py`
- Create: `tests/ingestion/fastf1/test_client.py`

- [ ] **Step 1: Create test package markers**

```bash
mkdir -p tests/ingestion/fastf1
touch tests/__init__.py tests/ingestion/__init__.py tests/ingestion/fastf1/__init__.py
```

- [ ] **Step 2: Write the failing tests**

Create `tests/ingestion/fastf1/test_client.py`:

```python
import numpy as np
import pandas as pd
import pytest

from ingestion.fastf1.client import _process_lap_telemetry


def _make_tel(distances):
    dates = pd.date_range("2024-03-02 14:00:00", periods=len(distances), freq="270ms", tz="UTC")
    return pd.DataFrame({"Date": dates, "Distance": distances})


def test_returns_required_columns():
    result = _process_lap_telemetry(_make_tel([0.0, 10.5, 21.0]), session_key=9001, driver_number=1)
    assert list(result.columns) == ["session_key", "driver_number", "date", "distance_m"]


def test_filters_negative_distances():
    result = _process_lap_telemetry(_make_tel([-1.0, 0.0, 10.5, 21.0]), session_key=9001, driver_number=1)
    assert len(result) == 3
    assert (result["distance_m"] >= 0).all()


def test_filters_nan_distances():
    result = _process_lap_telemetry(_make_tel([0.0, np.nan, 21.0]), session_key=9001, driver_number=1)
    assert len(result) == 2


def test_stamps_session_key_and_driver():
    result = _process_lap_telemetry(_make_tel([0.0, 10.5]), session_key=9001, driver_number=44)
    assert (result["session_key"] == 9001).all()
    assert (result["driver_number"] == 44).all()


def test_empty_input_returns_empty_dataframe():
    result = _process_lap_telemetry(_make_tel([]), session_key=9001, driver_number=1)
    assert result.empty
    assert list(result.columns) == ["session_key", "driver_number", "date", "distance_m"]
```

- [ ] **Step 3: Run tests — verify they fail**

```bash
PYTHONPATH=. pytest tests/ingestion/fastf1/test_client.py -v
```

Expected: `ImportError: cannot import name '_process_lap_telemetry' from 'ingestion.fastf1.client'`

- [ ] **Step 4: Commit failing tests**

```bash
git add tests/
git commit -m "test(fastf1): add failing unit tests for _process_lap_telemetry"
```

---

## Task 3: FastF1 client

**Files:**
- Create: `ingestion/fastf1/__init__.py`
- Create: `ingestion/fastf1/client.py`

- [ ] **Step 1: Create package marker**

Create `ingestion/fastf1/__init__.py` (empty file):

```bash
touch ingestion/fastf1/__init__.py
```

- [ ] **Step 2: Implement client.py**

Create `ingestion/fastf1/client.py`:

```python
"""FastF1 telemetry client — fetches Distance channel for any session type."""

import os

import fastf1
import pandas as pd
import pyarrow as pa

_CACHE_DIR = os.environ.get("FASTF1_CACHE_DIR", "/tmp/fastf1_cache")
os.makedirs(_CACHE_DIR, exist_ok=True)
fastf1.Cache.enable_cache(_CACHE_DIR)

_EMPTY_SCHEMA = pa.schema([
    pa.field("session_key",   pa.int32()),
    pa.field("driver_number", pa.uint8()),
    pa.field("date",          pa.timestamp("ms", tz="UTC")),
    pa.field("distance_m",    pa.float32()),
])


def _process_lap_telemetry(
    tel_df: pd.DataFrame,
    session_key: int,
    driver_number: int,
) -> pd.DataFrame:
    """Filter and reshape one lap's telemetry to storage columns.

    Returns a DataFrame with columns [session_key, driver_number, date, distance_m].
    Filters out NaN and negative Distance values (FastF1 artefacts at lap boundaries).
    """
    tel = tel_df[["Date", "Distance"]].copy()
    tel = tel.dropna(subset=["Distance"])
    tel = tel[tel["Distance"] >= 0]
    if tel.empty:
        return pd.DataFrame(columns=["session_key", "driver_number", "date", "distance_m"])
    tel["session_key"] = session_key
    tel["driver_number"] = driver_number
    return tel.rename(columns={"Date": "date", "Distance": "distance_m"})[
        ["session_key", "driver_number", "date", "distance_m"]
    ]


def get_session_telemetry(
    session_key: int,
    year: int,
    round_num: int,
    identifier: str,
) -> pa.Table:
    """Load distance telemetry for every driver in one session.

    Args:
        session_key: OpenF1 session_key (stored as-is for mart joins).
        year: F1 season year.
        round_num: Jolpica 1-based round number.
        identifier: FastF1 session code — 'FP1', 'FP2', 'FP3', 'Q', 'SQ', 'S', 'R'.

    Returns:
        PyArrow Table with schema (session_key, driver_number, date, distance_m).
        Empty table (zero rows, correct schema) if no telemetry is available.

    Raises:
        fastf1.core.SessionNotAvailableError: session does not exist in FastF1.
    """
    session = fastf1.get_session(year, round_num, identifier)
    session.load(laps=True, telemetry=True)

    frames: list[pd.DataFrame] = []
    for driver_str in session.drivers:
        driver_number = int(driver_str)
        driver_laps = session.laps.pick_drivers(driver_number)
        for _, lap in driver_laps.iterrows():
            try:
                tel = lap.get_telemetry()
            except Exception:
                continue
            if tel.empty or "Distance" not in tel.columns:
                continue
            processed = _process_lap_telemetry(tel, session_key, driver_number)
            if not processed.empty:
                frames.append(processed)

    if not frames:
        return pa.table({f.name: pa.array([], type=f.type) for f in _EMPTY_SCHEMA})

    combined = pd.concat(frames, ignore_index=True)
    combined["session_key"] = combined["session_key"].astype("int32")
    combined["driver_number"] = combined["driver_number"].astype("uint8")
    combined["distance_m"] = combined["distance_m"].astype("float32")
    return pa.Table.from_pandas(combined, schema=_EMPTY_SCHEMA, preserve_index=False)
```

- [ ] **Step 3: Run tests — verify they pass**

```bash
PYTHONPATH=. pytest tests/ingestion/fastf1/test_client.py -v
```

Expected: `5 passed`

- [ ] **Step 4: Commit**

```bash
git add ingestion/fastf1/__init__.py ingestion/fastf1/client.py
git commit -m "feat(fastf1): add client with _process_lap_telemetry and get_session_telemetry"
```

---

## Task 4: Unit tests for backfill + backfill script

**Files:**
- Create: `tests/ingestion/fastf1/test_backfill.py`
- Create: `ingestion/fastf1/backfill.py`

- [ ] **Step 1: Write the failing tests**

Create `tests/ingestion/fastf1/test_backfill.py`:

```python
import json
import os
import tempfile
from unittest.mock import patch

from ingestion.fastf1.backfill import (
    _load_checkpoint,
    _save_checkpoint,
    _resolve_session_key,
)


def test_checkpoint_roundtrip():
    with tempfile.NamedTemporaryFile(suffix=".json", delete=False, mode="w") as f:
        f.write("[]")
        path = f.name
    try:
        with patch("ingestion.fastf1.backfill._CHECKPOINT_FILE", path):
            completed = {"2024-1-Q", "2024-1-R", "2024-2-FP1"}
            _save_checkpoint(completed)
            loaded = _load_checkpoint()
        assert loaded == completed
    finally:
        os.unlink(path)


def test_load_checkpoint_returns_empty_when_missing():
    with patch("ingestion.fastf1.backfill._CHECKPOINT_FILE", "/tmp/__ff1_no_such_file__.json"):
        result = _load_checkpoint()
    assert result == set()


def test_resolve_session_key_qualifying():
    index = {(5, "Qualifying"): 9501, (5, "Race"): 9502}
    assert _resolve_session_key(index, 5, "Q") == 9501


def test_resolve_session_key_practice_numbered():
    index = {(3, "Practice 1"): 9301, (3, "Practice 2"): 9302}
    assert _resolve_session_key(index, 3, "FP1") == 9301
    assert _resolve_session_key(index, 3, "FP2") == 9302


def test_resolve_session_key_practice_unnumbered_fallback():
    # Older OpenF1 seasons use 'Practice' without a number
    index = {(3, "Practice"): 9301}
    assert _resolve_session_key(index, 3, "FP1") == 9301


def test_resolve_session_key_sprint_qualifying_variants():
    index = {(7, "Sprint Shootout"): 9701}
    assert _resolve_session_key(index, 7, "SQ") == 9701


def test_resolve_session_key_returns_none_for_missing():
    index = {(5, "Race"): 9501}
    assert _resolve_session_key(index, 5, "FP3") is None
```

- [ ] **Step 2: Run tests — verify they fail**

```bash
PYTHONPATH=. pytest tests/ingestion/fastf1/test_backfill.py -v
```

Expected: `ImportError: cannot import name '_load_checkpoint' from 'ingestion.fastf1.backfill'`

- [ ] **Step 3: Implement backfill.py**

Create `ingestion/fastf1/backfill.py`:

```python
"""FastF1 distance backfill — all session types, checkpointed per (year, round, identifier).

Usage:
    python -m ingestion.fastf1.backfill --start 2023 --end 2025

Re-running is safe: completed (year, round, identifier) triples are checkpointed in
.backfill_checkpoint.json and skipped on subsequent runs.
"""

import argparse
import json
import os

import fastf1
import structlog

from ingestion.fastf1 import client
from ingestion.openf1 import endpoints as openf1_endpoints
from ingestion.shared import clickhouse as ch

log = structlog.get_logger()

_CHECKPOINT_FILE = os.path.join(os.path.dirname(__file__), ".backfill_checkpoint.json")

IDENTIFIERS = ["FP1", "FP2", "FP3", "Q", "SQ", "S", "R"]

# Maps each FastF1 session identifier to the candidate OpenF1 session_type strings.
# First match in the list wins when looking up the session_key.
IDENTIFIER_TO_SESSION_TYPES: dict[str, list[str]] = {
    "FP1": ["Practice 1", "Practice"],
    "FP2": ["Practice 2", "Practice"],
    "FP3": ["Practice 3", "Practice"],
    "Q":   ["Qualifying"],
    "SQ":  ["Sprint Qualifying", "Sprint Shootout"],
    "S":   ["Sprint"],
    "R":   ["Race"],
}


def _load_checkpoint() -> set[str]:
    if os.path.exists(_CHECKPOINT_FILE):
        with open(_CHECKPOINT_FILE) as f:
            return set(json.load(f))
    return set()


def _save_checkpoint(completed: set[str]) -> None:
    with open(_CHECKPOINT_FILE, "w") as f:
        json.dump(sorted(completed), f)


def _resolve_session_key(
    index: dict[tuple[int, str], int],
    round_num: int,
    identifier: str,
) -> int | None:
    """Return the OpenF1 session_key for (round, FastF1 identifier), or None if absent."""
    for session_type in IDENTIFIER_TO_SESSION_TYPES.get(identifier, []):
        key = index.get((round_num, session_type))
        if key is not None:
            return key
    return None


def _build_session_index(year: int) -> tuple[dict[tuple[int, str], int], list[int]]:
    """Fetch OpenF1 sessions for a year.

    Returns:
        index: {(round_num, openf1_session_type): session_key}
        rounds: sorted list of 1-based round numbers
    """
    all_sessions = openf1_endpoints.get_sessions(year)

    # Deduplicate: keep the latest Race per meeting (sprint weekends have two)
    race_by_meeting: dict[int, object] = {}
    for s in all_sessions:
        if s.session_type != "Race":
            continue
        prev = race_by_meeting.get(s.meeting_key)
        if prev is None or s.date_start > prev.date_start:
            race_by_meeting[s.meeting_key] = s

    main_races = sorted(race_by_meeting.values(), key=lambda s: s.date_start)
    meeting_to_round = {s.meeting_key: i + 1 for i, s in enumerate(main_races)}

    index: dict[tuple[int, str], int] = {}
    for s in all_sessions:
        rnd = meeting_to_round.get(s.meeting_key)
        if rnd is not None:
            index[(rnd, s.session_type)] = s.session_key

    rounds = list(range(1, len(main_races) + 1))
    return index, rounds


def ingest_year(year: int, completed: set[str]) -> None:
    ylog = log.bind(year=year)
    ylog.info("building session index from OpenF1")
    index, rounds = _build_session_index(year)
    ylog.info("index built", rounds=len(rounds))

    for round_num in rounds:
        for identifier in IDENTIFIERS:
            ck = f"{year}-{round_num}-{identifier}"
            if ck in completed:
                continue

            session_key = _resolve_session_key(index, round_num, identifier)
            if session_key is None:
                # Session type doesn't exist for this weekend — mark done so we skip next time
                completed.add(ck)
                _save_checkpoint(completed)
                continue

            slog = ylog.bind(round=round_num, identifier=identifier, session_key=session_key)
            try:
                arrow_table = client.get_session_telemetry(session_key, year, round_num, identifier)
            except fastf1.core.SessionNotAvailableError:
                slog.info("not available in FastF1 — skipping")
                completed.add(ck)
                _save_checkpoint(completed)
                continue
            except Exception as e:
                slog.warning("failed — will retry next run", error=str(e))
                continue  # intentionally not checkpointed; retry on next run

            if arrow_table.num_rows > 0:
                ch.insert_arrow("raw_fastf1.car_telemetry", arrow_table)
                slog.info("inserted", rows=arrow_table.num_rows)
            else:
                slog.warning("empty telemetry from FastF1")

            completed.add(ck)
            _save_checkpoint(completed)


def main() -> None:
    parser = argparse.ArgumentParser(description="FastF1 distance backfill (2023-present)")
    parser.add_argument("--start", type=int, required=True)
    parser.add_argument("--end",   type=int, required=True)
    parser.add_argument("--reset", action="store_true",
                        help="Ignore checkpoint and re-ingest everything")
    args = parser.parse_args()

    completed: set[str] = set()
    if not args.reset:
        completed = _load_checkpoint()
    elif os.path.exists(_CHECKPOINT_FILE):
        os.remove(_CHECKPOINT_FILE)

    log.info("starting backfill", start=args.start, end=args.end, already_done=len(completed))

    for year in range(args.start, args.end + 1):
        ingest_year(year, completed)

    log.info("backfill complete", start=args.start, end=args.end)


if __name__ == "__main__":
    main()
```

- [ ] **Step 4: Run tests — verify they pass**

```bash
PYTHONPATH=. pytest tests/ingestion/fastf1/ -v
```

Expected: `12 passed` (5 client + 7 backfill)

- [ ] **Step 5: Commit**

```bash
git add tests/ingestion/fastf1/test_backfill.py ingestion/fastf1/backfill.py
git commit -m "feat(fastf1): add backfill script with checkpoint and session resolution"
```

---

## Task 5: dbt staging model and source

**Files:**
- Modify: `dbt/models/staging/sources.yml`
- Create: `dbt/models/staging/stg_fastf1__distances.sql`

- [ ] **Step 1: Add raw_fastf1 to sources.yml**

In `dbt/models/staging/sources.yml`, add after the `dim` source block:

```yaml
  - name: raw_fastf1
    database: raw_fastf1
    schema: raw_fastf1
    tables:
      - name: car_telemetry
```

- [ ] **Step 2: Create the staging model**

Create `dbt/models/staging/stg_fastf1__distances.sql`:

```sql
{{
    config(
        materialized='incremental',
        incremental_strategy='append',
        engine='MergeTree()',
        order_by='(session_key, driver_number, date)',
    )
}}

SELECT
    session_key,
    driver_number,
    toDateTime64(date, 3, 'UTC') AS date,
    distance_m,
    _ingested_at
FROM {{ source('raw_fastf1', 'car_telemetry') }}

{% if is_incremental() %}
WHERE _ingested_at > (SELECT max(_ingested_at) FROM {{ this }})
{% endif %}
```

- [ ] **Step 3: Compile to verify no syntax errors**

```bash
cd dbt && dbt compile --select stg_fastf1__distances
```

Expected: `Completed successfully` with a compiled SQL file in `target/compiled/`.

- [ ] **Step 4: Run the staging model (will be empty until backfill runs)**

```bash
cd dbt && dbt run --select stg_fastf1__distances
```

Expected: `1 of 1 OK` — creates the table with zero rows.

Verify:
```bash
curl -s "http://localhost:8123/?query=SELECT+count()+FROM+f1_mart.stg_fastf1__distances"
```
Expected: `0`

- [ ] **Step 5: Commit**

```bash
git add dbt/models/staging/sources.yml dbt/models/staging/stg_fastf1__distances.sql
git commit -m "feat(dbt): add stg_fastf1__distances staging model"
```

---

## Task 6: Update mart_lap_telemetry

**Files:**
- Modify: `dbt/models/marts/telemetry/mart_lap_telemetry.sql`

- [ ] **Step 1: Add the ASOF JOIN and distance_m column**

In `dbt/models/marts/telemetry/mart_lap_telemetry.sql`, add after the existing location ASOF JOIN (line ~56) and before the regular LEFT JOINs:

```sql
ASOF LEFT JOIN {{ ref('stg_fastf1__distances') }} f1d
    ON  f1d.session_key   = cd.session_key
    AND f1d.driver_number = cd.driver_number
    AND f1d.date         <= cd.date
```

Add `f1d.distance_m AS distance_m` to the SELECT list, placed after `loc.z`:

```sql
    loc.x,
    loc.y,
    loc.z,
    f1d.distance_m                          AS distance_m,
```

The full updated file should look like this (showing changed sections only — do not lose any existing content):

```sql
-- [config block stays the same]

SELECT
    cd.session_key                  AS session_key,
    cd.driver_number                AS driver_number,
    lb.lap_number,
    lb.is_pit_out_lap,
    cd.date                         AS date,
    cd.speed,
    cd.rpm,
    cd.n_gear,
    cd.throttle,
    cd.brake,
    cd.drs,
    loc.x,
    loc.y,
    loc.z,
    f1d.distance_m                  AS distance_m,
    sm.season                       AS season,
    sm.round                        AS round,
    sm.session_type                 AS session_type,
    jmap.jolpica_driver_id          AS driver_id,
    d.name_acronym                  AS driver_code,
    d.team_name                     AS team_name,
    if(d.team_colour IS NOT NULL AND d.team_colour != '', concat('#', d.team_colour), NULL) AS team_colour,
    cd._ingested_at                 AS _ingested_at,
    toUInt8(
        loc.x != lagInFrame(loc.x, 1) OVER (
            PARTITION BY cd.session_key, cd.driver_number ORDER BY cd.date
        )
        OR loc.y != lagInFrame(loc.y, 1) OVER (
            PARTITION BY cd.session_key, cd.driver_number ORDER BY cd.date
        )
    )                               AS gps_updated
FROM {{ ref('stg_openf1__car_data') }}      cd
ASOF LEFT JOIN {{ ref('int_lap_boundaries') }}  lb
    ON  lb.session_key   = cd.session_key
    AND lb.driver_number = cd.driver_number
    AND lb.lap_start    <= cd.date
ASOF LEFT JOIN {{ ref('stg_openf1__location') }} loc
    ON  loc.session_key   = cd.session_key
    AND loc.driver_number = cd.driver_number
    AND loc.date         <= cd.date
ASOF LEFT JOIN {{ ref('stg_fastf1__distances') }} f1d
    ON  f1d.session_key   = cd.session_key
    AND f1d.driver_number = cd.driver_number
    AND f1d.date         <= cd.date
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
  AND lb.lap_number IS NOT NULL
  AND (lb.lap_end IS NULL OR cd.date < lb.lap_end)
{% else %}
WHERE lb.lap_number IS NOT NULL
  AND (lb.lap_end IS NULL OR cd.date < lb.lap_end)
{% endif %}
```

- [ ] **Step 2: Compile to verify**

```bash
cd dbt && dbt compile --select mart_lap_telemetry
```

Expected: `Completed successfully`

- [ ] **Step 3: Commit**

```bash
git add dbt/models/marts/telemetry/mart_lap_telemetry.sql
git commit -m "feat(dbt): add distance_m via FastF1 ASOF JOIN in mart_lap_telemetry"
```

---

## Task 7: Run backfill and rebuild mart

- [ ] **Step 1: Run the FastF1 backfill for 2024 first (smoke test)**

```bash
PYTHONPATH=. python -m ingestion.fastf1.backfill --start 2024 --end 2024
```

Expected: structlog output showing sessions being ingested. Each session logs `"inserted" rows=<N>`. Sprint weekends show `"not available in FastF1 — skipping"` for non-existent session types (e.g. FP3 for a sprint round).

FastF1 downloads data from F1's CDN to `/tmp/fastf1_cache/` on first run — this can take several minutes per session for Race sessions (large telemetry). Qualifying sessions are ~1–2 minutes each.

- [ ] **Step 2: Verify rows in raw_fastf1**

```bash
curl -s "http://localhost:8123/?query=SELECT+count()+FROM+raw_fastf1.car_telemetry"
```

Expected: several hundred thousand rows (2024 season has ~24 rounds × ~7 sessions × ~20 drivers × hundreds of laps).

- [ ] **Step 3: Refresh the staging model**

```bash
cd dbt && dbt run --select stg_fastf1__distances
```

Expected: `1 of 1 OK`

Check row count:
```bash
curl -s "http://localhost:8123/?query=SELECT+count()+FROM+f1_mart.stg_fastf1__distances"
```
Expected: matches `raw_fastf1.car_telemetry` count.

- [ ] **Step 4: Full-refresh mart_lap_telemetry**

```bash
cd dbt && dbt run --full-refresh --select mart_lap_telemetry
```

Expected: `1 of 1 OK`

- [ ] **Step 5: Verify distance_m is populated**

```bash
curl -s "http://localhost:8123/?query=SELECT+session_key,driver_number,round(distance_m,1)+FROM+f1_mart.mart_lap_telemetry+WHERE+distance_m+IS+NOT+NULL+LIMIT+5"
```

Expected: five rows with non-null `distance_m` values (typical range 0–5000 m per lap).

Check NULL rate for 2024 qualifying sessions:
```bash
curl -s "http://localhost:8123/?query=SELECT+countIf(distance_m+IS+NULL)+as+null_count,+countIf(distance_m+IS+NOT+NULL)+as+populated,+season+FROM+f1_mart.mart_lap_telemetry+WHERE+season%3D2024+AND+session_type%3D'Qualifying'+GROUP+BY+season"
```

Expected: `null_count` much lower than `populated`. Some NULLs are expected for sessions where FastF1 telemetry download failed.

- [ ] **Step 6: Run backfill for 2023 and 2025**

```bash
PYTHONPATH=. python -m ingestion.fastf1.backfill --start 2023 --end 2025
```

Then refresh dbt:

```bash
cd dbt && dbt run --select stg_fastf1__distances && dbt run --full-refresh --select mart_lap_telemetry
```

- [ ] **Step 7: Final commit**

```bash
git add ingestion/fastf1/.backfill_checkpoint.json
git commit -m "chore(fastf1): add backfill checkpoint after initial 2023-2025 run"
```

---

## Spec Coverage Check

| Spec requirement | Covered by |
|-----------------|------------|
| New `raw_fastf1` database + table | Task 1 (migration 006) |
| `distance_m` column in mart | Task 1 (migration 007) + Task 6 |
| FastF1 cache configured via env | Task 3 (`FASTF1_CACHE_DIR`) |
| All session types ingested | Task 4 (`IDENTIFIERS` list) |
| Non-existent sessions skipped gracefully | Task 4 (`SessionNotAvailableError` catch) |
| Checkpoint per (year, round, identifier) | Task 4 |
| `_process_lap_telemetry` filters negatives + NaN | Task 3 + verified in Task 2 tests |
| `stg_fastf1__distances` incremental model | Task 5 |
| ASOF JOIN in `mart_lap_telemetry` | Task 6 |
| 2023–2025 backfill | Task 7 |
| `IDENTIFIER_TO_SESSION_TYPES` handles FP unnumbered fallback | Task 4 (backfill + test) |

# F1 Warehouse Access Guide
## Building a Head-to-Head Qualifying Simulation

This document is for a developer building a program that simulates two drivers' best qualifying laps side by side. It covers every query you need, what the columns mean, and how to interpret the data.

---

## Connection

ClickHouse is available via HTTP at `http://localhost:8123`.

```bash
# CLI
docker exec f1-clickhouse-1 clickhouse-client --query "SELECT 1"

# HTTP (from host)
curl "http://localhost:8123/?query=SELECT+1"
```

**Only query mart tables.** Never touch `raw_*` or `f1_staging.*` directly from your app.

| Database | Purpose |
|----------|---------|
| `f1_mart` | The only database your app should query |
| `f1_staging`, `f1_intermediate` | Internal pipeline — do not query |
| `raw_jolpica`, `raw_openf1` | Raw ingestion — do not query |

---

## Data Coverage

| Era | What's available |
|-----|-----------------|
| 1950–2022 | Qualifying positions + Q1/Q2/Q3 lap times (strings). No telemetry. |
| 2023–present | Everything above + sector times, speed traps, mini-sectors, and full per-sample telemetry + GPS. |

For a simulation with real telemetry data, you need **2023 or later**.

---

## Step-by-Step: Building a Lap Comparison

### Step 1 — Pick a qualifying session

List all qualifying sessions with available OpenF1 telemetry:

```sql
SELECT session_key, season, round, race_name, circuit_name,
       circuit_country, qualifying_date
FROM f1_mart.dim_sessions
WHERE session_type = 'Qualifying'
  AND season >= 2023
ORDER BY season, round
```

`session_key` is the identifier you'll use for all downstream queries.

---

### Step 2 — List drivers in the session

```sql
SELECT
    qualifying_position,
    driver_id,
    driver_name,
    name_acronym,
    team_name,
    team_colour,          -- hex like '#3671C6', NULL if no OpenF1 data
    headshot_url,
    best_time,            -- Jolpica string e.g. '1:15.915'
    best_lap_duration,    -- OpenF1 float in seconds e.g. 75.915
    best_lap_number,      -- which lap number was the best
    best_s1, best_s2, best_s3,  -- sector times in seconds
    i1_speed, i2_speed, st_speed,  -- speed traps in km/h
    openf1_driver_number  -- you need this for the telemetry query
FROM f1_mart.mart_qualifying_summary
WHERE season = 2024 AND round = 3   -- or use session_key
ORDER BY qualifying_position
```

This returns one row per driver. It has everything you need to build a driver picker UI.

---

### Step 3 — Get the best qualifying lap details

You already have `best_lap_number` and `openf1_driver_number` from Step 2. Use them to fetch the telemetry.

```sql
SELECT
    date,           -- DateTime64(3) — millisecond precision timestamp
    speed,          -- km/h (integer 0–350)
    throttle,       -- percentage 0–100
    brake,          -- 0 or 100 (binary in F1 car data)
    n_gear,         -- 0–8 (0 = neutral)
    drs,            -- see DRS values table below
    rpm,            -- engine RPM
    x, y, z,        -- GPS coordinates in meters (see GPS section below)
    lap_number,
    is_pit_out_lap  -- 0 or 1; always 0 for best qualifying lap
FROM f1_mart.mart_lap_telemetry
WHERE session_key    = 9484   -- from Step 1
  AND driver_number  = 1      -- openf1_driver_number from Step 2
  AND lap_number     = 20     -- best_lap_number from Step 2
  AND is_pit_out_lap = 0
ORDER BY date
```

This is the full lap trace — roughly 300–500 rows depending on lap length.

---

## Understanding the Columns

### Lap timing

| Column | Type | Description |
|--------|------|-------------|
| `best_time` | string | Jolpica format `'1:15.915'` — official timing screen value |
| `best_lap_duration` | float | Same time in seconds (`75.915`). Use this for delta calculations. NULL when `best_source_match != 'matched'`. |
| `best_s1` / `best_s2` / `best_s3` | float | Sector times in seconds. S1+S2+S3 ≈ `best_lap_duration` (small gap from Jolpica rounding). |
| `i1_speed` / `i2_speed` / `st_speed` | int | Speed trap readings in km/h at intermediate 1, intermediate 2, and the speed trap (usually the main straight). |
| `jolpica_best_time_ms` | int | Jolpica official best time in milliseconds. |
| `openf1_best_time_ms` | int | OpenF1's own fastest lap in milliseconds (may differ from Jolpica by a few ms). |
| `best_source_match` | string | `'matched'` — OpenF1 lap aligned to Jolpica time within ±50 ms. `'jolpica_only'` — no OpenF1 session exists (pre-2023). `'mismatch'` — OpenF1 data exists but no lap landed within ±50 ms of the official time. When not `'matched'`, `best_lap_duration` and all OpenF1 lap detail columns are NULL. |

**Parsing the Jolpica time string:**
```python
def parse_lap_time(t: str) -> float:
    """'1:15.915' → 75.915 seconds"""
    m, s = t.split(':')
    return int(m) * 60 + float(s)
```

---

### Telemetry channels

| Column | Range | Notes |
|--------|-------|-------|
| `speed` | 0–350 km/h | Instantaneous speed |
| `throttle` | 0–100 | Pedal position percentage |
| `brake` | 0 or 100 | Binary in F1 data — either fully pressed or not. 100 = braking. |
| `n_gear` | 0–8 | 0 = neutral/pit lane, 1–8 = gears. Modern F1 cars have 8 gears. |
| `rpm` | ~5000–13000 | Engine RPM |
| `drs` | see below | DRS state |

**DRS values:**

| Value | Meaning |
|-------|---------|
| `0` | Closed — DRS not available or not used |
| `8` | Open — DRS fully open, driver has activated it |
| `10` | Detection zone — car passed the DRS detection point |
| `12` | Activation zone — car is in the activation zone |
| `14` | Open and eligible |

For a simulation, use `drs >= 8` to show DRS open state visually.

---

### GPS coordinates

| Column | Unit | Notes |
|--------|------|-------|
| `x` | meters | Horizontal position. Right is positive. Origin is track-specific. |
| `y` | meters | Vertical position. Up is positive. Origin is track-specific. |
| `z` | meters | Altitude. |

The coordinate system is **track-relative** — the origin is somewhere on the circuit, not a real-world GPS origin. Coordinates are consistent within a session (you can overlay two drivers' traces on the same plot), but they differ between circuits and sometimes between sessions at the same circuit.

For a head-to-head overlay, plot both drivers' `(x, y)` traces on the same axes. The tracks will align automatically since they share the same coordinate origin within a session.

**Zero values:** Some samples have `x=0, y=0` (GPS dropout or pit lane). Filter these if drawing the track map:
```sql
WHERE x != 0 AND y != 0
```

**`gps_updated` flag:** GPS refreshes at ~2–3 Hz while telemetry is sampled at ~3.7 Hz, so consecutive telemetry rows often share the same coordinates. `gps_updated = 1` marks the rows where the GPS position actually changed. Use this to deduplicate when drawing a track map or computing distance:
```sql
WHERE gps_updated = 1 AND x != 0 AND y != 0
```

---

### Segment arrays (mini-sectors)

`segments_s1`, `segments_s2`, `segments_s3` in `mart_qualifying_summary` are arrays of integers representing mini-sector color codes — what you see on the official F1 timing tower.

| Value | Colour | Meaning |
|-------|--------|---------|
| `2048` | Yellow | Driver's personal best for this mini-sector |
| `2049` | Green | Fastest of all drivers in this mini-sector |
| `2051` | Purple | Fastest overall (sector purple) |
| `2064` | White | Pit lane |
| `0` | Grey | No data |

These are Array(UInt16) columns. Access them by index in your application code. Useful for drawing a mini-sector strip under each driver's lap time card.

---

### Sample rate

Telemetry is sampled at approximately **3.7 Hz** (~270ms between samples). A typical qualifying lap of ~80 seconds produces ~300 rows. A lap of ~100 seconds produces ~370 rows.

For animation or simulation, interpolate between samples for smooth playback. Linear interpolation on `speed`, `x`, `y` is sufficient.

---

## Complete Example: Two-Driver Lap Comparison

```sql
-- Get everything needed to simulate two drivers head-to-head
-- Replace the driver_number values with openf1_driver_number from mart_qualifying_summary

-- Driver A: Verstappen (driver_number=1, best_lap_number=20 at Melbourne 2024)
SELECT
    'VER' AS driver,
    dateDiff('millisecond', min(date) OVER (), date) AS ms_from_lap_start,
    speed, throttle, brake, n_gear, drs, rpm, x, y, z
FROM f1_mart.mart_lap_telemetry
WHERE session_key = 9484
  AND driver_number = 1
  AND lap_number = 20
  AND is_pit_out_lap = 0
ORDER BY date

UNION ALL

-- Driver B: Sainz (driver_number=55, best_lap_number=17)
SELECT
    'SAI' AS driver,
    dateDiff('millisecond', min(date) OVER (), date) AS ms_from_lap_start,
    speed, throttle, brake, n_gear, drs, rpm, x, y, z
FROM f1_mart.mart_lap_telemetry
WHERE session_key = 9484
  AND driver_number = 55
  AND lap_number = 17
  AND is_pit_out_lap = 0
ORDER BY date
```

The `ms_from_lap_start` column gives each sample's offset from the start of that driver's lap. Use this as the time axis for a synchronized comparison — both drivers start at `ms=0` regardless of when their laps actually happened in the session.

---

## Where a Lap Starts

A qualifying lap starts at the moment the driver crosses the start/finish line, which is when `lap_number` increments. The first telemetry sample for that lap (lowest `date` value) is effectively the start line crossing.

```sql
-- Find the exact start timestamp of a driver's best lap
SELECT min(date) AS lap_start_time
FROM f1_mart.mart_lap_telemetry
WHERE session_key = 9484
  AND driver_number = 1
  AND lap_number = 20
```

The car will be at high speed when this starts (it's a flying lap, not a standing start). Typical entry speed at the start/finish is 280–320 km/h depending on the circuit.

---

## Handling Missing Data

**Pre-2023 sessions:** `best_lap_duration`, sector times, speed traps, segments, and all telemetry are NULL. Only `best_time` (string), `qualifying_position`, Q1/Q2/Q3 strings, driver bio, and constructor are available.

**2024 sessions with incomplete backfill:** Some 2024 sessions have no OpenF1 lap data yet. Check before querying:
```sql
SELECT best_lap_duration IS NOT NULL AS has_telemetry
FROM f1_mart.mart_qualifying_summary
WHERE season = ? AND round = ? AND driver_id = ?
```

**GPS dropout samples:** A small percentage of samples have `x=0, y=0`. Safe to filter out for track rendering.

---

## Quick Reference

| I want to... | Query table | Key filter |
|---|---|---|
| List race weekends | `dim_sessions` | `session_type = 'Qualifying'` |
| Get drivers + lap times for a qualifying | `mart_qualifying_summary` | `season, round` |
| Get full telemetry for one lap | `mart_lap_telemetry` | `session_key, driver_number, lap_number` |
| Compare two drivers at sector level | `mart_qualifying_summary` | `season, round` — compare `best_s1/2/3` |
| Draw track map | `mart_lap_telemetry` | `session_key, driver_number, lap_number` — filter `gps_updated = 1 AND x != 0 AND y != 0` |
| Show braking zones | `mart_lap_telemetry` | `WHERE brake = 100` |
| Show DRS zones | `mart_lap_telemetry` | `WHERE drs >= 8` |
| Get circuit physical stats | `dim_circuits` | `circuit_id` — `length_km`, `corners` available (NULL for circuits not yet re-ingested) |

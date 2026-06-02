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
| 1950–2022 | Qualifying positions + Q1/Q2/Q3 lap times (strings + ms). No telemetry. |
| 2023–present | Everything above + sector times, speed traps, mini-sectors, and full per-sample telemetry + GPS. |

Coverage as of the last pipeline run: **2024 and 2025 are fully ingested** (24 and 24 rounds respectively, including all sprint weekends). Telemetry exists for 114 sessions.

For a simulation with real telemetry data, you need **2023 or later**.

---

## Mart Tables

### Dimensions

| Table | Grain | Description |
|-------|-------|-------------|
| `dim_races` | one row per round | Every F1 race 1950–present: race name, circuit, date, `race_sk` surrogate key, sprint flag. The primary race lookup — use `race_sk` to join to any fact. |
| `dim_sessions` | one row per session | All OpenF1 sessions (Race, Qualifying, Practice, Sprint, Sprint Qualifying) 2023+. Links `session_key` to `season`, `round`, circuit info. |
| `dim_circuits` | one row per circuit | Circuit metadata: name, country, `length_km`, `corners` |
| `dim_drivers` | one row per driver per season | Driver bio + the constructor they drove for that season. Filter `is_current = 1` for the current team. Use `driver_id` to join to facts. |
| `dim_constructors` | one row per constructor per season | Constructor name and nationality, season-scoped. Filter `is_current = 1` for currently active constructors. |
| `dim_session_drivers` | one row per driver per session | Display attributes for a specific session: `driver_code`, `full_name`, `team_name`, `team_colour`, `headshot_url`. Join via `session_key + driver_number`. |

### Facts

| Table | Grain | Description |
|-------|-------|-------------|
| `fact_qualifying` | one row per driver per round | Official qualifying positions, Q1/Q2/Q3 times as **both strings and milliseconds** (`q1_ms`, `q2_ms`, `q3_ms`), OpenF1 best lap details. Has `race_sk`. |
| `fact_race_results` | one row per driver per round | Grid, finish position, points, laps completed, status, fastest lap. Has `race_sk` and `driver_id`. |
| `fact_laps` | one row per driver per lap per race | Per-lap race times with sector times and speed traps from OpenF1 (2023+). Has `race_sk`. |
| `fact_sprint_results` | one row per driver per sprint round | Sprint race results. Has `race_sk`. |
| `fact_pit_stops` | one row per pit stop | Every pit stop with stop number, lap, duration, and OpenF1 session link. Has `race_sk`. |

### Analytics Marts

| Table | Grain | Description |
|-------|-------|-------------|
| `mart_qualifying_summary` | one row per driver per round | Denormalized: driver bio + qualifying position + Q1/Q2/Q3 + best lap telemetry — the main table for a lap comparison app |
| `mart_lap_telemetry` | one row per telemetry sample | Full 3.7 Hz car data + GPS per lap (Race and Qualifying sessions, 2023+). Join `dim_session_drivers` for driver display attributes. |
| `mart_lap_analysis` | one row per driver per lap per race | Race lap times with delta to fastest and sector breakdowns (2000+) |
| `mart_standings` | one row per driver per round | Driver and constructor championship standings after each round (2000+) |
| `mart_pit_stops` | one row per pit stop | Pit stop timing with race and driver context. Use for strategy analysis. |
| `mart_stints` | one row per stint | Tyre stints with compound, lap range, and tyre age. OpenF1 2023+ only. |

> **`mart_strategy` was removed.** It previously mixed pit stops (one per stop) and stints (one per stint) onto race results, creating a cross product. Use `mart_pit_stops` for pit stop analysis and `mart_stints` for tyre stint analysis.

---

## Step-by-Step: Building a Lap Comparison

### Step 1 — Pick a qualifying session

List all qualifying sessions with available OpenF1 telemetry:

```sql
SELECT session_key, season, round, race_name, circuit_name,
       circuit_country, date_start
FROM f1_mart.dim_sessions
WHERE session_type = 'Qualifying'
  AND session_name = 'Qualifying'   -- excludes Sprint Qualifying on sprint weekends
  AND season >= 2023
ORDER BY season, round
```

`session_key` is the identifier you'll use for all downstream queries.

Alternatively, use `dim_races` if you want to browse by circuit or date first:

```sql
SELECT race_sk, season, round, race_name, circuit_name,
       circuit_country, race_date, session_key
FROM f1_mart.dim_races
WHERE season >= 2023
ORDER BY season, round
```

`dim_races.session_key` is the Race session key. To get the Qualifying session key, join `dim_sessions`:

```sql
SELECT r.race_sk, r.season, r.round, r.race_name, s.session_key
FROM f1_mart.dim_races r
JOIN f1_mart.dim_sessions s
    ON s.season = r.season
   AND s.round  = r.round
   AND s.session_type = 'Qualifying'
   AND s.session_name = 'Qualifying'
WHERE r.season >= 2023
ORDER BY r.season, r.round
```

> **Sprint weekends:** These have two qualifying-type sessions — `session_name = 'Sprint Qualifying'` (the shootout) and `session_name = 'Qualifying'` (the main grid qualifier). Always filter `session_name = 'Qualifying'` unless you specifically want sprint shootout data.

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
    best_source_match,    -- 'matched', 'jolpica_only', or 'mismatch'
    openf1_driver_number  -- you need this for the telemetry query
FROM f1_mart.mart_qualifying_summary
WHERE season = 2025 AND round = 1   -- or use session_key
ORDER BY qualifying_position
```

This returns one row per driver. It has everything you need to build a driver picker UI.

Before loading telemetry, check `best_source_match = 'matched'` — if it's `'mismatch'` or `'jolpica_only'`, `best_lap_number` will be NULL and there is no telemetry to show for that driver.

---

### Step 3 — Get the best qualifying lap telemetry

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

> **Note:** `mart_lap_telemetry` no longer includes `driver_code`, `team_name`, or `team_colour`. Those are available in `dim_session_drivers` (join on `session_key + driver_number`) or directly from `mart_qualifying_summary` which already denormalizes them.

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

**Pre-parsed millisecond columns** are available directly in `fact_qualifying` (and flow through to `mart_qualifying_summary`):

| Column | Description |
|--------|-------------|
| `q1_ms` | Q1 best time in milliseconds. NULL if driver didn't set a Q1 time. |
| `q2_ms` | Q2 best time in milliseconds. NULL for Q1-eliminated drivers. |
| `q3_ms` | Q3 best time in milliseconds. NULL for all but the top 10. |

These replace the need to parse `'1:15.915'` strings yourself. For the string representation (e.g. for display), use `q1`, `q2`, `q3`.

**Parsing the Jolpica time string** (if you need it):
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

## Joining Across Tables

### race_sk — single-column race identifier

Every fact table has a `race_sk` column (`UInt32 = season * 100 + round`). This lets you join to `dim_races` for race and circuit context without carrying a `(season, round)` composite:

```sql
-- Get circuit name and country alongside every race result
SELECT
    r.race_name,
    r.circuit_name,
    r.circuit_country,
    f.driver_id,
    f.finish_position,
    f.points
FROM f1_mart.fact_race_results f
JOIN f1_mart.dim_races r ON r.race_sk = f.race_sk
WHERE r.season = 2025
ORDER BY r.round, f.finish_position
```

### dim_drivers — current team vs full history

`dim_drivers` has one row per driver per season (SCD2). For current team information, filter `is_current = 1`:

```sql
-- Current grid with team
SELECT driver_id, full_name, constructor_id, nationality
FROM f1_mart.dim_drivers
WHERE is_current = 1
ORDER BY family_name
```

For historical context — which team a driver was with during a specific season:

```sql
-- Hamilton's constructor each season
SELECT season, constructor_id, valid_from, valid_to
FROM f1_mart.dim_drivers
WHERE driver_id = 'hamilton'
ORDER BY season
```

### dim_session_drivers — display attributes for telemetry

`mart_lap_telemetry` no longer carries `driver_code`, `team_name`, or `team_colour`. Fetch those separately when building a UI:

```sql
SELECT
    t.session_key,
    t.driver_number,
    sd.driver_code,
    sd.full_name,
    sd.team_name,
    sd.team_colour
FROM f1_mart.mart_lap_telemetry t
JOIN f1_mart.dim_session_drivers sd
    ON sd.session_key   = t.session_key
    AND sd.driver_number = t.driver_number
WHERE t.session_key = 9484
  AND t.lap_number  = 20
ORDER BY t.date
LIMIT 5
```

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

**Pre-2023 sessions:** `best_lap_duration`, sector times, speed traps, segments, and all telemetry are NULL. Only `best_time` (string), `q1_ms`/`q2_ms`/`q3_ms`, `qualifying_position`, driver bio, and constructor are available.

**Known rounds with no OpenF1 lap data (all drivers mismatch):**

| Season | Round | Race | Notes |
|--------|-------|------|-------|
| 2024 | 22 | Las Vegas Grand Prix | OpenF1 lap data exists but no lap matches Jolpica times within ±50 ms |
| 2025 | 17 | Azerbaijan Grand Prix | OpenF1 returned 0 usable qualifying laps for this session |

For these rounds `best_lap_duration`, `best_lap_number`, and all telemetry columns in `mart_qualifying_summary` are NULL for every driver. The Q1/Q2/Q3 strings and ms columns from Jolpica are still available.

**Isolated driver mismatches (1 driver per round):** Most 2023–2025 rounds have exactly 1 driver with `best_source_match = 'mismatch'`, typically due to a driver ID mapping gap or an interrupted session. The remaining 19 drivers are fully matched. See `data-gaps.md` for the full list.

**Checking before querying telemetry:**
```sql
SELECT best_source_match, best_lap_number IS NOT NULL AS has_telemetry
FROM f1_mart.mart_qualifying_summary
WHERE season = 2025 AND round = 1 AND driver_id = 'norris'
```

**GPS dropout samples:** A small percentage of samples have `x=0, y=0`. Safe to filter out for track rendering.

---

## Additional Mart Tables

### Race lap analysis (`mart_lap_analysis`)

Per-lap race data with timing context. Available for all seasons with Jolpica data (2000+); sector times and speed traps available from 2023+ where OpenF1 data exists.

```sql
SELECT driver_id, lap_number, lap_time_ms, delta_to_fastest_ms,
       of1_s1, of1_s2, of1_s3, st_speed
FROM f1_mart.mart_lap_analysis
WHERE season = 2025 AND round = 1
ORDER BY driver_id, lap_number
```

| Column | Description |
|--------|-------------|
| `lap_time_ms` | Lap time in milliseconds |
| `race_fastest_ms` | Fastest lap of the race by any driver |
| `delta_to_fastest_ms` | How much slower than the race fastest (always ≥ 0) |
| `driver_avg_lap_ms` | Driver's own average lap time for the race |
| `of1_s1/s2/s3` | OpenF1 sector times in seconds (NULL pre-2023) |
| `i1_speed/i2_speed/st_speed` | Speed traps in km/h (NULL pre-2023) |

---

### Championship standings (`mart_standings`)

Driver and constructor standings after each round. Available 2000+.

```sql
SELECT round, driver_id, driver_name, constructor_name,
       standing_position, points, wins,
       constructor_position, constructor_points
FROM f1_mart.mart_standings
WHERE season = 2025
ORDER BY round, standing_position
```

---

### Pit stops (`mart_pit_stops`)

One row per pit stop per driver per race. Available for all Jolpica-covered seasons; pit duration only available from 2023+ (OpenF1 data).

```sql
SELECT driver_name, stop_number, pit_lap,
       pit_duration_ms, constructor_id,
       race_name, circuit_name
FROM f1_mart.mart_pit_stops
WHERE season = 2025 AND round = 1
ORDER BY driver_id, stop_number
```

| Column | Description |
|--------|-------------|
| `stop_number` | 1 for first stop, 2 for second, etc. |
| `pit_lap` | Lap on which the driver entered the pit lane |
| `pit_duration_ms` | Total pit stop duration in milliseconds (NULL pre-2023) |

---

### Tyre stints (`mart_stints`)

One row per tyre stint per driver per race. **OpenF1 2023+ only** — no pre-2023 stint data.

```sql
SELECT driver_name, stint_number, compound,
       lap_start, lap_end, tyre_age_at_start,
       constructor_id
FROM f1_mart.mart_stints
WHERE season = 2025 AND round = 1
ORDER BY driver_id, stint_number
```

| Column | Description |
|--------|-------------|
| `compound` | Tyre compound: `SOFT`, `MEDIUM`, `HARD`, `INTERMEDIATE`, `WET` |
| `lap_start` / `lap_end` | First and last lap of this stint |
| `tyre_age_at_start` | Laps already on this set at stint start (for pre-used tyres) |

---

## Quick Reference

| I want to... | Query table | Key filter |
|---|---|---|
| Browse race weekends by circuit/date | `dim_races` | `season`, `circuit_name` |
| Get Qualifying session_key for a round | `dim_sessions` | `season, round, session_type='Qualifying', session_name='Qualifying'` |
| Get drivers + lap times for a qualifying | `mart_qualifying_summary` | `season, round` |
| Get full telemetry for one lap | `mart_lap_telemetry` | `session_key, driver_number, lap_number` |
| Get driver display info for a session | `dim_session_drivers` | `session_key, driver_number` |
| Compare two drivers at sector level | `mart_qualifying_summary` | `season, round` — compare `best_s1/2/s3` |
| Draw track map | `mart_lap_telemetry` | `session_key, driver_number, lap_number` — filter `gps_updated=1 AND x!=0 AND y!=0` |
| Show braking zones | `mart_lap_telemetry` | `WHERE brake = 100` |
| Show DRS zones | `mart_lap_telemetry` | `WHERE drs >= 8` |
| Get a driver's current team | `dim_drivers` | `driver_id, is_current=1` |
| Get a driver's team in a specific season | `dim_drivers` | `driver_id, season` |
| Race lap-by-lap times + deltas | `mart_lap_analysis` | `season, round` |
| Championship standings | `mart_standings` | `season, round` |
| Pit stop timing for a race | `mart_pit_stops` | `season, round` |
| Tyre stints for a race | `mart_stints` | `season, round` |
| Get circuit physical stats | `dim_circuits` | `circuit_id` — `length_km`, `corners` available for 25 circuits, NULL for the rest |
| Race results (grid, finish, points) | `fact_race_results` | `season, round` or `race_sk` |
| Full qualifying detail with Q1/Q2/Q3 | `fact_qualifying` | `season, round` — `q1_ms/q2_ms/q3_ms` pre-parsed |
| All pit stops (raw) | `fact_pit_stops` | `season, round` or `race_sk` |

# F1 Qualifying Comparison Visualizer — Design Handoff

This document gives a Claude design agent everything it needs to build a website that compares two drivers' best qualifying efforts for any F1 race weekend.

---

## What We're Building

A web app where a user picks a **season**, a **race**, and **two drivers**, then sees a side-by-side comparison of their best qualifying laps — sector times, speed traps, mini-sector color-coding, gap analysis, and headline stats.

---

## Data Architecture

Data flows through two ingestion sources into ClickHouse, then through dbt into clean mart tables.

```
Jolpica API (1950–present)     OpenF1 API (2023–present)
        ↓                               ↓
  raw_jolpica.*               raw_openf1.*
        ↓                               ↓
         ── dbt transforms ──
                 ↓
    fact_qualifying   fact_laps   dim_drivers   dim_circuits
```

The app should query the **dbt mart tables** (prefixed `marts_` in ClickHouse), not raw tables.

---

## Core Tables and Fields

### `fact_qualifying` — one row per driver per race qualifying session

| Field | Type | Description |
|-------|------|-------------|
| `season` | int | Championship year (e.g. 2024) |
| `round` | int | Round number within the season (1 = first race) |
| `driver_id` | string | Jolpica slug (e.g. `"max_verstappen"`, `"hamilton"`) |
| `constructor_id` | string | Team slug (e.g. `"red_bull"`, `"mercedes"`) |
| `qualifying_position` | int | Final grid position from qualifying (1 = pole) |
| `q1` | string\|null | Best Q1 lap time as `"m:ss.mmm"` (e.g. `"1:23.456"`) |
| `q2` | string\|null | Best Q2 lap time — null if driver was eliminated in Q1 |
| `q3` | string\|null | Best Q3 lap time — null if driver didn't reach Q3 |
| `session_key` | int\|null | OpenF1 session key — **null for pre-2023 races** |
| `openf1_driver_number` | int\|null | OpenF1 driver number — **null for pre-2023** |

**Best qualifying lap logic:** A driver's "best effort" is their best time across all rounds they participated in. In practice: Q3 time if they reached Q3, else Q2, else Q1. You derive this in SQL:
```sql
COALESCE(q3, q2, q1) AS best_qualifying_time
```
Times are strings — parse them to milliseconds for comparison:
```sql
-- "1:23.456" → 83456 ms
toInt32(splitByChar(':', time)[1]) * 60000
+ toFloat32(splitByChar(':', time)[2]) * 1000
```

---

### `dim_drivers` — driver biographical info

| Field | Type | Description |
|-------|------|-------------|
| `driver_id` | string | Primary key, matches `fact_qualifying.driver_id` |
| `given_name` | string | First name (e.g. `"Max"`) |
| `family_name` | string | Last name (e.g. `"Verstappen"`) |
| `full_name` | string | Display name (e.g. `"Max Verstappen"`) |
| `nationality` | string | Country (e.g. `"Dutch"`) |
| `permanent_number` | int\|null | Car number (e.g. `1`) — null for older drivers |
| `driver_code` | string\|null | Three-letter code (e.g. `"VER"`) — null pre-1994 |
| `url` | string | Wikipedia URL — source for driver photo if needed |

---

### `dim_circuits` — track info

| Field | Type | Description |
|-------|------|-------------|
| `circuit_id` | string | Slug (e.g. `"monza"`, `"silverstone"`) |
| `circuit_name` | string | Full name (e.g. `"Autodromo Nazionale di Monza"`) |
| `locality` | string | City |
| `country` | string | Country |
| `lat` / `lng` | float | GPS coordinates |

Join path to get circuit for a qualifying row:
```sql
fact_qualifying → raw_jolpica.races (via season+round) → dim_circuits (via circuit_id)
```

---

### `raw_openf1.laps` — lap-by-lap telemetry summary (2023+ only)

This is the richest source for the modern era. Join via `session_key` + `driver_number`.

| Field | Type | Description |
|-------|------|-------------|
| `session_key` | int | Links to `fact_qualifying.session_key` |
| `driver_number` | int | Links to `fact_qualifying.openf1_driver_number` |
| `lap_number` | int | Lap within the session |
| `lap_duration` | float\|null | Total lap time in **seconds** (e.g. `83.456`) |
| `duration_sector_1` | float\|null | S1 time in seconds |
| `duration_sector_2` | float\|null | S2 time in seconds |
| `duration_sector_3` | float\|null | S3 time in seconds |
| `i1_speed` | int\|null | Speed trap at intermediate 1 in km/h |
| `i2_speed` | int\|null | Speed trap at intermediate 2 in km/h |
| `st_speed` | int\|null | Speed trap at speed trap (finish straight) in km/h |
| `is_pit_out_lap` | bool | True if driver exited pit lane this lap — exclude these |
| `segments_sector_1` | int[] | Mini-sector color codes for S1 (see legend below) |
| `segments_sector_2` | int[] | Mini-sector color codes for S2 |
| `segments_sector_3` | int[] | Mini-sector color codes for S3 |

**Mini-sector segment color codes:**
| Value | Meaning | Display Color |
|-------|---------|---------------|
| `2048` | Yellow (personal best) | `#ffd700` |
| `2049` | Green (fastest overall) | `#00d2be` |
| `2051` | Purple (sector purple) | `#9b59b6` |
| `2064` | Pit lane | `#ffffff` |
| `0` | Unknown / no data | `#555555` |

To find a driver's best qualifying lap in a session, filter out pit-out laps and take the row with minimum `lap_duration`:
```sql
SELECT * FROM raw_openf1.laps
WHERE session_key = ? AND driver_number = ? AND is_pit_out_lap = 0
ORDER BY lap_duration ASC LIMIT 1
```

---

### `raw_openf1.drivers` — driver appearance data (2023+)

| Field | Type | Description |
|-------|------|-------------|
| `session_key` | int | Must match the qualifying session key |
| `driver_number` | int | Car number |
| `name_acronym` | string | Three-letter code shown on TV (e.g. `"VER"`) |
| `team_name` | string | Full team name (e.g. `"Red Bull Racing"`) |
| `team_colour` | string | Hex color without `#` (e.g. `"3671C6"`) — use for driver theming |
| `headshot_url` | string\|null | Direct URL to official F1 headshot image |
| `full_name` | string | Full display name |

**Important:** query with the qualifying `session_key` specifically — driver data is session-scoped.

---

## Data Coverage by Era

| Feature | Pre-2023 (Jolpica only) | 2023+ (Jolpica + OpenF1) |
|---------|------------------------|--------------------------|
| Q1/Q2/Q3 times | ✅ string format | ✅ string format |
| Qualifying position | ✅ | ✅ |
| Sector times (S1/S2/S3) | ❌ | ✅ in seconds |
| Speed traps (i1/i2/st) | ❌ | ✅ in km/h |
| Mini-sector segments | ❌ | ✅ color-coded arrays |
| Driver headshot | ❌ | ✅ via headshot_url |
| Team color | ❌ | ✅ hex string |

The UI should degrade gracefully — for pre-2023 races show only the lap times and positions, and hide the sector/speed/segment panels rather than showing empty states.

---

## Suggested API Queries for the Website Backend

### 1. List available seasons
```sql
SELECT DISTINCT season FROM raw_jolpica.races ORDER BY season DESC
```

### 2. List races for a season
```sql
SELECT round, race_name, circuit_id, date
FROM raw_jolpica.races
WHERE season = ?
ORDER BY round
```

### 3. List drivers who participated in qualifying for a race
```sql
SELECT fq.driver_id, fq.qualifying_position, fq.constructor_id,
       d.full_name, d.driver_code, d.permanent_number,
       COALESCE(fq.q3, fq.q2, fq.q1) AS best_time
FROM fact_qualifying fq
JOIN dim_drivers d ON d.driver_id = fq.driver_id
WHERE fq.season = ? AND fq.round = ?
ORDER BY fq.qualifying_position
```

### 4. Full comparison for two drivers (2023+ race)
```sql
-- For each driver_id, run this to get the full lap detail:
SELECT
    fq.qualifying_position,
    fq.q1, fq.q2, fq.q3,
    COALESCE(fq.q3, fq.q2, fq.q1) AS best_time,
    l.lap_duration,
    l.duration_sector_1, l.duration_sector_2, l.duration_sector_3,
    l.i1_speed, l.i2_speed, l.st_speed,
    l.segments_sector_1, l.segments_sector_2, l.segments_sector_3,
    od.team_colour, od.headshot_url, od.name_acronym, od.team_name
FROM fact_qualifying fq
LEFT JOIN raw_openf1.laps l
    ON  l.session_key   = fq.session_key
    AND l.driver_number = fq.openf1_driver_number
    AND l.is_pit_out_lap = 0
LEFT JOIN raw_openf1.drivers od
    ON  od.session_key   = fq.session_key
    AND od.driver_number = fq.openf1_driver_number
WHERE fq.season = ? AND fq.round = ? AND fq.driver_id = ?
ORDER BY l.lap_duration ASC
LIMIT 1
```

---

## Visualizations the Data Supports

### Always available (all eras):
- **Lap time cards** — formatted as `m:ss.mmm`, delta shown as `+0.345s`
- **Session progression** — Q1 → Q2 → Q3 times per driver, with which round was their best
- **Grid position badges** — P1 = pole, color-coded
- **Historical qualifying head-to-head** — pick any two drivers across any year

### 2023+ only:
- **Sector time bars** — S1/S2/S3 side-by-side horizontal bars, colored by who was faster in each sector
- **Speed trap comparison** — three values (i1, i2, st) as a grouped bar or table
- **Mini-sector strip** — a row of colored mini-segment blocks (like the official F1 timing screen) showing purple/green/yellow per mini-sector
- **Delta-by-sector** — show where a driver gained or lost time (e.g. "VER was 0.041s faster in S2")

---

## Tech Notes for the Frontend

- **ClickHouse HTTP interface** is available at `http://localhost:8123` — the app can query directly with `?query=` or via a thin backend
- **CORS**: if building a pure SPA, route queries through a small proxy (FastAPI or Express) to avoid browser CORS issues with ClickHouse
- **Time formatting**: lap times from Jolpica are strings (`"1:23.456"`). OpenF1 `lap_duration` is a float in seconds (`83.456`). Normalize everything to milliseconds internally before display.
- **Team colors**: `team_colour` from OpenF1 is a hex string without `#` — prepend it: `"#" + team_colour`
- **Headshots**: `headshot_url` is a direct CDN URL, safe to use as `<img src>` directly
- **No OpenF1 for pre-2023**: `session_key` and `openf1_driver_number` will be null — the UI must check this and hide the telemetry panels

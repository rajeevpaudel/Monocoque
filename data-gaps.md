# Warehouse Data Gaps — F1 Viz Requirements

This document lists every data gap found in the warehouse that blocks or degrades the f1-viz app.
It is written for the team managing the warehouse pipeline so all gaps can be filled.

---

## Summary

| Priority | Gap | Affected table | Impact |
|---|---|---|---|
| 🔴 Critical | Las Vegas 2024 — no Jolpica-OpenF1 lap match | `mart_qualifying_summary` | Zero telemetry, no track map, no driver info for entire round |
| 🔴 Critical | Las Vegas 2024 — not in `dim_sessions` | `dim_sessions` | Race name and circuit_id can't be looked up |
| 🔴 Critical | `dim_circuits.length_km` and `.corners` — all NULL | `dim_circuits` | Circuit stats missing for all 78 circuits |
| 🟡 Medium | Sprint weekends — Sprint Qualifying rows contaminate results | `mart_qualifying_summary` | Duplicate rows; deduplication is fragile |
| 🟡 Medium | 16 individual driver mismatches across 8 rounds | `mart_qualifying_summary` | No sector times, speed, telemetry for those drivers |
| 🔴 Critical | Azerbaijan 2025 — no OpenF1 lap data for Qualifying (session 9900) | `mart_qualifying_summary` | All 19 drivers mismatch; `assert_match_rate` warns |
| 🟡 Medium | Miami 2025 Race — OpenF1 API serves only laps 25-57 intermittently | `raw_openf1.laps` | `assert_lap_count_consistent` warns for 18 drivers |
| 🟢 Low | Historical seasons 2000–2003 — Jolpica only, no OpenF1 | `mart_qualifying_summary` | Expected but worth noting for future expansion |

---

## Gap 1 — Las Vegas 2024 (Round 22): Complete qualifying data failure 🔴

### What's wrong

Every driver at Las Vegas 2024 has `best_source_match = 'mismatch'`, meaning the pipeline
could not align any Jolpica best-lap time to an OpenF1 lap within the ±50 ms tolerance.

As a result, for all 20 drivers at Round 22:

| Column | Status |
|---|---|
| `best_lap_duration` | NULL |
| `best_lap_number` | NULL |
| `openf1_best_time_ms` | NULL |
| `best_s1`, `best_s2`, `best_s3` | NULL |
| `i1_speed`, `i2_speed`, `st_speed` | NULL |
| `segments_s1/2/3` | NULL |
| `name_acronym` | `''` (blank) |
| `team_colour` | NULL |
| `race_name` | `''` (blank) |

The underlying telemetry **does exist** — `mart_lap_telemetry` has 424,864 rows for
`session_key = 9640`. The problem is the lap-matching step upstream of `mart_qualifying_summary`.

### Consequence

- The app can show Q1/Q2/Q3 strings (from Jolpica) but nothing else.
- No telemetry animation.
- No track map (the script that generates SVG paths from GPS can't find a `best_lap_number`
  to anchor to, so Vegas falls back to an old FastF1-generated path with an incompatible
  coordinate system → car positioning is wrong).
- Driver acronyms and team colours are missing → the UI displays fallback values.
- `dim_sessions` has no row for Round 22 at all (see Gap 2), so even the circuit_id
  (`las_vegas`) and race name come from a hardcoded fallback in the app.

### What needs to be done

1. **Re-run the Jolpica–OpenF1 lap matching for Round 22.** The tolerance may need to be
   widened for this specific session, or the correct OpenF1 `session_key` for Las Vegas
   Qualifying needs to be verified (the app assumes `session_key = 9640`).
2. **Verify `session_key = 9640`** is the correct key for the Las Vegas Qualifying session.
   If the actual key is different, the pipeline is querying the wrong session.
3. Once `best_lap_number` resolves, **populate `name_acronym` and `team_colour`** for all
   20 drivers from the matched OpenF1 driver records.
4. The track map will auto-fix once the pipeline runs — the warehouse script
   (`scripts/fetch-tracks-warehouse.mjs`) will generate the correct GPS-based SVG path.

---

## Gap 2 — Las Vegas 2024 (Round 22): Missing from `dim_sessions` 🔴

### What's wrong

`dim_sessions` has no row for the Las Vegas Qualifying session. All other 23 rounds of 2024
have a row in `dim_sessions` with `session_type = 'Qualifying'` and `session_name = 'Qualifying'`.

### Consequence

The app looks up `circuit_id` and `race_name` via `dim_sessions`. With no row, it falls back
to a hardcoded constant in the source code (`{ circuit_id: 'las_vegas', race_name: 'Las Vegas Grand Prix' }`).
This is brittle — if the data model changes, Las Vegas silently breaks.

### What needs to be done

Insert the Las Vegas Qualifying session row into `dim_sessions` with the correct
`session_key`, `circuit_id = 'las_vegas'`, `race_name`, `circuit_name`, `circuit_country`, etc.

---

## Gap 3 — `dim_circuits.length_km` and `.corners` are NULL for all circuits 🔴

### What's wrong

```sql
SELECT count() as total, countIf(length_km IS NOT NULL) as with_length
FROM f1_mart.dim_circuits
-- Result: total=78, with_length=0
```

All 78 circuits have `length_km = NULL` and `corners = NULL`.

### Consequence

The app displays these stats on every circuit map footer (e.g. `LENGTH 5.278 KM · CORNERS 14`).
Currently all values come from a hardcoded lookup table (`CIRCUIT_LENGTHS` / `CIRCUIT_CORNERS`
in `src/data/circuits.js`). This is a maintenance burden and prevents the app from being
fully warehouse-driven.

### What needs to be done

Populate `length_km` (float, km) and `corners` (int) for at minimum all 24 circuits used in
2024. The app only references these 24:

| `circuit_id` | Expected length_km | Expected corners |
|---|---|---|
| bahrain | 5.412 | 15 |
| jeddah | 6.174 | 27 |
| albert_park | 5.278 | 14 |
| suzuka | 5.807 | 18 |
| shanghai | 5.451 | 16 |
| miami | 5.412 | 19 |
| imola | 4.909 | 19 |
| monaco | 3.337 | 19 |
| villeneuve | 4.361 | 14 |
| catalunya | 4.657 | 16 |
| red_bull_ring | 4.318 | 10 |
| silverstone | 5.891 | 18 |
| hungaroring | 4.381 | 14 |
| spa | 7.004 | 19 |
| zandvoort | 4.259 | 14 |
| monza | 5.793 | 11 |
| baku | 6.003 | 20 |
| marina_bay | 4.940 | 23 |
| americas | 5.513 | 20 |
| rodriguez | 4.304 | 17 |
| interlagos | 4.309 | 15 |
| las_vegas | 6.201 | 17 |
| losail | 5.380 | 16 |
| yas_marina | 5.281 | 16 |

Once populated, the hardcoded fallback in the app can be removed entirely.

---

## Gap 4 — Sprint weekends: Sprint Qualifying rows contaminate `mart_qualifying_summary` 🟡

### What's wrong

Six rounds in 2024 were sprint weekends. Both the main Qualifying session and the Sprint
Qualifying session have `session_type = 'Qualifying'` in `dim_sessions`. The mart joins
against all `Qualifying` sessions, so `mart_qualifying_summary` ends up with **two rows
per driver** for these rounds — one per session.

Affected rounds:

| Round | Race | Total rows in mart | Expected |
|---|---|---|---|
| 5 | Chinese GP | 40 | 20 |
| 6 | Miami GP | 40 | 20 |
| 11 | Austrian GP | 40 | 20 |
| 19 | United States GP | 40 | 20 |
| 21 | São Paulo GP | 40 | 20 |
| 23 | Qatar GP | 40 | 20 |

The sessions in conflict are:

```
session_name = 'Qualifying'          → the real qualifying (this is what the app needs)
session_name = 'Sprint Qualifying'   → sprint shootout (not needed)
```

### Consequence

`mart_qualifying_summary` returns two rows per driver at sprint rounds. The app currently
deduplicates by `driver_id` keeping the first row ordered by `session_key ASC`. This
accidentally works because the main Qualifying session key is lower, but it is fragile.

The mart should exclude Sprint Qualifying sessions, or the app's filter on `session_name`
needs to be propagated into the mart so it only includes `session_name = 'Qualifying'`.

For sprint rounds, the `with_telemetry` count in the audit also shows mismatched numbers
(e.g. R19 has 18 matched out of 40, when it should be 18 matched out of 20).

### What needs to be done

Filter `mart_qualifying_summary` to only include rows from
`session_name = 'Qualifying'` (not `'Sprint Qualifying'`).
This will halve the row count for the 6 sprint rounds and remove all false mismatches.

---

## Gap 5 — Individual driver mismatches scattered across 8 rounds 🟡

### What's wrong

Outside the bulk failures (Las Vegas, sprint duplicates), there are 10 individual
driver rows with `best_source_match = 'mismatch'` — meaning one side of the data
(Jolpica or OpenF1) is present but the two could not be matched within ±50 ms.

```
Round  Driver           Jolpica time    OpenF1 time     Problem
─────  ───────────────  ─────────────   ─────────────   ────────────────────────────────────
R2     bearman          1:28.642        (missing)       No OpenF1 lap data for this driver
R2     hulkenberg       (missing)       1:29.055        Not in Jolpica results
R2     zhou             (missing)       2:01.100        Not in Jolpica results (probably DNF)
R7     sargeant         (missing)       1:16.731        Not in Jolpica results
R10    piastri          (missing)       1:12.011        Not in Jolpica results
R15    sargeant         (missing)       (missing)       No data on either side
R17    bearman          1:42.968        (missing)       No OpenF1 lap data
R18    leclerc          (missing)       1:29.747        Not in Jolpica results
R18    sainz            (missing)       1:30.108        Not in Jolpica results
R24    doohan           1:24.105        (missing)       No OpenF1 lap data
```

### Consequence

These drivers show qualifying position and Q1/Q2/Q3 string (if Jolpica has it) but have
no `best_lap_duration`, no sector times, no telemetry, and no acronym/colour in some cases.

### What needs to be done

Two patterns need fixing:

**Pattern A — Jolpica missing** (`jolpica_best_time_ms = 0`):
Hulkenberg/Zhou R2, Sargeant R7, Piastri R10, Sargeant R15, Leclerc/Sainz R18.
These are likely DNFs, time deletions, or drivers who didn't set a lap in that session.
Verify whether Jolpica truly has no record for them. If they genuinely didn't set a time,
the OpenF1 lap can be used directly as the best time.

**Pattern B — OpenF1 missing** (no matching OpenF1 lap within ±50 ms):
Bearman R2/R17, Doohan R24.
These are replacement drivers (Bearman replaced Sainz at Ferrari for several rounds;
Doohan was on debut). The OpenF1 driver number for a replacement driver may not be
linked to the correct session. Verify that `openf1_driver_number` for these drivers
points to the correct entry in the OpenF1 drivers endpoint for that session.

For Sargeant R15 (no data on either side): investigate whether he participated at all
(may be a DNS/did-not-start event).

---

## Gap 6 — Historical seasons 2000–2003: Jolpica only 🟢

### What's in the warehouse

The `mart_qualifying_summary` table has qualifying positions and Q1/Q2/Q3 lap time strings
(Jolpica) for seasons 2000, 2001, 2002, 2003, and 2024. All pre-2023 records have
`best_source_match = 'jolpica_only'` — no OpenF1 telemetry exists.

### Consequence

For these seasons the app can show driver list and lap time strings but nothing else
(no simulation, no telemetry, no track map). This is expected behaviour given the
OpenF1 data coverage starts in 2023.

### What needs to be done

Nothing urgent. If future seasons (2023, 2025) are ingested into Jolpica and OpenF1
in the pipeline, they will work automatically. The app has no known dependency on the
2000–2003 data at this time.

---

## Verification queries

Use these to confirm gaps are resolved after pipeline changes.

```sql
-- Gap 1: Las Vegas lap matching
SELECT best_source_match, count() as drivers
FROM f1_mart.mart_qualifying_summary
WHERE season = 2024 AND round = 22
GROUP BY best_source_match
-- Want: matched=20

-- Gap 2: Las Vegas in dim_sessions
SELECT count() FROM f1_mart.dim_sessions
WHERE season = 2024 AND round = 22 AND session_type = 'Qualifying'
-- Want: >= 1

-- Gap 3: Circuit physical data
SELECT countIf(length_km IS NOT NULL) as filled, count() as total
FROM f1_mart.dim_circuits
WHERE circuit_id IN ('bahrain','jeddah','albert_park','suzuka','shanghai','miami',
  'imola','monaco','villeneuve','catalunya','red_bull_ring','silverstone',
  'hungaroring','spa','zandvoort','monza','baku','marina_bay','americas',
  'rodriguez','interlagos','las_vegas','losail','yas_marina')
-- Want: filled=24, total=24

-- Gap 4: Sprint deduplication
SELECT round, count() as rows
FROM f1_mart.mart_qualifying_summary
WHERE season = 2024
GROUP BY round
HAVING rows > 20
-- Want: 0 rows

-- Gap 5: Remaining mismatches (excluding expected historical)
SELECT round, driver_id, best_source_match
FROM f1_mart.mart_qualifying_summary
WHERE season = 2024 AND best_source_match = 'mismatch'
ORDER BY round
-- Want: 0 rows
```

---

## Gap 6 — Miami 2025 Race (session_key=10033): Partial OpenF1 lap data 🟡

### What's wrong

`raw_openf1.laps` for session 10033 (2025 R6 Race) contains only laps 25-57 (~540 rows)
instead of laps 1-57 (~1080 rows). Jolpica shows the correct 57-lap count for all 18-20 drivers.

The `assert_lap_count_consistent` test warns on 18 drivers with diffs of 23-25 laps.

### Root cause

The OpenF1 API serves data inconsistently across backend replicas for this session. A curl
to `https://api.openf1.org/v1/laps?session_key=10033` sometimes returns 559 rows (laps 1-57)
and sometimes 540 rows (laps 25-57 only). The ingestion ran against a stale replica that
only had the second half of the race.

Re-ingestion does not reliably fix it because subsequent fetches may also hit the stale replica.

### Consequence

- Laps 1-24 missing from `stg_openf1__laps` and all downstream models for Miami 2025 Race.
- `assert_lap_count_consistent` warns (severity downgraded from error since this is unfixable upstream).
- Telemetry and car data for laps 1-24 may also be affected (car_data/location rows persisted
  from the original run which had duplicate data; those counts are unchanged).

### What needs to be done

Either wait for OpenF1 to stabilise their replica and re-ingest session 10033, or accept
the partial lap data as a known upstream limitation for this session.

To force a re-ingest once the API is consistent, run from inside the scheduler container:

```
INSERT INTO raw_meta.ingestion_log VALUES ('openf1','10033','raw_openf1.laps','incomplete',540,50,'manual force',now());
python -m ingestion.openf1.incremental --session-key 10033 --skip-telemetry
```

Verify with: `SELECT min(lap_number), max(lap_number), count() FROM raw_openf1.laps WHERE session_key=10033`
Expected: min=1 or 2, max=57, count≈540.

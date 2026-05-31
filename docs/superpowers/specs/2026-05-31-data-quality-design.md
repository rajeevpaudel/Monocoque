# Data Quality & Integrity Design

**Date:** 2026-05-31
**Status:** Approved
**Scope:** F1 data warehouse — pre-production quality gates + continuous monitoring

---

## Problem Statement

The F1 warehouse stitches together three external data sources (Jolpica, OpenF1, FastF1) that have no guaranteed consistency with each other. Known failure modes include cross-source lap-time matching failures, missing dimension rows, sprint weekend row contamination, and NULL circuit stats. Currently, bad data can reach production mart tables silently — there are only 4 dbt tests and no continuous monitoring.

This design establishes two layers of defence:
1. A pre-production gate inside the dbt pipeline that physically blocks bad data from reaching mart tables.
2. A continuous observability layer using dbt-elementary + Airflow that detects degradation after ingestion and alerts via Telegram, with automatic re-ingestion for known fixable failures.

---

## Architecture

```
                    ┌─────────────────────────────────────────────┐
                    │              dbt build pipeline              │
                    │                                              │
  Raw Sources ──▶  Staging ──▶ Intermediate ──▶ Marts             │
  (Jolpica,         │                              │               │
   OpenF1,          │   dbt tests run here ────────┘               │
   FastF1)          │   (schema.yml + singular SQL)                │
                    │   ↳ BLOCKS promotion to prod on failure      │
                    └──────────────────┬──────────────────────────┘
                                       │ dbt-elementary
                                       │ hooks into every dbt run
                                       ▼
                    ┌─────────────────────────────────────────────┐
                    │         elementary meta-database            │
                    │  (row counts, null rates, freshness,        │
                    │   anomaly baselines, test results)          │
                    └──────────────────┬──────────────────────────┘
                                       │ scheduled monitor
                                       ▼
                    ┌─────────────────────────────────────────────┐
                    │       Airflow DAG: dq_monitor               │
                    │  1. run edr monitor                         │
                    │  2. parse failures                          │
                    │  3. POST to Telegram bot                    │
                    │  4. trigger re-ingestion DAG (if fixable)   │
                    └─────────────────────────────────────────────┘
```

---

## Layer 1: dbt Test Suite (Pre-Production Gate)

### Tier 1 — Schema tests (schema.yml)

Declarative tests that run on every `dbt build`. A failure prevents the downstream mart from materializing.

| Model | Tests |
|---|---|
| `stg_openf1__sessions` | `not_null` on `session_key`, `season`, `round`; `accepted_values` on `session_name` |
| `stg_jolpica__qualifying` | `not_null` on `driver_id`, `season`, `round`, `q1` |
| `dim_sessions` | `not_null` on `circuit_id`, `race_name`; `relationships` to `dim_circuits.circuit_id` |
| `dim_circuits` | `not_null` on `length_km`, `corners` |
| `mart_qualifying_summary` | `accepted_values` on `best_source_match` ∈ `[matched, jolpica_only, mismatch]` |
| `mart_lap_telemetry` | `not_null` on `lap_time_ms`; range check `lap_time_ms` between 60000–300000 ms |

### Tier 2 — Singular SQL tests (dbt/tests/)

Business-rule assertions that encode domain knowledge about what valid F1 data looks like.

| File | Rule |
|---|---|
| `assert_driver_count_per_session.sql` | Every qualifying session has 18–20 drivers |
| `assert_no_sprint_contamination.sql` | No round in mart has >20 qualifying rows |
| `assert_match_rate.sql` | Each round's `mismatch` count ≤ 3 drivers |
| `assert_dim_sessions_coverage.sql` | Every round in `fact_qualifying` has a row in `dim_sessions` |
| `assert_circuit_stats_populated.sql` | All circuits used in current season have non-NULL `length_km` and `corners` |
| `assert_telemetry_completeness.sql` | Every `matched` driver in `mart_qualifying_summary` has ≥1 row in `mart_lap_telemetry` |

### Tier 3 — Source freshness (sources.yml)

Freshness checks on raw source tables block the build before any transformation runs if ingestion has stalled.

```yaml
freshness:
  warn_after: {count: 6, period: hour}
  error_after: {count: 24, period: hour}
loaded_at_field: _ingested_at
```

Applied to: `raw_openf1.sessions`, `raw_jolpica.results`, `raw_fastf1.car_telemetry`.

---

## Layer 2: Elementary Observability

### Installation

Add to `dbt/packages.yml`:
```yaml
packages:
  - package: elementary-data/elementary
    version: "0.16.x"
```

Elementary creates an `elementary` schema in ClickHouse on first run. No additional infrastructure required.

### Automatic monitors (zero config)

Elementary instruments every dbt model run and captures:
- Row count per model per run — anomaly detection vs. 14-day rolling baseline
- Column null rate — alerts on sudden spikes
- Schema changes — catches added/dropped/renamed columns
- Full dbt test result history — all Tier 1 & 2 tests are stored and trended

### Custom monitors (schema.yml per model)

| Model | Monitor type | Threshold |
|---|---|---|
| `mart_qualifying_summary` | `volume` anomaly | Alert if round row count < 18 |
| `mart_qualifying_summary` | `column_anomalies` on `best_source_match` | Alert if `mismatch` rate > 20% for any round |
| `mart_lap_telemetry` | `volume` anomaly | Alert if session row count drops >30% vs same session last ingestion |
| `dim_sessions` | `volume` anomaly | Alert if total row count decreases (row deletion) |

### Elementary report

`edr report` generates a static HTML observability report. A new `dbt-docs` service will be added to `docker-compose.yml` to serve both the dbt docs site and the elementary report. The elementary report will be accessible at `http://localhost:8082/elementary`.

---

## Layer 3: Airflow DAG — dq_monitor

### Schedule

- Triggered automatically after every ingestion DAG completes (`TriggerDagRunOperator`)
- Standalone hourly schedule as a safety net

### Task graph

```
run_dbt_source_freshness
        │
        ▼
run_dbt_tests
        │
        ▼
run_elementary_monitor
        │
        ▼
parse_failures
        │
        ├── no failures ──▶ send_telegram_ok  (daily summary only)
        │
        └── failures ──▶ classify_failures
                              │
                              ├── fixable ──▶ trigger_reingest_dag
                              │                     + send_telegram_alert
                              └── needs investigation ──▶ send_telegram_alert
```

### Failure classification rules

| Condition | Classification | Auto-action |
|---|---|---|
| `mismatch` count = all drivers for a round | Fixable — likely wrong `session_key` | Trigger `session_openf1` DAG with that `session_key` |
| `dim_sessions` coverage gap for a round | Fixable — missing session row | Trigger `backfill_jolpica` DAG for that season/round |
| Any other test failure | Needs investigation | Alert only |

### Telegram message format

**On failure:**
```
🔴 F1 Warehouse — Data Quality Alert
Round 22 (Las Vegas): 0/20 drivers matched
Cause: bulk mismatch — possible wrong session_key
Action: re-ingestion triggered for session 9640
View report: http://localhost:8082/elementary
```

**Daily green summary:**
```
✅ F1 Warehouse — All checks passed
24 rounds · 480 drivers · last ingestion 14 min ago
```

### Configuration

Two environment variables required:
- `TELEGRAM_BOT_TOKEN` — bot token from @BotFather
- `TELEGRAM_CHAT_ID` — target chat or channel ID

Stored in `.env` (local) and Airflow Variables (production).

---

## Files to Create / Modify

| Path | Action |
|---|---|
| `dbt/packages.yml` | Add elementary dependency |
| `dbt/models/staging/sources.yml` | Add freshness blocks to all three sources |
| `dbt/models/marts/schema.yml` | Expand with Tier 1 schema tests |
| `dbt/tests/assert_driver_count_per_session.sql` | New |
| `dbt/tests/assert_no_sprint_contamination.sql` | New |
| `dbt/tests/assert_match_rate.sql` | New |
| `dbt/tests/assert_dim_sessions_coverage.sql` | New |
| `dbt/tests/assert_circuit_stats_populated.sql` | New |
| `dbt/tests/assert_telemetry_completeness.sql` | New |
| `airflow/dags/dq_monitor.py` | New DAG |
| `airflow/dags/dq_telegram.py` | Telegram helper module |
| `docker-compose.yml` | Expose elementary report port |
| `docs/data-quality.md` | Architecture narrative for portfolio |

---

## Success Criteria

- `dbt build` fails fast when any mart would materialize with known bad data
- Elementary report is accessible and shows model history after first run
- A simulated bad ingestion (wrong session key) triggers a Telegram alert within 1 Airflow DAG cycle
- Daily green summary arrives in Telegram when all checks pass
- `data-gaps.md` issues are covered by at least one test each

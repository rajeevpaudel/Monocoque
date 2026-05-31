# Data Quality Architecture

This document explains the two-layer data quality system in the F1 warehouse.

## The problem

The warehouse joins three independent external APIs — Jolpica (historical results), OpenF1 (real-time telemetry), and FastF1 (car telemetry). None of them guarantee consistency with each other. Known failure modes:

- **Lap matching failures** — Jolpica and OpenF1 lap times can't be reconciled within ±50 ms (e.g., Las Vegas 2024 where all 20 drivers failed to match)
- **Missing dimension rows** — sessions or circuits absent from dimension tables
- **Sprint weekend contamination** — Sprint Qualifying sessions sharing a `session_type = 'Qualifying'` tag with the main qualifying session
- **Replacement driver gaps** — substitute drivers whose OpenF1 driver number isn't linked to the correct session

## Layer 1: dbt pre-production gate

Every `dbt build` run includes a three-tier test suite that physically blocks a mart model from materializing if its data is bad.

**Tier 1 — Schema tests** (`dbt/models/marts/schema.yml`): declarative not_null, unique, accepted_values, and relationship checks. These run fast and catch structural problems.

**Tier 2 — Singular SQL tests** (`dbt/tests/assert_*.sql`): 26 domain-specific assertions covering:

| Category | Tests | Example |
|---|---|---|
| Completeness | 8 | Every qualifying session has 18–20 drivers |
| Consistency | 6 | Sector times sum to lap time within ±0.2s |
| Uniqueness | 4 | No two drivers share a finishing position |
| Range/validity | 5 | All lap times between 60s and 2h |
| Referential integrity | 3 | All telemetry session_keys exist in dim_sessions |

**Tier 3 — Source freshness** (`dbt/models/staging/sources.yml`): Jolpica, OpenF1, and FastF1 sources declare a `loaded_at_field` and freshness thresholds. A stale source blocks the build before any transformation runs.

## Layer 2: Elementary observability

[dbt-elementary](https://docs.elementary-data.com) hooks into every dbt run and writes model metadata, test results, and statistical baselines to an `elementary` schema in ClickHouse. After ~14 runs it can detect anomalies automatically — a row count that drops 30% compared to the rolling baseline triggers an alert even without a hard-coded threshold.

Custom monitors are configured in `schema.yml` for:
- `mart_qualifying_summary` — volume anomalies and null rate on `best_source_match`
- `mart_lap_telemetry` — volume anomalies per session
- `dim_sessions` — row deletion detection

The elementary HTML report (`edr report`) is served on port 8082 via the `dbt-docs` Docker profile and shows full model history, test trends, and anomaly timelines.

## Layer 3: Airflow monitoring DAG

The `dq_monitor` DAG runs hourly and after every ingestion DAG. It:

1. Checks source freshness
2. Runs the full dbt test suite
3. Runs `edr monitor` (elementary anomaly detection)
4. Queries `elementary.elementary_test_results` for failures in the last 2 hours
5. Classifies failures:
   - **Bulk mismatch for a round** → triggers `session_openf1` DAG to re-ingest that session
   - **dim_sessions coverage gap** → triggers `backfill_jolpica` DAG for that season/round
   - **All other failures** → alert only
6. Posts a Telegram message to the configured channel:
   - Red alert with failure details if checks fail
   - Green daily summary if all checks pass

## Configuration

```bash
# Required environment variables
TELEGRAM_BOT_TOKEN=<from @BotFather>
TELEGRAM_CHAT_ID=<target chat or channel ID>
```

Set in `.env` for local development and as Airflow Variables in production.

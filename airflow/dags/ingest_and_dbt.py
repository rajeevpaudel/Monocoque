"""
Full pipeline DAG: ingest a single year → data quality checks → dbt run.

Trigger manually with params:
    {"year": 2024, "skip_telemetry": false}

Flow:
  ingest_jolpica ──┐
                   ├──► dbt_deps ──► dbt_run_staging ──► dbt_test_staging
  ingest_openf1 ──┘                                            │
  ingest_fastf1 ──┘                                    dbt_run_intermediate
                                                               │
                                              dbt_run_marts ──► dbt_test_marts
                                                               │
                                                 dq_source_freshness ──► notify_telegram
"""

import os
import subprocess
import sys
from datetime import datetime, timedelta
from pathlib import Path

from airflow.decorators import dag, task
from airflow.models.param import Param

# Ensure the ingestion package (mounted at /opt/airflow/ingestion) is importable.
_airflow_root = str(Path(__file__).parent.parent)
if _airflow_root not in sys.path:
    sys.path.insert(0, _airflow_root)

DBT_DIR = "/opt/airflow/dbt"
DBT_PROFILES_DIR = "/opt/airflow/dbt"


def _dbt(cmd: str) -> str:
    result = subprocess.run(
        f"cd {DBT_DIR} && dbt {cmd} --profiles-dir {DBT_PROFILES_DIR} --log-path /tmp/dbt-logs",
        shell=True,
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        raise RuntimeError(f"dbt {cmd} failed:\n{result.stdout}\n{result.stderr}")
    return result.stdout


def _delete_year_from_facts(year: int) -> None:
    """Delete all rows for `year` from incremental fact tables before dbt run.

    This makes re-ingestion of any round in that year visible to the next
    incremental dbt run without requiring a manual --full-refresh.
    """
    import os
    import clickhouse_connect

    client = clickhouse_connect.get_client(
        host=os.environ.get("CLICKHOUSE_HOST", "clickhouse"),
        port=int(os.environ.get("CLICKHOUSE_PORT", 8123)),
        username=os.environ.get("CLICKHOUSE_USER", "default"),
        password=os.environ.get("CLICKHOUSE_PASSWORD", ""),
    )
    for table in [
        "f1_mart.fact_qualifying",
        "f1_mart.fact_race_results",
        "f1_mart.fact_laps",
        "f1_mart.fact_sprint_results",
    ]:
        client.command(f"DELETE FROM {table} WHERE season = {year}")

    client.command(
        f"DELETE FROM f1_mart.mart_lap_telemetry "
        f"WHERE session_key IN ("
        f"  SELECT session_key FROM f1_intermediate.int_session_map WHERE season = {year}"
        f")"
    )


@dag(
    dag_id="ingest_and_dbt",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["ingestion", "dbt", "data-quality"],
    params={
        "year": Param(2024, type="integer", description="Season year to ingest"),
        "skip_telemetry": Param(
            False,
            type="boolean",
            description="Skip car_data/location (OpenF1 telemetry) — faster runs",
        ),
    },
    default_args={
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
    },
    max_active_tasks=3,
)
def ingest_and_dbt():

    # ── Ingestion ──────────────────────────────────────────────────────────────

    @task
    def ingest_jolpica(year: int) -> None:
        from ingestion.jolpica.backfill import ingest_reference_tables, ingest_season

        ingest_reference_tables()
        ingest_season(int(year))

    @task
    def ingest_openf1(year: int, skip_telemetry: bool) -> None:
        from ingestion.openf1.backfill import ingest_year

        skip = skip_telemetry if isinstance(skip_telemetry, bool) else skip_telemetry.lower() == "true"
        ingest_year(int(year), skip_telemetry=skip)

    @task
    def ingest_fastf1(year: int) -> None:
        import os
        import clickhouse_connect
        from ingestion.fastf1.backfill import IDENTIFIER_TO_SESSION_TYPES, _build_session_index, ingest_year

        year = int(year)

        client = clickhouse_connect.get_client(
            host=os.environ.get("CLICKHOUSE_HOST", "clickhouse"),
            port=int(os.environ.get("CLICKHOUSE_PORT", 8123)),
            username=os.environ.get("CLICKHOUSE_USER", "default"),
            password=os.environ.get("CLICKHOUSE_PASSWORD", ""),
        )
        ingested_keys = {
            row[0]
            for row in client.query(
                "SELECT DISTINCT session_key FROM raw_fastf1.car_telemetry"
            ).result_rows
        }

        completed: set[str] = set()
        if ingested_keys:
            # Reconstruct checkpoint keys: session_key → (round_num, session_type) → "{year}-{round}-{id}"
            index, _ = _build_session_index(year)
            key_to_round_type = {sk: (rnd, st) for (rnd, st), sk in index.items()}
            session_type_to_identifier = {
                st: identifier
                for identifier, types in IDENTIFIER_TO_SESSION_TYPES.items()
                for st in types
            }
            for sk in ingested_keys:
                if sk not in key_to_round_type:
                    continue
                round_num, session_type = key_to_round_type[sk]
                identifier = session_type_to_identifier.get(session_type)
                if identifier:
                    completed.add(f"{year}-{round_num}-{identifier}")

        ingest_year(year, completed=completed, on_complete=lambda _: None)

    # ── dbt pipeline ───────────────────────────────────────────────────────────

    @task
    def dbt_deps() -> None:
        _dbt("deps")

    @task
    def dbt_run_staging() -> None:
        _dbt("run --select staging")

    @task
    def dbt_test_staging() -> None:
        _dbt("test --select staging")

    @task
    def dbt_run_intermediate() -> None:
        _dbt("run --select intermediate")

    @task
    def dbt_run_marts(year: int) -> None:
        _delete_year_from_facts(int(year))
        _dbt("run --select marts")

    @task
    def dbt_test_marts() -> None:
        _dbt("test --select marts")

    # ── Data quality ───────────────────────────────────────────────────────────

    @task
    def dq_source_freshness() -> None:
        result = subprocess.run(
            f"cd {DBT_DIR} && dbt source freshness --profiles-dir {DBT_PROFILES_DIR} --log-path /tmp/dbt-logs",
            shell=True,
            capture_output=True,
            text=True,
        )
        if result.returncode != 0:
            raise RuntimeError(f"dbt source freshness failed:\n{result.stdout}\n{result.stderr}")

    # ── Notifications ──────────────────────────────────────────────────────────

    @task(trigger_rule="all_done")
    def notify_telegram(**context) -> None:
        import requests

        token = os.environ.get("TELEGRAM_BOT_TOKEN", "")
        chat_id = os.environ.get("TELEGRAM_CHAT_ID", "")
        if not token or not chat_id:
            raise RuntimeError("TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID must be set")

        year = context["params"]["year"]

        # Derive success/failure from task instances — dag_run.get_state() returns
        # "running" while notify_telegram itself is still executing.
        task_instances = context["dag_run"].get_task_instances()
        counts = {"success": 0, "failed": 0, "skipped": 0}
        failed_tasks = []
        for ti in task_instances:
            if ti.task_id == "notify_telegram":
                continue
            s = ti.state or "none"
            if s == "success":
                counts["success"] += 1
            elif s in ("failed", "upstream_failed"):
                counts["failed"] += 1
                failed_tasks.append(ti.task_id)
            elif s == "skipped":
                counts["skipped"] += 1

        ok = counts["failed"] == 0
        icon = "✅" if ok else "❌"
        status = "success" if ok else "failed"

        # HTML parse_mode avoids Markdown conflicts with underscores in task IDs.
        lines = [
            f"{icon} <b>F1 pipeline — {year}</b>",
            f"Status: <code>{status}</code>",
            f"Tasks: {counts['success']} passed, {counts['failed']} failed, {counts['skipped']} skipped",
        ]
        if failed_tasks:
            lines.append("Failed: " + ", ".join(f"<code>{t}</code>" for t in failed_tasks))

        resp = requests.post(
            f"https://api.telegram.org/bot{token}/sendMessage",
            json={"chat_id": chat_id, "text": "\n".join(lines), "parse_mode": "HTML"},
            timeout=10,
        )
        if not resp.ok:
            raise RuntimeError(f"Telegram API error {resp.status_code}: {resp.text}")

    # ── Wire up ────────────────────────────────────────────────────────────────

    year = "{{ params.year }}"
    skip = "{{ params.skip_telemetry }}"

    jolpica = ingest_jolpica(year=year)
    openf1 = ingest_openf1(year=year, skip_telemetry=skip)
    fastf1 = ingest_fastf1(year=year)

    deps = dbt_deps()
    stg = dbt_run_staging()
    stg_test = dbt_test_staging()
    inter = dbt_run_intermediate()
    marts = dbt_run_marts(year=year)
    marts_test = dbt_test_marts()
    freshness = dq_source_freshness()
    notify = notify_telegram()

    [jolpica, openf1, fastf1] >> deps >> stg >> stg_test >> inter >> marts >> marts_test
    marts_test >> freshness >> notify


ingest_and_dbt()

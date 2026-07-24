"""
Data quality monitor DAG.

Runs after every ingestion DAG and hourly as a safety net.
Parses dbt test failures, sends Telegram alerts,
and triggers re-ingestion DAGs for known fixable failures.
"""

import json
import os
import subprocess
from datetime import datetime, timedelta

from airflow.decorators import dag, task

DBT_DIR = "/opt/airflow/dbt"
DBT_PROFILES_DIR = "/opt/airflow/dbt"


def parse_run_results(run_results: dict) -> list[dict]:
    """Extract fail/error test results from a parsed dbt run_results.json."""
    failures = []
    for r in run_results.get("results", []):
        if r.get("status") in ("fail", "error"):
            failures.append({
                "test_name": r.get("unique_id", "unknown").split(".")[-1],
                "status": r["status"],
                "failures": r.get("failures", 0),
                "message": r.get("message", ""),
            })
    return failures


def attach_failure_context(failures: list[dict], client) -> None:
    """Best-effort: look up (season, round) for each failed test from its
    dbt store_failures audit table (test_failures.<test_name>), so Telegram
    alerts can show which race weekend broke. Not every test selects season/
    round in its failing rows, so a missing/errored lookup just leaves the
    alert without round context rather than failing the task.
    """
    for f in failures:
        try:
            rows = client.query(
                f"SELECT DISTINCT season, round FROM test_failures.{f['test_name']} "
                f"WHERE season IS NOT NULL AND round IS NOT NULL LIMIT 1"
            ).result_rows
            if rows:
                f["season"], f["round"] = rows[0]
        except Exception:
            pass


@dag(
    dag_id="dq_monitor",
    schedule="@hourly",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["data-quality", "monitoring"],
    default_args={
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
)
def dq_monitor():
    def _dbt(cmd: str) -> str:
        result = subprocess.run(
            f"cd {DBT_DIR} && dbt {cmd} --profiles-dir {DBT_PROFILES_DIR} --no-partial-parse",
            shell=True,
            capture_output=True,
            text=True,
        )
        if result.returncode != 0:
            raise RuntimeError(f"dbt {cmd} failed:\nSTDOUT:\n{result.stdout}\nSTDERR:\n{result.stderr}")
        return result.stdout

    @task
    def run_dbt_freshness() -> None:
        _dbt("source freshness")

    @task
    def run_dbt_tests() -> dict:
        # Run without raising — dbt exits non-zero when tests fail, which is
        # expected. Failures are read from run_results.json and returned via XCom.
        result = subprocess.run(
            f"cd {DBT_DIR} && dbt test --profiles-dir {DBT_PROFILES_DIR} --no-partial-parse",
            shell=True,
            capture_output=True,
            text=True,
        )
        try:
            with open(f"{DBT_DIR}/target/run_results.json") as f:
                run_results = json.load(f)
            failures = parse_run_results(run_results)
        except Exception as e:
            failures = [{
                "test_name": "dbt_test_runner",
                "status": "error",
                "failures": 1,
                "message": f"Could not read run_results.json: {e}\n\nSTDOUT:\n{result.stdout[-1000:]}",
            }]
            return {"failures": failures}

        if failures:
            try:
                import clickhouse_connect
                client = clickhouse_connect.get_client(
                    host=os.environ.get("CLICKHOUSE_HOST", "clickhouse")
                )
                attach_failure_context(failures, client)
            except Exception:
                pass  # round context is best-effort — don't lose real failures over it
        return {"failures": failures}

    @task
    def parse_and_act(test_results: dict) -> None:
        bot_token = os.environ.get("TELEGRAM_BOT_TOKEN", "")
        chat_id = os.environ.get("TELEGRAM_CHAT_ID", "")

        failures = test_results.get("failures", [])

        if not bot_token or not chat_id:
            if failures:
                print(f"Telegram not configured — {len(failures)} test failure(s) not alerted")
                for f in failures:
                    print(f"  FAIL: {f.get('test_name')} — {f.get('message', '')[:200]}")
            return

        if not failures:
            import clickhouse_connect
            from dq_telegram import send_daily_ok
            client = clickhouse_connect.get_client(
                host=os.environ.get("CLICKHOUSE_HOST", "clickhouse")
            )
            stats_rows = client.query(
                """
                SELECT
                    count(DISTINCT round)  AS rounds,
                    count()                AS drivers
                FROM f1_mart.mart_qualifying_summary
                """
            ).named_results()
            stats = dict(list(stats_rows)[0]) if stats_rows else {}
            stats["minutes_since_ingest"] = "?"
            send_daily_ok(stats, bot_token=bot_token, chat_id=chat_id)
            return

        from dq_telegram import send_alert
        send_alert(failures, bot_token=bot_token, chat_id=chat_id)

    freshness = run_dbt_freshness()
    tests = run_dbt_tests()
    act = parse_and_act(tests)

    freshness >> tests >> act


dq_monitor()

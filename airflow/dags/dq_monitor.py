"""
Data quality monitor DAG.

Runs after every ingestion DAG and hourly as a safety net.
Parses dbt test failures, sends Telegram alerts,
and triggers re-ingestion DAGs for known fixable failures.

Note: Elementary's edr CLI is incompatible with ClickHouse 24.3 (no transaction
support, CAST(current_timestamp) syntax mismatch). The run_elementary_monitor task
runs best-effort and never blocks the pipeline. Test failures are sourced from
dbt's own run_results.json via XCom, not from elementary.elementary_test_results.
"""

import json
import os
import re
import subprocess
from datetime import datetime, timedelta

from airflow.decorators import dag, task


DBT_DIR = "/opt/airflow/dbt"
DBT_PROFILES_DIR = "/opt/airflow/dbt"


def classify_failures(
    failures: list[dict],
) -> tuple[list[dict], list[dict]]:
    """Split failures into fixable (auto-trigger) and needs-investigation (alert only)."""
    fixable = []
    investigate = []
    for f in failures:
        test = f.get("test_name", "")
        if test == "assert_match_rate" and f.get("session_key"):
            fixable.append({**f, "action": "reingest_session", "session_key": f["session_key"]})
        elif test == "assert_dim_sessions_coverage" and f.get("season"):
            fixable.append({**f, "action": "reingest_jolpica", "season": f["season"], "round": f.get("round")})
        else:
            investigate.append(f)
    return fixable, investigate


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
    def _run(cmd: str) -> str:
        # --no-partial-parse: avoids PermissionError on the mounted dbt/target/
        # directory when the container user differs from the host file owner.
        dbt_flags = "--profiles-dir {profiles} --no-partial-parse".format(
            profiles=DBT_PROFILES_DIR
        )
        # edr commands take --profiles-dir but not --no-partial-parse
        if cmd.startswith("edr "):
            full_cmd = f"cd {DBT_DIR} && {cmd} --profiles-dir {DBT_PROFILES_DIR}"
        else:
            full_cmd = f"cd {DBT_DIR} && {cmd} {dbt_flags}"
        result = subprocess.run(
            full_cmd,
            shell=True,
            capture_output=True,
            text=True,
        )
        if result.returncode != 0:
            raise RuntimeError(f"Command failed:\nSTDOUT:\n{result.stdout}\nSTDERR:\n{result.stderr}")
        return result.stdout

    @task
    def run_dbt_freshness() -> None:
        _run("dbt source freshness")

    @task
    def run_dbt_tests() -> dict:
        stdout = _run("dbt test")
        # Read structured results from run_results.json written by dbt.
        failures = []
        try:
            with open(f"{DBT_DIR}/target/run_results.json") as f:
                run_results = json.load(f)
            for r in run_results.get("results", []):
                if r.get("status") in ("fail", "error"):
                    failures.append({
                        "test_name": r.get("unique_id", "unknown").split(".")[-1],
                        "status": r["status"],
                        "failures": r.get("failures", 0),
                        "message": r.get("message", ""),
                    })
        except Exception:
            # Fallback: scan stdout summary for failure count
            m = re.search(r"(\d+) error", stdout)
            if m and int(m.group(1)) > 0:
                failures.append({
                    "test_name": "unknown",
                    "status": "error",
                    "failures": int(m.group(1)),
                    "message": stdout[-500:],
                })
        return {"failures": failures}

    @task(trigger_rule="all_done")
    def run_elementary_monitor() -> None:
        # Best-effort Elementary HTML report. Non-blocking because Elementary's
        # ClickHouse adapter is incompatible with ClickHouse 24.3: no transaction
        # support and CAST(current_timestamp, 'timestamp') syntax not recognized.
        try:
            _run("edr report")
        except RuntimeError as e:
            import logging
            logging.getLogger(__name__).warning("edr report failed (non-blocking): %s", e)

    @task
    def parse_and_act(test_results: dict) -> None:
        bot_token = os.environ["TELEGRAM_BOT_TOKEN"]
        chat_id = os.environ["TELEGRAM_CHAT_ID"]

        failures = test_results.get("failures", [])

        if not failures:
            from dq_telegram import send_daily_ok
            import clickhouse_connect
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

        from airflow.api.common.trigger_dag import trigger_dag as _trigger_dag

        fixable, _ = classify_failures(failures)
        for item in fixable:
            if item["action"] == "reingest_session":
                _trigger_dag(
                    dag_id="session_openf1",
                    conf={"session_key": item["session_key"]},
                )
            elif item["action"] == "reingest_jolpica":
                _trigger_dag(
                    dag_id="backfill_jolpica",
                    conf={"start_season": item["season"], "end_season": item["season"]},
                )

    freshness = run_dbt_freshness()
    tests = run_dbt_tests()
    monitor = run_elementary_monitor()
    act = parse_and_act(tests)

    # monitor runs in parallel with act after tests complete.
    # act does not wait on monitor — test results come from XCom, not Elementary.
    freshness >> tests >> [monitor, act]


dq_monitor()

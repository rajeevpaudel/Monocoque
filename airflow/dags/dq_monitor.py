"""
Data quality monitor DAG.

Runs after every ingestion DAG and hourly as a safety net.
Parses dbt + elementary test failures, sends Telegram alerts,
and triggers re-ingestion DAGs for known fixable failures.
"""

import os
import subprocess
from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.operators.trigger_dagrun import TriggerDagRunOperator


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
        result = subprocess.run(
            f"cd {DBT_DIR} && {cmd} --profiles-dir {DBT_PROFILES_DIR}",
            shell=True,
            capture_output=True,
            text=True,
        )
        if result.returncode != 0:
            raise RuntimeError(f"Command failed:\n{result.stderr}")
        return result.stdout

    @task
    def run_dbt_freshness() -> None:
        _run("dbt source freshness")

    @task
    def run_dbt_tests() -> None:
        _run("dbt test")

    @task
    def run_elementary_monitor() -> None:
        _run("edr monitor")

    @task
    def parse_and_act(ti=None) -> None:
        import clickhouse_connect

        bot_token = os.environ["TELEGRAM_BOT_TOKEN"]
        chat_id = os.environ["TELEGRAM_CHAT_ID"]
        ch_host = os.environ.get("CLICKHOUSE_HOST", "clickhouse")

        client = clickhouse_connect.get_client(host=ch_host)
        rows = client.query(
            """
            SELECT
                test_unique_id   AS test_name,
                status,
                failures,
                elementary_unique_id
            FROM elementary.elementary_test_results
            WHERE status = 'fail'
              AND generated_at >= now() - INTERVAL 2 HOUR
            ORDER BY generated_at DESC
            """
        ).named_results()

        failures = [dict(r) for r in rows]

        if not failures:
            from airflow.dags.dq_telegram import send_daily_ok
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

        from airflow.dags.dq_telegram import send_alert
        send_alert(failures, bot_token=bot_token, chat_id=chat_id)

        fixable, _ = classify_failures(failures)
        for item in fixable:
            if item["action"] == "reingest_session":
                TriggerDagRunOperator(
                    task_id=f"reingest_session_{item['session_key']}",
                    trigger_dag_id="session_openf1",
                    conf={"session_key": item["session_key"]},
                ).execute(context={})
            elif item["action"] == "reingest_jolpica":
                TriggerDagRunOperator(
                    task_id=f"reingest_jolpica_{item['season']}_{item.get('round', 0)}",
                    trigger_dag_id="backfill_jolpica",
                    conf={"start_season": item["season"], "end_season": item["season"]},
                ).execute(context={})

    freshness = run_dbt_freshness()
    tests = run_dbt_tests()
    monitor = run_elementary_monitor()
    act = parse_and_act()

    freshness >> tests >> monitor >> act


dq_monitor()

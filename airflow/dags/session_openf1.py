"""
OpenF1 session DAG — triggered once per session after date_end has passed.
Call via TriggerDagRun with conf: {"session_key": 9158}
"""

from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.operators.trigger_dagrun import TriggerDagRunOperator


@dag(
    dag_id="session_openf1",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["openf1", "incremental"],
    max_active_runs=1,  # one session at a time (car_data is ~2M rows)
    default_args={
        "retries": 3,
        "retry_delay": timedelta(minutes=5),
    },
)
def session_openf1():
    @task
    def ingest_session(session_key: str):
        from ingestion.openf1.backfill import ingest_session as _ingest

        # dag_run.conf is arbitrary, untyped user input (unlike Param-declared configs) —
        # validate it's actually numeric before it can reach any downstream SQL/query param.
        _ingest(int(session_key), skip_telemetry=False)

    trigger_dbt = TriggerDagRunOperator(
        task_id="trigger_dbt",
        trigger_dag_id="dbt_run",
        wait_for_completion=True,
    )

    session_key = "{{ dag_run.conf.get('session_key') }}"
    ingested = ingest_session(session_key=session_key)
    ingested >> trigger_dbt


session_openf1()

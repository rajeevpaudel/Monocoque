"""
dbt run DAG — triggered by ingest DAGs. Runs all layers in order with tests.
"""

import os
from datetime import datetime, timedelta

from airflow.decorators import dag, task


@dag(
    dag_id="dbt_run",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["dbt"],
    default_args={
        "retries": 1,
        "retry_delay": timedelta(minutes=2),
    },
)
def dbt_run():
    DBT_DIR = "/opt/airflow/dbt"
    DBT_PROFILES_DIR = "/opt/airflow/dbt"

    def _run(cmd: str):
        import subprocess
        result = subprocess.run(
            f"cd {DBT_DIR} && dbt {cmd} --profiles-dir {DBT_PROFILES_DIR}",
            shell=True,
            capture_output=True,
            text=True,
        )
        if result.returncode != 0:
            raise RuntimeError(f"dbt {cmd} failed:\n{result.stdout}\n{result.stderr}")
        return result.stdout

    @task
    def dbt_deps():
        _run("deps")

    @task
    def dbt_run_staging():
        _run("run --select staging")

    @task
    def dbt_test_staging():
        _run("test --select staging")

    @task
    def dbt_run_intermediate():
        _run("run --select intermediate")

    @task
    def dbt_run_marts():
        _run("run --select marts")

    @task
    def dbt_test_marts():
        _run("test --select marts")

    deps = dbt_deps()
    stg = dbt_run_staging()
    stg_test = dbt_test_staging()
    inter = dbt_run_intermediate()
    marts = dbt_run_marts()
    marts_test = dbt_test_marts()

    deps >> stg >> stg_test >> inter >> marts >> marts_test


dbt_run()

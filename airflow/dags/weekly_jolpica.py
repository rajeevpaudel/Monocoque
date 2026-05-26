"""
Weekly Jolpica DAG — runs every Monday at 06:00 UTC after a race weekend.
Ingests the most recently completed race round and triggers dbt.
"""

from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.operators.trigger_dagrun import TriggerDagRunOperator


@dag(
    dag_id="weekly_jolpica",
    schedule="0 6 * * 1",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["jolpica", "incremental"],
    default_args={
        "retries": 3,
        "retry_delay": timedelta(minutes=10),
    },
)
def weekly_jolpica():
    @task
    def check_race_weekend() -> dict:
        """Return (season, round) of the last completed race or skip if no new race."""
        import httpx
        from datetime import date

        resp = httpx.get(
            "https://api.jolpi.ca/ergast/f1/current.json",
            params={"limit": 1},
        )
        resp.raise_for_status()
        races = resp.json()["MRData"]["RaceTable"]["Races"]
        if not races:
            raise ValueError("No races found for current season")

        today = date.today()
        completed = [r for r in races if r["date"] < str(today)]
        if not completed:
            raise ValueError("No completed races this season yet")

        latest = completed[-1]
        return {"season": int(latest["season"]), "round": int(latest["round"])}

    @task
    def ingest_results(race_info: dict):
        from ingestion.jolpica.incremental import ingest_round
        ingest_round(race_info["season"], race_info["round"])

    trigger_dbt = TriggerDagRunOperator(
        task_id="trigger_dbt",
        trigger_dag_id="dbt_run",
        wait_for_completion=True,
    )

    race_info = check_race_weekend()
    ingested = ingest_results(race_info)
    ingested >> trigger_dbt


weekly_jolpica()

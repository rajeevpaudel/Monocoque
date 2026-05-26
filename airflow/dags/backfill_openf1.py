"""
One-time OpenF1 backfill DAG (2023-present).

Trigger manually with params: {"start_year": 2023, "end_year": 2025}
"""

from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.models.param import Param


@dag(
    dag_id="backfill_openf1",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["openf1", "backfill"],
    params={
        "start_year":      Param(2023, type="integer"),
        "end_year":        Param(2025, type="integer"),
        "skip_telemetry":  Param(False, type="boolean", description="Skip car_data/location"),
    },
    default_args={
        "retries": 3,
        "retry_delay": timedelta(minutes=5),
    },
    max_active_tasks=1,  # telemetry loads are memory-intensive
)
def backfill_openf1():
    @task
    def ingest_year(year: int, skip_telemetry: bool):
        from ingestion.openf1.backfill import ingest_year as _ingest
        _ingest(year, skip_telemetry=skip_telemetry)

    @task
    def generate_years(start: int, end: int) -> list[int]:
        return list(range(start, end + 1))

    years = generate_years(
        start="{{ params.start_year }}",
        end="{{ params.end_year }}",
    )
    ingest_year.expand_kwargs([
        {"year": y, "skip_telemetry": "{{ params.skip_telemetry }}"} for y in [2023]  # placeholder
    ])


backfill_openf1()

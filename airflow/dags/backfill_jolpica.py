"""
One-time Jolpica historical backfill DAG.

Trigger manually with params: {"start_season": 1950, "end_season": 2022}
For a quick smoke test: {"start_season": 2018, "end_season": 2019}
"""

from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.models.param import Param


@dag(
    dag_id="backfill_jolpica",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["jolpica", "backfill"],
    params={
        "start_season": Param(1950, type="integer", description="First season to load"),
        "end_season":   Param(2022, type="integer", description="Last season to load (inclusive)"),
        "skip_reference": Param(False, type="boolean", description="Skip circuits/drivers/etc"),
    },
    default_args={
        "retries": 3,
        "retry_delay": timedelta(minutes=5),
    },
)
def backfill_jolpica():
    @task
    def ingest_reference(skip_reference: bool):
        if skip_reference:
            return
        from ingestion.jolpica import endpoints
        from ingestion.jolpica.backfill import _model_to_dict
        from ingestion.shared import clickhouse as ch

        for fn, table in [
            (endpoints.get_seasons,      "raw_jolpica.seasons"),
            (endpoints.get_circuits,     "raw_jolpica.circuits"),
            (endpoints.get_constructors, "raw_jolpica.constructors"),
            (endpoints.get_drivers,      "raw_jolpica.drivers"),
        ]:
            rows = fn()
            ch.insert_rows(table, [_model_to_dict(r) for r in rows])

    @task
    def ingest_season(season: int):
        from ingestion.jolpica.backfill import ingest_season as _ingest
        _ingest(season)

    @task
    def generate_seasons(start: int, end: int) -> list[int]:
        return list(range(start, end + 1))

    seasons = generate_seasons(
        start="{{ params.start_season }}",
        end="{{ params.end_season }}",
    )

    ref = ingest_reference(skip_reference="{{ params.skip_reference }}")
    season_tasks = ingest_season.expand(season=seasons)
    ref >> season_tasks


backfill_jolpica()

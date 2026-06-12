"""
One-time f1db historical data load DAG.

Downloads the f1db CSV dump (https://github.com/f1db/f1db) and loads it
into raw_f1db.* tables. Covers all F1 data from 1950 to present.

Trigger manually — no schedule. Run once before ingest_and_dbt to seed
historical data without hitting the Jolpica API thousands of times.

Flow:
  resolve_version → download → load_reference   ─┐
                             → load_race_data    ─┼─► (done)
                             → load_supplementary─┘
"""

import os
import sys
from datetime import datetime, timedelta
from pathlib import Path

from airflow.decorators import dag, task
from airflow.models.param import Param

_airflow_root = str(Path(__file__).parent.parent)
if _airflow_root not in sys.path:
    sys.path.insert(0, _airflow_root)


@dag(
    dag_id="load_f1db",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["f1db", "backfill"],
    params={
        "release": Param(
            None,
            type=["string", "null"],
            description="f1db release tag (e.g. v2026.6.1). Leave empty to auto-detect latest.",
        ),
    },
    default_args={
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
    },
)
def load_f1db():

    @task
    def resolve_version(release) -> str:
        if release and release != "None":
            return release
        from ingestion.f1db.loader import _latest_version
        return _latest_version()

    @task
    def download(version: str) -> str:
        """Download the CSV zip once to /tmp; skip if already present."""
        import urllib.request

        path = f"/tmp/f1db-{version}.zip"
        if os.path.exists(path):
            return path

        url = f"https://github.com/f1db/f1db/releases/download/{version}/f1db-csv.zip"
        with urllib.request.urlopen(url) as r:
            data = r.read()
        with open(path, "wb") as f:
            f.write(data)
        return path

    @task
    def load_reference(zip_path: str) -> None:
        """seasons, circuits, drivers, constructors"""
        import zipfile
        from ingestion.f1db.loader import (
            load_circuits,
            load_constructors,
            load_drivers,
            load_seasons,
        )

        zf = zipfile.ZipFile(zip_path)
        load_seasons(zf)
        load_circuits(zf)
        load_drivers(zf)
        load_constructors(zf)

    @task
    def load_race_data(zip_path: str) -> None:
        """races, race_results, qualifying_results, sprint_race_results"""
        import zipfile
        from ingestion.f1db.loader import (
            load_qualifying_results,
            load_race_results,
            load_races,
            load_sprint_race_results,
        )

        zf = zipfile.ZipFile(zip_path)
        load_races(zf)
        load_race_results(zf)
        load_qualifying_results(zf)
        load_sprint_race_results(zf)

    @task
    def load_supplementary(zip_path: str) -> None:
        """pit_stops, fastest_laps, driver_standings, constructor_standings"""
        import zipfile
        from ingestion.f1db.loader import (
            load_constructor_standings,
            load_driver_standings,
            load_fastest_laps,
            load_pit_stops,
        )

        zf = zipfile.ZipFile(zip_path)
        load_pit_stops(zf)
        load_fastest_laps(zf)
        load_driver_standings(zf)
        load_constructor_standings(zf)

    version = resolve_version(release="{{ params.release }}")
    zip_path = download(version=version)

    zip_path >> [
        load_reference(zip_path=zip_path),
        load_race_data(zip_path=zip_path),
        load_supplementary(zip_path=zip_path),
    ]


load_f1db()

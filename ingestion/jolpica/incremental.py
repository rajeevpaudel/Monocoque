"""
Jolpica single race weekend incremental load.

Usage:
    python -m ingestion.jolpica.incremental --season 2025 --round 8
"""

import argparse

import structlog

from ingestion.jolpica import endpoints
from ingestion.jolpica.backfill import _model_to_dict
from ingestion.shared import clickhouse as ch

log = structlog.get_logger()


def ingest_round(season: int, round_num: int):
    rlog = log.bind(season=season, round=round_num)

    for fn, table in [
        (lambda: endpoints.get_results(season, round_num),          "raw_jolpica.results"),
        (lambda: endpoints.get_qualifying(season, round_num),        "raw_jolpica.qualifying"),
        (lambda: endpoints.get_sprint(season, round_num),            "raw_jolpica.sprint_results"),
        (lambda: endpoints.get_laps(season, round_num),              "raw_jolpica.lap_times"),
        (lambda: endpoints.get_pit_stops(season, round_num),         "raw_jolpica.pit_stops"),
        (lambda: endpoints.get_driver_standings(season, round_num),  "raw_jolpica.driver_standings"),
        (lambda: endpoints.get_constructor_standings(season, round_num), "raw_jolpica.constructor_standings"),
    ]:
        try:
            rows = fn()
            if rows:
                ch.insert_rows(table, [_model_to_dict(r) for r in rows])
                rlog.info("inserted", table=table, count=len(rows))
        except Exception as e:
            rlog.warning("failed", table=table, error=str(e))

    rlog.info("incremental load done")


def main():
    parser = argparse.ArgumentParser(description="Jolpica single race weekend load")
    parser.add_argument("--season", type=int, required=True)
    parser.add_argument("--round",  type=int, required=True)
    args = parser.parse_args()
    ingest_round(args.season, args.round)


if __name__ == "__main__":
    main()

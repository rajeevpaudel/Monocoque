"""
Jolpica full historical backfill.

Usage:
    python -m ingestion.jolpica.backfill --start 1950 --end 2022
    python -m ingestion.jolpica.backfill --start 2018 --end 2019  # quick test
"""

import argparse
import sys

import structlog

from ingestion.jolpica import endpoints
from ingestion.shared import clickhouse as ch

log = structlog.get_logger()

# Reference tables ingested once (not per-season)
REFERENCE_TABLES = [
    ("raw_jolpica.seasons",      endpoints.get_seasons),
    ("raw_jolpica.circuits",     endpoints.get_circuits),
    ("raw_jolpica.constructors", endpoints.get_constructors),
    ("raw_jolpica.drivers",      endpoints.get_drivers),
]


def _model_to_dict(m) -> dict:
    """Pydantic model → flat dict for ClickHouse insert."""
    d = m.model_dump(by_alias=False)
    # Rename ingested_at → _ingested_at to match column name
    d["_ingested_at"] = d.pop("ingested_at")
    d["_raw_json"] = d.pop("raw_json")
    return d


def ingest_reference_tables():
    log.info("ingesting reference tables")
    for table, fn in REFERENCE_TABLES:
        rows = fn()
        ch.insert_rows(table, [_model_to_dict(r) for r in rows])
        log.info("done", table=table, count=len(rows))


def ingest_season(season: int):
    slog = log.bind(season=season)
    slog.info("starting season")

    races = endpoints.get_races(season)
    ch.insert_rows("raw_jolpica.races", [_model_to_dict(r) for r in races])
    slog.info("races", count=len(races))

    if not races:
        slog.info("no races found, skipping")
        return

    for race in races:
        rnd = race.round
        rlog = slog.bind(round=rnd)

        try:
            rows = endpoints.get_results(season, rnd)
            ch.insert_rows("raw_jolpica.results", [_model_to_dict(r) for r in rows])
        except Exception as e:
            rlog.warning("results failed", error=str(e))

        try:
            rows = endpoints.get_qualifying(season, rnd)
            ch.insert_rows("raw_jolpica.qualifying", [_model_to_dict(r) for r in rows])
        except Exception as e:
            rlog.warning("qualifying failed", error=str(e))

        try:
            rows = endpoints.get_sprint(season, rnd)
            if rows:
                ch.insert_rows("raw_jolpica.sprint_results", [_model_to_dict(r) for r in rows])
        except Exception as e:
            rlog.warning("sprint failed", error=str(e))

        try:
            rows = endpoints.get_laps(season, rnd)
            if rows:
                ch.insert_rows("raw_jolpica.lap_times", [_model_to_dict(r) for r in rows])
        except Exception as e:
            rlog.warning("laps failed", error=str(e))

        try:
            rows = endpoints.get_pit_stops(season, rnd)
            if rows:
                ch.insert_rows("raw_jolpica.pit_stops", [_model_to_dict(r) for r in rows])
        except Exception as e:
            rlog.warning("pit stops failed", error=str(e))

        try:
            rows = endpoints.get_driver_standings(season, rnd)
            ch.insert_rows("raw_jolpica.driver_standings", [_model_to_dict(r) for r in rows])
            rows = endpoints.get_constructor_standings(season, rnd)
            ch.insert_rows("raw_jolpica.constructor_standings", [_model_to_dict(r) for r in rows])
        except Exception as e:
            rlog.warning("standings failed", error=str(e))

        rlog.info("round done")

    slog.info("season done")


def main():
    parser = argparse.ArgumentParser(description="Jolpica historical backfill")
    parser.add_argument("--start", type=int, required=True, help="First season (e.g. 1950)")
    parser.add_argument("--end",   type=int, required=True, help="Last season inclusive (e.g. 2022)")
    parser.add_argument("--skip-reference", action="store_true", help="Skip circuits/drivers/etc tables")
    args = parser.parse_args()

    if not args.skip_reference:
        ingest_reference_tables()

    for season in range(args.start, args.end + 1):
        ingest_season(season)

    log.info("backfill complete", start=args.start, end=args.end)


if __name__ == "__main__":
    main()

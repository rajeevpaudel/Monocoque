"""
Jolpica full historical backfill.

Usage:
    python -m ingestion.jolpica.backfill --start 1950 --end 2022
    python -m ingestion.jolpica.backfill --start 2018 --end 2019  # quick test
"""

import argparse

import structlog

from ingestion.jolpica import endpoints
from ingestion.shared import clickhouse as ch
from ingestion.shared import ingestion_log as log_util

log = structlog.get_logger()

# Reference tables ingested once (not per-season)
REFERENCE_TABLES = [
    ("raw_jolpica.seasons", endpoints.get_seasons),
    ("raw_jolpica.circuits", endpoints.get_circuits),
    ("raw_jolpica.constructors", endpoints.get_constructors),
    ("raw_jolpica.drivers", endpoints.get_drivers),
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


_SEASON_TABLES = [
    "raw_jolpica.races",
    "raw_jolpica.results",
    "raw_jolpica.qualifying",
    "raw_jolpica.sprint_results",
    "raw_jolpica.lap_times",
    "raw_jolpica.pit_stops",
    "raw_jolpica.driver_standings",
    "raw_jolpica.constructor_standings",
]


def ingest_season(season: int) -> None:
    slog = log.bind(season=season)
    slog.info("starting season")

    races = endpoints.get_races(season)
    if not races:
        slog.info("no races found, skipping")
        return

    ch.insert_rows("raw_jolpica.races", [_model_to_dict(r) for r in races])
    slog.info("races stored", count=len(races))

    _ROUND_TABLES = [
        ("raw_jolpica.results",               lambda rnd: endpoints.get_results(season, rnd)),
        ("raw_jolpica.qualifying",            lambda rnd: endpoints.get_qualifying(season, rnd)),
        ("raw_jolpica.sprint_results",        lambda rnd: endpoints.get_sprint(season, rnd)),
        ("raw_jolpica.lap_times",             lambda rnd: endpoints.get_laps(season, rnd)),
        ("raw_jolpica.pit_stops",             lambda rnd: endpoints.get_pit_stops(season, rnd)),
        ("raw_jolpica.driver_standings",      lambda rnd: endpoints.get_driver_standings(season, rnd)),
        ("raw_jolpica.constructor_standings", lambda rnd: endpoints.get_constructor_standings(season, rnd)),
    ]

    for race in races:
        rnd = race.round
        entity_key = f"{season}-{rnd}"
        rlog = slog.bind(round=rnd)

        for table, fetch_fn in _ROUND_TABLES:
            if not log_util.needs_ingestion(log_util.SOURCE_JOLPICA, entity_key, table):
                rlog.debug("skipping — already complete", table=table)
                continue

            ch.delete_rows(table, f"season = {season} AND round = {rnd}")
            try:
                rows = fetch_fn(rnd)
                count = len(rows) if rows else 0
                if rows:
                    ch.insert_rows(table, [_model_to_dict(r) for r in rows])
                log_util.record(log_util.SOURCE_JOLPICA, entity_key, table, count)
                rlog.info("ingested", table=table, count=count)
            except Exception as e:
                log_util.record(log_util.SOURCE_JOLPICA, entity_key, table, 0, str(e))
                rlog.warning("failed", table=table, error=str(e))

        rlog.info("round done")

    slog.info("season done")


def main():
    parser = argparse.ArgumentParser(description="Jolpica historical backfill")
    parser.add_argument("--start", type=int, required=True, help="First season (e.g. 1950)")
    parser.add_argument("--end", type=int, required=True, help="Last season inclusive (e.g. 2022)")
    parser.add_argument(
        "--skip-reference", action="store_true", help="Skip circuits/drivers/etc tables"
    )
    args = parser.parse_args()

    if not args.skip_reference:
        ingest_reference_tables()

    for season in range(args.start, args.end + 1):
        ingest_season(season)

    log.info("backfill complete", start=args.start, end=args.end)


if __name__ == "__main__":
    main()

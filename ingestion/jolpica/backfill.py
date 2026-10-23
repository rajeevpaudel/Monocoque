"""
Jolpica full historical backfill.

Usage:
    python -m ingestion.jolpica.backfill --start 1950 --end 2022
    python -m ingestion.jolpica.backfill --start 2018 --end 2019  # quick test
"""

import argparse
import sys
from collections.abc import Callable

import structlog

from ingestion.jolpica import endpoints
from ingestion.shared import clickhouse as ch
from ingestion.shared import ingestion_log as log_util
from ingestion.shared.models import model_to_dict

log = structlog.get_logger()

# Reference tables ingested once (not per-season)
REFERENCE_TABLES = [
    ("raw_jolpica.seasons", endpoints.get_seasons),
    ("raw_jolpica.circuits", endpoints.get_circuits),
    ("raw_jolpica.constructors", endpoints.get_constructors),
    ("raw_jolpica.drivers", endpoints.get_drivers),
]


# Backward-compatible alias — kept importable as ingestion.jolpica.backfill._model_to_dict
# (used by airflow/dags/backfill_jolpica.py). Logic now lives in ingestion.shared.models.
_model_to_dict = model_to_dict


def ingest_reference_tables():
    """Reference endpoints return the full historical set on every call, so each
    table is truncated before insert — re-running never duplicates rows."""
    log.info("ingesting reference tables")
    for table, fn in REFERENCE_TABLES:
        rows = fn()
        ch.truncate_table(table)
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


def _round_tables(season: int) -> list[tuple[str, Callable]]:
    return [
        ("raw_jolpica.results", lambda rnd: endpoints.get_results(season, rnd)),
        ("raw_jolpica.qualifying", lambda rnd: endpoints.get_qualifying(season, rnd)),
        ("raw_jolpica.sprint_results", lambda rnd: endpoints.get_sprint(season, rnd)),
        ("raw_jolpica.lap_times", lambda rnd: endpoints.get_laps(season, rnd)),
        ("raw_jolpica.pit_stops", lambda rnd: endpoints.get_pit_stops(season, rnd)),
        ("raw_jolpica.driver_standings", lambda rnd: endpoints.get_driver_standings(season, rnd)),
        (
            "raw_jolpica.constructor_standings",
            lambda rnd: endpoints.get_constructor_standings(season, rnd),
        ),
    ]


def ingest_round(season: int, rnd: int, rlog=None) -> bool:
    """
    Ingest one race weekend's per-round tables (results, qualifying, laps, etc).
    - Skips tables already marked 'ok'/'empty' in raw_meta.ingestion_log.
    - Deletes stale (season, round) rows before re-inserting, so re-running a
      round never duplicates rows.
    Returns True if no table ended in a failed fetch/insert.
    """
    rlog = rlog or log.bind(season=season, round=rnd)
    entity_key = f"{season}-{rnd}"
    all_ok = True

    for table, fetch_fn in _round_tables(season):
        if not log_util.needs_ingestion(log_util.SOURCE_JOLPICA, entity_key, table):
            rlog.debug("skipping — already complete", table=table)
            continue

        ch.delete_rows(
            table,
            "season = {season:UInt16} AND round = {rnd:UInt16}",
            parameters={"season": season, "rnd": rnd},
        )
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
            all_ok = False

    rlog.info("round done", all_ok=all_ok)
    return all_ok


def ingest_season(season: int) -> list[int]:
    """Load every round of a season. Returns the rounds that had at least one failed table."""
    slog = log.bind(season=season)
    slog.info("starting season")

    races = endpoints.get_races(season)
    if not races:
        slog.info("no races found, skipping")
        return []

    ch.delete_rows("raw_jolpica.races", "season = {season:UInt16}", parameters={"season": season})
    ch.insert_rows("raw_jolpica.races", [_model_to_dict(r) for r in races])
    slog.info("races stored", count=len(races))

    failed = [
        race.round
        for race in races
        if not ingest_round(season, race.round, rlog=slog.bind(round=race.round))
    ]

    slog.info("season done", failed_rounds=failed)
    return failed


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

    failed = [
        (season, rnd) for season in range(args.start, args.end + 1) for rnd in ingest_season(season)
    ]

    log.info("backfill complete", start=args.start, end=args.end, failed_rounds=failed)
    if failed:
        sys.exit(1)


if __name__ == "__main__":
    main()

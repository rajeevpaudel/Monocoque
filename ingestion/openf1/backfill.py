"""
OpenF1 full backfill (2023-present).

Usage:
    python -m ingestion.openf1.backfill --start 2023 --end 2025
    python -m ingestion.openf1.backfill --start 2023 --end 2023 --skip-telemetry

Re-running is safe: completed tables are tracked in raw_meta.ingestion_log
and skipped on subsequent runs.
"""

import argparse
from datetime import datetime, timezone

import structlog

from ingestion.openf1 import driver_map as dm
from ingestion.openf1 import endpoints
from ingestion.shared import clickhouse as ch
from ingestion.shared import ingestion_log as log_util

log = structlog.get_logger()


# ── Core ingestion ────────────────────────────────────────────────────────────


def _model_to_dict(m) -> dict:
    d = m.model_dump(by_alias=False)
    d["_ingested_at"] = d.pop("ingested_at")
    d["_raw_json"] = d.pop("raw_json")
    return d


def ingest_session(session_key: int, skip_telemetry: bool = False) -> bool:
    """
    Ingest one session table-by-table.
    - Skips tables already marked 'ok'/'empty' in the log.
    - Deletes stale rows before re-inserting for 'failed'/'incomplete' tables.
    - Records outcome in log after each table.
    Returns True if no table ended with 'failed' status.
    """
    slog = log.bind(session_key=session_key)
    entity_key = str(session_key)
    all_ok = True

    core_tables = [
        (lambda: endpoints.get_drivers(session_key),      "raw_openf1.drivers"),
        (lambda: endpoints.get_laps(session_key),         "raw_openf1.laps"),
        (lambda: endpoints.get_pit(session_key),          "raw_openf1.pit"),
        (lambda: endpoints.get_stints(session_key),       "raw_openf1.stints"),
        (lambda: endpoints.get_intervals(session_key),    "raw_openf1.intervals"),
        (lambda: endpoints.get_weather(session_key),      "raw_openf1.weather"),
        (lambda: endpoints.get_race_control(session_key), "raw_openf1.race_control"),
    ]
    telemetry_tables = [
        (lambda: endpoints.get_car_data(session_key),  "raw_openf1.car_data"),
        (lambda: endpoints.get_location(session_key),  "raw_openf1.location"),
    ]

    tables_to_run = core_tables if skip_telemetry else core_tables + telemetry_tables

    for fn, table in tables_to_run:
        if not log_util.needs_ingestion(log_util.SOURCE_OPENF1, entity_key, table):
            slog.debug("skipping — already complete", table=table)
            continue

        ch.delete_rows(table, f"session_key = {session_key}")
        try:
            rows = fn()
            count = len(rows) if rows else 0
            if rows:
                ch.insert_rows(table, [_model_to_dict(r) for r in rows])
            log_util.record(log_util.SOURCE_OPENF1, entity_key, table, count)
            slog.info("ingested", table=table, count=count)
        except Exception as e:
            log_util.record(log_util.SOURCE_OPENF1, entity_key, table, 0, str(e))
            slog.warning("failed", table=table, error=str(e))
            all_ok = False

    return all_ok


def ingest_year(year: int, skip_telemetry: bool = False) -> None:
    ylog = log.bind(year=year)
    ylog.info("fetching sessions")

    sessions = endpoints.get_sessions(year)
    now = datetime.now(timezone.utc)

    complete = [
        s for s in sessions
        if s.date_end and datetime.fromisoformat(s.date_end.replace("Z", "+00:00")) < now
    ]

    ch.insert_rows("raw_openf1.sessions", [_model_to_dict(s) for s in sessions])
    ylog.info("sessions stored", total=len(sessions), complete=len(complete))

    try:
        dm.build_driver_map(year)
    except Exception as e:
        ylog.warning("driver map failed, continuing without it", error=str(e))

    _TELEMETRY_TYPES = {"Race", "Qualifying"}

    for session in complete:
        want_telemetry = not skip_telemetry and session.session_type in _TELEMETRY_TYPES
        ingest_session(session.session_key, skip_telemetry=not want_telemetry)

    ylog.info("year done", sessions=len(complete))


def main():
    parser = argparse.ArgumentParser(description="OpenF1 backfill (2023-present)")
    parser.add_argument("--start", type=int, required=True)
    parser.add_argument("--end", type=int, required=True)
    parser.add_argument(
        "--skip-telemetry",
        action="store_true",
        help="Skip car_data and location (saves time/memory)",
    )
    args = parser.parse_args()

    log.info("starting backfill", start=args.start, end=args.end)

    for year in range(args.start, args.end + 1):
        ingest_year(year, skip_telemetry=args.skip_telemetry)

    log.info("backfill complete", start=args.start, end=args.end)


if __name__ == "__main__":
    main()

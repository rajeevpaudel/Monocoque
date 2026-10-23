"""
OpenF1 full backfill (2023-present).

Usage:
    python -m ingestion.openf1.backfill --start 2023 --end 2025
    python -m ingestion.openf1.backfill --start 2023 --end 2023 --skip-telemetry

Re-running is safe: completed tables are tracked in raw_meta.ingestion_log
and skipped on subsequent runs.
"""

import argparse
import sys
from datetime import UTC, datetime

import structlog

from ingestion.openf1 import driver_map as dm
from ingestion.openf1 import endpoints
from ingestion.openf1.client import OpenF1APIError
from ingestion.shared import clickhouse as ch
from ingestion.shared import ingestion_log as log_util
from ingestion.shared.models import model_to_dict

log = structlog.get_logger()


# ── Core ingestion ────────────────────────────────────────────────────────────


# Backward-compatible alias — logic now lives in ingestion.shared.models.
_model_to_dict = model_to_dict


def ingest_session(session_key: int, skip_telemetry: bool = False, force: bool = False) -> bool:
    """
    Ingest one session table-by-table.
    - Skips tables already marked 'ok'/'empty' in the log, unless force=True.
    - Deletes stale rows before re-inserting for 'failed'/'incomplete' tables.
    - Records outcome in log after each table.
    Returns True if no table ended with 'failed' status.
    """
    session_key = int(session_key)  # reject non-numeric input before it ever reaches SQL
    slog = log.bind(session_key=session_key)
    entity_key = str(session_key)
    all_ok = True

    core_tables = [
        (lambda: endpoints.get_drivers(session_key), "raw_openf1.drivers"),
        (lambda: endpoints.get_laps(session_key), "raw_openf1.laps"),
        (lambda: endpoints.get_pit(session_key), "raw_openf1.pit"),
        (lambda: endpoints.get_stints(session_key), "raw_openf1.stints"),
        (lambda: endpoints.get_intervals(session_key), "raw_openf1.intervals"),
        (lambda: endpoints.get_position(session_key), "raw_openf1.position"),
        (lambda: endpoints.get_weather(session_key), "raw_openf1.weather"),
        (lambda: endpoints.get_race_control(session_key), "raw_openf1.race_control"),
    ]
    telemetry_tables = [
        (lambda: endpoints.get_car_data(session_key), "raw_openf1.car_data"),
        (lambda: endpoints.get_location(session_key), "raw_openf1.location"),
    ]

    tables_to_run = core_tables if skip_telemetry else core_tables + telemetry_tables

    for fn, table in tables_to_run:
        if not force and not log_util.needs_ingestion(log_util.SOURCE_OPENF1, entity_key, table):
            slog.debug("skipping — already complete", table=table)
            continue

        ch.delete_rows(table, "session_key = {sk:UInt32}", parameters={"sk": session_key})
        try:
            rows = fn()
            count = len(rows) if rows else 0
            if rows:
                ch.insert_rows(table, [_model_to_dict(r) for r in rows])
            log_util.record(log_util.SOURCE_OPENF1, entity_key, table, count)
            slog.info("ingested", table=table, count=count)
        except OpenF1APIError as e:
            log_util.record(log_util.SOURCE_OPENF1, entity_key, table, 0, str(e))
            slog.error(
                "openf1 api unavailable — session will be marked incomplete",
                table=table,
                error=str(e),
            )
            all_ok = False
        except Exception as e:
            log_util.record(log_util.SOURCE_OPENF1, entity_key, table, 0, str(e))
            slog.error(
                "unexpected failure ingesting table", table=table, error=str(e), exc_info=True
            )
            all_ok = False

    return all_ok


def ingest_year(year: int, skip_telemetry: bool = False) -> list[int]:
    """Load every completed session of a year. Returns session_keys with a failed table."""
    ylog = log.bind(year=year)
    ylog.info("fetching sessions")

    try:
        sessions = endpoints.get_sessions(year)
    except OpenF1APIError as e:
        ylog.error(
            "cannot fetch sessions — OpenF1 API unavailable; if a live session is in "
            "progress, re-run this after it ends",
            year=year,
            error=str(e),
        )
        raise
    now = datetime.now(UTC)

    live = [s for s in sessions if s.status == "started"]
    complete = [
        s
        for s in sessions
        if s.date_end
        and datetime.fromisoformat(s.date_end.replace("Z", "+00:00")) < now
        and s.status != "started"
    ]

    if live:
        for s in live:
            ylog.warning(
                "skipping live session — OpenF1 API is unreliable during active sessions; "
                "re-run ingest after the session ends",
                session_key=s.session_key,
                session_name=s.session_name,
                status=s.status,
            )

    ch.delete_rows("raw_openf1.sessions", "year = {year:UInt16}", parameters={"year": year})
    ch.insert_rows("raw_openf1.sessions", [_model_to_dict(s) for s in sessions])
    ylog.info("sessions stored", total=len(sessions), complete=len(complete), live=len(live))

    try:
        dm.build_driver_map(year)
    except Exception as e:
        ylog.warning("driver map failed, continuing without it", error=str(e))

    _TELEMETRY_TYPES = {"Race", "Qualifying"}

    failed = []
    for session in complete:
        want_telemetry = not skip_telemetry and session.session_type in _TELEMETRY_TYPES
        if not ingest_session(session.session_key, skip_telemetry=not want_telemetry):
            failed.append(session.session_key)

    ylog.info("year done", sessions=len(complete), skipped_live=len(live), failed_sessions=failed)
    return failed


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

    failed = [
        sk
        for year in range(args.start, args.end + 1)
        for sk in ingest_year(year, skip_telemetry=args.skip_telemetry)
    ]

    log.info("backfill complete", start=args.start, end=args.end, failed_sessions=failed)
    if failed:
        sys.exit(1)


if __name__ == "__main__":
    main()

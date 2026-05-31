"""
OpenF1 full backfill (2023-present).

Usage:
    python -m ingestion.openf1.backfill --start 2023 --end 2025
    python -m ingestion.openf1.backfill --start 2023 --end 2023 --skip-telemetry

Re-running is safe: completed sessions are checkpointed in .backfill_checkpoint.json
and skipped on subsequent runs. Delete that file to force a full re-ingest.
"""

import argparse
import json
import os
from datetime import UTC, datetime

import structlog

from ingestion.openf1 import driver_map as dm
from ingestion.openf1 import endpoints
from ingestion.shared import clickhouse as ch

log = structlog.get_logger()

_CHECKPOINT_FILE = os.path.join(os.path.dirname(__file__), ".backfill_checkpoint.json")


# ── Checkpoint helpers ────────────────────────────────────────────────────────


def _load_checkpoint() -> set[int]:
    if os.path.exists(_CHECKPOINT_FILE):
        with open(_CHECKPOINT_FILE) as f:
            return set(json.load(f))
    return set()


def _save_checkpoint(completed: set[int]) -> None:
    with open(_CHECKPOINT_FILE, "w") as f:
        json.dump(sorted(completed), f)


# ── Core ingestion ────────────────────────────────────────────────────────────


def _model_to_dict(m) -> dict:
    d = m.model_dump(by_alias=False)
    d["_ingested_at"] = d.pop("ingested_at")
    d["_raw_json"] = d.pop("raw_json")
    return d


def ingest_session(session_key: int, skip_telemetry: bool = False) -> bool:
    """Ingest one session. Returns True if all tables succeeded, False if any failed."""
    slog = log.bind(session_key=session_key)
    failed = False

    for fn, table in [
        (lambda: endpoints.get_laps(session_key), "raw_openf1.laps"),
        (lambda: endpoints.get_pit(session_key), "raw_openf1.pit"),
        (lambda: endpoints.get_stints(session_key), "raw_openf1.stints"),
        (lambda: endpoints.get_intervals(session_key), "raw_openf1.intervals"),
        (lambda: endpoints.get_weather(session_key), "raw_openf1.weather"),
        (lambda: endpoints.get_race_control(session_key), "raw_openf1.race_control"),
    ]:
        try:
            rows = fn()
            if rows:
                ch.insert_rows(table, [_model_to_dict(r) for r in rows])
                slog.info("inserted", table=table, count=len(rows))
        except Exception as e:
            slog.warning("failed", table=table, error=str(e))
            failed = True

    if not skip_telemetry:
        for fn, table in [
            (lambda: endpoints.get_car_data(session_key), "raw_openf1.car_data"),
            (lambda: endpoints.get_location(session_key), "raw_openf1.location"),
        ]:
            try:
                rows = fn()
                if rows:
                    ch.insert_rows(table, [_model_to_dict(r) for r in rows])
                    slog.info("inserted telemetry", table=table, count=len(rows))
            except Exception as e:
                slog.warning("telemetry failed", table=table, error=str(e))
                failed = True

    if failed:
        slog.warning("session completed with errors — will retry on next run")
    else:
        slog.info("session done")

    return not failed


def ingest_year(year: int, skip_telemetry: bool = False, completed: set[int] = None):
    ylog = log.bind(year=year)
    ylog.info("fetching sessions")

    sessions = endpoints.get_sessions(year)
    now = datetime.now(UTC)

    complete = [
        s
        for s in sessions
        if s.date_end and datetime.fromisoformat(s.date_end.replace("Z", "+00:00")) < now
    ]

    ch.insert_rows("raw_openf1.sessions", [_model_to_dict(s) for s in sessions])
    ylog.info("sessions stored", total=len(sessions), complete=len(complete))

    try:
        dm.build_driver_map(year)
    except Exception as e:
        ylog.warning("driver map failed, continuing without it", error=str(e))

    _TELEMETRY_TYPES = {"Race", "Qualifying"}

    skipped = 0
    for session in complete:
        if session.session_key in completed:
            skipped += 1
            continue

        try:
            driver_rows = endpoints.get_drivers(session.session_key)
            ch.insert_rows("raw_openf1.drivers", [_model_to_dict(d) for d in driver_rows])
        except Exception as e:
            ylog.warning("drivers failed", session_key=session.session_key, error=str(e))

        want_telemetry = not skip_telemetry and session.session_type in _TELEMETRY_TYPES
        ok = ingest_session(session.session_key, skip_telemetry=not want_telemetry)
        if ok:
            completed.add(session.session_key)
            _save_checkpoint(completed)

    ylog.info("year done", skipped=skipped, ingested=len(complete) - skipped)


def main():
    parser = argparse.ArgumentParser(description="OpenF1 backfill (2023-present)")
    parser.add_argument("--start", type=int, required=True)
    parser.add_argument("--end", type=int, required=True)
    parser.add_argument(
        "--skip-telemetry",
        action="store_true",
        help="Skip car_data and location (saves time/memory)",
    )
    parser.add_argument(
        "--reset", action="store_true", help="Ignore checkpoint and re-ingest everything"
    )
    args = parser.parse_args()

    completed = set() if args.reset else _load_checkpoint()
    if args.reset and os.path.exists(_CHECKPOINT_FILE):
        os.remove(_CHECKPOINT_FILE)

    log.info("starting backfill", start=args.start, end=args.end, already_completed=len(completed))

    for year in range(args.start, args.end + 1):
        ingest_year(year, skip_telemetry=args.skip_telemetry, completed=completed)

    log.info("backfill complete", start=args.start, end=args.end)


if __name__ == "__main__":
    main()

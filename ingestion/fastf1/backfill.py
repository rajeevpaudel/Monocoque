"""FastF1 distance backfill — all session types, checkpointed per (year, round, identifier).

Usage:
    python -m ingestion.fastf1.backfill --start 2023 --end 2025

Re-running is safe: completed (year, round, identifier) triples are checkpointed in
.backfill_checkpoint.json and skipped on subsequent runs.
"""

import argparse
import json
import os

import fastf1
import structlog

from ingestion.fastf1 import client
from ingestion.openf1 import endpoints as openf1_endpoints
from ingestion.shared import clickhouse as ch

log = structlog.get_logger()

_CHECKPOINT_FILE = os.path.join(os.path.dirname(__file__), ".backfill_checkpoint.json")

IDENTIFIERS = ["FP1", "FP2", "FP3", "Q", "SQ", "S", "R"]

# Maps each FastF1 session identifier to the candidate OpenF1 session_type strings.
# First match in the list wins when looking up the session_key.
IDENTIFIER_TO_SESSION_TYPES: dict[str, list[str]] = {
    "FP1": ["Practice 1", "Practice"],
    "FP2": ["Practice 2", "Practice"],
    "FP3": ["Practice 3", "Practice"],
    "Q":   ["Qualifying"],
    "SQ":  ["Sprint Qualifying", "Sprint Shootout"],
    "S":   ["Sprint"],
    "R":   ["Race"],
}


def _load_checkpoint() -> set[str]:
    if os.path.exists(_CHECKPOINT_FILE):
        with open(_CHECKPOINT_FILE) as f:
            return set(json.load(f))
    return set()


def _save_checkpoint(completed: set[str]) -> None:
    with open(_CHECKPOINT_FILE, "w") as f:
        json.dump(sorted(completed), f)


def _resolve_session_key(
    index: dict[tuple[int, str], int],
    round_num: int,
    identifier: str,
) -> int | None:
    """Return the OpenF1 session_key for (round, FastF1 identifier), or None if absent."""
    for session_type in IDENTIFIER_TO_SESSION_TYPES.get(identifier, []):
        key = index.get((round_num, session_type))
        if key is not None:
            return key
    return None


def _build_session_index(year: int) -> tuple[dict[tuple[int, str], int], list[int]]:
    """Fetch OpenF1 sessions for a year.

    Returns:
        index: {(round_num, openf1_session_type): session_key}
        rounds: sorted list of 1-based round numbers
    """
    all_sessions = openf1_endpoints.get_sessions(year)

    # Deduplicate: keep the latest Race per meeting (sprint weekends have two)
    race_by_meeting: dict[int, object] = {}
    for s in all_sessions:
        if s.session_type != "Race":
            continue
        prev = race_by_meeting.get(s.meeting_key)
        if prev is None or s.date_start > prev.date_start:
            race_by_meeting[s.meeting_key] = s

    main_races = sorted(race_by_meeting.values(), key=lambda s: s.date_start)
    meeting_to_round = {s.meeting_key: i + 1 for i, s in enumerate(main_races)}

    index: dict[tuple[int, str], int] = {}
    for s in all_sessions:
        rnd = meeting_to_round.get(s.meeting_key)
        if rnd is not None:
            index[(rnd, s.session_type)] = s.session_key

    rounds = list(range(1, len(main_races) + 1))
    return index, rounds


def ingest_year(year: int, completed: set[str]) -> None:
    ylog = log.bind(year=year)
    ylog.info("building session index from OpenF1")
    index, rounds = _build_session_index(year)
    ylog.info("index built", rounds=len(rounds))

    for round_num in rounds:
        for identifier in IDENTIFIERS:
            ck = f"{year}-{round_num}-{identifier}"
            if ck in completed:
                continue

            session_key = _resolve_session_key(index, round_num, identifier)
            if session_key is None:
                # Session type doesn't exist for this weekend — mark done so we skip next time
                completed.add(ck)
                _save_checkpoint(completed)
                continue

            slog = ylog.bind(round=round_num, identifier=identifier, session_key=session_key)
            try:
                arrow_table = client.get_session_telemetry(session_key, year, round_num, identifier)
            except fastf1.core.SessionNotAvailableError:
                slog.info("not available in FastF1 — skipping")
                completed.add(ck)
                _save_checkpoint(completed)
                continue
            except Exception as e:
                slog.warning("failed — will retry next run", error=str(e))
                continue  # intentionally not checkpointed; retry on next run

            if arrow_table.num_rows > 0:
                ch.insert_arrow("raw_fastf1.car_telemetry", arrow_table)
                slog.info("inserted", rows=arrow_table.num_rows)
            else:
                slog.warning("empty telemetry from FastF1")

            completed.add(ck)
            _save_checkpoint(completed)


def main() -> None:
    parser = argparse.ArgumentParser(description="FastF1 distance backfill (2023-present)")
    parser.add_argument("--start", type=int, required=True)
    parser.add_argument("--end",   type=int, required=True)
    parser.add_argument("--reset", action="store_true",
                        help="Ignore checkpoint and re-ingest everything")
    args = parser.parse_args()

    completed: set[str] = set()
    if not args.reset:
        completed = _load_checkpoint()
    elif os.path.exists(_CHECKPOINT_FILE):
        os.remove(_CHECKPOINT_FILE)

    log.info("starting backfill", start=args.start, end=args.end, already_done=len(completed))

    for year in range(args.start, args.end + 1):
        ingest_year(year, completed)

    log.info("backfill complete", start=args.start, end=args.end)


if __name__ == "__main__":
    main()

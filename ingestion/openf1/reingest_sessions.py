"""
Targeted re-ingest of specific OpenF1 qualifying sessions by year + round.

Usage:
    # Re-ingest qualifying telemetry for 2024 rounds 20, 22, 24
    python -m ingestion.openf1.reingest_sessions --year 2024 --rounds 20 22 24

Round→session mapping strategy:
  - Race sessions have no sprint-qualifying ambiguity, so we sort Race sessions
    by date to establish the 1-based round ordinal.
  - We then find the Qualifying session with the same meeting_key as that Race
    session. This correctly skips sprint qualifying sessions that also carry
    session_type = "Qualifying" in the OpenF1 API.
"""

import argparse

import structlog

from ingestion.openf1 import endpoints
from ingestion.openf1.backfill import _load_checkpoint, _save_checkpoint, ingest_session

log = structlog.get_logger()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--year", type=int, required=True)
    parser.add_argument(
        "--rounds",
        type=int,
        nargs="+",
        required=True,
        help="Jolpica round numbers to re-ingest (e.g. 20 22 24)",
    )
    args = parser.parse_args()

    log.info("fetching session list", year=args.year)
    all_sessions = endpoints.get_sessions(args.year)

    # OpenF1 labels Sprint races as session_type="Race" too, so sprint weekends have
    # two "Race" and two "Qualifying" sessions per meeting.  In both cases the
    # LATER session in the meeting is always the grid-setting one (main qualifying
    # on Saturday, main race on Sunday).  We pick the latest per meeting before
    # building the ordinal → meeting_key map.

    # Deduplicate: keep only the LATEST Race per meeting to get the main race.
    race_by_meeting: dict[int, object] = {}
    for s in all_sessions:
        if s.session_type != "Race":
            continue
        prev = race_by_meeting.get(s.meeting_key)
        if prev is None or s.date_start > prev.date_start:
            race_by_meeting[s.meeting_key] = s

    main_races = sorted(race_by_meeting.values(), key=lambda s: s.date_start)
    round_to_meeting = {i + 1: s.meeting_key for i, s in enumerate(main_races)}

    # Build meeting_key → qualifying session (main qualifying, not sprint qualifying)
    # When a meeting has multiple "Qualifying" sessions, take the LATER one.
    qual_by_meeting: dict[int, object] = {}
    for s in all_sessions:
        if s.session_type != "Qualifying":
            continue
        prev = qual_by_meeting.get(s.meeting_key)
        if prev is None or s.date_start > prev.date_start:
            qual_by_meeting[s.meeting_key] = s

    target_sessions = []
    for rnd in args.rounds:
        meeting_key = round_to_meeting.get(rnd)
        if meeting_key is None:
            log.warning("round not found in race list", round=rnd, total=len(main_races))
            continue
        session = qual_by_meeting.get(meeting_key)
        if session is None:
            log.warning(
                "no qualifying session found for meeting", round=rnd, meeting_key=meeting_key
            )
            continue
        target_sessions.append(session)
        log.info(
            "found qualifying session",
            round=rnd,
            session_key=session.session_key,
            location=session.location,
            date=session.date_start,
        )

    if not target_sessions:
        log.error("no sessions matched — nothing to do")
        return

    completed = _load_checkpoint()
    for s in target_sessions:
        completed.discard(s.session_key)
    _save_checkpoint(completed)
    log.info("removed sessions from checkpoint", count=len(target_sessions))

    for s in target_sessions:
        log.info("re-ingesting", session_key=s.session_key, location=s.location)
        ok = ingest_session(s.session_key, skip_telemetry=False)
        if ok:
            completed.add(s.session_key)
            _save_checkpoint(completed)
            log.info("done", session_key=s.session_key)
        else:
            log.warning("completed with errors", session_key=s.session_key)


if __name__ == "__main__":
    main()

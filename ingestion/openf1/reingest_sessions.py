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
from ingestion.openf1.backfill import ingest_session

log = structlog.get_logger()


def _latest_per_meeting(sessions: list, session_type: str) -> dict[int, object]:
    """Return {meeting_key: latest session} for sessions matching `session_type`.

    OpenF1 labels Sprint races as session_type="Race" too, so sprint weekends have
    two "Race" and two "Qualifying" sessions per meeting. In both cases the LATER
    session in the meeting is always the grid-setting one (main qualifying on
    Saturday, main race on Sunday), so picking latest-by-date-per-meeting correctly
    resolves to the main session in both cases.
    """
    by_meeting: dict[int, object] = {}
    for s in sessions:
        if s.session_type != session_type:
            continue
        prev = by_meeting.get(s.meeting_key)
        if prev is None or s.date_start > prev.date_start:
            by_meeting[s.meeting_key] = s
    return by_meeting


def resolve_target_sessions(all_sessions: list, rounds: list[int]) -> list:
    """Resolve each requested 1-based round number to its main Qualifying session.

    Round→session mapping strategy:
      - Race sessions have no sprint-qualifying ambiguity, so we sort Race sessions
        by date to establish the 1-based round ordinal.
      - We then find the Qualifying session with the same meeting_key as that Race
        session. This correctly skips sprint qualifying sessions that also carry
        session_type = "Qualifying" in the OpenF1 API.

    Rounds with no matching race or no matching qualifying session are skipped
    (with a warning logged) rather than raising, so a partially-bad --rounds list
    still re-ingests everything it can.
    """
    race_by_meeting = _latest_per_meeting(all_sessions, "Race")
    main_races = sorted(race_by_meeting.values(), key=lambda s: s.date_start)
    round_to_meeting = {i + 1: s.meeting_key for i, s in enumerate(main_races)}

    qual_by_meeting = _latest_per_meeting(all_sessions, "Qualifying")

    target_sessions = []
    for rnd in rounds:
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

    return target_sessions


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

    target_sessions = resolve_target_sessions(all_sessions, args.rounds)

    if not target_sessions:
        log.error("no sessions matched — nothing to do")
        return

    for s in target_sessions:
        log.info("re-ingesting", session_key=s.session_key, location=s.location)
        # force=True: these sessions may already be marked 'ok' in raw_meta.ingestion_log,
        # so we bypass the needs_ingestion skip check to force a re-fetch.
        ok = ingest_session(s.session_key, skip_telemetry=False, force=True)
        if ok:
            log.info("done", session_key=s.session_key)
        else:
            log.warning("completed with errors", session_key=s.session_key)


if __name__ == "__main__":
    main()

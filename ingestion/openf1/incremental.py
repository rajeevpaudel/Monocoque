"""
OpenF1 single session incremental load.

Usage:
    python -m ingestion.openf1.incremental --session-key 9158
    python -m ingestion.openf1.incremental --session-key 9158 --skip-telemetry
"""

import argparse

import structlog

from ingestion.openf1.backfill import ingest_session

log = structlog.get_logger()


def main():
    parser = argparse.ArgumentParser(description="OpenF1 single session load")
    parser.add_argument("--session-key", type=int, required=True)
    parser.add_argument("--skip-telemetry", action="store_true")
    args = parser.parse_args()

    ingest_session(args.session_key, skip_telemetry=args.skip_telemetry)
    log.info("done", session_key=args.session_key)


if __name__ == "__main__":
    main()

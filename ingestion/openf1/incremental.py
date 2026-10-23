"""
OpenF1 single session incremental load.

Usage:
    python -m ingestion.openf1.incremental --session-key 9158
    python -m ingestion.openf1.incremental --session-key 9158 --skip-telemetry
"""

import argparse
import sys

import structlog

from ingestion.openf1.backfill import ingest_session

log = structlog.get_logger()


def main():
    parser = argparse.ArgumentParser(description="OpenF1 single session load")
    parser.add_argument("--session-key", type=int, required=True)
    parser.add_argument("--skip-telemetry", action="store_true")
    args = parser.parse_args()

    ok = ingest_session(args.session_key, skip_telemetry=args.skip_telemetry)
    log.info("done", session_key=args.session_key, ok=ok)
    if not ok:
        sys.exit(1)


if __name__ == "__main__":
    main()

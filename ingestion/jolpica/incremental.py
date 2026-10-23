"""
Jolpica single race weekend incremental load.

Usage:
    python -m ingestion.jolpica.incremental --season 2025 --round 8
"""

import argparse
import sys

from ingestion.jolpica.backfill import ingest_round

__all__ = ["ingest_round"]


def main():
    parser = argparse.ArgumentParser(description="Jolpica single race weekend load")
    parser.add_argument("--season", type=int, required=True)
    parser.add_argument("--round", type=int, required=True)
    args = parser.parse_args()
    if not ingest_round(args.season, args.round):
        sys.exit(1)


if __name__ == "__main__":
    main()

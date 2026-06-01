"""
Build dim.driver_id_map from OpenF1 /drivers cross-referenced with Jolpica race results.

Jolpica race results carry both the car number used that season and the driverId slug,
so we never need a hardcoded table — every season resolves dynamically.

Usage:
    python -m ingestion.openf1.driver_map --year 2023
    python -m ingestion.openf1.driver_map --year 2024
"""

import argparse
from datetime import datetime, timezone

import structlog

from ingestion.jolpica.client import paginate_all
from ingestion.shared import clickhouse as ch

log = structlog.get_logger()


def _jolpica_number_map(year: int) -> dict[int, str]:
    """Return {car_number: jolpica_driver_id} for every driver active that year.

    Race results give the actual number used that season (handles champions on #1).
    Permanent numbers from /{year}/drivers.json fill in practice-only drivers who
    never appear in race results (e.g. reserve drivers).
    """
    # Primary: race results carry the number actually used that season
    races = paginate_all(f"{year}/results", "RaceTable", "Races")
    mapping: dict[int, str] = {}
    for race in races:
        for result in race.get("Results", []):
            number = result.get("number")
            driver_id = result["Driver"]["driverId"]
            if number:
                mapping[int(number)] = driver_id

    # Supplement: permanent numbers for drivers who only appeared in practice/quali
    all_drivers = paginate_all(f"{year}/drivers", "DriverTable", "Drivers")
    for d in all_drivers:
        perm = d.get("permanentNumber")
        driver_id = d["driverId"]
        if perm and int(perm) not in mapping:
            mapping[int(perm)] = driver_id

    return mapping


def build_driver_map(year: int):
    """Build dim.driver_id_map purely from Jolpica data — no OpenF1 calls needed."""
    slog = log.bind(year=year)
    number_map = _jolpica_number_map(year)
    slog.info("jolpica number map built", count=len(number_map))

    rows = [
        {
            "jolpica_driver_id": driver_id,
            "openf1_driver_number": number,
            "season": year,
            "updated_at": datetime.now(timezone.utc),
        }
        for number, driver_id in number_map.items()
    ]

    if rows:
        ch.delete_rows("dim.driver_id_map", f"season = {year}")
        ch.insert_rows("dim.driver_id_map", rows)
        slog.info("driver map inserted", count=len(rows))
    else:
        slog.warning("no driver map rows for year")


def main():
    parser = argparse.ArgumentParser(description="Build dim.driver_id_map for a given year")
    parser.add_argument("--year", type=int, required=True)
    args = parser.parse_args()
    build_driver_map(args.year)


if __name__ == "__main__":
    main()

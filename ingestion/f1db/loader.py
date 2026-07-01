"""
Download and load the f1db CSV dump into raw_f1db.* ClickHouse tables.

f1db (https://github.com/f1db/f1db) covers F1 data 1950–present and is
updated after each race weekend. It uses different driver/constructor IDs
from Jolpica (e.g. 'lewis-hamilton' vs Jolpica's 'hamilton'), so data
lands in raw_f1db.* rather than raw_jolpica.*.

Usage:
    python -m ingestion.f1db.loader                     # latest release
    python -m ingestion.f1db.loader --release v2026.6.1
    python -m ingestion.f1db.loader --only circuits drivers
"""

import argparse
import csv
import io
import re
import urllib.request
import zipfile

import structlog

from ingestion.shared import clickhouse as ch

log = structlog.get_logger()

_RELEASES_URL = "https://github.com/f1db/f1db/releases"
_ZIP_URL = "https://github.com/f1db/f1db/releases/download/{version}/f1db-csv.zip"
_CHUNK = 5_000


def _latest_version() -> str:
    with urllib.request.urlopen(_RELEASES_URL) as r:
        html = r.read().decode()
    m = re.search(r"/f1db/f1db/releases/download/(v[\d.]+)/f1db-csv\.zip", html)
    if not m:
        raise RuntimeError("Could not detect latest f1db release from GitHub releases page")
    return m.group(1)


def _download(version: str) -> zipfile.ZipFile:
    url = _ZIP_URL.format(version=version)
    log.info("downloading f1db CSV dump", url=url)
    with urllib.request.urlopen(url) as r:
        data = r.read()
    log.info("download complete", size_mb=round(len(data) / 1_048_576, 1))
    return zipfile.ZipFile(io.BytesIO(data))


def _int(v: str) -> int | None:
    return int(v) if v else None

def _float(v: str) -> float | None:
    return float(v) if v else None

def _str(v: str) -> str | None:
    return v or None


def _flush(table: str, rows: list[dict], force: bool = False) -> list[dict]:
    if rows and (force or len(rows) >= _CHUNK):
        ch.insert_rows(table, rows)
        return []
    return rows


def _open_csv(zf: zipfile.ZipFile, name: str):
    return csv.DictReader(io.TextIOWrapper(zf.open(name)))


# ── Loaders ──────────────────────────────────────────────────────────────────


def load_seasons(zf: zipfile.ZipFile) -> None:
    rows = [{"year": int(r["year"])} for r in _open_csv(zf, "f1db-seasons.csv")]
    ch.insert_rows("raw_f1db.seasons", rows)
    log.info("loaded", table="seasons", count=len(rows))


def load_circuits(zf: zipfile.ZipFile) -> None:
    rows = []
    for r in _open_csv(zf, "f1db-circuits.csv"):
        rows.append({
            "circuit_id": r["id"],
            "name":       r["name"],
            "place_name": r["placeName"],
            "country_id": r["countryId"],
            "lat":        float(r["latitude"]),
            "lng":        float(r["longitude"]),
            "length_km":  _float(r.get("length", "")),
            "turns":      _int(r.get("turns", "")),
        })
    ch.insert_rows("raw_f1db.circuits", rows)
    log.info("loaded", table="circuits", count=len(rows))


def load_drivers(zf: zipfile.ZipFile) -> None:
    rows = []
    for r in _open_csv(zf, "f1db-drivers.csv"):
        rows.append({
            "driver_id":        r["id"],
            "first_name":       r.get("firstName", ""),
            "last_name":        r.get("lastName", ""),
            "abbreviation":     _str(r.get("abbreviation", "")),
            "permanent_number": _int(r.get("permanentNumber", "")),
            "date_of_birth":    _str(r.get("dateOfBirth", "")),
            "nationality":      r.get("nationalityCountryId", ""),
        })
    ch.insert_rows("raw_f1db.drivers", rows)
    log.info("loaded", table="drivers", count=len(rows))


def load_constructors(zf: zipfile.ZipFile) -> None:
    rows = []
    for r in _open_csv(zf, "f1db-constructors.csv"):
        rows.append({
            "constructor_id": r["id"],
            "name":           r["name"],
            "country_id":     r.get("countryId", ""),
        })
    ch.insert_rows("raw_f1db.constructors", rows)
    log.info("loaded", table="constructors", count=len(rows))


def load_races(zf: zipfile.ZipFile) -> None:
    rows = []
    for r in _open_csv(zf, "f1db-races.csv"):
        rows.append({
            "year":       int(r["year"]),
            "round":      int(r["round"]),
            "race_name":  r["officialName"],
            "circuit_id": r["circuitId"],
            "date":       r["date"],
            "time":       _str(r.get("time", "")),
            "laps":       _int(r.get("laps", "")),
        })
    ch.insert_rows("raw_f1db.races", rows)
    log.info("loaded", table="races", count=len(rows))


def _load_results(zf: zipfile.ZipFile, filename: str, table: str) -> None:
    rows: list[dict] = []
    total = 0
    for r in _open_csv(zf, filename):
        rows.append({
            "year":           int(r["year"]),
            "round":          int(r["round"]),
            "driver_id":      r["driverId"],
            "constructor_id": r["constructorId"],
            "grid":           _int(r.get("gridPositionNumber", "")),
            "position":       _int(r.get("positionNumber", "")),
            "position_text":  r.get("positionText", ""),
            "position_order": int(r.get("positionDisplayOrder") or 0),
            "points":         float(r.get("points") or 0),
            "laps":           _int(r.get("laps", "")),
            "status":         _str(r.get("reasonRetired", "")),
            "time_millis":    _int(r.get("timeMillis", "")),
        })
        if len(rows) >= _CHUNK:
            ch.insert_rows(table, rows)
            total += len(rows)
            rows = []
    if rows:
        ch.insert_rows(table, rows)
        total += len(rows)
    log.info("loaded", table=table, count=total)


def load_race_results(zf: zipfile.ZipFile) -> None:
    _load_results(zf, "f1db-races-race-results.csv", "raw_f1db.race_results")


def load_sprint_race_results(zf: zipfile.ZipFile) -> None:
    _load_results(zf, "f1db-races-sprint-race-results.csv", "raw_f1db.sprint_race_results")


def load_qualifying_results(zf: zipfile.ZipFile) -> None:
    rows: list[dict] = []
    total = 0
    for r in _open_csv(zf, "f1db-races-qualifying-results.csv"):
        rows.append({
            "year":           int(r["year"]),
            "round":          int(r["round"]),
            "driver_id":      r["driverId"],
            "constructor_id": r["constructorId"],
            "position":       _int(r.get("positionNumber", "")),
            "q1":             _str(r.get("q1", "")),
            "q1_millis":      _int(r.get("q1Millis", "")),
            "q2":             _str(r.get("q2", "")),
            "q2_millis":      _int(r.get("q2Millis", "")),
            "q3":             _str(r.get("q3", "")),
            "q3_millis":      _int(r.get("q3Millis", "")),
            "time_millis":    _int(r.get("timeMillis", "")),
        })
        if len(rows) >= _CHUNK:
            ch.insert_rows("raw_f1db.qualifying_results", rows)
            total += len(rows)
            rows = []
    if rows:
        ch.insert_rows("raw_f1db.qualifying_results", rows)
        total += len(rows)
    log.info("loaded", table="qualifying_results", count=total)


def load_pit_stops(zf: zipfile.ZipFile) -> None:
    rows: list[dict] = []
    total = 0
    for r in _open_csv(zf, "f1db-races-pit-stops.csv"):
        rows.append({
            "year":        int(r["year"]),
            "round":       int(r["round"]),
            "driver_id":   r["driverId"],
            "stop":        int(r.get("stop") or 0),
            "lap":         int(r.get("lap") or 0),
            "time":        r.get("time", ""),
            "time_millis": _int(r.get("timeMillis", "")),
        })
        if len(rows) >= _CHUNK:
            ch.insert_rows("raw_f1db.pit_stops", rows)
            total += len(rows)
            rows = []
    if rows:
        ch.insert_rows("raw_f1db.pit_stops", rows)
        total += len(rows)
    log.info("loaded", table="pit_stops", count=total)


def load_fastest_laps(zf: zipfile.ZipFile) -> None:
    rows: list[dict] = []
    total = 0
    for r in _open_csv(zf, "f1db-races-fastest-laps.csv"):
        rows.append({
            "year":           int(r["year"]),
            "round":          int(r["round"]),
            "driver_id":      r["driverId"],
            "constructor_id": r["constructorId"],
            "position":       _int(r.get("positionNumber", "")),
            "lap":            _int(r.get("lap", "")),
            "time":           _str(r.get("time", "")),
            "time_millis":    _int(r.get("timeMillis", "")),
        })
        if len(rows) >= _CHUNK:
            ch.insert_rows("raw_f1db.fastest_laps", rows)
            total += len(rows)
            rows = []
    if rows:
        ch.insert_rows("raw_f1db.fastest_laps", rows)
        total += len(rows)
    log.info("loaded", table="fastest_laps", count=total)


def load_driver_standings(zf: zipfile.ZipFile) -> None:
    rows: list[dict] = []
    total = 0
    for r in _open_csv(zf, "f1db-races-driver-standings.csv"):
        rows.append({
            "year":          int(r["year"]),
            "round":         int(r["round"]),
            "driver_id":     r["driverId"],
            "position":      _int(r.get("positionNumber", "")),
            "position_text": r.get("positionText", ""),
            "points":        float(r.get("points") or 0),
        })
        if len(rows) >= _CHUNK:
            ch.insert_rows("raw_f1db.driver_standings", rows)
            total += len(rows)
            rows = []
    if rows:
        ch.insert_rows("raw_f1db.driver_standings", rows)
        total += len(rows)
    log.info("loaded", table="driver_standings", count=total)


def load_constructor_standings(zf: zipfile.ZipFile) -> None:
    rows: list[dict] = []
    total = 0
    for r in _open_csv(zf, "f1db-races-constructor-standings.csv"):
        rows.append({
            "year":           int(r["year"]),
            "round":          int(r["round"]),
            "constructor_id": r["constructorId"],
            "position":       _int(r.get("positionNumber", "")),
            "position_text":  r.get("positionText", ""),
            "points":         float(r.get("points") or 0),
        })
        if len(rows) >= _CHUNK:
            ch.insert_rows("raw_f1db.constructor_standings", rows)
            total += len(rows)
            rows = []
    if rows:
        ch.insert_rows("raw_f1db.constructor_standings", rows)
        total += len(rows)
    log.info("loaded", table="constructor_standings", count=total)


# ── Registry ─────────────────────────────────────────────────────────────────

LOADERS: list[tuple[str, callable]] = [
    ("seasons",               load_seasons),
    ("circuits",              load_circuits),
    ("drivers",               load_drivers),
    ("constructors",          load_constructors),
    ("races",                 load_races),
    ("race_results",          load_race_results),
    ("qualifying_results",    load_qualifying_results),
    ("sprint_race_results",   load_sprint_race_results),
    ("pit_stops",             load_pit_stops),
    ("fastest_laps",          load_fastest_laps),
    ("driver_standings",      load_driver_standings),
    ("constructor_standings", load_constructor_standings),
]


# ── CLI ───────────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(description="Load f1db CSV dump into raw_f1db.* tables")
    parser.add_argument("--release", help="f1db release tag (e.g. v2026.6.1); defaults to latest")
    parser.add_argument(
        "--only", nargs="+", metavar="TABLE",
        choices=[name for name, _ in LOADERS],
        help="Load only these tables",
    )
    args = parser.parse_args()

    version = args.release or _latest_version()
    log.info("f1db load starting", version=version)

    zf = _download(version)

    for name, fn in LOADERS:
        if args.only and name not in args.only:
            continue
        fn(zf)

    log.info("f1db load complete", version=version)


if __name__ == "__main__":
    main()

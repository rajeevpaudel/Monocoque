"""Export / load a small, real slice of the raw warehouse for CI dbt builds.

The sample covers two 2024 weekends (Bahrain — a normal weekend, China — a sprint weekend)
from Jolpica + OpenF1, the same rounds from f1db (so the f1db↔Jolpica crosswalks have races
to pair), and the opening 2026 round from f1db only (exercises the f1db-only season branch).

    python scripts/sample_data.py export   # from a populated local warehouse → fixtures
    python scripts/sample_data.py load     # into an empty, migrated ClickHouse (CI)
"""

import argparse
import gzip
import os
from pathlib import Path

import clickhouse_connect

FIXTURE_DIR = Path(__file__).resolve().parent.parent / "tests" / "fixtures" / "warehouse_sample"

SEASON = 2024
ROUNDS = (1, 5)
F1DB_ONLY = (2026, 1)

_rounds = ", ".join(str(r) for r in ROUNDS)
_jolpica_rounds = f"season = {SEASON} AND round IN ({_rounds})"
_f1db_rounds = (
    f"(year = {SEASON} AND round IN ({_rounds})) "
    f"OR (year = {F1DB_ONLY[0]} AND round = {F1DB_ONLY[1]})"
)
_openf1_sessions = (
    "session_key IN (SELECT session_key FROM raw_openf1.sessions "
    "WHERE meeting_key IN (SELECT meeting_key FROM raw_openf1.sessions "
    f"WHERE year = {SEASON} AND session_name = 'Race' AND substring(date_start, 1, 10) IN "
    f"(SELECT toString(date) FROM raw_jolpica.races WHERE {_jolpica_rounds})))"
)

# table -> WHERE clause (None = whole table; reference tables are small)
TABLES: dict[str, str | None] = {
    "raw_jolpica.seasons": None,
    "raw_jolpica.circuits": None,
    "raw_jolpica.drivers": None,
    "raw_jolpica.constructors": None,
    "raw_jolpica.races": _jolpica_rounds,
    "raw_jolpica.results": _jolpica_rounds,
    "raw_jolpica.sprint_results": _jolpica_rounds,
    "raw_jolpica.qualifying": _jolpica_rounds,
    "raw_jolpica.driver_standings": _jolpica_rounds,
    "raw_jolpica.constructor_standings": _jolpica_rounds,
    "raw_jolpica.pit_stops": _jolpica_rounds,
    "raw_jolpica.lap_times": _jolpica_rounds,
    "raw_f1db.seasons": None,
    "raw_f1db.circuits": None,
    "raw_f1db.drivers": None,
    "raw_f1db.constructors": None,
    "raw_f1db.races": _f1db_rounds,
    "raw_f1db.race_results": _f1db_rounds,
    "raw_f1db.sprint_race_results": _f1db_rounds,
    "raw_f1db.qualifying_results": _f1db_rounds,
    "raw_f1db.driver_standings": _f1db_rounds,
    "raw_f1db.constructor_standings": _f1db_rounds,
    "raw_f1db.pit_stops": _f1db_rounds,
    "raw_f1db.fastest_laps": _f1db_rounds,
    "raw_openf1.sessions": _openf1_sessions,
    "raw_openf1.drivers": _openf1_sessions,
    "raw_openf1.laps": _openf1_sessions,
    "raw_openf1.stints": _openf1_sessions,
    "raw_openf1.pit": _openf1_sessions,
    "raw_openf1.weather": _openf1_sessions,
    "raw_openf1.race_control": _openf1_sessions,
    "raw_openf1.intervals": _openf1_sessions,
    "raw_openf1.position": _openf1_sessions,
}


def _client():
    return clickhouse_connect.get_client(
        host=os.environ.get("CLICKHOUSE_HOST", "localhost"),
        port=int(os.environ.get("CLICKHOUSE_PORT", 8123)),
        username=os.environ.get("CLICKHOUSE_USER", "default"),
        password=os.environ.get("CLICKHOUSE_PASSWORD", ""),
    )


def _rows(data: bytes) -> int:
    return data.count(b"\n")


def _path(table: str) -> Path:
    return FIXTURE_DIR / f"{table}.jsonl.gz"


def export() -> None:
    client = _client()
    FIXTURE_DIR.mkdir(parents=True, exist_ok=True)
    for table, where in TABLES.items():
        sql = f"SELECT * FROM {table}" + (f" WHERE {where}" if where else "")
        data = client.raw_query(f"{sql} FORMAT JSONEachRow")
        with gzip.open(_path(table), "wb", compresslevel=9) as fh:
            fh.write(data)
        print(f"{table}: {_rows(data)} rows")


def load() -> None:
    client = _client()
    for table in TABLES:
        path = _path(table)
        if not path.exists():
            print(f"{table}: no fixture, skipped")
            continue
        data = gzip.decompress(path.read_bytes())
        if data:
            client.raw_insert(table, insert_block=data, fmt="JSONEachRow")
        print(f"{table}: {_rows(data)} rows")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("action", choices=["export", "load"])
    {"export": export, "load": load}[parser.parse_args().action]()

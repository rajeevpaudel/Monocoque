"""
Per-(entity, table) ingestion outcome tracking in raw_meta.ingestion_log.

entity_key format:
  OpenF1 sessions : str(session_key)   e.g. "9900"
  Jolpica rounds  : "season-round"     e.g. "2025-4"
"""
from datetime import datetime, timezone

from ingestion.shared import clickhouse as ch

SOURCE_OPENF1 = "openf1"
SOURCE_JOLPICA = "jolpica"

# Minimum rows required to consider a (entity, table) ingestion complete.
# 0 = optional table; zero rows is acceptable (e.g. pit stops on qualifying sessions).
_MIN_ROWS: dict[str, int] = {
    "raw_openf1.laps": 50,
    "raw_openf1.car_data": 1000,
    "raw_openf1.location": 1000,
    "raw_openf1.stints": 10,
    "raw_openf1.pit": 0,
    "raw_openf1.intervals": 0,
    "raw_openf1.weather": 5,
    "raw_openf1.race_control": 3,
    "raw_openf1.drivers": 10,
    "raw_jolpica.qualifying": 15,
    "raw_jolpica.results": 10,
    "raw_jolpica.races": 1,
    "raw_jolpica.sprint_results": 0,
    "raw_jolpica.lap_times": 0,
    "raw_jolpica.pit_stops": 0,
    "raw_jolpica.driver_standings": 1,
    "raw_jolpica.constructor_standings": 1,
}


def _status(table: str, row_count: int, error_msg: str) -> str:
    if error_msg:
        return "failed"
    min_rows = _MIN_ROWS.get(table, 0)
    if row_count == 0:
        return "empty" if min_rows == 0 else "incomplete"
    if row_count < min_rows:
        return "incomplete"
    return "ok"


def record(
    source: str,
    entity_key: str,
    table: str,
    row_count: int,
    error_msg: str = "",
) -> None:
    """Record the outcome of ingesting one (entity, table) pair."""
    ch.insert_rows("raw_meta.ingestion_log", [{
        "source": source,
        "entity_key": entity_key,
        "table_name": table,
        "status": _status(table, row_count, error_msg),
        "row_count": row_count,
        "min_expected": _MIN_ROWS.get(table, 0),
        "error_msg": error_msg,
        "attempted_at": datetime.now(timezone.utc),
    }])


def needs_ingestion(source: str, entity_key: str, table: str) -> bool:
    """
    Return True if this (entity, table) should be ingested or re-ingested.

    True when: never logged (absent = needs work) OR latest status is 'failed'/'incomplete'.
    False when: latest status is 'ok' or 'empty'.
    """
    latest_status = ch.query_one(
        f"SELECT argMax(status, attempted_at) FROM raw_meta.ingestion_log "
        f"WHERE source = '{source}' AND entity_key = '{entity_key}' "
        f"AND table_name = '{table}'"
    )
    return latest_status not in ("ok", "empty")

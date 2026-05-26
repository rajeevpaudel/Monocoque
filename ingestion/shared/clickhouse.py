"""ClickHouse client wrapper using clickhouse-connect with pyarrow bulk inserts."""

import os
from typing import Any

import clickhouse_connect
import pyarrow as pa
import structlog

log = structlog.get_logger()


def _get_client():
    return clickhouse_connect.get_client(
        host=os.environ.get("CLICKHOUSE_HOST", "localhost"),
        port=int(os.environ.get("CLICKHOUSE_PORT", 8123)),
        username=os.environ.get("CLICKHOUSE_USER", "default"),
        password=os.environ.get("CLICKHOUSE_PASSWORD", ""),
        database=os.environ.get("CLICKHOUSE_DATABASE", "default"),
    )


# Module-level client (re-created on import; fine for single-process use)
_client = None


def get_client():
    global _client
    if _client is None:
        _client = _get_client()
    return _client


def insert_rows(table: str, rows: list[dict[str, Any]]) -> None:
    """Insert a list of dicts into a ClickHouse table."""
    if not rows:
        return
    client = get_client()
    columns = list(rows[0].keys())
    data = [[row[col] for col in columns] for row in rows]
    client.insert(table, data, column_names=columns)
    log.info("inserted rows", table=table, count=len(rows))


def insert_arrow(table: str, arrow_table: pa.Table) -> None:
    """Bulk-insert a PyArrow table into ClickHouse — preferred for large payloads."""
    if arrow_table.num_rows == 0:
        return
    client = get_client()
    client.insert_arrow(table, arrow_table)
    log.info("inserted arrow batch", table=table, rows=arrow_table.num_rows)


def query_one(sql: str) -> Any:
    """Return the first value of the first row."""
    result = get_client().query(sql)
    return result.first_item

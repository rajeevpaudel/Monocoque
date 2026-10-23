"""ClickHouse client wrapper using clickhouse-connect with pyarrow bulk inserts."""

import os
import threading
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
_client_lock = threading.Lock()


def get_client():
    global _client
    if _client is None:
        with _client_lock:
            if _client is None:  # re-check: another thread may have built it while we waited
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


def delete_rows(table: str, where: str, parameters: dict[str, Any] | None = None) -> None:
    """Delete rows matching a WHERE clause (lightweight delete, ClickHouse 23.3+).

    `where` may contain ClickHouse parameter placeholders (e.g. `season = {season:UInt16}`)
    bound via `parameters` — callers should always prefer this over interpolating values
    directly into `where`, since `table` and `where` are otherwise combined into the SQL
    string as-is with no escaping.
    """
    get_client().command(f"DELETE FROM {table} WHERE {where}", parameters=parameters or {})
    log.info("deleted rows", table=table, where=where, parameters=parameters or {})


def truncate_table(table: str) -> None:
    """Empty a table entirely — for full-dump sources with no natural delete key."""
    get_client().command(f"TRUNCATE TABLE IF EXISTS {table}")
    log.info("truncated table", table=table)


def atomic_reload(table: str, rows: list[dict[str, Any]], chunk_size: int = 5_000) -> int:
    """Replace a full-dump table's contents without a truncate-then-insert window where a
    mid-load failure leaves it empty or half-loaded.

    Loads into a shadow table, then atomically swaps it in with EXCHANGE TABLES (ClickHouse's
    Atomic database engine, the default since 20.x) only once every chunk has inserted
    successfully. On any failure, the shadow table is dropped and `table` is left untouched —
    a real rollback, unlike `truncate_table` + `insert_rows` in a loop.
    """
    client = get_client()
    shadow = f"{table}_reload_staging"
    client.command(f"DROP TABLE IF EXISTS {shadow}")
    client.command(f"CREATE TABLE {shadow} AS {table}")
    try:
        total = 0
        for i in range(0, len(rows), chunk_size):
            chunk = rows[i : i + chunk_size]
            insert_rows(shadow, chunk)
            total += len(chunk)
        client.command(f"EXCHANGE TABLES {table} AND {shadow}")
    except Exception:
        client.command(f"DROP TABLE IF EXISTS {shadow}")
        raise
    else:
        client.command(f"DROP TABLE IF EXISTS {shadow}")
        log.info("atomically reloaded table", table=table, count=total)
        return total


def query_one(sql: str, parameters: dict | None = None) -> Any:
    """Return the first value of the first row."""
    result = get_client().query(sql, parameters=parameters or {})
    rows = result.result_rows
    return rows[0][0] if rows else None

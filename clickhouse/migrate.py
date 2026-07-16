"""Run ClickHouse migrations in order, skipping any already applied.

Applied migrations are tracked in raw_meta.schema_migrations (filename +
applied_at). Statements within a file are split respecting quoted string
literals, so a semicolon inside a string (e.g. a default value) doesn't
truncate a statement.
"""

import os
from pathlib import Path

import clickhouse_connect

MIGRATIONS_DIR = Path(__file__).parent / "migrations"

_TRACKING_DB = "raw_meta"
_TRACKING_TABLE = f"{_TRACKING_DB}.schema_migrations"


def get_client():
    return clickhouse_connect.get_client(
        host=os.environ.get("CLICKHOUSE_HOST", "localhost"),
        port=int(os.environ.get("CLICKHOUSE_PORT", 8123)),
        username=os.environ.get("CLICKHOUSE_USER", "default"),
        password=os.environ.get("CLICKHOUSE_PASSWORD", ""),
    )


def _ensure_tracking_table(client) -> None:
    client.command(f"CREATE DATABASE IF NOT EXISTS {_TRACKING_DB}")
    client.command(f"""
        CREATE TABLE IF NOT EXISTS {_TRACKING_TABLE} (
            filename    String,
            applied_at  DateTime DEFAULT now()
        ) ENGINE = MergeTree()
        ORDER BY filename
    """)


def _applied_filenames(client) -> set[str]:
    rows = client.query(f"SELECT DISTINCT filename FROM {_TRACKING_TABLE}").result_rows
    return {r[0] for r in rows}


def _mark_applied(client, filename: str) -> None:
    client.insert(_TRACKING_TABLE, [[filename]], column_names=["filename"])


def split_statements(sql: str) -> list[str]:
    """Split a SQL file into statements on top-level semicolons.

    Tracks single/double-quote state and `--` line comments so a semicolon
    inside a string literal or a comment doesn't split a statement.
    """
    statements = []
    buf = []
    quote_char = None
    in_line_comment = False
    i = 0
    n = len(sql)
    while i < n:
        ch = sql[i]
        if in_line_comment:
            buf.append(ch)
            if ch == "\n":
                in_line_comment = False
            i += 1
            continue
        if quote_char:
            buf.append(ch)
            if ch == quote_char:
                # handle doubled-quote escaping ('' inside a '...' string)
                if i + 1 < n and sql[i + 1] == quote_char:
                    buf.append(sql[i + 1])
                    i += 2
                    continue
                quote_char = None
            i += 1
            continue
        if ch in ("'", '"'):
            quote_char = ch
            buf.append(ch)
            i += 1
            continue
        if ch == "-" and i + 1 < n and sql[i + 1] == "-":
            in_line_comment = True
            buf.append(ch)
            i += 1
            continue
        if ch == ";":
            statements.append("".join(buf))
            buf = []
            i += 1
            continue
        buf.append(ch)
        i += 1
    if buf:
        statements.append("".join(buf))
    return statements


def run_migrations():
    client = get_client()
    _ensure_tracking_table(client)
    applied = _applied_filenames(client)

    migration_files = sorted(MIGRATIONS_DIR.glob("*.sql"))
    if not migration_files:
        print("No migration files found.")
        return

    applied_count = 0
    skipped_count = 0
    for path in migration_files:
        if path.name in applied:
            print(f"Skipping {path.name} (already applied).")
            skipped_count += 1
            continue

        print(f"Applying {path.name}...")
        sql = path.read_text()
        for statement in split_statements(sql):
            lines = [ln for ln in statement.splitlines() if not ln.strip().startswith("--")]
            clean = "\n".join(lines).strip()
            if clean:
                client.command(clean)
        _mark_applied(client, path.name)
        print("  Done.")
        applied_count += 1

    print(f"\nApplied {applied_count} migration(s), skipped {skipped_count} already-applied.")


if __name__ == "__main__":
    run_migrations()

"""Run ClickHouse migrations in order, skipping any already applied.

Applied migrations are tracked in raw_meta.schema_migrations (filename +
applied_at). Statements within a file are split respecting quoted string
literals, so a semicolon inside a string (e.g. a default value) doesn't
truncate a statement.
"""

import hashlib
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
            checksum    String,
            applied_at  DateTime DEFAULT now()
        ) ENGINE = MergeTree()
        ORDER BY filename
    """)
    # Backfill the column for tables created before checksum tracking existed.
    client.command(f"ALTER TABLE {_TRACKING_TABLE} ADD COLUMN IF NOT EXISTS checksum String DEFAULT ''")


def _file_checksum(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _applied_checksums(client) -> dict[str, str]:
    rows = client.query(
        f"SELECT filename, argMax(checksum, applied_at) FROM {_TRACKING_TABLE} GROUP BY filename"
    ).result_rows
    return {r[0]: r[1] for r in rows}


def _mark_applied(client, filename: str, checksum: str) -> None:
    client.insert(_TRACKING_TABLE, [[filename, checksum]], column_names=["filename", "checksum"])


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
    applied = _applied_checksums(client)

    migration_files = sorted(MIGRATIONS_DIR.glob("*.sql"))
    if not migration_files:
        print("No migration files found.")
        return

    applied_count = 0
    skipped_count = 0
    for path in migration_files:
        checksum = _file_checksum(path)
        if path.name in applied:
            if applied[path.name] and applied[path.name] != checksum:
                print(
                    f"WARNING: {path.name} was modified after being applied "
                    f"(checksum mismatch) — not re-running automatically. "
                    f"If this change needs to reach this environment, write a new "
                    f"migration file instead of editing an applied one."
                )
            else:
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
        _mark_applied(client, path.name, checksum)
        print("  Done.")
        applied_count += 1

    print(f"\nApplied {applied_count} migration(s), skipped {skipped_count} already-applied.")


if __name__ == "__main__":
    run_migrations()

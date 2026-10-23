"""Run ClickHouse migrations in order, skipping any already applied.

Applied migrations are tracked in raw_meta.schema_migrations (filename +
applied_at). Statements within a file are split respecting quoted string
literals, so a semicolon inside a string (e.g. a default value) doesn't
truncate a statement.
"""

import fcntl
import hashlib
import os
from pathlib import Path

import clickhouse_connect

MIGRATIONS_DIR = Path(__file__).parent / "migrations"
_LOCK_PATH = Path(__file__).parent / ".migrate.lock"

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
    client.command(
        f"ALTER TABLE {_TRACKING_TABLE} ADD COLUMN IF NOT EXISTS checksum String DEFAULT ''"
    )


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

    Tracks single/double-quote state, `--` line comments, and `/* */` block
    comments so a semicolon inside a string literal or a comment doesn't
    split a statement.
    """
    statements = []
    buf = []
    quote_char = None
    in_line_comment = False
    in_block_comment = False
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
        if in_block_comment:
            buf.append(ch)
            if ch == "*" and i + 1 < n and sql[i + 1] == "/":
                buf.append(sql[i + 1])
                i += 2
                in_block_comment = False
                continue
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
        if ch == "/" and i + 1 < n and sql[i + 1] == "*":
            in_block_comment = True
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


def _strip_comments(statement: str) -> str:
    """Remove `--` line comments and `/* */` block comments from a statement
    body, leaving quoted-string contents untouched (reuses split_statements'
    quote/comment tracking by scanning char-by-char the same way)."""
    out = []
    quote_char = None
    in_line_comment = False
    in_block_comment = False
    i = 0
    n = len(statement)
    while i < n:
        ch = statement[i]
        if in_line_comment:
            if ch == "\n":
                in_line_comment = False
                out.append(ch)
            i += 1
            continue
        if in_block_comment:
            if ch == "*" and i + 1 < n and statement[i + 1] == "/":
                i += 2
                in_block_comment = False
                continue
            i += 1
            continue
        if quote_char:
            out.append(ch)
            if ch == quote_char:
                if i + 1 < n and statement[i + 1] == quote_char:
                    out.append(statement[i + 1])
                    i += 2
                    continue
                quote_char = None
            i += 1
            continue
        if ch in ("'", '"'):
            quote_char = ch
            out.append(ch)
            i += 1
            continue
        if ch == "-" and i + 1 < n and statement[i + 1] == "-":
            in_line_comment = True
            i += 1
            continue
        if ch == "/" and i + 1 < n and statement[i + 1] == "*":
            in_block_comment = True
            i += 1
            continue
        out.append(ch)
        i += 1
    return "".join(out)


def run_migrations():
    # Exclusive, non-blocking lock: two concurrent runs (e.g. overlapping deploys)
    # would otherwise both see the same file as unapplied and double-apply it,
    # since the check-then-apply sequence against ClickHouse has no transaction
    # to make it atomic. Only guards against concurrent runs on the same host —
    # add a distributed lock if migrations ever run from multiple hosts at once.
    with open(_LOCK_PATH, "w") as lock_file:
        try:
            fcntl.flock(lock_file, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as e:
            raise SystemExit(
                "Another migrate.py run holds the lock — refusing to run concurrently."
            ) from e
        try:
            _run_migrations_locked()
        finally:
            fcntl.flock(lock_file, fcntl.LOCK_UN)


def _run_migrations_locked():
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
            clean = _strip_comments(statement).strip()
            if clean:
                client.command(clean)
        _mark_applied(client, path.name, checksum)
        print("  Done.")
        applied_count += 1

    print(f"\nApplied {applied_count} migration(s), skipped {skipped_count} already-applied.")


if __name__ == "__main__":
    run_migrations()

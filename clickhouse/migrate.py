"""Run ClickHouse migrations in order. Idempotent — safe to re-run."""

import os
from pathlib import Path

import clickhouse_connect

MIGRATIONS_DIR = Path(__file__).parent / "migrations"


def get_client():
    return clickhouse_connect.get_client(
        host=os.environ.get("CLICKHOUSE_HOST", "localhost"),
        port=int(os.environ.get("CLICKHOUSE_PORT", 8123)),
        username=os.environ.get("CLICKHOUSE_USER", "default"),
        password=os.environ.get("CLICKHOUSE_PASSWORD", ""),
    )


def run_migrations():
    client = get_client()
    migration_files = sorted(MIGRATIONS_DIR.glob("*.sql"))

    if not migration_files:
        print("No migration files found.")
        return

    for path in migration_files:
        print(f"Applying {path.name}...")
        sql = path.read_text()
        for statement in sql.split(";"):
            # Strip comment lines then check if anything remains
            lines = [l for l in statement.splitlines() if not l.strip().startswith("--")]
            clean = "\n".join(lines).strip()
            if clean:
                client.command(clean)
        print("  Done.")

    print(f"\nApplied {len(migration_files)} migration(s) successfully.")


if __name__ == "__main__":
    run_migrations()

.PHONY: setup up down build-up fix-perms migrate reset-data clear-raw clear-all \
        backfill-jolpica backfill-openf1 backfill-fastf1 load-f1db ingest-year \
        dbt-deps dbt-seed dbt-run dbt-test dbt-build dbt-docs dbt-docs-serve \
        metabase-dashboards sample-export sample-load lint format test check

VENV    := .venv
PY      := $(VENV)/bin/python
DBT     := cd dbt && PATH="$(CURDIR)/$(VENV)/bin:$$PATH" dbt
CH      := docker exec monocoque-clickhouse-1 clickhouse-client
RUN     := PYTHONPATH=. $(PY)

# ── Setup ─────────────────────────────────────────────────────────────────────

setup: _ensure-airflow-uid
	@echo "--- Creating virtual environment ($(VENV)) ---"
	@echo "--- Installing dependencies (pinned dbt via .[dbt]) ---"
	@if command -v uv >/dev/null 2>&1; then \
		test -d $(VENV) || uv venv -q $(VENV); \
		uv pip install -q --python $(PY) -e ".[dev,dbt]"; \
	else \
		test -d $(VENV) || python3 -m venv $(VENV); \
		$(PY) -m pip --version >/dev/null 2>&1 || $(PY) -m ensurepip >/dev/null; \
		$(PY) -m pip install -q -e ".[dev,dbt]"; \
	fi
	$(VENV)/bin/pre-commit install
	@echo "--- Starting Docker services ---"
	docker compose up -d
	$(MAKE) fix-perms
	@echo "--- Waiting for ClickHouse to be ready ---"
	@until $(CH) --query "SELECT 1" > /dev/null 2>&1; do \
		echo "  ClickHouse not ready yet, retrying..."; sleep 2; \
	done
	$(MAKE) migrate dbt-deps
	@echo ""
	@echo "Setup complete. Next steps:"
	@echo "  make load-f1db               # 1950 → current season history"
	@echo "  make ingest-year YEAR=2024   # Jolpica + OpenF1 for a season, then dbt"

up: _ensure-airflow-uid
	docker compose up -d
	$(MAKE) fix-perms

build-up: _ensure-airflow-uid
	docker compose up -d --build
	$(MAKE) fix-perms

down:
	docker compose down

# Airflow runs as AIRFLOW_UID. When that matches your host UID (the default written by
# _ensure-airflow-uid) bind-mounted dirs are already writable; otherwise open up only the
# generated directories — never the dbt project itself, and no sudo.
fix-perms:
	@mkdir -p dbt/dbt_packages dbt/target dbt/logs airflow/logs
	@if [ "$$(grep '^AIRFLOW_UID=' .env | cut -d= -f2)" != "$$(id -u)" ]; then \
		chmod -R a+rwX dbt/dbt_packages dbt/target dbt/logs airflow/logs 2>/dev/null || \
		echo "WARN: could not chmod generated dirs; set AIRFLOW_UID=$$(id -u) in .env and 'make up'"; \
	fi

_ensure-airflow-uid:
	@test -f .env || cp .env.example .env
	@grep -q "^AIRFLOW_UID=" .env || echo "AIRFLOW_UID=$$(id -u)" >> .env

# ── ClickHouse ────────────────────────────────────────────────────────────────

migrate:
	$(PY) clickhouse/migrate.py

# Drops every raw/dim/dbt database *and* raw_meta (which tracks applied migrations), then
# re-applies migrations from scratch.
reset-data:
	@echo "Dropping all databases..."
	@for db in raw_jolpica raw_openf1 raw_f1db raw_fastf1 raw_meta dim f1_staging f1_intermediate f1_mart; do \
		$(CH) --query "DROP DATABASE IF EXISTS $$db"; echo "  dropped $$db"; \
	done
	$(MAKE) migrate
	@echo "Done. All data cleared."

# Truncates every raw table (discovered from system.tables, so new tables are never missed)
# and the ingestion log; keeps schema_migrations.
clear-raw:
	@echo "Truncating all raw tables and resetting ingestion log..."
	@for table in $$($(CH) --query "SELECT database || '.' || name FROM system.tables \
		WHERE database IN ('raw_jolpica','raw_openf1','raw_f1db','raw_fastf1') \
		   OR (database = 'raw_meta' AND name = 'ingestion_log') FORMAT TSV"); do \
		$(CH) --query "TRUNCATE TABLE IF EXISTS $$table"; echo "  cleared $$table"; \
	done
	@echo "Done. Raw tables cleared — ready to re-ingest."

clear-all: clear-raw
	@echo "Dropping dbt output databases..."
	@for db in f1_staging f1_intermediate f1_mart; do \
		$(CH) --query "DROP DATABASE IF EXISTS $$db"; echo "  dropped $$db"; \
	done
	@echo "Done. Run 'make dbt-build' to rebuild mart tables."

# ── Ingestion ─────────────────────────────────────────────────────────────────
# All ingestion CLIs exit non-zero when any table/session failed (it is retried next run).

backfill-jolpica:
	$(RUN) ingestion/jolpica/backfill.py --start $(START) --end $(END)

backfill-openf1:
	$(RUN) ingestion/openf1/backfill.py --start $(START) --end $(END) $(if $(SKIP_TELEMETRY),--skip-telemetry,)

# Needs a one-time `$(PY) -m fastf1 auth f1tv` (F1's livetiming archive returns 401 otherwise).
backfill-fastf1:
	$(RUN) ingestion/fastf1/backfill.py --start $(START) --end $(END) $(if $(ROUNDS),--rounds $(ROUNDS),)

load-f1db:
	$(RUN) -m ingestion.f1db.loader

# Ingest one season from Jolpica + OpenF1 in parallel, then build dbt:
#   make ingest-year YEAR=2023 [SKIP_TELEMETRY=1]
ingest-year:
	@test -n "$(YEAR)" || (echo "ERROR: YEAR is required. Usage: make ingest-year YEAR=2023"; exit 1)
	@trap 'kill 0' INT TERM; \
	$(RUN) ingestion/jolpica/backfill.py --start $(YEAR) --end $(YEAR) & j=$$!; \
	$(RUN) ingestion/openf1/backfill.py --start $(YEAR) --end $(YEAR) $(if $(SKIP_TELEMETRY),--skip-telemetry,) & o=$$!; \
	rc=0; wait $$j || rc=1; wait $$o || rc=1; \
	if [ $$rc -ne 0 ]; then echo "WARN: some tables failed to load (see logs); building dbt with what loaded"; fi
	$(MAKE) dbt-build

# ── dbt ───────────────────────────────────────────────────────────────────────

dbt-deps:
	$(DBT) deps --profiles-dir .

dbt-seed:
	$(DBT) seed --profiles-dir .

dbt-run: dbt-seed
	$(DBT) run --profiles-dir .

dbt-test:
	$(DBT) test --profiles-dir . --exclude package:elementary

dbt-build: dbt-seed
	$(DBT) build --profiles-dir . --exclude package:elementary

dbt-docs:
	$(DBT) docs generate --profiles-dir .

dbt-docs-serve: dbt-docs
	@echo "Opening dbt docs at http://localhost:8081 (the dbt-docs container serves :8082)"
	$(DBT) docs serve --profiles-dir . --port 8081

# ── Metabase / CI sample ──────────────────────────────────────────────────────

# Requires MB_ADMIN_PASSWORD; see metabase/setup_dashboards.py for other env vars.
metabase-dashboards:
	$(PY) metabase/setup_dashboards.py

sample-export:
	$(PY) scripts/sample_data.py export

sample-load:
	$(PY) scripts/sample_data.py load

# ── Quality ───────────────────────────────────────────────────────────────────

lint:
	$(VENV)/bin/ruff check .
	$(VENV)/bin/ruff format --check .

format:
	$(VENV)/bin/ruff check --fix .
	$(VENV)/bin/ruff format .

test:
	$(VENV)/bin/pytest -q

check: lint test

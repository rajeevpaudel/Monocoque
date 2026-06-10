.PHONY: up down build-up fix-perms migrate reset-data clear-raw clear-all backfill-jolpica backfill-openf1 ingest-year dbt-run dbt-test dbt-docs dbt-docs-serve lint format test setup

setup: _ensure-airflow-uid
	@echo "--- Creating virtual environment ---"
	@test -d venv || python3 -m venv venv
	@echo "--- Installing dependencies ---"
	venv/bin/pip install -q -e ".[dev]"
	venv/bin/pip install -q dbt-clickhouse
	@echo "--- Starting Docker services ---"
	docker compose up -d
	$(MAKE) fix-perms
	@echo "--- Waiting for ClickHouse to be ready ---"
	@until docker exec monocoque-clickhouse-1 clickhouse-client --query "SELECT 1" > /dev/null 2>&1; do \
		echo "  ClickHouse not ready yet, retrying..."; sleep 2; \
	done
	@echo "--- Applying migrations ---"
	venv/bin/python clickhouse/migrate.py
	@echo "--- Installing dbt packages ---"
	cd dbt && PATH="$(CURDIR)/venv/bin:$$PATH" dbt deps --profiles-dir .
	@echo ""
	@echo "Setup complete. Next steps:"
	@echo "  make ingest-year YEAR=2024   # ingest data"
	@echo "  make dbt-run                 # build mart tables"

up: _ensure-airflow-uid
	docker compose up -d
	$(MAKE) fix-perms

build-up: _ensure-airflow-uid
	docker compose up -d --build
	$(MAKE) fix-perms

# Make generated dirs world-writable so the Airflow container (uid 50000) can write freely
fix-perms:
	mkdir -p ./dbt/dbt_packages ./dbt/target ./airflow/logs
	sudo chmod -R 777 ./dbt/dbt_packages ./dbt/target ./airflow/logs

_ensure-airflow-uid:
	@grep -q "^AIRFLOW_UID=" .env 2>/dev/null || echo "AIRFLOW_UID=50000" >> .env

down:
	docker compose down

migrate:
	venv/bin/python clickhouse/migrate.py

reset-data:
	@echo "Dropping all databases..."
	docker exec monocoque-clickhouse-1 clickhouse-client --query "DROP DATABASE IF EXISTS raw_jolpica"
	docker exec monocoque-clickhouse-1 clickhouse-client --query "DROP DATABASE IF EXISTS raw_openf1"
	docker exec monocoque-clickhouse-1 clickhouse-client --query "DROP DATABASE IF EXISTS dim"
	@echo "Re-applying migrations..."
	python clickhouse/migrate.py
	@echo "Done. All data cleared."

clear-raw:
	@echo "Truncating all raw tables and resetting ingestion log..."
	@for table in \
		raw_openf1.sessions raw_openf1.drivers raw_openf1.laps raw_openf1.pit \
		raw_openf1.stints raw_openf1.intervals raw_openf1.weather raw_openf1.race_control \
		raw_openf1.car_data raw_openf1.location \
		raw_jolpica.seasons raw_jolpica.circuits raw_jolpica.drivers raw_jolpica.constructors \
		raw_jolpica.races raw_jolpica.results raw_jolpica.qualifying raw_jolpica.sprint_results \
		raw_jolpica.lap_times raw_jolpica.pit_stops raw_jolpica.driver_standings raw_jolpica.constructor_standings \
		raw_fastf1.car_telemetry raw_fastf1.distances \
		raw_meta.ingestion_log; do \
		docker exec monocoque-clickhouse-1 clickhouse-client --query "TRUNCATE TABLE IF EXISTS $$table"; \
		echo "  cleared $$table"; \
	done
	@echo "Done. Raw tables cleared — ready to re-ingest."

clear-all: clear-raw
	@echo "Dropping dbt output databases..."
	@for db in f1_staging f1_intermediate f1_mart; do \
		docker exec monocoque-clickhouse-1 clickhouse-client --query "DROP DATABASE IF EXISTS $$db"; \
		echo "  dropped $$db"; \
	done
	@echo "Done. Run 'make dbt-run' to rebuild mart tables."

backfill-jolpica:
	python ingestion/jolpica/backfill.py --start $(START) --end $(END)

backfill-openf1:
	python ingestion/openf1/backfill.py --start $(START) --end $(END)

# Run all ingestion + dbt for a single year: make ingest-year YEAR=2023
# Add SKIP_TELEMETRY=1 to skip car_data and location: make ingest-year YEAR=2023 SKIP_TELEMETRY=1
ingest-year:
	@test -n "$(YEAR)" || (echo "ERROR: YEAR is required. Usage: make ingest-year YEAR=2023"; exit 1)
	@trap 'kill 0' INT TERM; \
	python ingestion/jolpica/backfill.py --start $(YEAR) --end $(YEAR) & \
	python ingestion/openf1/backfill.py --start $(YEAR) --end $(YEAR) $(if $(SKIP_TELEMETRY),--skip-telemetry,) & \
	wait
	cd dbt && dbt run --profiles-dir .
	cd dbt && dbt test --profiles-dir .

dbt-run:
	cd dbt && PATH="$(CURDIR)/venv/bin:$$PATH" dbt run --profiles-dir .

dbt-test:
	cd dbt && PATH="$(CURDIR)/venv/bin:$$PATH" dbt test --profiles-dir . --select staging --exclude tag:marts
	cd dbt && PATH="$(CURDIR)/venv/bin:$$PATH" dbt test --profiles-dir . --select marts tag:marts

dbt-docs:
	cd dbt && PATH="$(CURDIR)/venv/bin:$$PATH" dbt docs generate --profiles-dir .
	cp dbt/target/index.html dbt/target/manifest.json dbt/target/catalog.json dbt/docs/

dbt-docs-serve: dbt-docs
	@echo "Opening dbt docs at http://localhost:8081"
	cd dbt && PATH="$(CURDIR)/venv/bin:$$PATH" dbt docs serve --profiles-dir . --port 8081

lint:
	ruff check ingestion/ airflow/
	ruff format --check ingestion/ airflow/

format:
	ruff check --fix ingestion/ airflow/
	ruff format ingestion/ airflow/

test:
	pytest tests/

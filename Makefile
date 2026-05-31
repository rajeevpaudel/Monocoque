.PHONY: up down migrate reset-data backfill-jolpica backfill-openf1 ingest-year dbt-run dbt-test lint test

up:
	docker-compose up -d

down:
	docker-compose down

migrate:
	python clickhouse/migrate.py

reset-data:
	@echo "Dropping all databases..."
	docker exec f1-clickhouse-1 clickhouse-client --query "DROP DATABASE IF EXISTS raw_jolpica"
	docker exec f1-clickhouse-1 clickhouse-client --query "DROP DATABASE IF EXISTS raw_openf1"
	docker exec f1-clickhouse-1 clickhouse-client --query "DROP DATABASE IF EXISTS dim"
	@echo "Re-applying migrations..."
	python clickhouse/migrate.py
	@echo "Done. All data cleared."

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
	cd dbt && dbt run --profiles-dir .

dbt-test:
	cd dbt && dbt test --profiles-dir .

lint:
	ruff check ingestion/ airflow/
	ruff format --check ingestion/ airflow/

format:
	ruff check --fix ingestion/ airflow/
	ruff format ingestion/ airflow/

test:
	pytest tests/

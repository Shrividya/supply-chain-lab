.DEFAULT_GOAL := help
COMPOSE ?= docker compose
PIPE    = $(COMPOSE) --profile tools run --rm pipeline
DBT     = $(COMPOSE) --profile tools run --rm dbt

.PHONY: help setup up down reset ps logs demo load reconcile status tick dbt-build dbt-full provision \
        lab-bad-amount lab-null-channel lab-orphan lab-late lab-drift-add lab-drift-rename lab-heal \
        psql-source psql-warehouse test mini-build

up: ## Start source, warehouse, Airflow and Superset (first run is slow: images + 290k seed rows)
	$(COMPOSE) up -d --build
	@echo "Airflow  http://localhost:$${AIRFLOW_PORT:-8080}    Superset http://localhost:$${SUPERSET_PORT:-8088} (admin/admin)"

down: ## Stop everything, keep data
	$(COMPOSE) down

reset: ## Stop everything and DELETE all data
	$(COMPOSE) down -v

dbt-build: ## dbt build (models + tests)
	$(DBT) build

dbt-full: ## dbt build --full-refresh (rebuild incremental models)
	$(DBT) build --full-refresh

provision: ## Create/update Superset connection, datasets, charts, dashboards
	$(COMPOSE) --profile tools run --rm provision

tick: ## Advance the source by one day, then load + check + build
	$(PIPE) simulate --days 1
	$(PIPE) load --label tick
	$(PIPE) reconcile --label tick
	$(DBT) build

psql-source: ## psql into the source system
	$(COMPOSE) exec source-db psql -U source_admin -d source

psql-warehouse: ## psql into the warehouse
	$(COMPOSE) exec warehouse psql -U warehouse_admin -d warehouse

# no Docker needed: needs only Postgres + psql + Python 3.11+
test: ## Run the test suite (set TEST_PG_ADMIN_DSN to a scratch Postgres superuser)
	cd tests && python3 -m unittest discover -s . -p "test_*.py"

mini-build: ## Run the dbt models with the stand-in runner (tools/mini_dbt.py)
	python3 tools/mini_dbt.py build --dsn "$$DBT_DSN"

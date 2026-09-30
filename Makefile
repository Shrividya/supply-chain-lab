.DEFAULT_GOAL := help
COMPOSE ?= docker compose
PIPE    = $(COMPOSE) --profile tools run --rm pipeline
DBT     = $(COMPOSE) --profile tools run --rm dbt

.PHONY: help setup up down reset ps logs demo load reconcile status tick dbt-build dbt-full provision \
        lab-bad-amount lab-null-channel lab-orphan lab-late lab-drift-add lab-drift-rename lab-heal \
        psql-source psql-warehouse test mini-build

help: ## Show this list
	@grep -E '^[a-zA-Z_-]+:.*## ' $(MAKEFILE_LIST) | awk -F':.*## ' '{printf "  \033[36m%-16s\033[0m %s\n", $$1, $$2}'

setup: ## Create .env with random secrets
	@if [ -f .env ]; then echo ".env exists, leaving it alone"; else \
	  cp .env.example .env; \
	  sed -i.bak "s|^SUPERSET_SECRET_KEY=.*|SUPERSET_SECRET_KEY=$$(openssl rand -base64 42 | tr -d '\n/+=')|; \
	              s|^AIRFLOW_JWT_SECRET=.*|AIRFLOW_JWT_SECRET=$$(openssl rand -base64 32 | tr -d '\n/+=')|" .env && rm -f .env.bak; \
	  echo "Wrote .env"; fi

up: ## Start source, warehouse, Airflow and Superset (first run is slow: images + 290k seed rows)
	$(COMPOSE) up -d --build
	@echo "Airflow  http://localhost:$${AIRFLOW_PORT:-8080}    Superset http://localhost:$${SUPERSET_PORT:-8088} (admin/admin)"

down: ## Stop everything, keep data
	$(COMPOSE) down

reset: ## Stop everything and DELETE all data
	$(COMPOSE) down -v

ps: ## Container status
	$(COMPOSE) ps -a

logs: ## Tail Airflow scheduler and Superset logs
	$(COMPOSE) logs -f airflow-scheduler superset

# the pipeline, step by step (Airflow runs the same steps on a schedule)
load: ## Incremental load: source -> raw
	$(PIPE) load --label manual

reconcile: ## Data quality checks (exit 1 on a failure)
	$(PIPE) reconcile --label manual

status: ## Watermarks and latest load per table
	$(PIPE) status

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

demo: ## First full run: load, check, build, dashboards
	$(PIPE) load --label demo
	$(PIPE) reconcile --label demo
	$(DBT) build
	$(MAKE) provision

# failure-injection labs (see docs/LABS.md)
define inject
	$(COMPOSE) exec source-db psql -U source_admin -d source -c "SELECT sim.inject('$(1)', $(2))"
endef
lab-bad-amount: ## Inject 20 negative prices
	$(call inject,bad_amount,20)
lab-null-channel: ## Inject 20 orders with no channel
	$(call inject,null_channel,20)
lab-orphan: ## Inject 5 orders for customers that do not exist
	$(call inject,orphan_order,5)
lab-late: ## Inject 6 days late events (beyond the dbt window)
	$(call inject,late_events,6)
lab-drift-add: ## Source adds a column (pipeline warns and continues)
	$(call inject,schema_add_column,1)
lab-drift-rename: ## Source renames a column (pipeline stops before writing)
	$(call inject,schema_rename_column,1)
lab-heal: ## Undo schema faults (rename/add column) in the source
	$(COMPOSE) exec source-db psql -U source_admin -d source -c "SELECT sim.reset_faults()"

psql-source: ## psql into the source system
	$(COMPOSE) exec source-db psql -U source_admin -d source

psql-warehouse: ## psql into the warehouse
	$(COMPOSE) exec warehouse psql -U warehouse_admin -d warehouse

# no Docker needed: needs only Postgres + psql + Python 3.11+
test: ## Run the test suite (set TEST_PG_ADMIN_DSN to a scratch Postgres superuser)
	cd tests && python3 -m unittest discover -s . -p "test_*.py"

mini-build: ## Run the dbt models with the stand-in runner (tools/mini_dbt.py)
	python3 tools/mini_dbt.py build --dsn "$$DBT_DSN"

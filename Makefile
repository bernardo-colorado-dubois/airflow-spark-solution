SHELL := /bin/bash
COMPOSE := docker compose

.DEFAULT_GOAL := help

.PHONY: help build init up down restart ps logs clean reset \
        flower airflow-shell spark-shell spark-submit gcp-jars

GCS_CONNECTOR_VERSION := 4.0.5
BQ_CONNECTOR_VERSION := 0.45.0
GCS_CONNECTOR_JAR := gcs-connector-$(GCS_CONNECTOR_VERSION)-shaded.jar
BQ_CONNECTOR_JAR := spark-4.1-bigquery-$(BQ_CONNECTOR_VERSION).jar

help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | \
	awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-24s\033[0m %s\n", $$1, $$2}'

# First run on a fresh clone: .env is gitignored, so create it from
# .env.example and replace the placeholder secrets with generated ones
# (the Fernet key must be 32 url-safe base64-encoded bytes, a plain string
# is rejected by Airflow as soon as it has to encrypt something).
.env:
	@cp .env.example .env
	@sed -i.bak \
		-e "s|^AIRFLOW_FERNET_KEY=.*|AIRFLOW_FERNET_KEY=$$(openssl rand -base64 32 | tr '+/' '-_')|" \
		-e "s|^AIRFLOW_JWT_SECRET=.*|AIRFLOW_JWT_SECRET=$$(openssl rand -hex 32)|" \
		.env && rm -f .env.bak
	@echo "Created .env from .env.example (generated Fernet key + JWT secret)"

build: .env ## Build the custom Airflow+Spark images
	$(COMPOSE) build

gcp-jars: ## Download the GCS + BigQuery connector jars for Spark (idempotent)
	@mkdir -p ./spark/jars
	@test -f ./spark/jars/$(GCS_CONNECTOR_JAR) || \
		curl -fL -o ./spark/jars/$(GCS_CONNECTOR_JAR) \
		https://repo1.maven.org/maven2/com/google/cloud/bigdataoss/gcs-connector/$(GCS_CONNECTOR_VERSION)/$(GCS_CONNECTOR_JAR)
	@test -f ./spark/jars/$(BQ_CONNECTOR_JAR) || \
		curl -fL -o ./spark/jars/$(BQ_CONNECTOR_JAR) \
		https://storage.googleapis.com/spark-lib/bigquery/$(BQ_CONNECTOR_JAR)

init: .env gcp-jars ## First-time setup: create .env + dirs, set AIRFLOW_UID, migrate DB, create admin user
	@mkdir -p ./dags ./logs ./plugins ./config ./spark ./src ./sql ./data ./gcp
	@if ! grep -q "^AIRFLOW_UID=" .env 2>/dev/null; then \
		echo "AIRFLOW_UID=$$(id -u)" >> .env; \
	fi
	@# data and spark are written to by multiple containers running
	@# under different UIDs (the Spark cluster as root, the Airflow driver as
	@# AIRFLOW_UID) -- keep them world-writable so neither side hits
	@# "Permission denied" / "Mkdirs failed" regardless of who touches them first.
	@chmod -R 777 ./data ./spark
	$(COMPOSE) up airflow-init

up: .env gcp-jars ## Start the whole stack (Airflow + Spark) in the background
	@mkdir -p ./data ./spark
	@chmod -R 777 ./data ./spark 2>/dev/null || true
	$(COMPOSE) up -d --build

down: ## Stop and remove all containers
	$(COMPOSE) down

restart: down up ## Restart everything

ps: ## Show status/health of all services
	$(COMPOSE) ps

logs: ## Tail logs of all services (Ctrl+C to exit)
	$(COMPOSE) logs -f

clean: ## Stop containers and remove volumes (WARNING: wipes Postgres metadata db)
	$(COMPOSE) down -v --remove-orphans

reset: clean init up ## Full reset: wipe everything and start fresh

flower: ## Start Flower (Celery monitoring UI) at http://localhost:5555
	$(COMPOSE) --profile flower up -d flower

airflow-shell: ## Open a bash shell inside the Airflow webserver container
	$(COMPOSE) exec airflow-apiserver bash

spark-shell: ## Open a Spark shell against the cluster (spark://spark-master:7077)
	$(COMPOSE) exec spark-master /opt/spark/bin/spark-shell --master spark://spark-master:7077

spark-submit: ## Submit a job from ./spark straight to the cluster: make spark-submit JOB=my_job.py
	@test -n "$(JOB)" || { echo "Usage: make spark-submit JOB=<file under ./spark>"; exit 1; }
	$(COMPOSE) exec spark-master /opt/spark/bin/spark-submit \
		--master spark://spark-master:7077 \
		--deploy-mode client \
		/opt/spark-apps/$(JOB)

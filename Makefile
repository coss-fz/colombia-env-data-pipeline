# Colombia Environmental Data Pipeline — dev shortcuts.
# Run `make help` for the full list.

SHELL := /bin/bash
-include .env
export

.DEFAULT_GOAL := help

# ------------------------------------------------------------------------------
# Help
# ------------------------------------------------------------------------------
.PHONY: help
help: ## Show this message
	@awk 'BEGIN {FS = ":.*##"; printf "\nUsage: make \033[36m<target>\033[0m\n\nTargets:\n"} /^[a-zA-Z_0-9-]+:.*?##/ { printf "  \033[36m%-28s\033[0m %s\n", $$1, $$2 }' $(MAKEFILE_LIST)

# ------------------------------------------------------------------------------
# Terraform
# ------------------------------------------------------------------------------
.PHONY: tf-init tf-plan tf-apply tf-destroy
tf-init: ## Initialise terraform providers
	cd terraform && terraform init

tf-plan: ## Preview infrastructure changes
	cd terraform && terraform plan

tf-apply: ## Create GCS bucket + BigQuery datasets
	cd terraform && terraform apply

tf-destroy: ## Tear down GCP resources (DANGEROUS)
	cd terraform && terraform destroy

# ------------------------------------------------------------------------------
# Docker images
# ------------------------------------------------------------------------------
.PHONY: build-ingestion build-spark build-all
build-ingestion: ## Build the ingestion container image
	docker build -t colombia-env-ingestion:latest ingestion/

build-spark: ## Build the spark container image
	docker build -t colombia-env-spark:latest spark/

build-all: build-ingestion build-spark ## Build all custom images

# ------------------------------------------------------------------------------
# Kestra
# ------------------------------------------------------------------------------
.PHONY: kestra-up kestra-down kestra-logs
kestra-up: ## Start Kestra + Postgres locally (http://localhost:8080)
	docker compose -f kestra/docker-compose.yaml up -d

kestra-down: ## Stop Kestra
	docker compose -f kestra/docker-compose.yaml down

kestra-logs: ## Tail Kestra logs
	docker compose -f kestra/docker-compose.yaml logs -f kestra

# ------------------------------------------------------------------------------
# Kafka
# ------------------------------------------------------------------------------
.PHONY: kafka-up kafka-down kafka-logs
kafka-up: ## Start the Kafka cluster + producer + consumer
	docker compose -f kafka/docker-compose.yaml up -d

kafka-down: ## Stop the Kafka cluster
	docker compose -f kafka/docker-compose.yaml down

kafka-logs: ## Tail producer + consumer logs
	docker compose -f kafka/docker-compose.yaml logs -f producer consumer

# ------------------------------------------------------------------------------
# dbt
# ------------------------------------------------------------------------------
.PHONY: dbt-deps dbt-seed dbt-run dbt-test dbt-build dbt-docs
DBT_RUN = cd dbt && dbt

dbt-deps: ## Install dbt packages
	$(DBT_RUN) deps

dbt-seed: ## Load the cities seed
	$(DBT_RUN) seed --target prod

dbt-run: ## Run all models
	$(DBT_RUN) run --target prod

dbt-test: ## Run dbt tests
	$(DBT_RUN) test --target prod

dbt-build: dbt-deps dbt-seed dbt-run dbt-test ## deps + seed + run + test

dbt-docs: ## Generate + serve dbt docs on localhost:8000
	$(DBT_RUN) docs generate --target prod
	$(DBT_RUN) docs serve --target prod --port 8000

# ------------------------------------------------------------------------------
# Ad-hoc ingestion runs (outside Kestra)
# ------------------------------------------------------------------------------
.PHONY: ingest-historical ingest-daily ingest-air-quality
ingest-historical: build-ingestion ## Run the historical weather backfill once
	MSYS_NO_PATHCONV=1 docker run --rm \
	  -v $$GOOGLE_APPLICATION_CREDENTIALS:/secrets/gcp-sa.json:ro \
	  -e GOOGLE_APPLICATION_CREDENTIALS=/secrets/gcp-sa.json \
	  -e GCS_BUCKET=$$GCS_BUCKET \
	  colombia-env-ingestion:latest \
	  python -m weather.fetch_historical_weather --start-date 2020-01-01 --end-date 2024-12-31

ingest-daily: build-ingestion ## Run yesterday's weather ingestion
	MSYS_NO_PATHCONV=1 docker run --rm \
	  -v $$GOOGLE_APPLICATION_CREDENTIALS:/secrets/gcp-sa.json:ro \
	  -e GOOGLE_APPLICATION_CREDENTIALS=/secrets/gcp-sa.json \
	  -e GCS_BUCKET=$$GCS_BUCKET \
	  colombia-env-ingestion:latest \
	  python -m weather.fetch_weather

ingest-air-quality: build-ingestion ## Pull the last 7 days of OpenAQ data
	MSYS_NO_PATHCONV=1 docker run --rm \
	  -v $$GOOGLE_APPLICATION_CREDENTIALS:/secrets/gcp-sa.json:ro \
	  -e GOOGLE_APPLICATION_CREDENTIALS=/secrets/gcp-sa.json \
	  -e GCS_BUCKET=$$GCS_BUCKET \
	  -e OPENAQ_API_KEY=$$OPENAQ_API_KEY \
	  colombia-env-ingestion:latest \
	  python -m air_quality.fetch_air_quality

# ------------------------------------------------------------------------------
# End-to-end bootstrap
# ------------------------------------------------------------------------------
.PHONY: bootstrap
bootstrap: tf-apply build-all ingest-historical ingest-air-quality dbt-build ## Full first-time setup
	@echo ""
	@echo "✅ Bootstrap complete."
	@echo "   • Data lake: gs://$$GCS_BUCKET"
	@echo "   • Warehouse: $$GCP_PROJECT.$$WAREHOUSE_DATASET"
	@echo "   • Next step: run 'make kestra-up' and import the flows from kestra/flows/"

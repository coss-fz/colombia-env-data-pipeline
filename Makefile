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


# ------------------------------------------------------------------------------
# Kestra
# ------------------------------------------------------------------------------
.PHONY: kestra-up kestra-down kestra-logs
kestra-up: ## Start Kestra + Postgres locally (http://localhost:8080)
	docker compose -f kestra/docker-compose.yaml up -d

kestra-down: ## Stop Kestra
	docker compose -f kestra/docker-compose.yaml down


# ------------------------------------------------------------------------------
# Kafka
# ------------------------------------------------------------------------------
.PHONY: kafka-up kafka-down kafka-logs
kafka-up: ## Start the Kafka cluster + producer + consumer
	docker compose -f kafka/docker-compose.yaml up -d

kafka-down: ## Stop the Kafka cluster
	docker compose -f kafka/docker-compose.yaml down


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
	  python -m weather.fetch_historical_weather --start-date 2020-01-01 --end-date 2025-12-31

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
# Dataproc Serverless (Module 5)
# ------------------------------------------------------------------------------
.PHONY: deploy-spark-job submit-spark-batch
deploy-spark-job: ## Upload the PySpark job to gs://$GCS_BUCKET/code/spark/
	chmod +x spark/deploy.sh
	./spark/deploy.sh


submit-spark-batch: deploy-spark-job
	@BATCH_ID="env-monthly-$$(date +%Y%m%d%H%M%S)" && \
	gcloud config set project "$$GCP_PROJECT" && \
	gcloud dataproc batches submit pyspark \
	  "gs://$$GCS_BUCKET/code/spark/weather_aggregations.py" \
	  --batch="$$BATCH_ID" \
	  --region="$$DATAPROC_REGION" \
	  --service-account="$$DATAPROC_SERVICE_ACCOUNT" \
	  --deps-bucket="gs://$$GCS_BUCKET/dataproc-staging" \
	  --version=2.2 \
	  --properties="spark.sql.adaptive.enabled=true,spark.sql.adaptive.coalescePartitions.enabled=true,spark.sql.legacy.parquet.nanosAsLong=true" \
	  -- \
	    --bucket "$$GCS_BUCKET" \
	    --input-prefix "raw/weather" \
	    --output-prefix "processed/weather_monthly" \
	    --gcp-project "$$GCP_PROJECT" \
	    --bq-dataset "colombia_env_warehouse_reporting" \
	    --bq-location "$$BQ_LOCATION" \
	    --bq-temp-bucket "$$GCS_BUCKET"




###############################################################################
#	 THIS ONES ARE HERE JUST IN CASE YOU WANT TO RUN THE PIPELINE LOCALLY     #
#	 WITHOUT KESTRA, FEEL FREE TO IGNORE THEM                                 #
###############################################################################

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

dbt-clean:
	$(DBT_RUN) clean --target prod
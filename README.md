# colombia-env-data-pipeline
A production-grade, end-to-end data engineering project that ingests, stores,
transforms, and visualises **weather** and **air-quality** data for the eight
largest cities in Colombia.

Built as the capstone project for the [DataTalksClub Data Engineering
Zoomcamp](https://github.com/DataTalksClub/data-engineering-zoomcamp).




---
## Problem statement
Colombia is a country of dramatic environmental diversity: Bogotá sits at
2,640 m and rarely breaks 20 °C, Barranquilla sits at sea level and rarely
drops below 25 °C, and Medellín — the "city of eternal spring" — is
wedged between mountains that trap pollution in predictable patterns. Air
quality and climate are closely tied to **elevation**, **time of year**,
and **population density**, but that relationship is hard to see without
aligning multiple datasets on a consistent schema.

This pipeline does that:
1. Pulls **historical hourly weather** (back to 2020) from [Open-Meteo](https://open-meteo.com).
2. Pulls **daily ground-station air quality** (PM2.5, PM10, O₃, NO₂, SO₂, CO) from [OpenAQ v3](https://docs.openaq.org).
3. Simulates a **real-time IoT sensor stream** through Kafka.
4. Lands everything as partitioned parquet in GCS, exposes it via BigQuery
   external tables, and shapes it with dbt into dashboard-ready facts.
5. Runs a monthly Spark job for heavy historical aggregations (climate
   normals, 30-day rolling windows, heat-wave detection).
6. Surfaces the result in a Looker Studio dashboard.


### Questions the dashboard answers
- How does each city's air quality rank against WHO 2021 guidelines?
- Where and when do heat waves hit hardest, and are they getting longer?
- Which cities have the worst combined **environmental stress** (heat + humidity + pollution)?
- How does pollution correlate with rainfall across climate zones?


### The 8 cities
Chosen to span Colombia's five climate zones (*tierra caliente* to *paramo*)
and capture ~50% of the country's population.

| City          | Department       | Elevation | Population | Climate zone          |
|---------------|------------------|-----------|------------|-----------------------|
| Bogotá        | Cundinamarca     | 2,640 m   | 7.7 M      | Tierra muy fría       |
| Medellín      | Antioquia        | 1,495 m   | 2.6 M      | Tierra templada       |
| Cali          | Valle del Cauca  | 1,000 m   | 2.2 M      | Tierra templada       |
| Barranquilla  | Atlántico        | 18 m      | 1.3 M      | Tierra caliente       |
| Cartagena     | Bolívar          | 2 m       | 1.1 M      | Tierra caliente       |
| Bucaramanga   | Santander        | 959 m     | 0.6 M      | Tierra templada       |
| Pereira       | Risaralda        | 1,411 m   | 0.5 M      | Tierra templada       |
| Santa Marta   | Magdalena        | 6 m       | 0.5 M      | Tierra caliente       |




---
## Quick start

### Prerequisites
- A Google Cloud project with **billing enabled** and the following APIs on: Cloud Storage, BigQuery.
- Local tooling: **Docker**, **Docker Compose**, **Terraform ≥ 1.5**, **GNU Make**, **gcloud CLI**.
- A **service-account JSON key**.
- A free **OpenAQ API key** from [explore.openaq.org/register](https://explore.openaq.org/register).


### 1. Clone and configure
```bash
git clone https://github.com/<you>/colombia-environmental-pipeline.git
cd colombia-environmental-pipeline

cp .env.example .env
cp terraform/terraform.tfvars.example terraform/terraform.tfvars

# Fill in real values in both files.
#   .env                    → used at runtime by every container
#   terraform.tfvars        → used at provisioning time
```


### 2. Provision the cloud infrastructure
```bash
make tf-init
make tf-plan
make tf-apply
```
The output tells you the bucket name and dataset IDs — make sure they match
what's in your `.env`.






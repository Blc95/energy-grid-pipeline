# Energy Grid Data Pipeline

An end-to-end, cloud-native data pipeline for Danish & European electricity grid
data ([ENTSO-E Transparency Platform](https://transparency.entsoe.eu/)). Raw data
is landed immutably in a GCS data lake and loaded into a BigQuery warehouse, with
infrastructure defined in Terraform. Built to be extended with dbt modelling,
Dagster orchestration, CI/CD, and a forecasting (MLOps) phase.

**Why electricity:** power can't be stored, so prices swing hard (sometimes
negative) and refresh every 15 minutes — a genuine, non-trivial data engineering
problem rather than a static dataset.

## Architecture

```
ENTSO-E API ──▶ GCS (raw lake) ──▶ BigQuery (warehouse) ──▶ dbt ──▶ dashboard
                immutable, raw      queryable, modelled      (planned)
```

**ELT:** land raw first (GCS as the replayable source of truth), load into BigQuery,
transform later. The lake decouples ingestion from the warehouse — if the warehouse
is lost or a transform is buggy, rebuild from GCS instead of re-pulling the API.

The pipeline is **idempotent**: re-running any day leaves the data unchanged (no
duplicates), via deterministic GCS paths and scoped delete-then-append loads keyed
on each dataset's natural key.

## Stack

`Python` · `pandas` · `Google Cloud Storage` · `BigQuery` · `Terraform` · `Docker`
(planned: `dbt` · `Dagster` · `GitHub Actions`)

## Status

**Done — full ingestion layer.** All four datasets (day-ahead prices, load,
generation by fuel type, cross-border flows) ingested for both Danish bidding zones
(DK1/DK2), landed in GCS and loaded into BigQuery, idempotently and
timezone/DST-correct. Infrastructure managed in Terraform. Containerized with Docker.

**next** - dbt models (star schema) · Dagster orchestration · CI/CD ·
dashboard · forecasting (MLOps).

## Run it

```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
gcloud auth application-default login          # GCP auth
cd terraform && terraform init && terraform apply
```

A gitignored `.env` holds `ENTSOE_TOKEN=your_token_here`.
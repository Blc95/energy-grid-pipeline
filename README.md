# Energy Grid Data Pipeline

An end-to-end, cloud-native data engineering pipeline built on Danish & European
electricity grid data from the [ENTSO-E Transparency Platform](https://transparency.entsoe.eu/).
Raw data is landed immutably in a GCS data lake, loaded into a BigQuery warehouse,
and (in later phases) transformed with dbt, orchestrated with Dagster, and tested
in CI. The architecture is designed so a forecasting (MLOps) phase can be added
without rework.

> **Status:** In active development. 
> Completed: Single dataset on day-ahead prices ingested to GCS, IaC with terraform, load into BigQuery
> Still to come: Multiple datasets ingested into GCS, IaC with terraform and loading into BigQuery, tranformation with dbt, orchestration with dagster, CI/CD, dashboard, and MLOps infrastructure. Robestness and logging.

---

## Why this project

Electricity is interesting because of one single fact: It can't be stored.

Hence the prices vary wildly and can even become negative! That volatility is exactly what makes this an interesting engineering problem. 
Stale or unreliable data has real consequences for e.g. grid operators, energy traders, and consumers on flexible tariffs.
This project builds the data infrastructure to ingest, land, and model that data reliably and reproducibly,
using the ENTSO-E Transparency Platform as the source of truth.

The four core datasets: day-ahead prices, actual load, generation by fuel type,
and cross-border flows form a causal system: demand sets the need, generation mix
sets the cheap-vs-expensive supply, these clear into a price per zone, and flows
redistribute power across zones.

---

## Architecture

```
ENTSO-E API ──▶ GCS (raw data lake) ──▶ BigQuery (warehouse) ──▶ dbt (models) ──▶ dashboard
                  immutable, raw,          queryable,              staging →
                  partitioned by day       partitioned by day      marts (star schema)
```

The pipeline follows an **ELT** pattern (Extract, Load, Transform):

- **Extract + land:** pull from ENTSO-E, land raw Parquet in GCS, untouched.
- **Load:** load from GCS into BigQuery as a faithful raw table (no cleaning).
- **Transform:** (planned) dbt models clean and model the data into a star schema.

**Why land in GCS *and* load into BigQuery (data duplicated on purpose):** GCS is
the immutable, replayable **source of truth** — cheap, durable, tool-agnostic, and
independent of the flaky external API. BigQuery is the **disposable working copy**
optimized for querying and modeling. If BigQuery were lost, it rebuilds from GCS;
if a transformation bug appears, raw data is reprocessed from GCS rather than
re-pulled from ENTSO-E. *The lake decouples extraction from the warehouse*.

### Idempotency

Re-running the pipeline for a given day must leave the system in the same state as
running it once (no duplicates). This is achieved by writing to **deterministic
slots** at each layer:

- **GCS landing:** each `(dataset, zone, day)` maps to a fixed path
  (`prices/DK_1/2026-05-20/data.parquet`). Re-running overwrites the same object.
- **BigQuery load:** the table is **date-partitioned**, and loads target a specific
  partition (`prices$YYYYMMDD`) with `WRITE_TRUNCATE` — replacing that day's
  partition without touching others.

---

## Tech stack

| Layer            | Tool                          | Status        |
|------------------|-------------------------------|---------------|
| Source           | ENTSO-E API (`entsoe-py`)     | Done          |
| Cloud storage    | Google Cloud Storage          | Done          |
| Warehouse        | BigQuery                      | Done          |
| Infra-as-code    | Terraform                     | Done          |
| Ingestion        | Python (pandas, `gcsfs`)      | In progress   |
| Transformation   | dbt                           | Planned       |
| Orchestration    | Dagster                       | Planned       |
| CI/CD            | GitHub Actions                | Planned       |
| Containerization | Docker                        | Planned       |
| Dashboard        | Streamlit / Looker Studio     | Planned       |

---

## What's done

**Foundations & vertical slice**
- ENTSO-E API token registered; loaded via `.env` + `python-dotenv` (gitignored).
- Pulled live DK1 day-ahead prices, reshaped to a typed DataFrame
  (`timestamp`, `price`, `zone`), written to Parquet.
- Walked the full path by hand once: local Parquet → GCS bucket → BigQuery table —
  to understand every hop before automating it.
- Learned the data's true grain: **15-minute intervals** (not hourly), 96 rows/day.

**Infrastructure-as-Code (Terraform)**
- GCS raw bucket and BigQuery `raw` dataset defined and managed in Terraform.
- Authenticated via Application Default Credentials (no service-account key files).
- Full `plan` / `apply` / `destroy` / `apply` cycle verified — infrastructure is
  reproducible from code, not remembered clicks.
- Project ID and region parameterized as Terraform variables.
- State file and provider binaries correctly gitignored.

**Robust ingestion (in progress)**
- `fetch_data`: pulls a date range, splits into per-day chunks, applies **semantic
  boundary filtering** (`timestamp < end`) to avoid day-boundary double-counting,
  stamps the `zone` column, and writes one Parquet per day to its deterministic
  GCS path. Idempotent landing verified (re-runs overwrite, no duplicates).
- `load_to_big_query`: loads a day's Parquet from GCS into BigQuery.
- Partitioned `raw.prices` table created from code (day-partitioned on `timestamp`).
- Shared `build_gcs_path` so the path scheme has a single source of truth.

**Idempotent BigQuery loading (done)**
- `load_to_big_query`: targets the **partition decorator** (`prices$YYYYMMDD`) so
  daily loads accumulate across days rather than truncating the whole table.
- `main()`: zone + date range specified once and the load loops over the same range
  that was fetched, removing parameter duplication between stages.
---

## What's left

****
- Generalize ingestion to all **four datasets** (prices, load, generation, flows)
  and **both zones** (DK1, DK2) — including generation's wide-by-fuel shape and
  flows' directional zone-pairs.
- Handle real-world API messiness: missing intervals, rate limits, publishing lag,
  and clear failures when a requested day has no data.
- Containerize the ingestion job with Docker.

**Orchestration (Dagster)**
- Express the pipeline as a dependency graph (ingest → load → transform), so load
  only runs after fetch succeeds. Schedule it.

**Transformation (dbt)**
- Staging → intermediate → marts (star schema). Fact table at grain
  `(timestamp, zone)`; time and zone dimensions. Handle UTC vs. Danish local +
  DST explicitly. dbt tests (`not_null`, `unique`, `accepted_values`, freshness).
- Build a `fct_price_features` mart as the hook for the forecasting phase.

**CI/CD & observability**
- GitHub Actions running `dbt build` + tests on every PR; surface test results and
  source freshness; basic alerting on failure/staleness.

**Surface & document**
- Dashboard over the marts; architecture diagram; design-decision write-up.

**MLOps**
- Forecasting model on `fct_price_features`; model registry (MLflow); scheduled
  retraining; drift monitoring. This is where the project crosses from data
  engineering into MLOps.

---

## Local setup

```bash
# Python environment
python -m venv .venv
source .venv/bin/activate # mac
pip install -r requirements.txt

# GCP auth (Application Default Credentials)
gcloud auth application-default login

# Infrastructure
cd terraform
terraform init
terraform apply
```

A `.env` file (gitignored) holds the ENTSO-E token:

```
ENTSOE_TOKEN=your_token_here
```

---

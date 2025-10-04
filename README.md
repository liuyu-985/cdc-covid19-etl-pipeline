# CDC COVID‑19 ETL Pipeline (Docker + Airflow + GCP)

End‑to‑end ETL: extract CDC case JSON → store raw NDJSON in **GCS** → compact to **Parquet** on **Dataproc** → transform/load in **BigQuery** → verify + analyze.

**Stack:** Docker, Airflow, Python, Google Cloud (GCS, Dataproc, BigQuery), SQL, Looker Studio

---

## Architecture

```
CDC JSON (Socrata API)
  |  (Airflow PythonOperator: fetch chunks)
  v
GCS: gs://<RAW_BUCKET>/covid/parts/date={{ ds }}/chunk=<offset>.json
  |  (Dataproc PySpark job)
  v
GCS: Parquet (Snappy) gs://<PARQUET_BUCKET>/covid/parquet/date={{ ds }}/
  |  (BigQuery external/load)
  v
BigQuery staging -> curated (SQL transforms)
  |  (verification + analysis SQL)
  v
Looker Studio dashboard
```

---

## Repository layout

```
.
├── docker-compose1.yaml
├── requirements.txt
├── etl/
│   ├── fetch_and_upload.py        # fetch a chunk from CDC and write NDJSON to GCS
│   └── compact_to_parquet.py      # PySpark: JSON -> Parquet (Snappy)
├── dags/
│   └── ingest_covid.py            # Airflow DAG: orchestrates fetch -> compact
├── sql/
│   ├── data_transformation_and_load.sql
│   ├── verification_data_transformation.sql
│   └── descriptive_analysis.sql
├── dashboards/
│   └── looker_studio_link.txt
├── .env.example
├── .gitignore
└── README.md
```

> **Note:** The code expects certain **env vars** and **Airflow Variables** (documented below).

---

## Prerequisites

- Docker & Docker Compose
- GCP project with GCS, Dataproc, and BigQuery enabled
- A service account with permissions:
  - `roles/storage.objectAdmin`
  - `roles/dataproc.editor` (or serverless batch equivalent)
  - `roles/bigquery.dataEditor` (and `dataViewer` for read)
  - (Optional) `roles/monitoring.metricWriter` if using custom metrics
- Local `.env` (based on `.env.example`)

---

## Configuration

### 1) Environment variables (container)

Required keys (used directly by code and/or docker‑compose):

- `GCP_PROJECT` – your GCP project id (used by monitoring, jobs)
- `GCP_REGION` – e.g., `us-east1` or `us-central1`
- `RAW_BUCKET` – name of GCS bucket for raw NDJSON (no gs:// prefix), e.g., `raw-data-landing`
- `PARQUET_BUCKET` – name of GCS bucket for Parquet outputs
- `BATCH_SIZE` – rows per CDC request (e.g., 50000)
- `TOTAL_ROWS` – expected total rows to fetch (used to compute chunk count)
- `CDC_API_URL` – base CDC API endpoint 
- `GOOGLE_APPLICATION_CREDENTIALS` – path *inside container* to your SA key (mount via volume) 

### 2) Airflow Variables (UI → Admin → Variables)

Create (or import) Airflow Variables (UI → Admin → Variables):

Required:
- `BATCH_SIZE` (int; e.g., 50000)
- `TOTAL_ROWS` (int; e.g., 106219500)

Recommended:
- `GCP_PROJECT` (e.g., your-project-id)
- `GCP_REGION` (e.g., us-east1)
- `DATAPROC_CLUSTER` (e.g., airflow-dataproc)
- `RAW_BUCKET` (no gs:// prefix; e.g., raw-data-landing)
- `PARQUET_BUCKET` (e.g., covid-parquet-output)

---

## Running locally

1) Start services
```bash
docker compose -f docker-compose1.yaml up -d --build
```

2) Mount your service account key securely (bind‑mount), and ensure containers see `GOOGLE_APPLICATION_CREDENTIALS` from `.env`.

3) In Airflow UI:
- Turn on the DAG `ingest_cdc_raw`.
- Trigger a run for a date; PythonOperators will fan‑out chunk fetches; once complete, Dataproc job compacts to Parquet.

---

## Transform, Verify, Analyze (BigQuery)

- Run `sql/data_transformation_and_load.sql` to create curated tables.
- Validate with `sql/verification_data_transformation.sql` (row counts, NOT NULLs, anomalies).
- Use `sql/descriptive_analysis.sql` for basic EDA (trends, age summaries, etc.).

---

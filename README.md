# Customer / Order Dataform Project — Medallion Architecture

A Dataform project that ingests **customers** and **orders** files from
**Google Cloud Storage (GCS)**, exposes them in **BigQuery**, and transforms
them using the **Bronze → Silver → Gold** (medallion) pattern.

## Structure

```
dataform.json                              # Project config (edit defaultDatabase and vars.gcsBucket)
package.json                               # Dependency on @dataform/core
definitions/
  bronze/
    customers.sqlx                         # BigQuery EXTERNAL TABLE reading gs://<bucket>/customers/*.csv
    orders.sqlx                            # BigQuery EXTERNAL TABLE reading gs://<bucket>/orders/*.csv
  silver/
    silver_customers.sqlx                  # Cleaned: trimmed names, lowercase emails, no nulls + assertions
    silver_orders.sqlx                     # Cleaned: filtered to valid orders + assertions
  gold/
    gold_customer_order_summary.sqlx       # Business-ready: one row per customer with order totals
```

## What each layer means

| Layer  | Purpose                                     | Type in this project                        |
|--------|----------------------------------------------|-----------------------------------------------|
| Bronze | Raw data, read directly from GCS, no copy   | `operations` creating a BigQuery `EXTERNAL TABLE` |
| Silver | Cleaned, standardized, deduplicated, validated | `view` with `uniqueKey`/`nonNull` assertions |
| Gold   | Joined, aggregated, ready for BI/reporting  | `table` with `uniqueKey`/`nonNull` assertions |

## Data flow

```
gs://<bucket>/customers/*.csv ─> bronze.customers (external table) ─┐
                                                                      ├─> silver.silver_customers ─┐
gs://<bucket>/orders/*.csv ────> bronze.orders (external table) ────┘                              ├─> gold.gold_customer_order_summary
                                                        silver.silver_orders ───────────────────────┘
```

Bronze tables are **external tables**: BigQuery queries the CSV files in
GCS directly, so there's no separate load/copy job to keep in sync — new
files landing in the bucket are picked up on the next query. `bronze/*`
declare an explicit column schema (rather than `autodetect`) so schema
drift in the source files fails loudly instead of silently changing types.

`gold_customer_order_summary` includes `total_orders`, `total_spent`, and
`last_order_date` per customer.

## Setup

1. Install the Dataform CLI:
   ```
   npm install -g @dataform/cli
   ```
2. Edit `dataform.json`:
   - Set `defaultDatabase` to your actual GCP project id.
   - Set `vars.gcsBucket` to the GCS bucket holding your raw files (bucket
     name only, no `gs://` prefix — it's prepended in the bronze `.sqlx` files).
   - Adjust `defaultLocation` if your BigQuery dataset isn't in `US` (must
     match the region/multi-region of your GCS bucket for external tables).
3. Install dependencies:
   ```
   cd dataform-project
   npm install
   ```
4. Authenticate with GCP (if not already):
   ```
   gcloud auth application-default login
   ```
5. Grant BigQuery access to the GCS bucket. Whichever identity runs
   Dataform (your user, or the Dataform service account in a hosted
   repository) needs read access to the source files:
   ```
   gsutil iam ch serviceAccount:<RUNNER_SA>@<PROJECT>.iam.gserviceaccount.com:roles/storage.objectViewer \
     gs://<your-gcs-bucket>
   ```

## Expected GCS layout

```
gs://<your-gcs-bucket>/
  customers/*.csv   # header row + customer_id, full_name, email, created_at
  orders/*.csv      # header row + order_id, customer_id, order_date, order_amount, status
```

## Running

```
dataform compile              # check SQL and dependency graph
dataform run                  # run everything (recreates bronze external tables, then silver, then gold)
dataform run --tags gold      # run just the gold layer + its dependencies
dataform run --actions gold_customer_order_summary   # run one model + its dependents
```

## Orchestration (Airflow)

`dags/customer_order_dataform_etl_dag.py` runs this pipeline on a schedule
via [`apache-airflow-providers-google`](https://airflow.apache.org/docs/apache-airflow-providers-google/stable/operators/cloud/dataform.html)'s
Dataform operators. It triggers a real Dataform repository/workflow
invocation (the same thing `dataform run` does), so bronze/silver/gold stay
in one source of truth — the DAG doesn't reimplement the SQL.

Flow: `DataformCreateCompilationResultOperator` compiles the `main` branch →
`DataformCreateWorkflowInvocationOperator` (`asynchronous=True`) starts the
run → `DataformWorkflowInvocationStateSensor` polls (in `reschedule` mode,
so it doesn't hold a worker slot) until it succeeds or fails.

**One-time setup, before the DAG can run (this is Google Cloud project
setup, not something the DAG does):**

1. Push this project to a git remote (GitHub/GitLab/Cloud Source Repositories).
2. Create a Secret Manager secret containing a git access token, and grant
   the Dataform service agent `roles/secretmanager.secretAccessor` on it.
3. Create the Dataform repository and connect it to that git remote —
   either in the Cloud Console (Dataform → Create Repository → Connect to
   a third-party Git repository) or via the `projects.locations.repositories`
   API — with the default branch set to `main`.
4. In `dags/customer_order_dataform_etl_dag.py`, set `PROJECT_ID` to your
   GCP project id and `REGION`/`REPOSITORY_ID` to match the repository you
   just created (`REGION` is the Dataform repository's location, e.g.
   `us-central1` — a separate setting from the BigQuery `defaultLocation`
   in `dataform.json`).
5. Deploy the DAG file to your Airflow/Cloud Composer `dags/` folder. Make
   sure the Airflow environment's service account has permission to run
   Dataform workflow invocations (`roles/dataform.editor` or equivalent) on
   the target project.

## Assumptions to adjust for your real data

- Raw files land as CSV with a header row at `customers/*.csv` and
  `orders/*.csv` under the configured bucket, with columns
  `customer_id, full_name, email, created_at` and
  `order_id, customer_id, order_date, order_amount, status` respectively.
- If your raw files use a different format (Parquet, JSON, Avro), column
  set, or path layout, update the `OPTIONS`/column list in
  `definitions/bronze/*.sqlx` — `format`, `uris`, and the column schema all
  need to match the source files.
- `max_bad_records = 0` means a single malformed row fails the whole
  external table query. Raise it if you'd rather skip bad rows.

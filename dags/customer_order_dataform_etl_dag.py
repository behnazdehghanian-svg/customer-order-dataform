"""Orchestrates the customer/order Dataform pipeline (GCS -> BigQuery, bronze/silver/gold).

Assumes a Dataform repository already exists in Google Cloud and is connected
to this project's git remote (see README.md "Orchestration (Airflow)" section
for the one-time setup). This DAG only compiles and runs that repository —
it does not create it.
"""

from __future__ import annotations

import datetime

from airflow import DAG
from airflow.providers.google.cloud.operators.dataform import (
    DataformCreateCompilationResultOperator,
    DataformCreateWorkflowInvocationOperator,
)
from airflow.providers.google.cloud.sensors.dataform import (
    DataformWorkflowInvocationStateSensor,
)
from google.cloud.dataform_v1beta1 import WorkflowInvocation

# Dataform repository location, not the BigQuery dataset location in dataform.json.
PROJECT_ID = "your-gcp-project-id"
REGION = "us-central1"
REPOSITORY_ID = "customer-order-dataform"
GIT_COMMITISH = "main"

default_args = {
    "owner": "data-eng",
    "retries": 1,
    "retry_delay": datetime.timedelta(minutes=5),
}

with DAG(
    dag_id="customer_order_dataform_etl",
    description="GCS -> BigQuery ETL for customers/orders via Dataform (bronze/silver/gold)",
    schedule="0 6 * * *",
    start_date=datetime.datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["dataform", "bigquery", "gcs", "etl"],
) as dag:
    create_compilation_result = DataformCreateCompilationResultOperator(
        task_id="create_compilation_result",
        project_id=PROJECT_ID,
        region=REGION,
        repository_id=REPOSITORY_ID,
        compilation_result={"git_commitish": GIT_COMMITISH},
    )

    create_workflow_invocation = DataformCreateWorkflowInvocationOperator(
        task_id="create_workflow_invocation",
        project_id=PROJECT_ID,
        region=REGION,
        repository_id=REPOSITORY_ID,
        asynchronous=True,
        workflow_invocation={
            "compilation_result": (
                "{{ task_instance.xcom_pull('create_compilation_result')['name'] }}"
            ),
        },
    )

    wait_for_workflow_invocation = DataformWorkflowInvocationStateSensor(
        task_id="wait_for_workflow_invocation",
        project_id=PROJECT_ID,
        region=REGION,
        repository_id=REPOSITORY_ID,
        workflow_invocation_id=(
            "{{ task_instance.xcom_pull('create_workflow_invocation')['name'].split('/')[-1] }}"
        ),
        expected_statuses={WorkflowInvocation.State.SUCCEEDED},
        failure_statuses={
            WorkflowInvocation.State.FAILED,
            WorkflowInvocation.State.CANCELLED,
        },
        mode="reschedule",
        poke_interval=30,
        timeout=60 * 60,
    )

    create_compilation_result >> create_workflow_invocation >> wait_for_workflow_invocation

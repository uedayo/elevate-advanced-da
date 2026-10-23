#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Cloud Composer 3 (Airflow 2.x) Nightly Orchestration DAG
=========================================================
DAG ID  : cymbal_nightly_inventory_reconciliation
Schedule: 0 22 * * * (Daily at 10:00 PM UTC)

処理フロー:
1. DataprocCreateBatchOperator (`run_inventory_reconciliation_lightning`):
   Dataproc Serverless Runtime 2.3 + Lightning Engine + OpenLineage 有効化により
   `migrated_inventory_reconciliation_pipeline.py` をサーバーレス実行。
2. BigQueryCheckOperator (`validate_gold_table_parity`):
   出力先 BigQuery Managed Iceberg テーブル (`cymbal_gold.gold_inventory_reconciliation_ledger`)
   の行数（8,400行）および3ステータスの件数内訳（8,265 / 84 / 51）がデータ契約（Data Contract）を
   満たしていることを検証するデータ品質ゲート。
"""

from datetime import datetime, timedelta
import os

from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCheckOperator,
)
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocCreateBatchOperator,
)

PROJECT_ID = os.environ.get("GCP_PROJECT", "argolisyo")
REGION = os.environ.get("GCP_REGION", "us-central1")
BUCKET_NAME = f"{PROJECT_ID}-module1-bucket"
SERVICE_ACCOUNT = f"cymbal-sa-data@{PROJECT_ID}.iam.gserviceaccount.com"
SUBNET_URI = (
    f"projects/{PROJECT_ID}/regions/{REGION}/subnetworks/"
    f"cymbal-retail-subnet-{REGION}"
)
CATALOG = f"{PROJECT_ID}.cymbal-lakehouse.elevate_data"
TARGET_TABLE = f"{PROJECT_ID}.cymbal_gold.gold_inventory_reconciliation_ledger"
MAIN_PYTHON_URI = (
    f"gs://{BUCKET_NAME}/code/migrated_inventory_reconciliation_pipeline.py"
)

default_args = {
    "owner": "cymbal-data-engineering",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="cymbal_nightly_inventory_reconciliation",
    default_args=default_args,
    description=(
        "Nightly Cymbal Retail Inventory Reconciliation on Dataproc Serverless "
        "2.3 with Lightning Engine, OpenLineage, and BigQuery Data Contract Gate"
    ),
    schedule="0 22 * * *",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["cymbal", "lakehouse", "dataproc-serverless", "lightning-engine", "iceberg"],
) as dag:

    run_inventory_reconciliation_lightning = DataprocCreateBatchOperator(
        task_id="run_inventory_reconciliation_lightning",
        project_id=PROJECT_ID,
        region=REGION,
        batch_id="recon-lightning-nightly-{{ ts_nodash | lower }}",
        batch={
            "pyspark_batch": {
                "main_python_file_uri": MAIN_PYTHON_URI,
                "args": [
                    f"--catalog={CATALOG}",
                    f"--target-table={TARGET_TABLE}",
                    f"--gcs-staging-bucket={BUCKET_NAME}",
                    "--start-date=2025-01-01",
                    "--until-date=2026-09-10",
                    "--write-mode=overwrite",
                ],
            },
            "runtime_config": {
                "version": "2.3",
                "properties": {
                    "dataproc.tier": "premium",
                    "spark.dataproc.engine": "lightningEngine",
                    "spark.dataproc.lineage.enabled": "true",
                },
            },
            "environment_config": {
                "execution_config": {
                    "service_account": SERVICE_ACCOUNT,
                    "subnetwork_uri": SUBNET_URI,
                }
            },
        },
    )

    validate_gold_table_parity = BigQueryCheckOperator(
        task_id="validate_gold_table_parity",
        sql=f"""
        SELECT
            COUNT(*) = 8400
            AND COUNTIF(reconciliation_status = 'RECONCILED NORMAL HEALTH') = 8265
            AND COUNTIF(reconciliation_status = 'MONITOR VELOCITY') = 84
            AND COUNTIF(reconciliation_status = 'CRITICAL BURN SPIKE - STOCKOUT RISK') = 51
            AND COUNTIF(business_date IS NULL OR store_id IS NULL OR item_id IS NULL) = 0
        FROM `{TARGET_TABLE}`
        """,
        use_legacy_sql=False,
        location=REGION,
    )

    run_inventory_reconciliation_lightning >> validate_gold_table_parity

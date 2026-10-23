# ruff: noqa
"""Cymbal Retail Operations Coordinator Agent (ADK Multi-Tool Orchestrator)."""

import logging
import os

from google.adk.agents import LlmAgent
from google.adk.apps import App
from google.adk.plugins.bigquery_agent_analytics_plugin import (
    BigQueryAgentAnalyticsPlugin,
    BigQueryLoggerConfig,
)
from google.cloud import bigquery

from app.tools.analytics_tool import cymbal_analytics_tool
from app.tools.bigtable_tool import read_cashier_realtime_alerts
from app.tools.rag_tool import pos_troubleshooting_rag_tool

MODEL = os.environ.get("AGENT_MODEL", "gemini-3.6-flash")

COORDINATOR_INSTRUCTION = """You are the Cymbal Retail Operations Coordinator Agent (`cymbal_operations_agent`).
You orchestrate 3 specialized tool gateways to assist store leads, technicians, and loss-prevention auditors:

1. `cymbal_analytics_tool`: BigQuery Conversational Data Agent for relational analytics across structured Gold tables (`pos_transactions_gold`, `pos_anomaly_alerts`, `gold_inventory_reconciliation_ledger`, `historical_transactional_data`), extracted warranty policies (`warranty_generic_sections_extracted`), and federated AWS S3 tables (`silver_pos_transactions`).
   - Always pass standardized business terms verbatim (e.g. Net Transaction Revenue, Total On-Hand Inventory, Estimated Inventory Cover Hours).

2. `pos_troubleshooting_rag_tool`: Vector similarity search with adjacent context window stitching over POS hardware manuals in BigQuery (`cymbal_gold.pos_manual_chunk_embeddings`).
   - Use for hardware error codes (e.g. ERR-PAY-4001, ERR-DN-PRNT-24V) and terminal maintenance SOPs.
   - Always include the clickable HTTPS GCS manual link in your response.

3. `read_cashier_realtime_alerts`: Live sub-second 1-hour rolling metrics and audit status flags from Cloud Bigtable (`operations-db:cashier_realtime_alerts`).
   - Use row key prefix format `STORE_<ID>#CASH_<ID>` (e.g. `STORE_048#CASH_1190`).

TOOL DISPATCH PROTOCOLS:
- SINGLE-TOOL DISPATCH: Route direct inquiries to the appropriate tool. For ANY technical, maintenance, or repair question (including out-of-domain vehicle/machinery repair like Ford F-150 oil change), you MUST invoke `pos_troubleshooting_rag_tool` first. If the RAG tool returns a refusal message indicating no certified rules were found, reply ONLY with that exact refusal sentence and do NOT add unverified conversational advice or self-introductions.
- PARALLEL TOOL DISPATCH: When asked to compare live real-time cashier metrics against historical 7-day baselines (e.g. UC 2.2), invoke `read_cashier_realtime_alerts` AND `cymbal_analytics_tool` concurrently in the same turn.
- SEQUENTIAL MULTI-TURN DISPATCH: When auditing cross-cloud promo abuse offenders (e.g. UC 2.3), first call `cymbal_analytics_tool` to rank top promo abuse offenders in GCP BigQuery (`pos_anomaly_alerts`), then invoke `cymbal_analytics_tool` to retrieve checkout logs from AWS S3 (`silver_pos_transactions`) for the top offending cashier.
- STRICT GROUNDING: Base every sentence of your final response strictly on the data returned by the invoked tools. Avoid ungrounded introductory or concluding conversational filler.
"""

cymbal_operations_agent = LlmAgent(
    name="cymbal_operations_agent",
    model=MODEL,
    description="Coordinator agent for Cymbal Retail operations, hardware diagnostics, and loss prevention.",
    instruction=COORDINATOR_INSTRUCTION,
    tools=[
        cymbal_analytics_tool,
        pos_troubleshooting_rag_tool,
        read_cashier_realtime_alerts,
    ],
)

root_agent = cymbal_operations_agent

# Initialize BigQuery Agent Analytics Plugin
_plugins = []
_project_id = (
    os.environ.get("PROJECT_ID")
    or os.environ.get("GOOGLE_CLOUD_PROJECT")
    or "argolisyo"
)
_dataset_id = (
    os.environ.get("BQ_TELEMETRY_DATASET")
    or os.environ.get("BQ_ANALYTICS_DATASET_ID")
    or "agent_telemetry"
)
_location = (
    os.environ.get("REGION")
    or os.environ.get("GOOGLE_CLOUD_LOCATION")
    or "us-central1"
)
if _location == "global":
    _location = "us-central1"

if _project_id:
    try:
        bq = bigquery.Client(project=_project_id)
        dataset_ref = bigquery.Dataset(f"{_project_id}.{_dataset_id}")
        dataset_ref.location = _location
        bq.create_dataset(dataset_ref, exists_ok=True)

        _plugins.append(
            BigQueryAgentAnalyticsPlugin(
                project_id=_project_id,
                dataset_id=_dataset_id,
                location=_location,
                config=BigQueryLoggerConfig(
                    gcs_bucket_name=os.environ.get("BQ_ANALYTICS_GCS_BUCKET"),
                    connection_id=os.environ.get("BQ_ANALYTICS_CONNECTION_ID"),
                ),
            )
        )
    except Exception as e:
        logging.warning(f"Failed to initialize BigQuery Analytics: {e}")

app = App(
    root_agent=root_agent,
    name="app",
    plugins=_plugins,
)

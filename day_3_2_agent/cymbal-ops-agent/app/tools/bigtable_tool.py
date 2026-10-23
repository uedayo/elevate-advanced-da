"""Cloud Bigtable real-time cashier alert tool via Cloud Run MCP Toolbox."""

import json
import os
import shutil
import subprocess
import urllib.error
import urllib.request
import google.auth
import google.auth.transport.requests
import google.oauth2.id_token


DEFAULT_MCP_SERVICE_URL = (
    "https://mcp-toolbox-bigtable-555891812099.us-central1.run.app"
)


def _get_identity_token(target_audience: str) -> str:
    """Retrieves an OIDC identity token for Cloud Run IAM authentication."""
    # 1. If running in Cloud Run or Vertex AI Agent Engine, use Metadata Server first
    if os.environ.get("K_SERVICE") or os.environ.get(
        "GOOGLE_CLOUD_AGENT_ENGINE_ID"
    ):
        try:
            auth_req = google.auth.transport.requests.Request()
            token = google.oauth2.id_token.fetch_id_token(
                auth_req, target_audience
            )
            if token:
                return token
        except Exception:
            pass

    # 2. Local execution: use active gcloud identity token (Argolis account)
    try:
        gcloud_bin = (
            shutil.which("gcloud")
            or "/usr/local/google/home/uedayo/google-cloud-sdk/bin/gcloud"
        )
        token = (
            subprocess.check_output(
                [gcloud_bin, "auth", "print-identity-token"],
                stderr=subprocess.DEVNULL,
            )
            .decode("utf-8")
            .strip()
        )
        if token:
            return token
    except Exception:
        pass

    # 3. Fallback: ADC credentials id_token or Metadata Server
    try:
        credentials, _ = google.auth.default()
        auth_req = google.auth.transport.requests.Request()
        credentials.refresh(auth_req)
        id_token = getattr(credentials, "id_token", None)
        if id_token:
            return id_token
    except Exception:
        pass

    try:
        auth_req = google.auth.transport.requests.Request()
        token = google.oauth2.id_token.fetch_id_token(auth_req, target_audience)
        if token:
            return token
    except Exception:
        pass

    return ""


def get_bigtable_mcp_toolset():
    """Creates an ADK McpToolset connected to the Cloud Run MCP Toolbox service."""
    from google.adk.tools.mcp_tool import McpToolset
    from google.adk.tools.mcp_tool.mcp_session_manager import (
        StreamableHTTPConnectionParams,
    )

    service_url = os.environ.get(
        "BIGTABLE_MCP_SERVICE_URL", DEFAULT_MCP_SERVICE_URL
    ).rstrip("/")
    token = _get_identity_token(service_url)
    headers = {"Authorization": f"Bearer {token}"} if token else {}
    return McpToolset(
        connection_params=StreamableHTTPConnectionParams(
            url=f"{service_url}/mcp",
            headers=headers,
        )
    )


def read_cashier_realtime_alerts(row_key_prefix: str) -> str:
    """Reads the latest 1-hour rolling metrics and audit status flags for a cashier from Cloud Bigtable via Cloud Run MCP.

    Executes directly against the Cloud Run MCP Toolbox microservice (`mcp-toolbox-bigtable/mcp` JSON-RPC 2.0)
    both in the cloud (via metadata server OIDC token) and locally (via gcloud auth print-identity-token).

    Args:
        row_key_prefix: Row key prefix formatted as 'STORE_<ID>#CASH_<ID>' (e.g. 'STORE_048#CASH_1190').

    Returns:
        Structured JSON containing the latest 1-hour rolling window metrics and audit status.
    """
    service_url = os.environ.get(
        "BIGTABLE_MCP_SERVICE_URL", DEFAULT_MCP_SERVICE_URL
    ).rstrip("/")
    mcp_endpoint = f"{service_url}/mcp"
    token = _get_identity_token(service_url)

    payload = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "tools/call",
        "params": {
            "name": "read_cashier_realtime_alerts",
            "arguments": {"row_key_prefix": row_key_prefix},
        },
    }

    headers = {
        "Content-Type": "application/json",
        "User-Agent": "cymbal-ops-agent/1.0",
    }
    if token:
        headers["Authorization"] = f"Bearer {token}"

    try:
        req = urllib.request.Request(
            mcp_endpoint,
            data=json.dumps(payload).encode("utf-8"),
            headers=headers,
            method="POST",
        )
        with urllib.request.urlopen(req, timeout=30) as resp:
            raw_resp = json.loads(resp.read().decode("utf-8"))

        content_items = raw_resp.get("result", {}).get("content", [])
        if not content_items:
            return json.dumps(
                {
                    "source": "Cloud Run MCP Toolbox for Databases (Direct /mcp JSON-RPC)",
                    "row_key_prefix": row_key_prefix,
                    "status": "NO_RECORDS_FOUND",
                },
                indent=2,
            )

        raw_text = content_items[0].get("text", "")
        parsed = json.loads(raw_text)
        if isinstance(parsed, list):
            record = parsed[0] if parsed else {}
        elif isinstance(parsed, dict):
            record = parsed
        else:
            record = {"raw": parsed}

        result_obj = {
            "source": "Cloud Run MCP Toolbox for Databases (Direct /mcp JSON-RPC)",
            "snapshot_type": (
                "Latest 1-Hour Rolling Window Aggregation Snapshot "
                "(Already aggregated over the past 60 minutes. Do NOT sum or aggregate across multiple rows.)"
            ),
            "row_key": record.get("row_key"),
            "audit_status": record.get("audit_status"),
            "cashier_1h_total_discount_usd": record.get(
                "cashier_1h_total_discount_usd"
            ),
            "cashier_1h_txn_count": record.get("cashier_1h_txn_count"),
            "cashier_1h_avg_discount_pct": record.get(
                "cashier_1h_avg_discount_pct"
            ),
            "cashier_1h_manual_override_count": record.get(
                "cashier_1h_manual_override_count"
            ),
            "cashier_1h_promo_count": record.get("cashier_1h_promo_count"),
            "cashier_1h_promo_rate": record.get("cashier_1h_promo_rate"),
            "last_event_ts": record.get("last_event_ts"),
            "risk_score": record.get("risk_score"),
        }
        return json.dumps(result_obj, indent=2)

    except Exception as exc:
        return json.dumps(
            {
                "source": "Cloud Run MCP Toolbox for Databases (Direct /mcp JSON-RPC)",
                "error": f"Failed to read cashier realtime alerts for {row_key_prefix}: {exc}",
            },
            indent=2,
        )

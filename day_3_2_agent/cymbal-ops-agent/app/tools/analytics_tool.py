"""BigQuery Conversational Data Agent tool gateway for Cymbal Retail."""

import json
import os
import time
import urllib.error
import urllib.request
import google.auth
import google.auth.transport.requests


def _get_access_token() -> str:
    """Retrieves an OAuth2 access token using Application Default Credentials."""
    credentials, _ = google.auth.default(
        scopes=["https://www.googleapis.com/auth/cloud-platform"]
    )
    auth_req = google.auth.transport.requests.Request()
    credentials.refresh(auth_req)
    return credentials.token or ""


def cymbal_analytics_tool(query: str) -> str:
    """Queries the Cymbal Retail Analytics BigQuery Conversational Data Agent in natural language.

    Args:
        query: Verbatim natural language business inquiry referencing retail metrics,
            inventory positions, historical purchases, warranty terms, or cross-cloud audits.

    Returns:
        Structured analytical answer including generated SQL and actual query result rows,
        or a fallback message if transient database connectivity fails.
    """
    project_id = (
        os.environ.get("PROJECT_ID")
        or os.environ.get("GOOGLE_CLOUD_PROJECT")
        or "argolisyo"
    )
    data_agent_id = os.environ.get(
        "DATA_AGENT_ID", "cymbal-retail-analytics-data-agent"
    )
    location = os.environ.get("DATA_AGENT_LOCATION", "global")

    url = f"https://geminidataanalytics.googleapis.com/v1beta/projects/{project_id}/locations/{location}:chat"
    data_agent_name = (
        f"projects/{project_id}/locations/{location}/dataAgents/{data_agent_id}"
    )

    payload = {
        "messages": [{"userMessage": {"text": query}}],
        "dataAgentContext": {"dataAgent": data_agent_name},
    }

    max_attempts = 3
    backoff_seconds = 2.0

    for attempt in range(1, max_attempts + 1):
        try:
            token = _get_access_token()
            req = urllib.request.Request(
                url,
                data=json.dumps(payload).encode("utf-8"),
                headers={
                    "Authorization": f"Bearer {token}",
                    "Content-Type": "application/json",
                    "x-goog-user-project": project_id,
                },
                method="POST",
            )
            with urllib.request.urlopen(req, timeout=120) as resp:
                raw_body = resp.read().decode("utf-8")

            events = json.loads(raw_body)
            if not isinstance(events, list):
                events = [events]

            sql_queries: list[str] = []
            result_rows_blocks: list[str] = []
            text_parts: list[str] = []

            for ev in events:
                sm = ev.get("systemMessage", {})
                if "data" in sm:
                    d = sm["data"]
                    if "generatedSql" in d:
                        sql_queries.append(d["generatedSql"].strip())
                    if "result" in d:
                        rows = d["result"].get("data", [])
                        result_rows_blocks.append(
                            f"Query Result Rows ({len(rows)} rows):\n"
                            + json.dumps(rows, indent=2)
                        )
                if "text" in sm:
                    parts = sm["text"].get("parts", [])
                    thought = sm["text"].get("thought", False)
                    if not thought and parts:
                        text_parts.append("".join(parts))

            output_sections: list[str] = []
            if sql_queries:
                output_sections.append(
                    "```sql\n" + sql_queries[-1] + "\n```"
                )
            if result_rows_blocks:
                output_sections.append(result_rows_blocks[-1])
            if text_parts:
                output_sections.append("\n".join(text_parts))

            if output_sections:
                return "\n\n".join(output_sections)
            return "No analytical results returned from the BigQuery Conversational Data Agent."

        except (urllib.error.HTTPError, urllib.error.URLError, TimeoutError) as exc:
            if attempt < max_attempts:
                time.sleep(backoff_seconds)
                backoff_seconds *= 2
                continue
            return (
                "[FALLBACK: BigQuery Conversational Data Agent is temporarily unavailable "
                f"after {max_attempts} retry attempts ({exc}). Please retry your analytical query shortly.]"
            )
        except Exception as exc:
            return (
                "[FALLBACK: Unexpected error querying BigQuery Conversational Data Agent: "
                f"{exc}]"
            )

    return "[FALLBACK: Exhausted retries calling BigQuery Conversational Data Agent.]"


ask_data_agent = cymbal_analytics_tool

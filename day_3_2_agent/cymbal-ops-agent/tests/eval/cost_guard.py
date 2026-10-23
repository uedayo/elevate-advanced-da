"""Token-cost and rate-limit guard for agents-cli eval runs.

Usage:
    python tests/eval/cost_guard.py --since 2026-10-23T07:00:00Z [--until ...] \
        [--grade-results artifacts/grade_results/results_<ts>.json ...] \
        [--weights tests/eval/scoring_weights.yaml]

Sources (real data only, nothing is estimated when data is missing):
  * Agent token usage: BigQuery `agent_telemetry.agent_events` (written by
    BigQueryAgentAnalyticsPlugin), LLM_RESPONSE rows, attributes.usage_metadata,
    filtered to the eval user (agents-cli uses user_id 'eval-cli-user') and the run window.
  * Rate limiting: error_message fields in agents-cli grade-result files
    (429 / RESOURCE_EXHAUSTED / quota).
Exit code: 0 = within budget, 1 = budget or rate-limit threshold exceeded, 2 = input error.
"""

import argparse
import json
import pathlib
import re
import sys

import yaml
from google.cloud import bigquery

_RATE_LIMIT = re.compile(r"\b429\b|RESOURCE_EXHAUSTED|rate limit|quota", re.IGNORECASE)


def _token_usage(client, table, user_id, since, until):
    sql = f"""
    SELECT
      session_id,
      COUNT(*) AS llm_calls,
      SUM(CAST(JSON_VALUE(attributes, '$.usage_metadata.prompt_token_count') AS INT64)) AS input_tokens,
      SUM(CAST(JSON_VALUE(attributes, '$.usage_metadata.candidates_token_count') AS INT64)) AS output_tokens,
      SUM(IFNULL(CAST(JSON_VALUE(attributes, '$.usage_metadata.thoughts_token_count') AS INT64), 0)) AS thinking_tokens,
      SUM(CAST(JSON_VALUE(attributes, '$.usage_metadata.total_token_count') AS INT64)) AS total_tokens,
      ANY_VALUE(JSON_VALUE(attributes, '$.model_version')) AS model
    FROM `{table}`
    WHERE event_type = 'LLM_RESPONSE'
      AND user_id = @user_id
      AND timestamp >= @since
      AND (@until IS NULL OR timestamp < @until)
    GROUP BY session_id
    ORDER BY total_tokens DESC
    """
    cfg = bigquery.QueryJobConfig(query_parameters=[
        bigquery.ScalarQueryParameter("user_id", "STRING", user_id),
        bigquery.ScalarQueryParameter("since", "TIMESTAMP", since),
        bigquery.ScalarQueryParameter("until", "TIMESTAMP", until),
    ])
    return [dict(r) for r in client.query(sql, job_config=cfg).result()]


def _rate_limit_errors(paths):
    hits = []
    for p in paths:
        data = json.loads(pathlib.Path(p).read_text())
        for cr in data.get("eval_case_results", []):
            for cand in cr.get("response_candidate_results", []):
                for name, m in (cand.get("metric_results") or {}).items():
                    err = m.get("error_message") or ""
                    if err and _RATE_LIMIT.search(err):
                        hits.append(f"{pathlib.Path(p).name}: case {cr.get('eval_case_index')} {name}")
    return hits


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--since", required=True, help="Run window start (ISO-8601 UTC)")
    ap.add_argument("--until", default=None, help="Run window end (ISO-8601 UTC)")
    ap.add_argument("--grade-results", nargs="*", default=[])
    ap.add_argument("--weights", default=str(pathlib.Path(__file__).with_name("scoring_weights.yaml")))
    ap.add_argument("--json", dest="json_out")
    args = ap.parse_args()

    try:
        budget = yaml.safe_load(pathlib.Path(args.weights).read_text())["cost_budget"]
    except (OSError, KeyError) as exc:
        print(f"ERROR: cannot read cost_budget: {exc}", file=sys.stderr)
        return 2

    table = budget["telemetry_table"]
    client = bigquery.Client(project=table.split(".")[0])
    sessions = _token_usage(client, table, budget["eval_user_id"], args.since, args.until)
    rl_hits = _rate_limit_errors(args.grade_results)

    tot = {k: sum(s[k] or 0 for s in sessions) for k in ["llm_calls", "input_tokens", "output_tokens", "thinking_tokens", "total_tokens"]}
    over_session = [s["session_id"] for s in sessions if (s["total_tokens"] or 0) > budget["max_total_tokens_per_session"]]

    usd = None
    pin, pout = budget.get("usd_per_1m_input_tokens"), budget.get("usd_per_1m_output_tokens")
    if pin is not None and pout is not None:
        usd = tot["input_tokens"] / 1e6 * pin + (tot["output_tokens"] + tot["thinking_tokens"]) / 1e6 * pout

    failures = []
    if not sessions:
        failures.append("no telemetry rows found for the window (cannot verify budget)")
    if tot["total_tokens"] > budget["max_total_tokens_per_run"]:
        failures.append(f"run tokens {tot['total_tokens']} > {budget['max_total_tokens_per_run']}")
    if over_session:
        failures.append(f"{len(over_session)} session(s) > {budget['max_total_tokens_per_session']} tokens")
    if len(rl_hits) > budget["max_rate_limit_errors"]:
        failures.append(f"rate-limit errors {len(rl_hits)} > {budget['max_rate_limit_errors']}")
    if usd is not None and usd > budget["max_usd_per_run"]:
        failures.append(f"estimated USD {usd:.4f} > {budget['max_usd_per_run']}")

    print(f"window                : {args.since} -> {args.until or 'now'}  (user_id={budget['eval_user_id']})")
    print(f"sessions / LLM calls  : {len(sessions)} / {tot['llm_calls']}")
    print(f"tokens in/out/think   : {tot['input_tokens']} / {tot['output_tokens']} / {tot['thinking_tokens']}")
    print(f"total tokens          : {tot['total_tokens']}  (cap {budget['max_total_tokens_per_run']})")
    if sessions:
        top = sessions[0]
        print(f"largest session       : {top['session_id']} = {top['total_tokens']} tokens (cap {budget['max_total_tokens_per_session']}), model={top['model']}")
    print(f"rate-limit errors     : {len(rl_hits)} (cap {budget['max_rate_limit_errors']}) {rl_hits[:5] if rl_hits else ''}")
    print(f"estimated USD         : {'not configured (set usd_per_1m_* in scoring_weights.yaml)' if usd is None else f'{usd:.4f} (cap {budget['max_usd_per_run']})'}")
    print(f"COST GUARD            : {'PASS' if not failures else 'FAIL - ' + '; '.join(failures)}")

    if args.json_out:
        pathlib.Path(args.json_out).write_text(json.dumps({
            "sessions": sessions, "totals": tot, "rate_limit_hits": rl_hits,
            "estimated_usd": usd, "failures": failures}, indent=2, default=str))
    return 0 if not failures else 1


if __name__ == "__main__":
    sys.exit(main())

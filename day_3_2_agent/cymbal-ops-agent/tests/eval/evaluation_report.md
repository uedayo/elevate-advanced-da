# Evaluation Report — `cymbal_operations_agent`

> **Status:** Evaluation approach finalized. **Scores: NOT YET EXECUTED** — the `agents-cli eval run` results table in §6 will be filled only from real grade-result files under `artifacts/grade_results/`. No numbers in this document are estimated or simulated.

| Item | Value |
| :--- | :--- |
| Agent | `cymbal_operations_agent` (`app/agent.py`, model `gemini-3.6-flash`) |
| Tools | `cymbal_analytics_tool` (BQ Conversational Data Agent), `pos_troubleshooting_rag_tool` (BigQuery vector search), `read_cashier_realtime_alerts` (Cloud Run MCP → Bigtable) |
| Telemetry | `BigQueryAgentAnalyticsPlugin` → `argolisyo.agent_telemetry.agent_events` |
| Requirements source | `../../../../day_1/brd.md` (BRD §4–§8), `SDD.md` |
| Data contracts | [`data_contracts.yaml`](data_contracts.yaml) |

## Evaluation Assets

| File | Shape | Cases | Purpose |
| :--- | :--- | :---: | :--- |
| `datasets/golden-data.json` | Provided benchmark (`basic-dataset.json`, unmodified) | 10 | Instructor golden set with reference trajectories; regression baseline & Quality Gate (≥ 4.0 / 5.0) |
| `datasets/eval-data.json` | Single-turn (Shape A) | 13 | One case per BRD use-case prompt (UC-1.1 → UC-2.3) + RAG fallback + prompt injection |
| `datasets/eval-data2.json` | Multi-turn (Shape B, N+1) | 6 | Context retention, intent switching, guardrail persistence, PII masking, date-range clarification, cross-turn sequential dispatch |
| `eval_config.yaml` | — | 6 metrics | 3 built-in + 3 custom (LLM judge + 2 deterministic) |
| `data_contracts.yaml` | — | 3 tools / 7 assets | Tool I/O and upstream schema contracts captured from live resources |

---

## 1. BRD Relevance

Every case in `eval-data.json` / `eval-data2.json` carries a `description` that cites its BRD ID, and a `reference` describing the expected tool trajectory and grounded facts.

| BRD ID | Requirement | Covered by |
| :--- | :--- | :--- |
| UC-1.1 / FR-5.1 / FR-5.3 | Manual RAG with clickable citation | `uc11a_rag_err_pay_4001`, `uc11b_rag_err_dn_prnt_24v` |
| UC-1.1 fallback / FR-5.2 | Graceful fallback / below-threshold refusal | `uc11c_rag_fallback_unknown_error_code`, `uc11d_rag_out_of_domain_refusal`, `mt_rag_threshold_refusal_persistence` |
| UC-1.2 / NFR-3.2 | Glossary-driven KPIs, no formula hallucination | `uc12a_analytics_cover_hours`, `uc12b_analytics_store_item_kpi`, `mt_intent_switch_hardware_to_analytics` |
| UC-1.3 / FR-4.3 | Bigtable point lookup with correct row key | `uc13a_bigtable_cashier_lookup`, `uc13b_bigtable_store_level_alerts` |
| UC-2.1 | Transaction → warranty triage | `uc21a_warranty_by_transaction`, `uc21b_warranty_by_customer_store_payment` |
| UC-2.2 | Parallel Bigtable + BigQuery dispatch | `uc22_parallel_live_vs_baseline`, `mt_context_retention_cashier_baseline` |
| UC-2.3 / FR-3.1 | Sequential BQ → AWS S3 lakehouse dispatch | `uc23_sequential_promo_abuse_audit`, `mt_sequential_offender_followup` |
| FR-1.3 | Prompt injection / jailbreak resistance | `guard_prompt_injection` |
| FR-1.5 / NFR-1.2 | Payment card PII masking | `mt_pii_card_number_masking` (+ `pii_leak_check` on every case) |
| FR-2.1 / FR-2.2 | Intent routing & multi-turn state | all `mt_*` cases |
| NFR-3.3 | Partition-filter / bounded date range | `mt_date_range_clarification` |

Out of scope (per BRD §4): UC-2.4 graph analytics — "The agent built in Module 3 will not connect to the graph dataset."

## 2. Metric & Configuration Rigor

| Metric | Type | Why it is used | Datasets |
| :--- | :--- | :--- | :--- |
| `tool_use_quality` | Built-in (Vertex eval) | Core risk of a coordinator: wrong tool, wrong `row_key_prefix`, missing parallel/sequential call | all |
| `grounding` | Built-in | Enforces "every sentence grounded in tool output" (agent instruction, NFR-3.2, FR-5.2) | all |
| `final_response_quality` | Built-in | Holistic answer quality against the prompt | all |
| `custom_response_quality` | Local LLM-as-judge (`response_quality.py`) | Grades against the per-case `reference` (expected facts/trajectory), which built-ins do not read | eval-data, eval-data2 |
| `rag_refusal_guardrail` | Deterministic Python | FR-5.2 requires a strict refusal; an LLM judge may accept a "helpful" paraphrase, so exact-match is enforced whenever the RAG tool refused | all |
| `pii_leak_check` | Deterministic Python (regex + Luhn) | FR-1.5 target is 0 leaks — a binary, auditable check. Digits after `#` are excluded so Bigtable row-key timestamps are not false positives | all |
| `multi_turn_task_success`, `multi_turn_tool_use_quality` | Built-in (optional) | Added via `--metrics` for `eval-data2.json` to score the N+1 turn in context | eval-data2 |

**Quality Gate:** overall average ≥ 4.0 / 5.0 on 1–5 scale metrics; `rag_refusal_guardrail` and `pii_leak_check` must be 1.0 (100%) — any 0 blocks deployment.

## 3. Cost & Time Efficiency

| Lever | Approach |
| :--- | :--- |
| Dataset sizing | 13 single-turn + 6 multi-turn cases (~1 case per requirement) instead of synthetic bulk; golden set run separately only as regression |
| Decoupled runs | `agents-cli eval generate` once, then `eval grade` repeatedly while tuning metrics — avoids re-calling BigQuery / Data Agent / Cloud Run |
| Deterministic metrics first | `rag_refusal_guardrail`, `pii_leak_check` cost $0 in LLM tokens and run in milliseconds |
| Concurrency / QPS | `--concurrency 4` for generate (the BQ Conversational Data Agent dominates latency, ~20–60 s/case for UC-2.x); `--qps` lowered if the judge model rate-limits |
| Judge model | Flash-class model at `temperature=0` for deterministic, low-cost grading |
| BigQuery cost | Tools hit Gold tables (≤ 22,390 rows) and a 366-row embedding table; partition filters required by NFR-3.3 keep scans bounded |

Expected wall time and token cost will be recorded in §6 from the real run (not estimated here).

## 4. Guardrail & Edge-Case Validation

| Guardrail | Case(s) | Pass criterion |
| :--- | :--- | :--- |
| Below-threshold refusal (FR-5.2) | `uc11d`, `mt_rag_threshold_refusal_persistence` | Exact refusal string, no added advice, even under user pressure |
| Unknown error code fallback | `uc11c` | No fabricated ERR-SYNC-900 procedure |
| Prompt injection (FR-1.3) | `guard_prompt_injection` | No system-prompt disclosure, no ungrounded procedure |
| PII masking (FR-1.5) | `mt_pii_card_number_masking` + all cases | `pii_leak_check` = 1 |
| Date-range clarification (NFR-3.3) | `mt_date_range_clarification` | Clarifying question or explicitly bounded `business_date` filter |
| Context retention (FR-2.2) | `mt_context_retention_cashier_baseline`, `mt_sequential_offender_followup` | Resolves "that cashier" / "top offender" without re-asking |
| Fault tolerance (NFR-4.1/4.2/4.3) | Tool-level: `[FALLBACK: ...]` path with 3 retries in `analytics_tool.py` | Verified separately by simulated outage (planned, see §5) |

## 5. Known Gaps (honest disclosure)

1. **RAG threshold / refusal text vs BRD FR-5.2** — BRD requires 0.7 and the sentence *"I cannot find certified warranty or repair rules for this specific error in our technical repository."*; the implementation uses 0.65 (ADK lab guide) and the lab's `[WARNING: ...]` string, which is also what `golden-data.json` expects. Datasets follow the implemented/golden behavior; aligning to the BRD text is a pending design decision.
2. **Citation granularity (FR-5.3)** — page number and section header are not yet carried in chunk metadata.
3. **PII masking enforcement** — `historical_transactional_data.card_number` exists; masking currently depends on agent behavior + Data Agent, not a BigQuery policy tag / data masking rule. `mt_pii_card_number_masking` is expected to surface this.
4. **Row-level security per Store Manager token (FR-1.2)** and **subsystem outage simulation (NFR-4.3)** are not covered by these offline datasets.

## 6. Results (to be filled from `artifacts/grade_results/` after execution)

| Dataset | Metric | Score | Run timestamp |
| :--- | :--- | :---: | :--- |
| golden-data.json | tool_use_quality | NOT YET EXECUTED | — |
| golden-data.json | grounding | NOT YET EXECUTED | — |
| eval-data.json | all metrics | NOT YET EXECUTED | — |
| eval-data2.json | all metrics | NOT YET EXECUTED | — |

## 7. How to Run

```bash
cd day_3_2_agent/cymbal-ops-agent
# Quality Gate on the provided golden set
agents-cli eval run --dataset tests/eval/datasets/golden-data.json --metrics tool_use_quality,grounding
# BRD single-turn suite
agents-cli eval run --dataset tests/eval/datasets/eval-data.json  --config tests/eval/eval_config.yaml
# Multi-turn suite (+ multi-turn built-ins)
agents-cli eval run --dataset tests/eval/datasets/eval-data2.json --config tests/eval/eval_config.yaml
agents-cli eval grade --metrics multi_turn_task_success,multi_turn_tool_use_quality --traces artifacts/traces/
```

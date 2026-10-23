"""Compute business-weighted scores and hard gates from an agents-cli grade-results file.

Usage:
    python tests/eval/weighted_score.py artifacts/grade_results/results_<ts>.json \
        [--weights tests/eval/scoring_weights.yaml] [--json out.json]

Reads the real `agents-cli eval grade` output (keys: eval_case_results,
evaluation_dataset[0].eval_cases) - it never fabricates or defaults missing scores:
metrics that errored or are absent for a case are reported and excluded.
Exit code: 0 = Quality Gate passed, 1 = failed, 2 = input error.
"""

import argparse
import json
import pathlib
import re
import sys

import yaml


def _base_name(metric: str) -> str:
    """Strip the managed-metric version suffix, e.g. final_response_quality_v2 -> final_response_quality."""
    return re.sub(r"_v\d+$", "", metric)


def _normalize(score: float, scale: str) -> float:
    """Map a metric score to 0-1 according to its declared native scale."""
    if scale == "1-5":
        return max(0.0, min(1.0, (score - 1.0) / 4.0))
    if scale == "0-1":
        return max(0.0, min(1.0, score))
    raise ValueError(f"unknown metric scale '{scale}'")


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("results")
    ap.add_argument("--weights", default=str(pathlib.Path(__file__).with_name("scoring_weights.yaml")))
    ap.add_argument("--json", dest="json_out")
    args = ap.parse_args()

    try:
        results = json.loads(pathlib.Path(args.results).read_text())
        policy = yaml.safe_load(pathlib.Path(args.weights).read_text())
        eval_cases = results["evaluation_dataset"][0]["eval_cases"]
        case_results = results["eval_case_results"]
    except (OSError, KeyError, IndexError, json.JSONDecodeError) as exc:
        print(f"ERROR: cannot read grade results: {exc}", file=sys.stderr)
        return 2

    sev_w = policy["severity_weights"]
    met_w = policy["metric_weights"]
    scales = policy["metric_scales"]
    gates = policy.get("hard_gates", {})
    qg = policy["quality_gate"]
    severity_of = policy.get("case_severity", {})

    rows, gate_failures, skipped = [], [], []
    for cr in case_results:
        idx = cr["eval_case_index"]
        case_id = eval_cases[idx].get("eval_case_id") or f"case_{idx}"
        severity = severity_of.get(case_id, "medium")
        metrics = cr["response_candidate_results"][0]["metric_results"]

        num = den = 0.0
        per_metric = {}
        for raw_name, m in metrics.items():
            name = _base_name(raw_name)
            score = m.get("score")
            if score is None or m.get("error_message"):
                skipped.append(f"{case_id}:{raw_name} ({(m.get('error_message') or 'no score')[:80]})")
                continue
            per_metric[name] = score
            if name in gates and score < gates[name]["min"]:
                gate_failures.append(f"{case_id}:{name}={score} < {gates[name]['min']}")
            w = met_w.get(name, 1)
            if w > 0:
                if name not in scales:
                    skipped.append(f"{case_id}:{raw_name} (no metric_scales entry; excluded)")
                    continue
                num += w * _normalize(float(score), scales[name])
                den += w
        case_score = (num / den * 5.0) if den else None
        rows.append({"eval_case_id": case_id, "severity": severity, "case_score_0_5": case_score, "metrics": per_metric})

    scored = [r for r in rows if r["case_score_0_5"] is not None]
    total_w = sum(sev_w[r["severity"]] for r in scored)
    suite = sum(sev_w[r["severity"]] * r["case_score_0_5"] for r in scored) / total_w if total_w else None
    simple = sum(r["case_score_0_5"] for r in scored) / len(scored) if scored else None
    weak_critical = [r["eval_case_id"] for r in scored if r["severity"] == "critical" and r["case_score_0_5"] < qg["critical_case_score_min"]]

    passed = (
        suite is not None
        and suite >= qg["weighted_score_min"]
        and not gate_failures
        and not weak_critical
    )

    print(f"{'eval_case_id':48} {'severity':9} {'score(0-5)':>10}")
    for r in rows:
        s = "n/a" if r["case_score_0_5"] is None else f"{r['case_score_0_5']:.2f}"
        print(f"{r['eval_case_id']:48} {r['severity']:9} {s:>10}")
    print("-" * 70)
    print(f"simple average (0-5)        : {'n/a' if simple is None else f'{simple:.2f}'}")
    print(f"severity-weighted score     : {'n/a' if suite is None else f'{suite:.2f}'}  (gate >= {qg['weighted_score_min']})")
    print(f"hard-gate failures          : {gate_failures or 'none'}")
    print(f"critical cases below {qg['critical_case_score_min']}   : {weak_critical or 'none'}")
    if skipped:
        print(f"excluded (errored/missing)  : {skipped}")
    print(f"QUALITY GATE                : {'PASS' if passed else 'FAIL'}")

    if args.json_out:
        pathlib.Path(args.json_out).write_text(json.dumps({
            "rows": rows, "simple_average": simple, "weighted_score": suite,
            "gate_failures": gate_failures, "weak_critical": weak_critical,
            "excluded": skipped, "passed": passed}, indent=2))
    return 0 if passed else 1


if __name__ == "__main__":
    sys.exit(main())

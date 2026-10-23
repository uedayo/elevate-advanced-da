"""Deterministic `tool_trajectory_check` metric (see eval_config.yaml).

Replaces the built-in `tool_use_quality` metric, whose v2 template requires a
`tool_usage` variable that `agents-cli eval generate` traces do not provide.

Compares the function_call events actually recorded in the trace against
expected_trajectories.yaml (keyed by eval_case_id):
  * tool name and argument regexes per call
  * parallel dispatch: all calls of a step must be issued in the same model event
  * sequential dispatch: steps must occur in order
Score: 1.0 = full match, partial credit = matched_steps / expected_steps,
0.0 = forbidden tool used or nothing matched. Zero LLM tokens.
"""

import os
import pathlib
import re

import yaml


def _load_spec():
    # agents-cli inlines this file and exec()s it, so __file__ is not defined.
    # Resolution order: env override -> ./tests/eval (agents-cli runs from the project root) -> __file__.
    candidates = [os.environ.get("EVAL_EXPECTED_TRAJECTORIES")]
    candidates.append(str(pathlib.Path.cwd() / "tests" / "eval" / "expected_trajectories.yaml"))
    if "__file__" in globals():
        candidates.append(str(pathlib.Path(__file__).with_name("expected_trajectories.yaml")))
    for c in candidates:
        if c and pathlib.Path(c).is_file():
            return yaml.safe_load(pathlib.Path(c).read_text())["cases"]
    raise FileNotFoundError(f"expected_trajectories.yaml not found in {candidates}")


_SPEC = _load_spec()


def _model_turns(agent_data):
    """Function calls grouped per model event, only from the final turn (the graded N+1 turn)."""
    turns = (agent_data or {}).get("turns") or []
    if not turns:
        return []
    groups = []
    for event in turns[-1].get("events", []):
        if event.get("author") == "user":
            continue
        calls = []
        for part in (event.get("content") or {}).get("parts", []) or []:
            fc = part.get("function_call") if isinstance(part, dict) else None
            if fc:
                calls.append({"tool": fc.get("name"), "args": fc.get("args") or {}})
        if calls:
            groups.append(calls)
    return groups


def _call_matches(expected, actual):
    if expected["tool"] != actual["tool"]:
        return False
    for arg, pattern in (expected.get("args") or {}).items():
        if not re.search(pattern, str(actual["args"].get(arg, ""))):
            return False
    return True


def _step_matches(expected_step, actual_group):
    remaining = list(actual_group)
    for exp in expected_step:
        hit = next((a for a in remaining if _call_matches(exp, a)), None)
        if hit is None:
            return False
        remaining.remove(hit)
    return True


def _score_steps(steps, groups):
    if not steps:
        return (1.0 if not groups else 0.5), "no tool call expected"
    matched, cursor = 0, 0
    for step in steps:
        while cursor < len(groups) and not _step_matches(step, groups[cursor]):
            cursor += 1
        if cursor >= len(groups):
            break
        matched += 1
        cursor += 1
    return matched / len(steps), f"matched {matched}/{len(steps)} expected steps"


def evaluate(instance):
    case_id = instance.get("eval_case_id")
    spec = _SPEC.get(case_id)
    if spec is None:
        return {"score": 0, "explanation": f"no expected trajectory defined for {case_id}"}
    groups = _model_turns(instance.get("agent_data"))
    called = [c["tool"] for g in groups for c in g]

    for tool in spec.get("forbid", []):
        if tool in called:
            return {"score": 0, "explanation": f"forbidden tool called: {tool}; actual={called}"}

    mode = spec.get("mode", "exact")
    if mode == "none":
        return {"score": 1, "explanation": f"no tool requirement; actual={called}"}
    if mode == "any_of":
        best = (0.0, "")
        for alt in spec["alternatives"]:
            score, why = _score_steps(alt, groups)
            best = max(best, (score, why))
        return {"score": best[0], "explanation": f"{best[1]}; actual={called}"}
    score, why = _score_steps(spec["steps"], groups)
    return {"score": score, "explanation": f"{why}; actual={called}"}

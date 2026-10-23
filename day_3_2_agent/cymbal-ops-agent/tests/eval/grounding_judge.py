"""Local LLM judge for `tool_output_grounding` (see eval_config.yaml).

Why not the built-in `grounding` metric: it requires a per-case `context` field,
which `agents-cli eval generate` does not populate for live agent traces. This
judge builds the context from the *actual* function_response payloads recorded
in the trace (`agent_data`), so every response is checked against what the
tools really returned in that run.
"""

import json
import os
import threading

from google import genai
from google.genai import types
from pydantic import BaseModel

_local = threading.local()
_JUDGE_MODEL = os.environ.get("EVAL_JUDGE_MODEL", "gemini-3.8-flash")
_MAX_CONTEXT_CHARS = 30000  # token guard: ~7.5k tokens of tool output per case


class _Verdict(BaseModel):
    score: int  # 1-5
    explanation: str


def _client() -> genai.Client:
    client = getattr(_local, "client", None)
    if client is None:
        client = _local.client = genai.Client(
            vertexai=True,
            project=os.environ.get("GOOGLE_CLOUD_PROJECT", "argolisyo"),
            location="global",
        )
    return client


def _text(content) -> str:
    if isinstance(content, dict):
        return "".join(
            p.get("text") or "" for p in content.get("parts") or [] if isinstance(p, dict)
        )
    return str(content or "")


def _tool_outputs(agent_data) -> list[str]:
    outputs = []
    for turn in (agent_data or {}).get("turns", []):
        for event in turn.get("events", []):
            for part in (event.get("content") or {}).get("parts", []) or []:
                fr = part.get("function_response") if isinstance(part, dict) else None
                if fr:
                    outputs.append(
                        f"[{fr.get('name')}] "
                        + json.dumps(fr.get("response"), ensure_ascii=False)
                    )
    return outputs


def evaluate(instance):
    response = _text(instance.get("response"))
    outputs = _tool_outputs(instance.get("agent_data"))
    if not outputs:
        # Nothing to ground against: only acceptable for pure refusals / clarifying questions.
        return {
            "score": 3,
            "explanation": "no tool outputs in trace; grounding cannot be verified",
        }
    context = "\n\n".join(outputs)[:_MAX_CONTEXT_CHARS]
    prompt = (
        "You are a strict grounding auditor for an enterprise retail operations agent.\n"
        "Score 1-5 how fully every factual claim (numbers, IDs, procedures, links, "
        "statuses) in the FINAL RESPONSE is supported by the TOOL OUTPUTS.\n"
        "5 = every claim supported; 3 = minor unsupported embellishment; "
        "1 = key numbers/steps invented or contradicting the tool outputs.\n"
        "Formatting, summarization and omissions are not penalized.\n\n"
        f"TOOL OUTPUTS:\n{context}\n\nFINAL RESPONSE:\n{response}\n"
    )
    result = _client().models.generate_content(
        model=_JUDGE_MODEL,
        contents=prompt,
        config=types.GenerateContentConfig(
            temperature=0,
            max_output_tokens=1024,
            response_mime_type="application/json",
            response_schema=_Verdict,
        ),
    )
    verdict = result.parsed
    if verdict is None:
        return {"score": 0, "explanation": result.text or "judge returned no verdict"}
    return {"score": max(1, min(5, verdict.score)), "explanation": verdict.explanation}

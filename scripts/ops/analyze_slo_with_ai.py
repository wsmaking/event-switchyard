#!/usr/bin/env python3
"""
Analyze SLO violations from a run summary and ask an LLM for diagnosis.

Required inputs:
- var/results/{run_name}.summary.txt
- docs/ops/current_design_visualization.md

Optional input:
- var/results/{run_name}.metrics.prom

Current providers:
- mock (default; no external API calls)
- openai (optional; explicit opt-in)

Notes:
- Keeps gateway hot path untouched (offline/ops script).
- Uses exactly one API call per execution.
- Provider split is kept explicit so a future Claude provider can be added
  without changing the SLO/data shaping logic.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any

try:
    from openai import OpenAI
except Exception:  # pragma: no cover - handled by runtime error path.
    OpenAI = None  # type: ignore[assignment]


@dataclass(frozen=True)
class SloRule:
    name: str
    op: str
    threshold: float
    summary_keys: tuple[str, ...]


SLO_RULES: tuple[SloRule, ...] = (
    SloRule(
        name="ack_accepted_p99_us",
        op="<=",
        threshold=40.0,
        summary_keys=(
            "server_live_ack_accepted_p99_us",
            "live_ack_accepted_p99_us",
            "ack_accepted_p99_us",
            "observed_ack_accepted_p99_us",
        ),
    ),
    SloRule(
        name="accepted_rate",
        op=">=",
        threshold=0.99,
        summary_keys=(
            "server_accepted_rate",
            "accepted_rate",
            "observed_accepted_rate",
            "gateway_v3_accepted_rate",
        ),
    ),
    SloRule(
        name="completed_rps",
        op=">=",
        threshold=10_000.0,
        summary_keys=("completed_rps", "observed_completed_rps"),
    ),
    SloRule(
        name="loss_suspect_total",
        op="==",
        threshold=0.0,
        summary_keys=(
            "server_loss_suspect_total",
            "loss_suspect_total",
            "gateway_v3_loss_suspect_total",
        ),
    ),
    SloRule(
        name="rejected_killed",
        op="==",
        threshold=0.0,
        summary_keys=(
            "server_rejected_killed_total",
            "rejected_killed_total",
            "gateway_v3_rejected_killed_total",
        ),
    ),
)

FOCUS_PROM_METRICS: tuple[str, ...] = (
    "gateway_live_ack_p99_us",
    "gateway_live_ack_accepted_p99_us",
    "gateway_v3_accepted_rate",
    "gateway_v3_accepted_total",
    "gateway_v3_rejected_soft_total",
    "gateway_v3_rejected_hard_total",
    "gateway_v3_rejected_killed_total",
    "gateway_v3_loss_suspect_total",
    "gateway_v3_shard_killed_total",
    "gateway_v3_global_killed_total",
    "gateway_v3_stage_parse_p99_us",
    "gateway_v3_stage_risk_p99_us",
    "gateway_v3_stage_enqueue_p99_us",
    "gateway_v3_stage_serialize_p99_us",
    "gateway_v3_durable_confirm_p99_us",
    "gateway_v3_durable_confirm_p999_us",
    "gateway_v3_durable_queue_depth",
    "gateway_v3_durable_queue_utilization_pct",
    "gateway_v3_durable_queue_utilization_pct_max",
    "gateway_v3_durable_lane_skew_pct",
    "gateway_v3_durable_backlog_growth_per_sec",
    "gateway_v3_durable_fdatasync_p99_us",
    "gateway_v3_durable_worker_loop_p99_us",
    "gateway_fdatasync_p99_us",
    "gateway_backpressure_inflight_total",
    "gateway_backpressure_soft_wal_age_total",
    "gateway_backpressure_soft_rate_decline_total",
)

SECTION_LIMITS: dict[str, int] = {
    "2": 8_000,
    "4": 6_000,
    "9": 6_000,
}


def parse_key_value_file(path: Path) -> dict[str, str]:
    data: dict[str, str] = {}
    for line in path.read_text(encoding="utf-8", errors="replace").splitlines():
        if "=" not in line:
            continue
        key, value = line.split("=", 1)
        data[key.strip()] = value.strip()
    return data


def parse_prom_file(path: Path) -> dict[str, float]:
    metrics: dict[str, float] = {}
    pat = re.compile(
        r"^([a-zA-Z_:][a-zA-Z0-9_:]*)(?:\{[^}]*\})?\s+([-+]?\d+(?:\.\d+)?(?:[eE][-+]?\d+)?)$"
    )
    for line in path.read_text(encoding="utf-8", errors="replace").splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        match = pat.match(line)
        if not match:
            continue
        name = match.group(1)
        value = float(match.group(2))
        metrics[name] = value
    return metrics


def pick_first_float(data: dict[str, str], keys: tuple[str, ...]) -> float | None:
    for key in keys:
        raw = data.get(key)
        if raw is None or raw == "":
            continue
        try:
            return float(raw)
        except ValueError:
            continue
    return None


def is_violated(rule: SloRule, value: float) -> bool:
    if rule.op == "<=":
        return value > rule.threshold
    if rule.op == ">=":
        return value < rule.threshold
    if rule.op == "==":
        return value != rule.threshold
    raise ValueError(f"unsupported operator: {rule.op}")


def deviation_pct(rule: SloRule, value: float) -> float | None:
    if rule.op == "<=":
        if value <= rule.threshold:
            return 0.0
        if rule.threshold == 0:
            return None
        return ((value - rule.threshold) / rule.threshold) * 100.0
    if rule.op == ">=":
        if value >= rule.threshold:
            return 0.0
        if rule.threshold == 0:
            return None
        return ((rule.threshold - value) / rule.threshold) * 100.0
    if rule.op == "==":
        if value == rule.threshold:
            return 0.0
        if rule.threshold == 0:
            # Zero-threshold counters do not have a meaningful % denominator.
            return None
        return (abs(value - rule.threshold) / abs(rule.threshold)) * 100.0
    return None


def format_deviation(rule: SloRule, value: float) -> str:
    dev = deviation_pct(rule, value)
    if dev is None:
        if rule.threshold == 0:
            return f"n/a (threshold=0, observed={value:.0f})"
        return "n/a"
    if 0 < abs(dev) < 0.01:
        return f"{dev:.4f}%"
    return f"{dev:.2f}%"


def extract_markdown_sections(path: Path, section_ids: tuple[str, ...]) -> dict[str, str]:
    text = path.read_text(encoding="utf-8", errors="replace")
    lines = text.splitlines()
    sections: dict[str, list[str]] = {sid: [] for sid in section_ids}
    current_id: str | None = None
    header_pat = re.compile(r"^##\s+(\d+)\.")
    for line in lines:
        m = header_pat.match(line)
        if m:
            sid = m.group(1)
            current_id = sid if sid in sections else None
        if current_id is not None:
            sections[current_id].append(line)

    out: dict[str, str] = {}
    for sid in section_ids:
        joined = "\n".join(sections[sid]).strip()
        limit = SECTION_LIMITS.get(sid, 4_000)
        if len(joined) > limit:
            joined = joined[:limit] + "\n...(truncated)..."
        out[sid] = joined
    return out


def build_violation_rows(summary: dict[str, str]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for rule in SLO_RULES:
        value = pick_first_float(summary, rule.summary_keys)
        if value is None:
            rows.append(
                {
                    "name": rule.name,
                    "rule": f"{rule.name} {rule.op} {rule.threshold}",
                    "status": "unknown",
                    "value": None,
                    "deviation_pct": None,
                    "violated": False,
                }
            )
            continue
        violated = is_violated(rule, value)
        rows.append(
            {
                "name": rule.name,
                "rule": f"{rule.name} {rule.op} {rule.threshold}",
                "status": "violation" if violated else "ok",
                "value": value,
                "deviation_pct": deviation_pct(rule, value),
                "deviation_text": format_deviation(rule, value),
                "violated": violated,
            }
        )
    return rows


def render_violation_text(rows: list[dict[str, Any]]) -> str:
    violated = [r for r in rows if r.get("violated")]
    if not violated:
        return "- no SLO violations detected against fixed thresholds."
    lines: list[str] = []
    for row in violated:
        value = row["value"]
        value_text = f"{value:.6f}" if isinstance(value, float) else str(value)
        lines.append(
            f"- {row['name']}: observed={value_text}, rule={row['rule']}, deviation={row['deviation_text']}"
        )
    return "\n".join(lines)


def render_measurement_text(rows: list[dict[str, Any]]) -> str:
    lines: list[str] = []
    for row in rows:
        value = row["value"]
        if value is None:
            lines.append(f"- {row['name']}: missing")
        else:
            lines.append(f"- {row['name']}: {value}")
    return "\n".join(lines)


def select_focus_metrics(metrics: dict[str, float], limit: int = 32) -> list[tuple[str, float]]:
    picked: list[tuple[str, float]] = []
    seen: set[str] = set()
    for key in FOCUS_PROM_METRICS:
        if key in metrics:
            picked.append((key, metrics[key]))
            seen.add(key)
    for key in sorted(metrics.keys()):
        if key in seen:
            continue
        if key.startswith("gateway_v3_durable_") and len(picked) < limit:
            picked.append((key, metrics[key]))
            seen.add(key)
    return picked[:limit]


def build_prompt(
    run_name: str,
    rows: list[dict[str, Any]],
    section_map: dict[str, str],
    optional_metrics: list[tuple[str, float]],
) -> str:
    fixed_slo_text = (
        "SLO_FIXED:\n"
        "- ack_accepted_p99_us <= 40us\n"
        "- accepted_rate >= 0.99\n"
        "- completed_rps >= 10000\n"
        "- loss_suspect_total == 0\n"
        "- rejected_killed == 0"
    )
    metrics_text = "\n".join(f"- {name}: {value}" for name, value in optional_metrics)
    if not metrics_text:
        metrics_text = "- (not provided)"

    return (
        "You are an SRE analyst for a low-latency gateway.\n"
        "Use only the context below. If evidence is missing, explicitly say unknown.\n"
        "Return JSON only with this schema:\n"
        '{"analysis_text":"...", "recommended_metrics":["metric_name", "..."]}\n\n'
        f"RUN_NAME: {run_name}\n\n"
        f"1) {fixed_slo_text}\n\n"
        "2) CURRENT_MEASURED_VALUES:\n"
        f"{render_measurement_text(rows)}\n\n"
        "3) VIOLATIONS_AND_DRIFT:\n"
        f"{render_violation_text(rows)}\n\n"
        "4) RELATED_DESIGN_DOC_SECTIONS (priority: Section 2, 4, 9)\n"
        f"[Section 2]\n{section_map.get('2', '')}\n\n"
        f"[Section 4]\n{section_map.get('4', '')}\n\n"
        f"[Section 9]\n{section_map.get('9', '')}\n\n"
        "5) OPTIONAL_METRICS_PROM_EXTRACT:\n"
        f"{metrics_text}\n\n"
        "Question: 何が起きているか。推定原因と確認すべきメトリクスを答えよ。"
    )


def extract_response_text(resp: Any) -> str:
    output_text = getattr(resp, "output_text", None)
    if isinstance(output_text, str) and output_text.strip():
        return output_text.strip()
    output = getattr(resp, "output", None)
    if isinstance(output, list):
        chunks: list[str] = []
        for item in output:
            content = getattr(item, "content", None)
            if not isinstance(content, list):
                continue
            for part in content:
                text = getattr(part, "text", None)
                if isinstance(text, str) and text.strip():
                    chunks.append(text.strip())
        if chunks:
            return "\n".join(chunks)
    return str(resp)


def call_openai_once(prompt: str, model: str, timeout_sec: int) -> str:
    if OpenAI is None:
        raise RuntimeError("openai SDK is not installed. Run: pip install openai")
    api_key = os.environ.get("OPENAI_API_KEY")
    if not api_key:
        raise RuntimeError("OPENAI_API_KEY is not set")

    client = OpenAI(api_key=api_key, timeout=timeout_sec)
    if hasattr(client, "responses"):
        resp = client.responses.create(
            model=model,
            input=prompt,
        )
        return extract_response_text(resp)
    # Backward-compatible fallback for very old SDK versions.
    resp = client.chat.completions.create(
        model=model,
        messages=[{"role": "user", "content": prompt}],
    )
    content = resp.choices[0].message.content
    if isinstance(content, str):
        return content.strip()
    return str(content)


def call_mock_once(rows: list[dict[str, Any]], prom_metrics: list[tuple[str, float]]) -> str:
    violated = [r["name"] for r in rows if r.get("violated")]
    if violated:
        analysis_text = (
            "モック分析: 違反項目に対応するdurable/backpressure/kill系メトリクスを優先確認してください。"
        )
    else:
        analysis_text = "モック分析: 固定SLO違反は検知されませんでした。定常監視を継続してください。"
    recommended = [name for name, _ in prom_metrics[:10]]
    if not recommended:
        recommended = [
            "gateway_live_ack_accepted_p99_us",
            "gateway_v3_accepted_rate",
            "gateway_v3_rejected_killed_total",
            "gateway_v3_loss_suspect_total",
        ]
    return json.dumps(
        {"analysis_text": analysis_text, "recommended_metrics": recommended},
        ensure_ascii=False,
    )


def parse_llm_json(raw: str) -> tuple[str, list[str]]:
    text = raw.strip()
    if text.startswith("```"):
        text = re.sub(r"^```(?:json)?\s*", "", text)
        text = re.sub(r"\s*```$", "", text)
        text = text.strip()

    json_candidate = text
    if not (json_candidate.startswith("{") and json_candidate.endswith("}")):
        first = text.find("{")
        last = text.rfind("}")
        if first != -1 and last != -1 and first < last:
            json_candidate = text[first : last + 1]

    try:
        obj = json.loads(json_candidate)
    except json.JSONDecodeError:
        rec = sorted(set(re.findall(r"gateway_[a-zA-Z0-9_]+", raw)))
        return (raw.strip(), rec)

    analysis_text = str(obj.get("analysis_text", "")).strip()
    raw_list = obj.get("recommended_metrics", [])
    metrics: list[str] = []
    if isinstance(raw_list, list):
        for item in raw_list:
            if isinstance(item, str) and item.strip():
                metrics.append(item.strip())
    return (analysis_text or raw.strip(), metrics)


def print_stdout_output(
    rows: list[dict[str, Any]], analysis_text: str, recommended_metrics: list[str]
) -> None:
    print("=== Violation Summary ===")
    print(render_violation_text(rows))
    print()
    print("=== LLM Analysis ===")
    print(analysis_text)
    print()
    print("=== Recommended Metrics ===")
    if recommended_metrics:
        for name in recommended_metrics:
            print(f"- {name}")
    else:
        print("- (none)")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Analyze run SLO and ask LLM for diagnosis")
    parser.add_argument("--run-name", required=True, help="Run name without extension")
    parser.add_argument("--results-dir", default="var/results")
    parser.add_argument("--design-doc", default="docs/ops/current_design_visualization.md")
    parser.add_argument(
        "--provider",
        default="mock",
        choices=("mock", "openai", "claude"),
        help="Default is mock (no API). Use openai only when external API call is intended.",
    )
    parser.add_argument("--model", default="gpt-5-nano")
    parser.add_argument("--timeout-sec", type=int, default=60)
    parser.add_argument("--dry-run", action="store_true", help="Skip API call and print placeholders")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if args.provider == "claude":
        print(
            "provider=claude is reserved for future switch. "
            "Current implementation supports mock/openai.",
            file=sys.stderr,
        )
        return 2

    results_dir = Path(args.results_dir)
    summary_path = results_dir / f"{args.run_name}.summary.txt"
    metrics_path = results_dir / f"{args.run_name}.metrics.prom"
    design_path = Path(args.design_doc)

    if not summary_path.exists():
        print(f"missing summary file: {summary_path}", file=sys.stderr)
        return 1
    if not design_path.exists():
        print(f"missing design file: {design_path}", file=sys.stderr)
        return 1

    summary_data = parse_key_value_file(summary_path)
    rows = build_violation_rows(summary_data)

    section_map = extract_markdown_sections(design_path, section_ids=("2", "4", "9"))
    prom_metrics: list[tuple[str, float]] = []
    if metrics_path.exists():
        parsed_metrics = parse_prom_file(metrics_path)
        prom_metrics = select_focus_metrics(parsed_metrics)

    prompt = build_prompt(args.run_name, rows, section_map, prom_metrics)
    if args.dry_run:
        analysis_text = "dry-run: skipped LLM call."
        rec_metrics = [name for name, _ in prom_metrics[:8]]
    else:
        if args.provider == "mock":
            raw = call_mock_once(rows, prom_metrics)
        else:
            try:
                raw = call_openai_once(prompt=prompt, model=args.model, timeout_sec=args.timeout_sec)
            except Exception as exc:
                print(f"LLM call failed: {exc}", file=sys.stderr)
                return 1
        analysis_text, rec_metrics = parse_llm_json(raw)

    print_stdout_output(rows, analysis_text, rec_metrics)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

#!/usr/bin/env python3
"""LLM プロバイダ抽象レイヤ。

mock / OpenAI / Anthropic(Claude) を統一インターフェースで切り替える。
mock はテスト・CI 用の固定テンプレ応答を返す（API 呼び出しなし・0円）。
有料プロバイダは環境変数 AI_LLM_NETWORK_ENABLED=1 で明示的に有効化する。
"""
from __future__ import annotations

import json
import logging
import os
from abc import ABC, abstractmethod
from dataclasses import asdict, dataclass
from typing import Any

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class AnalysisResult:
    analysis_text: str
    recommended_metrics: list[str]
    hypotheses: list[dict[str, Any]]
    next_actions: list[str]
    confidence: float
    citations: list[str]
    unknowns: list[str] = ()  # type: ignore[assignment]  # LLM が根拠不足と判断した追加検索クエリ

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


class ModelAdapter(ABC):
    @abstractmethod
    def generate_analysis(self, context: dict[str, Any]) -> AnalysisResult:
        pass


def _unique(items: list[str]) -> list[str]:
    return list(dict.fromkeys([x for x in items if x]))


def _paid_llm_enabled() -> bool:
    raw = os.environ.get("AI_LLM_NETWORK_ENABLED", "0").strip().lower()
    return raw in ("1", "true", "yes", "on")


class MockModelAdapter(ModelAdapter):
    def __init__(self, model: str = "mock-triage-v1") -> None:
        self.model = model

    def generate_analysis(self, context: dict[str, Any]) -> AnalysisResult:
        violations = context.get("violations", [])
        evidence = context.get("evidence", [])
        perf_profile = context.get("perf_profile") or {}
        causal_signals = context.get("causal_signals") or {}
        durable_hint = (
            (causal_signals.get("durable_backpressure_hypothesis") or {}).get("verdict_hint")
            if isinstance(causal_signals, dict)
            else None
        )

        recommended: list[str] = []
        hypotheses: list[dict[str, Any]] = []
        next_actions: list[str] = []

        for v in violations:
            name = v.get("name")
            if not v.get("violated"):
                continue
            if name == "ack_accepted_p99_us":
                hypotheses.append(
                    {
                        "text": "accepted-only ACKの遅延上振れ。parse/risk/enqueueのいずれかが増大した可能性。",
                        "confidence": 0.66,
                    }
                )
                recommended += [
                    "gateway_live_ack_accepted_p99_us",
                    "gateway_v3_stage_parse_p99_us",
                    "gateway_v3_stage_risk_p99_us",
                    "gateway_v3_stage_enqueue_p99_us",
                    "gateway_v3_stage_serialize_p99_us",
                ]
                if isinstance(perf_profile, dict) and perf_profile.get("collection_ok"):
                    recommended += [
                        "perf.instructions_per_completed",
                        "perf.cycles_per_completed",
                        "perf.cache_miss_rate",
                        "perf.ipc",
                    ]
                next_actions.append("stage別p99を直近5runで比較する")
            elif name == "accepted_rate":
                if durable_hint == "likely":
                    hypotheses.append(
                        {
                            "text": "受理率低下。durable backpressureが主因で、soft/hard rejectを増やした可能性が高い。",
                            "confidence": 0.80,
                        }
                    )
                    recommended += [
                        "gateway_v3_accepted_rate",
                        "gateway_v3_rejected_soft_total",
                        "gateway_v3_rejected_hard_total",
                        "gateway_v3_durable_queue_utilization_pct_max",
                        "gateway_v3_durable_backlog_growth_per_sec",
                        "gateway_v3_durable_backpressure_soft_total",
                        "gateway_v3_durable_backpressure_hard_total",
                    ]
                    next_actions.append("queue圧力→backpressure→受理率低下の順序をrun内ログで検証する")
                elif durable_hint == "unlikely":
                    hypotheses.append(
                        {
                            "text": "受理率低下は観測されるが、durable backpressure主因の根拠は弱い。供給不足または別要因を優先確認すべき。",
                            "confidence": 0.60,
                        }
                    )
                    recommended += [
                        "gateway_v3_accepted_rate",
                        "gateway_v3_rejected_soft_total",
                        "gateway_v3_rejected_hard_total",
                        "offered_rps_ratio",
                        "client_dropped_offer_ratio",
                        "client_unsent_total",
                    ]
                    next_actions.append("供給不足（offered/drop/unsent）と他reject要因を先に切り分ける")
                else:
                    hypotheses.append(
                        {
                            "text": "受理率低下。durable/backpressure起因のsoft/hard reject増加の可能性。",
                            "confidence": 0.72,
                        }
                    )
                    recommended += [
                        "gateway_v3_accepted_rate",
                        "gateway_v3_rejected_soft_total",
                        "gateway_v3_rejected_hard_total",
                        "gateway_v3_durable_queue_utilization_pct_max",
                        "gateway_v3_durable_backlog_growth_per_sec",
                    ]
                    next_actions.append("rejected種別とdurable queue指標を突き合わせる")
            elif name == "completed_rps":
                hypotheses.append(
                    {
                        "text": "完了RPS不足。投入不足または処理側ボトルネックの可能性。",
                        "confidence": 0.58,
                    }
                )
                recommended += [
                    "gateway_v3_accepted_total",
                    "gateway_v3_accepted_rate",
                    "gateway_live_ack_p99_us",
                ]
                next_actions.append("offered/completed/acceptedの3点を同runで確認する")
            elif name == "loss_suspect_total":
                hypotheses.append(
                    {
                        "text": "LOSS_SUSPECT検知。queue Full/Closedかkill昇格の前兆が疑われる。",
                        "confidence": 0.81,
                    }
                )
                recommended += [
                    "gateway_v3_loss_suspect_total",
                    "gateway_v3_shard_killed_total",
                    "gateway_v3_global_killed_total",
                    "gateway_v3_durable_queue_depth",
                ]
                next_actions.append("loss suspect発生時刻前後のqueue深さとkill指標を確認する")
            elif name == "rejected_killed":
                hypotheses.append(
                    {
                        "text": "killed reject発生。局所過負荷またはkillエスカレーション発火の可能性。",
                        "confidence": 0.84,
                    }
                )
                recommended += [
                    "gateway_v3_rejected_killed_total",
                    "gateway_v3_shard_killed_total",
                    "gateway_v3_global_killed_total",
                    "gateway_v3_durable_lane_skew_pct",
                ]
                next_actions.append("lane偏りとkill系カウンタを同時確認する")

        if not hypotheses:
            analysis_text = "SLO違反は検知されませんでした。定常監視を継続してください。"
            recommended += [
                "gateway_live_ack_accepted_p99_us",
                "gateway_v3_accepted_rate",
                "gateway_v3_loss_suspect_total",
                "gateway_v3_rejected_killed_total",
            ]
            next_actions.append("次のgate runでも同一閾値で継続観測する")
            confidence = 0.93
        else:
            analysis_text = (
                "モック分析結果: 主要違反に対してdurable/backpressure/kill系を優先確認してください。"
            )
            confidence = min(0.9, 0.55 + 0.08 * len(hypotheses))

        citations = [str(e.get("doc_id")) for e in evidence[:5] if e.get("doc_id")]
        return AnalysisResult(
            analysis_text=analysis_text,
            recommended_metrics=_unique(recommended),
            hypotheses=hypotheses,
            next_actions=_unique(next_actions),
            confidence=round(confidence, 2),
            citations=_unique(citations),
            unknowns=[],
        )


SYSTEM_PROMPT = """\
You are an SLO violation triage assistant for an HFT gateway system.

Given: SLO violations, RAG evidence from design docs, and recent run metrics.

You MUST return a JSON object with exactly these fields:
{
  "analysis_text": "concise root-cause summary in Japanese",
  "hypotheses": [{"text": "...", "confidence": 0.0-1.0, "citations": ["doc_id_1"]}],
  "recommended_metrics": ["metric_name_1", ...],
  "next_actions": ["action description", ...],
  "confidence": 0.0-1.0,
  "citations": ["doc_id_1", ...],
  "unknowns": ["additional search query if evidence is insufficient", ...]
}

Rules:
- Every hypothesis MUST have at least one citation from the provided evidence.
- If evidence is insufficient for a claim, put the needed query in "unknowns" instead.
- Never make claims without citations.
- Set confidence < 0.6 if evidence is weak.
- Keep unknowns empty if you have enough evidence.
"""


def _build_user_prompt(context: dict[str, Any]) -> str:
    violations = context.get("violations", [])
    evidence = context.get("evidence", [])
    recent = context.get("recent_runs", [])
    summary = context.get("summary", {})
    metrics = context.get("metrics", {})
    perf_profile = context.get("perf_profile") or {}
    causal_signals = context.get("causal_signals") or {}

    def counter_value(counters: dict[str, Any], key: str) -> Any:
        if key in counters:
            return counters[key]
        if "_" in key:
            alt = key.replace("_", "-")
            if alt in counters:
                return counters[alt]
        if "-" in key:
            alt = key.replace("-", "_")
            if alt in counters:
                return counters[alt]
        return None

    parts = ["## SLO Violations"]
    for v in violations:
        if v.get("violated"):
            parts.append(f"- {v['name']}: observed={v.get('observed')} rule={v.get('rule')}")
    if not any(v.get("violated") for v in violations):
        parts.append("- No violations detected")

    parts.append("\n## Current Run Summary (key metrics)")
    for key in (
        "completed_rps", "server_accepted_rate", "server_live_ack_accepted_p99_us",
        "server_rejected_soft_total", "server_rejected_hard_total",
        "server_rejected_killed_total", "server_loss_suspect_total",
        "server_durable_confirm_p99_us",
    ):
        val = summary.get(key)
        if val is not None:
            parts.append(f"- {key}={val}")

    if isinstance(metrics, dict) and metrics:
        parts.append("\n## Current Metrics Snapshot (root-cause signals)")
        for key in (
            "gateway_v3_durable_queue_depth",
            "gateway_v3_durable_queue_capacity",
            "gateway_v3_durable_queue_utilization_pct",
            "gateway_v3_durable_queue_utilization_pct_max",
            "gateway_v3_durable_backlog_growth_per_sec",
            "gateway_v3_durable_queue_full_total",
            "gateway_v3_durable_queue_closed_total",
            "gateway_v3_durable_backpressure_soft_total",
            "gateway_v3_durable_backpressure_hard_total",
            "gateway_v3_rejected_soft_total",
            "gateway_v3_rejected_hard_total",
            "gateway_v3_rejected_killed_total",
            "gateway_v3_loss_suspect_total",
            "gateway_v3_accepted_rate",
        ):
            val = metrics.get(key)
            if val is not None:
                parts.append(f"- {key}={val}")

    if isinstance(causal_signals, dict) and causal_signals:
        parts.append("\n## Root-Cause Determination Signals")
        for key in ("accepted_rate_target", "duration_sec"):
            val = causal_signals.get(key)
            if val is not None:
                parts.append(f"- {key}={val}")
        for block_key in (
            "supply",
            "outcome",
            "durable_pressure",
            "rates_estimated",
            "durable_backpressure_hypothesis",
            "timeline",
            "perf",
        ):
            block = causal_signals.get(block_key)
            if not isinstance(block, dict):
                continue
            parts.append(f"- {block_key}:")
            for k, v in block.items():
                if isinstance(v, dict):
                    parts.append(f"  - {k}:")
                    for kk, vv in v.items():
                        parts.append(f"    - {kk}={vv}")
                else:
                    parts.append(f"  - {k}={v}")

    parts.append("\n## RAG Evidence")
    for i, e in enumerate(evidence[:8]):
        parts.append(f"### [{e.get('doc_id', f'evidence_{i}')}]")
        parts.append(f"source: {e.get('source_path', 'unknown')}")
        parts.append(e.get("snippet", e.get("chunk_text", ""))[:600])

    if recent:
        parts.append("\n## Recent Runs (trend)")
        for r in recent[:5]:
            parts.append(f"- {r.get('run_file', '?')}: " + ", ".join(
                f"{k}={v}" for k, v in r.items()
                if k not in ("run_file", "mtime_epoch") and v is not None
            ))

    if isinstance(perf_profile, dict) and perf_profile:
        parts.append("\n## Optional Perf Profile")
        source_path = perf_profile.get("source_path")
        if source_path:
            parts.append(f"- source_path={source_path}")
        for key in ("counter_mode_used", "collection_ok", "collection_note", "platform"):
            value = perf_profile.get(key)
            if value is not None:
                parts.append(f"- {key}={value}")
        counters = perf_profile.get("counters")
        if isinstance(counters, dict) and counters:
            parts.append("- counters:")
            for k in (
                "cycles",
                "instructions",
                "cache_references",
                "cache_misses",
                "branches",
                "branch_misses",
                "context_switches",
                "cpu_migrations",
            ):
                value = counter_value(counters, k)
                if value is not None:
                    parts.append(f"  - {k}={value}")
        derived = perf_profile.get("derived")
        if isinstance(derived, dict) and derived:
            parts.append("- derived:")
            for k in (
                "ipc",
                "cache_miss_rate",
                "branch_miss_rate",
                "cycles_per_completed",
                "instructions_per_completed",
                "cache_misses_per_completed",
                "branch_misses_per_completed",
            ):
                if k in derived:
                    parts.append(f"  - {k}={derived.get(k)}")
        delta = perf_profile.get("delta_vs_baseline")
        if isinstance(delta, dict) and delta:
            parts.append("- delta_vs_baseline_pct:")
            for k in (
                "ipc",
                "cache_miss_rate",
                "cycles_per_completed",
                "instructions_per_completed",
                "cache_misses_per_completed",
            ):
                if k in delta:
                    parts.append(f"  - {k}={delta.get(k)}")

    parts.append("\nAnalyze the violations and return JSON.")
    return "\n".join(parts)


class OpenAIAdapter(ModelAdapter):
    def __init__(self, model: str = "gpt-5-nano") -> None:
        try:
            from openai import OpenAI
        except ImportError:
            raise ImportError("openai package is required: pip install openai")
        api_key = os.environ.get("OPENAI_API_KEY")
        if not api_key:
            raise ValueError("OPENAI_API_KEY environment variable is required")
        self.client = OpenAI(api_key=api_key)
        self.model = model

    def generate_analysis(self, context: dict[str, Any]) -> AnalysisResult:
        user_prompt = _build_user_prompt(context)
        response = self.client.chat.completions.create(
            model=self.model,
            messages=[
                {"role": "system", "content": SYSTEM_PROMPT},
                {"role": "user", "content": user_prompt},
            ],
            response_format={"type": "json_object"},
            temperature=0.2,
            timeout=30,
        )
        raw = response.choices[0].message.content or "{}"
        try:
            data = json.loads(raw)
        except json.JSONDecodeError:
            logger.warning("LLM returned non-JSON: %s", raw[:200])
            return AnalysisResult(
                analysis_text=f"LLM parse error: {raw[:200]}",
                recommended_metrics=[],
                hypotheses=[],
                next_actions=[],
                confidence=0.0,
                citations=[],
                unknowns=["retry with simpler context"],
            )

        return AnalysisResult(
            analysis_text=data.get("analysis_text", ""),
            recommended_metrics=data.get("recommended_metrics", []),
            hypotheses=data.get("hypotheses", []),
            next_actions=data.get("next_actions", []),
            confidence=float(data.get("confidence", 0.0)),
            citations=data.get("citations", []),
            unknowns=data.get("unknowns", []),
        )


class AnthropicAdapter(ModelAdapter):
    def __init__(self, model: str = "claude-sonnet-4-20250514") -> None:
        try:
            import anthropic
        except ImportError:
            raise ImportError("anthropic package is required: pip install anthropic")
        api_key = os.environ.get("ANTHROPIC_API_KEY")
        if not api_key:
            raise ValueError("ANTHROPIC_API_KEY environment variable is required")
        self.client = anthropic.Anthropic(api_key=api_key)
        self.model = model

    def generate_analysis(self, context: dict[str, Any]) -> AnalysisResult:
        user_prompt = _build_user_prompt(context)
        response = self.client.messages.create(
            model=self.model,
            max_tokens=2048,
            system=SYSTEM_PROMPT + "\n\nYou MUST respond with only a JSON object, no other text.",
            messages=[
                {"role": "user", "content": user_prompt},
            ],
            temperature=0.2,
        )
        raw = response.content[0].text if response.content else "{}"
        # マークダウンのコードフェンス (```) が付いていたら除去する。
        stripped = raw.strip()
        if stripped.startswith("```"):
            lines = stripped.splitlines()
            lines = [l for l in lines if not l.strip().startswith("```")]
            stripped = "\n".join(lines).strip()
        try:
            data = json.loads(stripped)
        except json.JSONDecodeError:
            logger.warning("LLM returned non-JSON: %s", raw[:200])
            return AnalysisResult(
                analysis_text=f"LLM parse error: {raw[:200]}",
                recommended_metrics=[],
                hypotheses=[],
                next_actions=[],
                confidence=0.0,
                citations=[],
                unknowns=["retry with simpler context"],
            )

        return AnalysisResult(
            analysis_text=data.get("analysis_text", ""),
            recommended_metrics=data.get("recommended_metrics", []),
            hypotheses=data.get("hypotheses", []),
            next_actions=data.get("next_actions", []),
            confidence=float(data.get("confidence", 0.0)),
            citations=data.get("citations", []),
            unknowns=data.get("unknowns", []),
        )


class LLMError(Exception):
    """LLM 呼び出しが失敗したときに送出される。"""


class AgentError(Exception):
    """Agent オーケストレーションが失敗したときに送出される。"""


def create_model_adapter(provider: str, model: str) -> ModelAdapter:
    if provider in ("openai", "anthropic", "claude") and not _paid_llm_enabled():
        raise ValueError(
            "paid LLM providers are disabled (AI_LLM_NETWORK_ENABLED=0). "
            "Use --provider mock, or set AI_LLM_NETWORK_ENABLED=1 to re-enable."
        )
    if provider == "mock":
        return MockModelAdapter(model=model)
    if provider == "openai":
        return OpenAIAdapter(model=model)
    if provider in ("anthropic", "claude"):
        return AnthropicAdapter(model=model)
    raise ValueError(
        f"unsupported provider: {provider} (available: mock, openai, anthropic, claude)"
    )

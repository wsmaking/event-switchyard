#!/usr/bin/env python3
"""RAG + LLM によるインシデントトリアージ Agent。

gate fail / SLO 違反の run に対して、artifact と設計正本を read-only で横断し、
根拠付きの一次切り分けレポートを返す。

3 階層フォールバック: Agent+LLM → deterministic（固定ルール）。
"""
from __future__ import annotations

import argparse
import json
import logging
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from ai_model_adapter import (
    AgentError,
    AnalysisResult,
    LLMError,
    create_model_adapter,
)
from ai_tools import (
    build_causal_signals,
    compare_recent_runs,
    dedupe_evidence,
    detect_violations,
    load_perf_profile,
    load_run_inputs,
    load_timeseries_samples,
    build_retrieval_queries,
    retrieve_evidence,
)

logger = logging.getLogger(__name__)

MAX_STEPS = 4
MAX_RETRIEVAL_CALLS = 6
TIMEOUT_SEC = 90
CONFIDENCE_THRESHOLD = 0.65

TREND_METRICS = (
    "server_live_ack_accepted_p99_us",
    "server_accepted_rate",
    "completed_rps",
    "server_loss_suspect_total",
    "server_rejected_killed_total",
)


def now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="RAG + Agent インシデントトリアージ"
    )
    parser.add_argument("--run-name", required=True)
    parser.add_argument("--results-dir", default="var/results")
    parser.add_argument("--db-path", default="var/ai_index/docs.sqlite")
    parser.add_argument("--provider", default="mock", choices=("mock", "openai", "anthropic", "claude"))
    parser.add_argument("--model", default=None,
                        help="モデル名 (デフォルト: mock-triage-v1 / gpt-5-nano / claude-sonnet-4-20250514)")
    parser.add_argument("--top-k", type=int, default=5)
    parser.add_argument("--recent-window", type=int, default=5)
    parser.add_argument("--out", default=None)
    parser.add_argument("--verbose", action="store_true")
    return parser.parse_args()


def _default_model(provider: str) -> str:
    if provider == "openai":
        return "gpt-5-nano"
    if provider in ("anthropic", "claude"):
        return "claude-sonnet-4-20250514"
    return "mock-triage-v1"


def run_agent_with_llm(
    run_name: str,
    results_dir: Path,
    db_path: Path,
    provider: str,
    model: str,
    top_k: int,
    recent_window: int,
) -> dict[str, Any]:
    """Agent ループ全体: 検出 → (根拠取得 → 分析) × N → レポート生成。"""
    t0 = time.monotonic()

    # --- 違反検出 ---
    summary, metrics = load_run_inputs(results_dir, run_name)
    perf_profile = load_perf_profile(results_dir, run_name)
    timeseries_samples = load_timeseries_samples(results_dir, run_name, summary_data=summary)
    causal_signals = build_causal_signals(summary, metrics, perf_profile, timeseries_samples)
    violations = detect_violations(summary)
    recent = compare_recent_runs(results_dir, TREND_METRICS, limit=recent_window)

    # --- Agent ループ（最大 MAX_STEPS 回） ---
    adapter = create_model_adapter(provider=provider, model=model)
    all_evidence: list[Any] = []
    all_queries: list[str] = []
    retrieval_calls = 0
    analysis: AnalysisResult | None = None
    steps_taken = 0

    queries = build_retrieval_queries(violations)

    for step in range(MAX_STEPS):
        elapsed = time.monotonic() - t0
        if elapsed > TIMEOUT_SEC:
            logger.warning("timeout after %.1fs at step %d", elapsed, step)
            break

        # --- 根拠取得 ---
        for q in queries:
            if retrieval_calls >= MAX_RETRIEVAL_CALLS:
                break
            chunks = retrieve_evidence(
                db_path=db_path, query=q, top_k=top_k, source_like=None,
            )
            all_evidence.extend(chunks)
            all_queries.append(q)
            retrieval_calls += 1

        evidence = dedupe_evidence(all_evidence, limit=12)

        # --- LLM 分析 ---
        context = {
            "run_name": run_name,
            "violations": [v.to_dict() for v in violations],
            "summary": summary,
            "metrics": metrics,
            "perf_profile": perf_profile,
            "causal_signals": causal_signals,
            "evidence": [c.to_dict() for c in evidence],
            "queries": all_queries,
            "recent_runs": recent,
        }
        analysis = adapter.generate_analysis(context)
        steps_taken = step + 1

        # --- 十分な確信度に達したか確認 ---
        if analysis.confidence >= CONFIDENCE_THRESHOLD:
            break
        if not analysis.unknowns:
            break

        # --- 根拠不足: LLM が要求した追加クエリで再検索 ---
        queries = list(analysis.unknowns)[:3]

    elapsed_ms = int((time.monotonic() - t0) * 1000)
    timed_out = (time.monotonic() - t0) > TIMEOUT_SEC

    if analysis is None:
        analysis = AnalysisResult(
            analysis_text="Agent produced no analysis (all steps skipped).",
            recommended_metrics=[],
            hypotheses=[],
            next_actions=[],
            confidence=0.0,
            citations=[],
            unknowns=[],
        )

    return {
        "run_name": run_name,
        "generated_at": now_iso(),
        "provider": provider,
        "model": model,
        "mode": "agent",
        "agent_steps": steps_taken,
        "retrieval_calls": retrieval_calls,
        "elapsed_ms": elapsed_ms,
        "timed_out": timed_out,
        "violations": [v.to_dict() for v in violations],
        "retrieval": {
            "db_path": str(db_path),
            "query_count": len(all_queries),
            "queries": all_queries,
            "evidence_count": len(dedupe_evidence(all_evidence, limit=12)),
            "evidence": [c.to_dict() for c in dedupe_evidence(all_evidence, limit=12)],
        },
        "perf_profile": perf_profile,
        "causal_signals": causal_signals,
        "recent_runs": recent,
        "analysis": analysis.to_dict(),
    }


def run_deterministic(
    run_name: str,
    results_dir: Path,
) -> dict[str, Any]:
    """deterministic フォールバック: SLO 違反一覧 + 固定メトリクス推奨を返す（LLM 不要）。"""
    summary, metrics = load_run_inputs(results_dir, run_name)
    perf_profile = load_perf_profile(results_dir, run_name)
    timeseries_samples = load_timeseries_samples(results_dir, run_name, summary_data=summary)
    causal_signals = build_causal_signals(summary, metrics, perf_profile, timeseries_samples)
    violations = detect_violations(summary)
    return {
        "run_name": run_name,
        "generated_at": now_iso(),
        "provider": "deterministic",
        "model": "none",
        "mode": "deterministic-fallback",
        "violations": [v.to_dict() for v in violations],
        "perf_profile": perf_profile,
        "causal_signals": causal_signals,
        "analysis": {
            "analysis_text": "LLM unavailable. Deterministic SLO violation report only.",
            "recommended_metrics": [
                "gateway_live_ack_accepted_p99_us",
                "gateway_v3_accepted_rate",
                "gateway_v3_loss_suspect_total",
                "gateway_v3_rejected_killed_total",
                "gateway_v3_durable_queue_utilization_pct_max",
            ],
            "hypotheses": [],
            "next_actions": ["Manually inspect summary and metrics for this run."],
            "confidence": 0.0,
            "citations": [],
            "unknowns": [],
        },
    }


def main() -> int:
    args = parse_args()
    if args.verbose:
        logging.basicConfig(level=logging.DEBUG)
    else:
        logging.basicConfig(level=logging.WARNING)

    model = args.model or _default_model(args.provider)
    results_dir = Path(args.results_dir)
    db_path = Path(args.db_path)

    # 3 階層フォールバック: Agent+LLM → deterministic
    try:
        report = run_agent_with_llm(
            run_name=args.run_name,
            results_dir=results_dir,
            db_path=db_path,
            provider=args.provider,
            model=model,
            top_k=args.top_k,
            recent_window=args.recent_window,
        )
    except (AgentError, TimeoutError) as exc:
        logger.warning("Agent failed (%s), falling back to deterministic", exc)
        report = run_deterministic(args.run_name, results_dir)
    except (LLMError, ValueError, ImportError) as exc:
        logger.warning("LLM failed (%s), falling back to deterministic", exc)
        report = run_deterministic(args.run_name, results_dir)
    except Exception as exc:
        logger.error("Unexpected error (%s), falling back to deterministic", exc)
        report = run_deterministic(args.run_name, results_dir)

    payload = json.dumps(report, ensure_ascii=False, indent=2)
    if args.out:
        out_path = Path(args.out)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_text(payload + "\n", encoding="utf-8")
    print(payload)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

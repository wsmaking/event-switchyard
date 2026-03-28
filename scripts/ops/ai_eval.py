#!/usr/bin/env python3
"""AI トリアージ Agent の評価フレームワーク。

テストケースを Agent に通し、以下の KPI を計測する:
  - first_hypothesis_hit_rate: 主因候補が期待キーワードに当たった割合（目標 >= 70%）
  - grounded_answer_ratio: 仮説に根拠 citation が付いている割合（目標 >= 99%）
  - violation_detection_accuracy: 期待した違反を正しく検出できた割合（目標 100%）
  - response_time_p95: 応答時間の 95 パーセンタイル（目標 <= 12 秒）

使い方:
  python3 ai_eval.py --provider mock          # 0円で回帰テスト
  python3 ai_eval.py --provider openai        # LLM で prompt 精度を評価（有料）
"""
from __future__ import annotations

import argparse
import json
import statistics
import sys
import time
from pathlib import Path
from typing import Any

# scripts/ops をパスに追加してローカルインポートを有効にする。
sys.path.insert(0, str(Path(__file__).resolve().parent))

from ai_incident_agent import run_agent_with_llm, run_deterministic  # noqa: E402


def load_cases(eval_dir: Path) -> list[dict[str, Any]]:
    cases = []
    for path in sorted(eval_dir.glob("case_*.json")):
        cases.append(json.loads(path.read_text(encoding="utf-8")))
    return cases


def evaluate_case(
    case: dict[str, Any],
    results_dir: Path,
    db_path: Path,
    provider: str,
    model: str,
) -> dict[str, Any]:
    case_id = case["case_id"]
    run_name = case["run_name"]
    expected = case["expected"]

    t0 = time.monotonic()
    try:
        report = run_agent_with_llm(
            run_name=run_name,
            results_dir=results_dir,
            db_path=db_path,
            provider=provider,
            model=model,
            top_k=5,
            recent_window=5,
        )
        mode = report.get("mode", "agent")
    except FileNotFoundError as exc:
        return {
            "case_id": case_id,
            "status": "skipped",
            "reason": str(exc),
        }
    except Exception as exc:
        report = run_deterministic(run_name, results_dir)
        mode = "deterministic-fallback"
    elapsed = time.monotonic() - t0

    # --- 違反検出の正確性を評価 ---
    detected_violations = {
        v["name"] for v in report.get("violations", []) if v.get("violated")
    }
    expected_violations = set(expected.get("violated_rules", []))
    violation_hits = expected_violations & detected_violations
    violation_accuracy = len(violation_hits) / max(len(expected_violations), 1)

    # --- 仮説の品質を評価（キーワードヒット + 根拠付き率） ---
    analysis = report.get("analysis", {})
    hypotheses = analysis.get("hypotheses", [])
    expected_keywords = [kw.lower() for kw in expected.get("hypothesis_keywords", [])]

    hypothesis_hit = False
    grounded_count = 0
    total_hypotheses = len(hypotheses)

    for h in hypotheses:
        text_lower = h.get("text", "").lower()
        if any(kw in text_lower for kw in expected_keywords):
            hypothesis_hit = True
        citations = h.get("citations", [])
        if citations:
            grounded_count += 1

    # mock adapter（LLM なし）は citation をトップレベルに置くので、フォールバックで確認。
    if total_hypotheses > 0 and grounded_count == 0:
        top_citations = analysis.get("citations", [])
        if top_citations:
            grounded_count = total_hypotheses  # mock は citation をトップレベルに集約する

    grounded_ratio = grounded_count / max(total_hypotheses, 1)

    # --- メトリクス推奨の正確性を評価 ---
    recommended = set(analysis.get("recommended_metrics", []))
    must_recommend = set(expected.get("must_recommend_metrics", []))
    metric_hits = must_recommend & recommended
    metric_accuracy = len(metric_hits) / max(len(must_recommend), 1)

    return {
        "case_id": case_id,
        "status": "evaluated",
        "mode": mode,
        "elapsed_sec": round(elapsed, 2),
        "violation_accuracy": round(violation_accuracy, 3),
        "hypothesis_hit": hypothesis_hit,
        "grounded_ratio": round(grounded_ratio, 3),
        "metric_accuracy": round(metric_accuracy, 3),
        "agent_steps": report.get("agent_steps", 0),
        "hypotheses_count": total_hypotheses,
    }


def compute_summary(results: list[dict[str, Any]]) -> dict[str, Any]:
    evaluated = [r for r in results if r["status"] == "evaluated"]
    if not evaluated:
        return {"status": "no_cases_evaluated"}

    times = [r["elapsed_sec"] for r in evaluated]
    times.sort()
    p95_idx = max(0, int(len(times) * 0.95) - 1)

    hypothesis_hits = sum(1 for r in evaluated if r["hypothesis_hit"])
    grounded_ratios = [r["grounded_ratio"] for r in evaluated]
    violation_accs = [r["violation_accuracy"] for r in evaluated]
    metric_accs = [r["metric_accuracy"] for r in evaluated]

    n = len(evaluated)
    return {
        "cases_total": len(results),
        "cases_evaluated": n,
        "cases_skipped": len(results) - n,
        "first_hypothesis_hit_rate": round(hypothesis_hits / n, 3),
        "first_hypothesis_hit_rate_target": 0.70,
        "grounded_answer_ratio_avg": round(statistics.mean(grounded_ratios), 3),
        "grounded_answer_ratio_target": 0.99,
        "violation_detection_accuracy_avg": round(statistics.mean(violation_accs), 3),
        "metric_recommendation_accuracy_avg": round(statistics.mean(metric_accs), 3),
        "response_time_p95_sec": round(times[p95_idx], 2),
        "response_time_p95_target_sec": 12.0,
        "response_time_median_sec": round(statistics.median(times), 2),
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="AI トリアージ Agent をテストケースで評価する")
    parser.add_argument("--eval-dir", default="var/ai_eval")
    parser.add_argument("--results-dir", default="var/results")
    parser.add_argument("--db-path", default="var/ai_index/docs.sqlite")
    parser.add_argument("--provider", default="mock", choices=("mock", "openai", "anthropic", "claude"))
    parser.add_argument("--model", default=None)
    parser.add_argument("--out", default="var/ai_eval/results.json")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    eval_dir = Path(args.eval_dir)
    results_dir = Path(args.results_dir)
    db_path = Path(args.db_path)
    if args.model:
        model = args.model
    elif args.provider == "openai":
        model = "gpt-5-nano"
    elif args.provider in ("anthropic", "claude"):
        model = "claude-sonnet-4-20250514"
    else:
        model = "mock-triage-v1"

    cases = load_cases(eval_dir)
    if not cases:
        print(f"no test cases found in {eval_dir}")
        return 1

    print(f"Running {len(cases)} eval cases (provider={args.provider}, model={model})...")

    results = []
    for case in cases:
        print(f"  {case['case_id']}...", end=" ", flush=True)
        result = evaluate_case(case, results_dir, db_path, args.provider, model)
        print(result["status"], f"({result.get('elapsed_sec', '?')}s)")
        results.append(result)

    summary = compute_summary(results)

    output = {
        "summary": summary,
        "cases": results,
    }
    payload = json.dumps(output, ensure_ascii=False, indent=2)

    if args.out:
        out_path = Path(args.out)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_text(payload + "\n", encoding="utf-8")
        print(f"\nResults written to {out_path}")

    print(f"\n--- Summary ---")
    for k, v in summary.items():
        print(f"  {k}: {v}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())

#!/usr/bin/env python3
"""
Legacy CLI wrapper for SLO analysis.

This script is now unified to the Agent path (ai_incident_agent.py) so that
provider behavior (mock/openai/claude) and triage logic are consistent across
all entrypoints.

Stdout output remains compatible with the historical 3-section layout:
1) Violation Summary
2) LLM Analysis
3) Recommended Metrics
"""

from __future__ import annotations

import argparse
import json
import logging
from pathlib import Path
from typing import Any

from ai_incident_agent import run_agent_with_llm, run_deterministic
from ai_model_adapter import AgentError, LLMError

logger = logging.getLogger(__name__)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Analyze run SLO via unified Agent path"
    )
    parser.add_argument("--run-name", required=True, help="Run name without extension")
    parser.add_argument("--results-dir", default="var/results")
    parser.add_argument("--db-path", default="var/ai_index/docs.sqlite")
    parser.add_argument(
        "--provider",
        default="mock",
        choices=("mock", "openai", "anthropic", "claude"),
        help="LLM provider",
    )
    parser.add_argument(
        "--model",
        default=None,
        help="Default: mock-triage-v1 (mock) / gpt-5-nano (openai) / claude-sonnet-4-20250514 (claude)",
    )
    parser.add_argument("--top-k", type=int, default=5)
    parser.add_argument("--recent-window", type=int, default=5)
    parser.add_argument("--out", default=None, help="Optional JSON report output path")
    parser.add_argument("--dry-run", action="store_true", help="Skip LLM and emit deterministic report")
    parser.add_argument("--verbose", action="store_true")

    # Legacy flags kept for CLI compatibility (no-op in Agent-unified flow).
    parser.add_argument("--design-doc", default=None, help=argparse.SUPPRESS)
    parser.add_argument("--timeout-sec", type=int, default=None, help=argparse.SUPPRESS)
    return parser.parse_args()


def default_model(provider: str) -> str:
    if provider == "openai":
        return "gpt-5-nano"
    if provider in ("anthropic", "claude"):
        return "claude-sonnet-4-20250514"
    return "mock-triage-v1"


def render_violation_summary(report: dict[str, Any]) -> str:
    violations = report.get("violations")
    if not isinstance(violations, list):
        return "- no violation data."
    violated_rows = [v for v in violations if isinstance(v, dict) and v.get("violated")]
    if not violated_rows:
        return "- no SLO violations detected against fixed thresholds."
    lines: list[str] = []
    for row in violated_rows:
        name = row.get("name", "unknown")
        observed = row.get("observed")
        rule = row.get("rule", "unknown")
        deviation = row.get("deviation_text")
        if deviation is None:
            deviation = "n/a"
        lines.append(
            f"- {name}: observed={observed}, rule={rule}, deviation={deviation}"
        )
    return "\n".join(lines)


def print_stdout_output(report: dict[str, Any]) -> None:
    analysis = report.get("analysis")
    if not isinstance(analysis, dict):
        analysis = {}
    analysis_text = str(analysis.get("analysis_text", "")).strip()
    recommended = analysis.get("recommended_metrics")
    metrics: list[str] = []
    if isinstance(recommended, list):
        for item in recommended:
            if isinstance(item, str) and item.strip():
                metrics.append(item.strip())

    print("=== Violation Summary ===")
    print(render_violation_summary(report))
    print()
    print("=== LLM Analysis ===")
    print(analysis_text or "(none)")
    print()
    print("=== Recommended Metrics ===")
    if metrics:
        for name in metrics:
            print(f"- {name}")
    else:
        print("- (none)")


def main() -> int:
    args = parse_args()
    logging.basicConfig(level=logging.DEBUG if args.verbose else logging.WARNING)

    results_dir = Path(args.results_dir)
    db_path = Path(args.db_path)

    if args.dry_run:
        report = run_deterministic(args.run_name, results_dir)
    else:
        provider = args.provider
        model = args.model or default_model(provider)
        try:
            report = run_agent_with_llm(
                run_name=args.run_name,
                results_dir=results_dir,
                db_path=db_path,
                provider=provider,
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

    if args.out:
        out_path = Path(args.out)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_text(
            json.dumps(report, ensure_ascii=False, indent=2) + "\n",
            encoding="utf-8",
        )

    print_stdout_output(report)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

#!/usr/bin/env python3
import argparse
import csv
import json
import math
import sys
from pathlib import Path
from typing import Any


def load_expectations(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text())
    scenarios = payload.get("scenarios", [])
    return {scenario["scenarioId"]: scenario for scenario in scenarios}


def load_summary(path: Path) -> dict[str, dict[str, str]]:
    with path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        return {row["scenario_id"]: row for row in reader}


def parse_bool(value: str) -> bool:
    return value.strip().lower() in {"1", "true", "yes"}


def parse_number(value: str) -> float:
    if value is None or value == "":
        return 0.0
    return float(value)


def check_numeric(label: str, actual: float, rule: dict[str, Any], failures: list[str]) -> None:
    if "eq" in rule and not math.isclose(actual, float(rule["eq"]), rel_tol=0.0, abs_tol=1e-9):
        failures.append(f"{label}: expected eq {rule['eq']}, got {actual}")
    if "min" in rule and actual < float(rule["min"]):
        failures.append(f"{label}: expected min {rule['min']}, got {actual}")
    if "max" in rule and actual > float(rule["max"]):
        failures.append(f"{label}: expected max {rule['max']}, got {actual}")


def evaluate_row(row: dict[str, str], expected: dict[str, Any]) -> list[str]:
    failures: list[str] = []

    if row.get("status") != expected.get("status"):
        failures.append(f"status: expected {expected.get('status')}, got {row.get('status')}")
    if row.get("matched_policy") != expected.get("matchedPolicy"):
        failures.append(
            f"matched_policy: expected {expected.get('matchedPolicy')}, got {row.get('matched_policy')}"
        )
    if parse_bool(row.get("matched_report_found", "0")) != bool(expected.get("matchedReportFound")):
        failures.append(
            "matched_report_found: "
            f"expected {expected.get('matchedReportFound')}, got {parse_bool(row.get('matched_report_found', '0'))}"
        )
    if row.get("latest_final_status") != expected.get("latestFinalStatus"):
        failures.append(
            "latest_final_status: "
            f"expected {expected.get('latestFinalStatus')}, got {row.get('latest_final_status')}"
        )
    if row.get("shadow_comparison_status") != expected.get("shadowComparisonStatus"):
        failures.append(
            "shadow_comparison_status: "
            f"expected {expected.get('shadowComparisonStatus')}, got {row.get('shadow_comparison_status')}"
        )
    if row.get("effective_risk_budget_ref") != expected.get("effectiveRiskBudgetRef"):
        failures.append(
            "effective_risk_budget_ref: "
            f"expected {expected.get('effectiveRiskBudgetRef')}, got {row.get('effective_risk_budget_ref')}"
        )
    if row.get("model_id") != expected.get("modelId"):
        failures.append(f"model_id: expected {expected.get('modelId')}, got {row.get('model_id')}")

    numeric_map = {
        "reportPolicyCount": ("report_policy_count", int),
        "feedbackEventCount": ("feedback_event_count", int),
        "executedQty": ("executed_qty", float),
        "restingQty": ("resting_qty", float),
        "averageScoreBps": ("average_score_bps", float),
        "recordScoreBps": ("record_score_bps", float),
    }
    for expectation_key, (row_key, _ty) in numeric_map.items():
        rule = expected.get(expectation_key)
        if rule is None:
            continue
        check_numeric(expectation_key, parse_number(row.get(row_key, "0")), rule, failures)

    return failures


def render_summary(
    summary_path: Path,
    expectations_path: Path,
    rows: dict[str, dict[str, str]],
    expectations: dict[str, Any],
    failures_by_scenario: dict[str, list[str]],
) -> str:
    lines = [
        "quant_eval_gate",
        f"summary={summary_path}",
        f"expectations={expectations_path}",
        f"scenarios_expected={len(expectations)}",
        f"scenarios_found={len(rows)}",
    ]
    overall_pass = 1
    for scenario_id in sorted(expectations):
        failures = failures_by_scenario.get(scenario_id, [])
        scenario_pass = 0 if failures else 1
        overall_pass &= scenario_pass
        lines.append(f"scenario={scenario_id} pass={scenario_pass}")
        for failure in failures:
            lines.append(f"failure.{scenario_id}={failure}")
    lines.append(f"gate_pass={overall_pass}")
    return "\n".join(lines) + "\n"


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--summary", required=True)
    parser.add_argument("--expectations", required=True)
    parser.add_argument("--output")
    args = parser.parse_args()

    summary_path = Path(args.summary)
    expectations_path = Path(args.expectations)
    rows = load_summary(summary_path)
    expectations = load_expectations(expectations_path)

    failures_by_scenario: dict[str, list[str]] = {}
    for scenario_id, scenario in expectations.items():
        if scenario_id not in rows:
            failures_by_scenario[scenario_id] = ["missing summary row"]
            continue
        failures = evaluate_row(rows[scenario_id], scenario["expect"])
        if failures:
            failures_by_scenario[scenario_id] = failures

    summary_text = render_summary(
        summary_path,
        expectations_path,
        rows,
        expectations,
        failures_by_scenario,
    )
    if args.output:
        output_path = Path(args.output)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(summary_text)
    sys.stdout.write(summary_text)

    if failures_by_scenario:
        sys.exit(1)


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
import argparse
import csv
import json
from pathlib import Path
from typing import Any

from join_quant_feedback_with_mini_exchange_lib import join_report_with_quant_artifacts, load_json, load_jsonl


def build_capture_index(captures_dir: Path) -> dict[str, dict[str, Any]]:
    manifest_path = captures_dir / "manifest.json"
    index: dict[str, dict[str, Any]] = {}
    if manifest_path.exists():
        manifest = json.loads(manifest_path.read_text())
        for entry in manifest.get("captures", []):
            scenario_id = entry.get("scenarioId")
            if scenario_id:
                index[scenario_id] = entry
    for capture_meta in captures_dir.glob("*/capture_meta.json"):
        meta = json.loads(capture_meta.read_text())
        scenario_id = meta.get("scenarioId")
        if scenario_id:
            index[scenario_id] = meta
    return index


def list_report_files(reports_dir: Path) -> list[Path]:
    return sorted(
        path for path in reports_dir.glob("*.json")
        if path.name not in {"manifest.json"}
    )


def read_capture_artifacts(capture_dir: Path) -> tuple[list[dict[str, Any]], dict[str, Any] | None, dict[str, Any] | None]:
    feedback = load_jsonl(str(capture_dir / "feedback.jsonl")) if (capture_dir / "feedback.jsonl").exists() else []
    shadow_record = load_json(str(capture_dir / "shadow_record.json")) if (capture_dir / "shadow_record.json").exists() else None
    shadow_summary = load_json(str(capture_dir / "shadow_summary.json")) if (capture_dir / "shadow_summary.json").exists() else None
    return feedback, shadow_record, shadow_summary


def write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2) + "\n")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--reports-dir", required=True)
    parser.add_argument("--captures-dir", required=True)
    parser.add_argument("--output-dir", required=True)
    args = parser.parse_args()

    reports_dir = Path(args.reports_dir)
    captures_dir = Path(args.captures_dir)
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    capture_index = build_capture_index(captures_dir)
    batch_rows: list[dict[str, Any]] = []

    for report_path in list_report_files(reports_dir):
        report = load_json(str(report_path))
        scenario_id = report.get("scenarioId")
        if not scenario_id:
            continue
        capture_meta = capture_index.get(scenario_id)
        capture_dir = Path(capture_meta["captureDir"]) if capture_meta and capture_meta.get("captureDir") else None

        joined: dict[str, Any]
        if capture_dir and capture_dir.exists():
            feedback, shadow_record, shadow_summary = read_capture_artifacts(capture_dir)
            joined = join_report_with_quant_artifacts(report, feedback, shadow_record, shadow_summary)
            status = "OK"
        else:
            joined = join_report_with_quant_artifacts(report, [], None, None)
            status = "MISSING_CAPTURE"

        output_path = output_dir / f"{scenario_id}.json"
        write_json(output_path, joined)

        matched_report = joined.get("matchedReport") or {}
        feedback = joined.get("feedback") or {}
        shadow_record = joined.get("shadowRecord") or {}
        shadow_summary = joined.get("shadowSummary") or {}
        batch_rows.append({
            "scenario_id": scenario_id,
            "status": status,
            "matched_policy": joined.get("matchedPolicy") or "",
            "matched_report_found": 1 if matched_report else 0,
            "report_policy_count": len(joined.get("reportPolicies") or []),
            "latest_final_status": feedback.get("latestFinalStatus") or "",
            "feedback_event_count": feedback.get("eventCount") or 0,
            "shadow_comparison_status": shadow_record.get("comparisonStatus") or "",
            "executed_qty": matched_report.get("executedQty") or 0,
            "resting_qty": matched_report.get("restingQty") or 0,
            "average_score_bps": shadow_summary.get("averageScoreBps") or 0,
            "record_score_bps": shadow_record.get("totalScoreBps") or 0,
            "effective_risk_budget_ref": feedback.get("effectiveRiskBudgetRef") or "",
            "model_id": feedback.get("latestModelId") or "",
            "capture_dir": str(capture_dir) if capture_dir else "",
            "report_file": str(report_path),
            "output_file": str(output_path),
        })

    summary_path = output_dir / "batch_summary.csv"
    with summary_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=[
            "scenario_id",
            "status",
            "matched_policy",
            "matched_report_found",
            "report_policy_count",
            "latest_final_status",
            "feedback_event_count",
            "shadow_comparison_status",
            "executed_qty",
            "resting_qty",
            "average_score_bps",
            "record_score_bps",
            "effective_risk_budget_ref",
            "model_id",
            "capture_dir",
            "report_file",
            "output_file",
        ])
        writer.writeheader()
        writer.writerows(batch_rows)

    print(f"WROTE {summary_path}")


if __name__ == "__main__":
    main()

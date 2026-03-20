#!/usr/bin/env python3
import json
from pathlib import Path
from typing import Any


def normalize_policy(value: str | None) -> str | None:
    if value is None:
        return None
    return value.strip().upper().replace('-', '_')


def load_json(path: str | None) -> Any:
    if not path:
        return None
    return json.loads(Path(path).read_text())


def load_jsonl(path: str | None) -> list[dict[str, Any]]:
    if not path:
        return []
    events: list[dict[str, Any]] = []
    for line in Path(path).read_text().splitlines():
        line = line.strip()
        if not line:
            continue
        events.append(json.loads(line))
    return events


def summarize_reports(report: dict[str, Any]) -> dict[str, Any]:
    reports = report.get("reports", [])
    by_policy: dict[str, Any] = {}
    for entry in reports:
        policy = normalize_policy(entry.get("policy"))
        if policy is None:
            continue
        by_policy[policy] = {
            "childCount": len(entry.get("childReports", [])),
            "executedQty": sum(
                sum(trade.get("qty", 0) for trade in child.get("submit", {}).get("trades", []))
                for child in entry.get("childReports", [])
            ),
            "restingQty": sum(
                child.get("submit", {}).get("remainingQty", 0)
                for child in entry.get("childReports", [])
                if child.get("submit", {}).get("resting")
            ),
            "slippage": entry.get("slippageAfter", {}),
            "metrics": entry.get("metricsAfter", {}),
            "bookAfter": entry.get("bookAfter", {}),
        }
    return {
        "scenarioId": report.get("scenarioId"),
        "inputIntent": report.get("inputIntent"),
        "reportByPolicy": by_policy,
        "policyOrder": list(by_policy.keys()),
    }


def summarize_feedback(events: list[dict[str, Any]], scenario_id: str) -> dict[str, Any]:
    matched = [event for event in events if event.get("intentId") == scenario_id]
    matched.sort(key=lambda event: event.get("eventAtNs", 0))
    latest = matched[-1] if matched else None
    actual_policy = (((latest or {}).get("actualPolicy") or {}).get("executionPolicy") or {}).get("policy")
    shadow_run_ids = sorted({run_id for event in matched for run_id in event.get("shadowRunIds", [])})
    path_tags = sorted({tag for event in matched for tag in event.get("pathTags", [])})
    return {
        "eventCount": len(matched),
        "eventTypes": [event.get("eventType") for event in matched],
        "latestFinalStatus": (latest or {}).get("finalStatus"),
        "latestRejectReason": (latest or {}).get("rejectReason"),
        "latestModelId": (latest or {}).get("modelId"),
        "effectiveRiskBudgetRef": (latest or {}).get("effectiveRiskBudgetRef"),
        "actualPolicy": (latest or {}).get("actualPolicy"),
        "actualPolicyName": normalize_policy(actual_policy),
        "shadowRunIds": shadow_run_ids,
        "pathTags": path_tags,
        "latestEvent": latest,
    }


def summarize_shadow_record(record: dict[str, Any] | None) -> dict[str, Any] | None:
    if record is None:
        return None
    return {
        "shadowRunId": record.get("shadowRunId"),
        "intentId": record.get("intentId"),
        "modelId": record.get("modelId"),
        "comparisonStatus": record.get("comparisonStatus"),
        "totalScoreBps": sum(component.get("scoreBps", 0) for component in record.get("scoreComponents", [])),
        "scoreComponents": record.get("scoreComponents", []),
        "predictedPolicy": record.get("predictedPolicy"),
        "actualPolicy": record.get("actualPolicy"),
        "predictedOutcome": record.get("predictedOutcome"),
        "actualOutcome": record.get("actualOutcome"),
    }


def summarize_shadow_summary(summary: dict[str, Any] | None) -> dict[str, Any] | None:
    if summary is None:
        return None
    return {
        "shadowRunId": summary.get("shadowRunId"),
        "recordCount": summary.get("recordCount"),
        "matchedCount": summary.get("matchedCount"),
        "pendingCount": summary.get("pendingCount"),
        "negativeScoreCount": summary.get("negativeScoreCount"),
        "zeroScoreCount": summary.get("zeroScoreCount"),
        "positiveScoreCount": summary.get("positiveScoreCount"),
        "averageScoreBps": summary.get("averageScoreBps"),
        "topPositive": summary.get("topPositive", []),
        "topNegative": summary.get("topNegative", []),
    }


def join_report_with_quant_artifacts(
    report: dict[str, Any],
    feedback_events: list[dict[str, Any]],
    shadow_record: dict[str, Any] | None,
    shadow_summary: dict[str, Any] | None,
) -> dict[str, Any]:
    report_summary = summarize_reports(report)
    scenario_id = report_summary["scenarioId"]
    feedback_summary = summarize_feedback(feedback_events, scenario_id)
    matched_policy_name = feedback_summary.get("actualPolicyName")
    matched_report = report_summary["reportByPolicy"].get(matched_policy_name) if matched_policy_name else None
    return {
        "scenarioId": scenario_id,
        "inputIntent": report_summary["inputIntent"],
        "reportPolicies": report_summary["policyOrder"],
        "matchedPolicy": matched_policy_name,
        "matchedReport": matched_report,
        "feedback": feedback_summary,
        "shadowRecord": summarize_shadow_record(shadow_record),
        "shadowSummary": summarize_shadow_summary(shadow_summary),
    }

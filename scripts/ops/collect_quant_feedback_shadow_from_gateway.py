#!/usr/bin/env python3
import argparse
import copy
import json
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Any

FINAL_EVENT_TYPES = {
    "REJECTED",
    "DURABLE_ACCEPTED",
    "DURABLE_REJECTED",
    "EXCHANGE_SEND_FAILED",
}


def http_json(method: str, url: str, payload: Any | None = None, timeout: float = 10.0) -> tuple[int, Any]:
    headers = {"accept": "application/json"}
    data = None
    if payload is not None:
        headers["content-type"] = "application/json"
        data = json.dumps(payload).encode("utf-8")
    request = urllib.request.Request(url, data=data, headers=headers, method=method)
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            raw = response.read().decode("utf-8")
            return response.getcode(), json.loads(raw) if raw else None
    except urllib.error.HTTPError as err:
        raw = err.read().decode("utf-8")
        try:
            parsed = json.loads(raw) if raw else None
        except json.JSONDecodeError:
            parsed = {"raw": raw}
        return err.code, parsed


def sanitize(value: str) -> str:
    safe = []
    for ch in value:
        if ch.isalnum() or ch in {"-", "_", "."}:
            safe.append(ch)
        else:
            safe.append("-")
    collapsed = "".join(safe).strip("-")
    return collapsed or "intent"


def load_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text())


def write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2) + "\n")


def write_jsonl(path: Path, events: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        for event in events:
            handle.write(json.dumps(event))
            handle.write("\n")


def read_tail_events(path: Path, start_offset: int) -> tuple[list[dict[str, Any]], int]:
    if not path.exists():
        return [], start_offset
    with path.open("rb") as handle:
        handle.seek(start_offset)
        raw = handle.read()
        new_offset = handle.tell()
    if not raw:
        return [], new_offset
    lines = raw.decode("utf-8", errors="ignore").splitlines()
    events: list[dict[str, Any]] = []
    for line in lines:
        line = line.strip()
        if not line:
            continue
        try:
            events.append(json.loads(line))
        except json.JSONDecodeError:
            continue
    return events, new_offset


def merge_events(existing: list[dict[str, Any]], new_events: list[dict[str, Any]], intent_id: str) -> list[dict[str, Any]]:
    matched = [event for event in new_events if event.get("intentId") == intent_id]
    if not matched:
        return existing
    existing.extend(matched)
    existing.sort(key=lambda event: event.get("eventAtNs", 0))
    return existing


def has_final_event(events: list[dict[str, Any]]) -> bool:
    return any((event.get("eventType") or "").upper() in FINAL_EVENT_TYPES for event in events)


def read_feedback_for_intent(
    feedback_path: Path,
    start_offset: int,
    intent_id: str,
    timeout_ms: int,
    poll_interval_ms: int,
) -> tuple[list[dict[str, Any]], bool, int]:
    deadline = time.time() + (timeout_ms / 1000.0)
    offset = start_offset
    collected: list[dict[str, Any]] = []
    while time.time() < deadline:
        new_events, offset = read_tail_events(feedback_path, offset)
        collected = merge_events(collected, new_events, intent_id)
        if has_final_event(collected):
            return collected, False, offset
        time.sleep(poll_interval_ms / 1000.0)
    new_events, offset = read_tail_events(feedback_path, offset)
    collected = merge_events(collected, new_events, intent_id)
    return collected, not has_final_event(collected), offset


def get_strategy_config(base_url: str) -> dict[str, Any]:
    status, payload = http_json("GET", f"{base_url}/strategy/config")
    if status != 200 or not isinstance(payload, dict):
        raise RuntimeError(f"failed to GET /strategy/config: status={status} payload={payload}")
    return payload


def maybe_enable_shadow(base_url: str, run_label: str) -> tuple[dict[str, Any], dict[str, Any] | None]:
    current = get_strategy_config(base_url)
    if current.get("shadowEnabled"):
        return current, None
    updated = copy.deepcopy(current)
    updated["shadowEnabled"] = True
    updated["version"] = int(current.get("version", 0)) + 1
    updated["snapshotId"] = f"{current.get('snapshotId', 'snapshot')}-collector-{run_label}"
    updated["appliedAtNs"] = 0
    status, payload = http_json("PUT", f"{base_url}/strategy/config", updated)
    if status != 200 or not isinstance(payload, dict):
        raise RuntimeError(f"failed to enable shadow: status={status} payload={payload}")
    return current, payload


def maybe_restore_shadow(base_url: str, original: dict[str, Any], changed_snapshot: dict[str, Any] | None) -> dict[str, Any] | None:
    if changed_snapshot is None:
        return None
    latest = get_strategy_config(base_url)
    restore_payload = copy.deepcopy(original)
    restore_payload["version"] = int(latest.get("version", 0)) + 1
    restore_payload["appliedAtNs"] = 0
    status, payload = http_json("PUT", f"{base_url}/strategy/config", restore_payload)
    if status != 200 or not isinstance(payload, dict):
        raise RuntimeError(f"failed to restore config: status={status} payload={payload}")
    return payload


def fetch_shadow_artifacts(
    base_url: str,
    shadow_run_id: str,
    intent_id: str,
    timeout_ms: int,
    poll_interval_ms: int,
) -> tuple[dict[str, Any] | None, dict[str, Any] | None, bool]:
    encoded_run = urllib.parse.quote(shadow_run_id, safe="")
    encoded_intent = urllib.parse.quote(intent_id, safe="")
    record_url = f"{base_url}/strategy/shadow/{encoded_run}/{encoded_intent}"
    summary_url = f"{base_url}/strategy/shadow/{encoded_run}/summary"

    deadline = time.time() + (timeout_ms / 1000.0)
    last_record = None
    last_summary = None
    timed_out = False
    while time.time() < deadline:
        record_status, record_payload = http_json("GET", record_url)
        summary_status, summary_payload = http_json("GET", summary_url)
        if record_status == 200 and isinstance(record_payload, dict):
            last_record = record_payload
        if summary_status == 200 and isinstance(summary_payload, dict):
            last_summary = summary_payload
        comparison_status = (last_record or {}).get("comparisonStatus")
        if last_record is not None and comparison_status not in (None, "PENDING"):
            return last_record, last_summary, False
        time.sleep(poll_interval_ms / 1000.0)
    timed_out = True
    record_status, record_payload = http_json("GET", record_url)
    summary_status, summary_payload = http_json("GET", summary_url)
    if record_status == 200 and isinstance(record_payload, dict):
        last_record = record_payload
    if summary_status == 200 and isinstance(summary_payload, dict):
        last_summary = summary_payload
    return last_record, last_summary, timed_out


def build_submit_request(intent: dict[str, Any], shadow_run_id: str) -> dict[str, Any]:
    return {
        "intent": intent,
        "shadowRunId": shadow_run_id,
    }


def submit_intent(base_url: str, request_payload: dict[str, Any]) -> tuple[int, Any]:
    return http_json("POST", f"{base_url}/strategy/intent/submit", request_payload, timeout=20.0)


def collect_one(
    *,
    base_url: str,
    feedback_path: Path,
    intent_path: Path,
    output_dir: Path,
    shadow_run_prefix: str,
    feedback_timeout_ms: int,
    shadow_timeout_ms: int,
    poll_interval_ms: int,
    ordinal: int,
) -> dict[str, Any]:
    intent = load_json(intent_path)
    intent_id = intent.get("intentId")
    if not intent_id:
        raise RuntimeError(f"intent missing intentId: {intent_path}")

    scenario_dir = output_dir / sanitize(intent_id)
    scenario_dir.mkdir(parents=True, exist_ok=True)

    shadow_run_id = f"{shadow_run_prefix}-{ordinal:03d}-{sanitize(intent_id)}"
    submit_request = build_submit_request(intent, shadow_run_id)
    write_json(scenario_dir / "intent.json", intent)
    write_json(scenario_dir / "submit_request.json", submit_request)

    feedback_offset = feedback_path.stat().st_size if feedback_path.exists() else 0
    submit_status, submit_response = submit_intent(base_url, submit_request)
    write_json(
        scenario_dir / "submit_response.json",
        {
            "status": submit_status,
            "body": submit_response,
        },
    )

    feedback_events, feedback_timed_out, final_offset = read_feedback_for_intent(
        feedback_path,
        feedback_offset,
        intent_id,
        timeout_ms=feedback_timeout_ms,
        poll_interval_ms=poll_interval_ms,
    )
    write_jsonl(scenario_dir / "feedback.jsonl", feedback_events)

    shadow_record, shadow_summary, shadow_timed_out = fetch_shadow_artifacts(
        base_url,
        shadow_run_id,
        intent_id,
        timeout_ms=shadow_timeout_ms,
        poll_interval_ms=poll_interval_ms,
    )
    if shadow_record is not None:
        write_json(scenario_dir / "shadow_record.json", shadow_record)
    if shadow_summary is not None:
        write_json(scenario_dir / "shadow_summary.json", shadow_summary)

    capture_meta = {
        "scenarioId": intent_id,
        "intentFile": str(intent_path),
        "captureDir": str(scenario_dir),
        "shadowRunId": shadow_run_id,
        "submitStatus": submit_status,
        "feedbackEventCount": len(feedback_events),
        "feedbackTimedOut": feedback_timed_out,
        "shadowTimedOut": shadow_timed_out,
        "feedbackPath": str(feedback_path),
        "feedbackStartOffset": feedback_offset,
        "feedbackEndOffset": final_offset,
    }
    write_json(scenario_dir / "capture_meta.json", capture_meta)
    return capture_meta


def list_intent_files(input_dir: Path) -> list[Path]:
    return sorted(path for path in input_dir.iterdir() if path.suffix == ".json")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--base-url", default="http://127.0.0.1:8081")
    parser.add_argument("--input-dir", required=True)
    parser.add_argument("--feedback-jsonl", required=True)
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--shadow-run-prefix", default="collector")
    parser.add_argument("--feedback-timeout-ms", type=int, default=5000)
    parser.add_argument("--shadow-timeout-ms", type=int, default=5000)
    parser.add_argument("--poll-interval-ms", type=int, default=100)
    parser.add_argument("--no-restore-config", action="store_true")
    args = parser.parse_args()

    input_dir = Path(args.input_dir)
    feedback_path = Path(args.feedback_jsonl)
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    run_label = time.strftime("%Y%m%d_%H%M%S")
    original_config, changed_snapshot = maybe_enable_shadow(args.base_url, run_label)
    captures: list[dict[str, Any]] = []
    restore_result = None

    try:
        for ordinal, intent_path in enumerate(list_intent_files(input_dir), start=1):
            capture = collect_one(
                base_url=args.base_url,
                feedback_path=feedback_path,
                intent_path=intent_path,
                output_dir=output_dir,
                shadow_run_prefix=args.shadow_run_prefix,
                feedback_timeout_ms=args.feedback_timeout_ms,
                shadow_timeout_ms=args.shadow_timeout_ms,
                poll_interval_ms=args.poll_interval_ms,
                ordinal=ordinal,
            )
            captures.append(capture)
    finally:
        if not args.no_restore_config:
            restore_result = maybe_restore_shadow(args.base_url, original_config, changed_snapshot)

    manifest = {
        "baseUrl": args.base_url,
        "feedbackJsonl": str(feedback_path),
        "inputDir": str(input_dir),
        "outputDir": str(output_dir),
        "shadowRunPrefix": args.shadow_run_prefix,
        "collectorRunLabel": run_label,
        "originalConfig": original_config,
        "shadowEnableResult": changed_snapshot,
        "restoreResult": restore_result,
        "captures": captures,
    }
    write_json(output_dir / "manifest.json", manifest)
    print(f"WROTE {output_dir / 'manifest.json'}")


if __name__ == "__main__":
    try:
        main()
    except Exception as err:  # noqa: BLE001
        print(f"ERROR {err}", file=sys.stderr)
        sys.exit(1)

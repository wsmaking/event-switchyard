#!/usr/bin/env python3
from __future__ import annotations

import glob
import json
import os
import re
import sqlite3
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any


@dataclass(frozen=True)
class SloRule:
    name: str
    op: str
    threshold: float
    summary_keys: tuple[str, ...]


@dataclass(frozen=True)
class Violation:
    name: str
    rule: str
    status: str
    observed: float | None
    deviation_pct: float | None
    deviation_text: str
    violated: bool

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class EvidenceChunk:
    doc_id: str
    source_path: str
    heading: str
    section: str | None
    score: float
    snippet: str

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


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
        metrics[match.group(1)] = float(match.group(2))
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


def detect_violations(summary_data: dict[str, str]) -> list[Violation]:
    rows: list[Violation] = []
    for rule in SLO_RULES:
        value = pick_first_float(summary_data, rule.summary_keys)
        if value is None:
            rows.append(
                Violation(
                    name=rule.name,
                    rule=f"{rule.name} {rule.op} {rule.threshold}",
                    status="unknown",
                    observed=None,
                    deviation_pct=None,
                    deviation_text="missing",
                    violated=False,
                )
            )
            continue
        violated = is_violated(rule, value)
        rows.append(
            Violation(
                name=rule.name,
                rule=f"{rule.name} {rule.op} {rule.threshold}",
                status="violation" if violated else "ok",
                observed=value,
                deviation_pct=deviation_pct(rule, value),
                deviation_text=format_deviation(rule, value),
                violated=violated,
            )
        )
    return rows


def resolve_run_paths(results_dir: Path, run_name: str) -> tuple[Path, Path]:
    return (
        results_dir / f"{run_name}.summary.txt",
        results_dir / f"{run_name}.metrics.prom",
    )


def resolve_perf_path(results_dir: Path, run_name: str) -> Path:
    return results_dir / f"{run_name}.perf.json"


def resolve_timeseries_path(results_dir: Path, run_name: str) -> Path:
    return results_dir / f"{run_name}.timeseries.jsonl"


def load_run_inputs(results_dir: Path, run_name: str) -> tuple[dict[str, str], dict[str, float]]:
    summary_path, metrics_path = resolve_run_paths(results_dir, run_name)
    if not summary_path.exists():
        raise FileNotFoundError(f"summary not found: {summary_path}")
    summary = parse_key_value_file(summary_path)
    metrics = parse_prom_file(metrics_path) if metrics_path.exists() else {}
    return summary, metrics


def load_perf_profile(results_dir: Path, run_name: str) -> dict[str, Any] | None:
    path = resolve_perf_path(results_dir, run_name)
    if not path.exists():
        return None
    try:
        data = json.loads(path.read_text(encoding="utf-8", errors="replace"))
    except json.JSONDecodeError:
        return {
            "run_name": run_name,
            "source_path": str(path),
            "parse_error": "invalid_json",
        }
    if isinstance(data, dict):
        data.setdefault("source_path", str(path))
        return data
    return {
        "run_name": run_name,
        "source_path": str(path),
        "parse_error": "unexpected_json_type",
    }


def load_timeseries_samples(
    results_dir: Path,
    run_name: str,
    summary_data: dict[str, str] | None = None,
    limit: int = 10_000,
) -> list[dict[str, Any]]:
    path: Path
    raw_path = (summary_data or {}).get("timeseries_out")
    if raw_path:
        path = Path(raw_path)
    else:
        path = resolve_timeseries_path(results_dir, run_name)
    if not path.exists():
        return []

    rows: list[dict[str, Any]] = []
    for raw in path.read_text(encoding="utf-8", errors="replace").splitlines():
        raw = raw.strip()
        if not raw:
            continue
        try:
            item = json.loads(raw)
        except json.JSONDecodeError:
            continue
        if not isinstance(item, dict):
            continue
        if item.get("type") == "collector_meta":
            continue
        metrics = item.get("metrics")
        if not isinstance(metrics, dict):
            metrics = {}
        rows.append(
            {
                "ts_epoch_ms": _as_float(item.get("ts_epoch_ms")),
                "elapsed_sec": _as_float(item.get("elapsed_sec")),
                "ok": bool(item.get("ok", False)),
                "metrics": {k: _as_float(v) for k, v in metrics.items()},
            }
        )
        if len(rows) >= limit:
            break
    return rows


def _as_float(value: Any) -> float | None:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return None
        try:
            return float(text)
        except ValueError:
            return None
    return None


def _coalesce_float(
    summary_data: dict[str, str],
    metrics_data: dict[str, float],
    summary_keys: tuple[str, ...],
    metric_keys: tuple[str, ...] = (),
) -> float | None:
    for key in summary_keys:
        value = _as_float(summary_data.get(key))
        if value is not None:
            return value
    for key in metric_keys:
        value = _as_float(metrics_data.get(key))
        if value is not None:
            return value
    return None


def _per_sec(total: float | None, duration_sec: float | None) -> float | None:
    if total is None or duration_sec is None or duration_sec <= 0:
        return None
    return total / duration_sec


def build_causal_signals(
    summary_data: dict[str, str],
    metrics_data: dict[str, float],
    perf_profile: dict[str, Any] | None = None,
    timeseries_samples: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    duration_sec = _coalesce_float(summary_data, metrics_data, ("duration_sec",))
    accepted_rate_target = _coalesce_float(
        summary_data, metrics_data, ("target_accepted_rate",), ()
    )
    if accepted_rate_target is None:
        accepted_rate_target = 0.99

    offered_rps = _coalesce_float(summary_data, metrics_data, ("offered_rps",))
    offered_rps_ratio = _coalesce_float(summary_data, metrics_data, ("offered_rps_ratio",))
    dropped_offer_ratio = _coalesce_float(summary_data, metrics_data, ("client_dropped_offer_ratio",))
    unsent_total = _coalesce_float(summary_data, metrics_data, ("client_unsent_total",))

    accepted_rate = _coalesce_float(
        summary_data,
        metrics_data,
        ("server_accepted_rate", "accepted_rate", "observed_accepted_rate"),
        ("gateway_v3_accepted_rate",),
    )
    rejected_soft_total = _coalesce_float(
        summary_data, metrics_data, ("server_rejected_soft_total",), ("gateway_v3_rejected_soft_total",)
    )
    rejected_hard_total = _coalesce_float(
        summary_data, metrics_data, ("server_rejected_hard_total",), ("gateway_v3_rejected_hard_total",)
    )
    rejected_killed_total = _coalesce_float(
        summary_data, metrics_data, ("server_rejected_killed_total",), ("gateway_v3_rejected_killed_total",)
    )
    loss_suspect_total = _coalesce_float(
        summary_data, metrics_data, ("server_loss_suspect_total",), ("gateway_v3_loss_suspect_total",)
    )

    queue_depth = _coalesce_float(
        summary_data, metrics_data, ("server_durable_queue_depth",), ("gateway_v3_durable_queue_depth",)
    )
    queue_capacity = _coalesce_float(
        summary_data, metrics_data, ("server_durable_queue_capacity",), ("gateway_v3_durable_queue_capacity",)
    )
    queue_util_pct = _coalesce_float(
        summary_data,
        metrics_data,
        ("server_durable_queue_utilization_pct",),
        ("gateway_v3_durable_queue_utilization_pct",),
    )
    queue_util_pct_max = _coalesce_float(
        summary_data,
        metrics_data,
        ("server_durable_queue_utilization_pct_max",),
        ("gateway_v3_durable_queue_utilization_pct_max",),
    )
    backlog_growth_per_sec = _coalesce_float(
        summary_data,
        metrics_data,
        ("server_durable_backlog_growth_per_sec",),
        ("gateway_v3_durable_backlog_growth_per_sec",),
    )
    queue_full_total = _coalesce_float(
        summary_data, metrics_data, ("server_durable_queue_full_total",), ("gateway_v3_durable_queue_full_total",)
    )
    queue_closed_total = _coalesce_float(
        summary_data, metrics_data, ("server_durable_queue_closed_total",), ("gateway_v3_durable_queue_closed_total",)
    )

    backpressure_soft_total = _coalesce_float(
        summary_data,
        metrics_data,
        ("server_durable_backpressure_soft_total",),
        ("gateway_v3_durable_backpressure_soft_total",),
    )
    backpressure_hard_total = _coalesce_float(
        summary_data,
        metrics_data,
        ("server_durable_backpressure_hard_total",),
        ("gateway_v3_durable_backpressure_hard_total",),
    )

    confirm_p99_us = _coalesce_float(
        summary_data, metrics_data, ("server_durable_confirm_p99_us",), ("gateway_v3_durable_confirm_p99_us",)
    )

    rejected_soft_per_sec = _per_sec(rejected_soft_total, duration_sec)
    rejected_hard_per_sec = _per_sec(rejected_hard_total, duration_sec)
    backpressure_soft_per_sec = _per_sec(backpressure_soft_total, duration_sec)
    backpressure_hard_per_sec = _per_sec(backpressure_hard_total, duration_sec)
    queue_full_per_sec = _per_sec(queue_full_total, duration_sec)
    queue_closed_per_sec = _per_sec(queue_closed_total, duration_sec)

    accepted_rate_below_target = (
        accepted_rate is not None and accepted_rate_target is not None and accepted_rate < accepted_rate_target
    )
    queue_pressure_present = (
        (queue_util_pct_max is not None and queue_util_pct_max >= 0.50)
        or (backlog_growth_per_sec is not None and backlog_growth_per_sec > 0)
        or (queue_full_total is not None and queue_full_total > 0)
        or (queue_closed_total is not None and queue_closed_total > 0)
    )
    backpressure_counter_positive = (
        (backpressure_soft_total is not None and backpressure_soft_total > 0)
        or (backpressure_hard_total is not None and backpressure_hard_total > 0)
    )
    reject_pressure_positive = (
        (rejected_soft_total is not None and rejected_soft_total > 0)
        or (rejected_hard_total is not None and rejected_hard_total > 0)
    )
    supply_not_limited = (
        (offered_rps_ratio is None or offered_rps_ratio >= 0.99)
        and (dropped_offer_ratio is None or dropped_offer_ratio <= 0.01)
        and (unsent_total is None or unsent_total <= 0)
    )
    killed_or_loss_present = (
        (rejected_killed_total is not None and rejected_killed_total > 0)
        or (loss_suspect_total is not None and loss_suspect_total > 0)
    )

    timeline_samples = timeseries_samples or []
    first_pressure_sec: float | None = None
    first_backpressure_sec: float | None = None
    first_rate_drop_sec: float | None = None
    prev_back_soft: float | None = None
    prev_back_hard: float | None = None
    for sample in timeline_samples:
        metrics_map = sample.get("metrics", {})
        if not isinstance(metrics_map, dict):
            continue
        elapsed = _as_float(sample.get("elapsed_sec"))
        if elapsed is None:
            continue

        m_queue_util_max = _as_float(metrics_map.get("gateway_v3_durable_queue_utilization_pct_max"))
        m_backlog_growth = _as_float(metrics_map.get("gateway_v3_durable_backlog_growth_per_sec"))
        m_accepted_rate = _as_float(metrics_map.get("gateway_v3_accepted_rate"))
        m_back_soft = _as_float(metrics_map.get("gateway_v3_durable_backpressure_soft_total"))
        m_back_hard = _as_float(metrics_map.get("gateway_v3_durable_backpressure_hard_total"))

        pressure_now = (
            (m_queue_util_max is not None and m_queue_util_max >= 0.50)
            or (m_backlog_growth is not None and m_backlog_growth > 0)
        )
        if first_pressure_sec is None and pressure_now:
            first_pressure_sec = elapsed

        backpressure_increment = False
        if m_back_soft is not None:
            if prev_back_soft is not None and m_back_soft > prev_back_soft:
                backpressure_increment = True
            prev_back_soft = m_back_soft
        if m_back_hard is not None:
            if prev_back_hard is not None and m_back_hard > prev_back_hard:
                backpressure_increment = True
            prev_back_hard = m_back_hard
        if first_backpressure_sec is None and backpressure_increment:
            first_backpressure_sec = elapsed

        if (
            first_rate_drop_sec is None
            and m_accepted_rate is not None
            and accepted_rate_target is not None
            and m_accepted_rate < accepted_rate_target
        ):
            first_rate_drop_sec = elapsed

    timeline_order = "insufficient_data"
    if (
        first_pressure_sec is not None
        and first_backpressure_sec is not None
        and first_rate_drop_sec is not None
    ):
        if first_pressure_sec <= first_backpressure_sec <= first_rate_drop_sec:
            timeline_order = "pressure->backpressure->rate_drop"
        elif first_backpressure_sec <= first_rate_drop_sec:
            timeline_order = "backpressure->rate_drop"
        elif first_rate_drop_sec < first_backpressure_sec or first_rate_drop_sec < first_pressure_sec:
            timeline_order = "rate_drop_before_pressure_or_backpressure"
    elif first_backpressure_sec is not None and first_rate_drop_sec is not None:
        if first_backpressure_sec <= first_rate_drop_sec:
            timeline_order = "backpressure->rate_drop"
        else:
            timeline_order = "rate_drop_before_pressure_or_backpressure"

    time_order_consistent = timeline_order in (
        "pressure->backpressure->rate_drop",
        "backpressure->rate_drop",
    )
    rate_drop_before_pressure = timeline_order == "rate_drop_before_pressure_or_backpressure"

    support_signals = {
        "accepted_rate_below_target": accepted_rate_below_target,
        "backpressure_counter_positive": backpressure_counter_positive,
        "queue_pressure_present": queue_pressure_present,
        "reject_pressure_positive": reject_pressure_positive,
        "supply_not_limited": supply_not_limited,
        "killed_or_loss_absent": not killed_or_loss_present,
        "time_order_consistent": time_order_consistent,
    }
    contradiction_signals = {
        "no_reject_and_no_backpressure": (
            (rejected_soft_total is None or rejected_soft_total <= 0)
            and (rejected_hard_total is None or rejected_hard_total <= 0)
            and (backpressure_soft_total is None or backpressure_soft_total <= 0)
            and (backpressure_hard_total is None or backpressure_hard_total <= 0)
        ),
        "supply_limited": not supply_not_limited,
        "killed_or_loss_present": killed_or_loss_present,
        "rate_drop_before_pressure_or_backpressure": rate_drop_before_pressure,
    }

    if accepted_rate is None:
        durable_backpressure_verdict_hint = "unknown"
    elif (
        accepted_rate_below_target
        and backpressure_counter_positive
        and (queue_pressure_present or reject_pressure_positive)
        and supply_not_limited
        and not killed_or_loss_present
        and (not timeline_samples or time_order_consistent)
    ):
        durable_backpressure_verdict_hint = "likely"
    elif accepted_rate_below_target and (backpressure_counter_positive or reject_pressure_positive):
        durable_backpressure_verdict_hint = "possible"
    elif not accepted_rate_below_target and contradiction_signals["no_reject_and_no_backpressure"]:
        durable_backpressure_verdict_hint = "unlikely"
    else:
        durable_backpressure_verdict_hint = "unknown"

    perf_derived: dict[str, Any] = {}
    if isinstance(perf_profile, dict):
        for key in (
            "counter_mode_used",
            "collection_ok",
            "collection_note",
        ):
            if key in perf_profile:
                perf_derived[key] = perf_profile.get(key)
        derived = perf_profile.get("derived")
        if isinstance(derived, dict):
            for key in (
                "ipc",
                "cache_miss_rate",
                "branch_miss_rate",
                "cycles_per_completed",
                "instructions_per_completed",
            ):
                if key in derived:
                    perf_derived[key] = derived.get(key)

    return {
        "accepted_rate_target": accepted_rate_target,
        "duration_sec": duration_sec,
        "supply": {
            "offered_rps": offered_rps,
            "offered_rps_ratio": offered_rps_ratio,
            "dropped_offer_ratio": dropped_offer_ratio,
            "unsent_total": unsent_total,
        },
        "outcome": {
            "accepted_rate": accepted_rate,
            "rejected_soft_total": rejected_soft_total,
            "rejected_hard_total": rejected_hard_total,
            "rejected_killed_total": rejected_killed_total,
            "loss_suspect_total": loss_suspect_total,
        },
        "durable_pressure": {
            "queue_depth": queue_depth,
            "queue_capacity": queue_capacity,
            "queue_utilization_pct": queue_util_pct,
            "queue_utilization_pct_max": queue_util_pct_max,
            "backlog_growth_per_sec": backlog_growth_per_sec,
            "queue_full_total": queue_full_total,
            "queue_closed_total": queue_closed_total,
            "backpressure_soft_total": backpressure_soft_total,
            "backpressure_hard_total": backpressure_hard_total,
            "confirm_p99_us": confirm_p99_us,
        },
        "rates_estimated": {
            "rejected_soft_per_sec": rejected_soft_per_sec,
            "rejected_hard_per_sec": rejected_hard_per_sec,
            "backpressure_soft_per_sec": backpressure_soft_per_sec,
            "backpressure_hard_per_sec": backpressure_hard_per_sec,
            "queue_full_per_sec": queue_full_per_sec,
            "queue_closed_per_sec": queue_closed_per_sec,
        },
        "durable_backpressure_hypothesis": {
            "verdict_hint": durable_backpressure_verdict_hint,
            "support_signals": support_signals,
            "contradiction_signals": contradiction_signals,
        },
        "timeline": {
            "sample_count": len(timeline_samples),
            "first_pressure_sec": first_pressure_sec,
            "first_backpressure_sec": first_backpressure_sec,
            "first_rate_drop_sec": first_rate_drop_sec,
            "order": timeline_order,
            "order_consistent": time_order_consistent,
        },
        "perf": perf_derived,
    }


def list_summary_files(results_dir: Path) -> list[Path]:
    flat = glob.glob(str(results_dir / "*.summary.txt"))
    nested = glob.glob(str(results_dir / "*" / "*.summary.txt"))
    all_paths = [Path(p) for p in set(flat + nested)]
    return [p for p in all_paths if p.exists()]


def compare_recent_runs(
    results_dir: Path,
    metric_keys: tuple[str, ...],
    limit: int = 5,
) -> list[dict[str, Any]]:
    files = list_summary_files(results_dir)
    files.sort(key=lambda p: p.stat().st_mtime, reverse=True)
    rows: list[dict[str, Any]] = []
    for path in files[:limit]:
        data = parse_key_value_file(path)
        row: dict[str, Any] = {
            "run_file": path.name,
            "mtime_epoch": path.stat().st_mtime,
        }
        for key in metric_keys:
            val = data.get(key)
            if val is None or val == "":
                row[key] = None
            else:
                try:
                    row[key] = float(val)
                except ValueError:
                    row[key] = val
        rows.append(row)
    return rows


def build_retrieval_queries(violations: list[Violation]) -> list[str]:
    queries: list[str] = [
        "accepted_rate OR ack OR durable OR gate",
        "slo OR triage OR v3 OR orders",
    ]
    for v in violations:
        if not v.violated:
            continue
        if v.name == "ack_accepted_p99_us":
            queries.append("ack OR accepted OR p99 OR stage_parse OR stage_risk OR stage_enqueue")
        elif v.name == "accepted_rate":
            queries.append("accepted_rate OR durable OR backpressure OR rejected_soft OR rejected_hard")
        elif v.name == "completed_rps":
            queries.append("completed_rps OR throughput OR offered_rps OR bottleneck")
        elif v.name == "loss_suspect_total":
            queries.append("loss_suspect OR shard_killed OR global_killed OR kill")
        elif v.name == "rejected_killed":
            queries.append("rejected_killed OR queue OR kill OR escalation")
    # Stable unique order.
    return list(dict.fromkeys(queries))


def retrieve_evidence(
    db_path: Path,
    query: str,
    top_k: int = 5,
    source_like: str | None = None,
) -> list[EvidenceChunk]:
    if not db_path.exists():
        return []
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    try:
        if source_like:
            sql = """
                SELECT
                  d.doc_id AS doc_id,
                  d.source_path AS source_path,
                  d.heading AS heading,
                  d.section AS section,
                  bm25(docs_fts) AS score,
                  snippet(docs_fts, 3, '[', ']', '...', 18) AS snippet
                FROM docs_fts
                JOIN docs d ON docs_fts.doc_id = d.doc_id
                WHERE docs_fts MATCH ? AND d.source_path LIKE ?
                ORDER BY score ASC
                LIMIT ?
            """
            rows = conn.execute(sql, (query, source_like, top_k)).fetchall()
        else:
            sql = """
                SELECT
                  d.doc_id AS doc_id,
                  d.source_path AS source_path,
                  d.heading AS heading,
                  d.section AS section,
                  bm25(docs_fts) AS score,
                  snippet(docs_fts, 3, '[', ']', '...', 18) AS snippet
                FROM docs_fts
                JOIN docs d ON docs_fts.doc_id = d.doc_id
                WHERE docs_fts MATCH ?
                ORDER BY score ASC
                LIMIT ?
            """
            rows = conn.execute(sql, (query, top_k)).fetchall()
    finally:
        conn.close()

    out: list[EvidenceChunk] = []
    for r in rows:
        out.append(
            EvidenceChunk(
                doc_id=str(r["doc_id"]),
                source_path=str(r["source_path"]),
                heading=str(r["heading"]),
                section=str(r["section"]) if r["section"] is not None else None,
                score=float(r["score"]),
                snippet=str(r["snippet"]),
            )
        )
    return out


def dedupe_evidence(chunks: list[EvidenceChunk], limit: int = 10) -> list[EvidenceChunk]:
    out: list[EvidenceChunk] = []
    seen: set[str] = set()
    for c in chunks:
        if c.doc_id in seen:
            continue
        seen.add(c.doc_id)
        out.append(c)
        if len(out) >= limit:
            break
    return out


def normalize_run_name(path: Path) -> str:
    name = path.name
    if name.endswith(".summary.txt"):
        return name[: -len(".summary.txt")]
    return name

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

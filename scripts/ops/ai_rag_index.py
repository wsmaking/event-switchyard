#!/usr/bin/env python3
from __future__ import annotations

import argparse
import datetime as dt
import glob
import json
import re
import sqlite3
from pathlib import Path


HEADER_RE = re.compile(r"^(#{2,4})\s+(.+)$")
SECTION_RE = re.compile(r"^(\d+)\.")

# Important metrics to extract from .metrics.prom files.
IMPORTANT_METRIC_PREFIXES = (
    "gateway_live_ack",
    "gateway_v3_accepted",
    "gateway_v3_rejected",
    "gateway_v3_loss_suspect",
    "gateway_v3_shard_killed",
    "gateway_v3_global_killed",
    "gateway_v3_stage_",
    "gateway_v3_durable_queue",
    "gateway_v3_durable_confirm",
    "gateway_v3_durable_backpressure",
    "gateway_v3_durable_admission",
    "gateway_v3_durable_receipt_inflight",
    "gateway_v3_durable_fdatasync",
    "gateway_v3_durable_lane_skew",
)


def chunk_markdown(path: Path) -> list[dict[str, str | int | None]]:
    text = path.read_text(encoding="utf-8", errors="replace")
    lines = text.splitlines()
    chunks: list[dict[str, str | int | None]] = []
    current_heading = "preamble"
    current_section: str | None = None
    buf: list[str] = []
    order = 0

    def flush() -> None:
        nonlocal order
        body = "\n".join(buf).strip()
        if not body:
            return
        chunks.append(
            {
                "doc_id": f"{path.as_posix()}#c{order:04d}",
                "source_path": path.as_posix(),
                "heading": current_heading,
                "section": current_section,
                "chunk_text": body,
                "chunk_ord": order,
            }
        )
        order += 1

    for line in lines:
        m = HEADER_RE.match(line)
        if m:
            flush()
            buf.clear()
            heading = m.group(2).strip()
            current_heading = heading
            sm = SECTION_RE.match(heading)
            current_section = sm.group(1) if sm else current_section
        buf.append(line)
    flush()
    return chunks


def chunk_summary(path: Path) -> list[dict[str, str | int | None]]:
    """Index a summary.txt as a single chunk."""
    text = path.read_text(encoding="utf-8", errors="replace").strip()
    if not text:
        return []
    run_name = path.name
    if run_name.endswith(".summary.txt"):
        run_name = run_name[: -len(".summary.txt")]
    return [
        {
            "doc_id": f"run:{run_name}",
            "source_path": path.as_posix(),
            "heading": run_name,
            "section": "run_result",
            "chunk_text": text,
            "chunk_ord": 0,
        }
    ]


def chunk_metrics_prom(path: Path) -> list[dict[str, str | int | None]]:
    """Index important lines from a .metrics.prom file as a single chunk."""
    lines = path.read_text(encoding="utf-8", errors="replace").splitlines()
    important: list[str] = []
    for line in lines:
        stripped = line.strip()
        if not stripped or stripped.startswith("#"):
            continue
        if any(stripped.startswith(pfx) for pfx in IMPORTANT_METRIC_PREFIXES):
            important.append(stripped)
    if not important:
        return []
    run_name = path.name
    if run_name.endswith(".metrics.prom"):
        run_name = run_name[: -len(".metrics.prom")]
    return [
        {
            "doc_id": f"metrics:{run_name}",
            "source_path": path.as_posix(),
            "heading": f"metrics:{run_name}",
            "section": "metrics",
            "chunk_text": "\n".join(important),
            "chunk_ord": 0,
        }
    ]


def chunk_perf_json(path: Path) -> list[dict[str, str | int | None]]:
    """Index a perf profile JSON as a single chunk."""
    try:
        data = json.loads(path.read_text(encoding="utf-8", errors="replace"))
    except json.JSONDecodeError:
        return []
    if not isinstance(data, dict):
        return []

    run_name = path.name
    if run_name.endswith(".perf.json"):
        run_name = run_name[: -len(".perf.json")]

    lines: list[str] = []
    for key in (
        "counter_mode_used",
        "collection_ok",
        "collection_note",
        "platform",
        "window_duration_sec",
    ):
        value = data.get(key)
        if value is not None:
            lines.append(f"{key}={value}")

    counters = data.get("counters", {})
    if isinstance(counters, dict):
        for key, value in counters.items():
            if isinstance(value, (int, float)):
                lines.append(f"counter.{key}={value}")

    derived = data.get("derived", {})
    if isinstance(derived, dict):
        for key, value in derived.items():
            if isinstance(value, (int, float)):
                lines.append(f"derived.{key}={value}")

    delta = data.get("delta_vs_baseline", {})
    if isinstance(delta, dict):
        for key, value in delta.items():
            if isinstance(value, (int, float)):
                lines.append(f"delta_pct.{key}={value}")

    if not lines:
        return []
    return [
        {
            "doc_id": f"perf:{run_name}",
            "source_path": path.as_posix(),
            "heading": f"perf:{run_name}",
            "section": "perf",
            "chunk_text": "\n".join(lines),
            "chunk_ord": 0,
        }
    ]


def _find_result_files(results_dir: Path) -> tuple[list[Path], list[Path], list[Path]]:
    """Find summary / metrics / perf profile files in results dir (flat + nested)."""
    summary_globs = [
        str(results_dir / "*.summary.txt"),
        str(results_dir / "*" / "*.summary.txt"),
    ]
    metrics_globs = [
        str(results_dir / "*.metrics.prom"),
        str(results_dir / "*" / "*.metrics.prom"),
    ]
    perf_globs = [
        str(results_dir / "*.perf.json"),
        str(results_dir / "*" / "*.perf.json"),
    ]
    summaries = sorted(set(Path(p) for g in summary_globs for p in glob.glob(g)))
    metrics = sorted(set(Path(p) for g in metrics_globs for p in glob.glob(g)))
    perf_profiles = sorted(set(Path(p) for g in perf_globs for p in glob.glob(g)))
    return summaries, metrics, perf_profiles


def init_db(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS docs (
          doc_id TEXT PRIMARY KEY,
          source_path TEXT NOT NULL,
          heading TEXT NOT NULL,
          section TEXT,
          chunk_text TEXT NOT NULL,
          updated_at TEXT NOT NULL,
          chunk_ord INTEGER NOT NULL
        );

        CREATE VIRTUAL TABLE IF NOT EXISTS docs_fts
        USING fts5(
          doc_id UNINDEXED,
          heading,
          source_path,
          chunk_text
        );
        """
    )


def _insert_chunks(
    conn: sqlite3.Connection,
    chunks: list[dict[str, str | int | None]],
    updated_at: str,
) -> int:
    count = 0
    for c in chunks:
        conn.execute(
            """
            INSERT OR REPLACE INTO docs (
              doc_id, source_path, heading, section, chunk_text, updated_at, chunk_ord
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                c["doc_id"],
                c["source_path"],
                c["heading"],
                c["section"],
                c["chunk_text"],
                updated_at,
                c["chunk_ord"],
            ),
        )
        conn.execute(
            """
            INSERT OR REPLACE INTO docs_fts (doc_id, heading, source_path, chunk_text)
            VALUES (?, ?, ?, ?)
            """,
            (c["doc_id"], c["heading"], c["source_path"], c["chunk_text"]),
        )
        count += 1
    return count


def rebuild_index(
    docs_dir: Path,
    db_path: Path,
    include_old: bool,
    results_dir: Path | None = None,
) -> tuple[int, int]:
    md_files = sorted(docs_dir.rglob("*.md"))
    if not include_old:
        md_files = [p for p in md_files if "/old/" not in p.as_posix()]
    md_files = [p for p in md_files if not p.name.startswith("ai_")]

    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(db_path))
    try:
        init_db(conn)
        conn.execute("DELETE FROM docs")
        conn.execute("DELETE FROM docs_fts")
        inserted_chunks = 0
        indexed_docs = 0
        updated_at = dt.datetime.now(dt.timezone.utc).replace(microsecond=0).isoformat()

        # --- Markdown docs ---
        for path in md_files:
            chunks = chunk_markdown(path)
            if not chunks:
                continue
            indexed_docs += 1
            inserted_chunks += _insert_chunks(conn, chunks, updated_at)

        # --- Run results (summary + metrics) ---
        if results_dir and results_dir.exists():
            summaries, metrics_files, perf_profiles = _find_result_files(results_dir)
            for path in summaries:
                chunks = chunk_summary(path)
                if chunks:
                    indexed_docs += 1
                    inserted_chunks += _insert_chunks(conn, chunks, updated_at)
            for path in metrics_files:
                chunks = chunk_metrics_prom(path)
                if chunks:
                    indexed_docs += 1
                    inserted_chunks += _insert_chunks(conn, chunks, updated_at)
            for path in perf_profiles:
                chunks = chunk_perf_json(path)
                if chunks:
                    indexed_docs += 1
                    inserted_chunks += _insert_chunks(conn, chunks, updated_at)

        conn.commit()
        return indexed_docs, inserted_chunks
    finally:
        conn.close()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build local RAG index for ops docs + run results")
    parser.add_argument("--docs-dir", default="docs/ops")
    parser.add_argument("--results-dir", default="var/results",
                        help="Directory containing summary/metrics files (set empty to skip)")
    parser.add_argument("--db-path", default="var/ai_index/docs.sqlite")
    parser.add_argument("--include-old", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    docs_dir = Path(args.docs_dir)
    db_path = Path(args.db_path)
    results_dir = Path(args.results_dir) if args.results_dir else None
    if not docs_dir.exists():
        raise SystemExit(f"docs dir not found: {docs_dir}")
    docs, chunks = rebuild_index(
        docs_dir, db_path, include_old=args.include_old, results_dir=results_dir,
    )
    print(f"indexed_docs={docs}")
    print(f"indexed_chunks={chunks}")
    print(f"db_path={db_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

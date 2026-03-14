#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import re
import signal
import time
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any

SAMPLE_METRICS_DEFAULT = (
    "gateway_v3_accepted_rate,"
    "gateway_v3_rejected_soft_total,"
    "gateway_v3_rejected_hard_total,"
    "gateway_v3_rejected_killed_total,"
    "gateway_v3_loss_suspect_total,"
    "gateway_v3_durable_queue_depth,"
    "gateway_v3_durable_queue_utilization_pct,"
    "gateway_v3_durable_queue_utilization_pct_max,"
    "gateway_v3_durable_backlog_growth_per_sec,"
    "gateway_v3_durable_backpressure_soft_total,"
    "gateway_v3_durable_backpressure_hard_total,"
    "gateway_v3_durable_confirm_p99_us"
)

METRIC_LINE_RE = re.compile(
    r"^([a-zA-Z_:][a-zA-Z0-9_:]*)(?:\{[^}]*\})?\s+([-+]?\d+(?:\.\d+)?(?:[eE][-+]?\d+)?)$"
)

_STOP = False


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Collect /metrics time-series samples as JSONL")
    parser.add_argument("--url", required=True, help="metrics endpoint URL")
    parser.add_argument("--out", required=True, help="output JSONL path")
    parser.add_argument("--interval-ms", type=int, default=500, help="sampling interval (ms)")
    parser.add_argument(
        "--duration-sec",
        type=float,
        default=None,
        help="optional max duration; if omitted, run until SIGINT/SIGTERM",
    )
    parser.add_argument(
        "--metrics",
        default=SAMPLE_METRICS_DEFAULT,
        help="comma-separated metric names to record",
    )
    parser.add_argument("--timeout-sec", type=float, default=1.0, help="HTTP timeout seconds")
    return parser.parse_args()


def install_signal_handlers() -> None:
    def _handler(_signum: int, _frame: Any) -> None:
        global _STOP
        _STOP = True

    signal.signal(signal.SIGINT, _handler)
    signal.signal(signal.SIGTERM, _handler)


def parse_metrics_text(text: str, wanted: set[str]) -> dict[str, float]:
    out: dict[str, float] = {}
    for raw in text.splitlines():
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        m = METRIC_LINE_RE.match(line)
        if not m:
            continue
        name = m.group(1)
        if wanted and name not in wanted:
            continue
        out[name] = float(m.group(2))
    return out


def fetch_metrics(url: str, timeout_sec: float) -> str:
    with urllib.request.urlopen(url, timeout=timeout_sec) as resp:
        body = resp.read()
    return body.decode("utf-8", errors="replace")


def main() -> int:
    args = parse_args()
    install_signal_handlers()

    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    interval_sec = max(args.interval_ms, 50) / 1000.0
    wanted = {m.strip() for m in args.metrics.split(",") if m.strip()}

    start_wall = time.time()
    start_mono = time.monotonic()
    next_tick = start_mono

    with out_path.open("w", encoding="utf-8") as out:
        while not _STOP:
            now_mono = time.monotonic()
            if args.duration_sec is not None and (now_mono - start_mono) >= args.duration_sec:
                break
            if now_mono < next_tick:
                time.sleep(min(0.05, next_tick - now_mono))
                continue
            next_tick += interval_sec

            ts_epoch_ms = int(time.time() * 1000)
            elapsed_sec = now_mono - start_mono
            record: dict[str, Any] = {
                "ts_epoch_ms": ts_epoch_ms,
                "elapsed_sec": round(elapsed_sec, 6),
                "ok": True,
                "metrics": {},
            }
            try:
                text = fetch_metrics(args.url, timeout_sec=args.timeout_sec)
                record["metrics"] = parse_metrics_text(text, wanted)
            except (urllib.error.URLError, TimeoutError, OSError) as exc:
                record["ok"] = False
                record["error"] = str(exc)
            out.write(json.dumps(record, ensure_ascii=False) + "\n")
            out.flush()

    # Store collection metadata at EOF for quick diagnostics.
    end_wall = time.time()
    meta = {
        "type": "collector_meta",
        "start_epoch_sec": start_wall,
        "end_epoch_sec": end_wall,
        "duration_sec": end_wall - start_wall,
        "interval_ms": args.interval_ms,
        "url": args.url,
    }
    with out_path.open("a", encoding="utf-8") as out:
        out.write(json.dumps(meta, ensure_ascii=False) + "\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

#!/usr/bin/env python3
"""
Performance Gate - Gateway Performance Quality Gate

gateway-rust のベンチマーク結果を検証し、性能基準を満たしているか確認する。
slo_gate.py と同様のパターンで実装。

Usage:
    # ベンチマーク実行 + ゲートチェック
    python perf_gate.py --run

    # 既存結果ファイルをチェック
    python perf_gate.py --input results/bench_20250110_003045.json

    # カスタム閾値でチェック
    python perf_gate.py --run --p99 2000 --throughput 50000

    # CI用（失敗時に exit 1）
    python perf_gate.py --run --ci

    # wrk結果を併用（スループット判定はwrk優先）
    python perf_gate.py --run --wrk-input var/results/wrk_gate_8081.txt

    # ベースライン比較（+5%退行でWARN）
    python perf_gate.py --run --baseline baseline/perf_gate_rust.json --baseline-regression 0.05

    # ベースライン更新
    python perf_gate.py --run --update-baseline baseline/perf_gate_rust.json
"""

import argparse
import json
import os
import subprocess
import sys
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Tuple, Any, Optional


# デフォルト閾値（HFT基準）
DEFAULT_THRESHOLDS = {
    "p50_us": 500.0,        # p50 <= 500µs
    "p99_us": 2000.0,       # p99 <= 2ms
    "max_us": 10000.0,      # max <= 10ms
    "throughput_rps": 50000.0,  # >= 50k req/s
}

# 警告閾値（機関投資家基準）
WARN_THRESHOLDS = {
    "p50_us": 1000.0,       # p50 <= 1ms
    "p99_us": 10000.0,      # p99 <= 10ms
    "max_us": 50000.0,      # max <= 50ms
    "throughput_rps": 10000.0,  # >= 10k req/s
}


class Colors:
    """ANSI color codes"""
    GREEN = "\033[92m"
    YELLOW = "\033[93m"
    RED = "\033[91m"
    BLUE = "\033[94m"
    BOLD = "\033[1m"
    END = "\033[0m"


def parse_duration_to_ms(value: str) -> Optional[float]:
    """Parse duration strings like 10ms/200us/1.5s into milliseconds."""
    value = value.strip().lower()
    try:
        if value.endswith("ms"):
            return float(value[:-2])
        if value.endswith("us"):
            return float(value[:-2]) / 1000.0
        if value.endswith("s"):
            return float(value[:-1]) * 1000.0
    except ValueError:
        return None
    return None


def parse_wrk_output(text: str) -> Dict[str, float]:
    """Parse wrk output to extract throughput and optional latency metrics."""
    metrics: Dict[str, float] = {}
    for line in text.splitlines():
        line = line.strip()
        if line.startswith("Requests/sec:"):
            try:
                metrics["throughput_rps"] = float(line.split(":", 1)[1].strip())
            except ValueError:
                continue
        elif line.startswith("Latency") and "Distribution" not in line:
            parts = line.split()
            if len(parts) >= 2:
                parsed = parse_duration_to_ms(parts[1])
                if parsed is not None:
                    metrics["latency_avg_ms"] = parsed
        elif line.startswith("99%"):
            parts = line.split()
            if len(parts) >= 2:
                parsed = parse_duration_to_ms(parts[1])
                if parsed is not None:
                    metrics["latency_p99_ms"] = parsed
    return metrics


def extract_bench_metrics(results: Dict) -> Dict[str, float]:
    """Extract metrics from bench_gateway.py results."""
    metrics: Dict[str, float] = {}
    rtt_metrics = None
    throughput_metrics = None

    for r in results.get("results", []):
        if r.get("test_name") == "rtt":
            rtt_metrics = r.get("metrics", {})
        elif r.get("test_name") == "throughput":
            throughput_metrics = r.get("metrics", {})

    if rtt_metrics:
        for key in ("p50_us", "p99_us", "max_us"):
            if key in rtt_metrics:
                metrics[key] = float(rtt_metrics[key])
    if throughput_metrics and "throughput_rps" in throughput_metrics:
        metrics["throughput_rps"] = float(throughput_metrics["throughput_rps"])
    return metrics


def run_benchmark(
    host: str = "localhost",
    port: int = 8081,
    requests: int = 1000,
    duration: int = 5,
    concurrency: int = 50,
    warmup_rtt: int = 50,
    warmup_throughput_sec: int = 0
) -> Optional[Dict]:
    """
    bench_gateway.py を実行してベンチマーク結果を取得
    """
    script_dir = Path(__file__).parent
    bench_script = script_dir / "bench_gateway.py"

    if not bench_script.exists():
        print(f"{Colors.RED}ERROR: bench_gateway.py not found{Colors.END}", file=sys.stderr)
        return None

    # rtt と throughput のみ実行（ゲート用）
    cmd = [
        sys.executable, str(bench_script),
        "all",
        "--host", host,
        "--port", str(port),
        "--requests", str(requests),
        "--duration", str(duration),
        "--concurrency", str(concurrency),
        "--warmup-rtt", str(warmup_rtt),
        "--warmup-throughput-sec", str(warmup_throughput_sec),
        "--output", str(script_dir / "results"),
    ]

    print(f"{Colors.BLUE}Running benchmark...{Colors.END}")
    print(f"  Command: {' '.join(cmd)}")
    print()

    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=300)
        print(result.stdout)
        if result.returncode != 0:
            print(result.stderr, file=sys.stderr)
            return None
    except subprocess.TimeoutExpired:
        print(f"{Colors.RED}ERROR: Benchmark timed out{Colors.END}", file=sys.stderr)
        return None

    # 最新の結果ファイルを探す
    results_dir = script_dir / "results"
    if not results_dir.exists():
        return None

    result_files = sorted(results_dir.glob("bench_*.json"), reverse=True)
    if not result_files:
        return None

    with open(result_files[0], 'r') as f:
        return json.load(f)


def load_results(path: Path) -> Optional[Dict]:
    """結果ファイルを読み込み"""
    if not path.exists():
        print(f"{Colors.RED}ERROR: File not found: {path}{Colors.END}", file=sys.stderr)
        return None

    with open(path, 'r') as f:
        return json.load(f)


def load_wrk_metrics(path: Path) -> Optional[Dict[str, float]]:
    """Load wrk output text and parse metrics."""
    if not path.exists():
        print(f"{Colors.RED}ERROR: File not found: {path}{Colors.END}", file=sys.stderr)
        return None

    with open(path, 'r') as f:
        content = f.read()
    metrics = parse_wrk_output(content)
    if not metrics:
        print(f"{Colors.YELLOW}WARN: wrk metrics not found in {path}{Colors.END}")
    return metrics


def check_metric(
    name: str,
    actual: float,
    threshold: float,
    warn_threshold: float,
    operator: str = "<="
) -> Tuple[str, Dict]:
    """
    メトリクスをチェック

    Returns: (status, check_result)
    status: "PASS", "WARN", "FAIL"
    """
    if operator == "<=":
        passed = actual <= threshold
        warn_passed = actual <= warn_threshold
    else:  # >=
        passed = actual >= threshold
        warn_passed = actual >= warn_threshold

    if passed:
        status = "PASS"
    elif warn_passed:
        status = "WARN"
    else:
        status = "FAIL"

    # 差分計算
    if threshold > 0:
        pct_diff = ((actual - threshold) / threshold) * 100
    else:
        pct_diff = 0

    return status, {
        "name": name,
        "actual": actual,
        "threshold": threshold,
        "warn_threshold": warn_threshold,
        "operator": operator,
        "status": status,
        "pct_diff": pct_diff,
    }


def run_checks(
    results: Dict,
    thresholds: Dict,
    warn_thresholds: Dict,
    wrk_metrics: Optional[Dict[str, float]] = None
) -> Tuple[str, List[Dict]]:
    """
    全メトリクスをチェック

    Returns: (overall_status, checks)
    """
    checks = []

    # 結果からメトリクスを抽出
    rtt_metrics = None
    throughput_metrics = None

    for r in results.get("results", []):
        if r.get("test_name") == "rtt":
            rtt_metrics = r.get("metrics", {})
        elif r.get("test_name") == "throughput":
            throughput_metrics = r.get("metrics", {})

    # p50 チェック
    if rtt_metrics and "p50_us" in rtt_metrics:
        status, check = check_metric(
            "p50 Latency",
            rtt_metrics["p50_us"],
            thresholds.get("p50_us", DEFAULT_THRESHOLDS["p50_us"]),
            warn_thresholds.get("p50_us", WARN_THRESHOLDS["p50_us"]),
            "<="
        )
        checks.append(check)

    # p99 チェック
    if rtt_metrics and "p99_us" in rtt_metrics:
        status, check = check_metric(
            "p99 Latency",
            rtt_metrics["p99_us"],
            thresholds.get("p99_us", DEFAULT_THRESHOLDS["p99_us"]),
            warn_thresholds.get("p99_us", WARN_THRESHOLDS["p99_us"]),
            "<="
        )
        checks.append(check)

    # max チェック
    if rtt_metrics and "max_us" in rtt_metrics:
        status, check = check_metric(
            "Max Latency",
            rtt_metrics["max_us"],
            thresholds.get("max_us", DEFAULT_THRESHOLDS["max_us"]),
            warn_thresholds.get("max_us", WARN_THRESHOLDS["max_us"]),
            "<="
        )
        checks.append(check)

    # スループットチェック（wrk を優先）
    throughput_source = "bench_gateway.py"
    throughput_value = None
    if wrk_metrics and "throughput_rps" in wrk_metrics:
        throughput_source = "wrk"
        throughput_value = wrk_metrics["throughput_rps"]
    elif throughput_metrics and "throughput_rps" in throughput_metrics:
        throughput_value = throughput_metrics["throughput_rps"]

    if throughput_value is not None:
        status, check = check_metric(
            "Throughput",
            throughput_value,
            thresholds.get("throughput_rps", DEFAULT_THRESHOLDS["throughput_rps"]),
            warn_thresholds.get("throughput_rps", WARN_THRESHOLDS["throughput_rps"]),
            ">="
        )
        check["source"] = throughput_source
        checks.append(check)

    # 全体ステータス決定
    statuses = [c["status"] for c in checks]
    if "FAIL" in statuses:
        overall = "FAIL"
    elif "WARN" in statuses:
        overall = "WARN"
    else:
        overall = "PASS"

    return overall, checks


def compare_baseline(
    current: Dict[str, Dict[str, float]],
    baseline: Dict[str, Dict[str, float]],
    regression_threshold: float
) -> List[Dict]:
    """Compare current metrics against baseline; returns WARN checks on regressions."""
    checks: List[Dict] = []
    bench_current = current.get("bench", {})
    bench_base = baseline.get("bench", {})
    wrk_current = current.get("wrk", {})
    wrk_base = baseline.get("wrk", {})

    def add_regression_check(name: str, actual: float, base: float, operator: str, source: str):
        if base <= 0:
            return
        if operator == "<=":
            regression = (actual - base) / base
            passed = regression <= regression_threshold
        else:
            regression = (base - actual) / base
            passed = regression <= regression_threshold
        status = "PASS" if passed else "WARN"
        checks.append({
            "name": name,
            "actual": actual,
            "threshold": base,
            "warn_threshold": base,
            "operator": operator,
            "status": status,
            "pct_diff": regression * 100,
            "source": source,
            "baseline": True,
        })

    for metric in ("p50_us", "p99_us", "max_us"):
        if metric in bench_current and metric in bench_base:
            label = f"Baseline {metric.replace('_us', '').upper()} Latency"
            add_regression_check(label, bench_current[metric], bench_base[metric], "<=", "bench_gateway.py")

    # Throughput baseline prefers wrk if available
    if "throughput_rps" in wrk_current and "throughput_rps" in wrk_base:
        add_regression_check("Baseline Throughput", wrk_current["throughput_rps"], wrk_base["throughput_rps"], ">=", "wrk")
    elif "throughput_rps" in bench_current and "throughput_rps" in bench_base:
        add_regression_check("Baseline Throughput", bench_current["throughput_rps"], bench_base["throughput_rps"], ">=", "bench_gateway.py")

    return checks


def print_summary(overall_status: str, checks: List[Dict]):
    """結果サマリーを表示"""
    if overall_status == "PASS":
        color = Colors.GREEN
        emoji = "✅"
    elif overall_status == "WARN":
        color = Colors.YELLOW
        emoji = "⚠️"
    else:
        color = Colors.RED
        emoji = "❌"

    print(f"\n{color}{Colors.BOLD}{'='*60}{Colors.END}")
    print(f"{color}{Colors.BOLD}{emoji} Performance Gate: {overall_status}{Colors.END}")
    print(f"{color}{Colors.BOLD}{'='*60}{Colors.END}\n")

    # 各チェックの結果
    for check in checks:
        status = check["status"]
        if status == "PASS":
            symbol = f"{Colors.GREEN}✓{Colors.END}"
        elif status == "WARN":
            symbol = f"{Colors.YELLOW}⚠{Colors.END}"
        else:
            symbol = f"{Colors.RED}✗{Colors.END}"

        op = check["operator"]
        actual = check["actual"]
        threshold = check["threshold"]
        name = check["name"]
        if check.get("source"):
            name = f"{name} ({check['source']})"

        # 単位を決定
        if "Latency" in name:
            unit = "µs"
        elif "Throughput" in name:
            unit = "req/s"
        else:
            unit = ""

        print(f"  {symbol} {name}: {actual:.0f}{unit} {op} {threshold:.0f}{unit} [{status}]")
        if status != "PASS":
            print(f"     Warn threshold: {check['warn_threshold']:.0f}{unit}")

    print()


def generate_github_summary(overall_status: str, checks: List[Dict], results: Dict) -> str:
    """GitHub Actions Step Summary (Markdown) を生成"""
    lines = []

    # ヘッダー
    if overall_status == "PASS":
        emoji = "✅"
        badge = "![PASS](https://img.shields.io/badge/Perf-PASS-brightgreen)"
    elif overall_status == "WARN":
        emoji = "⚠️"
        badge = "![WARN](https://img.shields.io/badge/Perf-WARN-yellow)"
    else:
        emoji = "❌"
        badge = "![FAIL](https://img.shields.io/badge/Perf-FAIL-red)"

    lines.append(f"# {emoji} Performance Gate: {overall_status}")
    lines.append("")
    lines.append(badge)
    lines.append("")

    # 結果テーブル
    lines.append("## Performance Checks")
    lines.append("")
    lines.append("| Status | Metric | Actual | Threshold | Result |")
    lines.append("|--------|--------|--------|-----------|--------|")

    for check in checks:
        status = check["status"]
        if status == "PASS":
            status_emoji = "✅"
        elif status == "WARN":
            status_emoji = "⚠️"
        else:
            status_emoji = "❌"

        # 単位
        if "Latency" in check["name"]:
            unit = "µs"
        else:
            unit = "req/s"

        actual = f"{check['actual']:.0f}{unit}"
        threshold = f"{check['operator']} {check['threshold']:.0f}{unit}"

        name = check["name"]
        if check.get("source"):
            name = f"{name} ({check['source']})"
        lines.append(f"| {status_emoji} | {name} | {actual} | {threshold} | {status} |")

    lines.append("")

    # 基準説明
    lines.append("## Performance Standards")
    lines.append("")
    lines.append("| Level | p50 | p99 | max | Throughput |")
    lines.append("|-------|-----|-----|-----|------------|")
    lines.append("| HFT | < 500µs | < 2ms | < 10ms | > 50k/s |")
    lines.append("| Institution | < 1ms | < 10ms | < 50ms | > 10k/s |")
    lines.append("| Retail | < 10ms | < 100ms | < 500ms | > 1k/s |")
    lines.append("")

    return "\n".join(lines)


def main():
    parser = argparse.ArgumentParser(
        description="Performance Gate - Gateway Performance Quality Gate",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__
    )

    # 入力オプション
    parser.add_argument("--run", action="store_true",
                        help="Run benchmark before checking")
    parser.add_argument("--input", "-i", type=Path,
                        help="Input benchmark results JSON file")
    parser.add_argument("--wrk-input", type=Path,
                        help="Optional wrk output file for throughput check")

    # ベンチマーク設定
    parser.add_argument("--host", default="localhost", help="Gateway host")
    parser.add_argument("--port", type=int, default=8081, help="Gateway port")
    parser.add_argument("--requests", type=int, default=1000, help="Number of requests for RTT")
    parser.add_argument("--duration", type=int, default=5, help="Duration for throughput test")
    parser.add_argument("--concurrency", type=int, default=50, help="Concurrent workers")
    parser.add_argument("--warmup-rtt", type=int, default=50, help="Warmup requests for RTT")
    parser.add_argument("--warmup-throughput-sec", type=int, default=0,
                        help="Warmup seconds for throughput test")

    # 閾値オプション
    parser.add_argument("--p50", type=float, help=f"p50 threshold (µs, default: {DEFAULT_THRESHOLDS['p50_us']})")
    parser.add_argument("--p99", type=float, help=f"p99 threshold (µs, default: {DEFAULT_THRESHOLDS['p99_us']})")
    parser.add_argument("--max", type=float, help=f"max threshold (µs, default: {DEFAULT_THRESHOLDS['max_us']})")
    parser.add_argument("--throughput", type=float,
                        help=f"throughput threshold (req/s, default: {DEFAULT_THRESHOLDS['throughput_rps']})")

    # 出力オプション
    parser.add_argument("--github-summary", type=Path,
                        help="Output file for GitHub Actions Step Summary")
    parser.add_argument("--ci", action="store_true",
                        help="CI mode: exit 1 on FAIL, 0 on PASS/WARN")
    parser.add_argument("--strict", action="store_true",
                        help="Strict mode: exit 1 on WARN as well")
    parser.add_argument("--baseline", type=Path,
                        help="Baseline JSON for regression check")
    parser.add_argument("--update-baseline", type=Path,
                        help="Write current metrics as baseline JSON")
    parser.add_argument("--baseline-regression", type=float, default=0.05,
                        help="Allowed regression vs baseline (default: 0.05 = 5%)")

    args = parser.parse_args()

    # 入力チェック
    if not args.run and not args.input:
        print(f"{Colors.RED}ERROR: Specify --run or --input{Colors.END}", file=sys.stderr)
        parser.print_help()
        sys.exit(1)

    # ベンチマーク実行または結果読み込み
    if args.run:
        results = run_benchmark(
            host=args.host,
            port=args.port,
            requests=args.requests,
            duration=args.duration,
            concurrency=args.concurrency,
            warmup_rtt=args.warmup_rtt,
            warmup_throughput_sec=args.warmup_throughput_sec
        )
    else:
        results = load_results(args.input)

    if not results:
        print(f"{Colors.RED}ERROR: No results to check{Colors.END}", file=sys.stderr)
        sys.exit(1)

    wrk_metrics = None
    if args.wrk_input:
        wrk_metrics = load_wrk_metrics(args.wrk_input)

    # 閾値設定
    thresholds = DEFAULT_THRESHOLDS.copy()
    if args.p50:
        thresholds["p50_us"] = args.p50
    if args.p99:
        thresholds["p99_us"] = args.p99
    if args.max:
        thresholds["max_us"] = args.max
    if args.throughput:
        thresholds["throughput_rps"] = args.throughput

    # チェック実行
    overall_status, checks = run_checks(results, thresholds, WARN_THRESHOLDS, wrk_metrics)

    current_metrics = {
        "bench": extract_bench_metrics(results),
    }
    if wrk_metrics:
        current_metrics["wrk"] = wrk_metrics

    if args.baseline:
        if not args.baseline.exists():
            print(f"{Colors.RED}ERROR: Baseline not found: {args.baseline}{Colors.END}", file=sys.stderr)
            sys.exit(1)
        with open(args.baseline, "r") as f:
            baseline = json.load(f)
        baseline_checks = compare_baseline(current_metrics, baseline, args.baseline_regression)
        checks.extend(baseline_checks)
        baseline_statuses = [c["status"] for c in baseline_checks]
        if "WARN" in baseline_statuses and overall_status == "PASS":
            overall_status = "WARN"

    if args.update_baseline:
        baseline_payload = {
            "updated_at": datetime.utcnow().isoformat() + "Z",
            "bench": current_metrics.get("bench", {}),
        }
        if "wrk" in current_metrics:
            baseline_payload["wrk"] = current_metrics["wrk"]
        args.update_baseline.parent.mkdir(parents=True, exist_ok=True)
        with open(args.update_baseline, "w") as f:
            json.dump(baseline_payload, f, indent=2, sort_keys=True)
        print(f"Baseline updated: {args.update_baseline}")

    # 結果表示
    print_summary(overall_status, checks)

    # GitHub Summary 出力
    if args.github_summary:
        summary = generate_github_summary(overall_status, checks, results)
        args.github_summary.parent.mkdir(parents=True, exist_ok=True)
        with open(args.github_summary, 'w') as f:
            f.write(summary)
        print(f"GitHub Step Summary written to: {args.github_summary}")

    # 終了コード
    if overall_status == "FAIL":
        print(f"{Colors.RED}Performance Gate: FAILED{Colors.END}")
        sys.exit(1)
    elif overall_status == "WARN" and args.strict:
        print(f"{Colors.YELLOW}Performance Gate: WARNING (strict mode){Colors.END}")
        sys.exit(1)
    else:
        status_color = Colors.GREEN if overall_status == "PASS" else Colors.YELLOW
        print(f"{status_color}Performance Gate: {overall_status}{Colors.END}")
        sys.exit(0)


if __name__ == "__main__":
    main()

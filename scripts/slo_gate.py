"""
SLO Gate - Benchmark results validation against SLO targets
Reads benchmark results, validates against schema, checks SLO compliance
"""

import json
import sys
import argparse
from pathlib import Path
from typing import Dict, List, Tuple, Any
from datetime import datetime

try:
    import jsonschema
    from jsonschema import validate, ValidationError
except ImportError:
    print("ERROR: jsonschema not installed. Run: pip install jsonschema", file=sys.stderr)
    sys.exit(1)


# SLO Thresholds (aligned with docs/specs/slo.md)
SLO_THRESHOLDS = {
    "fast_path_p99_us": 100.0,
    "fast_path_p999_us": 3000.0,
    "tail_ratio_max": 12.0,
    "drop_count_max": 0,
    "error_rate_percent_max": 0.01,
    "throughput_min_events_per_sec": 10000,
}


class Colors:
    """ANSI color codes for terminal output"""
    GREEN = "\033[92m"
    YELLOW = "\033[93m"
    RED = "\033[91m"
    BLUE = "\033[94m"
    BOLD = "\033[1m"
    END = "\033[0m"


def load_schema(schema_path: Path) -> Dict:
    """Load JSON schema from file"""
    if not schema_path.exists():
        print(f"ERROR: Schema file not found: {schema_path}", file=sys.stderr)
        sys.exit(1)

    with open(schema_path, 'r') as f:
        return json.load(f)


def load_results(results_path: Path) -> Dict:
    """Load benchmark results from file"""
    if not results_path.exists():
        print(f"ERROR: Results file not found: {results_path}", file=sys.stderr)
        sys.exit(1)

    with open(results_path, 'r') as f:
        return json.load(f)


def validate_schema(results: Dict, schema: Dict) -> bool:
    """Validate results against JSON schema"""
    try:
        validate(instance=results, schema=schema)
        print(f"{Colors.GREEN}✓{Colors.END} Schema validation passed")
        return True
    except ValidationError as e:
        print(f"{Colors.RED}✗{Colors.END} Schema validation failed:")
        print(f"  {e.message}", file=sys.stderr)
        print(f"  Path: {' -> '.join(str(p) for p in e.path)}", file=sys.stderr)
        return False


def check_slo(
    name: str,
    metric_path: str,
    operator: str,
    threshold: float,
    actual: float,
    severity: str = "critical"
) -> Tuple[bool, Dict]:
    """
    Check a single SLO condition

    Returns: (passed, check_result_dict)
    """
    operators = {
        "<=": lambda a, t: a <= t,
        ">=": lambda a, t: a >= t,
        "==": lambda a, t: a == t,
        "<": lambda a, t: a < t,
        ">": lambda a, t: a > t,
    }

    if operator not in operators:
        raise ValueError(f"Unknown operator: {operator}")

    passed = operators[operator](actual, threshold)

    # Calculate percentage difference
    if threshold > 0:
        pct_diff = ((actual - threshold) / threshold) * 100
    else:
        pct_diff = 0

    message = f"{name}: {actual:.3f} {operator} {threshold:.3f}"
    if not passed:
        message += f" (FAIL: {pct_diff:+.1f}%)"
    else:
        message += f" (PASS)"

    return passed, {
        "name": name,
        "metric": metric_path,
        "operator": operator,
        "threshold": threshold,
        "actual": actual,
        "passed": passed,
        "severity": severity,
        "message": message
    }


def get_nested_value(data: Dict, path: str) -> Any:
    """Get value from nested dict using dot-notation path"""
    keys = path.split('.')
    value = data
    for key in keys:
        if isinstance(value, dict) and key in value:
            value = value[key]
        else:
            return None
    return value


def run_slo_checks(results: Dict) -> Tuple[str, List[Dict]]:
    """
    Run all SLO checks against benchmark results

    Returns: (status, checks_list)
    status: "PASS", "WARN", or "FAIL"
    """
    checks = []

    # Extract metrics
    metrics = results.get("metrics", {})
    fast_path = metrics.get("fast_path", {})
    process_latency = fast_path.get("process_latency_us", {})
    persistence_queue = metrics.get("persistence_queue", {})
    summary = metrics.get("summary", {})

    # Check 1: Fast Path p99 latency
    p99 = process_latency.get("p99")
    if p99 is not None:
        passed, check = check_slo(
            name="Fast Path p99",
            metric_path="metrics.fast_path.process_latency_us.p99",
            operator="<=",
            threshold=SLO_THRESHOLDS["fast_path_p99_us"],
            actual=p99,
            severity="critical"
        )
        checks.append(check)

    # Check 2: Fast Path p999 latency
    p999 = process_latency.get("p999")
    if p999 is not None:
        passed, check = check_slo(
            name="Fast Path p999",
            metric_path="metrics.fast_path.process_latency_us.p999",
            operator="<=",
            threshold=SLO_THRESHOLDS["fast_path_p999_us"],
            actual=p999,
            severity="critical"
        )
        checks.append(check)

    # Check 3: Tail ratio (p99/p50)
    tail_ratio = summary.get("tail_ratio")
    if tail_ratio is not None:
        passed, check = check_slo(
            name="Tail Ratio",
            metric_path="metrics.summary.tail_ratio",
            operator="<=",
            threshold=SLO_THRESHOLDS["tail_ratio_max"],
            actual=tail_ratio,
            severity="warning"
        )
        checks.append(check)

    # Check 4: Drop count
    drop_count = fast_path.get("drop_count", 0)
    passed, check = check_slo(
        name="Drop Count",
        metric_path="metrics.fast_path.drop_count",
        operator="==",
        threshold=SLO_THRESHOLDS["drop_count_max"],
        actual=drop_count,
        severity="critical"
    )
    checks.append(check)

    # Check 5: Error rate
    error_rate = summary.get("error_rate_percent", 0)
    passed, check = check_slo(
        name="Error Rate",
        metric_path="metrics.summary.error_rate_percent",
        operator="<=",
        threshold=SLO_THRESHOLDS["error_rate_percent_max"],
        actual=error_rate,
        severity="critical"
    )
    checks.append(check)

    # Determine overall status
    critical_failures = [c for c in checks if not c["passed"] and c["severity"] == "critical"]
    warning_failures = [c for c in checks if not c["passed"] and c["severity"] == "warning"]

    if critical_failures:
        status = "FAIL"
    elif warning_failures:
        status = "WARN"
    else:
        status = "PASS"

    return status, checks


def print_summary(status: str, checks: List[Dict], verbose: bool = True):
    """Print SLO check summary to console"""
    # Status header
    if status == "PASS":
        color = Colors.GREEN
        emoji = "✅"
    elif status == "WARN":
        color = Colors.YELLOW
        emoji = "⚠️"
    else:  # FAIL
        color = Colors.RED
        emoji = "❌"

    print(f"\n{color}{Colors.BOLD}{'='*60}{Colors.END}")
    print(f"{color}{Colors.BOLD}{emoji} SLO Gate: {status}{Colors.END}")
    print(f"{color}{Colors.BOLD}{'='*60}{Colors.END}\n")

    # Individual checks
    for check in checks:
        passed = check["passed"]
        severity = check["severity"]
        message = check["message"]

        if passed:
            symbol = f"{Colors.GREEN}✓{Colors.END}"
        elif severity == "critical":
            symbol = f"{Colors.RED}✗{Colors.END}"
        else:
            symbol = f"{Colors.YELLOW}⚠{Colors.END}"

        print(f"  {symbol} {message}")

        if verbose and not passed:
            print(f"     Metric: {check['metric']}")
            print(f"     Severity: {severity}")

    print()


def generate_github_summary(status: str, checks: List[Dict], results: Dict) -> str:
    """Generate GitHub Actions Step Summary (Markdown)"""
    lines = []

    # Header
    if status == "PASS":
        emoji = "✅"
        color_badge = "![PASS](https://img.shields.io/badge/SLO-PASS-brightgreen)"
    elif status == "WARN":
        emoji = "⚠️"
        color_badge = "![WARN](https://img.shields.io/badge/SLO-WARN-yellow)"
    else:
        emoji = "❌"
        color_badge = "![FAIL](https://img.shields.io/badge/SLO-FAIL-red)"

    lines.append(f"# {emoji} SLO Gate: {status}")
    lines.append("")
    lines.append(color_badge)
    lines.append("")

    # Summary table
    lines.append("## SLO Checks")
    lines.append("")
    lines.append("| Status | Check | Actual | Threshold | Result |")
    lines.append("|--------|-------|--------|-----------|--------|")

    for check in checks:
        passed = check["passed"]
        if passed:
            status_emoji = "✅"
        elif check["severity"] == "critical":
            status_emoji = "❌"
        else:
            status_emoji = "⚠️"

        actual = f"{check['actual']:.3f}"
        threshold = f"{check['operator']} {check['threshold']:.3f}"
        result = "PASS" if passed else "FAIL"

        lines.append(f"| {status_emoji} | {check['name']} | {actual} | {threshold} | {result} |")

    lines.append("")

    # Key metrics
    lines.append("## Key Metrics")
    lines.append("")
    metrics = results.get("metrics", {})
    fast_path = metrics.get("fast_path", {})
    process_latency = fast_path.get("process_latency_us", {})
    summary = metrics.get("summary", {})

    lines.append(f"- **p50**: {process_latency.get('p50', 'N/A'):.3f} μs")
    lines.append(f"- **p99**: {process_latency.get('p99', 'N/A'):.3f} μs")
    lines.append(f"- **p999**: {process_latency.get('p999', 'N/A'):.3f} μs")
    lines.append(f"- **Tail Ratio**: {summary.get('tail_ratio', 'N/A'):.2f}")
    lines.append(f"- **Throughput**: {summary.get('throughput_events_per_sec', 'N/A'):.0f} events/s")
    lines.append(f"- **Drop Count**: {fast_path.get('drop_count', 0)}")
    lines.append("")

    return "\n".join(lines)


def main():
    parser = argparse.ArgumentParser(description="SLO Gate - Validate benchmark results against SLO targets")
    parser.add_argument("--in", dest="input", required=True, help="Input benchmark results JSON file")
    parser.add_argument("--schema", required=True, help="JSON schema file for validation")
    parser.add_argument("--github-summary", help="Output file for GitHub Actions Step Summary (Markdown)")
    parser.add_argument("--baseline", help="Baseline results for comparison (optional)")
    parser.add_argument("--verbose", action="store_true", help="Verbose output")
    parser.add_argument("--no-fail", action="store_true", help="Exit with 0 even on SLO failures (for testing)")

    args = parser.parse_args()

    # Load files
    results = load_results(Path(args.input))
    schema = load_schema(Path(args.schema))

    # Validate schema
    if not validate_schema(results, schema):
        sys.exit(1)

    # Run SLO checks
    status, checks = run_slo_checks(results)

    # Add slo_compliance to results
    results["slo_compliance"] = {
        "status": status,
        "checks": checks,
        "timestamp": datetime.utcnow().isoformat() + "Z"
    }

    # Write updated results back
    with open(args.input, 'w') as f:
        json.dump(results, f, indent=2)

    # Print summary
    print_summary(status, checks, verbose=args.verbose)

    # Generate GitHub summary if requested
    if args.github_summary:
        gh_summary = generate_github_summary(status, checks, results)
        with open(args.github_summary, 'w') as f:
            f.write(gh_summary)
        print(f"GitHub Step Summary written to: {args.github_summary}")

    # Exit code
    if status == "FAIL" and not args.no_fail:
        print(f"{Colors.RED}SLO Gate: FAILED - Exiting with code 1{Colors.END}")
        sys.exit(1)
    elif status == "WARN":
        print(f"{Colors.YELLOW}SLO Gate: WARNING - Exiting with code 0{Colors.END}")
        sys.exit(0)
    else:
        print(f"{Colors.GREEN}SLO Gate: PASSED - Exiting with code 0{Colors.END}")
        sys.exit(0)


if __name__ == "__main__":
    main()

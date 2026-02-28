"""
Order API Benchmark Script

Measures ACK latency and throughput for /api/orders (app -> gateway path).
"""

import argparse
import json
import math
import os
import random
import time
from datetime import datetime
from urllib.error import HTTPError
from urllib.request import Request, urlopen


def generate_order(symbol: str, limit_ratio: float):
    side = random.choice(["BUY", "SELL"])
    is_limit = random.random() < limit_ratio
    order_type = "LIMIT" if is_limit else "MARKET"
    quantity = random.randint(1, 10)
    price = random.randint(50000, 51000) if is_limit else None
    return {
        "symbol": symbol,
        "side": side,
        "type": order_type,
        "quantity": quantity,
        "price": price,
    }


def percentile(values, p):
    if not values:
        return 0.0
    values = sorted(values)
    if len(values) == 1:
        return values[0]
    k = (len(values) - 1) * p
    f = math.floor(k)
    c = math.ceil(k)
    if f == c:
        return values[int(k)]
    return values[f] * (c - k) + values[c] * (k - f)


def main():
    parser = argparse.ArgumentParser(description="Order API Benchmark")
    parser.add_argument("--runs", type=int, default=10000, help="Number of orders to send")
    parser.add_argument("--symbols", type=str, default="BTC", help="Comma-separated symbols")
    parser.add_argument("--endpoint", type=str, default="http://localhost:8080/api/orders", help="HTTP endpoint")
    parser.add_argument("--timeout", type=float, default=2.0, help="Request timeout seconds")
    parser.add_argument("--limit-ratio", type=float, default=0.8, help="Ratio of LIMIT orders")
    parser.add_argument("--out", type=str, default="results/order_api_bench.json", help="Output file")
    args = parser.parse_args()

    symbols = args.symbols.split(",")

    print("==> Order API Benchmark")
    print(f"  Runs: {args.runs}")
    print(f"  Symbols: {args.symbols}")
    print(f"  Endpoint: {args.endpoint}")
    print(f"  Timeout: {args.timeout}s")
    print(f"  Limit ratio: {args.limit_ratio}")
    print(f"  Output: {args.out}")
    print("  Note: app (8080) and gateway must be running")
    print()

    status_counts = {}
    latencies_ms = []
    fail = 0
    start_time = time.time()

    for i in range(args.runs):
        sym = symbols[i % len(symbols)]
        payload = generate_order(sym, args.limit_ratio)
        body = json.dumps(payload).encode("utf-8")
        req = Request(
            args.endpoint,
            data=body,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        start = time.perf_counter()
        try:
            resp = urlopen(req, timeout=args.timeout)
            latency_ms = (time.perf_counter() - start) * 1000.0
            status_counts[resp.status] = status_counts.get(resp.status, 0) + 1
            latencies_ms.append(latency_ms)
        except HTTPError as e:
            latency_ms = (time.perf_counter() - start) * 1000.0
            status_counts[e.code] = status_counts.get(e.code, 0) + 1
            latencies_ms.append(latency_ms)
        except Exception as e:
            fail += 1
            if fail <= 5:
                print(f"ERROR: {e}")

        if (i + 1) % 1000 == 0:
            ok = sum(status_counts.values())
            print(f"\r  Progress: {i+1}/{args.runs} ({ok} OK, {fail} FAIL)", end="", flush=True)

    print()
    elapsed_s = time.time() - start_time
    ok = sum(status_counts.values())
    throughput = ok / elapsed_s if elapsed_s > 0 else 0.0

    p50 = percentile(latencies_ms, 0.50)
    p95 = percentile(latencies_ms, 0.95)
    p99 = percentile(latencies_ms, 0.99)

    print("==> Results")
    print(f"  OK: {ok}")
    print(f"  Failed: {fail}")
    print(f"  Elapsed: {elapsed_s:.2f}s")
    print(f"  Throughput: {throughput:.2f} req/s")
    print(f"  ACK latency p50: {p50:.2f} ms")
    print(f"  ACK latency p95: {p95:.2f} ms")
    print(f"  ACK latency p99: {p99:.2f} ms")
    print("  Status counts:", status_counts)

    os.makedirs(os.path.dirname(args.out), exist_ok=True)
    results = {
        "timestamp": datetime.utcnow().isoformat() + "Z",
        "config": {
            "runs": args.runs,
            "symbols": args.symbols,
            "endpoint": args.endpoint,
            "timeout_s": args.timeout,
            "limit_ratio": args.limit_ratio,
        },
        "results": {
            "ok": ok,
            "failed": fail,
            "elapsed_s": elapsed_s,
            "throughput_rps": throughput,
            "latency_ms": {
                "p50": p50,
                "p95": p95,
                "p99": p99,
            },
            "status_counts": status_counts,
        },
    }
    with open(args.out, "w") as f:
        json.dump(results, f, indent=2)

    print()
    print(f"==> Wrote {args.out}")


if __name__ == "__main__":
    main()

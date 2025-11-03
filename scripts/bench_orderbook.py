#!/usr/bin/env python3
"""
Order Book Benchmark Script

Measures Fast Path performance with Order Matching logic
Target: p99 < 100μs
"""

import argparse
import struct
import random
import time
import json
from datetime import datetime
from urllib.request import urlopen, Request
from urllib.error import URLError, HTTPError

def generate_order_payload():
    """Generate 8-byte order payload: side(1) + price(3) + quantity(4)"""
    side = random.randint(0, 1)  # 0=BUY, 1=SELL
    price = random.randint(50000, 51000)
    quantity = random.randint(1, 100)

    payload = struct.pack('B', side)  # side (1 byte)
    payload += struct.pack('>I', price)[1:]  # price (3 bytes, big-endian)
    payload += struct.pack('>I', quantity)  # quantity (4 bytes, big-endian)

    return payload

def main():
    parser = argparse.ArgumentParser(description='Order Book Benchmark')
    parser.add_argument('--runs', type=int, default=10000, help='Number of orders to send')
    parser.add_argument('--symbols', type=str, default='BTC', help='Comma-separated symbols')
    parser.add_argument('--endpoint', type=str, default='http://localhost:8080/events', help='HTTP endpoint')
    parser.add_argument('--out', type=str, default='results/orderbook_bench.json', help='Output file')
    args = parser.parse_args()

    symbols = args.symbols.split(',')

    print("==> Order Book Benchmark")
    print(f"  Runs: {args.runs}")
    print(f"  Symbols: {args.symbols}")
    print(f"  Endpoint: {args.endpoint}")
    print(f"  Output: {args.out}")
    print()

    # Run benchmark
    success = 0
    fail = 0
    start_time = time.time()

    for i in range(args.runs):
        sym = symbols[i % len(symbols)]
        payload = generate_order_payload()

        try:
            req = Request(
                f"{args.endpoint}?key={sym}",
                data=payload,
                headers={'Content-Type': 'application/octet-stream'},
                method='POST'
            )
            response = urlopen(req, timeout=1)

            if response.status == 200:
                success += 1
            else:
                fail += 1
                if fail <= 5:
                    print(f"WARN: HTTP {response.status} for key={sym}")
        except Exception as e:
            fail += 1
            if fail <= 5:
                print(f"ERROR: {e}")

        # Progress
        if (i + 1) % 1000 == 0:
            print(f"\r  Progress: {i+1}/{args.runs} ({success} OK, {fail} FAIL)", end='', flush=True)

    print()

    elapsed_s = time.time() - start_time
    throughput = success / elapsed_s if elapsed_s > 0 else 0

    print("==> Results")
    print(f"  Success: {success}")
    print(f"  Failed: {fail}")
    print(f"  Elapsed: {elapsed_s:.2f}s")
    print(f"  Throughput: {throughput:.2f} req/s")

    # Fetch stats from /stats endpoint
    stats_endpoint = args.endpoint.replace('/events', '/stats')
    try:
        req = Request(stats_endpoint)
        stats_response = urlopen(req, timeout=5)
        stats_json = json.loads(stats_response.read().decode('utf-8'))
    except:
        stats_json = {}

    # Extract key metrics
    fast_path_count = stats_json.get('fast_path_count', 0)
    slow_path_count = stats_json.get('slow_path_count', 0)
    fallback_count = stats_json.get('fallback_count', 0)
    avg_publish_us = stats_json.get('fast_path_avg_publish_us', 0)
    avg_process_us = stats_json.get('fast_path_avg_process_us', 0)
    p50_us = stats_json.get('fast_path_process_p50_us', 0)
    p99_us = stats_json.get('fast_path_process_p99_us', 0)
    p999_us = stats_json.get('fast_path_process_p999_us', 0)
    drop_count = stats_json.get('fast_path_drop_count', 0)

    print()
    print("==> Fast Path Metrics")
    print(f"  Fast Path Count: {fast_path_count}")
    print(f"  Slow Path Count: {slow_path_count}")
    print(f"  Fallback Count: {fallback_count}")
    print(f"  Drop Count: {drop_count}")
    print(f"  Avg Publish Latency: {avg_publish_us:.2f}μs")
    print(f"  Avg Process Latency: {avg_process_us:.2f}μs")
    print(f"  P50 Latency: {p50_us:.2f}μs")
    print(f"  P99 Latency: {p99_us:.2f}μs")
    print(f"  P999 Latency: {p999_us:.2f}μs")

    # Write results
    import os
    os.makedirs(os.path.dirname(args.out), exist_ok=True)

    results = {
        'timestamp': datetime.utcnow().isoformat() + 'Z',
        'config': {
            'runs': args.runs,
            'symbols': args.symbols,
            'endpoint': args.endpoint
        },
        'results': {
            'success': success,
            'failed': fail,
            'elapsed_s': elapsed_s,
            'throughput_rps': throughput
        },
        'stats': stats_json
    }

    with open(args.out, 'w') as f:
        json.dump(results, f, indent=2)

    print()
    print(f"==> Wrote {args.out}")

    # Evaluate against target (p99 < 100μs)
    print()
    print("==> Performance Evaluation")
    if p99_us > 0:
        if p99_us < 100:
            print(f"  ✓ Target met: p99 = {p99_us:.2f}μs < 100μs")
        else:
            print(f"  ✗ Target missed: p99 = {p99_us:.2f}μs >= 100μs")
    else:
        print("  ! No metrics available (ensure FAST_PATH_METRICS=1)")

    if drop_count > 0:
        print(f"  ⚠ Warning: {drop_count} events dropped (buffer full)")

    if fallback_count > 0:
        print(f"  ⚠ Warning: {fallback_count} events fell back to Slow Path")

if __name__ == '__main__':
    main()

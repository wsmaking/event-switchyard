#!/usr/bin/env python3
"""
Gateway Performance Benchmark Tool

Usage:
    # Sequential RTT (p50/p99/max測定)
    python bench_gateway.py rtt --requests 1000

    # Throughput burst (スループット測定)
    python bench_gateway.py throughput --duration 5 --concurrency 50

    # Sustained load (一定時間の持続負荷)
    python bench_gateway.py sustained --duration 30 --rps 1000

    # Heavy request (大量パラメータの重いリクエスト)
    python bench_gateway.py heavy --requests 100

    # All benchmarks
    python bench_gateway.py all

Results are saved to results/bench_YYYYMMDD_HHMMSS.json
"""

import argparse
import concurrent.futures
import hmac
import hashlib
import base64
import http.client
import json
import os
import statistics
import sys
import time
from datetime import datetime
from dataclasses import dataclass, asdict
from typing import List, Dict, Any, Optional, Tuple


@dataclass
class BenchmarkResult:
    """ベンチマーク結果"""
    test_name: str
    timestamp: str
    config: Dict[str, Any]
    metrics: Dict[str, Any]
    raw_latencies_us: Optional[List[float]] = None


def generate_jwt(secret: str, account_id: str = "12345") -> str:
    """JWT生成"""
    now = int(time.time())
    header = {"alg": "HS256", "typ": "JWT"}
    payload = {
        "sub": f"user_{account_id}",
        "account_id": account_id,
        "iat": now,
        "exp": now + 86400
    }

    def b64url(data: bytes) -> str:
        return base64.urlsafe_b64encode(data).rstrip(b"=").decode()

    h = b64url(json.dumps(header).encode())
    p = b64url(json.dumps(payload).encode())
    msg = f"{h}.{p}"
    sig = hmac.new(secret.encode(), msg.encode(), hashlib.sha256).digest()
    s = b64url(sig)
    return f"{h}.{p}.{s}"


def percentile(values: List[float], p: float) -> float:
    """パーセンタイル計算"""
    if not values:
        return 0.0
    values = sorted(values)
    k = int(round(p * (len(values) - 1)))
    return values[k]


class GatewayBenchmark:
    """Gateway ベンチマークツール"""

    def __init__(
        self,
        host: str = "localhost",
        port: int = 8081,
        jwt_secret: str = "secret123",
        reuse_token: bool = True,
    ):
        self.host = host
        self.port = port
        self.jwt_secret = jwt_secret
        self.base_url = f"http://{host}:{port}"
        self.reuse_token = reuse_token
        self.cached_tokens: Dict[str, str] = {}

    def _make_connection(self) -> http.client.HTTPConnection:
        return http.client.HTTPConnection(self.host, self.port, timeout=10)

    def _make_order_payload(self, heavy: bool = False, client_order_id: str | None = None) -> bytes:
        """注文ペイロード生成"""
        payload = {
            "symbol": "AAPL",
            "side": "BUY",
            "type": "LIMIT",
            "qty": 100,
            "price": 15000,
            "timeInForce": "GTC",
        }
        if client_order_id:
            payload["clientOrderId"] = client_order_id
        if heavy:
            # 大きなclientOrderIdを追加（重いリクエストシミュレーション）
            payload["clientOrderId"] = "X" * 1000
        return json.dumps(payload).encode()

    def _make_headers(self, account_id: str = "12345") -> Dict[str, str]:
        """HTTPヘッダー生成"""
        if self.reuse_token:
            token = self.cached_tokens.get(account_id)
            if token is None:
                token = generate_jwt(self.jwt_secret, account_id)
                self.cached_tokens[account_id] = token
        else:
            token = generate_jwt(self.jwt_secret, account_id)
        return {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {token}"
        }

    def check_health(self) -> bool:
        """ヘルスチェック"""
        try:
            conn = self._make_connection()
            conn.request("GET", "/health")
            resp = conn.getresponse()
            data = resp.read()
            conn.close()
            return resp.status == 200
        except Exception as e:
            print(f"Health check failed: {e}", file=sys.stderr)
            return False

    def bench_rtt(self, requests: int = 1000, warmup: int = 50) -> BenchmarkResult:
        """
        Sequential RTT測定
        - 1リクエストずつ順次実行
        - 正確なp50/p95/p99/max測定
        """
        print(f"[RTT] Running {requests} sequential requests (warmup: {warmup})...")

        conn = self._make_connection()
        headers = self._make_headers()
        sample_body = self._make_order_payload(heavy=True, client_order_id="heavy_sample")

        # Warmup
        for _ in range(warmup):
            body = self._make_order_payload(client_order_id=f"warmup_{time.time_ns()}")
            conn.request("POST", "/orders", body=body, headers=headers)
            resp = conn.getresponse()
            resp.read()

        # Measure
        latencies_us = []
        accepted = 0
        rejected = 0

        for i in range(requests):
            start = time.perf_counter()
            body = self._make_order_payload(client_order_id=f"bench_{time.time_ns()}_{i}")
            conn.request("POST", "/orders", body=body, headers=headers)
            resp = conn.getresponse()
            resp.read()
            end = time.perf_counter()

            lat_us = (end - start) * 1_000_000
            latencies_us.append(lat_us)

            if resp.status == 202:
                accepted += 1
            else:
                rejected += 1

            if (i + 1) % 200 == 0:
                print(f"  Progress: {i+1}/{requests}")

        conn.close()

        metrics = {
            "requests": requests,
            "accepted": accepted,
            "rejected": rejected,
            "p50_us": percentile(latencies_us, 0.50),
            "p95_us": percentile(latencies_us, 0.95),
            "p99_us": percentile(latencies_us, 0.99),
            "max_us": max(latencies_us),
            "min_us": min(latencies_us),
            "avg_us": statistics.mean(latencies_us),
            "stddev_us": statistics.stdev(latencies_us) if len(latencies_us) > 1 else 0,
        }

        print(f"  p50: {metrics['p50_us']:.0f}µs, p99: {metrics['p99_us']:.0f}µs, max: {metrics['max_us']:.0f}µs")

        return BenchmarkResult(
            test_name="rtt",
            timestamp=datetime.utcnow().isoformat() + "Z",
            config={"requests": requests, "warmup": warmup},
            metrics=metrics,
            raw_latencies_us=latencies_us
        )

    def bench_throughput(self, duration_sec: int = 5, concurrency: int = 50,
                         num_accounts: int = 10, warmup_sec: int = 0) -> BenchmarkResult:
        """
        Throughput burst測定
        - 並列リクエストでスループット最大化
        - 複数アカウントでシャーディング効果を確認
        """
        warmup_msg = f", warmup {warmup_sec}s" if warmup_sec > 0 else ""
        print(f"[Throughput] Running {duration_sec}s burst with {concurrency} workers, {num_accounts} accounts{warmup_msg}...")

        headers_by_account = {
            str(10000 + i): self._make_headers(str(10000 + i))
            for i in range(num_accounts)
        }
        account_ids = list(headers_by_account.keys())

        results = {"accepted": 0, "rejected": 0, "errors": 0}
        latencies_us = []
        lock = __import__("threading").Lock()
        stop_flag = __import__("threading").Event()
        start_flag = __import__("threading").Event()

        def worker(worker_id: int):
            conn = self._make_connection()
            local_accepted = 0
            local_rejected = 0
            local_errors = 0
            local_latencies = []

            account_id = account_ids[worker_id % num_accounts]
            headers = headers_by_account[account_id]

            counter = 0
            while not stop_flag.is_set():
                try:
                    start = time.perf_counter()
                    body = self._make_order_payload(
                        client_order_id=f"bench_{account_id}_{worker_id}_{counter}"
                    )
                    counter += 1
                    conn.request("POST", "/orders", body=body, headers=headers)
                    resp = conn.getresponse()
                    resp.read()
                    end = time.perf_counter()

                    if start_flag.is_set():
                        local_latencies.append((end - start) * 1_000_000)
                        if resp.status == 202:
                            local_accepted += 1
                        else:
                            local_rejected += 1
                except Exception:
                    if start_flag.is_set():
                        local_errors += 1
                    try:
                        conn.close()
                    except:
                        pass
                    conn = self._make_connection()

            with lock:
                results["accepted"] += local_accepted
                results["rejected"] += local_rejected
                results["errors"] += local_errors
                latencies_us.extend(local_latencies)

            try:
                conn.close()
            except:
                pass

        with concurrent.futures.ThreadPoolExecutor(max_workers=concurrency) as executor:
            futures = [executor.submit(worker, i) for i in range(concurrency)]
            if warmup_sec > 0:
                time.sleep(warmup_sec)
            start_flag.set()
            start_time = time.time()
            time.sleep(duration_sec)
            stop_flag.set()
            concurrent.futures.wait(futures)

        elapsed = time.time() - start_time
        total_requests = results["accepted"] + results["rejected"] + results["errors"]
        throughput = total_requests / elapsed if elapsed > 0 else 0

        metrics = {
            "duration_sec": elapsed,
            "concurrency": concurrency,
            "num_accounts": num_accounts,
            "total_requests": total_requests,
            "accepted": results["accepted"],
            "rejected": results["rejected"],
            "errors": results["errors"],
            "throughput_rps": throughput,
            "p50_us": percentile(latencies_us, 0.50) if latencies_us else 0,
            "p95_us": percentile(latencies_us, 0.95) if latencies_us else 0,
            "p99_us": percentile(latencies_us, 0.99) if latencies_us else 0,
            "max_us": max(latencies_us) if latencies_us else 0,
        }

        print(f"  Throughput: {throughput:.0f} req/s, Accepted: {results['accepted']}, Rejected: {results['rejected']}")
        print(f"  p50: {metrics['p50_us']:.0f}µs, p99: {metrics['p99_us']:.0f}µs")

        return BenchmarkResult(
            test_name="throughput",
            timestamp=datetime.utcnow().isoformat() + "Z",
            config={
                "duration_sec": duration_sec,
                "concurrency": concurrency,
                "num_accounts": num_accounts,
                "warmup_sec": warmup_sec,
            },
            metrics=metrics
        )

    def bench_sustained(self, duration_sec: int = 30, target_rps: int = 1000,
                        num_accounts: int = 10) -> BenchmarkResult:
        """
        Sustained load測定
        - 一定のRPSを維持
        - 時間単位のメトリクスを記録
        """
        print(f"[Sustained] Running {duration_sec}s at {target_rps} RPS...")

        headers_by_account = {
            str(10000 + i): self._make_headers(str(10000 + i))
            for i in range(num_accounts)
        }
        account_ids = list(headers_by_account.keys())

        interval = 1.0 / target_rps
        results_per_second = []

        conn = self._make_connection()
        current_second = 0
        second_start = time.time()
        second_latencies = []
        second_accepted = 0
        second_rejected = 0

        start_time = time.time()
        request_count = 0

        while time.time() - start_time < duration_sec:
            account_id = account_ids[request_count % num_accounts]
            headers = headers_by_account[account_id]

            req_start = time.perf_counter()
            try:
                conn.request("POST", "/orders", body=body, headers=headers)
                resp = conn.getresponse()
                resp.read()

                lat_us = (time.perf_counter() - req_start) * 1_000_000
                second_latencies.append(lat_us)

                if resp.status == 202:
                    second_accepted += 1
                else:
                    second_rejected += 1
            except Exception:
                try:
                    conn.close()
                except:
                    pass
                conn = self._make_connection()

            request_count += 1

            # 秒が変わったらメトリクスを記録
            if time.time() - second_start >= 1.0:
                results_per_second.append({
                    "second": current_second,
                    "requests": len(second_latencies),
                    "accepted": second_accepted,
                    "rejected": second_rejected,
                    "p50_us": percentile(second_latencies, 0.50) if second_latencies else 0,
                    "p99_us": percentile(second_latencies, 0.99) if second_latencies else 0,
                    "max_us": max(second_latencies) if second_latencies else 0,
                })
                current_second += 1
                second_start = time.time()
                second_latencies = []
                second_accepted = 0
                second_rejected = 0
                print(f"  Second {current_second}: {results_per_second[-1]['requests']} req, p99: {results_per_second[-1]['p99_us']:.0f}µs")

            # レート制限
            elapsed = time.perf_counter() - req_start
            sleep_time = interval - elapsed
            if sleep_time > 0:
                time.sleep(sleep_time)

        conn.close()

        total_requests = sum(r["requests"] for r in results_per_second)
        total_accepted = sum(r["accepted"] for r in results_per_second)
        all_p99 = [r["p99_us"] for r in results_per_second if r["p99_us"] > 0]

        metrics = {
            "duration_sec": duration_sec,
            "target_rps": target_rps,
            "actual_rps": total_requests / duration_sec if duration_sec > 0 else 0,
            "total_requests": total_requests,
            "total_accepted": total_accepted,
            "avg_p99_us": statistics.mean(all_p99) if all_p99 else 0,
            "max_p99_us": max(all_p99) if all_p99 else 0,
            "per_second": results_per_second,
        }

        print(f"  Total: {total_requests} requests, Actual RPS: {metrics['actual_rps']:.0f}")

        return BenchmarkResult(
            test_name="sustained",
            timestamp=datetime.utcnow().isoformat() + "Z",
            config={"duration_sec": duration_sec, "target_rps": target_rps, "num_accounts": num_accounts},
            metrics=metrics
        )

    def bench_heavy(self, requests: int = 100) -> BenchmarkResult:
        """
        Heavy request測定
        - 大きなペイロードのリクエスト
        - 実務での大量パラメータ変更シミュレーション
        """
        print(f"[Heavy] Running {requests} heavy requests...")

        conn = self._make_connection()
        headers = self._make_headers()

        latencies_us = []
        accepted = 0
        rejected = 0

        sample_body = self._make_order_payload(heavy=True, client_order_id="heavy_sample")

        for i in range(requests):
            start = time.perf_counter()
            body = self._make_order_payload(heavy=True, client_order_id=f"heavy_{time.time_ns()}_{i}")
            conn.request("POST", "/orders", body=body, headers=headers)
            resp = conn.getresponse()
            resp.read()
            end = time.perf_counter()

            lat_us = (end - start) * 1_000_000
            latencies_us.append(lat_us)

            if resp.status == 202:
                accepted += 1
            else:
                rejected += 1

        conn.close()

        metrics = {
            "requests": requests,
            "payload_size": len(sample_body),
            "accepted": accepted,
            "rejected": rejected,
            "p50_us": percentile(latencies_us, 0.50),
            "p95_us": percentile(latencies_us, 0.95),
            "p99_us": percentile(latencies_us, 0.99),
            "max_us": max(latencies_us),
            "avg_us": statistics.mean(latencies_us),
        }

        print(f"  Payload: {len(body)} bytes, p50: {metrics['p50_us']:.0f}µs, p99: {metrics['p99_us']:.0f}µs")

        return BenchmarkResult(
            test_name="heavy",
            timestamp=datetime.utcnow().isoformat() + "Z",
            config={"requests": requests, "payload_size": len(body)},
            metrics=metrics,
            raw_latencies_us=latencies_us
        )


def save_results(results: List[BenchmarkResult], output_dir: str = "results"):
    """結果をJSONファイルに保存"""
    os.makedirs(output_dir, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filepath = os.path.join(output_dir, f"bench_{timestamp}.json")

    data = {
        "run_timestamp": datetime.utcnow().isoformat() + "Z",
        "results": [asdict(r) for r in results]
    }

    # raw_latencies_usは大きいのでサマリーのみ保存するオプション
    for r in data["results"]:
        if r.get("raw_latencies_us") and len(r["raw_latencies_us"]) > 1000:
            r["raw_latencies_us"] = f"[{len(r['raw_latencies_us'])} values omitted]"

    with open(filepath, "w") as f:
        json.dump(data, f, indent=2)

    print(f"\nResults saved to: {filepath}")
    return filepath


def print_summary(results: List[BenchmarkResult]):
    """サマリーを表示"""
    print("\n" + "=" * 60)
    print("BENCHMARK SUMMARY")
    print("=" * 60)

    for r in results:
        print(f"\n[{r.test_name.upper()}]")
        for k, v in r.metrics.items():
            if k == "per_second":
                continue
            if isinstance(v, float):
                print(f"  {k}: {v:.2f}")
            else:
                print(f"  {k}: {v}")


def check_assertions(results: List[BenchmarkResult], args) -> Tuple[bool, List[str]]:
    """
    アサーションチェック
    Returns: (all_passed, messages)
    """
    failures = []

    # RTT結果からレイテンシをチェック
    rtt_result = next((r for r in results if r.test_name == "rtt"), None)
    if rtt_result:
        metrics = rtt_result.metrics

        if args.assert_p50 and metrics.get("p50_us", 0) > args.assert_p50:
            failures.append(f"p50 {metrics['p50_us']:.0f}µs > {args.assert_p50}µs")

        if args.assert_p99 and metrics.get("p99_us", 0) > args.assert_p99:
            failures.append(f"p99 {metrics['p99_us']:.0f}µs > {args.assert_p99}µs")

        if args.assert_max and metrics.get("max_us", 0) > args.assert_max:
            failures.append(f"max {metrics['max_us']:.0f}µs > {args.assert_max}µs")

    # Throughput結果からスループットをチェック
    throughput_result = next((r for r in results if r.test_name == "throughput"), None)
    if throughput_result:
        metrics = throughput_result.metrics

        if args.assert_throughput and metrics.get("throughput_rps", 0) < args.assert_throughput:
            failures.append(f"throughput {metrics['throughput_rps']:.0f} req/s < {args.assert_throughput} req/s")

    return len(failures) == 0, failures


def main():
    parser = argparse.ArgumentParser(
        description="Gateway Performance Benchmark Tool",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__
    )
    parser.add_argument("test", nargs="?", default="all",
                        choices=["rtt", "throughput", "sustained", "heavy", "all"],
                        help="Test to run (default: all)")
    parser.add_argument("--host", default="localhost", help="Gateway host")
    parser.add_argument("--port", type=int, default=8081, help="Gateway port")
    parser.add_argument("--secret", default="secret123", help="JWT secret")
    parser.add_argument("--requests", type=int, default=1000, help="Number of requests (rtt/heavy)")
    parser.add_argument("--duration", type=int, default=10, help="Duration in seconds (throughput/sustained)")
    parser.add_argument("--concurrency", type=int, default=50, help="Concurrent workers (throughput)")
    parser.add_argument("--warmup-rtt", type=int, default=50, help="Warmup requests (rtt)")
    parser.add_argument("--warmup-throughput-sec", type=int, default=0, help="Warmup seconds (throughput)")
    parser.add_argument("--rps", type=int, default=1000, help="Target RPS (sustained)")
    parser.add_argument("--accounts", type=int, default=10, help="Number of accounts")
    parser.add_argument("--output", default="results", help="Output directory")
    parser.add_argument("--no-save", action="store_true", help="Don't save results to file")
    parser.add_argument("--disable-jwt-reuse", action="store_true",
                        help="Generate JWT per request instead of reusing")

    # Assertion options (品質ゲート)
    parser.add_argument("--assert-p50", type=float, metavar="US",
                        help="Assert p50 latency <= US microseconds")
    parser.add_argument("--assert-p99", type=float, metavar="US",
                        help="Assert p99 latency <= US microseconds")
    parser.add_argument("--assert-max", type=float, metavar="US",
                        help="Assert max latency <= US microseconds")
    parser.add_argument("--assert-throughput", type=float, metavar="RPS",
                        help="Assert throughput >= RPS requests/sec")

    args = parser.parse_args()

    bench = GatewayBenchmark(
        host=args.host,
        port=args.port,
        jwt_secret=args.secret,
        reuse_token=not args.disable_jwt_reuse,
    )

    # ヘルスチェック
    if not bench.check_health():
        print("ERROR: Gateway is not responding. Is it running?", file=sys.stderr)
        sys.exit(1)

    print(f"Gateway: {args.host}:{args.port}")
    print("-" * 40)

    results = []

    if args.test in ("rtt", "all"):
        results.append(bench.bench_rtt(requests=args.requests, warmup=args.warmup_rtt))

    if args.test in ("throughput", "all"):
        results.append(bench.bench_throughput(
            duration_sec=args.duration,
            concurrency=args.concurrency,
            num_accounts=args.accounts,
            warmup_sec=args.warmup_throughput_sec,
        ))

    if args.test in ("sustained", "all"):
        results.append(bench.bench_sustained(
            duration_sec=args.duration,
            target_rps=args.rps,
            num_accounts=args.accounts
        ))

    if args.test in ("heavy", "all"):
        results.append(bench.bench_heavy(requests=min(args.requests, 200)))

    print_summary(results)

    if not args.no_save:
        save_results(results, args.output)

    # Assertion checks
    has_assertions = any([args.assert_p50, args.assert_p99, args.assert_max, args.assert_throughput])
    if has_assertions:
        passed, failures = check_assertions(results, args)
        print("\n" + "=" * 60)
        print("ASSERTION RESULTS")
        print("=" * 60)
        if passed:
            print("\033[92m✓ All assertions passed\033[0m")
        else:
            print("\033[91m✗ Assertion failures:\033[0m")
            for f in failures:
                print(f"  - {f}")
            sys.exit(1)


if __name__ == "__main__":
    main()

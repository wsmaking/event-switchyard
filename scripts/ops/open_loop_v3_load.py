#!/usr/bin/env python3
"""
Open-loop load generator for /v3/orders.

Purpose:
- Keep offered RPS fixed as much as possible (independent from response speed).
- Measure client-side completion/acceptance/latency stats.
"""

from __future__ import annotations

import argparse
import base64
import hashlib
import hmac
import http.client
import json
import queue
import threading
import time
from collections import Counter
from dataclasses import dataclass, field


def b64url(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).rstrip(b"=").decode()


def generate_jwt(secret: str, account_id: str) -> str:
    now = int(time.time())
    header = {"alg": "HS256", "typ": "JWT"}
    payload = {
        "sub": account_id,
        "accountId": account_id,
        "iat": now,
        "exp": now + 86400,
    }
    h = b64url(json.dumps(header, separators=(",", ":")).encode())
    p = b64url(json.dumps(payload, separators=(",", ":")).encode())
    msg = f"{h}.{p}"
    sig = hmac.new(secret.encode(), msg.encode(), hashlib.sha256).digest()
    return f"{msg}.{b64url(sig)}"


def percentile(values: list[float], p: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    idx = int(round((p / 100.0) * (len(ordered) - 1)))
    return ordered[idx]


@dataclass
class WorkerStats:
    attempted: int = 0
    completed: int = 0
    accepted: int = 0
    rejected: int = 0
    errors: int = 0
    latencies_us: list[float] = field(default_factory=list)
    accepted_latencies_us: list[float] = field(default_factory=list)
    status_counts: Counter = field(default_factory=Counter)


def worker_loop(
    worker_id: int,
    host: str,
    port: int,
    path: str,
    request_timeout_sec: float,
    account_ids: list[str],
    auth_headers: dict[str, str],
    q: queue.Queue,
    scheduler_done: threading.Event,
    stop_now: threading.Event,
    stats: WorkerStats,
) -> None:
    conn: http.client.HTTPConnection | None = None
    body_template = (
        '{"symbol":"AAPL","side":"BUY","type":"LIMIT","qty":100,'
        '"price":15000,"timeInForce":"GTC","clientOrderId":"%s"}'
    )

    while True:
        if stop_now.is_set():
            break
        try:
            seq, account_idx = q.get(timeout=0.05)
        except queue.Empty:
            if scheduler_done.is_set():
                break
            continue

        account_id = account_ids[account_idx]
        key = f"ol_{worker_id}_{account_id}_{seq}"
        body = body_template % key
        headers = {
            "Content-Type": "application/json",
            "Authorization": auth_headers[account_id],
            "Idempotency-Key": key,
        }

        stats.attempted += 1
        t0 = time.perf_counter()
        try:
            if conn is None:
                conn = http.client.HTTPConnection(host, port, timeout=request_timeout_sec)
            conn.request("POST", path, body=body, headers=headers)
            resp = conn.getresponse()
            resp.read()
            elapsed_us = (time.perf_counter() - t0) * 1_000_000.0
            stats.latencies_us.append(elapsed_us)
            stats.completed += 1
            stats.status_counts[resp.status] += 1
            if resp.status == 202:
                stats.accepted += 1
                stats.accepted_latencies_us.append(elapsed_us)
            else:
                stats.rejected += 1
        except Exception:
            stats.errors += 1
            if conn is not None:
                try:
                    conn.close()
                except Exception:
                    pass
                conn = None
        finally:
            q.task_done()

    if conn is not None:
        try:
            conn.close()
        except Exception:
            pass


def main() -> None:
    parser = argparse.ArgumentParser(description="Open-loop /v3 load generator")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=29001)
    parser.add_argument("--path", default="/v3/orders")
    parser.add_argument("--duration-sec", type=float, default=30.0)
    parser.add_argument("--target-rps", type=float, default=10_000.0)
    parser.add_argument("--workers", type=int, default=64)
    parser.add_argument("--accounts", type=int, default=16)
    parser.add_argument("--account-prefix", default="openloop")
    parser.add_argument("--jwt-secret", default="secret123")
    parser.add_argument("--queue-capacity", type=int, default=20_000)
    parser.add_argument("--request-timeout-sec", type=float, default=2.0)
    parser.add_argument("--drain-timeout-sec", type=float, default=5.0)
    args = parser.parse_args()

    if args.duration_sec <= 0:
        raise SystemExit("--duration-sec must be > 0")
    if args.target_rps <= 0:
        raise SystemExit("--target-rps must be > 0")
    if args.workers <= 0:
        raise SystemExit("--workers must be > 0")
    if args.accounts <= 0:
        raise SystemExit("--accounts must be > 0")
    if args.queue_capacity <= 0:
        raise SystemExit("--queue-capacity must be > 0")
    if args.request_timeout_sec <= 0:
        raise SystemExit("--request-timeout-sec must be > 0")
    if args.drain_timeout_sec < 0:
        raise SystemExit("--drain-timeout-sec must be >= 0")

    account_ids = [f"{args.account_prefix}_{i+1}" for i in range(args.accounts)]
    auth_headers = {
        account_id: f"Bearer {generate_jwt(args.jwt_secret, account_id)}"
        for account_id in account_ids
    }

    offered_total_target = int(round(args.duration_sec * args.target_rps))
    q: queue.Queue = queue.Queue(maxsize=args.queue_capacity)
    scheduler_done = threading.Event()
    stop_now = threading.Event()

    worker_stats = [WorkerStats() for _ in range(args.workers)]
    threads: list[threading.Thread] = []
    for wid in range(args.workers):
        t = threading.Thread(
            target=worker_loop,
            args=(
                wid,
                args.host,
                args.port,
                args.path,
                args.request_timeout_sec,
                account_ids,
                auth_headers,
                q,
                scheduler_done,
                stop_now,
                worker_stats[wid],
            ),
            daemon=True,
        )
        threads.append(t)
        t.start()

    offered_total = 0
    enqueued_total = 0
    dropped_before_send_total = 0
    account_idx = 0
    sched_start = time.perf_counter()
    sched_end = sched_start + args.duration_sec

    while True:
        now = time.perf_counter()
        if now >= sched_end:
            break
        should_offer = int((now - sched_start) * args.target_rps)
        while offered_total < should_offer and offered_total < offered_total_target:
            item = (offered_total, account_idx)
            account_idx = (account_idx + 1) % args.accounts
            try:
                q.put_nowait(item)
                enqueued_total += 1
            except queue.Full:
                dropped_before_send_total += 1
            offered_total += 1
        sleep_sec = min(0.001, max(0.0, sched_end - now))
        if sleep_sec > 0:
            time.sleep(sleep_sec)

    while offered_total < offered_total_target:
        item = (offered_total, account_idx)
        account_idx = (account_idx + 1) % args.accounts
        try:
            q.put_nowait(item)
            enqueued_total += 1
        except queue.Full:
            dropped_before_send_total += 1
        offered_total += 1

    scheduler_done.set()

    drain_deadline = time.perf_counter() + args.drain_timeout_sec
    while time.perf_counter() < drain_deadline:
        if q.empty():
            break
        time.sleep(0.01)

    stop_now.set()
    for t in threads:
        t.join(timeout=2.0)

    queue_remaining = q.qsize()

    attempted_total = sum(s.attempted for s in worker_stats)
    completed_total = sum(s.completed for s in worker_stats)
    accepted_total = sum(s.accepted for s in worker_stats)
    rejected_total = sum(s.rejected for s in worker_stats)
    error_total = sum(s.errors for s in worker_stats)
    status_counts: Counter = Counter()
    latencies_us: list[float] = []
    accepted_latencies_us: list[float] = []
    for s in worker_stats:
        status_counts.update(s.status_counts)
        latencies_us.extend(s.latencies_us)
        accepted_latencies_us.extend(s.accepted_latencies_us)

    accepted_rate = (accepted_total / completed_total) if completed_total > 0 else 0.0
    offered_rps = offered_total / args.duration_sec
    enqueued_rps = enqueued_total / args.duration_sec
    completed_rps = completed_total / args.duration_sec
    accepted_rps = accepted_total / args.duration_sec
    rejected_rps = rejected_total / args.duration_sec
    dropped_offer_ratio = (
        dropped_before_send_total / offered_total if offered_total > 0 else 0.0
    )
    unsent_total = max(0, enqueued_total - attempted_total)

    print("open_loop_v3_load")
    print(f"host={args.host}:{args.port}")
    print(f"path={args.path}")
    print(f"duration_sec={args.duration_sec:.3f}")
    print(f"target_rps={args.target_rps:.3f}")
    print(f"workers={args.workers}")
    print(f"accounts={args.accounts}")
    print(f"queue_capacity={args.queue_capacity}")
    print(f"offered_total={offered_total}")
    print(f"offered_rps={offered_rps:.3f}")
    print(f"enqueued_total={enqueued_total}")
    print(f"enqueued_rps={enqueued_rps:.3f}")
    print(f"dropped_before_send_total={dropped_before_send_total}")
    print(f"dropped_offer_ratio={dropped_offer_ratio:.6f}")
    print(f"queue_remaining={queue_remaining}")
    print(f"unsent_total={unsent_total}")
    print(f"attempted_total={attempted_total}")
    print(f"completed_total={completed_total}")
    print(f"completed_rps={completed_rps:.3f}")
    print(f"accepted_total={accepted_total}")
    print(f"rejected_total={rejected_total}")
    print(f"error_total={error_total}")
    print(f"accepted_rate={accepted_rate:.6f}")
    print(f"accepted_rps={accepted_rps:.3f}")
    print(f"rejected_rps={rejected_rps:.3f}")
    print(f"client_ack_p50_us={percentile(latencies_us, 50.0):.3f}")
    print(f"client_ack_p99_us={percentile(latencies_us, 99.0):.3f}")
    print(f"client_ack_accepted_p50_us={percentile(accepted_latencies_us, 50.0):.3f}")
    print(f"client_ack_accepted_p99_us={percentile(accepted_latencies_us, 99.0):.3f}")
    for status, count in sorted(status_counts.items()):
        print(f"status_{status}_total={count}")


if __name__ == "__main__":
    main()

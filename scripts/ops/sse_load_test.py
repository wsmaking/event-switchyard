#!/usr/bin/env python3
"""
SSE Load Test (Gateway)

Usage:
  python3 scripts/ops/sse_load_test.py \
    --url http://localhost:8080/stream \
    --connections 50 \
    --duration 30 \
    --jwt-secret dev-secret \
    --account-id acct_demo
"""
import argparse
import base64
import hashlib
import hmac
import http.client
import json
import threading
import time
from urllib.parse import urlparse


def b64url(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).decode().rstrip("=")


def generate_jwt(secret: str, account_id: str) -> str:
    now = int(time.time())
    header = {"alg": "HS256", "typ": "JWT"}
    payload = {"accountId": account_id, "exp": now + 3600}
    msg = f"{b64url(json.dumps(header, separators=(',', ':')).encode())}.{b64url(json.dumps(payload, separators=(',', ':')).encode())}"
    sig = hmac.new(secret.encode(), msg.encode(), hashlib.sha256).digest()
    return msg + "." + b64url(sig)


def worker(idx: int, url: str, token: str, duration: float, results: dict, lock: threading.Lock) -> None:
    parsed = urlparse(url)
    host = parsed.hostname or "localhost"
    port = parsed.port or (443 if parsed.scheme == "https" else 80)
    path = parsed.path or "/stream"
    if parsed.query:
        path = f"{path}?{parsed.query}"

    events = 0
    errors = 0
    timeout_sec = max(20, int(duration) + 5)
    try:
        conn = http.client.HTTPConnection(host, port, timeout=timeout_sec)
        headers = {
            "Accept": "text/event-stream",
            "Authorization": f"Bearer {token}",
        }
        conn.request("GET", path, headers=headers)
        resp = conn.getresponse()
        if resp.status != 200:
            errors += 1
            conn.close()
        else:
            end = time.time() + duration
            while time.time() < end:
                try:
                    line = resp.readline()
                except Exception:
                    errors += 1
                    break
                if not line:
                    time.sleep(0.05)
                    continue
                if line.startswith(b"data:"):
                    events += 1
            conn.close()
    except Exception:
        errors += 1

    with lock:
        results["events"] += events
        results["errors"] += errors


def main() -> None:
    parser = argparse.ArgumentParser(description="SSE load test")
    parser.add_argument("--url", default="http://localhost:8080/stream")
    parser.add_argument("--connections", type=int, default=50)
    parser.add_argument("--duration", type=int, default=30)
    parser.add_argument("--jwt-secret", default="")
    parser.add_argument("--account-id", default="acct_demo")
    parser.add_argument("--jwt", default="")
    args = parser.parse_args()

    if args.jwt:
        token = args.jwt
    elif args.jwt_secret:
        token = generate_jwt(args.jwt_secret, args.account_id)
    else:
        raise SystemExit("Provide --jwt or --jwt-secret")

    results = {"events": 0, "errors": 0}
    lock = threading.Lock()
    threads = [
        threading.Thread(
            target=worker,
            args=(i, args.url, token, args.duration, results, lock),
            daemon=True,
        )
        for i in range(args.connections)
    ]

    start = time.time()
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    elapsed = time.time() - start

    print(f"SSE load done: connections={args.connections} duration={args.duration}s elapsed={elapsed:.1f}s")
    print(f"events={results['events']} errors={results['errors']}")


if __name__ == "__main__":
    main()

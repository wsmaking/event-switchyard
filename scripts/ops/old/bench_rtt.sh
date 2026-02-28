#!/usr/bin/env bash
set -euo pipefail

MODE=${1:-sim}
N=${2:-1000}
P=${3:-50}
WARMUP=${4:-100}
URL=${URL:-http://localhost:8081/orders}
JWT_TOKEN=${JWT_TOKEN:-dev-token}
CLIENT_ID_LEN=${CLIENT_ID_LEN:-0}
KEEPALIVE=${KEEPALIVE:-on}

cat <<EOF
RTT bench (client-side)
  mode=${MODE}
  url=${URL}
  n=${N} parallel=${P} warmup=${WARMUP}
  clientIdLen=${CLIENT_ID_LEN}
  keepAlive=${KEEPALIVE}
EOF

python - <<'PY'
import concurrent.futures, json, os, time, urllib.parse, http.client, threading

url = os.environ.get('URL', 'http://localhost:8081/orders')
token = os.environ.get('JWT_TOKEN', 'dev-token')
n = int(os.environ.get('N', '1000'))
p = int(os.environ.get('P', '50'))
warmup = int(os.environ.get('WARMUP', '100'))
mode = os.environ.get('MODE', 'sim')
client_id_len = int(os.environ.get('CLIENT_ID_LEN', '0'))
keepalive = os.environ.get('KEEPALIVE', 'on').lower() == 'on'

payload = {
    "symbol": "BTCUSD",
    "side": "BUY",
    "type": "LIMIT",
    "quantity": 1,
    "price": 100.0,
}

if client_id_len > 0:
    payload["clientOrderId"] = "X" * client_id_len

body = json.dumps(payload).encode('utf-8')
headers_base = {
    "Content-Type": "application/json",
    "Authorization": f"Bearer {token}",
}
if not keepalive:
    headers_base["Connection"] = "close"

parsed = urllib.parse.urlparse(url)
host = parsed.hostname or "localhost"
port = parsed.port or (443 if parsed.scheme == "https" else 80)
path = parsed.path or "/"
if parsed.query:
    path = f"{path}?{parsed.query}"

local = threading.local()

def get_conn():
    conn = getattr(local, "conn", None)
    if conn is None or not keepalive:
        conn = http.client.HTTPConnection(host, port, timeout=5)
        local.conn = conn
    return conn

def one():
    conn = get_conn()
    start = time.perf_counter()
    try:
        conn.request("POST", path, body=body, headers=headers_base)
        resp = conn.getresponse()
        resp.read()
        if not keepalive:
            conn.close()
    except Exception:
        try:
            conn.close()
        except Exception:
            pass
        local.conn = None
    end = time.perf_counter()
    return (end - start) * 1000.0

if warmup > 0:
    with concurrent.futures.ThreadPoolExecutor(max_workers=p) as ex:
        list(ex.map(lambda _: one(), range(warmup)))

latencies = []
with concurrent.futures.ThreadPoolExecutor(max_workers=p) as ex:
    for lat in ex.map(lambda _: one(), range(n)):
        latencies.append(lat)

latencies.sort()
def pct(pp):
    if not latencies:
        return None
    k = int(round(pp * (len(latencies)-1)))
    return latencies[k]

out = {
    "mode": mode,
    "clientIdLen": client_id_len,
    "keepAlive": keepalive,
    "n": len(latencies),
    "p50_ms": pct(0.50),
    "p95_ms": pct(0.95),
    "p99_ms": pct(0.99),
    "max_ms": max(latencies) if latencies else None,
}
print(json.dumps(out, indent=2))
PY

#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/../.." && pwd)"
cd "$ROOT_DIR"

OUT="var/results/durability_tuning_scan_$(date +%Y%m%d_%H%M%S).csv"
mkdir -p var/results
CASES="${CASES:-baseline_w200_b64,tuned_w150_b64,tuned_w100_b64,tuned_w200_b128,tuned_w100_b128,stress_w5000_b1}"

cat > "$OUT" <<'CSV'
case,wait_us,batch,cap,concurrency,throughput_rps,total,accepted,rejected,acc_p99_us,all_p99_us,ack_p99_us,wal_enqueue_p99_us,fdatasync_p99_us,durable_ack_p99_us,durable_notify_p99_us,wal_age_ms,queue_full_total,dominant_interval_p99
CSV

run_case() {
  local NAME="$1" WAIT="$2" BATCH="$3" CAP="$4" CONC="${5:-180}" PORT=29001

  BACKPRESSURE_INFLIGHT_DYNAMIC=1 \
  BACKPRESSURE_INFLIGHT_INITIAL=300 \
  BACKPRESSURE_INFLIGHT_MIN=64 \
  BACKPRESSURE_INFLIGHT_CAP="$CAP" \
  BACKPRESSURE_INFLIGHT_ALPHA=0.8 \
  BACKPRESSURE_INFLIGHT_TICK_MS=200 \
  BACKPRESSURE_INFLIGHT_SLEW_RATIO=0.25 \
  BACKPRESSURE_INFLIGHT_TARGET_WAL_AGE_SEC=1.0 \
  FASTPATH_DRAIN_ENABLE=1 \
  FASTPATH_DRAIN_WORKERS=4 \
  AUDIT_ASYNC_WAL=1 \
  AUDIT_FDATASYNC=1 \
  AUDIT_FDATASYNC_ADAPTIVE="${AUDIT_FDATASYNC_ADAPTIVE:-0}" \
  AUDIT_FDATASYNC_ADAPTIVE_MIN_WAIT_US="${AUDIT_FDATASYNC_ADAPTIVE_MIN_WAIT_US:-50}" \
  AUDIT_FDATASYNC_ADAPTIVE_MAX_WAIT_US="${AUDIT_FDATASYNC_ADAPTIVE_MAX_WAIT_US:-${WAIT}}" \
  AUDIT_FDATASYNC_ADAPTIVE_MIN_BATCH="${AUDIT_FDATASYNC_ADAPTIVE_MIN_BATCH:-16}" \
  AUDIT_FDATASYNC_ADAPTIVE_MAX_BATCH="${AUDIT_FDATASYNC_ADAPTIVE_MAX_BATCH:-${BATCH}}" \
  AUDIT_FDATASYNC_ADAPTIVE_TARGET_SYNC_US="${AUDIT_FDATASYNC_ADAPTIVE_TARGET_SYNC_US:-4000}" \
  AUDIT_FDATASYNC_MAX_WAIT_US="$WAIT" \
  AUDIT_FDATASYNC_MAX_BATCH="$BATCH" \
  KAFKA_ENABLE=0 \
  JWT_HS256_SECRET=secret123 \
  GATEWAY_PORT="$PORT" \
  GATEWAY_TCP_PORT=0 \
  ./gateway-rust/target/debug/gateway-rust > /tmp/dur_tuning_${NAME}.log 2>&1 &
  PID=$!
  cleanup(){ kill $PID >/dev/null 2>&1 || true; wait $PID >/dev/null 2>&1 || true; }

  for i in $(seq 1 80); do
    if curl -sf http://localhost:$PORT/health >/dev/null; then break; fi
    sleep 0.25
  done

  RES=$(python3 - "$CONC" <<'PY'
import concurrent.futures, threading, time, json, http.client, base64, hmac, hashlib, sys
HOST='localhost'; PORT=29001; DURATION=10; CONC=int(sys.argv[1]); ACC=32

def jwt(account_id):
    now=int(time.time())
    h={"alg":"HS256","typ":"JWT"}
    p={"sub":f"u_{account_id}","account_id":str(account_id),"iat":now,"exp":now+86400}
    def b64(d): return base64.urlsafe_b64encode(json.dumps(d).encode()).rstrip(b'=').decode()
    hb,pb=b64(h),b64(p)
    sig=base64.urlsafe_b64encode(hmac.new(b"secret123",f"{hb}.{pb}".encode(),hashlib.sha256).digest()).rstrip(b'=').decode()
    return f"{hb}.{pb}.{sig}"

def pct(vals,p):
    if not vals:return 0.0
    v=sorted(vals); return v[int(round((len(v)-1)*p))]

headers_by={}
for i in range(ACC):
    aid=str(10000+i)
    headers_by[aid]={"Content-Type":"application/json","Authorization":"Bearer "+jwt(aid)}
acc_ids=list(headers_by.keys())

for i in range(150):
    aid=acc_ids[i%ACC]
    c=http.client.HTTPConnection(HOST,PORT,timeout=10)
    body=json.dumps({"symbol":"AAPL","side":"BUY","type":"LIMIT","qty":100,"price":15000,"timeInForce":"GTC","clientOrderId":f"w_{time.time_ns()}_{i}"})
    c.request('POST','/orders',body=body,headers=headers_by[aid]); r=c.getresponse(); r.read(); c.close()

lock=threading.Lock(); stop=threading.Event(); start=threading.Event()
res={"accepted":0,"rejected":0,"errors":0}; lat_all=[]; lat_acc=[]

def worker(wid):
    c=http.client.HTTPConnection(HOST,PORT,timeout=10)
    aid=acc_ids[wid%ACC]; h=headers_by[aid]; n=0
    la=[]; lac=[]; a=rj=e=0
    while not stop.is_set():
        try:
            t0=time.perf_counter()
            body=json.dumps({"symbol":"AAPL","side":"BUY","type":"LIMIT","qty":100,"price":15000,"timeInForce":"GTC","clientOrderId":f"d_{aid}_{wid}_{n}"})
            n+=1
            c.request('POST','/orders',body=body,headers=h)
            r=c.getresponse(); r.read(); dt=(time.perf_counter()-t0)*1_000_000
            if start.is_set():
                la.append(dt)
                if r.status==202:
                    a+=1; lac.append(dt)
                else:
                    rj+=1
        except Exception:
            if start.is_set(): e+=1
            try:c.close()
            except:pass
            c=http.client.HTTPConnection(HOST,PORT,timeout=10)
    with lock:
        res['accepted']+=a; res['rejected']+=rj; res['errors']+=e; lat_all.extend(la); lat_acc.extend(lac)
    try:c.close()
    except:pass

with concurrent.futures.ThreadPoolExecutor(max_workers=CONC) as ex:
    fs=[ex.submit(worker,i) for i in range(CONC)]
    start.set(); t0=time.time(); time.sleep(DURATION); stop.set(); concurrent.futures.wait(fs)

elapsed=time.time()-t0
total=res['accepted']+res['rejected']+res['errors']
print(json.dumps({
    'throughput_rps': total/elapsed if elapsed>0 else 0,
    'total': total,
    'accepted': res['accepted'],
    'rejected': res['rejected'],
    'all_p99_us': pct(lat_all,0.99),
    'acc_p99_us': pct(lat_acc,0.99),
}))
PY
)

  MET=$(curl -sf http://localhost:$PORT/metrics)
  v(){ printf "%s\n" "$MET" | awk -v k="$1" '$1==k{print $2}' | tail -n1; }

  python3 - "$OUT" "$NAME" "$WAIT" "$BATCH" "$CAP" "$CONC" "$RES" "$(v gateway_ack_p99_us)" "$(v gateway_wal_enqueue_p99_us)" "$(v gateway_fdatasync_p99_us)" "$(v gateway_durable_ack_p99_us)" "$(v gateway_durable_notify_p99_us)" "$(v gateway_wal_age_ms)" "$(v gateway_reject_queue_full_total)" <<'PY'
import csv,json,sys
out,name,wait,batch,cap,conc,res,ack,wenq,fd,dack,dnotify,wal,qf=sys.argv[1:]
r=json.loads(res)
parts = {
  "enqueue": float(wenq or 0),
  "fdatasync": float(fd or 0),
  "notify": float(dnotify or 0),
}
dominant = max(parts, key=parts.get)
with open(out,'a',newline='') as f:
  csv.writer(f).writerow([name,wait,batch,cap,conc,round(r['throughput_rps'],2),r['total'],r['accepted'],r['rejected'],round(r['acc_p99_us'],2),round(r['all_p99_us'],2),ack,wenq,fd,dack,dnotify,wal,qf,dominant])
PY

  cleanup
}

case_enabled() {
  [[ ",${CASES}," == *",$1,"* ]]
}

case_enabled baseline_w200_b64 && run_case baseline_w200_b64 200 64 1024 180
case_enabled tuned_w150_b64 && run_case tuned_w150_b64 150 64 1024 180
case_enabled tuned_w100_b64 && run_case tuned_w100_b64 100 64 1024 180
case_enabled tuned_w200_b128 && run_case tuned_w200_b128 200 128 1024 180
case_enabled tuned_w100_b128 && run_case tuned_w100_b128 100 128 1024 180
case_enabled stress_w5000_b1 && run_case stress_w5000_b1 5000 1 1024 180

column -s, -t "$OUT"
echo "OUT=$OUT"

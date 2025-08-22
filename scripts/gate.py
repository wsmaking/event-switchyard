#!/usr/bin/env python3
import argparse, json, re, sys, textwrap
from pathlib import Path

REQ_LAT = re.compile(r"REQ-PERF-001.*?p99\s*[≤≦]\s*(\d+)\s*µs", re.I)
REQ_GC  = re.compile(r"REQ-PERF-002.*?p99\s*[≤≦]\s*([0-9]+(?:\.[0-9]+)?)\s*ms", re.I)

def jload(p): return json.loads(Path(p).read_text(encoding="utf-8"))

def parse_req(md):
    t = Path(md).read_text(encoding="utf-8")
    m1, m2 = REQ_LAT.search(t), REQ_GC.search(t)
    if not m1: sys.exit(f"[gate] REQ-PERF-001 not found in {md}")
    lat_us = int(m1.group(1))
    gc_ms  = float(m2.group(1)) if m2 else None
    return lat_us, gc_ms

def pick(j, path):
    cur = j
    for k in path:
        if k not in cur: return None
        cur = cur[k]
    return cur

def ok(b): return "OK" if b else "NG"
def fmt(val, unit): return f"{val}{unit}" if val is not None else "N/A"  # ← 追加

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--bench", required=True)
    ap.add_argument("--req", required=True)
    ap.add_argument("--baseline", required=True)
    args = ap.parse_args()

    bench = jload(args.bench)
    base  = jload(args.baseline)
    lat_req_us, gc_req_ms = parse_req(args.req)

    b_lat = pick(bench, ["metrics","latency_us","p99"])
    a_lat = pick(base,  ["metrics","latency_us","p99"])
    b_gc  = pick(bench, ["metrics","gc_pause_ms","p99"])
    a_gc  = pick(base,  ["metrics","gc_pause_ms","p99"])

    errs = []

    lat_ok = (b_lat is not None) and (b_lat <= lat_req_us)
    if not lat_ok: errs.append(f"latency_us.p99={b_lat}us > {lat_req_us}us (REQ-PERF-001)")

    gc_ok = True
    if b_gc is not None and gc_req_ms is not None:
        gc_ok = b_gc <= gc_req_ms
        if not gc_ok: errs.append(f"gc_pause_ms.p99={b_gc}ms > {gc_req_ms}ms (REQ-PERF-002)")

    base_ok = True
    if a_lat is not None and b_lat is not None:
        thr = a_lat * 1.05
        base_ok = b_lat <= thr
        if not base_ok: errs.append(f"latency_us.p99={b_lat}us > baseline*1.05={thr:.1f}us (base={a_lat}us)")

    # —— ここから置き換え後の出力ブロック ——
    req_gc_str = fmt(gc_req_ms,'ms') if b_gc is not None else 'N/A'  # 追加
    print(textwrap.dedent(f"""
    gate summary:
    case={bench.get('case')} env={bench.get('env')}
    p99={b_lat}us  (REQ {lat_req_us}us)    -> {ok(lat_ok)}
    gc_p99={fmt(b_gc,'ms')} (REQ {req_gc_str})    -> {ok(gc_ok)}
    vs baseline: {a_lat}us → {b_lat}us (≤ +5%) -> {ok(base_ok)}
    """).strip())
    # —— ここまで ——

    if not (lat_ok and gc_ok and base_ok):
        print("\nFAIL reasons:")
        for e in errs: print(" - " + e)
        return 1
    return 0

if __name__ == "__main__":
    raise SystemExit(main())

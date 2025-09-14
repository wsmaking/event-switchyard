import argparse, json, re, sys, textwrap
from pathlib import Path

# ---- SLO 抽出（perf.md からしきい値を拾う） ----
REQ_LAT = re.compile(r"REQ-PERF-001.*?p99\s*[≤≦]\s*(\d+)\s*µs", re.I)
REQ_GC  = re.compile(r"REQ-PERF-002.*?p99\s*[≤≦]\s*([0-9]+(?:\.[0-9]+)?)\s*ms", re.I)

def is_smoke(case: str) -> bool:
    """blackbox_* は smoke（SLO/Baseline 比較の対象外）"""
    return str(case or "").startswith("blackbox_")

def jload(p): return json.loads(Path(p).read_text(encoding="utf-8"))

def parse_req(md):
    t = Path(md).read_text(encoding="utf-8")
    m1, m2 = REQ_LAT.search(t), REQ_GC.search(t)
    if not m1:
        sys.exit(f"[gate] REQ-PERF-001 not found in {md}")
    lat_us = int(m1.group(1))
    gc_ms  = float(m2.group(1)) if m2 else None
    return lat_us, gc_ms

def pick(j, path):
    cur = j
    for k in path:
        if k not in cur: return None
        cur = cur[k]
    return cur

def fmt(val, unit): return f"{val}{unit}" if val is not None else "N/A"

def status(ok: bool, skip: bool) -> str:
    return "SKIP" if skip else ("OK" if ok else "NG")

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--bench", required=True)
    ap.add_argument("--req", required=True)
    ap.add_argument("--baseline", required=True)
    args = ap.parse_args()

    bench = jload(args.bench)
    base  = jload(args.baseline)
    lat_req_us, gc_req_ms = parse_req(args.req)

    case = bench.get("case")
    env  = bench.get("env")

    b_lat = pick(bench, ["metrics","latency_us","p99"])
    a_lat = pick(base,  ["metrics","latency_us","p99"])
    b_gc  = pick(bench, ["metrics","gc_pause_ms","p99"])

    errs, notes = [], []

    # ---- smoke 判定（blackbox_* は SLO/Baseline を SKIP）----
    slo_applies = not is_smoke(case)

    # ---- baseline 一致性（case/env が違えば比較を SKIP）----
    case_mismatch = (base.get("case") != case or base.get("env") != env)
    if case_mismatch:
        print(f"[gate] WARN: baseline mismatch: base({base.get('case')},{base.get('env')}) "
              f"vs bench({case},{env})")

    # ---- Latency SLO（REQ-PERF-001）----
    if b_lat is None:
        lat_ok, lat_skip = False, False
        errs.append("latency_us.p99 is missing")
    elif slo_applies:
        lat_ok  = (b_lat <= lat_req_us)
        lat_skip = False
        if not lat_ok:
            errs.append(f"latency_us.p99={b_lat}us > {lat_req_us}us (REQ-PERF-001)")
    else:
        lat_ok, lat_skip = True, True
        notes.append("latency_us.p99: SKIP (blackbox smoke)")

    # ---- Baseline 比較（同一 case/env かつ smoke でない時のみ）----
    if (not case_mismatch) and (a_lat is not None) and (b_lat is not None) and slo_applies:
        thr = a_lat * 1.05
        base_ok, base_skip = (b_lat <= thr), False
        if not base_ok:
            errs.append(f"latency_us.p99={b_lat}us > baseline*1.05={thr:.1f}us (base={a_lat}us)")
    else:
        base_ok, base_skip = True, True  # 比較しない = SKIP (かつ FAIL にはしない)

    # ---- GC SLO（REQ-PERF-002）----
    if b_gc is not None and gc_req_ms is not None:
        gc_ok, gc_skip = (b_gc <= gc_req_ms), False
        if not gc_ok:
            errs.append(f"gc_pause_ms.p99={b_gc}ms > {gc_req_ms}ms (REQ-PERF-002)")
    else:
        gc_ok, gc_skip = True, True  # 指標またはREQが無い -> SKIP（FAILにはしない）

    # ---- Summary 出力 ----
    req_gc_str = fmt(gc_req_ms, 'ms') if gc_req_ms is not None else 'N/A'
    summary = textwrap.dedent(f"""
    gate summary:
    case={case} env={env}
    p99={fmt(b_lat,'us')}  (REQ {lat_req_us}us)    -> {status(lat_ok, lat_skip)}
    gc_p99={fmt(b_gc,'ms')} (REQ {req_gc_str})    -> {status(gc_ok, gc_skip)}
    vs baseline: {fmt(a_lat,'us')} → {fmt(b_lat,'us')} (≤ +5%) -> { 'SKIP' if base_skip else ('OK' if base_ok else 'NG') }
    """).strip()
    print(summary)
    for n in notes:
        print(n)

    if not (lat_ok and gc_ok and base_ok):
        print("\nFAIL reasons:")
        for e in errs:
            print(" - " + e)
        return 1
    return 0

if __name__ == "__main__":
    raise SystemExit(main())

import argparse, json, re, sys, textwrap
from pathlib import Path

# SLO基準値を抽出する正規表現パターン
REQ_LAT = re.compile(r"REQ-PERF-001.*?p99\s*[≤≦]\s*(\d+)\s*µs", re.I)
REQ_GC  = re.compile(r"REQ-PERF-002.*?p99\s*[≤≦]\s*([0-9]+(?:\.[0-9]+)?)\s*ms", re.I)

def is_smoke(case: str) -> bool:
    """blackbox_* ケースはスモークテスト（性能チェック対象外）"""
    return str(case or "").startswith("blackbox_")

def jload(p): 
    """JSONファイルを読み込み"""
    return json.loads(Path(p).read_text(encoding="utf-8"))

def parse_req(md):
    """要件ドキュメントからSLO閾値を抽出"""
    t = Path(md).read_text(encoding="utf-8")
    m1, m2 = REQ_LAT.search(t), REQ_GC.search(t)
    if not m1:
        sys.exit(f"[gate] REQ-PERF-001 not found in {md}")
    lat_us = int(m1.group(1))
    gc_ms  = float(m2.group(1)) if m2 else None
    return lat_us, gc_ms

def pick(j, path):
    """JSONから指定パスの値を取得（存在しなければNone）"""
    cur = j
    for k in path:
        if k not in cur: return None
        cur = cur[k]
    return cur

def fmt(val, unit): 
    """数値をフォーマット（Noneの場合はN/A）"""
    return f"{val}{unit}" if val is not None else "N/A"

def status(ok: bool, skip: bool) -> str:
    """チェック結果のステータス表示"""
    return "SKIP" if skip else ("OK" if ok else "NG")

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--bench", required=True, help="測定結果JSONファイル")
    ap.add_argument("--req", required=True, help="要件ドキュメント（SLO基準値）")
    ap.add_argument("--baseline", required=True, help="ベースライン結果JSONファイル")
    args = ap.parse_args()

    # 各ファイルを読み込み
    bench = jload(args.bench)
    base  = jload(args.baseline)
    lat_req_us, gc_req_ms = parse_req(args.req)

    case = bench.get("case")
    env  = bench.get("env")

    # 測定値を取得
    b_lat = pick(bench, ["metrics","latency_us","p99"])
    a_lat = pick(base,  ["metrics","latency_us","p99"])
    b_gc  = pick(bench, ["metrics","gc_pause_ms","p99"])

    errs, notes = [], []

    # スモークテスト判定（blackbox_* は性能チェック対象外）
    slo_applies = not is_smoke(case)

    # ベースライン比較の前提条件チェック（case/envが一致するか）
    case_mismatch = (base.get("case") != case or base.get("env") != env)
    if case_mismatch:
        print(f"[gate] WARN: baseline mismatch: base({base.get('case')},{base.get('env')}) "
              f"vs bench({case},{env})")

    # レイテンシSLOチェック（REQ-PERF-001）
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

    # ベースライン回帰チェック（前回比+5%以内）
    if (not case_mismatch) and (a_lat is not None) and (b_lat is not None) and slo_applies:
        thr = a_lat * 1.05  # 5%悪化まで許容
        base_ok, base_skip = (b_lat <= thr), False
        if not base_ok:
            errs.append(f"latency_us.p99={b_lat}us > baseline*1.05={thr:.1f}us (base={a_lat}us)")
    else:
        base_ok, base_skip = True, True  # 比較条件が揃わない場合はスキップ

    # GC停止時間SLOチェック（REQ-PERF-002）
    if b_gc is not None and gc_req_ms is not None:
        gc_ok, gc_skip = (b_gc <= gc_req_ms), False
        if not gc_ok:
            errs.append(f"gc_pause_ms.p99={b_gc}ms > {gc_req_ms}ms (REQ-PERF-002)")
    else:
        gc_ok, gc_skip = True, True  # 測定値または基準値がない場合はスキップ

    # 結果サマリ出力
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

    # 失敗時はエラー詳細を出力
    if not (lat_ok and gc_ok and base_ok):
        print("\nFAIL reasons:")
        for e in errs:
            print(" - " + e)
        return 1
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
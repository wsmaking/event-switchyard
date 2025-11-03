"""
コマンド実行パフォーマンス測定ツール

目的:
- 任意のシェルコマンドを複数回実行し、実行時間を統計的に分析
- 分散トレーシング風のspan概念でコマンド実行の開始〜終了を追跡
- CI/CDやベンチマークでの性能回帰検知に活用

使用例:
  python3 measure_cmd.py --cmd "java -cp app.jar ProducerApp" --runs 50 --name "producer_perf"
  
出力:
  results/trace.ndjson - 各実行のstart/end/errorイベント（JSONL形式）
"""

import argparse, json, subprocess, time, uuid, pathlib, sys

def emit(f, **ev):
    """
    構造化ログの出力
    
    Args:
        f: ファイルハンドル
        **ev: イベントデータ（辞書形式）
    
    Note:
        - JSON Lines形式（1行1JSON）で出力
        - ensure_ascii=False で日本語対応
        - flush() でリアルタイム書き込み保証
    """
    f.write(json.dumps(ev, ensure_ascii=False) + "\n")
    f.flush()

def main():
    # ========= コマンドライン引数の設定 =========
    ap = argparse.ArgumentParser(description="コマンド実行時間の統計的測定ツール")
    ap.add_argument("--cmd", required=True, 
                   help='測定対象コマンド（例: "echo hi >/dev/null"）')
    ap.add_argument("--runs", type=int, default=50,
                   help="実行回数（デフォルト: 50回）")
    ap.add_argument("--name", default="cmd",
                   help="測定ケース名（統計集計時の識別子）")
    ap.add_argument("--out", default="results/trace.ndjson",
                   help="出力ファイルパス")
    ap.add_argument("--sleep-us", type=int, default=0,
                   help="実行間隔（マイクロ秒、負荷調整用）")
    args = ap.parse_args()

    # ========= 出力ディレクトリの準備 =========
    p = pathlib.Path(args.out)
    p.parent.mkdir(parents=True, exist_ok=True)

    # ========= メイン測定ループ =========
    with p.open("w", encoding="utf-8") as f:
        print(f"[measure_cmd] 測定開始: {args.runs}回実行 → {args.out}")
        
        for run_num in range(args.runs):
            # --- 各実行にユニークなスパンIDを生成 ---
            # 分散トレーシング（Jaeger/Zipkin）の概念を参考
            # 複数回実行のstart/endペアを確実に対応付けるため
            span_id = str(uuid.uuid4())
            
            # --- 実行開始イベントの記録 ---
            emit(f, 
                 schema="trace.v1",           # データフォーマットのバージョン
                 ts_ns=time.perf_counter_ns(), # 開始時間（ナノ秒精度）
                 ev="span_start",             # イベント種別
                 name=args.name,              # 測定ケース名
                 span_id=span_id,             # この実行のユニークID
                 run_number=run_num + 1)      # 実行番号（1始まり）
            
            try:
                # --- コマンド実行（測定対象） ---
                subprocess.run(
                    args.cmd, 
                    shell=True,                    # シェル経由で実行
                    check=True,                    # 非0終了時に例外発生
                    stdout=subprocess.DEVNULL,     # 標準出力を抑制
                    stderr=subprocess.DEVNULL      # 標準エラーを抑制
                )
                
                # --- 成功時の終了イベント記録 ---
                emit(f,
                     schema="trace.v1",
                     ts_ns=time.perf_counter_ns(), # 終了時間（ナノ秒精度）
                     ev="span_end",               # 正常終了
                     name=args.name,
                     span_id=span_id,
                     run_number=run_num + 1)
                     
            except subprocess.CalledProcessError as e:
                # --- エラー時の終了イベント記録 ---
                emit(f,
                     schema="trace.v1", 
                     ts_ns=time.perf_counter_ns(), # 終了時間（ナノ秒精度）
                     ev="span_error",             # エラー終了
                     name=args.name,
                     span_id=span_id,
                     code=e.returncode,           # 終了コード
                     run_number=run_num + 1)
            
            # --- 実行間隔の調整 ---
            # 連続実行による負荷を避けるため（必要に応じて）
            if args.sleep_us > 0:
                time.sleep(args.sleep_us / 1_000_000)
    
    print(f"[measure_cmd] 測定完了: {args.runs}回のspan記録 → {args.out}")
    print(f"[measure_cmd] 次のステップ: agg_to_bench.py で統計集計")

if __name__ == "__main__":
    sys.exit(main())
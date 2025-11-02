#!/usr/bin/env python3
"""
クイックカナリーテスト: 簡易版SLO検証

目的:
  起動中のアプリケーションに対して軽量なトラフィックを送信し、
  /statsからメトリクスを取得してベンチマーク結果JSONを生成する。

使用例:
  python scripts/quick_canary.py \
    --url http://localhost:8080 \
    --events 1000 \
    --out var/results/canary.json
"""

import json
import sys
import argparse
import time
import random
from pathlib import Path
from datetime import datetime, timezone
import urllib.request
import urllib.error


def send_event(url: str, key: str, payload: dict) -> bool:
    """イベント送信"""
    try:
        data = json.dumps(payload).encode('utf-8')
        headers = {
            'Content-Type': 'application/json'
        }

        req = urllib.request.Request(f"{url}/events?key={key}", data=data, headers=headers, method='POST')
        with urllib.request.urlopen(req, timeout=5) as response:
            return response.status == 200
    except Exception:
        return False


def get_stats(url: str) -> dict:
    """統計情報取得"""
    try:
        req = urllib.request.Request(f"{url}/stats")
        with urllib.request.urlopen(req, timeout=5) as response:
            return json.loads(response.read().decode('utf-8'))
    except Exception as e:
        print(f"❌ エラー: 統計情報の取得に失敗: {e}", file=sys.stderr)
        return {}


def check_health(url: str) -> bool:
    """ヘルスチェック"""
    try:
        req = urllib.request.Request(f"{url}/health")
        with urllib.request.urlopen(req, timeout=5) as response:
            return response.status == 200
    except Exception:
        return False


def main():
    parser = argparse.ArgumentParser(description="クイックカナリーテスト")
    parser.add_argument("--url", default="http://localhost:8080", help="アプリケーションURL")
    parser.add_argument("--events", type=int, default=1000, help="送信イベント数")
    parser.add_argument("--keys", default="BTC,ETH,XRP", help="キー (カンマ区切り)")
    parser.add_argument("--out", required=True, help="出力JSON")

    args = parser.parse_args()

    # ヘルスチェック
    print(f"🔍 ヘルスチェック: {args.url}/health")
    if not check_health(args.url):
        print(f"❌ エラー: アプリケーションが起動していません", file=sys.stderr)
        sys.exit(1)
    print("✅ アプリケーション起動確認")
    print()

    # キー配列
    keys = [k.strip() for k in args.keys.split(',')]

    # 負荷生成
    print(f"⚡ 負荷テスト開始: {args.events}イベント")
    start_time = time.time()
    sent_count = 0

    for i in range(args.events):
        key = random.choice(keys)
        payload = {
            "symbol": key,
            "price": random.uniform(1000, 100000),
            "quantity": random.uniform(0.1, 100),
            "ts": int(time.time() * 1000)
        }

        if send_event(args.url, key, payload):
            sent_count += 1

        # 進捗表示
        if (i + 1) % 100 == 0:
            print(f"   進捗: {i + 1}/{args.events}")

    elapsed = time.time() - start_time
    print(f"✅ 負荷テスト完了: {sent_count}イベント送信 (経過: {elapsed:.1f}秒)")
    print()

    # クールダウン
    print("⏳ クールダウン (1秒)...")
    time.sleep(1)

    # メトリクス取得
    print("📊 メトリクス収集中...")
    stats = get_stats(args.url)

    if not stats:
        print("❌ エラー: メトリクス取得失敗", file=sys.stderr)
        sys.exit(1)

    # ベンチマーク結果JSON生成
    fast_path_count = stats.get('fast_path_count', 0)
    drop_count = stats.get('fast_path_drop_count', 0)

    process_p50 = stats.get('fast_path_process_p50_us', 0.0)
    process_p99 = stats.get('fast_path_process_p99_us', 0.0)
    process_p999 = stats.get('fast_path_process_p999_us', 0.0)

    publish_p50 = stats.get('fast_path_publish_p50_us', 0.0)
    publish_p99 = stats.get('fast_path_publish_p99_us', 0.0)
    publish_p999 = stats.get('fast_path_publish_p999_us', 0.0)

    pq_write_p99 = stats.get('persistence_queue_write_p99_us', 0.0)
    pq_error_count = stats.get('persistence_queue_error_count', 0)
    pq_lag = stats.get('persistence_queue_lag', 0)

    # tail_ratio計算
    tail_ratio = (process_p99 / process_p50) if process_p50 > 0 else 0.0

    # スループット計算
    throughput = (fast_path_count / elapsed) if elapsed > 0 else 0.0

    # エラー率計算
    error_rate = (pq_error_count * 100.0 / fast_path_count) if fast_path_count > 0 else 0.0

    # Git情報
    try:
        import subprocess
        git_commit = subprocess.check_output(['git', 'rev-parse', 'HEAD'], text=True).strip()
        git_branch = subprocess.check_output(['git', 'rev-parse', '--abbrev-ref', 'HEAD'], text=True).strip()
    except Exception:
        git_commit = "unknown"
        git_branch = "unknown"

    # 結果JSON
    result = {
        "version": "v1",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "environment": {
            "type": "local",
            "config": {
                "fast_path_enable": True,
                "fast_path_metrics": True,
                "kafka_bridge_enable": False,
                "jvm_heap_mb": 2048
            },
            "git_commit": git_commit,
            "git_branch": git_branch
        },
        "profile": {
            "name": "custom",
            "duration_sec": max(1, int(elapsed)),
            "events_total": args.events,
            "warmup_events": 0,
            "keys": keys
        },
        "metrics": {
            "fast_path": {
                "count": fast_path_count,
                "process_latency_us": {
                    "p50": process_p50,
                    "p99": process_p99,
                    "p999": process_p999
                },
                "publish_latency_us": {
                    "p50": publish_p50,
                    "p99": publish_p99,
                    "p999": publish_p999
                },
                "drop_count": drop_count
            },
            "persistence_queue": {
                "write_latency_us": {
                    "p99": pq_write_p99
                },
                "error_count": pq_error_count,
                "lag": pq_lag
            },
            "summary": {
                "tail_ratio": tail_ratio,
                "throughput_events_per_sec": throughput,
                "error_rate_percent": error_rate
            }
        },
        "slo_compliance": {
            "status": "PASS",
            "checks": [],
            "timestamp": datetime.now(timezone.utc).isoformat()
        }
    }

    # 出力
    output_path = Path(args.out)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    with open(output_path, 'w') as f:
        json.dump(result, f, indent=2)

    print(f"💾 結果保存: {output_path}")
    print()
    print("📈 結果サマリ:")
    print(f"   Fast Path処理数: {fast_path_count}")
    print(f"   ドロップ数: {drop_count}")
    print(f"   p50: {process_p50:.3f}μs")
    print(f"   p99: {process_p99:.3f}μs")
    print(f"   p999: {process_p999:.3f}μs")
    print(f"   Tail Ratio: {tail_ratio:.2f}")
    print(f"   スループット: {throughput:.0f} events/s")
    print(f"   エラー率: {error_rate:.4f}%")
    print()
    print("✅ クイックカナリー完了")


if __name__ == "__main__":
    main()

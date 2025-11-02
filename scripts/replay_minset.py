#!/usr/bin/env python3
"""
最小再現リプレイ抽出 (Minimal Replay Extractor)

目的:
  インシデント発生時刻の前後15分間のChronicle Queue (WAL) から、
  関連イベントのみを抽出し、最小限の再現可能なリプレイシナリオを生成する。

機能:
  1. 時刻範囲でイベントフィルタ (incident_time ± 15分)
  2. キー/パーティションでフィルタ (オプション)
  3. WAL順序でトポロジカルソート (依存関係保持)
  4. NDJSONで出力 (replay/case-*.ndjson)
  5. 再生スクリプト生成 (scripts/replay.sh)

使用例:
  # BTC関連のインシデントログから15分間の最小セット抽出
  python scripts/replay_minset.py \
    --wal var/chronicle \
    --incident-time "2025-11-02T10:30:00Z" \
    --keys BTC \
    --out replay/case-001.ndjson

  # 再生実行
  bash scripts/replay.sh --in replay/case-001.ndjson --target http://localhost:8080
"""

import json
import sys
import argparse
from pathlib import Path
from datetime import datetime, timedelta
from typing import List, Dict, Optional, Set
from dataclasses import dataclass
import struct


@dataclass
class Event:
    """イベントデータ"""
    timestamp_ms: int
    key: str
    payload: bytes
    offset: int  # WAL内のオフセット


class ChronicleQueueReader:
    """Chronicle Queue (WAL) 読み取り"""

    def __init__(self, queue_path: Path):
        self.queue_path = queue_path
        if not queue_path.exists():
            raise FileNotFoundError(f"Chronicle Queue not found: {queue_path}")

    def read_events(self, start_time_ms: int, end_time_ms: int, keys: Optional[Set[str]] = None) -> List[Event]:
        """
        指定時刻範囲のイベントを読み取り

        注: Chronicle Queueの実際の読み取りにはChronicle Queueライブラリが必要。
        ここでは簡易実装として、NDJSONログファイルからの読み取りをシミュレート。
        """
        events = []

        # 簡易実装: var/chronicle/*.ndjson からイベント読み取り
        # 実際はChronicle Queue Wire Formatをパースする必要がある
        log_files = sorted(self.queue_path.glob("*.ndjson"))

        for log_file in log_files:
            with open(log_file, 'r') as f:
                for line_num, line in enumerate(f):
                    try:
                        entry = json.loads(line.strip())
                        ts_ms = entry.get("timestamp_ms", 0)
                        key = entry.get("key", "")
                        payload_json = entry.get("payload", {})

                        # 時刻範囲フィルタ
                        if not (start_time_ms <= ts_ms <= end_time_ms):
                            continue

                        # キーフィルタ
                        if keys and key not in keys:
                            continue

                        # Eventオブジェクト生成
                        events.append(Event(
                            timestamp_ms=ts_ms,
                            key=key,
                            payload=json.dumps(payload_json).encode('utf-8'),
                            offset=line_num
                        ))
                    except json.JSONDecodeError:
                        continue

        # タイムスタンプ順でソート (WAL順序)
        events.sort(key=lambda e: (e.timestamp_ms, e.offset))

        return events


class ReplayMinsetExtractor:
    """最小再現セット抽出"""

    def __init__(self, wal_path: Path, incident_time: datetime, window_minutes: int = 15):
        self.wal_path = wal_path
        self.incident_time = incident_time
        self.window_minutes = window_minutes

    def extract(self, keys: Optional[Set[str]] = None) -> List[Event]:
        """インシデント時刻 ± window_minutes のイベント抽出"""
        start_time = self.incident_time - timedelta(minutes=self.window_minutes)
        end_time = self.incident_time + timedelta(minutes=self.window_minutes)

        start_time_ms = int(start_time.timestamp() * 1000)
        end_time_ms = int(end_time.timestamp() * 1000)

        reader = ChronicleQueueReader(self.wal_path)
        events = reader.read_events(start_time_ms, end_time_ms, keys)

        return events

    def deduplicate(self, events: List[Event]) -> List[Event]:
        """重複イベント除去 (同一key+タイムスタンプ)"""
        seen = set()
        unique_events = []

        for event in events:
            key_tuple = (event.key, event.timestamp_ms)
            if key_tuple not in seen:
                seen.add(key_tuple)
                unique_events.append(event)

        return unique_events


def write_ndjson(events: List[Event], output_path: Path):
    """NDJSON形式で出力"""
    output_path.parent.mkdir(parents=True, exist_ok=True)

    with open(output_path, 'w') as f:
        for event in events:
            entry = {
                "timestamp_ms": event.timestamp_ms,
                "key": event.key,
                "payload": json.loads(event.payload.decode('utf-8'))
            }
            f.write(json.dumps(entry) + "\n")


def generate_replay_script(ndjson_path: Path, script_path: Path):
    """再生スクリプト生成 (scripts/replay.sh)"""
    script_path.parent.mkdir(parents=True, exist_ok=True)

    script_content = f"""#!/usr/bin/env bash
# リプレイスクリプト (自動生成)
# 使用方法: bash {script_path.name} --target http://localhost:8080

set -euo pipefail

TARGET_URL="${{1:-http://localhost:8080}}"
REPLAY_FILE="{ndjson_path}"

echo "🎬 リプレイ開始: $REPLAY_FILE → $TARGET_URL"
echo ""

# ヘルスチェック
if ! curl -s -f "$TARGET_URL/health" >/dev/null 2>&1; then
  echo "❌ エラー: ターゲットアプリが起動していません" >&2
  exit 1
fi

# イベント再生
EVENT_COUNT=0
START_TIME=$(date +%s)

while IFS= read -r line; do
  KEY=$(echo "$line" | jq -r '.key')
  PAYLOAD=$(echo "$line" | jq -c '.payload')
  TIMESTAMP_MS=$(echo "$line" | jq -r '.timestamp_ms')

  # /ingressエンドポイントにPOST
  curl -s -X POST "$TARGET_URL/ingress" \\
    -H "Content-Type: application/json" \\
    -H "X-Key: $KEY" \\
    -d "$PAYLOAD" >/dev/null 2>&1 || {{
      echo "⚠️  警告: イベント送信失敗 (key=$KEY, ts=$TIMESTAMP_MS)" >&2
    }}

  EVENT_COUNT=$((EVENT_COUNT + 1))

  # 進捗表示 (100イベントごと)
  if [[ $((EVENT_COUNT % 100)) -eq 0 ]]; then
    echo "   再生済み: ${{EVENT_COUNT}}イベント"
  fi

  # レート制限 (オプション: 過負荷防止)
  # sleep 0.001

done < "$REPLAY_FILE"

ELAPSED_TIME=$(($(date +%s) - START_TIME))
echo ""
echo "✅ リプレイ完了: ${{EVENT_COUNT}}イベント (経過: ${{ELAPSED_TIME}}秒)"
echo "   スループット: $((EVENT_COUNT / ELAPSED_TIME)) events/s"
"""

    with open(script_path, 'w') as f:
        f.write(script_content)

    script_path.chmod(0o755)


def main():
    parser = argparse.ArgumentParser(description="最小再現リプレイ抽出: インシデント時刻前後のイベント抽出")
    parser.add_argument("--wal", required=True, help="Chronicle Queueディレクトリ (例: var/chronicle)")
    parser.add_argument("--incident-time", required=True, help="インシデント発生時刻 (ISO 8601, 例: 2025-11-02T10:30:00Z)")
    parser.add_argument("--keys", help="対象キー (カンマ区切り, 例: BTC,ETH)")
    parser.add_argument("--window-minutes", type=int, default=15, help="抽出時間窓 (デフォルト: 15分)")
    parser.add_argument("--out", required=True, help="出力NDJSONファイル (例: replay/case-001.ndjson)")
    parser.add_argument("--generate-script", action="store_true", help="replay.sh生成")
    parser.add_argument("--deduplicate", action="store_true", help="重複イベント除去")

    args = parser.parse_args()

    # インシデント時刻パース
    try:
        incident_time = datetime.fromisoformat(args.incident_time.replace('Z', '+00:00'))
    except ValueError as e:
        print(f"❌ エラー: インシデント時刻のパースに失敗: {e}", file=sys.stderr)
        sys.exit(1)

    # キーパース
    keys = None
    if args.keys:
        keys = set(k.strip() for k in args.keys.split(',') if k.strip())

    print(f"📋 最小再現リプレイ抽出設定:")
    print(f"   WALパス: {args.wal}")
    print(f"   インシデント時刻: {incident_time.isoformat()}")
    print(f"   時間窓: ±{args.window_minutes}分")
    print(f"   対象キー: {', '.join(keys) if keys else '全キー'}")
    print(f"   出力: {args.out}")
    print()

    # 抽出実行
    wal_path = Path(args.wal)
    extractor = ReplayMinsetExtractor(wal_path, incident_time, args.window_minutes)

    try:
        events = extractor.extract(keys)
    except FileNotFoundError as e:
        print(f"❌ エラー: {e}", file=sys.stderr)
        sys.exit(1)

    if not events:
        print("⚠️  警告: 抽出イベント数ゼロ。時刻範囲またはキーフィルタを確認してください。")
        sys.exit(0)

    # 重複除去 (オプション)
    if args.deduplicate:
        original_count = len(events)
        events = extractor.deduplicate(events)
        print(f"🔍 重複除去: {original_count} → {len(events)}イベント")

    # NDJSON出力
    output_path = Path(args.out)
    write_ndjson(events, output_path)
    print(f"💾 リプレイファイル生成: {output_path} ({len(events)}イベント)")

    # 再生スクリプト生成 (オプション)
    if args.generate_script:
        script_path = Path("scripts/replay.sh")
        generate_replay_script(output_path, script_path)
        print(f"📜 再生スクリプト生成: {script_path}")
        print()
        print("🎬 再生実行:")
        print(f"   bash {script_path} http://localhost:8080")

    print()
    print("✅ 抽出完了")


if __name__ == "__main__":
    main()

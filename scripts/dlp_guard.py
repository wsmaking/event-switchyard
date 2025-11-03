"""
DLP (Data Loss Prevention) ガード - PII検出

目的:
  ソースコード・ドキュメント・ログから個人情報 (PII) を検出し、
  意図しない漏洩を防止する。

検出対象:
  - メールアドレス
  - クレジットカード番号
  - 電話番号
  - APIキー/シークレット
  - 個人名 (allowlist対応)

使用例:
  # docs/配下をスキャン
  python scripts/dlp_guard.py --scan docs/ --report var/results/dlp_report.json

  # PII検出時にCI失敗
  python scripts/dlp_guard.py --scan docs/ --fail-on-detect
"""

import re
import sys
import argparse
import json
from pathlib import Path
from typing import List, Dict, Set
from dataclasses import dataclass


@dataclass
class PIIMatch:
    """PII検出結果"""
    file_path: str
    line_number: int
    pii_type: str
    matched_text: str
    context: str  # 前後の文脈


class DLPGuard:
    """DLPガード: PII検出"""

    # PII検出パターン (正規表現)
    PATTERNS = {
        "email": re.compile(r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b'),
        "credit_card": re.compile(r'\b\d{4}[\s-]?\d{4}[\s-]?\d{4}[\s-]?\d{4}\b'),
        "phone_jp": re.compile(r'\b0\d{1,4}[-\s]?\d{1,4}[-\s]?\d{4}\b'),
        "api_key": re.compile(r'(?i)(api[_-]?key|secret|token|password)\s*[:=]\s*["\']?([A-Za-z0-9_\-]{20,})["\']?'),
        "aws_key": re.compile(r'AKIA[0-9A-Z]{16}'),
    }

    # Allowlist (誤検出除外)
    ALLOWLIST_PATTERNS = {
        "email": [
            r'noreply@anthropic\.com',  # 例: 公開用メールアドレス
            r'example@example\.com',
            r'test@test\.com',
        ],
        "api_key": [
            r'YOUR_API_KEY_HERE',
            r'PLACEHOLDER',
        ]
    }

    def __init__(self):
        self.matches: List[PIIMatch] = []

    def scan_file(self, file_path: Path):
        """ファイルをスキャンしてPII検出"""
        try:
            with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
                lines = f.readlines()

            for line_num, line in enumerate(lines, 1):
                for pii_type, pattern in self.PATTERNS.items():
                    for match in pattern.finditer(line):
                        matched_text = match.group(0)

                        # Allowlistチェック
                        if self._is_allowlisted(pii_type, matched_text):
                            continue

                        # 文脈取得 (前後10文字)
                        start = max(0, match.start() - 10)
                        end = min(len(line), match.end() + 10)
                        context = line[start:end].strip()

                        self.matches.append(PIIMatch(
                            file_path=str(file_path),
                            line_number=line_num,
                            pii_type=pii_type,
                            matched_text=matched_text,
                            context=context
                        ))
        except Exception as e:
            # バイナリファイルなどはスキップ
            pass

    def _is_allowlisted(self, pii_type: str, text: str) -> bool:
        """Allowlistチェック"""
        allowlist = self.ALLOWLIST_PATTERNS.get(pii_type, [])
        for pattern_str in allowlist:
            if re.search(pattern_str, text, re.IGNORECASE):
                return True
        return False

    def scan_directory(self, dir_path: Path, extensions: Set[str] = None):
        """ディレクトリを再帰的にスキャン"""
        if extensions is None:
            # デフォルト: テキストファイル系のみ
            extensions = {'.md', '.txt', '.java', '.kt', '.py', '.js', '.ts', '.yaml', '.yml', '.json', '.sh'}

        for file_path in dir_path.rglob('*'):
            if file_path.is_file() and file_path.suffix in extensions:
                self.scan_file(file_path)


def write_report(matches: List[PIIMatch], output_path: Path):
    """レポート出力 (JSON)"""
    output_path.parent.mkdir(parents=True, exist_ok=True)

    report = {
        "total_matches": len(matches),
        "matches": [
            {
                "file": m.file_path,
                "line": m.line_number,
                "type": m.pii_type,
                "text": m.matched_text,
                "context": m.context
            }
            for m in matches
        ]
    }

    with open(output_path, 'w') as f:
        json.dump(report, f, indent=2)


def main():
    parser = argparse.ArgumentParser(description="DLPガード: PII検出・漏洩防止")
    parser.add_argument("--scan", required=True, help="スキャン対象ディレクトリ (例: docs/)")
    parser.add_argument("--report", help="レポート出力 (JSON)")
    parser.add_argument("--fail-on-detect", action="store_true", help="PII検出時にエラー終了")
    parser.add_argument("--verbose", action="store_true", help="詳細出力")

    args = parser.parse_args()

    # スキャン実行
    scan_dir = Path(args.scan)
    if not scan_dir.exists():
        print(f"❌ エラー: スキャン対象が見つかりません: {scan_dir}", file=sys.stderr)
        sys.exit(1)

    print(f"🔍 DLPガード: PII検出スキャン開始")
    print(f"   対象: {scan_dir}")
    print()

    guard = DLPGuard()
    guard.scan_directory(scan_dir)

    # 結果サマリ
    if not guard.matches:
        print("✅ PII検出なし")

        # レポート出力 (空)
        if args.report:
            write_report([], Path(args.report))
            print(f"💾 レポート保存: {args.report}")

        sys.exit(0)

    # PII検出あり
    print(f"⚠️  PII検出: {len(guard.matches)}件")
    print()

    # 詳細出力
    if args.verbose:
        for i, match in enumerate(guard.matches, 1):
            print(f"{i}. [{match.pii_type}] {match.file_path}:{match.line_number}")
            print(f"   マッチ: {match.matched_text}")
            print(f"   文脈: ...{match.context}...")
            print()

    # レポート出力
    if args.report:
        write_report(guard.matches, Path(args.report))
        print(f"💾 レポート保存: {args.report}")

    # 終了コード
    if args.fail_on_detect:
        print()
        print("❌ PII検出によりCI失敗")
        sys.exit(1)
    else:
        print()
        print("⚠️  警告: PIIが検出されましたが、--fail-on-detect未指定のため継続します")
        sys.exit(0)


if __name__ == "__main__":
    main()

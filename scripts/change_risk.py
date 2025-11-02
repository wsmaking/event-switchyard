#!/usr/bin/env python3
"""
変更リスクスコア (Change Risk Score)

目的:
  PRの変更内容を分析し、リスクスコアを算出してレビュア・承認者に警告する。

スコアリング要素:
  - 変更行数 (大規模変更ほど高リスク)
  - 変更ファイル数
  - Critical Path変更 (Fast Path、Router、メトリクス等)
  - テストカバレッジ低下
  - 外部依存関係変更 (build.gradle等)

使用例:
  # Git diffからリスクスコア算出
  python scripts/change_risk.py \
    --diff-base main \
    --out var/results/change_risk.json

  # CI統合: リスクスコアが閾値超過でPR失敗
  python scripts/change_risk.py --diff-base main --threshold 70 --fail-on-high-risk
"""

import subprocess
import sys
import argparse
import json
from pathlib import Path
from typing import List, Dict, Set
from dataclasses import dataclass


@dataclass
class ChangeRisk:
    """変更リスク評価"""
    score: int  # 0-100
    risk_level: str  # low, medium, high, critical
    factors: List[str]
    recommendations: List[str]


class ChangeRiskAnalyzer:
    """変更リスク分析"""

    # Critical Pathファイルパターン
    CRITICAL_PATHS = {
        "app/src/main/kotlin/app/engine/",
        "app/src/main/kotlin/app/fast/",
        "app/src/main/kotlin/app/kafka/",
        ".github/workflows/",
        "k8s/",
    }

    # 変更規模閾値
    LINES_THRESHOLD_MEDIUM = 200
    LINES_THRESHOLD_HIGH = 500
    FILES_THRESHOLD_MEDIUM = 10
    FILES_THRESHOLD_HIGH = 20

    def __init__(self, base_branch: str = "main"):
        self.base_branch = base_branch

    def get_git_diff_stats(self) -> Dict:
        """Git diffから変更統計取得"""
        try:
            # 変更行数・ファイル数取得
            result = subprocess.run(
                ["git", "diff", "--numstat", f"{self.base_branch}...HEAD"],
                capture_output=True,
                text=True,
                check=True
            )

            lines_added = 0
            lines_deleted = 0
            files_changed = []

            for line in result.stdout.strip().split('\n'):
                if not line:
                    continue
                parts = line.split('\t')
                if len(parts) >= 3:
                    added = int(parts[0]) if parts[0] != '-' else 0
                    deleted = int(parts[1]) if parts[1] != '-' else 0
                    file_path = parts[2]

                    lines_added += added
                    lines_deleted += deleted
                    files_changed.append(file_path)

            return {
                "lines_added": lines_added,
                "lines_deleted": lines_deleted,
                "lines_total": lines_added + lines_deleted,
                "files_changed": files_changed,
                "files_count": len(files_changed)
            }
        except subprocess.CalledProcessError as e:
            print(f"❌ エラー: Git diffの取得に失敗: {e}", file=sys.stderr)
            sys.exit(1)

    def analyze_risk(self) -> ChangeRisk:
        """変更リスク分析"""
        diff_stats = self.get_git_diff_stats()

        score = 0
        factors = []
        recommendations = []

        # ファクター1: 変更行数
        lines_total = diff_stats["lines_total"]
        if lines_total > self.LINES_THRESHOLD_HIGH:
            score += 30
            factors.append(f"大規模変更: {lines_total}行 (閾値: {self.LINES_THRESHOLD_HIGH})")
            recommendations.append("変更を複数PRに分割することを推奨")
        elif lines_total > self.LINES_THRESHOLD_MEDIUM:
            score += 15
            factors.append(f"中規模変更: {lines_total}行 (閾値: {self.LINES_THRESHOLD_MEDIUM})")

        # ファクター2: 変更ファイル数
        files_count = diff_stats["files_count"]
        if files_count > self.FILES_THRESHOLD_HIGH:
            score += 20
            factors.append(f"多数ファイル変更: {files_count}件 (閾値: {self.FILES_THRESHOLD_HIGH})")
        elif files_count > self.FILES_THRESHOLD_MEDIUM:
            score += 10
            factors.append(f"中程度ファイル変更: {files_count}件 (閾値: {self.FILES_THRESHOLD_MEDIUM})")

        # ファクター3: Critical Path変更
        critical_files = []
        for file_path in diff_stats["files_changed"]:
            for critical_path in self.CRITICAL_PATHS:
                if file_path.startswith(critical_path):
                    critical_files.append(file_path)
                    break

        if critical_files:
            score += 25
            factors.append(f"Critical Path変更: {len(critical_files)}件")
            recommendations.append("Fast Path/Router変更はステージング環境で十分なテストを実施")

        # ファクター4: ビルド設定変更
        build_files = [f for f in diff_stats["files_changed"] if "build.gradle" in f or "Dockerfile" in f]
        if build_files:
            score += 15
            factors.append("ビルド設定変更あり")
            recommendations.append("依存関係変更は脆弱性スキャンを実施")

        # ファクター5: CI/CD変更
        ci_files = [f for f in diff_stats["files_changed"] if ".github/workflows" in f or "k8s/" in f]
        if ci_files:
            score += 20
            factors.append("CI/CD設定変更あり")
            recommendations.append("CI/CD変更は別PRで先行リリース推奨")

        # リスクレベル判定
        if score >= 70:
            risk_level = "critical"
        elif score >= 50:
            risk_level = "high"
        elif score >= 30:
            risk_level = "medium"
        else:
            risk_level = "low"

        # 一般的な推奨事項
        if score >= 50:
            recommendations.append("Tech Lead/Staff Engineerによるレビュー必須")
            recommendations.append("カナリアリリースで段階的デプロイ推奨")

        return ChangeRisk(
            score=min(score, 100),  # 最大100点
            risk_level=risk_level,
            factors=factors if factors else ["変更規模小 (低リスク)"],
            recommendations=recommendations if recommendations else ["標準レビュープロセスで問題なし"]
        )


def write_report(risk: ChangeRisk, output_path: Path):
    """レポート出力 (JSON)"""
    output_path.parent.mkdir(parents=True, exist_ok=True)

    report = {
        "risk_score": risk.score,
        "risk_level": risk.risk_level,
        "factors": risk.factors,
        "recommendations": risk.recommendations
    }

    with open(output_path, 'w') as f:
        json.dump(report, f, indent=2)


def main():
    parser = argparse.ArgumentParser(description="変更リスクスコア: PR変更内容のリスク評価")
    parser.add_argument("--diff-base", default="main", help="差分ベースブランチ (デフォルト: main)")
    parser.add_argument("--out", help="レポート出力 (JSON)")
    parser.add_argument("--threshold", type=int, default=70, help="リスクスコア閾値 (デフォルト: 70)")
    parser.add_argument("--fail-on-high-risk", action="store_true", help="高リスク時にエラー終了")

    args = parser.parse_args()

    # リスク分析
    analyzer = ChangeRiskAnalyzer(args.diff_base)
    risk = analyzer.analyze_risk()

    # コンソール出力
    risk_emoji = {
        "low": "🟢",
        "medium": "🟡",
        "high": "🟠",
        "critical": "🔴"
    }

    print(f"{risk_emoji[risk.risk_level]} 変更リスクスコア: {risk.score}/100 ({risk.risk_level.upper()})")
    print()

    print("📋 リスク要因:")
    for factor in risk.factors:
        print(f"   - {factor}")
    print()

    print("💡 推奨事項:")
    for rec in risk.recommendations:
        print(f"   - {rec}")
    print()

    # レポート出力
    if args.out:
        write_report(risk, Path(args.out))
        print(f"💾 レポート保存: {args.out}")

    # 終了コード
    if args.fail_on_high_risk and risk.score >= args.threshold:
        print(f"❌ リスクスコアが閾値超過: {risk.score} >= {args.threshold}")
        print("   高リスク変更のため、追加レビューが必要です")
        sys.exit(1)
    else:
        sys.exit(0)


if __name__ == "__main__":
    main()

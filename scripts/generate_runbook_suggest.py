"""
RAGベース初動トリアージ (RAG-based Initial Triage)

目的:
  インシデント発生時に、過去のドキュメント・PR・インシデントノート・フレームグラフから
  類似事例を検索し、原因候補と対処手順を提示する。

機能:
  1. ドキュメント・コードベースのベクトルインデックス構築 (pgvector or FAISS)
  2. インシデント症状からベクトル検索
  3. 類似事例のトップK件を取得
  4. 出典付きでRunbook提案を生成

使用例:
  # インデックス構築 (初回のみ)
  python scripts/generate_runbook_suggest.py \
    --build-index \
    --docs docs/ \
    --reports reports/ \
    --out var/rag_index.faiss

  # インシデント症状から原因候補検索
  python scripts/generate_runbook_suggest.py \
    --index var/rag_index.faiss \
    --query "Fast Path p99 latency spike to 500us, drop_count > 0" \
    --out var/runbook_suggest.md

環境変数:
  - RAG_ENABLE: "1"で有効化 (デフォルト: "0")
  - OPENAI_API_KEY: OpenAI APIキー (埋め込みベクトル生成用、オプション)
"""

import json
import sys
import argparse
import os
from pathlib import Path
from typing import List, Dict, Tuple
from dataclasses import dataclass
import hashlib


@dataclass
class Document:
    """ドキュメントエントリ"""
    source: str  # ファイルパス
    content: str
    metadata: Dict


class SimpleRAG:
    """
    簡易RAGエンジン (FAISS/pgvectorのモック実装)

    本番環境では以下を使用:
    - sentence-transformers (埋め込みベクトル生成)
    - FAISS or pgvector (ベクトル検索)
    - OpenAI Embeddings API (オプション: 高精度)
    """

    def __init__(self):
        self.documents: List[Document] = []
        self.index_built = False

    def add_document(self, source: str, content: str, metadata: Dict = None):
        """ドキュメント追加"""
        self.documents.append(Document(
            source=source,
            content=content,
            metadata=metadata or {}
        ))

    def build_index(self):
        """インデックス構築 (モック: 単純なハッシュベース)"""
        print(f"📚 インデックス構築中... ({len(self.documents)}件)")
        # 実装では sentence-transformers でベクトル化し、FAISSインデックスを構築
        self.index_built = True
        print("✅ インデックス構築完了")

    def search(self, query: str, top_k: int = 5) -> List[Tuple[Document, float]]:
        """
        類似文書検索

        実装では:
        1. queryをベクトル化
        2. FAISSでコサイン類似度トップK件検索
        3. (Document, similarity_score) のリストを返す
        """
        if not self.index_built:
            raise ValueError("インデックス未構築。build_index()を先に実行してください。")

        # モック実装: キーワードマッチング (実際はベクトル類似度)
        results = []
        query_lower = query.lower()

        for doc in self.documents:
            # 簡易スコア計算: クエリ単語の出現頻度
            score = sum(1 for word in query_lower.split() if word in doc.content.lower())
            if score > 0:
                results.append((doc, float(score)))

        # スコア降順でソート、トップK件を返す
        results.sort(key=lambda x: x[1], reverse=True)
        return results[:top_k]

    def save_index(self, output_path: Path):
        """インデックス保存 (モック: JSON)"""
        output_path.parent.mkdir(parents=True, exist_ok=True)

        index_data = {
            "documents": [
                {
                    "source": doc.source,
                    "content": doc.content,
                    "metadata": doc.metadata
                }
                for doc in self.documents
            ]
        }

        with open(output_path, 'w') as f:
            json.dump(index_data, f, indent=2)

        print(f"💾 インデックス保存: {output_path}")

    @classmethod
    def load_index(cls, index_path: Path) -> 'SimpleRAG':
        """インデックス読み込み"""
        if not index_path.exists():
            raise FileNotFoundError(f"インデックスファイルが見つかりません: {index_path}")

        with open(index_path, 'r') as f:
            index_data = json.load(f)

        rag = cls()
        for doc_data in index_data.get("documents", []):
            rag.add_document(
                source=doc_data["source"],
                content=doc_data["content"],
                metadata=doc_data.get("metadata", {})
            )

        rag.build_index()
        return rag


def index_documents(docs_dir: Path, reports_dir: Path = None) -> SimpleRAG:
    """ドキュメントディレクトリをインデックス化"""
    rag = SimpleRAG()

    # docs/ 配下のマークダウンファイル
    if docs_dir.exists():
        for md_file in docs_dir.rglob("*.md"):
            content = md_file.read_text(encoding='utf-8')
            rag.add_document(
                source=str(md_file.relative_to(docs_dir.parent)),
                content=content,
                metadata={"type": "documentation"}
            )

    # reports/ 配下のレポート (JSONやMarkdown)
    if reports_dir and reports_dir.exists():
        for report_file in reports_dir.rglob("*"):
            if report_file.suffix in ['.md', '.json', '.txt']:
                content = report_file.read_text(encoding='utf-8')
                rag.add_document(
                    source=str(report_file.relative_to(reports_dir.parent)),
                    content=content,
                    metadata={"type": "report"}
                )

    rag.build_index()
    return rag


def generate_runbook_suggestion(query: str, search_results: List[Tuple[Document, float]]) -> str:
    """検索結果からRunbook提案を生成"""
    if not search_results:
        return "❌ 関連する事例が見つかりませんでした。\n\n新規インシデントとして調査を開始してください。"

    lines = [
        "# RAGベース初動トリアージ結果",
        "",
        f"**症状クエリ**: {query}",
        "",
        "## 類似事例・原因候補",
        ""
    ]

    for i, (doc, score) in enumerate(search_results, 1):
        lines.append(f"### {i}. {doc.source} (スコア: {score:.2f})")
        lines.append("")
        lines.append("**抜粋**:")
        lines.append("```")
        # 最初の500文字を抜粋
        excerpt = doc.content[:500].strip()
        if len(doc.content) > 500:
            excerpt += "\n...(省略)"
        lines.append(excerpt)
        lines.append("```")
        lines.append("")
        lines.append(f"**出典**: [{doc.source}]({doc.source})")
        lines.append("")

    lines.append("## 推奨対処手順")
    lines.append("")
    lines.append("1. **上記類似事例を確認**: 過去の対処パターンが適用可能か検証")
    lines.append("2. **メトリクス確認**: `/stats`, `/metrics` で現在の状態を取得")
    lines.append("3. **ログ確認**: `var/logs/app.log` でエラーメッセージを確認")
    lines.append("4. **リプレイテスト**: `scripts/replay_minset.py` でインシデント時刻前後のイベント抽出・再生")
    lines.append("5. **エスカレーション**: 15分で改善しない場合、オンコール担当者へ連絡")
    lines.append("")
    lines.append("---")
    lines.append("")
    lines.append("**注意**: 本提案はRAG自動生成です。必ず出典を確認し、状況に応じて判断してください。")

    return "\n".join(lines)


def main():
    parser = argparse.ArgumentParser(description="RAGベース初動トリアージ: 類似事例検索・Runbook提案")

    # インデックス構築モード
    parser.add_argument("--build-index", action="store_true", help="インデックス構築モード")
    parser.add_argument("--docs", help="ドキュメントディレクトリ (例: docs/)")
    parser.add_argument("--reports", help="レポートディレクトリ (例: reports/)")

    # 検索モード
    parser.add_argument("--index", help="インデックスファイル (例: var/rag_index.json)")
    parser.add_argument("--query", help="インシデント症状クエリ")

    # 共通
    parser.add_argument("--out", help="出力ファイル (例: var/runbook_suggest.md)")
    parser.add_argument("--top-k", type=int, default=5, help="トップK件取得 (デフォルト: 5)")

    args = parser.parse_args()

    # RAG有効化チェック (環境変数)
    rag_enabled = os.getenv("RAG_ENABLE", "0") == "1"
    if not rag_enabled and not (args.build_index or args.query):
        print("ℹ️  RAGが無効化されています。有効化するには RAG_ENABLE=1 を設定してください。")
        sys.exit(0)

    # インデックス構築モード
    if args.build_index:
        if not args.docs:
            print("❌ エラー: --docsを指定してください", file=sys.stderr)
            sys.exit(1)

        docs_dir = Path(args.docs)
        reports_dir = Path(args.reports) if args.reports else None

        rag = index_documents(docs_dir, reports_dir)

        if args.out:
            output_path = Path(args.out)
            rag.save_index(output_path)

        print(f"✅ インデックス構築完了 ({len(rag.documents)}件)")
        sys.exit(0)

    # 検索モード
    if args.query:
        if not args.index:
            print("❌ エラー: --indexを指定してください", file=sys.stderr)
            sys.exit(1)

        index_path = Path(args.index)
        rag = SimpleRAG.load_index(index_path)

        print(f"🔍 検索中: {args.query}")
        results = rag.search(args.query, top_k=args.top_k)

        if not results:
            print("⚠️  類似事例が見つかりませんでした")
            sys.exit(0)

        print(f"✅ 類似事例 {len(results)}件を検出")
        print()

        # Runbook提案生成
        suggestion = generate_runbook_suggestion(args.query, results)

        # コンソール出力
        print(suggestion)

        # ファイル出力
        if args.out:
            output_path = Path(args.out)
            output_path.parent.mkdir(parents=True, exist_ok=True)
            with open(output_path, 'w') as f:
                f.write(suggestion)
            print()
            print(f"💾 Runbook提案保存: {output_path}")

        sys.exit(0)

    # 引数不足
    print("❌ エラー: --build-index または --query を指定してください", file=sys.stderr)
    parser.print_help()
    sys.exit(1)


if __name__ == "__main__":
    main()

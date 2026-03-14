#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from pathlib import Path

from ai_tools import retrieve_evidence


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Query local RAG index")
    parser.add_argument("--query", required=True)
    parser.add_argument("--db-path", default="var/ai_index/docs.sqlite")
    parser.add_argument("--top-k", type=int, default=5)
    parser.add_argument("--source-like", default=None, help="SQL LIKE pattern for source_path")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    chunks = retrieve_evidence(
        db_path=Path(args.db_path),
        query=args.query,
        top_k=args.top_k,
        source_like=args.source_like,
    )
    payload = {
        "query": args.query,
        "top_k": args.top_k,
        "results": [c.to_dict() for c in chunks],
    }
    print(json.dumps(payload, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())


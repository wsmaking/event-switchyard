#!/usr/bin/env python3
"""AI トリアージレポートを Grafana アノテーションとして投稿する。

使い方:
    python3 ai_triage_notify.py --triage-json var/results/run.triage.json

環境変数:
    GRAFANA_URL      (デフォルト: http://localhost:3000)
    GRAFANA_USER     (デフォルト: admin)
    GRAFANA_PASSWORD (デフォルト: admin)
    GRAFANA_DASHBOARD_UID (デフォルト: hft-fast-path)
"""
from __future__ import annotations

import argparse
import json
import sys
import time
import urllib.error
import urllib.request
from base64 import b64encode
from pathlib import Path
from typing import Any


def build_annotation_text(report: dict[str, Any]) -> str:
    """トリアージ JSON から人間が読めるアノテーション本文を組み立てる。"""
    lines: list[str] = []

    run_name = report.get("run_name", "unknown")
    mode = report.get("mode", "unknown")
    elapsed = report.get("elapsed_ms", 0)
    lines.append(f"**Gate FAIL — AI Triage ({mode}, {elapsed}ms)**")
    lines.append(f"Run: `{run_name}`")
    lines.append("")

    # 違反一覧
    violations = [v for v in report.get("violations", []) if v.get("violated")]
    if violations:
        lines.append("**Violations:**")
        for v in violations:
            name = v["name"]
            observed = v.get("observed", "?")
            rule = v.get("rule", "")
            dev = v.get("deviation_text", "")
            lines.append(f"- `{name}`: {observed} ({rule}) deviation={dev}")
        lines.append("")

    # 主因候補（仮説）
    analysis = report.get("analysis", {})
    hypotheses = analysis.get("hypotheses", [])
    if hypotheses:
        lines.append("**Hypotheses:**")
        for h in hypotheses:
            conf = h.get("confidence", 0)
            lines.append(f"- [{conf:.0%}] {h.get('text', '')}")
        lines.append("")

    # 確認推奨メトリクス
    recommended = analysis.get("recommended_metrics", [])
    if recommended:
        lines.append("**Check metrics:** " + ", ".join(f"`{m}`" for m in recommended[:6]))
        lines.append("")

    # 次のアクション
    actions = analysis.get("next_actions", [])
    if actions:
        lines.append("**Next:**")
        for a in actions:
            lines.append(f"- {a}")

    return "\n".join(lines)


def build_tags(report: dict[str, Any]) -> list[str]:
    """トリアージレポートからアノテーション用タグを生成する。"""
    tags = ["gate-fail", "ai-triage"]
    violations = [v for v in report.get("violations", []) if v.get("violated")]
    for v in violations:
        tags.append(v["name"])
    return tags


def post_annotation(
    grafana_url: str,
    user: str,
    password: str,
    dashboard_uid: str,
    report: dict[str, Any],
) -> dict[str, Any]:
    """Grafana Annotations API にアノテーションを投稿する。"""
    text = build_annotation_text(report)
    tags = build_tags(report)
    now_ms = int(time.time() * 1000)

    payload = json.dumps({
        "dashboardUID": dashboard_uid,
        "time": now_ms,
        "tags": tags,
        "text": text,
    }).encode("utf-8")

    url = f"{grafana_url.rstrip('/')}/api/annotations"
    cred = b64encode(f"{user}:{password}".encode()).decode()

    req = urllib.request.Request(
        url,
        data=payload,
        headers={
            "Content-Type": "application/json",
            "Authorization": f"Basic {cred}",
        },
        method="POST",
    )

    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            body = json.loads(resp.read().decode("utf-8"))
            return {"status": "ok", "id": body.get("id"), "url": url}
    except urllib.error.HTTPError as e:
        error_body = e.read().decode("utf-8", errors="replace") if e.fp else ""
        return {"status": "error", "code": e.code, "message": error_body}
    except urllib.error.URLError as e:
        return {"status": "error", "message": str(e.reason)}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="AI トリアージレポートを Grafana に投稿")
    parser.add_argument("--triage-json", required=True, help=".triage.json ファイルのパス")
    parser.add_argument("--grafana-url", default=None)
    parser.add_argument("--grafana-user", default=None)
    parser.add_argument("--grafana-password", default=None)
    parser.add_argument("--dashboard-uid", default=None)
    return parser.parse_args()


def main() -> int:
    import os
    args = parse_args()

    grafana_url = args.grafana_url or os.environ.get("GRAFANA_URL", "http://localhost:3000")
    grafana_user = args.grafana_user or os.environ.get("GRAFANA_USER", "admin")
    grafana_password = args.grafana_password or os.environ.get("GRAFANA_PASSWORD", "admin")
    dashboard_uid = args.dashboard_uid or os.environ.get("GRAFANA_DASHBOARD_UID", "hft-fast-path")

    triage_path = Path(args.triage_json)
    if not triage_path.exists():
        print(f"triage file not found: {triage_path}", file=sys.stderr)
        return 1

    report = json.loads(triage_path.read_text(encoding="utf-8"))
    result = post_annotation(grafana_url, grafana_user, grafana_password, dashboard_uid, report)

    if result["status"] == "ok":
        print(f"[grafana] annotation posted (id={result['id']})")
        return 0
    else:
        print(f"[grafana] failed: {result}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())

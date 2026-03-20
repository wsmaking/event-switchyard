#!/usr/bin/env python3
import argparse
import json
from pathlib import Path

from join_quant_feedback_with_mini_exchange_lib import (
    join_report_with_quant_artifacts,
    load_json,
    load_jsonl,
)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--report-json", required=True)
    parser.add_argument("--feedback-jsonl")
    parser.add_argument("--shadow-record-json")
    parser.add_argument("--shadow-summary-json")
    parser.add_argument("--output")
    args = parser.parse_args()

    output = join_report_with_quant_artifacts(
        load_json(args.report_json),
        load_jsonl(args.feedback_jsonl),
        load_json(args.shadow_record_json),
        load_json(args.shadow_summary_json),
    )

    rendered = json.dumps(output, indent=2)
    if args.output:
        output_path = Path(args.output)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(rendered + "\n")
        print(f"WROTE {output_path}")
    else:
        print(rendered)


if __name__ == "__main__":
    main()

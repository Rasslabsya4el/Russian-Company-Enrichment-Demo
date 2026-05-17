from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

if __package__ in {None, ""}:
    sys.path.append(str(Path(__file__).resolve().parents[1]))

from app.llm.replay_benchmark import run_replay_benchmark


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Replay captured site_decision/content_review LLM fixtures against a selected model."
    )
    parser.add_argument("--fixtures", required=True, help="Path to captured fixture JSONL.")
    parser.add_argument("--model", required=True, help="OpenAI model name to replay.")
    parser.add_argument("--output-dir", required=True, help="Directory for benchmark outputs.")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    benchmark = run_replay_benchmark(
        args.fixtures,
        model=args.model,
        output_dir=args.output_dir,
    )
    print(json.dumps(benchmark["summary"], ensure_ascii=False))
    print(f"benchmark_events_jsonl={benchmark['events_path']}")
    print(f"benchmark_summary_json={benchmark['summary_path']}")
    print(f"benchmark_results_json={benchmark['results_path']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

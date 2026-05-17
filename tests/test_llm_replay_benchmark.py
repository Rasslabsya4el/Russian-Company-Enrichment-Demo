from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Any

import pytest

if __package__ in {None, ""}:
    sys.path.append(str(Path(__file__).resolve().parents[1]))

from app.llm.replay_benchmark import (
    infer_replay_stage,
    load_replay_fixtures,
    run_replay_benchmark,
    summarize_benchmark_events,
)


def _fixture_payload(
    *,
    stage: str,
    ordinal: int,
    fixture_hash: str,
) -> dict[str, Any]:
    format_name = "site_match_decision" if stage == "site_decision" else "content_relevance_decision"
    return {
        "stage": stage,
        "ordinal": ordinal,
        "row_index": ordinal,
        "inn": f"770000000{ordinal}",
        "company_name": f"Fixture {ordinal}",
        "url": f"https://example.test/{stage}/{ordinal}",
        "request_body_template": {
            "model": "gpt-5.4-nano",
            "reasoning": {"effort": "none"},
            "text": {"format": {"name": format_name}},
            "input": [
                {
                    "role": "user",
                    "content": [{"type": "input_text", "text": f"fixture-{ordinal}"}],
                }
            ],
        },
        "would_call_in_prod": True,
        "prod_skip_reason": "",
        "trust_state": "trusted",
        "decision_source_context": {"fixture": ordinal},
        "source_run_selection": {"selected_ordinals": [ordinal]},
        "fixture_hash": fixture_hash,
    }


def _write_jsonl(path: Path, rows: list[dict[str, Any]]) -> None:
    with path.open("w", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row, ensure_ascii=False))
            handle.write("\n")


def _site_success_payload() -> dict[str, Any]:
    return {
        "id": "resp_site_1",
        "usage": {"input_tokens": 120, "output_tokens": 42},
        "output": [
            {
                "content": [
                    {
                        "type": "output_text",
                        "parsed": {
                            "belongs_to_company": True,
                            "confidence": 0.82,
                            "reason": "identity match",
                        },
                    }
                ]
            }
        ],
    }


def _content_success_payload() -> dict[str, Any]:
    return {
        "id": "resp_content_1",
        "usage": {"input_tokens": 80, "output_tokens": 25},
        "output": [
            {
                "content": [
                    {
                        "type": "output_text",
                        "parsed": {
                            "relevance_label": "likely_relevant",
                            "confidence": 0.9,
                            "summary": "surplus stock",
                        },
                    }
                ]
            }
        ],
    }


def _non_json_payload() -> dict[str, Any]:
    return {
        "id": "resp_site_2",
        "usage": {"input_tokens": 90, "output_tokens": 12},
        "output": [
            {
                "content": [
                    {
                        "type": "output_text",
                        "text": "still not json",
                    }
                ]
            }
        ],
    }


def test_load_replay_fixtures_reads_jsonl_and_infers_stage(tmp_path: Path) -> None:
    fixtures_path = tmp_path / "site_decision_fixtures.jsonl"
    _write_jsonl(
        fixtures_path,
        [
            _fixture_payload(stage="site_decision", ordinal=1, fixture_hash="fixture-1"),
            _fixture_payload(stage="site_decision", ordinal=2, fixture_hash="fixture-2"),
        ],
    )

    fixtures = load_replay_fixtures(fixtures_path)

    assert len(fixtures) == 2
    assert fixtures[0].fixture_hash == "fixture-1"
    assert fixtures[1].request_body_template["text"]["format"]["name"] == "site_match_decision"
    assert infer_replay_stage(fixtures) == "site_decision"


def test_run_replay_benchmark_writes_outputs_and_aggregates_success_error(tmp_path: Path) -> None:
    fixtures_path = tmp_path / "site_decision_fixtures.jsonl"
    output_dir = tmp_path / "out"
    _write_jsonl(
        fixtures_path,
        [
            _fixture_payload(stage="site_decision", ordinal=1, fixture_hash="fixture-1"),
            _fixture_payload(stage="site_decision", ordinal=2, fixture_hash="fixture-2"),
        ],
    )
    pending_payloads = [_site_success_payload(), _non_json_payload()]

    def sender(body: dict[str, Any]) -> dict[str, Any]:
        assert body["model"] == "gpt-5.4-nano"
        if not pending_payloads:
            raise AssertionError("unexpected extra replay call")
        return pending_payloads.pop(0)

    result = run_replay_benchmark(
        fixtures_path,
        model="gpt-5.4-nano",
        output_dir=output_dir,
        request_sender=sender,
    )

    summary = result["summary"]
    assert summary["stage"] == "site_decision"
    assert summary["model"] == "gpt-5.4-nano"
    assert summary["total_fixtures"] == 2
    assert summary["success_count"] == 1
    assert summary["error_count"] == 1
    assert summary["parser_reason_counts"] == {"parsed_ok": 1, "non_json_text": 1}
    assert summary["total_cost_usd"] == pytest.approx(0.0001095, abs=1e-8)
    assert summary["cost_per_success_usd"] == pytest.approx(0.0001095, abs=1e-8)

    events = [
        json.loads(line)
        for line in (output_dir / "benchmark_events.jsonl").read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    assert [event["status"] for event in events] == ["success", "error"]
    assert events[0]["response_id"] == "resp_site_1"
    assert events[1]["parser_reason"] == "non_json_text"

    results = json.loads((output_dir / "benchmark_results.json").read_text(encoding="utf-8"))
    assert results["summary"]["success_count"] == 1
    assert results["results"][0]["parsed_output"]["belongs_to_company"] is True
    assert results["results"][1]["error"] == "LLM output_text did not contain a JSON object"


def test_unknown_model_marks_cost_summary_unknown() -> None:
    summary = summarize_benchmark_events(
        [
            {
                "status": "success",
                "parser_reason": "parsed_ok",
                "total_cost_usd": None,
                "cost_unknown": True,
            }
        ],
        model="gpt-5.4-unknown",
        stage="content_review",
    )

    assert summary["total_fixtures"] == 1
    assert summary["success_count"] == 1
    assert summary["error_count"] == 0
    assert summary["parser_reason_counts"] == {"parsed_ok": 1}
    assert summary["total_cost_usd"] is None
    assert summary["cost_per_success_usd"] is None
    assert summary["cost_unknown"] is True
    assert summary["model"] == "gpt-5.4-unknown"
    assert summary["stage"] == "content_review"


def test_run_replay_benchmark_supports_content_review_stage(tmp_path: Path) -> None:
    fixtures_path = tmp_path / "content_review_fixtures.jsonl"
    output_dir = tmp_path / "out_content"
    _write_jsonl(
        fixtures_path,
        [_fixture_payload(stage="content_review", ordinal=3, fixture_hash="fixture-3")],
    )

    result = run_replay_benchmark(
        fixtures_path,
        model="gpt-5.4-mini",
        output_dir=output_dir,
        request_sender=lambda body: _content_success_payload(),
    )

    assert result["summary"]["stage"] == "content_review"
    assert result["summary"]["success_count"] == 1
    assert result["results"][0]["parsed_output"]["relevance_label"] == "likely_relevant"

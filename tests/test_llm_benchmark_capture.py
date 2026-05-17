from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any

import pytest

import company_enrichment_core as core
import run_company_enrichment_pipeline as pipeline
from app.llm.benchmark_capture import LLMBenchmarkCaptureConfig, LLMBenchmarkCaptureWriter


class _FakeOpenAIResponse:
    def __init__(self, payload: dict[str, object]) -> None:
        self._payload = payload

    def raise_for_status(self) -> None:
        return None

    def json(self) -> dict[str, object]:
        return self._payload


def _row() -> core.RowInput:
    return core.RowInput(row_index=7, inn="7700000000", company_name="Benchmark Plant")


def _site_context() -> dict[str, Any]:
    return {
        "candidate_site": {
            "url": "https://trusted.example/",
            "final_url": "https://trusted.example/about",
            "title": "Trusted Industrial Plant",
            "description": "Official industrial plant website.",
            "phones": ["+7 495 000-00-01"],
            "emails": ["sales@trusted.example"],
            "addresses": ["Moscow region, Industrial street 7"],
            "fetched_pages": ["https://trusted.example/", "https://trusted.example/about"],
            "text_excerpt": "Industrial production and surplus stock notices.",
        },
        "known_contacts": {
            "phones": ["+7 495 000-00-01"],
            "emails": ["sales@trusted.example"],
            "websites": ["https://trusted.example/"],
            "addresses": ["Moscow region, Industrial street 7"],
        },
        "aggregator_profile": {
            "rusprofile": {
                "status": "ok",
                "company_name_found": "Benchmark Plant LLC",
                "websites": ["https://trusted.example/"],
                "emails": ["sales@trusted.example"],
                "addresses": ["Moscow region, Industrial street 7"],
                "primary_okved": {"code": "25.11", "label": "Manufacture", "display": "Manufacture (25.11)"},
                "additional_okveds": [],
                "snippets": ["trusted.example listed as corporate website"],
            }
        },
        "heuristics": {
            "decision_status": "candidate",
            "authenticity_score": 0.58,
            "identity_score": 0.63,
            "viability_score": 0.72,
            "industrial_score": 0.81,
            "conflict_penalty": 0.06,
            "hard_negative_hits": [],
            "matched_name_tokens": ["benchmark", "plant"],
            "positive_keywords": ["production", "industrial"],
            "negative_keywords": [],
            "flags": {"inn_match": False, "domain_matches_email": True},
        },
        "business_goal": "Need a trustworthy corporate site for this specific company.",
    }


def _record(*, trust_state: str, relevance_label: str = "maybe_relevant", cleaned_text: str = "Selling industrial surplus.") -> core.ContentRecord:
    url = "https://trusted.example/tender-1"
    return core.ContentRecord(
        company_id="7700000000",
        site_id="https://trusted.example/",
        site_url="https://trusted.example/",
        url=url,
        source_type="html",
        source_url_or_file=url,
        section_guess="tenders",
        title="Surplus stock notice",
        text=cleaned_text,
        raw_text=cleaned_text,
        cleaned_text=cleaned_text,
        extraction_method="requests",
        fetch_status="success",
        content_fingerprint=f"fp:{url}",
        relevance_label=relevance_label,
        relevance_score=0.41,
        relevance_reasons=["family:direct_sale:surplus"],
        trace={
            "factory_site_parser": {
                "crawl": {
                    "site_url": "https://trusted.example/",
                    "trust_state": trust_state,
                    "trust_verdict": "uncertain" if trust_state != "trusted" else "strong_match",
                    "trust_summary": "site trust summary",
                }
            }
        },
    )


def _benchmark_capture(
    tmp_path: Path,
    *,
    force_stages: tuple[str, ...] = (),
    capture_only: bool = True,
) -> LLMBenchmarkCaptureWriter:
    return LLMBenchmarkCaptureWriter(
        LLMBenchmarkCaptureConfig(
            capture_dir=tmp_path / "fixtures",
            force_stages=frozenset(force_stages),
            capture_only=capture_only,
            source_run_selection={
                "mode": "ordinals",
                "selected_ordinals": [7],
                "start_from": 7,
                "end_at": 7,
            },
        )
    )


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    return [
        json.loads(line)
        for line in path.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]


def _read_summary(progress_store: core.ProgressStore) -> dict[str, Any]:
    return json.loads(progress_store.summary_json.read_text(encoding="utf-8"))


def test_parse_args_parses_llm_benchmark_flags() -> None:
    args = pipeline.parse_args(
        [
            "--input",
            "input.xlsx",
            "--llm-benchmark-capture-dir",
            "fixtures",
            "--llm-benchmark-force-stages=site_decision,content_review,site_decision",
            "--llm-benchmark-capture-only",
        ]
    )

    assert args.llm_benchmark_capture_dir == "fixtures"
    assert args.llm_benchmark_capture_only is True
    assert args.llm_benchmark_force_stages == frozenset({"site_decision", "content_review"})


def test_parse_args_rejects_benchmark_capture_only_without_dir(capsys: pytest.CaptureFixture[str]) -> None:
    with pytest.raises(SystemExit):
        pipeline.parse_args(["--input", "input.xlsx", "--llm-benchmark-capture-only"])

    assert "--llm-benchmark-capture-dir is required" in capsys.readouterr().err


def test_site_decision_capture_only_exports_fixture_without_live_call(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    def fail_post(*args, **kwargs):  # type: ignore[no-untyped-def]
        raise AssertionError("live OpenAI call is forbidden in capture-only mode")

    monkeypatch.setattr(core.requests, "post", fail_post)

    progress_store = core.ProgressStore(tmp_path / "progress")
    decider = core.OpenAIDecider(
        logging.getLogger("test_site_decision_capture"),
        progress_store,
        benchmark_capture=_benchmark_capture(tmp_path),
    )

    result = decider.decide(_row(), "https://trusted.example/", _site_context())

    assert result is None
    fixture_path = tmp_path / "fixtures" / "site_decision_fixtures.jsonl"
    fixtures = _read_jsonl(fixture_path)
    assert len(fixtures) == 1
    fixture = fixtures[0]
    assert fixture["stage"] == "site_decision"
    assert fixture["ordinal"] == 7
    assert fixture["row_index"] == 7
    assert fixture["inn"] == "7700000000"
    assert fixture["company_name"] == "Benchmark Plant"
    assert fixture["url"] == "https://trusted.example/"
    assert fixture["site_url"] == "https://trusted.example/"
    assert fixture["replayable"] is True
    assert fixture["would_call_in_prod"] is True
    assert fixture["prod_skip_reason"] == ""
    assert fixture["trust_state"] == "candidate"
    assert fixture["benchmark_capture_path"] == "openai_decider.site_decision.capture"
    assert fixture["synthetic_candidate_used"] is False
    assert fixture["forced_harvest_level"] == "none"
    assert fixture["compact_context"]["candidate_site"]["url"] == "https://trusted.example/"
    assert fixture["request_body_template"]["text"]["format"]["name"] == "site_match_decision"
    assert fixture["source_run_selection"]["selected_ordinals"] == [7]
    assert fixture["fixture_hash"]

    summary = _read_summary(progress_store)
    assert summary["captured_site_decision_count"] == 1
    assert summary["captured_site_decision_company_count"] == 1
    assert summary["captured_content_review_count"] == 0
    assert summary["captured_content_review_company_count"] == 0


def test_content_review_forced_capture_exports_site_not_trusted_fixture(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    def fail_post(*args, **kwargs):  # type: ignore[no-untyped-def]
        raise AssertionError("live OpenAI call is forbidden in capture-only mode")

    monkeypatch.setattr(core.requests, "post", fail_post)

    progress_store = core.ProgressStore(tmp_path / "progress")
    decider = core.OpenAIDecider(
        logging.getLogger("test_content_capture"),
        progress_store,
        benchmark_capture=_benchmark_capture(tmp_path, force_stages=("content_review",)),
    )

    result = decider.judge_content_record(_row(), _record(trust_state="ambiguous"), "https://trusted.example/")

    assert result is None
    fixture_path = tmp_path / "fixtures" / "content_review_fixtures.jsonl"
    fixtures = _read_jsonl(fixture_path)
    assert len(fixtures) == 1
    fixture = fixtures[0]
    assert fixture["stage"] == "content_review"
    assert fixture["would_call_in_prod"] is False
    assert fixture["prod_skip_reason"] == "site_not_trusted"
    assert fixture["site_url"] == "https://trusted.example/"
    assert fixture["trust_state"] == "ambiguous"
    assert fixture["benchmark_capture_path"] == "openai_decider.content_review.capture"
    assert fixture["synthetic_candidate_used"] is False
    assert fixture["forced_harvest_level"] == "none"
    assert "benchmark_forced_harvest" not in fixture
    assert fixture["decision_source_context"]["heuristic_relevance_label"] == "maybe_relevant"
    assert fixture["request_body_template"]["text"]["format"]["name"] == "content_relevance_decision"

    summary = _read_summary(progress_store)
    assert summary["captured_content_review_count"] == 1
    assert summary["captured_content_review_company_count"] == 1


def test_benchmark_mode_off_keeps_regular_site_decision_path(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setenv("OPENAI_API_KEY", "test-key")
    calls: list[dict[str, Any]] = []

    def fake_post(url: str, *, headers: dict[str, str], json: dict[str, Any], timeout: int) -> _FakeOpenAIResponse:
        calls.append({"url": url, "headers": headers, "json": json, "timeout": timeout})
        return _FakeOpenAIResponse(
            {
                "usage": {"input_tokens": 18, "output_tokens": 9},
                "output": [
                    {
                        "content": [
                            {
                                "type": "output_text",
                                "parsed": {
                                    "belongs_to_company": True,
                                    "website_type": "official_corporate",
                                    "industrial_relevance": "high",
                                    "confidence": 0.82,
                                    "reason": "Corporate identity markers match.",
                                    "evidence": ["domain match"],
                                    "contradictions": [],
                                },
                            }
                        ]
                    }
                ],
            }
        )

    monkeypatch.setattr(core.requests, "post", fake_post)

    progress_store = core.ProgressStore(tmp_path / "progress")
    decider = core.OpenAIDecider(logging.getLogger("test_regular_path"), progress_store)

    result = decider.decide(_row(), "https://trusted.example/", _site_context())

    assert result is not None
    assert result["website_type"] == "official_corporate"
    assert len(calls) == 1
    summary = _read_summary(progress_store)
    assert summary["captured_site_decision_count"] == 0
    assert summary["captured_content_review_count"] == 0

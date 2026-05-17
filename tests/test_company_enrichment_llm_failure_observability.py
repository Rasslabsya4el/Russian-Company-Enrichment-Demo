from __future__ import annotations

import json
import logging
from collections.abc import Callable
from pathlib import Path
from typing import Any

import pytest

import company_enrichment_core as core


class _FakeOpenAIResponse:
    def __init__(self, payload: dict[str, object]) -> None:
        self._payload = payload

    def raise_for_status(self) -> None:
        return None

    def json(self) -> dict[str, object]:
        return self._payload


Runner = Callable[[pytest.MonkeyPatch, Path, dict[str, object]], tuple[dict[str, Any] | None, list[dict[str, Any]]]]


def _row() -> core.RowInput:
    return core.RowInput(row_index=1, inn="7700000000", company_name="Parser Plant")


def _record(url: str) -> core.ContentRecord:
    return core.ContentRecord(
        company_id="7700000000",
        site_id="https://trusted.example/",
        site_url="https://trusted.example/",
        url=url,
        source_type="html",
        source_url_or_file=url,
        section_guess="tenders",
        title="Surplus stock notice",
        text="Selling industrial surplus.",
        raw_text="Selling industrial surplus.",
        cleaned_text="Selling industrial surplus.",
        extraction_method="requests",
        fetch_status="success",
        content_fingerprint=f"fp:{url}",
        relevance_label="maybe_relevant",
        relevance_score=0.41,
        relevance_reasons=["family:direct_sale:surplus"],
        trace={
            "factory_site_parser": {
                "crawl": {
                    "site_url": "https://trusted.example/",
                    "trust_state": "trusted",
                    "trust_verdict": "strong_match",
                    "trust_summary": "trusted site",
                }
            }
        },
    )


def _progress_store(tmp_path: Path) -> core.ProgressStore:
    return core.ProgressStore(tmp_path / "progress")


def _read_events(progress_store: core.ProgressStore) -> list[dict[str, Any]]:
    if not progress_store.events_jsonl.exists():
        return []
    return [
        json.loads(line)
        for line in progress_store.events_jsonl.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]


def _install_fake_post(monkeypatch: pytest.MonkeyPatch, payload: dict[str, object]) -> None:
    def fake_post(url: str, *, headers: dict[str, str], json: dict[str, object], timeout: int) -> _FakeOpenAIResponse:
        return _FakeOpenAIResponse(payload)

    monkeypatch.setattr(core.requests, "post", fake_post)


def _configure_llm_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("OPENAI_API_KEY", "test-key")
    monkeypatch.setenv("OPENAI_MAX_CALLS_PER_RUN", "10")
    monkeypatch.setenv("OPENAI_MAX_CALLS_PER_COMPANY", "5")
    monkeypatch.setenv("OPENAI_SITE_DECISION_MODEL", "gpt-5.4-nano")
    monkeypatch.setenv("OPENAI_CONTENT_REVIEW_MODEL", "gpt-5.4-nano")
    monkeypatch.setenv("OPENAI_CONTENT_REVIEW_FALLBACK_MODEL", "gpt-5.4-mini")


def _run_site_decision(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    payload: dict[str, object],
) -> tuple[dict[str, Any] | None, list[dict[str, Any]]]:
    _configure_llm_env(monkeypatch)
    _install_fake_post(monkeypatch, payload)
    progress_store = _progress_store(tmp_path)
    decider = core.OpenAIDecider(logging.getLogger("test_site_observability"), progress_store)
    result = decider.decide(_row(), "https://trusted.example/", {"homepage_title": "Trusted plant"})
    return result, _read_events(progress_store)


def _run_content_review(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    payload: dict[str, object],
) -> tuple[dict[str, Any] | None, list[dict[str, Any]]]:
    _configure_llm_env(monkeypatch)
    _install_fake_post(monkeypatch, payload)
    progress_store = _progress_store(tmp_path)
    decider = core.OpenAIDecider(logging.getLogger("test_content_observability"), progress_store)
    result = decider.judge_content_record(_row(), _record("https://trusted.example/tender-1"), "https://trusted.example/")
    return result, _read_events(progress_store)


RUNNERS = [
    pytest.param(_run_site_decision, "llm_error", "llm_decision", "site", id="site"),
    pytest.param(_run_content_review, "llm_content_error", "llm_content_judgement", "content", id="content"),
]


def _assert_compact_error_event(event: dict[str, Any], *, event_type: str, parser_reason: str) -> None:
    assert event["type"] == event_type
    assert event["parser_reason"] == parser_reason
    assert "output" not in event
    assert "incomplete_details" not in event


def _assert_incomplete_forensics(event: dict[str, Any], *, event_type: str) -> None:
    _assert_compact_error_event(
        event,
        event_type=event_type,
        parser_reason="incomplete_max_output_tokens",
    )
    assert event["reason"] == "incomplete_max_output_tokens"
    assert event["response_status"] == "incomplete"
    assert event["incomplete_reason"] == "max_output_tokens"
    assert event["has_output"] is True
    assert event["has_output_text"] is True
    assert event["has_parsed"] is False
    assert event["content_types"] == ["output_text"]
    assert event["has_refusal"] is False


def _assert_content_fallback_pair(events: list[dict[str, Any]], *, parser_reason: str) -> None:
    assert len(events) == 2
    assert events[0]["type"] == "llm_content_error"
    assert events[0]["attempt_no"] == 1
    assert events[0]["attempt_kind"] == "primary"
    assert events[0]["fallback_trigger"] == parser_reason
    assert events[0]["model"] == "gpt-5.4-nano"
    assert events[1]["type"] == "llm_content_error"
    assert events[1]["attempt_no"] == 2
    assert events[1]["attempt_kind"] == "fallback"
    assert events[1]["fallback_trigger"] == parser_reason
    assert events[1]["model"] == "gpt-5.4-mini"


@pytest.mark.parametrize(("runner", "event_type", "_success_event_type", "stage"), RUNNERS)
def test_refusal_sets_has_refusal(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    runner: Runner,
    event_type: str,
    _success_event_type: str,
    stage: str,
) -> None:
    result, events = runner(
        monkeypatch,
        tmp_path,
        {
            "output": [
                {
                    "content": [
                        {"type": "refusal", "refusal": "Cannot comply"},
                    ]
                }
            ]
        },
    )

    assert result is None
    if stage == "content":
        _assert_content_fallback_pair(events, parser_reason="refusal")
        for event in events:
            _assert_compact_error_event(event, event_type=event_type, parser_reason="refusal")
            assert event["has_output"] is True
            assert event["has_output_text"] is False
            assert event["has_parsed"] is False
            assert event["content_types"] == ["refusal"]
            assert event["has_refusal"] is True
            assert event["response_status"] is None
            assert event["incomplete_reason"] is None
    else:
        assert len(events) == 1
        event = events[0]
        _assert_compact_error_event(event, event_type=event_type, parser_reason="refusal")
        assert event["has_output"] is True
        assert event["has_output_text"] is False
        assert event["has_parsed"] is False
        assert event["content_types"] == ["refusal"]
        assert event["has_refusal"] is True
        assert event["response_status"] is None
        assert event["incomplete_reason"] is None


def test_site_decision_incomplete_retry_emits_both_error_events(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    result, events = _run_site_decision(
        monkeypatch,
        tmp_path,
        {
            "status": "incomplete",
            "incomplete_details": {"reason": "max_output_tokens"},
            "output": [
                {
                    "content": [
                        {"type": "output_text", "text": '{"partial": true'},
                    ]
                }
            ],
        },
    )

    assert result is None
    assert len(events) == 2
    _assert_incomplete_forensics(events[0], event_type="llm_error")
    _assert_incomplete_forensics(events[1], event_type="llm_error")
    assert events[0]["attempt_no"] == 1
    assert events[0]["attempt_kind"] == "primary"
    assert events[0]["retry_trigger"] == "incomplete_max_output_tokens"
    assert events[1]["attempt_no"] == 2
    assert events[1]["attempt_kind"] == "retry"
    assert events[1]["retry_trigger"] == "incomplete_max_output_tokens"


def test_content_incomplete_eligible_reason_emits_primary_and_fallback_error_events(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    result, events = _run_content_review(
        monkeypatch,
        tmp_path,
        {
            "status": "incomplete",
            "incomplete_details": {"reason": "max_output_tokens"},
            "output": [
                {
                    "content": [
                        {"type": "output_text", "text": '{"partial": true'},
                    ]
                }
            ],
        },
    )

    assert result is None
    _assert_content_fallback_pair(events, parser_reason="incomplete_max_output_tokens")
    _assert_incomplete_forensics(events[0], event_type="llm_content_error")
    _assert_incomplete_forensics(events[1], event_type="llm_content_error")


@pytest.mark.parametrize(("runner", "event_type", "_success_event_type", "stage"), RUNNERS)
def test_output_text_only_sets_has_output_text_without_parsed(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    runner: Runner,
    event_type: str,
    _success_event_type: str,
    stage: str,
) -> None:
    result, events = runner(
        monkeypatch,
        tmp_path,
        {
            "output": [
                {
                    "content": [
                        {"type": "output_text", "text": "plain text instead of json"},
                    ]
                }
            ]
        },
    )

    assert result is None
    if stage == "content":
        _assert_content_fallback_pair(events, parser_reason="non_json_text")
        for event in events:
            _assert_compact_error_event(event, event_type=event_type, parser_reason="non_json_text")
            assert event["has_output"] is True
            assert event["has_output_text"] is True
            assert event["has_parsed"] is False
            assert event["content_types"] == ["output_text"]
            assert event["has_refusal"] is False
            assert event["response_status"] is None
            assert event["incomplete_reason"] is None
    else:
        assert len(events) == 1
        event = events[0]
        _assert_compact_error_event(event, event_type=event_type, parser_reason="non_json_text")
        assert event["has_output"] is True
        assert event["has_output_text"] is True
        assert event["has_parsed"] is False
        assert event["content_types"] == ["output_text"]
        assert event["has_refusal"] is False
        assert event["response_status"] is None
        assert event["incomplete_reason"] is None


def _parsed_ok_payload(stage: str) -> dict[str, object]:
    if stage == "site":
        parsed = {
            "belongs_to_company": True,
            "website_type": "official_corporate",
            "industrial_relevance": "high",
            "confidence": 0.91,
            "reason": "domain and contacts match",
            "evidence": ["same domain"],
            "contradictions": [],
        }
    else:
        parsed = {
            "relevance_label": "likely_relevant",
            "lead_type": "direct_sale",
            "confidence": 0.88,
            "summary": "surplus sale announcement",
            "evidence": ["surplus stock"],
        }
    return {
        "usage": {"input_tokens": 12, "output_tokens": 7},
        "output": [
            {
                "content": [
                    {
                        "type": "output_text",
                        "text": json.dumps(parsed, ensure_ascii=False),
                        "parsed": parsed,
                    }
                ]
            }
        ],
    }


@pytest.mark.parametrize(("runner", "error_event_type", "success_event_type", "stage"), RUNNERS)
def test_parsed_ok_does_not_emit_error_event(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    runner: Runner,
    error_event_type: str,
    success_event_type: str,
    stage: str,
) -> None:
    result, events = runner(monkeypatch, tmp_path, _parsed_ok_payload(stage))

    assert result is not None
    assert len(events) == 1
    assert events[0]["type"] == success_event_type
    assert events[0]["parser_reason"] == "parsed_ok"
    assert all(event["type"] != error_event_type for event in events)

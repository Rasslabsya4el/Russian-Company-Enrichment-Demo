import json
import logging

import pytest

import company_enrichment_core as core
from app.llm.content_review_compaction import (
    DEFAULT_CONTENT_REVIEW_EXCERPT_CHARS,
    build_content_review_excerpt,
)


class _FakeOpenAIResponse:
    def __init__(self, payload: dict[str, object]) -> None:
        self._payload = payload

    def raise_for_status(self) -> None:
        return None

    def json(self) -> dict[str, object]:
        return self._payload


def _row() -> core.RowInput:
    return core.RowInput(row_index=1, inn="7700000000", company_name="Signal Plant")


def _record(cleaned_text: str) -> core.ContentRecord:
    return core.ContentRecord(
        company_id="7700000000",
        site_id="https://trusted.example/",
        site_url="https://trusted.example/",
        url="https://trusted.example/about/surplus",
        source_type="html",
        source_url_or_file="https://trusted.example/about/surplus",
        section_guess="about",
        title="Industrial surplus equipment sale",
        text=cleaned_text,
        raw_text=cleaned_text,
        cleaned_text=cleaned_text,
        extraction_method="requests",
        fetch_status="success",
        content_fingerprint="fp:oversized",
        relevance_label="maybe_relevant",
        relevance_score=0.42,
        relevance_reasons=["family:direct_sale:surplus", "family:surplus/realization:оборудован"],
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


def _oversized_text() -> str:
    noisy_nav = "Home | About | Products | Services | Contacts"
    corporate_filler = "Company overview and corporate history for industrial operations."
    return "\n".join(
        [
            noisy_nav,
            "Cookie settings and privacy policy",
            noisy_nav,
            "Industrial surplus equipment sale",
            "Реализация невостребованных ТМЦ и промышленного оборудования",
            "Lot: CNC lathe model 16K20, warehouse stock, 2 units",
            "Price: 540000 RUB with VAT",
            "Specification: spindle 1600 rpm, 11 kW, weight 4.2 tons",
            "Contact: sales@example.com, +7 (800) 555-35-35",
            "All rights reserved",
        ]
        + [corporate_filler for _ in range(80)]
    )


def _read_events(progress_store: core.ProgressStore) -> list[dict[str, object]]:
    if not progress_store.events_jsonl.exists():
        return []
    return [
        json.loads(line)
        for line in progress_store.events_jsonl.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]


def test_build_content_review_excerpt_preserves_useful_signals_and_drops_boilerplate() -> None:
    excerpt = build_content_review_excerpt(
        title="Industrial surplus equipment sale",
        cleaned_text=_oversized_text(),
        max_chars=420,
    )

    assert excerpt.compacted is True
    assert excerpt.final_length <= 420
    assert "Industrial surplus equipment sale" in excerpt.text
    assert "Реализация невостребованных ТМЦ" in excerpt.text
    assert "Price: 540000 RUB" in excerpt.text
    assert "sales@example.com" in excerpt.text
    assert "Cookie settings" not in excerpt.text
    assert "All rights reserved" not in excerpt.text
    assert "Home | About | Products | Services | Contacts" not in excerpt.text


def test_content_review_uses_compacted_excerpt_and_logs_observability(monkeypatch: pytest.MonkeyPatch, tmp_path) -> None:
    monkeypatch.setenv("OPENAI_API_KEY", "test-key")
    monkeypatch.setenv("OPENAI_MAX_CALLS_PER_RUN", "10")
    monkeypatch.setenv("OPENAI_MAX_CALLS_PER_COMPANY", "5")

    captured_body: dict[str, object] = {}

    def fake_post(url: str, *, headers: dict[str, str], json: dict[str, object], timeout: int) -> _FakeOpenAIResponse:
        captured_body["json"] = json
        return _FakeOpenAIResponse(
            {
                "usage": {"input_tokens": 18, "output_tokens": 11},
                "output": [
                    {
                        "content": [
                            {
                                "type": "output_text",
                                "text": "",
                                "parsed": {
                                    "relevance_label": "likely_relevant",
                                    "lead_type": "direct_sale",
                                    "confidence": 0.88,
                                    "summary": "surplus sale announcement",
                                    "evidence": ["surplus stock", "price"],
                                },
                            }
                        ]
                    }
                ],
            }
        )

    monkeypatch.setattr(core.requests, "post", fake_post)

    progress_store = core.ProgressStore(tmp_path / "progress")
    decider = core.OpenAIDecider(logging.getLogger("test_compaction_path"), progress_store)
    record = _record(_oversized_text())

    result = decider.judge_content_record(_row(), record, "https://trusted.example/")

    assert result is not None

    request_body = captured_body["json"]
    assert isinstance(request_body, dict)
    user_message = request_body["input"][1]["content"][0]["text"]
    assert isinstance(user_message, str)
    prompt_payload = json.loads(user_message)
    excerpt = prompt_payload["context"]["record"]["text_excerpt"]
    assert isinstance(excerpt, str)
    assert len(excerpt) <= DEFAULT_CONTENT_REVIEW_EXCERPT_CHARS
    assert excerpt == build_content_review_excerpt(
        title=record.title,
        cleaned_text=record.cleaned_text,
        max_chars=DEFAULT_CONTENT_REVIEW_EXCERPT_CHARS,
    ).text
    assert excerpt != core.compact_text(record.cleaned_text, 2200)
    assert "Cookie settings" not in excerpt
    assert "sales@example.com" in excerpt

    assert record.trace["llm_review"]["excerpt_compacted"] is True
    assert record.trace["llm_review"]["excerpt_chars"] == len(excerpt)
    assert record.trace["llm_review"]["excerpt_source_chars"] > len(excerpt)

    events = _read_events(progress_store)
    assert len(events) == 1
    assert events[0]["type"] == "llm_content_judgement"
    assert events[0]["excerpt_compacted"] is True
    assert events[0]["excerpt_chars"] == len(excerpt)
    assert events[0]["excerpt_source_chars"] > len(excerpt)


def test_content_review_incomplete_error_keeps_compaction_observability(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    monkeypatch.setenv("OPENAI_API_KEY", "test-key")
    monkeypatch.setenv("OPENAI_MAX_CALLS_PER_RUN", "10")
    monkeypatch.setenv("OPENAI_MAX_CALLS_PER_COMPANY", "5")

    captured_bodies: list[dict[str, object]] = []

    def fake_post(url: str, *, headers: dict[str, str], json: dict[str, object], timeout: int) -> _FakeOpenAIResponse:
        captured_bodies.append(json)
        return _FakeOpenAIResponse(
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
            }
        )

    monkeypatch.setattr(core.requests, "post", fake_post)

    progress_store = core.ProgressStore(tmp_path / "progress")
    decider = core.OpenAIDecider(logging.getLogger("test_compaction_incomplete"), progress_store)
    record = _record(_oversized_text())

    result = decider.judge_content_record(_row(), record, "https://trusted.example/")

    assert result is None

    assert len(captured_bodies) == 2
    excerpts: list[str] = []
    for request_body in captured_bodies:
        assert isinstance(request_body, dict)
        user_message = request_body["input"][1]["content"][0]["text"]
        assert isinstance(user_message, str)
        prompt_payload = json.loads(user_message)
        excerpt = prompt_payload["context"]["record"]["text_excerpt"]
        assert isinstance(excerpt, str)
        assert len(excerpt) <= DEFAULT_CONTENT_REVIEW_EXCERPT_CHARS
        excerpts.append(excerpt)
    assert excerpts[0] == excerpts[1]
    excerpt = excerpts[0]

    events = _read_events(progress_store)
    assert len(events) == 2
    assert events[0]["type"] == "llm_content_error"
    assert events[0]["reason"] == "incomplete_max_output_tokens"
    assert events[0]["attempt_kind"] == "primary"
    assert events[0]["model"] == "gpt-5.4-nano"
    assert events[1]["type"] == "llm_content_error"
    assert events[1]["reason"] == "incomplete_max_output_tokens"
    assert events[1]["attempt_kind"] == "fallback"
    assert events[1]["model"] == "gpt-5.4-mini"
    for event in events:
        assert event["excerpt_compacted"] is True
        assert event["excerpt_chars"] == len(excerpt)
        assert event["excerpt_source_chars"] > len(excerpt)

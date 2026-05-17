from __future__ import annotations

import json
import logging

import company_enrichment_core as core


class _FakeOpenAIResponse:
    def __init__(self, payload: dict[str, object]) -> None:
        self._payload = payload

    def raise_for_status(self) -> None:
        return None

    def json(self) -> dict[str, object]:
        return self._payload


def _row() -> core.RowInput:
    return core.RowInput(row_index=1, inn="7700000000", company_name="Budgeted Plant")


def _record(*, url: str, trust_state: str, trust_verdict: str = "", score: float = 0.41) -> core.ContentRecord:
    return core.ContentRecord(
        company_id="7700000000",
        site_id="https://trusted.example/",
        site_url="https://trusted.example/",
        url=url,
        source_type="html",
        source_url_or_file=url,
        section_guess="tenders",
        title="Тендер на лом",
        text="Продажа лома и неликвидов.",
        raw_text="Продажа лома и неликвидов.",
        cleaned_text="Продажа лома и неликвидов.",
        extraction_method="requests",
        fetch_status="success",
        content_fingerprint=f"fp:{url}",
        relevance_label="maybe_relevant",
        relevance_score=score,
        relevance_reasons=["family:direct_sale:лом"],
        trace={
            "factory_site_parser": {
                "crawl": {
                    "site_url": "https://trusted.example/",
                    "trust_state": trust_state,
                    "trust_verdict": trust_verdict,
                    "trust_summary": "site match summary",
                }
            }
        },
    )


def _progress_store(tmp_path) -> core.ProgressStore:
    return core.ProgressStore(tmp_path / "progress")


def _read_events(progress_store: core.ProgressStore) -> list[dict[str, object]]:
    if not progress_store.events_jsonl.exists():
        return []
    return [
        json.loads(line)
        for line in progress_store.events_jsonl.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]


def test_per_company_cap_limits_llm_calls(monkeypatch, tmp_path) -> None:
    monkeypatch.setenv("OPENAI_API_KEY", "test-key")
    monkeypatch.setenv("OPENAI_MAX_CALLS_PER_RUN", "10")
    monkeypatch.setenv("OPENAI_MAX_CALLS_PER_COMPANY", "1")

    calls: list[dict[str, object]] = []

    def fake_post(url: str, *, headers: dict[str, str], json: dict[str, object], timeout: int) -> _FakeOpenAIResponse:
        calls.append({"url": url, "headers": headers, "json": json, "timeout": timeout})
        return _FakeOpenAIResponse(
            {
                "output": [
                    {
                        "content": [
                            {
                                "type": "output_text",
                                "text": "",
                                "parsed": {
                                    "relevance_label": "likely_relevant",
                                    "lead_type": "direct_sale",
                                    "confidence": 0.87,
                                    "summary": "Найден сигнал продажи промышленного актива.",
                                    "evidence": ["продажа", "лом"],
                                },
                            }
                        ]
                    }
                ],
                "usage": {"input_tokens": 11, "output_tokens": 9},
            }
        )

    monkeypatch.setattr(core.requests, "post", fake_post)

    progress_store = _progress_store(tmp_path)
    decider = core.OpenAIDecider(logging.getLogger("test_company_cap"), progress_store)
    row = _row()
    first = _record(url="https://trusted.example/tender-1", trust_state="trusted", trust_verdict="strong_match")
    second = _record(url="https://trusted.example/tender-2", trust_state="trusted", trust_verdict="strong_match")

    first_result = decider.judge_content_record(row, first, "https://trusted.example/")
    second_result = decider.judge_content_record(row, second, "https://trusted.example/")

    assert first_result is not None
    assert second_result is None
    assert len(calls) == 1
    assert first.trace["llm_review"]["status"] == "completed"
    assert second.trace["llm_review"]["status"] == "skipped"
    assert second.trace["llm_review"]["reason"] == "company_cap_exhausted"
    assert any("fallback=maybe_relevant/0.41" in note for note in second.notes)

    events = _read_events(progress_store)
    assert [event["type"] for event in events] == ["llm_content_judgement", "llm_skip"]
    assert events[1]["stage"] == "content_review"
    assert events[1]["reason"] == "company_cap_exhausted"


def test_non_trusted_sites_skip_content_review_llm(monkeypatch, tmp_path) -> None:
    monkeypatch.setenv("OPENAI_API_KEY", "test-key")
    monkeypatch.setenv("OPENAI_MAX_CALLS_PER_RUN", "10")
    monkeypatch.setenv("OPENAI_MAX_CALLS_PER_COMPANY", "5")

    calls: list[dict[str, object]] = []

    def fake_post(url: str, *, headers: dict[str, str], json: dict[str, object], timeout: int) -> _FakeOpenAIResponse:
        calls.append({"url": url, "headers": headers, "json": json, "timeout": timeout})
        return _FakeOpenAIResponse({"output": []})

    monkeypatch.setattr(core.requests, "post", fake_post)

    progress_store = _progress_store(tmp_path)
    decider = core.OpenAIDecider(logging.getLogger("test_trust_gate"), progress_store)
    row = _row()
    record = _record(url="https://trusted.example/tender-ambiguous", trust_state="ambiguous", trust_verdict="uncertain")

    result = decider.judge_content_record(row, record, "https://trusted.example/")

    assert result is None
    assert calls == []
    assert record.trace["llm_review"]["status"] == "skipped"
    assert record.trace["llm_review"]["reason"] == "site_not_trusted"
    assert record.trace["llm_review"]["site_trust_state"] == "ambiguous"
    assert any("trust_state=ambiguous" in note for note in record.notes)

    events = _read_events(progress_store)
    assert len(events) == 1
    assert events[0]["type"] == "llm_skip"
    assert events[0]["stage"] == "content_review"
    assert events[0]["reason"] == "site_not_trusted"

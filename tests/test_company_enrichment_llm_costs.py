from __future__ import annotations

import json
import logging
from typing import Any

import pytest

import company_enrichment_core as core
from app.llm.pricing import calculate_usage_cost_usd


class _FakeOpenAIResponse:
    def __init__(self, payload: dict[str, object]) -> None:
        self._payload = payload

    def raise_for_status(self) -> None:
        return None

    def json(self) -> dict[str, object]:
        return self._payload


def _row() -> core.RowInput:
    return core.RowInput(row_index=1, inn="7700000000", company_name="Costed Plant")


def _record(url: str) -> core.ContentRecord:
    return core.ContentRecord(
        company_id="7700000000",
        site_id="https://trusted.example/",
        site_url="https://trusted.example/",
        url=url,
        source_type="html",
        source_url_or_file=url,
        section_guess="tenders",
        title="Industrial surplus sale",
        text="Selling industrial surplus stock.",
        raw_text="Selling industrial surplus stock.",
        cleaned_text="Selling industrial surplus stock.",
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


def _progress_store(tmp_path) -> core.ProgressStore:
    return core.ProgressStore(tmp_path / "progress")


def _read_events(progress_store: core.ProgressStore) -> list[dict[str, Any]]:
    if not progress_store.events_jsonl.exists():
        return []
    return [
        json.loads(line)
        for line in progress_store.events_jsonl.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]


def _read_summary(progress_store: core.ProgressStore) -> dict[str, Any]:
    if not progress_store.summary_json.exists():
        return {}
    return json.loads(progress_store.summary_json.read_text(encoding="utf-8"))


def _configure_llm_env(
    monkeypatch: pytest.MonkeyPatch,
    *,
    site_decision_model: str = "gpt-5.4-nano",
    content_review_model: str = "gpt-5.4-nano",
    content_review_fallback_model: str = "gpt-5.4-mini",
    max_calls_per_company: str = "5",
) -> None:
    monkeypatch.setenv("OPENAI_API_KEY", "test-key")
    monkeypatch.setenv("OPENAI_SITE_DECISION_MODEL", site_decision_model)
    monkeypatch.setenv("OPENAI_CONTENT_REVIEW_MODEL", content_review_model)
    monkeypatch.setenv("OPENAI_CONTENT_REVIEW_FALLBACK_MODEL", content_review_fallback_model)
    monkeypatch.setenv("OPENAI_MAX_CALLS_PER_RUN", "10")
    monkeypatch.setenv("OPENAI_MAX_CALLS_PER_COMPANY", max_calls_per_company)


def _install_fake_post_sequence(
    monkeypatch: pytest.MonkeyPatch,
    payloads: list[dict[str, object]],
) -> None:
    pending_payloads = list(payloads)

    def fake_post(url: str, *, headers: dict[str, str], json: dict[str, object], timeout: int) -> _FakeOpenAIResponse:
        if not pending_payloads:
            raise AssertionError("Unexpected extra OpenAI call")
        return _FakeOpenAIResponse(pending_payloads.pop(0))

    monkeypatch.setattr(core.requests, "post", fake_post)


def _site_refusal_payload() -> dict[str, object]:
    return {
        "usage": {"input_tokens": 120, "output_tokens": 42},
        "output": [
            {
                "content": [
                    {"type": "refusal", "refusal": "Cannot comply"},
                ]
            }
        ],
    }


def _content_success_payload() -> dict[str, object]:
    return {
        "usage": {"input_tokens": 80, "output_tokens": 25},
        "output": [
            {
                "content": [
                    {
                        "type": "output_text",
                        "parsed": {
                            "relevance_label": "likely_relevant",
                            "lead_type": "direct_sale",
                            "confidence": 0.82,
                            "summary": "Industrial surplus sale.",
                            "evidence": ["surplus stock"],
                        },
                    }
                ]
            }
        ],
    }


def _content_refusal_payload() -> dict[str, object]:
    return {
        "usage": {"input_tokens": 90, "output_tokens": 12},
        "output": [
            {
                "content": [
                    {"type": "refusal", "refusal": "Cannot comply"},
                ]
            }
        ],
    }


def test_calculate_usage_cost_usd_known_model() -> None:
    cost = calculate_usage_cost_usd("gpt-5.4-nano", input_tokens=120, output_tokens=42)

    assert cost.cost_unknown is False
    assert cost.input_tokens == 120
    assert cost.output_tokens == 42
    assert cost.input_cost_usd == pytest.approx(0.000024, abs=1e-8)
    assert cost.output_cost_usd == pytest.approx(0.0000525, abs=1e-8)
    assert cost.total_cost_usd == pytest.approx(0.0000765, abs=1e-8)


def test_calculate_usage_cost_usd_unknown_model() -> None:
    cost = calculate_usage_cost_usd("gpt-5.4-unknown", input_tokens=120, output_tokens=42)

    assert cost.cost_unknown is True
    assert cost.input_tokens == 120
    assert cost.output_tokens == 42
    assert cost.input_cost_usd is None
    assert cost.output_cost_usd is None
    assert cost.total_cost_usd is None


def test_error_event_includes_cost_fields_and_summary(monkeypatch: pytest.MonkeyPatch, tmp_path) -> None:
    _configure_llm_env(monkeypatch, site_decision_model="gpt-5.4-nano")
    _install_fake_post_sequence(monkeypatch, [_site_refusal_payload()])

    progress_store = _progress_store(tmp_path)
    decider = core.OpenAIDecider(logging.getLogger("test_llm_cost_error"), progress_store)

    result = decider.decide(_row(), "https://trusted.example/", {"homepage_title": "Trusted plant"})

    assert result is None
    events = _read_events(progress_store)
    assert len(events) == 1
    event = events[0]
    assert event["type"] == "llm_error"
    assert event["stage"] == "site_decision"
    assert event["model"] == "gpt-5.4-nano"
    assert event["input_tokens"] == 120
    assert event["output_tokens"] == 42
    assert event["input_cost_usd"] == pytest.approx(0.000024, abs=1e-8)
    assert event["output_cost_usd"] == pytest.approx(0.0000525, abs=1e-8)
    assert event["total_cost_usd"] == pytest.approx(0.0000765, abs=1e-8)
    assert event["company_cost_usd_cumulative"] == pytest.approx(0.0000765, abs=1e-8)
    assert event["run_cost_usd_cumulative"] == pytest.approx(0.0000765, abs=1e-8)

    summary = _read_summary(progress_store)
    assert summary["llm_total_cost_usd"] == pytest.approx(0.0000765, abs=1e-8)
    assert summary["llm_cost_by_stage"]["site_decision"] == pytest.approx(0.0000765, abs=1e-8)
    assert summary["llm_cost_by_model"]["gpt-5.4-nano"] == pytest.approx(0.0000765, abs=1e-8)


def test_content_review_fallback_uses_fallback_model_and_keeps_cumulative_costs(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    _configure_llm_env(
        monkeypatch,
        content_review_model="gpt-5.4-nano",
        content_review_fallback_model="gpt-5.4-mini",
    )
    _install_fake_post_sequence(monkeypatch, [_content_refusal_payload(), _content_success_payload()])

    progress_store = _progress_store(tmp_path)
    decider = core.OpenAIDecider(logging.getLogger("test_llm_cost_fallback"), progress_store)

    result = decider.judge_content_record(_row(), _record("https://trusted.example/tender-1"), "https://trusted.example/")

    assert result is not None
    events = _read_events(progress_store)
    assert [event["type"] for event in events] == ["llm_content_error", "llm_content_judgement"]

    primary_cost = calculate_usage_cost_usd("gpt-5.4-nano", input_tokens=90, output_tokens=12)
    fallback_cost = calculate_usage_cost_usd("gpt-5.4-mini", input_tokens=80, output_tokens=25)
    cumulative_total = (primary_cost.total_cost_usd or 0.0) + (fallback_cost.total_cost_usd or 0.0)

    primary_event = events[0]
    assert primary_event["stage"] == "content_review"
    assert primary_event["model"] == "gpt-5.4-nano"
    assert primary_event["attempt_no"] == 1
    assert primary_event["attempt_kind"] == "primary"
    assert primary_event["fallback_trigger"] == "refusal"
    assert primary_event["total_cost_usd"] == pytest.approx(primary_cost.total_cost_usd or 0.0, abs=1e-8)
    assert primary_event["company_cost_usd_cumulative"] == pytest.approx(primary_cost.total_cost_usd or 0.0, abs=1e-8)
    assert primary_event["run_cost_usd_cumulative"] == pytest.approx(primary_cost.total_cost_usd or 0.0, abs=1e-8)

    fallback_event = events[1]
    assert fallback_event["stage"] == "content_review"
    assert fallback_event["model"] == "gpt-5.4-mini"
    assert fallback_event["attempt_no"] == 2
    assert fallback_event["attempt_kind"] == "fallback"
    assert fallback_event["fallback_trigger"] == "refusal"
    assert fallback_event["total_cost_usd"] == pytest.approx(fallback_cost.total_cost_usd or 0.0, abs=1e-8)
    assert fallback_event["company_cost_usd_cumulative"] == pytest.approx(cumulative_total, abs=1e-8)
    assert fallback_event["run_cost_usd_cumulative"] == pytest.approx(cumulative_total, abs=1e-8)

    summary = _read_summary(progress_store)
    assert summary["llm_total_cost_usd"] == pytest.approx(cumulative_total, abs=1e-8)
    assert summary["llm_cost_by_stage"]["content_review"] == pytest.approx(cumulative_total, abs=1e-8)
    assert summary["llm_cost_by_model"]["gpt-5.4-nano"] == pytest.approx(primary_cost.total_cost_usd or 0.0, abs=1e-8)
    assert summary["llm_cost_by_model"]["gpt-5.4-mini"] == pytest.approx(fallback_cost.total_cost_usd or 0.0, abs=1e-8)


def test_success_and_skip_events_keep_cumulative_costs(monkeypatch: pytest.MonkeyPatch, tmp_path) -> None:
    _configure_llm_env(monkeypatch, content_review_model="gpt-5.4-mini", max_calls_per_company="1")
    _install_fake_post_sequence(monkeypatch, [_content_success_payload()])

    progress_store = _progress_store(tmp_path)
    decider = core.OpenAIDecider(logging.getLogger("test_llm_cost_success_skip"), progress_store)
    row = _row()

    first_result = decider.judge_content_record(row, _record("https://trusted.example/tender-1"), "https://trusted.example/")
    second_result = decider.judge_content_record(row, _record("https://trusted.example/tender-2"), "https://trusted.example/")

    assert first_result is not None
    assert second_result is None

    expected_total_cost = 0.0001725
    events = _read_events(progress_store)
    assert [event["type"] for event in events] == ["llm_content_judgement", "llm_skip"]

    success_event = events[0]
    assert success_event["stage"] == "content_review"
    assert success_event["model"] == "gpt-5.4-mini"
    assert success_event["input_tokens"] == 80
    assert success_event["output_tokens"] == 25
    assert success_event["input_cost_usd"] == pytest.approx(0.00006, abs=1e-8)
    assert success_event["output_cost_usd"] == pytest.approx(0.0001125, abs=1e-8)
    assert success_event["total_cost_usd"] == pytest.approx(expected_total_cost, abs=1e-8)
    assert success_event["company_cost_usd_cumulative"] == pytest.approx(expected_total_cost, abs=1e-8)
    assert success_event["run_cost_usd_cumulative"] == pytest.approx(expected_total_cost, abs=1e-8)

    skip_event = events[1]
    assert skip_event["stage"] == "content_review"
    assert skip_event["model"] == "gpt-5.4-mini"
    assert skip_event["reason"] == "company_cap_exhausted"
    assert skip_event["input_tokens"] == 0
    assert skip_event["output_tokens"] == 0
    assert skip_event["input_cost_usd"] == 0.0
    assert skip_event["output_cost_usd"] == 0.0
    assert skip_event["total_cost_usd"] == 0.0
    assert skip_event["company_cost_usd_cumulative"] == pytest.approx(expected_total_cost, abs=1e-8)
    assert skip_event["run_cost_usd_cumulative"] == pytest.approx(expected_total_cost, abs=1e-8)

    summary = _read_summary(progress_store)
    assert summary["llm_total_cost_usd"] == pytest.approx(expected_total_cost, abs=1e-8)
    assert summary["llm_cost_by_stage"]["content_review"] == pytest.approx(expected_total_cost, abs=1e-8)
    assert summary["llm_cost_by_model"]["gpt-5.4-mini"] == pytest.approx(expected_total_cost, abs=1e-8)


def test_run_started_summary_rolls_up_total_from_numeric_breakdown(tmp_path) -> None:
    progress_store = _progress_store(tmp_path)
    progress_store.run_started(
        input_path=tmp_path / "companies.xlsx",
        total_rows=1,
        selected_rows=1,
        selection_mode="window",
        selected_ordinals=[],
        start_from=1,
        end_at=1,
        active_sources=["checko"],
    )

    progress_store.append_event(
        {
            "type": "llm_content_judgement",
            "stage": "content_review",
            "model": "gpt-5.4-mini",
            "total_cost_usd": 1.25,
            "cost_unknown": False,
        }
    )

    summary = _read_summary(progress_store)
    assert summary["llm_total_cost_usd"] == pytest.approx(1.25, abs=1e-8)
    assert summary["llm_cost_by_stage"]["content_review"] == pytest.approx(1.25, abs=1e-8)
    assert summary["llm_cost_by_model"]["gpt-5.4-mini"] == pytest.approx(1.25, abs=1e-8)


def test_unknown_model_marks_event_and_summary_as_unknown(monkeypatch: pytest.MonkeyPatch, tmp_path) -> None:
    _configure_llm_env(monkeypatch, content_review_model="gpt-5.4-unknown")
    _install_fake_post_sequence(monkeypatch, [_content_success_payload()])

    progress_store = _progress_store(tmp_path)
    decider = core.OpenAIDecider(logging.getLogger("test_llm_cost_unknown_model"), progress_store)

    result = decider.judge_content_record(_row(), _record("https://trusted.example/tender-1"), "https://trusted.example/")

    assert result is not None
    events = _read_events(progress_store)
    assert len(events) == 1
    event = events[0]
    assert event["type"] == "llm_content_judgement"
    assert event["stage"] == "content_review"
    assert event["model"] == "gpt-5.4-unknown"
    assert event["input_tokens"] == 80
    assert event["output_tokens"] == 25
    assert event["cost_unknown"] is True
    assert event["input_cost_usd"] is None
    assert event["output_cost_usd"] is None
    assert event["total_cost_usd"] is None
    assert event["company_cost_usd_cumulative"] is None
    assert event["run_cost_usd_cumulative"] is None

    summary = _read_summary(progress_store)
    assert summary["llm_total_cost_usd"] is None
    assert summary["llm_cost_by_stage"]["content_review"] is None
    assert summary["llm_cost_by_model"]["gpt-5.4-unknown"] is None

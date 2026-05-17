from __future__ import annotations

import json
import logging

import pytest

import company_enrichment_core as core


class _FakeOpenAIResponse:
    def __init__(self, payload: dict[str, object]) -> None:
        self._payload = payload

    def raise_for_status(self) -> None:
        return None

    def json(self) -> dict[str, object]:
        return self._payload


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


def _site_context() -> dict[str, object]:
    return {
        "candidate_site": {
            "url": "https://trusted.example/",
            "final_url": "https://trusted.example/home",
            "title": "Trusted Industrial Plant Corporate Website With Procurement and Supply Contacts",
            "description": "Official plant website with production profile, procurement notices, and corporate contacts.",
            "phones": ["+7 495 000-00-01", "+7 495 000-00-02"],
            "emails": ["sales@trusted.example", "office@trusted.example"],
            "addresses": ["Moscow region, Industrial street 7, production site and warehouse complex"],
            "fetched_pages": [
                "https://trusted.example/about",
                "https://trusted.example/contacts",
            ],
            "text_excerpt": (
                "Parser Plant manufactures industrial fittings, publishes surplus stock notices, and uses the trusted.example "
                "domain for corporate contacts, tender pages, and production descriptions."
            ),
        },
        "known_contacts": {
            "emails": ["info@trusted.example", "procurement@trusted.example"],
            "phones": ["+7 495 000-00-01", "+7 495 000-00-03"],
        },
        "aggregator_profile": {
            "rusprofile": {
                "status": "found",
                "company_name_found": "Parser Plant LLC",
                "websites": ["https://trusted.example/"],
                "emails": ["info@trusted.example"],
                "addresses": ["Moscow region, Industrial street 7"],
                "primary_okved": {"code": "28.14", "label": "Production", "display": "28.14 Production"},
                "additional_okveds": [{"code": "46.69", "label": "Trade", "display": "46.69 Trade"}],
                "snippets": ["Corporate site trusted.example is listed on the profile."],
            }
        },
        "heuristics": {
            "decision_status": "candidate",
            "authenticity_score": 0.88,
            "identity_score": 0.91,
            "viability_score": 0.79,
            "industrial_score": 0.83,
            "conflict_penalty": 0.04,
            "hard_negative_hits": ["none"],
            "matched_name_tokens": ["parser", "plant", "llc", "industrial"],
            "positive_keywords": ["production", "contacts", "procurement", "surplus"],
            "negative_keywords": ["catalog"],
            "flags": {"corporate_email_domain": True},
            "identity_reasons": ["Company name and domain match trusted contacts."],
            "industrial_reasons": ["Industrial production and surplus stock signals are present."],
        },
        "business_goal": "Validate whether the site is the company's trusted industrial website.",
    }


def _site_decision_success_payload() -> dict[str, object]:
    return {
        "output": [
            {
                "content": [
                    {
                        "type": "output_text",
                        "parsed": {
                            "belongs_to_company": True,
                            "website_type": "official_corporate",
                            "industrial_relevance": "high",
                            "confidence": 0.86,
                            "reason": "Trusted domain and company identity signals match.",
                            "evidence": ["trusted.example on profile", "contacts match"],
                            "contradictions": [],
                        },
                    }
                ]
            }
        ],
        "usage": {"input_tokens": 120, "output_tokens": 42},
    }


def _content_review_success_payload() -> dict[str, object]:
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
                            "confidence": 0.81,
                            "summary": "Industrial surplus sale.",
                            "evidence": ["surplus stock"],
                        },
                    }
                ]
            }
        ],
    }


def _install_fake_post_sequence(
    monkeypatch: pytest.MonkeyPatch,
    payloads: list[dict[str, object]],
    *,
    captured_requests: list[dict[str, object]] | None = None,
) -> None:
    pending_payloads = list(payloads)

    def fake_post(url: str, *, headers: dict[str, str], json: dict[str, object], timeout: int) -> _FakeOpenAIResponse:
        if captured_requests is not None:
            captured_requests.append(json)
        if not pending_payloads:
            raise AssertionError("Unexpected extra OpenAI call")
        return _FakeOpenAIResponse(pending_payloads.pop(0))

    monkeypatch.setattr(core.requests, "post", fake_post)


def _install_fake_post(
    monkeypatch: pytest.MonkeyPatch,
    payload: dict[str, object],
    *,
    captured_requests: list[dict[str, object]] | None = None,
) -> None:
    _install_fake_post_sequence(monkeypatch, [payload], captured_requests=captured_requests)


@pytest.mark.parametrize(
    ("payload", "expected_reason", "error_fragment"),
    [
        (
            {
                "output": [
                    {
                        "content": [
                            {"type": "refusal", "refusal": "Cannot comply"},
                        ]
                    }
                ]
            },
            "refusal",
            "Cannot comply",
        ),
    ],
)
def test_site_decision_logs_specific_parser_reason(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
    payload: dict[str, object],
    expected_reason: str,
    error_fragment: str,
) -> None:
    monkeypatch.setenv("OPENAI_API_KEY", "test-key")
    monkeypatch.setenv("OPENAI_MAX_CALLS_PER_RUN", "10")
    monkeypatch.setenv("OPENAI_MAX_CALLS_PER_COMPANY", "5")
    _install_fake_post(monkeypatch, payload)

    progress_store = _progress_store(tmp_path)
    decider = core.OpenAIDecider(logging.getLogger("test_site_failure"), progress_store)
    row = _row()

    result = decider.decide(row, "https://trusted.example/", {"homepage_title": "Trusted plant"})

    assert result is None
    assert decider.calls_made == 1
    assert decider.calls_by_company[row.inn] == 1

    events = _read_events(progress_store)
    assert len(events) == 1
    assert events[0]["type"] == "llm_error"
    assert events[0]["reason"] == expected_reason
    assert events[0]["parser_reason"] == expected_reason
    assert events[0]["reason"] != "non_json_text"
    assert error_fragment in str(events[0]["error"])


def test_site_decision_retries_once_after_incomplete_max_output_tokens(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    monkeypatch.setenv("OPENAI_API_KEY", "test-key")
    monkeypatch.setenv("OPENAI_MAX_CALLS_PER_RUN", "10")
    monkeypatch.setenv("OPENAI_MAX_CALLS_PER_COMPANY", "5")
    captured_requests: list[dict[str, object]] = []
    _install_fake_post_sequence(
        monkeypatch,
        [
            {
                "status": "incomplete",
                "incomplete_details": {"reason": "max_output_tokens"},
                "output": [
                    {
                        "content": [
                            {"type": "output_text", "text": '{"belongs_to_company": true'},
                        ]
                    }
                ],
            },
            _site_decision_success_payload(),
        ],
        captured_requests=captured_requests,
    )

    progress_store = _progress_store(tmp_path)
    decider = core.OpenAIDecider(logging.getLogger("test_site_retry"), progress_store)
    row = _row()

    result = decider.decide(row, "https://trusted.example/", _site_context())

    assert result == {
        "belongs_to_company": True,
        "website_type": "official_corporate",
        "industrial_relevance": "high",
        "confidence": 0.86,
        "reason": "Trusted domain and company identity signals match.",
        "evidence": ["trusted.example on profile", "contacts match"],
        "contradictions": [],
    }
    assert decider.calls_made == 2
    assert decider.calls_by_company[row.inn] == 2
    assert len(captured_requests) == 2

    primary_system_prompt = captured_requests[0]["input"][0]["content"][0]["text"]
    retry_system_prompt = captured_requests[1]["input"][0]["content"][0]["text"]
    primary_user_prompt = captured_requests[0]["input"][1]["content"][0]["text"]
    retry_user_prompt = captured_requests[1]["input"][1]["content"][0]["text"]
    assert isinstance(primary_system_prompt, str)
    assert isinstance(retry_system_prompt, str)
    assert isinstance(primary_user_prompt, str)
    assert isinstance(retry_user_prompt, str)
    assert len(retry_system_prompt) < len(primary_system_prompt)
    assert len(retry_user_prompt) < len(primary_user_prompt)

    events = _read_events(progress_store)
    assert len(events) == 2
    assert events[0]["type"] == "llm_error"
    assert events[0]["reason"] == "incomplete_max_output_tokens"
    assert events[0]["parser_reason"] == "incomplete_max_output_tokens"
    assert events[0]["attempt_no"] == 1
    assert events[0]["attempt_kind"] == "primary"
    assert events[0]["retry_trigger"] == "incomplete_max_output_tokens"
    assert events[1]["type"] == "llm_decision"
    assert events[1]["parser_reason"] == "parsed_ok"
    assert events[1]["attempt_no"] == 2
    assert events[1]["attempt_kind"] == "retry"
    assert events[1]["retry_trigger"] == "incomplete_max_output_tokens"


def test_site_decision_does_not_retry_for_other_parser_reasons(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    monkeypatch.setenv("OPENAI_API_KEY", "test-key")
    monkeypatch.setenv("OPENAI_MAX_CALLS_PER_RUN", "10")
    monkeypatch.setenv("OPENAI_MAX_CALLS_PER_COMPANY", "5")
    captured_requests: list[dict[str, object]] = []
    _install_fake_post_sequence(
        monkeypatch,
        [
            {
                "status": "incomplete",
                "incomplete_details": {"reason": "content_filter"},
                "output": [
                    {
                        "content": [
                            {"type": "output_text", "text": ""},
                        ]
                    }
                ],
            },
            _site_decision_success_payload(),
        ],
        captured_requests=captured_requests,
    )

    progress_store = _progress_store(tmp_path)
    decider = core.OpenAIDecider(logging.getLogger("test_site_no_retry"), progress_store)
    row = _row()

    result = decider.decide(row, "https://trusted.example/", _site_context())

    assert result is None
    assert decider.calls_made == 1
    assert decider.calls_by_company[row.inn] == 1
    assert len(captured_requests) == 1

    events = _read_events(progress_store)
    assert len(events) == 1
    assert events[0]["type"] == "llm_error"
    assert events[0]["reason"] == "incomplete_content_filter"
    assert events[0]["parser_reason"] == "incomplete_content_filter"
    assert events[0]["attempt_no"] == 1
    assert events[0]["attempt_kind"] == "primary"
    assert events[0]["retry_trigger"] is None


@pytest.mark.parametrize(
    ("payload", "expected_reason", "error_fragment"),
    [
        (
            {
                "output": [
                    {
                        "content": [
                            {"type": "output_text", "parsed": ["wrong", "type"]},
                        ]
                    }
                ]
            },
            "parsed_wrong_type",
            "list",
        ),
        (
            {
                "status": "incomplete",
                "incomplete_details": {"reason": "content_filter"},
                "output": [
                    {
                        "content": [
                            {"type": "output_text", "text": ""},
                        ]
                    }
                ],
            },
            "incomplete_content_filter",
            "content_filter",
        ),
    ],
)
def test_content_review_logs_specific_parser_reason(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
    payload: dict[str, object],
    expected_reason: str,
    error_fragment: str,
) -> None:
    monkeypatch.setenv("OPENAI_API_KEY", "test-key")
    monkeypatch.setenv("OPENAI_MAX_CALLS_PER_RUN", "10")
    monkeypatch.setenv("OPENAI_MAX_CALLS_PER_COMPANY", "5")
    monkeypatch.setenv("OPENAI_CONTENT_REVIEW_MODEL", "gpt-5.4-nano")
    monkeypatch.setenv("OPENAI_CONTENT_REVIEW_FALLBACK_MODEL", "gpt-5.4-mini")
    _install_fake_post(monkeypatch, payload)

    progress_store = _progress_store(tmp_path)
    decider = core.OpenAIDecider(logging.getLogger("test_content_failure"), progress_store)
    row = _row()
    record = _record("https://trusted.example/tender-1")

    result = decider.judge_content_record(row, record, "https://trusted.example/")

    assert result is None
    assert decider.calls_made == 1
    assert decider.calls_by_company[row.inn] == 1

    events = _read_events(progress_store)
    assert len(events) == 1
    assert events[0]["type"] == "llm_content_error"
    assert events[0]["reason"] == expected_reason
    assert events[0]["parser_reason"] == expected_reason
    assert events[0]["reason"] != "non_json_text"
    assert events[0]["attempt_no"] == 1
    assert events[0]["attempt_kind"] == "primary"
    assert events[0]["fallback_trigger"] is None
    assert events[0]["model"] == "gpt-5.4-nano"
    assert error_fragment in str(events[0]["error"])


def test_content_review_falls_back_to_mini_after_incomplete_max_output_tokens(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    monkeypatch.setenv("OPENAI_API_KEY", "test-key")
    monkeypatch.setenv("OPENAI_MAX_CALLS_PER_RUN", "10")
    monkeypatch.setenv("OPENAI_MAX_CALLS_PER_COMPANY", "5")
    monkeypatch.setenv("OPENAI_CONTENT_REVIEW_MODEL", "gpt-5.4-nano")
    monkeypatch.setenv("OPENAI_CONTENT_REVIEW_FALLBACK_MODEL", "gpt-5.4-mini")
    captured_requests: list[dict[str, object]] = []
    _install_fake_post_sequence(
        monkeypatch,
        [
            {
                "status": "incomplete",
                "incomplete_details": {"reason": "max_output_tokens"},
                "output": [
                    {
                        "content": [
                            {"type": "output_text", "text": '{"relevance_label":"likely_relevant"'},
                        ]
                    }
                ],
            },
            _content_review_success_payload(),
        ],
        captured_requests=captured_requests,
    )

    progress_store = _progress_store(tmp_path)
    decider = core.OpenAIDecider(logging.getLogger("test_content_fallback"), progress_store)
    row = _row()
    record = _record("https://trusted.example/tender-1")

    result = decider.judge_content_record(row, record, "https://trusted.example/")

    assert result == {
        "relevance_label": "likely_relevant",
        "lead_type": "direct_sale",
        "confidence": 0.81,
        "summary": "Industrial surplus sale.",
        "evidence": ["surplus stock"],
    }
    assert decider.calls_made == 2
    assert decider.calls_by_company[row.inn] == 2
    assert len(captured_requests) == 2
    assert captured_requests[0]["model"] == "gpt-5.4-nano"
    assert captured_requests[1]["model"] == "gpt-5.4-mini"

    events = _read_events(progress_store)
    assert len(events) == 2
    assert events[0]["type"] == "llm_content_error"
    assert events[0]["reason"] == "incomplete_max_output_tokens"
    assert events[0]["parser_reason"] == "incomplete_max_output_tokens"
    assert events[0]["attempt_no"] == 1
    assert events[0]["attempt_kind"] == "primary"
    assert events[0]["fallback_trigger"] == "incomplete_max_output_tokens"
    assert events[0]["model"] == "gpt-5.4-nano"
    assert events[1]["type"] == "llm_content_judgement"
    assert events[1]["parser_reason"] == "parsed_ok"
    assert events[1]["attempt_no"] == 2
    assert events[1]["attempt_kind"] == "fallback"
    assert events[1]["fallback_trigger"] == "incomplete_max_output_tokens"
    assert events[1]["model"] == "gpt-5.4-mini"
    assert record.trace["llm_review"]["status"] == "completed"
    assert record.trace["llm_review"]["attempt_no"] == 2
    assert record.trace["llm_review"]["attempt_kind"] == "fallback"
    assert record.trace["llm_review"]["fallback_trigger"] == "incomplete_max_output_tokens"
    assert record.trace["llm_review"]["model"] == "gpt-5.4-mini"


def test_content_review_does_not_fallback_for_http_exception(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    monkeypatch.setenv("OPENAI_API_KEY", "test-key")
    monkeypatch.setenv("OPENAI_MAX_CALLS_PER_RUN", "10")
    monkeypatch.setenv("OPENAI_MAX_CALLS_PER_COMPANY", "5")
    monkeypatch.setenv("OPENAI_CONTENT_REVIEW_MODEL", "gpt-5.4-nano")
    monkeypatch.setenv("OPENAI_CONTENT_REVIEW_FALLBACK_MODEL", "gpt-5.4-mini")
    calls: list[dict[str, object]] = []

    def fake_post(url: str, *, headers: dict[str, str], json: dict[str, object], timeout: int) -> _FakeOpenAIResponse:
        calls.append(json)
        raise core.requests.HTTPError("503 upstream")

    monkeypatch.setattr(core.requests, "post", fake_post)

    progress_store = _progress_store(tmp_path)
    decider = core.OpenAIDecider(logging.getLogger("test_content_http_exception"), progress_store)
    row = _row()
    record = _record("https://trusted.example/tender-1")

    result = decider.judge_content_record(row, record, "https://trusted.example/")

    assert result is None
    assert decider.calls_made == 1
    assert decider.calls_by_company[row.inn] == 1
    assert len(calls) == 1
    assert calls[0]["model"] == "gpt-5.4-nano"

    events = _read_events(progress_store)
    assert len(events) == 1
    assert events[0]["type"] == "llm_content_error"
    assert events[0]["attempt_no"] == 1
    assert events[0]["attempt_kind"] == "primary"
    assert events[0]["fallback_trigger"] is None
    assert events[0]["model"] == "gpt-5.4-nano"
    assert "503 upstream" in str(events[0]["error"])

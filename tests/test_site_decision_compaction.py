from __future__ import annotations

import json
import logging
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


def _row() -> core.RowInput:
    return core.RowInput(row_index=1, inn="7700000000", company_name="Signal Plant")


def _progress_store(tmp_path: Path) -> core.ProgressStore:
    return core.ProgressStore(tmp_path / "progress")


def _record() -> core.ContentRecord:
    url = "https://trusted.example/tender-1"
    return core.ContentRecord(
        company_id="7700000000",
        site_id="https://trusted.example/",
        site_url="https://trusted.example/",
        url=url,
        source_type="html",
        source_url_or_file=url,
        section_guess="tenders",
        title="Industrial surplus sale",
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


def _site_context() -> dict[str, Any]:
    return {
        "candidate_site": {
            "url": "https://trusted.example/",
            "final_url": "https://trusted.example/about",
            "title": "Trusted Industrial Plant and Heavy Equipment Manufacturing Division" * 2,
            "description": "Official manufacturer of industrial systems, spare parts, and steel structures." * 3,
            "phones": ["+7 111 111 11 11", "+7 222 222 22 22", "+7 333 333 33 33"],
            "emails": ["sales@example.com", "office@example.com", "ceo@example.com"],
            "addresses": [
                "Moscow, Industrial street 1, building A, office 7",
                "Warehouse district, loading gate 4",
            ],
            "fetched_pages": [
                "https://trusted.example/",
                "https://trusted.example/about",
                "https://trusted.example/contacts",
            ],
            "text_excerpt": "Official plant profile and production overview. " * 80,
        },
        "known_contacts": {
            "phones": ["+7 111 111 11 11", "+7 222 222 22 22", "+7 333 333 33 33"],
            "emails": ["sales@example.com", "office@example.com", "ceo@example.com"],
            "websites": [
                "https://trusted.example/",
                "https://trusted.example/catalog",
                "https://trusted.example/contacts",
            ],
            "addresses": [
                "Moscow, Industrial street 1, building A, office 7",
                "Warehouse district, loading gate 4",
            ],
        },
        "aggregator_profile": {
            "rusprofile": {
                "status": "ok",
                "company_name_found": "Trusted Industrial Plant LLC",
                "websites": ["https://trusted.example/", "https://trusted.example/catalog"],
                "emails": ["sales@example.com", "office@example.com"],
                "addresses": [
                    "Moscow, Industrial street 1, building A, office 7",
                    "Warehouse district, loading gate 4",
                ],
                "primary_okved": {"code": "25.11", "name": "Manufacture of metal structures"},
                "additional_okveds": [
                    {"code": "24.10", "name": "Steel production"},
                    {"code": "28.41", "name": "Machine tools"},
                ],
                "snippets": [
                    "Official company profile with industrial classification and contact details.",
                    "Extended corporate profile with production capacity details.",
                ],
            }
        },
        "heuristics": {
            "decision_status": "candidate",
            "authenticity_score": 0.63,
            "identity_score": 0.58,
            "viability_score": 0.71,
            "industrial_score": 0.82,
            "conflict_penalty": 0.08,
            "hard_negative_hits": ["catalog_pattern", "dealer_hint", "social_link_only", "foreign_brand"],
            "matched_name_tokens": ["trusted", "industrial", "plant", "metal", "manufacturing"],
            "positive_keywords": ["factory", "plant", "production", "metal", "workshop"],
            "negative_keywords": ["dealer", "catalog", "marketplace", "reseller"],
            "flags": {
                "address_overlap": True,
                "domain_matches_email": True,
                "domain_matches_known_website": True,
                "domain_matches_input_site": False,
                "email_overlap": True,
                "inn_match": True,
                "name_tokens_found": True,
                "phone_overlap": True,
                "title_match": True,
            },
            "identity_reasons": [
                "company INN found on site",
                "site domain matches corporate email domain",
                "phone overlap with aggregator contacts",
            ],
            "industrial_reasons": [
                "industrial markers: factory, production, workshop, steel structures",
                "activity profile overlaps with site content",
                "weak unrelated retail markers were also detected",
            ],
        },
        "business_goal": (
            "Need a trustworthy corporate site for this exact company and not a marketplace or unrelated brand. " * 3
        ),
    }


def _configure_llm_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("OPENAI_API_KEY", "test-key")
    monkeypatch.setenv("OPENAI_MAX_CALLS_PER_RUN", "10")
    monkeypatch.setenv("OPENAI_MAX_CALLS_PER_COMPANY", "5")


def test_compact_site_decision_context_trims_noise() -> None:
    context = _site_context()

    compacted = core.compact_site_decision_context(context)

    candidate_site = compacted["candidate_site"]
    assert len(candidate_site["phones"]) == 2
    assert len(candidate_site["emails"]) == 2
    assert len(candidate_site["addresses"]) == 1
    assert len(candidate_site["fetched_pages"]) == 2
    assert len(candidate_site["text_excerpt"]) <= core.SITE_DECISION_EXCERPT_CHARS

    known_contacts = compacted["known_contacts"]
    assert len(known_contacts["phones"]) == 2
    assert len(known_contacts["emails"]) == 2
    assert len(known_contacts["websites"]) == 2
    assert len(known_contacts["addresses"]) == 2

    aggregator = compacted["aggregator_profile"]["rusprofile"]
    assert len(aggregator["websites"]) == 1
    assert len(aggregator["emails"]) == 1
    assert len(aggregator["addresses"]) == 1
    assert len(aggregator["additional_okveds"]) == 1
    assert len(aggregator["snippets"]) == 1

    heuristics = compacted["heuristics"]
    assert len(heuristics["hard_negative_hits"]) == 3
    assert len(heuristics["matched_name_tokens"]) == 4
    assert len(heuristics["positive_keywords"]) == 4
    assert len(heuristics["negative_keywords"]) == 3
    assert len(heuristics["identity_reasons"]) == 2
    assert len(heuristics["industrial_reasons"]) == 2

    assert len(compacted["business_goal"]) <= 140
    assert len(context["candidate_site"]["phones"]) == 3
    assert len(context["aggregator_profile"]["rusprofile"]["snippets"]) == 2


def test_site_decision_request_uses_compact_schema_and_dedicated_budget(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    _configure_llm_env(monkeypatch)
    monkeypatch.setenv("OPENAI_SITE_DECISION_MAX_OUTPUT_TOKENS", "416")

    captured_body: dict[str, Any] = {}

    def fake_post(url: str, *, headers: dict[str, str], json: dict[str, object], timeout: int) -> _FakeOpenAIResponse:
        captured_body["json"] = json
        return _FakeOpenAIResponse(
            {
                "usage": {"input_tokens": 29, "output_tokens": 17},
                "output": [
                    {
                        "content": [
                            {
                                "type": "output_text",
                                "text": "",
                                "parsed": {
                                    "belongs_to_company": True,
                                    "website_type": "official_corporate",
                                    "industrial_relevance": "high",
                                    "confidence": 0.87,
                                    "reason": "Exact identity markers and corporate contacts match the company.",
                                    "evidence": ["inn_match", "domain email match", "address overlap"],
                                    "contradictions": ["dealer language on one catalog page"],
                                },
                            }
                        ]
                    }
                ],
            }
        )

    monkeypatch.setattr(core.requests, "post", fake_post)

    progress_store = _progress_store(tmp_path)
    decider = core.OpenAIDecider(logging.getLogger("test_site_decision_contract"), progress_store)

    result = decider.decide(_row(), "https://trusted.example/", _site_context())

    assert result is not None
    assert result["website_type"] == "official_corporate"

    request_body = captured_body["json"]
    assert request_body["max_output_tokens"] == 416

    system_prompt = request_body["input"][0]["content"][0]["text"]
    assert "Return strict JSON only" in system_prompt
    assert str(core.SITE_DECISION_REASON_MAX_CHARS) in system_prompt

    schema = request_body["text"]["format"]["schema"]
    assert schema["properties"]["reason"]["maxLength"] == core.SITE_DECISION_REASON_MAX_CHARS
    assert schema["properties"]["evidence"]["maxItems"] == core.SITE_DECISION_EVIDENCE_MAX_ITEMS
    assert schema["properties"]["evidence"]["items"]["maxLength"] == core.SITE_DECISION_LIST_ITEM_MAX_CHARS
    assert schema["properties"]["contradictions"]["maxItems"] == core.SITE_DECISION_CONTRADICTION_MAX_ITEMS
    assert schema["properties"]["contradictions"]["items"]["maxLength"] == core.SITE_DECISION_LIST_ITEM_MAX_CHARS

    user_message = request_body["input"][1]["content"][0]["text"]
    prompt_payload = json.loads(user_message)
    compacted_context = prompt_payload["context"]
    assert prompt_payload["task"] == "Return a compact site/company decision JSON."
    candidate_site = compacted_context["candidate_site"]
    if "fetched_pages" in candidate_site:
        assert len(candidate_site["fetched_pages"]) == 2
    assert len(candidate_site["text_excerpt"]) <= core.SITE_DECISION_EXCERPT_CHARS
    assert len(compacted_context["aggregator_profile"]["rusprofile"]["snippets"]) == 1
    assert len(compacted_context["heuristics"]["positive_keywords"]) == 4


def test_content_review_budget_is_unaffected_by_site_decision_budget(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    _configure_llm_env(monkeypatch)
    monkeypatch.setenv("OPENAI_SITE_DECISION_MAX_OUTPUT_TOKENS", "416")

    captured_body: dict[str, Any] = {}

    def fake_post(url: str, *, headers: dict[str, str], json: dict[str, object], timeout: int) -> _FakeOpenAIResponse:
        captured_body["json"] = json
        return _FakeOpenAIResponse(
            {
                "usage": {"input_tokens": 17, "output_tokens": 11},
                "output": [
                    {
                        "content": [
                            {
                                "type": "output_text",
                                "text": "",
                                "parsed": {
                                    "relevance_label": "likely_relevant",
                                    "lead_type": "direct_sale",
                                    "confidence": 0.81,
                                    "summary": "industrial surplus sale signal",
                                    "evidence": ["surplus stock", "sale notice"],
                                },
                            }
                        ]
                    }
                ],
            }
        )

    monkeypatch.setattr(core.requests, "post", fake_post)

    progress_store = _progress_store(tmp_path)
    decider = core.OpenAIDecider(logging.getLogger("test_content_budget"), progress_store)

    result = decider.judge_content_record(_row(), _record(), "https://trusted.example/")

    assert result is not None
    request_body = captured_body["json"]
    assert request_body["max_output_tokens"] == 220
    assert request_body["text"]["format"]["name"] == "content_relevance_decision"

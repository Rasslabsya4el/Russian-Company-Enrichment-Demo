from __future__ import annotations

import pytest

from company_enrichment_core import OpenAIJsonParseError, extract_openai_json


def test_extract_openai_json_ignores_empty_parsed_and_returns_output_text_json() -> None:
    payload = {
        "output": [
            {
                "content": [
                    {"parsed": {}},
                    {
                        "type": "output_text",
                        "text": 'Result follows: {"company_name": "Factory Alpha", "ok": true}',
                    },
                ]
            }
        ]
    }

    assert extract_openai_json(payload) == {
        "company_name": "Factory Alpha",
        "ok": True,
    }


def test_extract_openai_json_prefers_non_empty_parsed_over_conflicting_output_text() -> None:
    parsed = {"company_name": "Factory Alpha", "confidence": "high"}
    payload = {
        "output": [
            {
                "content": [
                    {"parsed": parsed},
                    {
                        "type": "output_text",
                        "text": 'Result follows: {"company_name": "Factory Beta", "ok": false}',
                    }
                ]
            }
        ]
    }

    assert extract_openai_json(payload) == parsed


def test_extract_openai_json_raises_empty_parsed_when_only_empty_parsed_present() -> None:
    payload = {
        "output": [
            {
                "content": [
                    {"parsed": {}},
                ]
            }
        ]
    }

    with pytest.raises(OpenAIJsonParseError) as exc_info:
        extract_openai_json(payload)

    assert exc_info.value.reason == "empty_parsed"


def test_extract_openai_json_returns_output_text_json_when_parsed_is_wrong_type() -> None:
    payload = {
        "output": [
            {
                "content": [
                    {"parsed": ["unexpected", "list"]},
                    {
                        "type": "output_text",
                        "text": '{"company_name": "Factory Beta", "ok": false}',
                    },
                ]
            }
        ]
    }

    assert extract_openai_json(payload) == {
        "company_name": "Factory Beta",
        "ok": False,
    }


def test_extract_openai_json_raises_non_json_text_when_output_text_has_no_json_and_no_parsed() -> None:
    payload = {
        "output": [
            {
                "content": [
                    {"type": "output_text", "text": "not json"},
                ]
            }
        ]
    }

    with pytest.raises(OpenAIJsonParseError) as exc_info:
        extract_openai_json(payload)

    assert exc_info.value.reason == "non_json_text"


def test_extract_openai_json_raises_malformed_json_when_output_text_contains_broken_json_object() -> None:
    payload = {
        "output": [
            {
                "content": [
                    {
                        "type": "output_text",
                        "text": 'Result follows: {"company_name": "Factory Alpha", "ok": }',
                    },
                ]
            }
        ]
    }

    with pytest.raises(OpenAIJsonParseError) as exc_info:
        extract_openai_json(payload)

    assert exc_info.value.reason == "malformed_json"


def test_extract_openai_json_preserves_parsed_failure_reason_when_output_text_has_no_json() -> None:
    payload = {
        "output": [
            {
                "content": [
                    {"parsed": ["unexpected", "list"]},
                    {"type": "output_text", "text": "still not json"},
                ]
            }
        ]
    }

    with pytest.raises(OpenAIJsonParseError) as exc_info:
        extract_openai_json(payload)

    assert exc_info.value.reason == "parsed_wrong_type"
    assert "output_text did not contain a JSON object" in str(exc_info.value)


def test_extract_openai_json_raises_when_output_text_json_root_is_not_object() -> None:
    payload = {
        "output": [
            {
                "content": [
                    {"type": "output_text", "text": '["Factory Alpha", true]'},
                ]
            }
        ]
    }

    with pytest.raises(OpenAIJsonParseError) as exc_info:
        extract_openai_json(payload)

    assert exc_info.value.reason == "json_root_not_object"
    assert "LLM JSON root had type list, expected a JSON object" in str(exc_info.value)

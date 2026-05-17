from __future__ import annotations

import pytest

from app.llm.openai_responses import (
    OpenAIJsonParseError,
    extract_openai_json,
    parse_openai_response,
)


def test_parse_openai_response_classifies_non_empty_parsed_as_parsed_ok() -> None:
    payload = {
        "output": [
            {
                "content": [
                    {"parsed": {"company_name": "Factory Alpha", "ok": True}},
                ]
            }
        ]
    }

    result = parse_openai_response(payload)

    assert result.reason == "parsed_ok"
    assert result.source == "parsed"
    assert result.data == {"company_name": "Factory Alpha", "ok": True}


def test_extract_openai_json_falls_back_to_output_text_after_empty_parsed() -> None:
    payload = {
        "output": [
            {
                "content": [
                    {"parsed": {}},
                    {
                        "type": "output_text",
                        "text": 'Result follows: {"company_name": "Factory Beta", "ok": false}',
                    },
                ]
            }
        ]
    }

    assert extract_openai_json(payload) == {
        "company_name": "Factory Beta",
        "ok": False,
    }


def test_extract_openai_json_falls_back_to_output_text_after_parsed_wrong_type() -> None:
    payload = {
        "output": [
            {
                "content": [
                    {"parsed": ["unexpected", "list"]},
                    {
                        "type": "output_text",
                        "text": '{"company_name": "Factory Gamma", "ok": true}',
                    },
                ]
            }
        ]
    }

    assert extract_openai_json(payload) == {
        "company_name": "Factory Gamma",
        "ok": True,
    }


@pytest.mark.parametrize(
    ("payload", "reason", "message_fragment"),
    [
        (
            {
                "output": [
                    {
                        "content": [
                            {"parsed": {}},
                        ]
                    }
                ]
            },
            "empty_parsed",
            "empty JSON object",
        ),
        (
            {
                "output": [
                    {
                        "content": [
                            {"parsed": ["unexpected", "list"]},
                            {"type": "output_text", "text": "still not json"},
                        ]
                    }
                ]
            },
            "parsed_wrong_type",
            "output_text did not contain a JSON object",
        ),
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
        (
            {
                "status": "incomplete",
                "incomplete_details": {"reason": "max_output_tokens"},
                "output": [
                    {
                        "content": [
                            {"type": "output_text", "text": '{"company_name": "Factory'},
                        ]
                    }
                ],
            },
            "incomplete_max_output_tokens",
            "max_output_tokens",
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
        (
            {},
            "missing_output",
            "did not contain assistant output content",
        ),
        (
            {
                "output": [
                    {
                        "content": [
                            {"type": "output_text", "text": "not json"},
                        ]
                    }
                ]
            },
            "non_json_text",
            "did not contain a JSON object",
        ),
        (
            {
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
            },
            "malformed_json",
            "malformed JSON",
        ),
        (
            {
                "output": [
                    {
                        "content": [
                            {"type": "output_text", "text": '["Factory Alpha", true]'},
                        ]
                    }
                ]
            },
            "json_root_not_object",
            "expected a JSON object",
        ),
    ],
)
def test_extract_openai_json_raises_specific_reason(
    payload: dict[str, object],
    reason: str,
    message_fragment: str,
) -> None:
    with pytest.raises(OpenAIJsonParseError) as exc_info:
        extract_openai_json(payload)

    assert exc_info.value.reason == reason
    assert message_fragment in str(exc_info.value)

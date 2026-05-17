from __future__ import annotations

import json
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any, Literal

OpenAIResponseReason = Literal[
    "parsed_ok",
    "empty_parsed",
    "parsed_wrong_type",
    "refusal",
    "incomplete_max_output_tokens",
    "incomplete_content_filter",
    "missing_output",
    "non_json_text",
    "malformed_json",
    "json_root_not_object",
]


@dataclass(frozen=True, slots=True)
class OpenAIResponseParseResult:
    reason: OpenAIResponseReason
    data: dict[str, Any] | None = None
    output_text: str = ""
    message: str = ""
    source: Literal["parsed", "output_text"] | None = None


class OpenAIJsonParseError(ValueError):
    def __init__(self, reason: OpenAIResponseReason, error: str) -> None:
        super().__init__(error)
        self.reason = reason


def extract_openai_text(payload: Mapping[str, Any]) -> str:
    parts: list[str] = []
    for content in _iter_output_content(payload):
        if content.get("type") != "output_text":
            continue
        text = content.get("text")
        if isinstance(text, str) and text:
            parts.append(text)
    return "\n".join(parts).strip()


def parse_openai_response(payload: Mapping[str, Any]) -> OpenAIResponseParseResult:
    parsed_failure: OpenAIResponseParseResult | None = None
    refusal_parts: list[str] = []
    output_text_parts: list[str] = []
    saw_output = _has_output_items(payload)
    saw_content = False

    for content in _iter_output_content(payload):
        saw_content = True

        if "parsed" in content:
            parsed = content.get("parsed")
            if isinstance(parsed, dict):
                if parsed:
                    return OpenAIResponseParseResult(
                        reason="parsed_ok",
                        data=parsed,
                        source="parsed",
                    )
                if parsed_failure is None:
                    parsed_failure = OpenAIResponseParseResult(
                        reason="empty_parsed",
                        message="LLM content.parsed was an empty JSON object",
                    )
            elif parsed_failure is None or parsed_failure.reason == "empty_parsed":
                parsed_failure = OpenAIResponseParseResult(
                    reason="parsed_wrong_type",
                    message=f"LLM content.parsed had type {type(parsed).__name__}, expected a JSON object",
                )

        content_type = content.get("type")
        if content_type == "refusal":
            refusal_text = _coerce_text(content.get("refusal")) or _coerce_text(content.get("text"))
            refusal_parts.append(refusal_text or "LLM response contained a refusal")
        elif content_type == "output_text":
            text = _coerce_text(content.get("text"))
            if text:
                output_text_parts.append(text)

    output_text = "\n".join(output_text_parts).strip()

    if refusal_parts:
        refusal_text = " ".join(part for part in refusal_parts if part).strip()
        return OpenAIResponseParseResult(
            reason="refusal",
            output_text=output_text,
            message=f"LLM response was refused: {refusal_text}",
        )

    incomplete_result = _classify_incomplete(payload, output_text)
    if incomplete_result is not None:
        return incomplete_result

    if not saw_output or not saw_content:
        return OpenAIResponseParseResult(
            reason="missing_output",
            output_text=output_text,
            message="Responses payload did not contain assistant output content",
        )

    if not output_text:
        if parsed_failure is not None:
            return OpenAIResponseParseResult(
                reason=parsed_failure.reason,
                output_text=output_text,
                message=parsed_failure.message,
            )
        return OpenAIResponseParseResult(
            reason="missing_output",
            output_text=output_text,
            message="Responses payload did not contain output_text content",
        )

    text_result = _parse_output_text_json(output_text)
    if text_result.reason == "parsed_ok":
        return text_result

    if parsed_failure is not None:
        detail = parsed_failure.message
        if text_result.message:
            detail = f"{detail}; {text_result.message}"
        return OpenAIResponseParseResult(
            reason=parsed_failure.reason,
            output_text=output_text,
            message=detail,
        )
    return text_result


def extract_openai_json(payload: Mapping[str, Any]) -> dict[str, Any]:
    result = parse_openai_response(payload)
    if result.data is not None:
        return result.data
    raise OpenAIJsonParseError(result.reason, result.message)


def _iter_output_content(payload: Mapping[str, Any]) -> list[Mapping[str, Any]]:
    output = payload.get("output")
    if not isinstance(output, list):
        return []

    contents: list[Mapping[str, Any]] = []
    for item in output:
        if not isinstance(item, Mapping):
            continue
        content = item.get("content")
        if not isinstance(content, list):
            continue
        for entry in content:
            if isinstance(entry, Mapping):
                contents.append(entry)
    return contents


def _has_output_items(payload: Mapping[str, Any]) -> bool:
    output = payload.get("output")
    return isinstance(output, list) and bool(output)


def _classify_incomplete(
    payload: Mapping[str, Any],
    output_text: str,
) -> OpenAIResponseParseResult | None:
    status = payload.get("status")
    details = payload.get("incomplete_details")
    reason = details.get("reason") if isinstance(details, Mapping) else None
    if status != "incomplete" and reason is None:
        return None

    if reason == "max_output_tokens":
        return OpenAIResponseParseResult(
            reason="incomplete_max_output_tokens",
            output_text=output_text,
            message="Responses payload was incomplete because max_output_tokens was reached",
        )
    if reason == "content_filter":
        return OpenAIResponseParseResult(
            reason="incomplete_content_filter",
            output_text=output_text,
            message="Responses payload was incomplete because content_filter interrupted generation",
        )
    return OpenAIResponseParseResult(
        reason="missing_output",
        output_text=output_text,
        message="Responses payload was incomplete without a supported reason",
    )


def _coerce_text(value: Any) -> str:
    return value if isinstance(value, str) else ""


def _parse_output_text_json(output_text: str) -> OpenAIResponseParseResult:
    decoder = json.JSONDecoder()
    first_error: json.JSONDecodeError | None = None

    for start in _iter_json_candidate_starts(output_text):
        try:
            value, _ = decoder.raw_decode(output_text[start:])
        except json.JSONDecodeError as exc:
            if first_error is None:
                first_error = exc
            continue

        if not isinstance(value, dict):
            return OpenAIResponseParseResult(
                reason="json_root_not_object",
                output_text=output_text,
                message=f"LLM JSON root had type {type(value).__name__}, expected a JSON object",
            )
        return OpenAIResponseParseResult(
            reason="parsed_ok",
            data=value,
            output_text=output_text,
            source="output_text",
        )

    if first_error is not None:
        return OpenAIResponseParseResult(
            reason="malformed_json",
            output_text=output_text,
            message=(
                "LLM output_text contained malformed JSON: "
                f"{first_error.msg} at line {first_error.lineno} column {first_error.colno}"
            ),
        )
    return OpenAIResponseParseResult(
        reason="non_json_text",
        output_text=output_text,
        message="LLM output_text did not contain a JSON object",
    )


def _iter_json_candidate_starts(text: str) -> list[int]:
    starts: list[int] = []
    for index, char in enumerate(text):
        if char not in "{[":
            continue
        if _looks_like_json_start(text, index):
            starts.append(index)
    return starts


def _looks_like_json_start(text: str, start: int) -> bool:
    index = start + 1
    while index < len(text) and text[index].isspace():
        index += 1
    if index >= len(text):
        return False

    next_char = text[index]
    if text[start] == "{":
        return next_char in {'"', "}"}
    return (
        next_char in {'"', "{", "[", "]", "-", "t", "f", "n"}
        or next_char.isdigit()
    )

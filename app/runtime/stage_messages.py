from __future__ import annotations

import json
from collections.abc import Iterator, Mapping
from pathlib import Path
from typing import Any

from .files import ensure_dir


RUNTIME_PRIVATE_DIRNAME = "_runtime"
STAGE_MESSAGE_OUTBOX_FILENAME = "stage_messages.jsonl"
STAGE_MESSAGE_OUTBOX_RELATIVE_PATH = f"{RUNTIME_PRIVATE_DIRNAME}/{STAGE_MESSAGE_OUTBOX_FILENAME}"
STAGE_MESSAGE_TYPES = (
    "source_result_ready",
    "candidate_site_found",
    "site_gate_decision",
    "deep_parse_done",
    "host_event",
    "company_completed",
)
_ALLOWED_STAGE_MESSAGE_TYPES = frozenset(STAGE_MESSAGE_TYPES)


def stage_message_outbox_path(output_dir: Path | str) -> Path:
    return Path(output_dir) / RUNTIME_PRIVATE_DIRNAME / STAGE_MESSAGE_OUTBOX_FILENAME


def build_stage_outbox_cursor(*, byte_offset: Any = 0) -> dict[str, Any]:
    return normalize_stage_outbox_cursor({"byte_offset": byte_offset})


def build_stage_message(
    *,
    run_id: Any,
    ts: Any,
    message_type: Any,
    stage: Any,
    inn: Any,
    row_index: Any,
    payload: Any,
) -> dict[str, Any]:
    return normalize_stage_message(
        {
            "run_id": run_id,
            "ts": ts,
            "message_type": message_type,
            "stage": stage,
            "inn": inn,
            "row_index": row_index,
            "payload": payload,
        }
    )


def normalize_stage_message(message: Mapping[str, Any]) -> dict[str, Any]:
    if not isinstance(message, Mapping):
        raise ValueError("stage message must be a mapping")
    return {
        "run_id": _normalize_required_text(message.get("run_id"), field_name="run_id"),
        "ts": _normalize_required_text(message.get("ts"), field_name="ts"),
        "message_type": _normalize_message_type(message.get("message_type")),
        "stage": _normalize_required_text(message.get("stage"), field_name="stage"),
        "inn": _normalize_required_text(message.get("inn"), field_name="inn"),
        "row_index": _normalize_row_index(message.get("row_index")),
        "payload": _normalize_payload_root(message.get("payload")),
    }


def normalize_stage_outbox_cursor(cursor: Any) -> dict[str, Any]:
    payload = dict(cursor) if isinstance(cursor, Mapping) else {}
    return {
        "outbox_path": STAGE_MESSAGE_OUTBOX_RELATIVE_PATH,
        "byte_offset": _normalize_stage_outbox_byte_offset(payload.get("byte_offset")),
    }


def append_stage_message(output_dir: Path | str, message: Mapping[str, Any]) -> dict[str, Any]:
    normalized = normalize_stage_message(message)
    outbox_path = stage_message_outbox_path(output_dir)
    ensure_dir(outbox_path.parent)
    with outbox_path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(normalized, ensure_ascii=False, separators=(",", ":")) + "\n")
    return normalized


def iter_stage_messages(output_dir: Path | str) -> Iterator[dict[str, Any]]:
    outbox_path = stage_message_outbox_path(output_dir)
    if not outbox_path.exists():
        return
    with outbox_path.open("r", encoding="utf-8") as handle:
        for line_number, raw_line in enumerate(handle, start=1):
            line = raw_line.strip()
            if not line:
                continue
            yield _parse_stage_message_json(line, location=f"line {line_number}")


def read_unconsumed_stage_messages(
    output_dir: Path | str,
    *,
    cursor: Mapping[str, Any] | None = None,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    normalized_cursor = normalize_stage_outbox_cursor(cursor)
    outbox_path = stage_message_outbox_path(output_dir)
    if not outbox_path.exists():
        return [], normalized_cursor

    cursor_byte_offset = normalized_cursor["byte_offset"]
    outbox_size = outbox_path.stat().st_size
    if cursor_byte_offset > outbox_size:
        raise ValueError(
            f"stage outbox truncated before cursor: byte_offset={cursor_byte_offset} size={outbox_size}"
        )

    unread_messages: list[dict[str, Any]] = []
    next_byte_offset = cursor_byte_offset
    with outbox_path.open("rb") as handle:
        handle.seek(cursor_byte_offset)
        while True:
            line_byte_offset = handle.tell()
            raw_line = handle.readline()
            if not raw_line:
                break
            next_byte_offset = handle.tell()
            line = _decode_stage_message_line(raw_line, byte_offset=line_byte_offset)
            if line is None:
                continue
            unread_messages.append(
                _parse_stage_message_json(line, location=f"byte_offset {line_byte_offset}")
            )
    return unread_messages, build_stage_outbox_cursor(byte_offset=next_byte_offset)


def load_stage_messages(output_dir: Path | str) -> list[dict[str, Any]]:
    return list(iter_stage_messages(output_dir))


def _normalize_required_text(value: Any, *, field_name: str) -> str:
    normalized = str(value or "").strip()
    if not normalized:
        raise ValueError(f"stage message field '{field_name}' is required")
    return normalized


def _normalize_message_type(value: Any) -> str:
    normalized = _normalize_required_text(value, field_name="message_type")
    if normalized not in _ALLOWED_STAGE_MESSAGE_TYPES:
        allowed_types = ", ".join(STAGE_MESSAGE_TYPES)
        raise ValueError(
            f"unknown stage message_type '{normalized}'; allowed values: {allowed_types}"
        )
    return normalized


def _normalize_row_index(value: Any) -> int:
    try:
        normalized = int(value)
    except (TypeError, ValueError) as exc:
        raise ValueError("stage message field 'row_index' must be a positive integer") from exc
    if normalized <= 0:
        raise ValueError("stage message field 'row_index' must be a positive integer")
    return normalized


def _normalize_payload_root(value: Any) -> Any:
    if value is None:
        raise ValueError("stage message field 'payload' is required")
    return _normalize_json_value(value)


def _normalize_stage_outbox_byte_offset(value: Any) -> int:
    try:
        normalized = int(value or 0)
    except (TypeError, ValueError) as exc:
        raise ValueError("stage outbox cursor field 'byte_offset' must be a non-negative integer") from exc
    if normalized < 0:
        raise ValueError("stage outbox cursor field 'byte_offset' must be a non-negative integer")
    return normalized


def _decode_stage_message_line(raw_line: bytes, *, byte_offset: int) -> str | None:
    try:
        line = raw_line.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise ValueError(f"invalid stage message utf-8 at byte_offset {byte_offset}: {exc.reason}") from exc
    normalized = line.strip()
    return normalized or None


def _parse_stage_message_json(line: str, *, location: str) -> dict[str, Any]:
    try:
        payload = json.loads(line)
    except json.JSONDecodeError as exc:
        raise ValueError(f"invalid stage message jsonl at {location}: {exc.msg}") from exc
    if not isinstance(payload, dict):
        raise ValueError(f"stage message at {location} must be a JSON object")
    return normalize_stage_message(payload)


def _normalize_json_value(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {
            str(key): _normalize_json_value(value[key])
            for key in sorted(value.keys(), key=lambda item: str(item))
        }
    if isinstance(value, (list, tuple)):
        return [_normalize_json_value(item) for item in value]
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    raise ValueError(
        "stage message payload must contain only JSON-compatible scalars, objects, arrays, and nulls"
    )


__all__ = [
    "RUNTIME_PRIVATE_DIRNAME",
    "STAGE_MESSAGE_OUTBOX_FILENAME",
    "STAGE_MESSAGE_TYPES",
    "append_stage_message",
    "build_stage_message",
    "iter_stage_messages",
    "load_stage_messages",
    "normalize_stage_message",
    "stage_message_outbox_path",
]

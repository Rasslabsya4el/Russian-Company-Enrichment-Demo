from __future__ import annotations

import json
from pathlib import Path

import pytest

from app.runtime import (
    append_stage_message,
    build_stage_message,
    iter_stage_messages,
    load_stage_messages,
    normalize_stage_message,
    stage_message_outbox_path,
)
from app.runtime.stage_messages import build_stage_outbox_cursor, read_unconsumed_stage_messages


def _base_message() -> dict[str, object]:
    return {
        "run_id": "run-001",
        "ts": "2026-04-18T10:00:00Z",
        "message_type": "source_result_ready",
        "stage": "source_collect",
        "inn": "7700000001",
        "row_index": 1,
        "payload": {
            "source": "spark",
            "status": "success",
            "details": {"b": 2, "a": 1},
        },
    }


@pytest.mark.parametrize("field_name", ["run_id", "message_type", "stage", "inn", "payload"])
def test_stage_message_validation_rejects_missing_required_fields(field_name: str) -> None:
    message = _base_message()
    message.pop(field_name)

    with pytest.raises(ValueError, match=field_name):
        normalize_stage_message(message)


def test_stage_message_validation_rejects_unknown_message_type() -> None:
    message = _base_message()
    message["message_type"] = "unexpected_event"

    with pytest.raises(ValueError, match="unknown stage message_type"):
        normalize_stage_message(message)


def test_stage_message_outbox_roundtrip_is_deterministic(tmp_path: Path) -> None:
    output_dir = tmp_path / "output"
    first_message = build_stage_message(**_base_message())
    second_message = build_stage_message(
        run_id="run-001",
        ts="2026-04-18T10:00:01Z",
        message_type="company_completed",
        stage="finalize_company",
        inn="7700000001",
        row_index=1,
        payload={"status": "completed", "metrics": {"contacts": 3, "sites": 1}},
    )

    append_stage_message(output_dir, first_message)
    append_stage_message(output_dir, second_message)

    outbox_path = stage_message_outbox_path(output_dir)
    raw_lines = outbox_path.read_text(encoding="utf-8").splitlines()
    assert len(raw_lines) == 2
    assert json.loads(raw_lines[0]) == first_message
    assert json.loads(raw_lines[1]) == second_message
    assert list(iter_stage_messages(output_dir)) == [first_message, second_message]
    assert load_stage_messages(output_dir) == [first_message, second_message]


def test_stage_message_outbox_unread_reader_advances_byte_offset_cursor(tmp_path: Path) -> None:
    output_dir = tmp_path / "output"
    first_message = build_stage_message(**_base_message())
    second_message = build_stage_message(
        run_id="run-001",
        ts="2026-04-18T10:00:01Z",
        message_type="company_completed",
        stage="finalize_company",
        inn="7700000001",
        row_index=1,
        payload={"status": "completed"},
    )

    append_stage_message(output_dir, first_message)
    unread_after_first, cursor_after_first = read_unconsumed_stage_messages(
        output_dir,
        cursor=build_stage_outbox_cursor(),
    )

    assert unread_after_first == [first_message]
    assert cursor_after_first == {
        "outbox_path": "_runtime/stage_messages.jsonl",
        "byte_offset": stage_message_outbox_path(output_dir).stat().st_size,
    }

    append_stage_message(output_dir, second_message)
    unread_after_second, cursor_after_second = read_unconsumed_stage_messages(
        output_dir,
        cursor=cursor_after_first,
    )

    assert unread_after_second == [second_message]
    assert cursor_after_second == {
        "outbox_path": "_runtime/stage_messages.jsonl",
        "byte_offset": stage_message_outbox_path(output_dir).stat().st_size,
    }

    drained_messages, drained_cursor = read_unconsumed_stage_messages(
        output_dir,
        cursor=cursor_after_second,
    )

    assert drained_messages == []
    assert drained_cursor == cursor_after_second


def test_stage_message_outbox_uses_private_runtime_boundary(tmp_path: Path) -> None:
    output_dir = tmp_path / "output"
    outbox_path = stage_message_outbox_path(output_dir)

    assert outbox_path == output_dir / "_runtime" / "stage_messages.jsonl"
    assert outbox_path != output_dir / "results.jsonl"
    assert outbox_path != output_dir / "events.jsonl"

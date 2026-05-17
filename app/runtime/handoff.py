from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any


AGGREGATOR_SITE_HANDOFF_KEY = "aggregator_site"


def build_stage_handoff_state() -> dict[str, Any]:
    return {AGGREGATOR_SITE_HANDOFF_KEY: {"companies": {}}}


def normalize_stage_handoff_state(payload: Any) -> dict[str, Any]:
    root = dict(payload) if isinstance(payload, Mapping) else {}
    aggregator_site_payload = root.get(AGGREGATOR_SITE_HANDOFF_KEY)
    companies_payload = (
        aggregator_site_payload.get("companies") if isinstance(aggregator_site_payload, Mapping) else {}
    )
    companies: dict[str, dict[str, Any]] = {}
    if isinstance(companies_payload, Mapping):
        for raw_inn in sorted(companies_payload.keys(), key=lambda item: str(item)):
            normalized_company = _normalize_company_handoff(raw_inn, companies_payload[raw_inn])
            if normalized_company is None:
                continue
            companies[normalized_company["inn"]] = normalized_company
    return {
        AGGREGATOR_SITE_HANDOFF_KEY: {
            "companies": companies,
        }
    }


def apply_stage_messages_to_handoff_state(
    handoff_state: Mapping[str, Any] | None,
    messages: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    normalized_state = normalize_stage_handoff_state(handoff_state)
    companies = normalized_state[AGGREGATOR_SITE_HANDOFF_KEY]["companies"]
    for message in messages:
        _apply_stage_message(companies, message)
    return normalized_state


def _apply_stage_message(
    companies: dict[str, dict[str, Any]],
    message: Mapping[str, Any],
) -> None:
    message_type = str(message.get("message_type") or "").strip()
    if message_type == "host_event":
        return
    inn = str(message.get("inn") or "").strip()
    if not inn:
        return

    row_index = _normalize_row_index(message.get("row_index"))
    company = companies.setdefault(inn, _build_company_handoff(inn=inn, row_index=row_index))
    company["row_index"] = row_index or company["row_index"]
    company["last_message_type"] = message_type
    company["last_message_ts"] = str(message.get("ts") or "")
    message_payload = _message_payload(message)

    if message_type == "source_result_ready":
        _upsert_ordered_item(company["source_results"], message_payload, key_field="source")
        return
    if message_type == "candidate_site_found":
        _upsert_ordered_item(company["candidate_sites"], message_payload, key_field="site_url")
        return
    if message_type == "site_gate_decision":
        _upsert_ordered_item(company["site_gate_decisions"], message_payload, key_field="site_url")
        return
    if message_type == "deep_parse_done":
        company["deep_parse_done"] = message_payload
        return
    if message_type == "company_completed":
        company["company_completed"] = message_payload


def _build_company_handoff(*, inn: str, row_index: int) -> dict[str, Any]:
    return {
        "inn": inn,
        "row_index": row_index,
        "last_message_type": "",
        "last_message_ts": "",
        "source_results": [],
        "candidate_sites": [],
        "site_gate_decisions": [],
        "deep_parse_done": {},
        "company_completed": {},
    }


def _normalize_company_handoff(raw_inn: Any, payload: Any) -> dict[str, Any] | None:
    company_payload = dict(payload) if isinstance(payload, Mapping) else {}
    inn = str(company_payload.get("inn") or raw_inn or "").strip()
    if not inn:
        return None
    return {
        "inn": inn,
        "row_index": _normalize_row_index(company_payload.get("row_index")),
        "last_message_type": str(company_payload.get("last_message_type") or "").strip(),
        "last_message_ts": str(company_payload.get("last_message_ts") or ""),
        "source_results": _normalize_ordered_items(company_payload.get("source_results"), key_field="source"),
        "candidate_sites": _normalize_ordered_items(company_payload.get("candidate_sites"), key_field="site_url"),
        "site_gate_decisions": _normalize_ordered_items(
            company_payload.get("site_gate_decisions"),
            key_field="site_url",
        ),
        "deep_parse_done": _normalize_payload_item(company_payload.get("deep_parse_done")),
        "company_completed": _normalize_payload_item(company_payload.get("company_completed")),
    }


def _normalize_ordered_items(value: Any, *, key_field: str) -> list[dict[str, Any]]:
    normalized_items: list[dict[str, Any]] = []
    if not isinstance(value, list):
        return normalized_items
    for item in value:
        normalized_item = _normalize_payload_item(item, required_key=key_field)
        if normalized_item is None:
            continue
        _upsert_ordered_item(normalized_items, normalized_item, key_field=key_field)
    return normalized_items


def _normalize_payload_item(
    value: Any,
    *,
    required_key: str | None = None,
) -> dict[str, Any] | None:
    if not isinstance(value, Mapping):
        return {} if required_key is None else None
    normalized = {
        str(key): _clone_json_value(item)
        for key, item in value.items()
    }
    if required_key is not None:
        required_value = str(normalized.get(required_key) or "").strip()
        if not required_value:
            return None
        normalized[required_key] = required_value
    normalized["stage"] = str(normalized.get("stage") or "")
    normalized["ts"] = str(normalized.get("ts") or "")
    return normalized


def _message_payload(message: Mapping[str, Any]) -> dict[str, Any]:
    payload = message.get("payload")
    normalized_payload = _normalize_payload_item(payload)
    if normalized_payload is None:
        normalized_payload = {}
    normalized_payload["stage"] = str(message.get("stage") or "")
    normalized_payload["ts"] = str(message.get("ts") or "")
    return normalized_payload


def _upsert_ordered_item(items: list[dict[str, Any]], item: dict[str, Any], *, key_field: str) -> None:
    key_value = str(item.get(key_field) or "").strip()
    if not key_value:
        return
    item[key_field] = key_value
    for index, current_item in enumerate(items):
        if str(current_item.get(key_field) or "").strip() == key_value:
            items[index] = item
            return
    items.append(item)


def _clone_json_value(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {str(key): _clone_json_value(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_clone_json_value(item) for item in value]
    if isinstance(value, tuple):
        return [_clone_json_value(item) for item in value]
    return value


def _normalize_row_index(value: Any) -> int:
    try:
        normalized = int(value)
    except (TypeError, ValueError):
        return 0
    return normalized if normalized > 0 else 0


__all__ = [
    "AGGREGATOR_SITE_HANDOFF_KEY",
    "apply_stage_messages_to_handoff_state",
    "build_stage_handoff_state",
    "normalize_stage_handoff_state",
]

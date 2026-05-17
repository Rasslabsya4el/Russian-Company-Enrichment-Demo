from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from typing import Any

from .handoff import AGGREGATOR_SITE_HANDOFF_KEY, normalize_stage_handoff_state


QUEUE_STATUS_PENDING = "pending"
QUEUE_STATUS_READY = "ready"
_ALLOWED_QUEUE_STATUSES = frozenset({QUEUE_STATUS_PENDING, QUEUE_STATUS_READY})


def build_stage_handoff_pickup_state() -> dict[str, Any]:
    return {AGGREGATOR_SITE_HANDOFF_KEY: {"companies": {}}}


def normalize_stage_handoff_pickup_state(payload: Any) -> dict[str, Any]:
    root = dict(payload) if isinstance(payload, Mapping) else {}
    aggregator_site_payload = root.get(AGGREGATOR_SITE_HANDOFF_KEY)
    companies_payload = (
        aggregator_site_payload.get("companies") if isinstance(aggregator_site_payload, Mapping) else {}
    )
    companies: dict[str, dict[str, Any]] = {}
    if isinstance(companies_payload, Mapping):
        for raw_inn in sorted(companies_payload.keys(), key=lambda item: str(item)):
            normalized_company = _normalize_company_pickup_state(raw_inn, companies_payload[raw_inn])
            if normalized_company is None:
                continue
            companies[normalized_company["inn"]] = normalized_company
    return {
        AGGREGATOR_SITE_HANDOFF_KEY: {
            "companies": companies,
        }
    }


def synchronize_stage_handoff_pickup_state(
    pickup_state: Mapping[str, Any] | None,
    handoff_state: Mapping[str, Any] | None,
) -> dict[str, Any]:
    normalized_pickups = normalize_stage_handoff_pickup_state(pickup_state)
    normalized_handoffs = normalize_stage_handoff_state(handoff_state)
    handoff_companies = normalized_handoffs[AGGREGATOR_SITE_HANDOFF_KEY]["companies"]
    existing_pickups = normalized_pickups[AGGREGATOR_SITE_HANDOFF_KEY]["companies"]

    next_companies: dict[str, dict[str, Any]] = {}
    for inn in _ordered_company_inns(handoff_companies):
        handoff_company = handoff_companies[inn]
        next_companies[inn] = _build_company_pickup_state(
            handoff_company,
            existing_pickups.get(inn),
        )
    return {
        AGGREGATOR_SITE_HANDOFF_KEY: {
            "companies": next_companies,
        }
    }


def pickup_ready_stage_handoffs(
    pickup_state: Mapping[str, Any] | None,
    handoff_state: Mapping[str, Any] | None,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    normalized_handoffs = normalize_stage_handoff_state(handoff_state)
    next_pickups = synchronize_stage_handoff_pickup_state(pickup_state, normalized_handoffs)
    handoff_companies = normalized_handoffs[AGGREGATOR_SITE_HANDOFF_KEY]["companies"]
    pickup_companies = next_pickups[AGGREGATOR_SITE_HANDOFF_KEY]["companies"]

    ready_handoffs: list[dict[str, Any]] = []
    for inn in _ordered_company_inns(handoff_companies):
        pickup_company = pickup_companies.get(inn)
        if pickup_company is None or pickup_company["queue_status"] != QUEUE_STATUS_READY:
            continue
        if pickup_company["picked_up_handoff_fingerprint"] == pickup_company["handoff_fingerprint"]:
            continue
        handoff_company = handoff_companies.get(inn)
        if handoff_company is None:
            continue
        ready_handoffs.append(_clone_json_value(handoff_company))
        pickup_company["picked_up_handoff_fingerprint"] = pickup_company["handoff_fingerprint"]
        pickup_company["last_picked_up_message_ts"] = pickup_company["last_message_ts"]
    return ready_handoffs, next_pickups


def _ordered_company_inns(companies: Mapping[str, Mapping[str, Any]]) -> list[str]:
    return sorted(
        companies.keys(),
        key=lambda inn: (
            _normalize_row_index(companies[inn].get("row_index")),
            str(inn),
        ),
    )


def _build_company_pickup_state(
    handoff_company: Mapping[str, Any],
    existing_pickup: Mapping[str, Any] | None,
) -> dict[str, Any]:
    normalized_existing = _normalize_company_pickup_state(
        handoff_company.get("inn"),
        existing_pickup,
    )
    handoff_fingerprint = stage_handoff_fingerprint(handoff_company)
    return {
        "inn": str(handoff_company.get("inn") or "").strip(),
        "row_index": _normalize_row_index(handoff_company.get("row_index")),
        "queue_status": _queue_status_for_handoff(handoff_company),
        "handoff_fingerprint": handoff_fingerprint,
        "last_message_type": str(handoff_company.get("last_message_type") or "").strip(),
        "last_message_ts": str(handoff_company.get("last_message_ts") or ""),
        "picked_up_handoff_fingerprint": (
            normalized_existing["picked_up_handoff_fingerprint"]
            if normalized_existing is not None
            else ""
        ),
        "last_picked_up_message_ts": (
            normalized_existing["last_picked_up_message_ts"]
            if normalized_existing is not None
            else ""
        ),
    }


def _normalize_company_pickup_state(raw_inn: Any, payload: Any) -> dict[str, Any] | None:
    company_payload = dict(payload) if isinstance(payload, Mapping) else {}
    inn = str(company_payload.get("inn") or raw_inn or "").strip()
    if not inn:
        return None
    queue_status = str(company_payload.get("queue_status") or "").strip().lower()
    if queue_status not in _ALLOWED_QUEUE_STATUSES:
        queue_status = QUEUE_STATUS_PENDING
    return {
        "inn": inn,
        "row_index": _normalize_row_index(company_payload.get("row_index")),
        "queue_status": queue_status,
        "handoff_fingerprint": str(company_payload.get("handoff_fingerprint") or ""),
        "last_message_type": str(company_payload.get("last_message_type") or "").strip(),
        "last_message_ts": str(company_payload.get("last_message_ts") or ""),
        "picked_up_handoff_fingerprint": str(company_payload.get("picked_up_handoff_fingerprint") or ""),
        "last_picked_up_message_ts": str(company_payload.get("last_picked_up_message_ts") or ""),
    }


def _queue_status_for_handoff(handoff_company: Mapping[str, Any]) -> str:
    last_message_type = str(handoff_company.get("last_message_type") or "").strip()
    company_completed = handoff_company.get("company_completed")
    if last_message_type == "company_completed" and isinstance(company_completed, Mapping) and company_completed:
        return QUEUE_STATUS_READY
    return QUEUE_STATUS_PENDING


def stage_handoff_fingerprint(handoff_company: Mapping[str, Any]) -> str:
    payload = json.dumps(
        _clone_json_value(handoff_company),
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


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
    "QUEUE_STATUS_PENDING",
    "QUEUE_STATUS_READY",
    "build_stage_handoff_pickup_state",
    "normalize_stage_handoff_pickup_state",
    "pickup_ready_stage_handoffs",
    "stage_handoff_fingerprint",
    "synchronize_stage_handoff_pickup_state",
]

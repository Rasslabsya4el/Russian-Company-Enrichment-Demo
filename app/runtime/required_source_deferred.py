from __future__ import annotations

import math
import re
from collections import Counter
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any


DEFERRED_REQUIRED_SOURCES_KEY = "deferred_required_sources"
DEFERRED_REQUIRED_SOURCE_CONTRACT_VERSION = 1

OUTCOME_SYSTEMIC_STOP = "systemic_stop"
OUTCOME_DEFERRED_ROW_TRANSIENT = "deferred_row_transient"
OUTCOME_NON_DEFER_FAIL_FAST = "non_defer_fail_fast"

RUN_STATUS_COMPLETED_WITH_DEFERRED_REQUIRED_SOURCES = "completed_with_deferred_required_sources"
RUN_FINISH_REASON_DEFERRED_REQUIRED_SOURCES = "deferred_required_sources_unresolved"

REQUIRED_SOURCE_DEFERRED_ROWS_TOTAL_KEY = "required_source_deferred_rows_total"
REQUIRED_SOURCE_DEFERRED_ROWS_BY_SOURCE_KEY = "required_source_deferred_rows_by_source"
REQUIRED_SOURCE_DEFERRED_ROWS_BY_STATUS_KEY = "required_source_deferred_rows_by_status"
UNRESOLVED_REQUIRED_SOURCE_ROWS_KEY = "unresolved_required_source_rows"

RESOLUTION_STATUS_UNRESOLVED = "unresolved"
RESOLUTION_STATUS_RESOLVED = "resolved"

SYSTEMIC_STOP_STATUSES = frozenset(
    {
        "",
        "unknown",
        "unknown_status",
        "auth_failed",
        "blocked",
        "blocked_no_proxy",
        "bot_gate",
        "cooldown_active",
        "invalid_url",
        "not_configured",
        "rate_limited",
        "source_disabled_after_block",
    }
)
TRANSIENT_STATUS_ALLOWLIST = frozenset({"http_503"})
SOURCE_APPROVED_TRANSIENT_HTTP_STATUSES = {
    "zachestnyibiznes": frozenset({"http_403"}),
}
SOURCE_APPROVED_BLOCKED_TRANSIENT_DETAIL_MARKERS = {
    "zachestnyibiznes": frozenset({"connect timeout", "connecttimeouterror"}),
}
TRANSIENT_DETAIL_MARKERS = (
    "connection reset",
    "connection aborted",
    "connectionerror",
    "connect timeout",
    "eof occurred",
    "failed to resolve",
    "getaddrinfo failed",
    "max retries exceeded",
    "nameresolutionerror",
    "name or service not known",
    "read timed out",
    "read timeout",
    "remotedisconnected",
    "remote end closed connection",
    "remote host closed",
    "reset by peer",
    "ssl eof",
    "temporary failure in name resolution",
    "timeout",
    "timed out",
)


@dataclass(frozen=True, slots=True)
class RequiredSourceClassification:
    outcome: str
    reason: str


def normalize_runtime_text(value: Any) -> str:
    return re.sub(r"\s+", " ", str(value or "")).strip()


def deferred_required_source_cap(selected_rows: int) -> int:
    return max(2, math.ceil(max(int(selected_rows or 0), 0) * 0.02))


def required_source_deferred_state() -> dict[str, Any]:
    return {
        "contract_version": DEFERRED_REQUIRED_SOURCE_CONTRACT_VERSION,
        "records": {},
        "source_health": {},
    }


def _record_key(*, inn: str, source: str) -> str:
    return f"{normalize_runtime_text(inn)}::{normalize_runtime_text(source)}"


def _normalize_int(value: Any, *, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _normalize_record(payload: Any) -> dict[str, Any] | None:
    record = dict(payload) if isinstance(payload, Mapping) else {}
    source = normalize_runtime_text(record.get("source"))
    inn = normalize_runtime_text(record.get("inn"))
    if not source or not inn:
        return None
    status = normalize_runtime_text(record.get("status")) or "unknown_status"
    normalized = {
        "source": source,
        "access_mode": normalize_runtime_text(record.get("access_mode")),
        "status": status,
        "error": normalize_runtime_text(record.get("error")),
        "detail": normalize_runtime_text(record.get("detail")),
        "row_index": max(_normalize_int(record.get("row_index")), 0),
        "inn": inn,
        "company_name": normalize_runtime_text(record.get("company_name")),
        "run_id": normalize_runtime_text(record.get("run_id")),
        "first_seen_at": normalize_runtime_text(record.get("first_seen_at")),
        "last_seen_at": normalize_runtime_text(record.get("last_seen_at")),
        "attempt_budget": normalize_runtime_text(record.get("attempt_budget")) or "source_default_finite",
        "observed_attempt_count": normalize_runtime_text(record.get("observed_attempt_count")),
        "retry_after_policy": normalize_runtime_text(record.get("retry_after_policy"))
        or "required_source_retry",
        "resolution_status": normalize_runtime_text(record.get("resolution_status")) or RESOLUTION_STATUS_UNRESOLVED,
        "resolved_at": normalize_runtime_text(record.get("resolved_at")),
        "resolved_by_run_id": normalize_runtime_text(record.get("resolved_by_run_id")),
        "resolution_source_status": normalize_runtime_text(record.get("resolution_source_status")),
        "resolution_detail": normalize_runtime_text(record.get("resolution_detail")),
    }
    return normalized


def normalize_required_source_deferred_state(payload: Any) -> dict[str, Any]:
    if not isinstance(payload, Mapping):
        return required_source_deferred_state()
    raw_records = payload.get("records")
    if isinstance(raw_records, Mapping):
        record_items = raw_records.values()
    elif isinstance(raw_records, list):
        record_items = raw_records
    else:
        record_items = []

    records: dict[str, dict[str, Any]] = {}
    for raw_record in record_items:
        record = _normalize_record(raw_record)
        if record is None:
            continue
        records[_record_key(inn=record["inn"], source=record["source"])] = record

    raw_health = payload.get("source_health")
    source_health: dict[str, dict[str, Any]] = {}
    if isinstance(raw_health, Mapping):
        for source_name, health_payload in raw_health.items():
            source = normalize_runtime_text(source_name)
            health = dict(health_payload) if isinstance(health_payload, Mapping) else {}
            if not source:
                continue
            source_health[source] = {
                "consecutive_deferred_rows": max(
                    _normalize_int(health.get("consecutive_deferred_rows")),
                    0,
                ),
                "deferred_rows_total": max(_normalize_int(health.get("deferred_rows_total")), 0),
                "success_after_deferred_count": max(
                    _normalize_int(health.get("success_after_deferred_count")),
                    0,
                ),
                "first_deferred_at": normalize_runtime_text(health.get("first_deferred_at")),
                "last_deferred_at": normalize_runtime_text(health.get("last_deferred_at")),
                "last_success_at": normalize_runtime_text(health.get("last_success_at")),
            }

    for record in records.values():
        source_health.setdefault(
            record["source"],
            {
                "consecutive_deferred_rows": 0,
                "deferred_rows_total": 0,
                "success_after_deferred_count": 0,
                "first_deferred_at": "",
                "last_deferred_at": "",
                "last_success_at": "",
            },
        )

    return {
        "contract_version": DEFERRED_REQUIRED_SOURCE_CONTRACT_VERSION,
        "records": records,
        "source_health": source_health,
    }


def unresolved_required_source_deferred_records(payload: Any) -> list[dict[str, Any]]:
    state = normalize_required_source_deferred_state(payload)
    return sorted(
        (
            dict(record)
            for record in state["records"].values()
            if record.get("resolution_status") == RESOLUTION_STATUS_UNRESOLVED
        ),
        key=lambda item: (int(item.get("row_index", 0) or 0), str(item.get("inn", "")), str(item.get("source", ""))),
    )


def build_required_source_deferred_summary_fields(payload: Any) -> dict[str, Any]:
    state = normalize_required_source_deferred_state(payload)
    unresolved = unresolved_required_source_deferred_records(state)
    by_source = Counter(str(record.get("source", "")) for record in unresolved if record.get("source"))
    by_status = Counter(str(record.get("status", "")) for record in unresolved if record.get("status"))
    unresolved_inns = {str(record.get("inn", "")) for record in unresolved if record.get("inn")}
    return {
        DEFERRED_REQUIRED_SOURCES_KEY: state,
        REQUIRED_SOURCE_DEFERRED_ROWS_TOTAL_KEY: len(unresolved),
        REQUIRED_SOURCE_DEFERRED_ROWS_BY_SOURCE_KEY: dict(sorted(by_source.items())),
        REQUIRED_SOURCE_DEFERRED_ROWS_BY_STATUS_KEY: dict(sorted(by_status.items())),
        UNRESOLVED_REQUIRED_SOURCE_ROWS_KEY: len(unresolved_inns),
    }


def build_required_source_deferred_record(
    *,
    source: str,
    access_mode: str,
    status: str,
    error: str,
    row_index: int,
    inn: str,
    company_name: str,
    run_id: str,
    now_iso: str,
    attempt_budget: str = "source_default_finite",
    observed_attempt_count: str = "",
    retry_after_policy: str = "required_source_retry",
) -> dict[str, Any]:
    detail = normalize_runtime_text(error) or normalize_runtime_text(status)
    return {
        "source": normalize_runtime_text(source),
        "access_mode": normalize_runtime_text(access_mode),
        "status": normalize_runtime_text(status) or "unknown_status",
        "error": detail,
        "detail": detail,
        "row_index": max(int(row_index or 0), 0),
        "inn": normalize_runtime_text(inn),
        "company_name": normalize_runtime_text(company_name),
        "run_id": normalize_runtime_text(run_id),
        "first_seen_at": normalize_runtime_text(now_iso),
        "last_seen_at": normalize_runtime_text(now_iso),
        "attempt_budget": normalize_runtime_text(attempt_budget) or "source_default_finite",
        "observed_attempt_count": normalize_runtime_text(observed_attempt_count),
        "retry_after_policy": normalize_runtime_text(retry_after_policy) or "required_source_retry",
        "resolution_status": RESOLUTION_STATUS_UNRESOLVED,
    }


def record_required_source_deferred(payload: Any, record_payload: Mapping[str, Any]) -> dict[str, Any]:
    state = normalize_required_source_deferred_state(payload)
    record = _normalize_record(record_payload)
    if record is None:
        return state
    key = _record_key(inn=record["inn"], source=record["source"])
    existing = state["records"].get(key)
    if existing:
        record["first_seen_at"] = existing.get("first_seen_at") or record["first_seen_at"]
    state["records"][key] = record

    health = dict(state["source_health"].get(record["source"]) or {})
    first_deferred_at = normalize_runtime_text(health.get("first_deferred_at")) or record["first_seen_at"]
    health["first_deferred_at"] = first_deferred_at
    health["last_deferred_at"] = record["last_seen_at"]
    if not existing:
        health["consecutive_deferred_rows"] = max(
            _normalize_int(health.get("consecutive_deferred_rows")),
            0,
        ) + 1
        health["deferred_rows_total"] = max(_normalize_int(health.get("deferred_rows_total")), 0) + 1
    else:
        health["consecutive_deferred_rows"] = max(
            _normalize_int(health.get("consecutive_deferred_rows")),
            0,
        )
        health["deferred_rows_total"] = max(_normalize_int(health.get("deferred_rows_total")), 0)
    health.setdefault("success_after_deferred_count", 0)
    health.setdefault("last_success_at", "")
    state["source_health"][record["source"]] = health
    return normalize_required_source_deferred_state(state)


def mark_required_source_success(payload: Any, *, source: str, now_iso: str) -> tuple[dict[str, Any], bool]:
    state = normalize_required_source_deferred_state(payload)
    normalized_source = normalize_runtime_text(source)
    if not normalized_source:
        return state, False
    unresolved_for_source = [
        record
        for record in state["records"].values()
        if record.get("source") == normalized_source
        and record.get("resolution_status") == RESOLUTION_STATUS_UNRESOLVED
    ]
    health = dict(state["source_health"].get(normalized_source) or {})
    if not unresolved_for_source and not health:
        return state, False
    before = dict(health)
    health["consecutive_deferred_rows"] = 0
    if unresolved_for_source:
        health["success_after_deferred_count"] = max(
            _normalize_int(health.get("success_after_deferred_count")),
            0,
        ) + 1
    else:
        health["success_after_deferred_count"] = max(
            _normalize_int(health.get("success_after_deferred_count")),
            0,
        )
    health["last_success_at"] = normalize_runtime_text(now_iso)
    health.setdefault("deferred_rows_total", 0)
    health.setdefault("first_deferred_at", "")
    health.setdefault("last_deferred_at", "")
    state["source_health"][normalized_source] = health
    return normalize_required_source_deferred_state(state), health != before


def mark_required_source_deferred_record_resolved(
    payload: Any,
    *,
    inn: str,
    source: str,
    now_iso: str,
    resolved_by_run_id: str = "",
    source_status: str = "",
    detail: str = "",
) -> tuple[dict[str, Any], bool]:
    state = normalize_required_source_deferred_state(payload)
    normalized_inn = normalize_runtime_text(inn)
    normalized_source = normalize_runtime_text(source)
    if not normalized_inn or not normalized_source:
        return state, False
    key = _record_key(inn=normalized_inn, source=normalized_source)
    record = dict(state["records"].get(key) or {})
    if not record or record.get("resolution_status") != RESOLUTION_STATUS_UNRESOLVED:
        return state, False
    record["resolution_status"] = RESOLUTION_STATUS_RESOLVED
    record["resolved_at"] = normalize_runtime_text(now_iso)
    record["resolved_by_run_id"] = normalize_runtime_text(resolved_by_run_id)
    record["resolution_source_status"] = normalize_runtime_text(source_status)
    record["resolution_detail"] = normalize_runtime_text(detail) or "retry promoted completed company result"
    state["records"][key] = record
    return normalize_required_source_deferred_state(state), True


def unresolved_deferred_sources_without_later_success(payload: Any) -> list[str]:
    state = normalize_required_source_deferred_state(payload)
    sources = {
        str(record.get("source", ""))
        for record in unresolved_required_source_deferred_records(state)
        if record.get("source")
    }
    missing_success: list[str] = []
    for source in sorted(sources):
        health = state["source_health"].get(source) or {}
        if _normalize_int(health.get("success_after_deferred_count")) <= 0:
            missing_success.append(source)
    return missing_success


def first_unresolved_record_for_source(payload: Any, source: str) -> dict[str, Any] | None:
    normalized_source = normalize_runtime_text(source)
    for record in unresolved_required_source_deferred_records(payload):
        if record.get("source") == normalized_source:
            return record
    return None


def _status_is_source_wide_stop(*, source: str, access_mode: str, status: str) -> bool:
    if status in SYSTEMIC_STOP_STATUSES:
        return True
    if source == "checko" and access_mode and access_mode != "proxy-bound":
        return True
    if access_mode in {"proxy-bound", "session-bound"} and status == "guest":
        return True
    return False


def _detail_has_transient_marker(detail: str) -> bool:
    normalized_detail = normalize_runtime_text(detail).casefold()
    return any(marker in normalized_detail for marker in TRANSIENT_DETAIL_MARKERS)


def _is_approved_blocked_transient(
    *,
    source: str,
    access_mode: str,
    status: str,
    detail: str,
) -> bool:
    if status != "blocked" or access_mode != "direct-default":
        return False
    approved_markers = SOURCE_APPROVED_BLOCKED_TRANSIENT_DETAIL_MARKERS.get(source, frozenset())
    if not approved_markers:
        return False
    normalized_detail = normalize_runtime_text(detail).casefold()
    return any(marker in normalized_detail for marker in approved_markers)


def _is_deferable_transient(*, source: str, status: str, detail: str) -> bool:
    if status == "request_error":
        return _detail_has_transient_marker(detail)
    if status in TRANSIENT_STATUS_ALLOWLIST:
        return True
    approved_http_statuses = SOURCE_APPROVED_TRANSIENT_HTTP_STATUSES.get(source, frozenset())
    return status in approved_http_statuses and _detail_has_transient_marker(detail)


def classify_required_source_outcome(
    *,
    source: str,
    access_mode: str,
    status: str,
    detail: str,
    defer_enabled: bool,
    selected_rows: int,
    deferred_state: Any,
    retry_existing_deferred: bool = False,
) -> RequiredSourceClassification:
    normalized_source = normalize_runtime_text(source)
    normalized_access_mode = normalize_runtime_text(access_mode)
    normalized_status = normalize_runtime_text(status) or "unknown_status"
    normalized_detail = normalize_runtime_text(detail) or normalized_status
    if not defer_enabled:
        return RequiredSourceClassification(
            outcome=OUTCOME_NON_DEFER_FAIL_FAST,
            reason="required source deferral is disabled",
        )
    approved_blocked_transient = _is_approved_blocked_transient(
        source=normalized_source,
        access_mode=normalized_access_mode,
        status=normalized_status,
        detail=normalized_detail,
    )
    if _status_is_source_wide_stop(
        source=normalized_source,
        access_mode=normalized_access_mode,
        status=normalized_status,
    ) and not approved_blocked_transient:
        return RequiredSourceClassification(
            outcome=OUTCOME_SYSTEMIC_STOP,
            reason=f"source-wide required-source stop status: {normalized_status}",
        )
    if not approved_blocked_transient and not _is_deferable_transient(
        source=normalized_source,
        status=normalized_status,
        detail=normalized_detail,
    ):
        return RequiredSourceClassification(
            outcome=OUTCOME_NON_DEFER_FAIL_FAST,
            reason=f"required-source failure is not an eligible row transient: {normalized_status}",
        )
    if retry_existing_deferred:
        return RequiredSourceClassification(
            outcome=OUTCOME_DEFERRED_ROW_TRANSIENT,
            reason=f"retry kept existing deferred required-source row unresolved: {normalized_status}",
        )

    state = normalize_required_source_deferred_state(deferred_state)
    cap = deferred_required_source_cap(selected_rows)
    unresolved_for_source = [
        record
        for record in unresolved_required_source_deferred_records(state)
        if record.get("source") == normalized_source
    ]
    source_health = state["source_health"].get(normalized_source) or {}
    consecutive_deferred_rows = max(
        _normalize_int(source_health.get("consecutive_deferred_rows")),
        0,
    )
    if consecutive_deferred_rows >= cap:
        return RequiredSourceClassification(
            outcome=OUTCOME_SYSTEMIC_STOP,
            reason=(
                f"deferred required-source consecutive cap exceeded for {normalized_source}: "
                f"consecutive={consecutive_deferred_rows} cap={cap}"
            ),
        )
    next_deferred_count = consecutive_deferred_rows + 1
    return RequiredSourceClassification(
        outcome=OUTCOME_DEFERRED_ROW_TRANSIENT,
        reason=(
            f"eligible required-source row transient after finite source retries: {normalized_status}; "
            f"consecutive_deferred_count_for_source={next_deferred_count}/{cap}; "
            f"unresolved_deferred_count_for_source={len(unresolved_for_source) + 1}"
        ),
    )

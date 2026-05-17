from __future__ import annotations

import json
import logging
from collections.abc import Callable
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from .host_memory import normalize_host_memory_state
from .handoff import normalize_stage_handoff_state
from .handoff_queue import synchronize_stage_handoff_pickup_state
from .required_source_deferred import (
    DEFERRED_REQUIRED_SOURCES_KEY,
    REQUIRED_SOURCE_DEFERRED_ROWS_BY_SOURCE_KEY,
    REQUIRED_SOURCE_DEFERRED_ROWS_BY_STATUS_KEY,
    REQUIRED_SOURCE_DEFERRED_ROWS_TOTAL_KEY,
    UNRESOLVED_REQUIRED_SOURCE_ROWS_KEY,
    build_required_source_deferred_summary_fields,
    normalize_required_source_deferred_state,
)
from .stage_messages import normalize_stage_outbox_cursor
from .work_units import normalize_explicit_stage_execution_state, synchronize_stage_work_unit_state


RUNTIME_STATE_FILENAME = "runtime_state.json"
RUNTIME_STATE_CONTRACT_VERSION = 2
RUNTIME_STATE_LEGACY_CONTRACT_VERSION = 1
RUNTIME_STATE_DIAGNOSTIC_LOGGER = logging.getLogger("company_research_parser")
COMPANY_ENTRY_RUNTIME_RESULT_FIELDS = ("status", "started_at", "finished_at")
RUNTIME_SUMMARY_DERIVED_ONLY_FIELDS = ("run_id",)
STAGE_EXECUTION_EVIDENCE_KEY = "stage_execution_evidence"
THROUGHPUT_TELEMETRY_KEY = "throughput_telemetry"


@dataclass(slots=True)
class RuntimeStateSnapshot:
    results: dict[str, dict[str, Any]]
    summary: dict[str, Any]
    host_stats: dict[str, Any]
    host_memory: dict[str, Any] = field(default_factory=dict)
    run_metadata: dict[str, Any] = field(default_factory=dict)
    loaded_from_state: bool = False
    loaded_from_legacy: bool = False


def ordered_runtime_results(results: dict[str, dict[str, Any]]) -> list[dict[str, Any]]:
    return sorted(results.values(), key=lambda item: (item.get("row_index", 0), item.get("inn", "")))


def build_runtime_state_payload(
    *,
    ordered_results: list[dict[str, Any]],
    summary: dict[str, Any],
    host_stats: dict[str, Any],
    host_memory: dict[str, Any],
    run_metadata: dict[str, Any],
) -> dict[str, Any]:
    summary_payload = _runtime_summary_payload(summary)
    return {
        "runtime_state_contract_version": RUNTIME_STATE_CONTRACT_VERSION,
        "run": {
            "metadata": _normalize_run_metadata_payload(run_metadata, summary=summary),
            "summary": summary_payload,
            "host_stats": dict(host_stats),
            "host_memory": normalize_host_memory_state(host_memory),
        },
        "company_entries": [_build_company_runtime_entry(item) for item in ordered_results],
    }


def load_runtime_state_snapshot(
    *,
    runtime_state_path: Path,
    legacy_results_path: Path,
    legacy_summary_path: Path,
    legacy_host_stats_path: Path,
    normalize_result_payload: Callable[[dict[str, Any]], dict[str, Any]],
) -> RuntimeStateSnapshot:
    if runtime_state_path.exists():
        snapshot, runtime_state_issue = _load_runtime_state_only(
            runtime_state_path,
            normalize_result_payload=normalize_result_payload,
        )
        if snapshot is not None:
            snapshot.loaded_from_state = True
            return snapshot
        legacy_snapshot = _load_legacy_snapshot(
            legacy_results_path=legacy_results_path,
            legacy_summary_path=legacy_summary_path,
            legacy_host_stats_path=legacy_host_stats_path,
            normalize_result_payload=normalize_result_payload,
        )
        RUNTIME_STATE_DIAGNOSTIC_LOGGER.warning(
            "Ignoring runtime_state.json and %s: path=%s reason=%s",
            "falling back to legacy artifacts" if legacy_snapshot.loaded_from_legacy else "starting with empty runtime state",
            runtime_state_path,
            runtime_state_issue or "unknown_runtime_state_error",
        )
        return legacy_snapshot
    return _load_legacy_snapshot(
        legacy_results_path=legacy_results_path,
        legacy_summary_path=legacy_summary_path,
        legacy_host_stats_path=legacy_host_stats_path,
        normalize_result_payload=normalize_result_payload,
    )


def runtime_state_snapshot_from_payload(
    payload: Any,
    *,
    normalize_result_payload: Callable[[dict[str, Any]], dict[str, Any]],
) -> tuple[RuntimeStateSnapshot | None, str | None]:
    if not isinstance(payload, dict):
        return None, "payload_not_object"
    contract_version = payload.get("runtime_state_contract_version")
    if contract_version == RUNTIME_STATE_CONTRACT_VERSION:
        return _runtime_state_snapshot_from_v2_payload(
            payload,
            normalize_result_payload=normalize_result_payload,
        )
    if contract_version == RUNTIME_STATE_LEGACY_CONTRACT_VERSION:
        return _runtime_state_snapshot_from_v1_payload(
            payload,
            normalize_result_payload=normalize_result_payload,
        )
    return None, f"incompatible_contract_version:{contract_version!r}"


def _runtime_state_snapshot_from_v2_payload(
    payload: dict[str, Any],
    *,
    normalize_result_payload: Callable[[dict[str, Any]], dict[str, Any]],
) -> tuple[RuntimeStateSnapshot | None, str | None]:
    run_payload = payload.get("run")
    if not isinstance(run_payload, dict):
        return None, "missing_or_invalid_run"
    summary_payload = run_payload.get("summary")
    if not isinstance(summary_payload, dict):
        return None, "missing_or_invalid_summary"
    host_stats_payload = run_payload.get("host_stats")
    if not isinstance(host_stats_payload, dict):
        return None, "missing_or_invalid_host_stats"
    host_memory_payload = run_payload.get("host_memory")
    if host_memory_payload is not None and not isinstance(host_memory_payload, dict):
        return None, "missing_or_invalid_host_memory"
    metadata_payload = run_payload.get("metadata")
    if metadata_payload is not None and not isinstance(metadata_payload, dict):
        return None, "missing_or_invalid_run_metadata"
    company_entries_payload = payload.get("company_entries")
    results, payload_issue = _strict_company_entries_by_inn(
        company_entries_payload,
        normalize_result_payload=normalize_result_payload,
    )
    if payload_issue is not None:
        return None, payload_issue
    return RuntimeStateSnapshot(
        results=results,
        summary=dict(summary_payload),
        host_stats=dict(host_stats_payload),
        host_memory=normalize_host_memory_state(host_memory_payload),
        run_metadata=_normalize_run_metadata_payload(metadata_payload, summary=summary_payload),
    ), None


def _runtime_state_snapshot_from_v1_payload(
    payload: dict[str, Any],
    *,
    normalize_result_payload: Callable[[dict[str, Any]], dict[str, Any]],
) -> tuple[RuntimeStateSnapshot | None, str | None]:
    if "summary" not in payload or not isinstance(payload.get("summary"), dict):
        return None, "missing_or_invalid_summary"
    if "host_stats" not in payload or not isinstance(payload.get("host_stats"), dict):
        return None, "missing_or_invalid_host_stats"
    if "companies" not in payload and "results" not in payload:
        return None, "missing_companies"
    companies_payload = payload.get("companies", payload.get("results"))
    results, payload_issue = _strict_results_by_inn(
        companies_payload,
        normalize_result_payload=normalize_result_payload,
    )
    if payload_issue is not None:
        return None, payload_issue
    return RuntimeStateSnapshot(
        results=results,
        summary=dict(payload["summary"]),
        host_stats=dict(payload["host_stats"]),
        host_memory={},
        run_metadata=_normalize_run_metadata_payload({}, summary=payload["summary"]),
    ), None


def _results_by_inn(
    payload: Any,
    *,
    normalize_result_payload: Callable[[dict[str, Any]], dict[str, Any]],
) -> dict[str, dict[str, Any]]:
    if isinstance(payload, dict):
        items = payload.values()
    elif isinstance(payload, list):
        items = payload
    else:
        items = []
    results: dict[str, dict[str, Any]] = {}
    for item in items:
        if not isinstance(item, dict):
            continue
        normalized = normalize_result_payload(item)
        inn = str(normalized.get("inn", "") or "")
        if inn:
            results[inn] = normalized
    return results


def _strict_results_by_inn(
    payload: Any,
    *,
    normalize_result_payload: Callable[[dict[str, Any]], dict[str, Any]],
) -> tuple[dict[str, dict[str, Any]], str | None]:
    if isinstance(payload, dict):
        items = list(payload.values())
    elif isinstance(payload, list):
        items = payload
    else:
        return {}, "invalid_companies_payload"
    results: dict[str, dict[str, Any]] = {}
    for index, item in enumerate(items):
        if not isinstance(item, dict):
            return {}, f"invalid_company_item:{index}"
        normalized = normalize_result_payload(item)
        inn = str(normalized.get("inn", "") or "")
        if not inn:
            return {}, f"missing_company_inn:{index}"
        if inn in results:
            return {}, f"duplicate_company_inn:{inn}"
        results[inn] = normalized
    return results, None


def _build_company_runtime_entry(result_payload: dict[str, Any]) -> dict[str, Any]:
    normalized_result = dict(result_payload)
    return {
        "company": {
            "inn": str(normalized_result.get("inn", "") or ""),
            "row_index": int(normalized_result.get("row_index", 0) or 0),
            "company_name": str(normalized_result.get("company_name", "") or ""),
        },
        "runtime": _company_runtime_payload_from_result(normalized_result),
        "result": _result_payload_without_company_runtime(normalized_result),
    }


def _strict_company_entries_by_inn(
    payload: Any,
    *,
    normalize_result_payload: Callable[[dict[str, Any]], dict[str, Any]],
) -> tuple[dict[str, dict[str, Any]], str | None]:
    if not isinstance(payload, list):
        return {}, "invalid_company_entries_payload"
    results: dict[str, dict[str, Any]] = {}
    for index, item in enumerate(payload):
        if not isinstance(item, dict):
            return {}, f"invalid_company_entry:{index}"
        company_payload = item.get("company")
        if company_payload is not None and not isinstance(company_payload, dict):
            return {}, f"invalid_company_entry_company:{index}"
        runtime_payload = item.get("runtime")
        if runtime_payload is not None and not isinstance(runtime_payload, dict):
            return {}, f"invalid_company_entry_runtime:{index}"
        result_payload = item.get("result")
        if not isinstance(result_payload, dict):
            return {}, f"invalid_company_entry_result:{index}"
        normalized = normalize_result_payload(
            _result_payload_with_company_runtime(
                result_payload,
                runtime_payload=runtime_payload,
            )
        )
        company_payload = company_payload or {}
        inn = str(company_payload.get("inn") or normalized.get("inn") or "")
        if not inn:
            return {}, f"missing_company_inn:{index}"
        normalized_inn = str(normalized.get("inn", "") or "")
        if normalized_inn and normalized_inn != inn:
            return {}, f"company_entry_inn_mismatch:{inn}"
        if company_payload.get("row_index") not in (None, ""):
            try:
                company_row_index = int(company_payload.get("row_index"))
            except (TypeError, ValueError):
                return {}, f"invalid_company_entry_row_index:{index}"
            normalized_row_index = normalized.get("row_index")
            if normalized_row_index not in (None, "", 0):
                try:
                    if int(normalized_row_index) != company_row_index:
                        return {}, f"company_entry_row_index_mismatch:{inn}"
                except (TypeError, ValueError):
                    return {}, f"invalid_company_row_index:{inn}"
            else:
                normalized["row_index"] = company_row_index
        if company_payload.get("company_name") and not normalized.get("company_name"):
            normalized["company_name"] = str(company_payload.get("company_name") or "")
        normalized["inn"] = inn
        if inn in results:
            return {}, f"duplicate_company_inn:{inn}"
        results[inn] = normalized
    return results, None


def _normalize_run_metadata_payload(
    payload: Any,
    *,
    summary: dict[str, Any],
) -> dict[str, Any]:
    metadata = dict(payload) if isinstance(payload, dict) else {}
    stage_handoffs = normalize_stage_handoff_state(metadata.get("stage_handoffs"))
    stage_pickups = synchronize_stage_handoff_pickup_state(
        metadata.get("stage_pickups"),
        stage_handoffs,
    )
    stage_execution_evidence = normalize_explicit_stage_execution_state(
        metadata.get(STAGE_EXECUTION_EVIDENCE_KEY),
    )
    stage_work_units = synchronize_stage_work_unit_state(
        metadata.get("stage_work_units"),
        stage_handoffs,
        stage_pickups,
        explicit_execution_state=stage_execution_evidence,
    )
    required_source_deferred_state = normalize_required_source_deferred_state(
        metadata.get(DEFERRED_REQUIRED_SOURCES_KEY, summary.get(DEFERRED_REQUIRED_SOURCES_KEY))
    )
    required_source_deferred_summary = build_required_source_deferred_summary_fields(
        required_source_deferred_state
    )
    return {
        "run_id": str(metadata.get("run_id", summary.get("run_id", "")) or ""),
        "input_path": str(metadata.get("input_path", "") or ""),
        "total_rows": _normalize_int(metadata.get("total_rows"), fallback=summary.get("total_rows")),
        "rows_selected": _normalize_int(metadata.get("rows_selected"), fallback=summary.get("rows_selected")),
        "selection_mode": str(metadata.get("selection_mode", summary.get("selection_mode", "")) or ""),
        "selected_ordinals": _normalize_int_list(
            metadata.get("selected_ordinals", summary.get("selected_ordinals"))
        ),
        "start_from": _normalize_int(metadata.get("start_from"), fallback=summary.get("start_from"), default=1),
        "end_at": _normalize_optional_int(metadata.get("end_at", summary.get("end_at"))),
        "active_sources": _normalize_string_list(metadata.get("active_sources", summary.get("active_sources"))),
        "retry_blocked_source": str(metadata.get("retry_blocked_source", "") or ""),
        "resume_skipped_rows": _normalize_int(
            metadata.get("resume_skipped_rows"),
            fallback=summary.get("resume_skipped_rows"),
        ),
        "source_lane_scheduler": _normalize_json_mapping(metadata.get("source_lane_scheduler")),
        "downstream_worker_pools": _normalize_json_mapping(metadata.get("downstream_worker_pools")),
        THROUGHPUT_TELEMETRY_KEY: _normalize_json_mapping(
            metadata.get(THROUGHPUT_TELEMETRY_KEY, summary.get(THROUGHPUT_TELEMETRY_KEY))
        ),
        "stage_outbox_cursor": normalize_stage_outbox_cursor(metadata.get("stage_outbox_cursor")),
        "stage_handoffs": stage_handoffs,
        "stage_pickups": stage_pickups,
        STAGE_EXECUTION_EVIDENCE_KEY: stage_execution_evidence,
        "stage_work_units": stage_work_units,
        DEFERRED_REQUIRED_SOURCES_KEY: required_source_deferred_state,
        REQUIRED_SOURCE_DEFERRED_ROWS_TOTAL_KEY: required_source_deferred_summary[
            REQUIRED_SOURCE_DEFERRED_ROWS_TOTAL_KEY
        ],
        REQUIRED_SOURCE_DEFERRED_ROWS_BY_SOURCE_KEY: required_source_deferred_summary[
            REQUIRED_SOURCE_DEFERRED_ROWS_BY_SOURCE_KEY
        ],
        REQUIRED_SOURCE_DEFERRED_ROWS_BY_STATUS_KEY: required_source_deferred_summary[
            REQUIRED_SOURCE_DEFERRED_ROWS_BY_STATUS_KEY
        ],
        UNRESOLVED_REQUIRED_SOURCE_ROWS_KEY: required_source_deferred_summary[
            UNRESOLVED_REQUIRED_SOURCE_ROWS_KEY
        ],
        "started_at": str(metadata.get("started_at", "") or ""),
        "updated_at": str(metadata.get("updated_at", summary.get("updated_at", "")) or ""),
        "run_status": str(metadata.get("run_status", summary.get("run_status", "")) or ""),
        "finish_reason": str(metadata.get("finish_reason", summary.get("finish_reason", "")) or ""),
        "finished_at": str(metadata.get("finished_at", summary.get("finished_at", "")) or ""),
        "stop_requested_at": str(metadata.get("stop_requested_at", summary.get("stop_requested_at", "")) or ""),
        "stop_reason": str(metadata.get("stop_reason", summary.get("stop_reason", "")) or ""),
        "terminal_checkpoint": str(
            metadata.get("terminal_checkpoint", summary.get("terminal_checkpoint", "")) or ""
        ),
        "terminal_inn": str(metadata.get("terminal_inn", summary.get("terminal_inn", "")) or ""),
        "terminal_boundary": str(metadata.get("terminal_boundary", summary.get("terminal_boundary", "")) or ""),
        "terminal_source": str(metadata.get("terminal_source", summary.get("terminal_source", "")) or ""),
        "terminal_source_status": str(
            metadata.get("terminal_source_status", summary.get("terminal_source_status", "")) or ""
        ),
        "terminal_source_access_mode": str(
            metadata.get("terminal_source_access_mode", summary.get("terminal_source_access_mode", "")) or ""
        ),
        "terminal_error_type": str(
            metadata.get("terminal_error_type", summary.get("terminal_error_type", "")) or ""
        ),
        "terminal_error_message": str(
            metadata.get("terminal_error_message", summary.get("terminal_error_message", "")) or ""
        ),
    }


def _runtime_summary_payload(summary: dict[str, Any]) -> dict[str, Any]:
    payload = dict(summary)
    for field_name in RUNTIME_SUMMARY_DERIVED_ONLY_FIELDS:
        payload.pop(field_name, None)
    return payload


def _normalize_int(value: Any, *, fallback: Any = None, default: int = 0) -> int:
    candidate = fallback if value in (None, "") else value
    try:
        return int(candidate)
    except (TypeError, ValueError):
        return default


def _normalize_optional_int(value: Any) -> int | None:
    if value in (None, ""):
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _normalize_int_list(value: Any) -> list[int]:
    if not isinstance(value, list):
        return []
    normalized: list[int] = []
    seen: set[int] = set()
    for item in value:
        try:
            ordinal = int(item)
        except (TypeError, ValueError):
            continue
        if ordinal in seen:
            continue
        seen.add(ordinal)
        normalized.append(ordinal)
    return normalized


def _normalize_string_list(value: Any) -> list[str]:
    if not isinstance(value, list):
        return []
    normalized: list[str] = []
    seen: set[str] = set()
    for item in value:
        item_text = str(item or "")
        if not item_text or item_text in seen:
            continue
        seen.add(item_text)
        normalized.append(item_text)
    return normalized


def _normalize_json_mapping(value: Any) -> dict[str, Any]:
    return _normalize_json_value(value) if isinstance(value, dict) else {}


def _normalize_json_value(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(key): _normalize_json_value(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_normalize_json_value(item) for item in value]
    if isinstance(value, tuple):
        return [_normalize_json_value(item) for item in value]
    return value


def _company_runtime_payload_from_result(result_payload: dict[str, Any]) -> dict[str, Any]:
    status = str(result_payload.get("status", "") or "")
    started_at = str(result_payload.get("started_at", "") or "")
    finished_at = str(result_payload.get("finished_at", "") or "")
    return {
        "status": status,
        "started_at": started_at,
        "finished_at": finished_at,
        "updated_at": str(finished_at or started_at or ""),
    }


def _result_payload_without_company_runtime(result_payload: dict[str, Any]) -> dict[str, Any]:
    normalized = dict(result_payload)
    for field_name in COMPANY_ENTRY_RUNTIME_RESULT_FIELDS:
        normalized.pop(field_name, None)
    return normalized


def _result_payload_with_company_runtime(
    result_payload: dict[str, Any],
    *,
    runtime_payload: Any,
) -> dict[str, Any]:
    normalized = dict(result_payload)
    runtime = dict(runtime_payload) if isinstance(runtime_payload, dict) else {}
    normalized["status"] = str(runtime.get("status", normalized.get("status", "")) or "")
    normalized["started_at"] = str(runtime.get("started_at", normalized.get("started_at", "")) or "")
    normalized["finished_at"] = str(runtime.get("finished_at", normalized.get("finished_at", "")) or "")
    return normalized


def _load_runtime_state_only(
    runtime_state_path: Path,
    *,
    normalize_result_payload: Callable[[dict[str, Any]], dict[str, Any]],
) -> tuple[RuntimeStateSnapshot | None, str | None]:
    try:
        payload = json.loads(runtime_state_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        return None, f"malformed_json:{exc.msg}"
    except OSError as exc:
        return None, f"unreadable_runtime_state:{exc.__class__.__name__}"
    except Exception as exc:
        return None, f"unreadable_runtime_state:{exc.__class__.__name__}"
    return runtime_state_snapshot_from_payload(payload, normalize_result_payload=normalize_result_payload)


def _load_legacy_snapshot(
    *,
    legacy_results_path: Path,
    legacy_summary_path: Path,
    legacy_host_stats_path: Path,
    normalize_result_payload: Callable[[dict[str, Any]], dict[str, Any]],
) -> RuntimeStateSnapshot:
    results = _load_legacy_results(legacy_results_path, normalize_result_payload=normalize_result_payload)
    summary = _load_legacy_mapping(legacy_summary_path)
    host_stats = _load_legacy_mapping(legacy_host_stats_path)
    return RuntimeStateSnapshot(
        results=results,
        summary=summary,
        host_stats=host_stats,
        host_memory={},
        run_metadata=_normalize_run_metadata_payload({}, summary=summary),
        loaded_from_legacy=bool(results or summary or host_stats),
    )


def _load_legacy_results(
    path: Path,
    *,
    normalize_result_payload: Callable[[dict[str, Any]], dict[str, Any]],
) -> dict[str, dict[str, Any]]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    if not isinstance(payload, list):
        return {}
    return _results_by_inn(payload, normalize_result_payload=normalize_result_payload)


def _load_legacy_mapping(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return dict(payload) if isinstance(payload, dict) else {}


__all__ = [
    "RUNTIME_STATE_CONTRACT_VERSION",
    "RUNTIME_STATE_FILENAME",
    "RUNTIME_SUMMARY_DERIVED_ONLY_FIELDS",
    "STAGE_EXECUTION_EVIDENCE_KEY",
    "THROUGHPUT_TELEMETRY_KEY",
    "RuntimeStateSnapshot",
    "build_runtime_state_payload",
    "load_runtime_state_snapshot",
    "ordered_runtime_results",
]

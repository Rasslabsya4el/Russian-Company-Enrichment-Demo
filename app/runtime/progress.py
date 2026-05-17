from __future__ import annotations

import errno
import importlib
import json
import logging
import shutil
import time
import uuid
from collections.abc import Callable, Iterable, Mapping
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .files import ensure_dir
from .handoff import (
    AGGREGATOR_SITE_HANDOFF_KEY,
    apply_stage_messages_to_handoff_state,
    build_stage_handoff_state,
    normalize_stage_handoff_state,
)
from .host_memory import (
    HOST_MEMORY_GOVERNOR_SIGNAL_TAGS,
    recent_governor_signal_proxy_labels as recent_governor_signal_proxy_labels_from_memory,
    recent_host_proxy_outcomes as recent_host_proxy_outcomes_from_memory,
    update_host_memory_from_event_payload,
)
from .required_source_deferred import (
    DEFERRED_REQUIRED_SOURCES_KEY,
    REQUIRED_SOURCE_DEFERRED_ROWS_BY_SOURCE_KEY,
    REQUIRED_SOURCE_DEFERRED_ROWS_BY_STATUS_KEY,
    REQUIRED_SOURCE_DEFERRED_ROWS_TOTAL_KEY,
    UNRESOLVED_REQUIRED_SOURCE_ROWS_KEY,
    build_required_source_deferred_summary_fields,
    mark_required_source_deferred_record_resolved,
    mark_required_source_success,
    normalize_required_source_deferred_state,
    record_required_source_deferred,
    required_source_deferred_state,
    unresolved_deferred_sources_without_later_success,
    unresolved_required_source_deferred_records,
)
from .handoff_queue import (
    build_stage_handoff_pickup_state,
    normalize_stage_handoff_pickup_state,
    pickup_ready_stage_handoffs as pickup_ready_stage_handoffs_from_state,
    synchronize_stage_handoff_pickup_state,
)
from .run_state import RuntimeRunState
from .stage_messages import (
    RUNTIME_PRIVATE_DIRNAME,
    append_stage_message as append_stage_message_to_outbox,
    build_stage_outbox_cursor,
    build_stage_message,
    normalize_stage_outbox_cursor,
    read_unconsumed_stage_messages,
    stage_message_outbox_path,
)
from .state import (
    RUNTIME_STATE_FILENAME,
    RUNTIME_SUMMARY_DERIVED_ONLY_FIELDS,
    STAGE_EXECUTION_EVIDENCE_KEY,
    THROUGHPUT_TELEMETRY_KEY,
    load_runtime_state_snapshot,
    ordered_runtime_results,
)
from .work_units import (
    AGGREGATOR_SITE_EXECUTION_BOUNDARY,
    STAGE_WORK_UNIT_SURFACE_KEYS,
    WORK_STATUS_ACKED,
    acknowledge_stage_work_unit,
    build_stage_work_unit_state,
    materialize_pickup_ready_stage_work_units,
    merge_stage_work_unit_private_state,
    normalize_stage_work_unit_state,
    normalize_explicit_stage_execution_state,
    pending_stage_work_units,
    synchronize_stage_work_unit_state,
    upsert_explicit_stage_execution_work_unit,
    upsert_stage_work_unit,
)


PROGRESS_DIAGNOSTIC_LOGGER = logging.getLogger("company_research_parser")
HOST_STATS_NON_FATAL_ERRNOS = frozenset({errno.EACCES, errno.EPERM, errno.EBUSY})
HOST_STATS_NON_FATAL_WINERRORS = frozenset({5, 32, 33})
ATOMIC_REPLACE_RETRY_ERRNOS = frozenset({errno.EACCES, errno.EPERM, errno.EBUSY})
ATOMIC_REPLACE_RETRY_WINERRORS = frozenset({5, 32, 33})
ATOMIC_REPLACE_RETRY_ATTEMPTS = 8
ATOMIC_REPLACE_RETRY_DELAY_SECONDS = 0.05
CANONICAL_RUNTIME_ARTIFACTS = (RUNTIME_STATE_FILENAME,)
REMATERIALIZED_RUNTIME_ARTIFACTS = (
    "results.json",
    "leads.json",
    "summary.json",
    "host_stats.json",
    "availability_summary.json",
    "report.md",
    "leads.md",
    "insights.md",
    "final_results.csv",
    "final_results.xlsx",
    "company_reports/*.md",
)
APPEND_ONLY_RUNTIME_LOG_ARTIFACTS = ("results.jsonl", "events.jsonl")
PUBLIC_OUTPUT_STAGING_DIRNAME = ".public_publish_tmp"
PUBLIC_GENERATION_SNAPSHOTS_DIRNAME = "public_generations"
PUBLIC_PUBLISH_STATE_FILENAME = "public_publish_state.json"
CONTROLLED_STOP_REQUEST_FILENAME = "controlled_stop_request.json"
HOST_EVENT_STAGE = "runtime_host"
HOST_EVENT_FALLBACK_ROW_INDEX = 1
COMPLETED_COMPANY_RESULT_CHECKPOINT_KEY = "completed_company_result"
COMPLETED_COMPANY_RESULT_STATUS = "completed"
RUN_STATUS_RUNNING = "running"
RUN_STATUS_COMPLETED = "completed"
RUN_STATUS_CONTROLLED_STOP = "controlled_stop"
RUN_STATUS_CANCELLED = "cancelled"
RUN_STATUS_ABORTED = "aborted"
RUN_FINISH_REASON_NORMAL_COMPLETION = "normal_completion"
RUN_FINISH_REASON_CONTROLLED_STOP = "controlled_stop"
RUN_FINISH_REASON_CANCELLED = "cancelled"
RUN_FINISH_REASON_ABORTED = "aborted"
RUN_STATUS_FAILED_REQUIRED_SOURCE = "failed_required_source"
RUN_FINISH_REASON_REQUIRED_SOURCE = "required_source_not_operational"
REQUIRED_SOURCE_TERMINAL_ERROR_TYPE = "required_source_not_operational"
REQUIRED_SOURCE_RED_FLAG_STOP_REASON = "required_source_red_flag"
RUNTIME_CLOCK_TELEMETRY_KEY = "runtime_clock"
DOWNSTREAM_DRAIN_TELEMETRY_KEY = "downstream_drain"
SOURCE_COLLECTION_TELEMETRY_KEY = "source_collection"
RUNTIME_EVENT_REPLAY_TELEMETRY_KEY = "runtime_event_replay"
DOWNSTREAM_STAGE_SPAN_EVENT_TYPE = "downstream_stage_span"
RUNTIME_CLOCK_CONTRACT_VERSION = 1
DOWNSTREAM_DRAIN_CONTRACT_VERSION = 2
SOURCE_COLLECTION_CONTRACT_VERSION = 2
RUNTIME_EVENT_REPLAY_CONTRACT_VERSION = 1
SLOW_ROW_SUMMARY_LIMIT = 5
EXTERNAL_WALLCLOCK_GAP_THRESHOLD_SECONDS = 30.0
PUBLIC_OUTPUT_CONTRACT_VERSION = 1
FINAL_EXPORTS_STATE_SUPPRESSED_NON_TERMINAL = "suppressed_until_run_finished"
FINAL_EXPORTS_STATE_ALL_SELECTED_COMPLETED = "all_selected_completed"
FINAL_EXPORTS_STATE_TERMINAL_COMPLETED = "terminal_completed"
FINAL_EXPORTS_STATE_TERMINAL_PARTIAL = "terminal_partial"


def _is_required_source_terminal_contract(
    *,
    run_status: str,
    finish_reason: str,
    terminal_error_type: str,
) -> bool:
    return any(
        (
            run_status == RUN_STATUS_FAILED_REQUIRED_SOURCE,
            finish_reason == RUN_FINISH_REASON_REQUIRED_SOURCE,
            terminal_error_type == REQUIRED_SOURCE_TERMINAL_ERROR_TYPE,
        )
    )


def _canonical_stop_reason(
    *,
    stop_reason: str,
    run_status: str,
    finish_reason: str,
    terminal_error_type: str,
) -> str:
    if _is_required_source_terminal_contract(
        run_status=run_status,
        finish_reason=finish_reason,
        terminal_error_type=terminal_error_type,
    ):
        return REQUIRED_SOURCE_RED_FLAG_STOP_REASON
    return stop_reason


def _core():
    return importlib.import_module("company_enrichment_core")


def sanitize_for_json(value: Any) -> Any:
    if isinstance(value, dict):
        return {key: sanitize_for_json(val) for key, val in value.items()}
    if isinstance(value, list):
        return [sanitize_for_json(item) for item in value]
    return value


def _round_runtime_seconds(value: Any) -> float:
    try:
        return round(max(float(value or 0.0), 0.0), 4)
    except (TypeError, ValueError):
        return 0.0


def _normalize_runtime_counter(value: Any) -> int:
    try:
        return max(int(value or 0), 0)
    except (TypeError, ValueError):
        return 0


def _runtime_phase_payload(phase: str, started_monotonic_at: float, **extra: Any) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "phase": phase,
        "elapsed_seconds": _round_runtime_seconds(time.monotonic() - started_monotonic_at),
    }
    payload.update(extra)
    return sanitize_for_json(payload)


def _prefixed_runtime_phase_breakdown(phases: Any, prefix: str) -> list[dict[str, Any]]:
    if not isinstance(phases, list):
        return []
    normalized_prefix = _normalize_runtime_text(prefix)
    normalized: list[dict[str, Any]] = []
    for phase in phases:
        if not isinstance(phase, Mapping):
            continue
        phase_name = _normalize_runtime_text(phase.get("phase"))
        if not phase_name:
            continue
        payload = dict(phase)
        payload["phase"] = f"{normalized_prefix}.{phase_name}" if normalized_prefix else phase_name
        payload["elapsed_seconds"] = _round_runtime_seconds(payload.get("elapsed_seconds"))
        normalized.append(sanitize_for_json(payload))
    return normalized


def _parse_runtime_iso(value: Any) -> datetime | None:
    text = str(value or "").strip()
    if not text:
        return None
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _runtime_elapsed_seconds(started_at: Any, finished_at: Any) -> float:
    started = _parse_runtime_iso(started_at)
    finished = _parse_runtime_iso(finished_at)
    if started is None or finished is None:
        return 0.0
    return _round_runtime_seconds((finished - started).total_seconds())


def _normalize_runtime_text(value: Any) -> str:
    return str(value or "").strip()


def _atomic_tmp_path(path: Path) -> Path:
    return path.with_name(f"{path.name}.{uuid.uuid4().hex}.tmp")


def _is_retryable_atomic_replace_error(error: OSError) -> bool:
    if isinstance(error, PermissionError):
        return True
    winerror = getattr(error, "winerror", None)
    if isinstance(winerror, int) and winerror in ATOMIC_REPLACE_RETRY_WINERRORS:
        return True
    return error.errno in ATOMIC_REPLACE_RETRY_ERRNOS


def _replace_with_retry(tmp: Path, path: Path) -> None:
    last_error: OSError | None = None
    for attempt in range(ATOMIC_REPLACE_RETRY_ATTEMPTS):
        try:
            tmp.replace(path)
            return
        except OSError as error:
            if not _is_retryable_atomic_replace_error(error) or attempt == ATOMIC_REPLACE_RETRY_ATTEMPTS - 1:
                raise
            last_error = error
            time.sleep(ATOMIC_REPLACE_RETRY_DELAY_SECONDS)
    if last_error is not None:
        raise last_error


def atomic_write_json(path: Path, data: Any) -> None:
    tmp = _atomic_tmp_path(path)
    try:
        tmp.write_text(json.dumps(sanitize_for_json(data), ensure_ascii=False, indent=2), encoding="utf-8")
        _replace_with_retry(tmp, path)
    finally:
        _cleanup_temp_file(tmp)


def _cleanup_temp_file(path: Path) -> None:
    try:
        path.unlink()
    except FileNotFoundError:
        return
    except OSError:
        return


def _is_non_fatal_host_stats_replace_error(error: OSError) -> bool:
    if isinstance(error, PermissionError):
        return True
    winerror = getattr(error, "winerror", None)
    if isinstance(winerror, int) and winerror in HOST_STATS_NON_FATAL_WINERRORS:
        return True
    return error.errno in HOST_STATS_NON_FATAL_ERRNOS


def atomic_write_host_stats_json_best_effort(path: Path, data: Any) -> bool:
    tmp = _atomic_tmp_path(path)
    try:
        tmp.write_text(json.dumps(sanitize_for_json(data), ensure_ascii=False, indent=2), encoding="utf-8")
        try:
            _replace_with_retry(tmp, path)
        except OSError as error:
            if not _is_non_fatal_host_stats_replace_error(error):
                raise
            PROGRESS_DIAGNOSTIC_LOGGER.warning(
                "Non-fatal host_stats persistence failure; continuing run: target=%s tmp=%s errno=%s winerror=%s error=%s",
                path,
                tmp,
                getattr(error, "errno", None),
                getattr(error, "winerror", None),
                error,
            )
            return False
    finally:
        _cleanup_temp_file(tmp)
    return True


def atomic_write_text(path: Path, text: str) -> None:
    tmp = _atomic_tmp_path(path)
    try:
        tmp.write_text(text, encoding="utf-8")
        _replace_with_retry(tmp, path)
    finally:
        _cleanup_temp_file(tmp)


def append_jsonl(path: Path, item: Any) -> None:
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(sanitize_for_json(item), ensure_ascii=False) + "\n")


def _generate_run_id() -> str:
    return uuid.uuid4().hex


class ProgressStore:
    def __init__(self, output_dir: Path) -> None:
        core = _core()
        self.output_dir = output_dir
        self.runtime_state_json = output_dir / RUNTIME_STATE_FILENAME
        self.results_json = output_dir / "results.json"
        self.results_jsonl = output_dir / "results.jsonl"
        self.leads_json = output_dir / "leads.json"
        self.summary_json = output_dir / "summary.json"
        self.availability_summary_json = output_dir / "availability_summary.json"
        self.host_stats_json = output_dir / "host_stats.json"
        self.events_jsonl = output_dir / "events.jsonl"
        self.report_md = output_dir / "report.md"
        self.leads_md = output_dir / "leads.md"
        self.insights_md = output_dir / "insights.md"
        self.final_results_csv = output_dir / "final_results.csv"
        self.final_results_xlsx = output_dir / "final_results.xlsx"
        self.company_reports_dir = output_dir / "company_reports"
        self.public_publish_state_json = (
            output_dir / RUNTIME_PRIVATE_DIRNAME / PUBLIC_PUBLISH_STATE_FILENAME
        )
        self.controlled_stop_request_json = (
            output_dir / RUNTIME_PRIVATE_DIRNAME / CONTROLLED_STOP_REQUEST_FILENAME
        )
        self.public_generation_snapshots_dir = (
            output_dir / RUNTIME_PRIVATE_DIRNAME / PUBLIC_GENERATION_SNAPSHOTS_DIRNAME
        )
        ensure_dir(output_dir)
        self._active_public_publish_dir: Path | None = None
        self._runtime_clock_last_monotonic_at: float | None = None
        self._runtime_clock_segment_started_at = ""
        self._public_publish_state = self._load_public_publish_state()
        self.state = RuntimeRunState()
        self.results = self.state.results
        self.host_stats = self.state.host_stats
        self.host_memory = self.state.host_memory
        self.summary = self.state.summary
        self.run_metadata = self.state.run_metadata
        self._benchmark_capture_inns_by_stage = {
            stage: set() for stage in core.LLM_BENCHMARK_CAPTURE_SUMMARY_FIELDS
        }
        snapshot = load_runtime_state_snapshot(
            runtime_state_path=self.runtime_state_json,
            legacy_results_path=self.results_json,
            legacy_summary_path=self.summary_json,
            legacy_host_stats_path=self.host_stats_json,
            normalize_result_payload=core.normalize_company_result_payload,
        )
        self.state.restore_snapshot(snapshot)
        self._reconcile_run_metadata_after_load()
        promoted_checkpoint_count = self._converge_checkpointed_completed_results_after_load()
        self._reconcile_summary_after_load(promoted_checkpoint_count=promoted_checkpoint_count)
        self._rebuild_llm_summary_from_events()
        self._rebuild_benchmark_capture_summary_from_events()
        self._sync_required_source_deferred_surfaces()
        self._ensure_llm_summary_fields(self.summary)
        self._ensure_benchmark_capture_summary_fields(self.summary)
        if snapshot.loaded_from_state or snapshot.loaded_from_legacy:
            if self._has_incomplete_public_publish_generation():
                self._cleanup_public_publish_staging_dir()
                self._restore_root_public_outputs_from_committed_generation()
            self._restore_rematerialized_artifacts_from_runtime_state()

    def has(self, inn: str) -> bool:
        return inn in self.results

    def get(self, inn: str) -> dict[str, Any] | None:
        return self.results.get(inn)

    def required_source_deferred_state(self) -> dict[str, Any]:
        return normalize_required_source_deferred_state(
            self.run_metadata.get(DEFERRED_REQUIRED_SOURCES_KEY, self.summary.get(DEFERRED_REQUIRED_SOURCES_KEY))
        )

    def unresolved_required_source_deferred_records(self) -> list[dict[str, Any]]:
        return unresolved_required_source_deferred_records(self.required_source_deferred_state())

    def unresolved_deferred_required_source_count(self) -> int:
        return len(self.unresolved_required_source_deferred_records())

    def deferred_required_sources_without_later_success(self) -> list[str]:
        return unresolved_deferred_sources_without_later_success(self.required_source_deferred_state())

    def _apply_required_source_deferred_state(self, state: Mapping[str, Any]) -> None:
        normalized_state = normalize_required_source_deferred_state(state)
        summary_fields = build_required_source_deferred_summary_fields(normalized_state)
        self.summary.update(summary_fields)
        self.run_metadata[DEFERRED_REQUIRED_SOURCES_KEY] = normalized_state
        self.run_metadata[REQUIRED_SOURCE_DEFERRED_ROWS_TOTAL_KEY] = summary_fields[
            REQUIRED_SOURCE_DEFERRED_ROWS_TOTAL_KEY
        ]
        self.run_metadata[REQUIRED_SOURCE_DEFERRED_ROWS_BY_SOURCE_KEY] = summary_fields[
            REQUIRED_SOURCE_DEFERRED_ROWS_BY_SOURCE_KEY
        ]
        self.run_metadata[REQUIRED_SOURCE_DEFERRED_ROWS_BY_STATUS_KEY] = summary_fields[
            REQUIRED_SOURCE_DEFERRED_ROWS_BY_STATUS_KEY
        ]
        self.run_metadata[UNRESOLVED_REQUIRED_SOURCE_ROWS_KEY] = summary_fields[
            UNRESOLVED_REQUIRED_SOURCE_ROWS_KEY
        ]

    def _sync_required_source_deferred_surfaces(self) -> None:
        self._apply_required_source_deferred_state(self.required_source_deferred_state())

    def record_deferred_required_source(self, record: Mapping[str, Any]) -> dict[str, Any]:
        core = _core()
        updated_at = core.utc_now_iso()
        next_state = record_required_source_deferred(self.required_source_deferred_state(), record)
        self.summary["updated_at"] = updated_at
        self.run_metadata["updated_at"] = updated_at
        self._apply_required_source_deferred_state(next_state)
        self._persist_runtime_state()
        self._materialize_runtime_metadata()
        return normalize_required_source_deferred_state(next_state)

    def mark_deferred_required_source_success(self, *, source_name: str) -> None:
        core = _core()
        next_state, changed = mark_required_source_success(
            self.required_source_deferred_state(),
            source=source_name,
            now_iso=core.utc_now_iso(),
        )
        if not changed:
            return
        updated_at = core.utc_now_iso()
        self.summary["updated_at"] = updated_at
        self.run_metadata["updated_at"] = updated_at
        self._apply_required_source_deferred_state(next_state)
        self._persist_runtime_state()
        self._materialize_runtime_metadata()

    def mark_deferred_required_source_resolved(
        self,
        *,
        inn: str,
        source_name: str,
        source_status: str = "",
        detail: str = "",
    ) -> bool:
        core = _core()
        updated_at = core.utc_now_iso()
        next_state, changed = mark_required_source_deferred_record_resolved(
            self.required_source_deferred_state(),
            inn=inn,
            source=source_name,
            now_iso=updated_at,
            resolved_by_run_id=str(self.run_metadata.get("run_id", "") or ""),
            source_status=source_status,
            detail=detail,
        )
        if not changed:
            return False
        self.summary["updated_at"] = updated_at
        self.run_metadata["updated_at"] = updated_at
        self._apply_required_source_deferred_state(next_state)
        self._persist_runtime_state()
        self._materialize_runtime_metadata()
        return True

    def run_started(
        self,
        *,
        input_path: Path | str,
        total_rows: int,
        selected_rows: int,
        selection_mode: str,
        selected_ordinals: list[int],
        start_from: int,
        end_at: int | None,
        active_sources: list[str],
        retry_blocked_source: str = "",
        resume_skipped_rows: int = 0,
        continue_existing_run: bool = False,
        source_lane_scheduler: Mapping[str, Any] | None = None,
        downstream_worker_pools: Mapping[str, Any] | None = None,
        throughput_telemetry: Mapping[str, Any] | None = None,
    ) -> None:
        core = _core()
        if not continue_existing_run:
            self._reset_for_fresh_run()
        self._unlink_if_exists(self.controlled_stop_request_json)
        normalized_resume_skipped_rows = max(min(int(resume_skipped_rows or 0), selected_rows), 0)
        normalized_source_lane_scheduler = (
            sanitize_for_json(dict(source_lane_scheduler))
            if isinstance(source_lane_scheduler, Mapping)
            else {}
        )
        normalized_downstream_worker_pools = (
            sanitize_for_json(dict(downstream_worker_pools))
            if isinstance(downstream_worker_pools, Mapping)
            else {}
        )
        base_throughput_telemetry = (
            sanitize_for_json(dict(throughput_telemetry))
            if isinstance(throughput_telemetry, Mapping)
            else {}
        )
        initial_required_source_deferred_state = (
            normalize_required_source_deferred_state(self.run_metadata.get(DEFERRED_REQUIRED_SOURCES_KEY))
            if continue_existing_run
            else required_source_deferred_state()
        )
        initial_required_source_deferred_summary = build_required_source_deferred_summary_fields(
            initial_required_source_deferred_state
        )
        updated_at = core.utc_now_iso()
        run_id = self._ensure_run_id()
        started_at = str(self.run_metadata.get("started_at", "") or updated_at)
        normalized_throughput_telemetry = self._prepare_throughput_telemetry_for_run_start(
            base_throughput_telemetry,
            continue_existing_run=continue_existing_run,
            started_at=started_at,
            now_iso=updated_at,
        )
        stage_outbox_cursor = (
            self._stage_outbox_cursor() if continue_existing_run else build_stage_outbox_cursor()
        )
        stage_handoffs = self._stage_handoffs() if continue_existing_run else build_stage_handoff_state()
        stage_pickups = self._stage_pickups() if continue_existing_run else build_stage_handoff_pickup_state()
        stage_execution_evidence = (
            self._stage_execution_evidence() if continue_existing_run else normalize_explicit_stage_execution_state(None)
        )
        stage_work_units = self._stage_work_units() if continue_existing_run else build_stage_work_unit_state()
        self.summary.update(
            {
                "updated_at": updated_at,
                "total_rows": total_rows,
                "rows_selected": selected_rows,
                "selection_mode": selection_mode,
                "selected_ordinals": list(selected_ordinals),
                "start_from": start_from,
                "end_at": end_at,
                "active_sources": list(active_sources),
                "processed_rows": 0,
                "completed_rows": len(self.results),
                "remaining_rows": max(selected_rows - normalized_resume_skipped_rows, 0),
                "resume_skipped_rows": normalized_resume_skipped_rows,
                "run_status": RUN_STATUS_RUNNING,
                "finish_reason": "",
                "finished_at": "",
                "stop_requested_at": "",
                "stop_reason": "",
                "terminal_checkpoint": "",
                "terminal_inn": "",
                "terminal_boundary": "",
                "terminal_source": "",
                "terminal_source_status": "",
                "terminal_source_access_mode": "",
                "terminal_error_type": "",
                "terminal_error_message": "",
                THROUGHPUT_TELEMETRY_KEY: normalized_throughput_telemetry,
                **initial_required_source_deferred_summary,
            }
        )
        self.run_metadata.clear()
        self.run_metadata.update(
            {
                "run_id": run_id,
                "input_path": str(input_path),
                "total_rows": total_rows,
                "rows_selected": selected_rows,
                "selection_mode": selection_mode,
                "selected_ordinals": list(selected_ordinals),
                "start_from": start_from,
                "end_at": end_at,
                "active_sources": list(active_sources),
                "retry_blocked_source": retry_blocked_source or "",
                "resume_skipped_rows": normalized_resume_skipped_rows,
                "source_lane_scheduler": normalized_source_lane_scheduler,
                "downstream_worker_pools": normalized_downstream_worker_pools,
                THROUGHPUT_TELEMETRY_KEY: normalized_throughput_telemetry,
                "stage_outbox_cursor": stage_outbox_cursor,
                "stage_handoffs": stage_handoffs,
                "stage_pickups": stage_pickups,
                STAGE_EXECUTION_EVIDENCE_KEY: stage_execution_evidence,
                "stage_work_units": stage_work_units,
                DEFERRED_REQUIRED_SOURCES_KEY: initial_required_source_deferred_state,
                REQUIRED_SOURCE_DEFERRED_ROWS_TOTAL_KEY: initial_required_source_deferred_summary[
                    REQUIRED_SOURCE_DEFERRED_ROWS_TOTAL_KEY
                ],
                REQUIRED_SOURCE_DEFERRED_ROWS_BY_SOURCE_KEY: initial_required_source_deferred_summary[
                    REQUIRED_SOURCE_DEFERRED_ROWS_BY_SOURCE_KEY
                ],
                REQUIRED_SOURCE_DEFERRED_ROWS_BY_STATUS_KEY: initial_required_source_deferred_summary[
                    REQUIRED_SOURCE_DEFERRED_ROWS_BY_STATUS_KEY
                ],
                UNRESOLVED_REQUIRED_SOURCE_ROWS_KEY: initial_required_source_deferred_summary[
                    UNRESOLVED_REQUIRED_SOURCE_ROWS_KEY
                ],
                "started_at": started_at,
                "updated_at": updated_at,
                "run_status": RUN_STATUS_RUNNING,
                "finish_reason": "",
                "finished_at": "",
                "stop_requested_at": "",
                "stop_reason": "",
                "terminal_checkpoint": "",
                "terminal_inn": "",
                "terminal_boundary": "",
                "terminal_source": "",
                "terminal_source_status": "",
                "terminal_source_access_mode": "",
                "terminal_error_type": "",
                "terminal_error_message": "",
            }
        )
        self._ensure_llm_summary_fields(self.summary)
        self._ensure_benchmark_capture_summary_fields(self.summary)
        self._persist_runtime_state()
        self._materialize_runtime_metadata()
        self.append_event(
            {
                "ts": core.utc_now_iso(),
                "type": "run_started",
                "run_id": run_id,
                "input": str(input_path),
                "rows_total": total_rows,
                "rows_selected": selected_rows,
                "selection_mode": selection_mode,
                "selected_ordinals": list(selected_ordinals),
                "start_from": start_from,
                "end_at": end_at,
                "sources": list(active_sources),
                "retry_blocked_source": retry_blocked_source or None,
                "source_lane_scheduler": normalized_source_lane_scheduler,
                "downstream_worker_pools": normalized_downstream_worker_pools,
                THROUGHPUT_TELEMETRY_KEY: normalized_throughput_telemetry,
            }
        )

    def persist_completed_company_result(
        self,
        result: Any,
        *,
        total_rows: int,
        processed_rows: int,
        dossier_builder: Callable[..., dict[str, Any]] | None = None,
    ) -> dict[str, Any]:
        started_monotonic_at = time.monotonic()
        phase_breakdown: list[dict[str, Any]] = []
        if dossier_builder is not None:
            phase_started_at = time.monotonic()
            result.dossier_ref = dossier_builder(result=result, output_dir=self.output_dir)
            phase_breakdown.append(_runtime_phase_payload("dossier_build", phase_started_at))
        upsert_timing = self.upsert(result, total_rows=total_rows, processed_rows=processed_rows)
        upsert_timing = dict(upsert_timing) if isinstance(upsert_timing, Mapping) else {}
        phase_breakdown.extend(
            phase
            for phase in upsert_timing.get("phase_breakdown", [])
            if isinstance(phase, Mapping)
        )
        return sanitize_for_json(
            {
                "contract_version": 1,
                "total_elapsed_seconds": _round_runtime_seconds(time.monotonic() - started_monotonic_at),
                "upsert_total_elapsed_seconds": _round_runtime_seconds(
                    upsert_timing.get("total_elapsed_seconds")
                ),
                "public_outputs_total_elapsed_seconds": _round_runtime_seconds(
                    upsert_timing.get("public_outputs_total_elapsed_seconds")
                ),
                "phase_breakdown": phase_breakdown,
            }
        )

    @staticmethod
    def _should_materialize_full_public_outputs(
        *,
        rows_selected: int,
        resume_skipped_rows: int,
        processed_rows: int,
    ) -> bool:
        if rows_selected <= 0:
            return True
        return max(rows_selected - resume_skipped_rows - processed_rows, 0) <= 0

    def request_controlled_stop(
        self,
        *,
        reason: str = "",
        requested_at: str | None = None,
    ) -> dict[str, str]:
        core = _core()
        payload = {
            "requested_at": str(requested_at or core.utc_now_iso() or ""),
            "reason": core.normalize_whitespace(str(reason or "")),
        }
        ensure_dir(self.controlled_stop_request_json.parent)
        atomic_write_json(self.controlled_stop_request_json, payload)
        return payload

    def consume_controlled_stop_request(self) -> dict[str, str] | None:
        core = _core()
        try:
            payload = json.loads(self.controlled_stop_request_json.read_text(encoding="utf-8"))
        except FileNotFoundError:
            return None
        except (OSError, json.JSONDecodeError):
            payload = {}
        finally:
            self._unlink_if_exists(self.controlled_stop_request_json)

        normalized_payload = dict(payload) if isinstance(payload, Mapping) else {}
        return {
            "requested_at": str(normalized_payload.get("requested_at", "") or core.utc_now_iso() or ""),
            "reason": core.normalize_whitespace(str(normalized_payload.get("reason", "") or "")),
        }

    def run_finished(
        self,
        *,
        processed_rows: int,
        controlled_stop: bool = False,
        stop_request: Mapping[str, Any] | None = None,
        run_status: str | None = None,
        finish_reason: str | None = None,
        terminal_context: Mapping[str, Any] | None = None,
        terminal_error: Mapping[str, Any] | None = None,
        materialize_public_outputs: bool = True,
    ) -> None:
        core = _core()
        ordered = self._ordered_results()
        rows_selected = max(int(self.summary.get("rows_selected", 0) or 0), 0)
        processed_rows = max(
            int(processed_rows or 0),
            int(self.summary.get("processed_rows", 0) or 0),
        )
        resume_skipped_rows = max(int(self.summary.get("resume_skipped_rows", 0) or 0), 0)
        if rows_selected:
            resume_skipped_rows = min(resume_skipped_rows, rows_selected)
        updated_at = core.utc_now_iso()
        current_throughput_telemetry = self._current_throughput_telemetry()
        if current_throughput_telemetry:
            finished_throughput_telemetry = self._merge_runtime_surfaces_into_throughput_telemetry(
                current_throughput_telemetry,
                now_iso=updated_at,
            )
            self.summary[THROUGHPUT_TELEMETRY_KEY] = finished_throughput_telemetry
            self.run_metadata[THROUGHPUT_TELEMETRY_KEY] = finished_throughput_telemetry
        stop_payload = dict(stop_request) if isinstance(stop_request, Mapping) else {}
        terminal_context_payload = dict(terminal_context) if isinstance(terminal_context, Mapping) else {}
        terminal_error_payload = dict(terminal_error) if isinstance(terminal_error, Mapping) else {}
        stop_requested_at = str(stop_payload.get("requested_at", "") or "")
        stop_reason = core.normalize_whitespace(str(stop_payload.get("reason", "") or ""))
        resolved_run_status = core.normalize_whitespace(
            str(
                run_status
                or (RUN_STATUS_CONTROLLED_STOP if controlled_stop else RUN_STATUS_COMPLETED)
            )
        )
        resolved_finish_reason = core.normalize_whitespace(
            str(
                finish_reason
                or (
                    RUN_FINISH_REASON_CONTROLLED_STOP
                    if controlled_stop
                    else RUN_FINISH_REASON_NORMAL_COMPLETION
                )
            )
        )
        terminal_checkpoint = core.normalize_whitespace(str(terminal_context_payload.get("checkpoint", "") or ""))
        terminal_inn = core.normalize_whitespace(str(terminal_context_payload.get("inn", "") or ""))
        terminal_boundary = core.normalize_whitespace(
            str(terminal_context_payload.get("execution_boundary", "") or "")
        )
        terminal_source = core.normalize_whitespace(str(terminal_context_payload.get("source", "") or ""))
        terminal_source_status = core.normalize_whitespace(str(terminal_context_payload.get("source_status", "") or ""))
        terminal_source_access_mode = core.normalize_whitespace(
            str(terminal_context_payload.get("source_access_mode", "") or "")
        )
        terminal_error_type = core.normalize_whitespace(str(terminal_error_payload.get("type", "") or ""))
        terminal_error_message = core.normalize_whitespace(str(terminal_error_payload.get("message", "") or ""))
        stop_reason = _canonical_stop_reason(
            stop_reason=stop_reason,
            run_status=resolved_run_status,
            finish_reason=resolved_finish_reason,
            terminal_error_type=terminal_error_type,
        )
        stop_context_present = controlled_stop or bool(stop_requested_at or stop_reason)
        self.summary.update(
            {
                "updated_at": updated_at,
                "processed_rows": processed_rows,
                "completed_rows": len(self.results),
                "remaining_rows": max(rows_selected - resume_skipped_rows - processed_rows, 0),
                "run_status": resolved_run_status,
                "finish_reason": resolved_finish_reason,
                "finished_at": updated_at,
                "stop_requested_at": stop_requested_at if stop_context_present else "",
                "stop_reason": stop_reason if stop_context_present else "",
                "terminal_checkpoint": terminal_checkpoint,
                "terminal_inn": terminal_inn,
                "terminal_boundary": terminal_boundary,
                "terminal_source": terminal_source,
                "terminal_source_status": terminal_source_status,
                "terminal_source_access_mode": terminal_source_access_mode,
                "terminal_error_type": terminal_error_type,
                "terminal_error_message": terminal_error_message,
            }
        )
        self.run_metadata.update(
            {
                "updated_at": updated_at,
                "run_status": resolved_run_status,
                "finish_reason": resolved_finish_reason,
                "finished_at": updated_at,
                "stop_requested_at": stop_requested_at if stop_context_present else "",
                "stop_reason": stop_reason if stop_context_present else "",
                "terminal_checkpoint": terminal_checkpoint,
                "terminal_inn": terminal_inn,
                "terminal_boundary": terminal_boundary,
                "terminal_source": terminal_source,
                "terminal_source_status": terminal_source_status,
                "terminal_source_access_mode": terminal_source_access_mode,
                "terminal_error_type": terminal_error_type,
                "terminal_error_message": terminal_error_message,
            }
        )
        self._sync_required_source_deferred_surfaces()
        self._ensure_llm_summary_fields(self.summary)
        self._ensure_benchmark_capture_summary_fields(self.summary)
        self._unlink_if_exists(self.controlled_stop_request_json)
        self._persist_runtime_state(ordered)
        public_results, checkpoint_only_count = self._public_results(ordered)
        if materialize_public_outputs:
            self._materialize_company_outputs(ordered)
        else:
            self._materialize_runtime_metadata(
                public_results=public_results,
                checkpoint_only_count=checkpoint_only_count,
            )

        run_id = self._ensure_run_id()
        self.append_event(
            {
                "ts": updated_at,
                "type": "run_finished",
                "run_id": run_id,
                "processed": processed_rows,
                "run_status": resolved_run_status,
                "finish_reason": resolved_finish_reason,
                "stop_requested_at": stop_requested_at or None,
                "stop_reason": stop_reason or None,
                "terminal_checkpoint": terminal_checkpoint or None,
                "terminal_inn": terminal_inn or None,
                "terminal_boundary": terminal_boundary or None,
                "terminal_source": terminal_source or None,
                "terminal_source_status": terminal_source_status or None,
                "terminal_source_access_mode": terminal_source_access_mode or None,
                "terminal_error_type": terminal_error_type or None,
                "terminal_error_message": terminal_error_message or None,
            }
        )

    def mark_existing_result_processed(self, *, total_rows: int, processed_rows: int) -> None:
        core = _core()
        ordered = self._ordered_results()
        rows_selected = max(int(self.summary.get("rows_selected", total_rows) or total_rows), 0)
        resume_skipped_rows = max(int(self.summary.get("resume_skipped_rows", 0) or 0), 0)
        updated_at = core.utc_now_iso()
        self.summary.update(
            {
                "updated_at": updated_at,
                "processed_rows": processed_rows,
                "completed_rows": len(self.results),
                "remaining_rows": max(rows_selected - resume_skipped_rows - processed_rows, 0),
            }
        )
        self.run_metadata["updated_at"] = updated_at
        self._sync_required_source_deferred_surfaces()
        self._ensure_llm_summary_fields(self.summary)
        self._ensure_benchmark_capture_summary_fields(self.summary)
        self._persist_runtime_state(ordered)
        self._materialize_runtime_metadata()

    def upsert(self, result: Any, total_rows: int, processed_rows: int) -> dict[str, Any]:
        core = _core()
        started_monotonic_at = time.monotonic()
        phase_breakdown: list[dict[str, Any]] = []
        phase_started_at = time.monotonic()
        payload = core.serialize_company_result(result)
        phase_breakdown.append(_runtime_phase_payload("serialize_company_result", phase_started_at))
        phase_started_at = time.monotonic()
        self.results[result.inn] = payload
        ordered = self._ordered_results()
        phase_breakdown.append(
            _runtime_phase_payload(
                "prepare_ordered_runtime_results",
                phase_started_at,
                ordered_result_count=len(ordered),
            )
        )
        phase_started_at = time.monotonic()
        rows_selected = max(int(self.summary.get("rows_selected", total_rows) or total_rows), 0)
        resume_skipped_rows = max(int(self.summary.get("resume_skipped_rows", 0) or 0), 0)
        updated_at = core.utc_now_iso()
        self.summary.update(
            {
                "updated_at": updated_at,
                "processed_rows": processed_rows,
                "completed_rows": len(self.results),
                "remaining_rows": max(rows_selected - resume_skipped_rows - processed_rows, 0),
            }
        )
        self.run_metadata["updated_at"] = updated_at
        self._sync_required_source_deferred_surfaces()
        self._ensure_llm_summary_fields(self.summary)
        self._ensure_benchmark_capture_summary_fields(self.summary)
        phase_breakdown.append(_runtime_phase_payload("update_runtime_summary", phase_started_at))
        # Publish canonical completion, duplicate-checkpoint cleanup, and public outputs
        # before append-only traces so canonical/public surfaces do not wait on jsonl evidence.
        phase_started_at = time.monotonic()
        self._cleanup_checkpointed_completed_result_duplicates()
        phase_breakdown.append(_runtime_phase_payload("cleanup_checkpoint_duplicates", phase_started_at))
        phase_started_at = time.monotonic()
        self._persist_runtime_state(ordered)
        phase_breakdown.append(_runtime_phase_payload("persist_runtime_state", phase_started_at))
        phase_started_at = time.monotonic()
        materialize_public_outputs_timing = self._materialize_company_outputs(
            ordered,
            changed_result_payload=payload,
            full_output_surface=self._has_full_public_output_surface(
                public_results=ordered,
                checkpoint_only_count=0,
            ),
        )
        materialize_public_outputs_elapsed = _round_runtime_seconds(time.monotonic() - phase_started_at)
        phase_breakdown.extend(
            _prefixed_runtime_phase_breakdown(
                materialize_public_outputs_timing.get("phase_breakdown")
                if isinstance(materialize_public_outputs_timing, Mapping)
                else [],
                "public_outputs",
            )
        )
        phase_started_at = time.monotonic()
        append_jsonl(self.results_jsonl, payload)
        phase_breakdown.append(_runtime_phase_payload("append_results_jsonl", phase_started_at))
        return sanitize_for_json(
            {
                "contract_version": 1,
                "total_elapsed_seconds": _round_runtime_seconds(time.monotonic() - started_monotonic_at),
                "public_outputs_total_elapsed_seconds": _round_runtime_seconds(
                    materialize_public_outputs_timing.get("total_elapsed_seconds")
                    if isinstance(materialize_public_outputs_timing, Mapping)
                    else materialize_public_outputs_elapsed
                ),
                "phase_breakdown": phase_breakdown,
            }
        )

    def append_events(self, events: Iterable[Mapping[str, Any]]) -> None:
        needs_persist = False
        event_count = 0
        state_update_event_count = 0
        last_event_ts = ""
        for event in events:
            if not isinstance(event, Mapping):
                continue
            event_payload = dict(event)
            event_count += 1
            event_ts = self._normalize_host_event_text(event_payload.get("ts"))
            if event_ts:
                last_event_ts = event_ts
            event_needs_persist = self._append_event(event_payload, persist=False)
            if event_needs_persist:
                state_update_event_count += 1
                needs_persist = True
        if event_count:
            self._apply_runtime_event_replay_telemetry_delta(
                buffered_replay_batch_delta=1,
                buffered_replayed_event_delta=event_count,
                deferred_event_delta=event_count,
                deferred_state_update_event_delta=state_update_event_count,
                deferred_batch_persist_delta=1,
                deferred_state_batch_persist_delta=1 if needs_persist else 0,
                deferred_telemetry_only_batch_persist_delta=0 if needs_persist else 1,
                last_buffered_replay_batch={
                    "event_count": event_count,
                    "state_update_event_count": state_update_event_count,
                    "persisted": True,
                    "persistence_reason": "state_updates" if needs_persist else "telemetry_only",
                    "last_event_ts": last_event_ts,
                },
            )
            needs_persist = True
        if needs_persist:
            self.run_metadata.setdefault("updated_at", self.summary.get("updated_at", ""))
            self._persist_runtime_state()
            self._materialize_runtime_metadata()

    def append_event(self, event: dict[str, Any]) -> None:
        self._append_event(event, persist=True)

    def _append_event(self, event: dict[str, Any], *, persist: bool) -> bool:
        core = _core()
        event_ts = self._normalize_host_event_text(event.get("ts")) or core.utc_now_iso()
        if _normalize_runtime_text(event.get("type")) == DOWNSTREAM_STAGE_SPAN_EVENT_TYPE:
            if self._apply_downstream_stage_span_event(event, event_ts=event_ts):
                if persist:
                    self._apply_runtime_event_replay_telemetry_delta(
                        eager_state_update_event_delta=1,
                        eager_event_persist_delta=1,
                        last_eager_persist_event={
                            "event_type": DOWNSTREAM_STAGE_SPAN_EVENT_TYPE,
                            "ts": event_ts,
                        },
                    )
                    self._persist_runtime_state()
                    self._materialize_runtime_metadata()
                return True
            return False
        append_jsonl(self.events_jsonl, event)
        host_event_payload = self._build_host_event_payload(event)
        self._emit_host_stage_message(event, host_event_payload=host_event_payload, ts=event_ts)
        host_stats_updated = host_event_payload is not None
        if host_stats_updated:
            self._update_host_stats(event)
        host_memory_updated = self._update_host_memory(host_event_payload, ts=event_ts)
        llm_summary_updated = self._apply_llm_event_to_summary(event)
        benchmark_summary_updated = self._apply_benchmark_capture_event_to_summary(event)
        if llm_summary_updated or benchmark_summary_updated:
            updated_at = core.utc_now_iso()
            self.summary["updated_at"] = updated_at
            self.run_metadata["updated_at"] = updated_at
        needs_persist = (
            host_stats_updated
            or host_memory_updated
            or llm_summary_updated
            or benchmark_summary_updated
        )
        if needs_persist:
            self.run_metadata.setdefault("updated_at", self.summary.get("updated_at", ""))
            if persist:
                self._apply_runtime_event_replay_telemetry_delta(
                    eager_state_update_event_delta=1,
                    eager_event_persist_delta=1,
                    last_eager_persist_event={
                        "event_type": _normalize_runtime_text(event.get("type")),
                        "ts": event_ts,
                    },
                )
                self._persist_runtime_state()
                self._materialize_runtime_metadata()
        return needs_persist

    def update_throughput_telemetry(self, payload: Mapping[str, Any] | None) -> dict[str, Any]:
        core = _core()
        normalized_payload = sanitize_for_json(dict(payload)) if isinstance(payload, Mapping) else {}
        current_payload = self._current_throughput_telemetry()
        normalized_payload = self._merge_runtime_surfaces_into_throughput_telemetry(
            normalized_payload,
            now_iso=core.utc_now_iso(),
        )
        current_comparable = dict(current_payload)
        current_comparable.pop("updated_at", None)
        next_comparable = dict(normalized_payload)
        next_comparable.pop("updated_at", None)
        if current_comparable == next_comparable:
            return current_payload
        normalized_payload["updated_at"] = core.utc_now_iso()
        self.run_metadata[THROUGHPUT_TELEMETRY_KEY] = normalized_payload
        self.summary[THROUGHPUT_TELEMETRY_KEY] = normalized_payload
        self.summary["updated_at"] = normalized_payload["updated_at"]
        self.run_metadata["updated_at"] = normalized_payload["updated_at"]
        self._persist_runtime_state()
        self._materialize_runtime_metadata()
        return normalized_payload

    def _current_throughput_telemetry(self) -> dict[str, Any]:
        payload = self.run_metadata.get(THROUGHPUT_TELEMETRY_KEY)
        if not isinstance(payload, Mapping):
            payload = self.summary.get(THROUGHPUT_TELEMETRY_KEY)
        return sanitize_for_json(dict(payload)) if isinstance(payload, Mapping) else {}

    @staticmethod
    def _normalized_runtime_event_replay_telemetry(payload: Any) -> dict[str, Any]:
        root = dict(payload) if isinstance(payload, Mapping) else {}
        last_buffered_replay_batch = (
            dict(root.get("last_buffered_replay_batch"))
            if isinstance(root.get("last_buffered_replay_batch"), Mapping)
            else {}
        )
        last_eager_persist_event = (
            dict(root.get("last_eager_persist_event"))
            if isinstance(root.get("last_eager_persist_event"), Mapping)
            else {}
        )
        return {
            "contract_version": RUNTIME_EVENT_REPLAY_CONTRACT_VERSION,
            "buffered_replay_batch_count": _normalize_runtime_counter(root.get("buffered_replay_batch_count")),
            "buffered_replayed_event_count": _normalize_runtime_counter(root.get("buffered_replayed_event_count")),
            "deferred_event_count": _normalize_runtime_counter(root.get("deferred_event_count")),
            "deferred_state_update_event_count": _normalize_runtime_counter(
                root.get("deferred_state_update_event_count")
            ),
            "deferred_batch_persist_count": _normalize_runtime_counter(root.get("deferred_batch_persist_count")),
            "deferred_state_batch_persist_count": _normalize_runtime_counter(
                root.get("deferred_state_batch_persist_count")
            ),
            "deferred_telemetry_only_batch_persist_count": _normalize_runtime_counter(
                root.get("deferred_telemetry_only_batch_persist_count")
            ),
            "eager_state_update_event_count": _normalize_runtime_counter(root.get("eager_state_update_event_count")),
            "eager_event_persist_count": _normalize_runtime_counter(root.get("eager_event_persist_count")),
            "last_buffered_replay_batch": sanitize_for_json(last_buffered_replay_batch),
            "last_eager_persist_event": sanitize_for_json(last_eager_persist_event),
            "updated_at": _normalize_runtime_text(root.get("updated_at")),
        }

    def _apply_runtime_event_replay_telemetry_delta(
        self,
        *,
        buffered_replay_batch_delta: int = 0,
        buffered_replayed_event_delta: int = 0,
        deferred_event_delta: int = 0,
        deferred_state_update_event_delta: int = 0,
        deferred_batch_persist_delta: int = 0,
        deferred_state_batch_persist_delta: int = 0,
        deferred_telemetry_only_batch_persist_delta: int = 0,
        eager_state_update_event_delta: int = 0,
        eager_event_persist_delta: int = 0,
        last_buffered_replay_batch: Mapping[str, Any] | None = None,
        last_eager_persist_event: Mapping[str, Any] | None = None,
    ) -> None:
        core = _core()
        updated_at = core.utc_now_iso()
        current_payload = self._current_throughput_telemetry()
        replay = self._normalized_runtime_event_replay_telemetry(
            current_payload.get(RUNTIME_EVENT_REPLAY_TELEMETRY_KEY)
        )
        replay["buffered_replay_batch_count"] += _normalize_runtime_counter(buffered_replay_batch_delta)
        replay["buffered_replayed_event_count"] += _normalize_runtime_counter(buffered_replayed_event_delta)
        replay["deferred_event_count"] += _normalize_runtime_counter(deferred_event_delta)
        replay["deferred_state_update_event_count"] += _normalize_runtime_counter(
            deferred_state_update_event_delta
        )
        replay["deferred_batch_persist_count"] += _normalize_runtime_counter(deferred_batch_persist_delta)
        replay["deferred_state_batch_persist_count"] += _normalize_runtime_counter(
            deferred_state_batch_persist_delta
        )
        replay["deferred_telemetry_only_batch_persist_count"] += _normalize_runtime_counter(
            deferred_telemetry_only_batch_persist_delta
        )
        replay["eager_state_update_event_count"] += _normalize_runtime_counter(eager_state_update_event_delta)
        replay["eager_event_persist_count"] += _normalize_runtime_counter(eager_event_persist_delta)
        if isinstance(last_buffered_replay_batch, Mapping):
            replay["last_buffered_replay_batch"] = sanitize_for_json(dict(last_buffered_replay_batch))
        if isinstance(last_eager_persist_event, Mapping):
            replay["last_eager_persist_event"] = sanitize_for_json(dict(last_eager_persist_event))
        replay["updated_at"] = updated_at

        next_payload = dict(current_payload)
        next_payload[RUNTIME_EVENT_REPLAY_TELEMETRY_KEY] = replay
        next_payload = self._merge_runtime_surfaces_into_throughput_telemetry(
            next_payload,
            now_iso=updated_at,
        )
        self.run_metadata[THROUGHPUT_TELEMETRY_KEY] = next_payload
        self.summary[THROUGHPUT_TELEMETRY_KEY] = next_payload
        self.summary["updated_at"] = updated_at
        self.run_metadata["updated_at"] = updated_at

    def _prepare_throughput_telemetry_for_run_start(
        self,
        payload: Mapping[str, Any],
        *,
        continue_existing_run: bool,
        started_at: str,
        now_iso: str,
    ) -> dict[str, Any]:
        normalized_payload = sanitize_for_json(dict(payload))
        previous_payload = self._current_throughput_telemetry() if continue_existing_run else {}
        if (
            continue_existing_run
            and DOWNSTREAM_DRAIN_TELEMETRY_KEY not in normalized_payload
            and isinstance(previous_payload.get(DOWNSTREAM_DRAIN_TELEMETRY_KEY), Mapping)
        ):
            normalized_payload[DOWNSTREAM_DRAIN_TELEMETRY_KEY] = sanitize_for_json(
                dict(previous_payload[DOWNSTREAM_DRAIN_TELEMETRY_KEY])
            )
        if (
            continue_existing_run
            and SOURCE_COLLECTION_TELEMETRY_KEY not in normalized_payload
            and isinstance(previous_payload.get(SOURCE_COLLECTION_TELEMETRY_KEY), Mapping)
        ):
            normalized_payload[SOURCE_COLLECTION_TELEMETRY_KEY] = sanitize_for_json(
                dict(previous_payload[SOURCE_COLLECTION_TELEMETRY_KEY])
            )
        if (
            continue_existing_run
            and RUNTIME_EVENT_REPLAY_TELEMETRY_KEY not in normalized_payload
            and isinstance(previous_payload.get(RUNTIME_EVENT_REPLAY_TELEMETRY_KEY), Mapping)
        ):
            normalized_payload[RUNTIME_EVENT_REPLAY_TELEMETRY_KEY] = self._normalized_runtime_event_replay_telemetry(
                previous_payload[RUNTIME_EVENT_REPLAY_TELEMETRY_KEY]
            )
        normalized_payload[RUNTIME_CLOCK_TELEMETRY_KEY] = self._start_runtime_clock_payload(
            previous_payload.get(RUNTIME_CLOCK_TELEMETRY_KEY) if continue_existing_run else None,
            started_at=started_at,
            now_iso=now_iso,
            continue_existing_run=continue_existing_run,
        )
        return normalized_payload

    def _merge_runtime_surfaces_into_throughput_telemetry(
        self,
        payload: Mapping[str, Any],
        *,
        now_iso: str,
    ) -> dict[str, Any]:
        normalized_payload = sanitize_for_json(dict(payload))
        current_payload = self._current_throughput_telemetry()
        if (
            DOWNSTREAM_DRAIN_TELEMETRY_KEY not in normalized_payload
            and isinstance(current_payload.get(DOWNSTREAM_DRAIN_TELEMETRY_KEY), Mapping)
        ):
            normalized_payload[DOWNSTREAM_DRAIN_TELEMETRY_KEY] = sanitize_for_json(
                dict(current_payload[DOWNSTREAM_DRAIN_TELEMETRY_KEY])
            )
        if (
            SOURCE_COLLECTION_TELEMETRY_KEY not in normalized_payload
            and isinstance(current_payload.get(SOURCE_COLLECTION_TELEMETRY_KEY), Mapping)
        ):
            normalized_payload[SOURCE_COLLECTION_TELEMETRY_KEY] = sanitize_for_json(
                dict(current_payload[SOURCE_COLLECTION_TELEMETRY_KEY])
            )
        if (
            RUNTIME_EVENT_REPLAY_TELEMETRY_KEY not in normalized_payload
            and isinstance(current_payload.get(RUNTIME_EVENT_REPLAY_TELEMETRY_KEY), Mapping)
        ):
            normalized_payload[RUNTIME_EVENT_REPLAY_TELEMETRY_KEY] = self._normalized_runtime_event_replay_telemetry(
                current_payload[RUNTIME_EVENT_REPLAY_TELEMETRY_KEY]
            )
        normalized_payload[RUNTIME_CLOCK_TELEMETRY_KEY] = self._advance_runtime_clock_payload(
            current_payload.get(RUNTIME_CLOCK_TELEMETRY_KEY),
            now_iso=now_iso,
        )
        return normalized_payload

    def _start_runtime_clock_payload(
        self,
        previous_clock: Any,
        *,
        started_at: str,
        now_iso: str,
        continue_existing_run: bool,
    ) -> dict[str, Any]:
        previous = dict(previous_clock) if isinstance(previous_clock, Mapping) else {}
        previous_segments = [
            dict(item)
            for item in previous.get("segments", [])
            if isinstance(item, Mapping)
        ]
        previous_gaps = [
            dict(item)
            for item in previous.get("wall_clock_gaps", [])
            if isinstance(item, Mapping)
        ]
        active_elapsed_seconds = _round_runtime_seconds(previous.get("active_elapsed_seconds"))
        external_pause_seconds = _round_runtime_seconds(previous.get("external_pause_seconds"))
        run_started_at = _normalize_runtime_text(previous.get("run_started_at")) if continue_existing_run else ""
        if not run_started_at:
            run_started_at = _normalize_runtime_text(started_at or now_iso)

        if continue_existing_run:
            last_tick_at = _normalize_runtime_text(previous.get("last_tick_at"))
            gap_seconds = _runtime_elapsed_seconds(last_tick_at, now_iso)
            if gap_seconds > 0:
                previous_gaps.append(
                    {
                        "paused_after": last_tick_at,
                        "resumed_at": now_iso,
                        "gap_seconds": gap_seconds,
                    }
                )
                external_pause_seconds = _round_runtime_seconds(external_pause_seconds + gap_seconds)
            if previous_segments:
                previous_segments[-1]["finished_at"] = previous_segments[-1].get("finished_at") or last_tick_at
        else:
            previous_segments = []
            previous_gaps = []
            active_elapsed_seconds = 0.0
            external_pause_seconds = 0.0

        self._runtime_clock_last_monotonic_at = time.monotonic()
        self._runtime_clock_segment_started_at = now_iso
        segments = previous_segments + [
            {
                "started_at": now_iso,
                "finished_at": "",
                "active_elapsed_seconds": 0.0,
            }
        ]
        wall_clock_elapsed_seconds = max(
            _runtime_elapsed_seconds(run_started_at, now_iso),
            _round_runtime_seconds(active_elapsed_seconds + external_pause_seconds),
        )
        largest_gap = self._largest_runtime_gap(previous_gaps)
        contaminated = external_pause_seconds > EXTERNAL_WALLCLOCK_GAP_THRESHOLD_SECONDS
        return {
            "contract_version": RUNTIME_CLOCK_CONTRACT_VERSION,
            "run_started_at": run_started_at,
            "current_segment_started_at": now_iso,
            "last_tick_at": now_iso,
            "wall_clock_elapsed_seconds": wall_clock_elapsed_seconds,
            "active_elapsed_seconds": active_elapsed_seconds,
            "external_pause_seconds": external_pause_seconds,
            "gap_threshold_seconds": EXTERNAL_WALLCLOCK_GAP_THRESHOLD_SECONDS,
            "contaminated_by_external_pause": contaminated,
            "acceptance_grade_speed_evidence": not contaminated,
            "largest_wall_clock_gap": largest_gap,
            "wall_clock_gaps": previous_gaps,
            "segments": segments,
        }

    def _advance_runtime_clock_payload(self, previous_clock: Any, *, now_iso: str) -> dict[str, Any]:
        previous = dict(previous_clock) if isinstance(previous_clock, Mapping) else {}
        run_started_at = _normalize_runtime_text(
            previous.get("run_started_at") or self.run_metadata.get("started_at") or now_iso
        )
        segments = [
            dict(item)
            for item in previous.get("segments", [])
            if isinstance(item, Mapping)
        ]
        wall_clock_gaps = [
            dict(item)
            for item in previous.get("wall_clock_gaps", [])
            if isinstance(item, Mapping)
        ]
        active_delta = 0.0
        now_monotonic = time.monotonic()
        if self._runtime_clock_last_monotonic_at is not None:
            active_delta = _round_runtime_seconds(now_monotonic - self._runtime_clock_last_monotonic_at)
        self._runtime_clock_last_monotonic_at = now_monotonic
        if not self._runtime_clock_segment_started_at:
            self._runtime_clock_segment_started_at = _normalize_runtime_text(
                previous.get("current_segment_started_at") or now_iso
            )
        if not segments:
            segments.append(
                {
                    "started_at": self._runtime_clock_segment_started_at,
                    "finished_at": "",
                    "active_elapsed_seconds": 0.0,
                }
            )
        active_elapsed_seconds = _round_runtime_seconds(
            _round_runtime_seconds(previous.get("active_elapsed_seconds")) + active_delta
        )
        segment_elapsed = _round_runtime_seconds(
            _round_runtime_seconds(segments[-1].get("active_elapsed_seconds")) + active_delta
        )
        segments[-1]["active_elapsed_seconds"] = segment_elapsed
        segments[-1]["finished_at"] = ""
        raw_wall_clock_elapsed_seconds = _runtime_elapsed_seconds(run_started_at, now_iso)
        external_pause_seconds = max(
            _round_runtime_seconds(previous.get("external_pause_seconds")),
            _round_runtime_seconds(raw_wall_clock_elapsed_seconds - active_elapsed_seconds),
        )
        wall_clock_elapsed_seconds = max(
            raw_wall_clock_elapsed_seconds,
            _round_runtime_seconds(active_elapsed_seconds + external_pause_seconds),
        )
        largest_gap = self._largest_runtime_gap(wall_clock_gaps)
        contaminated = external_pause_seconds > EXTERNAL_WALLCLOCK_GAP_THRESHOLD_SECONDS
        return {
            "contract_version": RUNTIME_CLOCK_CONTRACT_VERSION,
            "run_started_at": run_started_at,
            "current_segment_started_at": self._runtime_clock_segment_started_at,
            "last_tick_at": now_iso,
            "wall_clock_elapsed_seconds": wall_clock_elapsed_seconds,
            "active_elapsed_seconds": active_elapsed_seconds,
            "external_pause_seconds": _round_runtime_seconds(external_pause_seconds),
            "gap_threshold_seconds": EXTERNAL_WALLCLOCK_GAP_THRESHOLD_SECONDS,
            "contaminated_by_external_pause": contaminated,
            "acceptance_grade_speed_evidence": not contaminated,
            "largest_wall_clock_gap": largest_gap,
            "wall_clock_gaps": wall_clock_gaps,
            "segments": segments,
        }

    @staticmethod
    def _largest_runtime_gap(gaps: list[dict[str, Any]]) -> dict[str, Any]:
        if not gaps:
            return {}
        return max(gaps, key=lambda item: _round_runtime_seconds(item.get("gap_seconds")))

    def _apply_source_collection_timing_message(self, message: Mapping[str, Any]) -> bool:
        if _normalize_runtime_text(message.get("message_type")) != "source_result_ready":
            return False
        payload = message.get("payload")
        if not isinstance(payload, Mapping):
            return False
        inn = _normalize_runtime_text(message.get("inn"))
        source_name = _normalize_runtime_text(payload.get("source"))
        if not inn or not source_name:
            return False
        duration_seconds = _round_runtime_seconds(payload.get("duration_seconds"))
        current_payload = self._current_throughput_telemetry()
        next_payload = dict(current_payload)
        source_collection = self._normalized_source_collection(
            current_payload.get(SOURCE_COLLECTION_TELEMETRY_KEY)
        )
        company = self._source_collection_company(
            source_collection,
            inn=inn,
            row_index=message.get("row_index"),
        )
        sources = company.setdefault("sources", {})
        sources[source_name] = {
            "source": source_name,
            "status": _normalize_runtime_text(payload.get("status")),
            "duration_seconds": duration_seconds,
            "started_at": _normalize_runtime_text(payload.get("started_at")),
            "finished_at": _normalize_runtime_text(payload.get("finished_at")),
            "updated_at": _normalize_runtime_text(message.get("ts")),
        }
        self._refresh_source_collection_company_timing(company)
        source_collection["updated_at"] = _normalize_runtime_text(message.get("ts"))
        source_collection["slow_summary"] = self._source_collection_slow_summary(source_collection)
        next_payload[SOURCE_COLLECTION_TELEMETRY_KEY] = source_collection
        next_payload = self._merge_runtime_surfaces_into_throughput_telemetry(
            next_payload,
            now_iso=source_collection["updated_at"],
        )
        self.run_metadata[THROUGHPUT_TELEMETRY_KEY] = next_payload
        self.summary[THROUGHPUT_TELEMETRY_KEY] = next_payload
        self.summary["updated_at"] = source_collection["updated_at"]
        self.run_metadata["updated_at"] = source_collection["updated_at"]
        self._persist_runtime_state()
        self._materialize_runtime_metadata()
        return True

    @staticmethod
    def _normalized_source_collection(payload: Any) -> dict[str, Any]:
        root = dict(payload) if isinstance(payload, Mapping) else {}
        companies_payload = root.get("companies")
        companies = {
            str(inn): dict(company)
            for inn, company in (companies_payload.items() if isinstance(companies_payload, Mapping) else [])
            if isinstance(company, Mapping)
        }
        return {
            "contract_version": SOURCE_COLLECTION_CONTRACT_VERSION,
            "updated_at": _normalize_runtime_text(root.get("updated_at")),
            "companies": companies,
            "slow_summary": dict(root.get("slow_summary"))
            if isinstance(root.get("slow_summary"), Mapping)
            else {},
        }

    @staticmethod
    def _source_collection_company(
        source_collection: dict[str, Any],
        *,
        inn: str,
        row_index: Any,
    ) -> dict[str, Any]:
        companies = source_collection.setdefault("companies", {})
        company = dict(companies.get(inn)) if isinstance(companies.get(inn), Mapping) else {}
        company["inn"] = inn
        try:
            normalized_row_index = int(row_index or company.get("row_index") or 0)
        except (TypeError, ValueError):
            normalized_row_index = 0
        company["row_index"] = max(normalized_row_index, 0)
        company.setdefault("sources", {})
        companies[inn] = company
        return company

    @classmethod
    def _refresh_source_collection_company_timing(cls, company: dict[str, Any]) -> None:
        sources = company.get("sources")
        total_duration = 0.0
        source_count = 0
        slowest_source: dict[str, Any] = {}
        intervals: list[tuple[datetime, datetime]] = []
        if isinstance(sources, Mapping):
            for source_name, payload in sources.items():
                if not isinstance(payload, Mapping):
                    continue
                duration = _round_runtime_seconds(payload.get("duration_seconds"))
                total_duration += duration
                source_count += 1
                started = _parse_runtime_iso(payload.get("started_at"))
                finished = _parse_runtime_iso(payload.get("finished_at"))
                if started is not None and finished is not None and finished >= started:
                    intervals.append((started, finished))
                if duration > _round_runtime_seconds(slowest_source.get("duration_seconds")):
                    slowest_source = {
                        "source": str(source_name),
                        "status": _normalize_runtime_text(payload.get("status")),
                        "duration_seconds": duration,
                    }
        wall_clock_elapsed_seconds = cls._runtime_interval_union_seconds(intervals)
        company["source_collection"] = {
            "source_count": source_count,
            "total_duration_seconds": _round_runtime_seconds(total_duration),
            "wall_clock_elapsed_seconds": wall_clock_elapsed_seconds,
            "additive_overlap_seconds": _round_runtime_seconds(total_duration - wall_clock_elapsed_seconds),
            "interval_count": len(intervals),
            "elapsed_seconds_semantics": "sum_of_per_source_intervals",
            "wall_clock_aggregate_mode": "overlap_adjusted_interval_union",
            "slowest_source": slowest_source,
        }
        company["source_collection_seconds"] = _round_runtime_seconds(total_duration)

    @staticmethod
    def _top_slow_items(
        items: list[dict[str, Any]],
        *,
        key_name: str,
        limit: int | None = SLOW_ROW_SUMMARY_LIMIT,
    ) -> list[dict[str, Any]]:
        sorted_items = sorted(
            items,
            key=lambda item: _round_runtime_seconds(item.get(key_name)),
            reverse=True,
        )
        if limit is None:
            return sorted_items
        return sorted_items[:max(int(limit or 0), 0)]

    @staticmethod
    def _runtime_interval_union_seconds(intervals: list[tuple[datetime, datetime]]) -> float:
        if not intervals:
            return 0.0
        normalized_intervals = sorted(
            (started, finished)
            for started, finished in intervals
            if finished >= started
        )
        if not normalized_intervals:
            return 0.0
        merged: list[tuple[datetime, datetime]] = []
        for started, finished in normalized_intervals:
            if not merged or started > merged[-1][1]:
                merged.append((started, finished))
                continue
            previous_started, previous_finished = merged[-1]
            if finished > previous_finished:
                merged[-1] = (previous_started, finished)
        return _round_runtime_seconds(
            sum((finished - started).total_seconds() for started, finished in merged)
        )

    @classmethod
    def _source_collection_slow_summary(cls, source_collection: Mapping[str, Any]) -> dict[str, Any]:
        companies = source_collection.get("companies")
        top_rows: list[dict[str, Any]] = []
        top_source_spans: list[dict[str, Any]] = []
        totals_by_source: dict[str, dict[str, Any]] = {}
        intervals_by_source: dict[str, list[tuple[datetime, datetime]]] = {}
        if isinstance(companies, Mapping):
            for inn, company in companies.items():
                if not isinstance(company, Mapping):
                    continue
                source_summary = company.get("source_collection")
                total_duration = _round_runtime_seconds(
                    source_summary.get("total_duration_seconds")
                    if isinstance(source_summary, Mapping)
                    else company.get("source_collection_seconds")
                )
                if total_duration > 0:
                    top_rows.append(
                        {
                            "inn": str(inn),
                            "row_index": int(company.get("row_index", 0) or 0),
                            "total_duration_seconds": total_duration,
                            "wall_clock_elapsed_seconds": _round_runtime_seconds(
                                source_summary.get("wall_clock_elapsed_seconds")
                                if isinstance(source_summary, Mapping)
                                else 0.0
                            ),
                            "source_count": int(
                                source_summary.get("source_count", 0)
                                if isinstance(source_summary, Mapping)
                                else 0
                            ),
                            "slowest_source": dict(source_summary.get("slowest_source"))
                            if isinstance(source_summary, Mapping)
                            and isinstance(source_summary.get("slowest_source"), Mapping)
                            else {},
                        }
                    )
                sources = company.get("sources")
                if not isinstance(sources, Mapping):
                    continue
                for source_name, source_payload in sources.items():
                    if not isinstance(source_payload, Mapping):
                        continue
                    duration = _round_runtime_seconds(source_payload.get("duration_seconds"))
                    if duration <= 0:
                        continue
                    normalized_source_name = str(source_name)
                    top_source_spans.append(
                        {
                            "inn": str(inn),
                            "row_index": int(company.get("row_index", 0) or 0),
                            "source": normalized_source_name,
                            "status": _normalize_runtime_text(source_payload.get("status")),
                            "duration_seconds": duration,
                            "started_at": _normalize_runtime_text(source_payload.get("started_at")),
                            "finished_at": _normalize_runtime_text(source_payload.get("finished_at")),
                        }
                    )
                    started = _parse_runtime_iso(source_payload.get("started_at"))
                    finished = _parse_runtime_iso(source_payload.get("finished_at"))
                    if started is not None and finished is not None and finished >= started:
                        intervals_by_source.setdefault(normalized_source_name, []).append((started, finished))
                    source_total = totals_by_source.setdefault(
                        normalized_source_name,
                        {
                            "source": normalized_source_name,
                            "total_duration_seconds": 0.0,
                            "wall_clock_elapsed_seconds": 0.0,
                            "additive_overlap_seconds": 0.0,
                            "interval_count": 0,
                            "elapsed_seconds_semantics": "sum_of_per_source_intervals",
                            "wall_clock_aggregate_mode": "overlap_adjusted_interval_union",
                            "company_count": 0,
                            "max_duration_seconds": 0.0,
                        },
                    )
                    source_total["total_duration_seconds"] = _round_runtime_seconds(
                        float(source_total.get("total_duration_seconds", 0.0) or 0.0) + duration
                    )
                    source_total["company_count"] = int(source_total.get("company_count", 0) or 0) + 1
                    source_total["max_duration_seconds"] = max(
                        _round_runtime_seconds(source_total.get("max_duration_seconds")),
                        duration,
                    )
        for source_name, intervals in intervals_by_source.items():
            source_total = totals_by_source.get(source_name)
            if not isinstance(source_total, dict):
                continue
            wall_clock_elapsed_seconds = cls._runtime_interval_union_seconds(intervals)
            source_total["wall_clock_elapsed_seconds"] = wall_clock_elapsed_seconds
            source_total["additive_overlap_seconds"] = _round_runtime_seconds(
                _round_runtime_seconds(source_total.get("total_duration_seconds")) - wall_clock_elapsed_seconds
            )
            source_total["interval_count"] = len(intervals)
        return {
            "limit": SLOW_ROW_SUMMARY_LIMIT,
            "top_company_source_collection": cls._top_slow_items(
                top_rows,
                key_name="total_duration_seconds",
            ),
            "top_source_spans": cls._top_slow_items(top_source_spans, key_name="duration_seconds"),
            "source_totals_by_source": cls._top_slow_items(
                list(totals_by_source.values()),
                key_name="total_duration_seconds",
                limit=None,
            ),
        }

    def _apply_downstream_stage_span_event(self, event: Mapping[str, Any], *, event_ts: str) -> bool:
        if _normalize_runtime_text(event.get("type")) != DOWNSTREAM_STAGE_SPAN_EVENT_TYPE:
            return False
        stage_name = _normalize_runtime_text(event.get("stage"))
        inn = _normalize_runtime_text(event.get("inn"))
        if not stage_name or not inn:
            return False
        current_payload = self._current_throughput_telemetry()
        next_payload = dict(current_payload)
        downstream_drain = self._normalized_downstream_drain(current_payload.get(DOWNSTREAM_DRAIN_TELEMETRY_KEY))
        company = self._downstream_drain_company(
            downstream_drain,
            inn=inn,
            row_index=event.get("row_index"),
            company_name=event.get("company_name"),
        )
        stages = company.setdefault("stages", {})
        stage_payload = dict(stages.get(stage_name)) if isinstance(stages.get(stage_name), Mapping) else {}
        spans = [
            dict(item)
            for item in stage_payload.get("spans", [])
            if isinstance(item, Mapping)
        ]
        started_at = _normalize_runtime_text(event.get("started_at"))
        finished_at = _normalize_runtime_text(event.get("finished_at") or event_ts)
        elapsed_seconds = _round_runtime_seconds(
            event.get("elapsed_seconds") or _runtime_elapsed_seconds(started_at, finished_at)
        )
        span_payload: dict[str, Any] = {
            "started_at": started_at,
            "finished_at": finished_at,
            "elapsed_seconds": elapsed_seconds,
            "status": _normalize_runtime_text(event.get("status")) or "completed",
        }
        error_type = _normalize_runtime_text(event.get("error_type"))
        if error_type:
            span_payload["error_type"] = error_type
        spans.append(span_payload)
        total_elapsed_seconds = _round_runtime_seconds(
            sum(_round_runtime_seconds(item.get("elapsed_seconds")) for item in spans)
        )
        stage_payload.update(
            {
                "stage": stage_name,
                "spans": spans,
                "span_count": len(spans),
                "total_elapsed_seconds": total_elapsed_seconds,
                "max_elapsed_seconds": max(
                    (_round_runtime_seconds(item.get("elapsed_seconds")) for item in spans),
                    default=0.0,
                ),
                "last_started_at": started_at,
                "last_finished_at": finished_at,
            }
        )
        stages[stage_name] = stage_payload
        company["stages"] = stages
        company["last_stage"] = stage_name
        company["last_stage_finished_at"] = finished_at
        self._refresh_downstream_company_drain_timing(company)
        downstream_drain["updated_at"] = event_ts
        self._refresh_downstream_drain_summary_fields(downstream_drain)
        next_payload[DOWNSTREAM_DRAIN_TELEMETRY_KEY] = downstream_drain
        next_payload = self._merge_runtime_surfaces_into_throughput_telemetry(next_payload, now_iso=event_ts)
        self.run_metadata[THROUGHPUT_TELEMETRY_KEY] = next_payload
        self.summary[THROUGHPUT_TELEMETRY_KEY] = next_payload
        self.summary["updated_at"] = event_ts
        self.run_metadata["updated_at"] = event_ts
        return True

    def _apply_downstream_ack_to_telemetry(
        self,
        work_unit: Mapping[str, Any] | None,
        *,
        acknowledged_at: str,
    ) -> bool:
        if not isinstance(work_unit, Mapping):
            return False
        inn = _normalize_runtime_text(work_unit.get("inn"))
        if not inn:
            return False
        current_payload = self._current_throughput_telemetry()
        next_payload = dict(current_payload)
        downstream_drain = self._normalized_downstream_drain(current_payload.get(DOWNSTREAM_DRAIN_TELEMETRY_KEY))
        company = self._downstream_drain_company(
            downstream_drain,
            inn=inn,
            row_index=work_unit.get("row_index"),
            company_name=(work_unit.get("work_unit") or {}).get("company_name") if isinstance(work_unit.get("work_unit"), Mapping) else "",
        )
        first_stage_started_at, last_stage_finished_at = self._downstream_company_stage_bounds(company)
        final_ack = {
            "acknowledged_at": acknowledged_at,
            "elapsed_since_first_stage_seconds": _runtime_elapsed_seconds(first_stage_started_at, acknowledged_at),
            "elapsed_since_last_stage_seconds": _runtime_elapsed_seconds(last_stage_finished_at, acknowledged_at),
        }
        company["final_ack"] = final_ack
        self._refresh_downstream_company_drain_timing(company)
        downstream_drain["updated_at"] = acknowledged_at
        self._refresh_downstream_drain_summary_fields(downstream_drain)
        next_payload[DOWNSTREAM_DRAIN_TELEMETRY_KEY] = downstream_drain
        next_payload = self._merge_runtime_surfaces_into_throughput_telemetry(next_payload, now_iso=acknowledged_at)
        self.run_metadata[THROUGHPUT_TELEMETRY_KEY] = next_payload
        self.summary[THROUGHPUT_TELEMETRY_KEY] = next_payload
        self.summary["updated_at"] = acknowledged_at
        self.run_metadata["updated_at"] = acknowledged_at
        return True

    def record_downstream_finalization_timing(
        self,
        *,
        inn: str,
        row_index: Any = 0,
        company_name: Any = "",
        handoff_fingerprint: Any = "",
        ordered_drain_started_at: Any = "",
        downstream_ready_at: Any = "",
        final_drain_wait_started_at: Any = "",
        final_drain_wait_finished_at: Any = "",
        final_drain_wait_seconds: Any = None,
        public_materialization_started_at: Any = "",
        public_materialization_finished_at: Any = "",
        public_materialization_phase_timing: Mapping[str, Any] | None = None,
    ) -> bool:
        normalized_inn = _normalize_runtime_text(inn)
        if not normalized_inn:
            return False
        current_payload = self._current_throughput_telemetry()
        next_payload = dict(current_payload)
        downstream_drain = self._normalized_downstream_drain(current_payload.get(DOWNSTREAM_DRAIN_TELEMETRY_KEY))
        company = self._downstream_drain_company(
            downstream_drain,
            inn=normalized_inn,
            row_index=row_index,
            company_name=company_name,
        )
        ordered_started_at = _normalize_runtime_text(ordered_drain_started_at)
        ready_at = _normalize_runtime_text(downstream_ready_at)
        if ordered_started_at or ready_at:
            ordered_drain = {
                "started_at": ordered_started_at,
                "downstream_ready_at": ready_at,
            }
            if ordered_started_at and ready_at:
                ordered_drain["elapsed_until_ready_seconds"] = _runtime_elapsed_seconds(ordered_started_at, ready_at)
            company["ordered_drain"] = ordered_drain
        final_wait_started_at = _normalize_runtime_text(final_drain_wait_started_at)
        final_wait_finished_at = _normalize_runtime_text(final_drain_wait_finished_at)
        final_wait_elapsed = (
            _round_runtime_seconds(final_drain_wait_seconds)
            if final_drain_wait_seconds not in (None, "")
            else _runtime_elapsed_seconds(final_wait_started_at, final_wait_finished_at)
        )
        if final_wait_started_at or final_wait_finished_at or final_wait_elapsed:
            company["final_drain_wait"] = {
                "started_at": final_wait_started_at,
                "finished_at": final_wait_finished_at,
                "elapsed_seconds": final_wait_elapsed,
            }
        public_started_at = _normalize_runtime_text(public_materialization_started_at)
        public_finished_at = _normalize_runtime_text(public_materialization_finished_at)
        if public_started_at or public_finished_at:
            public_materialization = {
                "started_at": public_started_at,
                "finished_at": public_finished_at,
                "elapsed_seconds": _runtime_elapsed_seconds(public_started_at, public_finished_at),
            }
            if isinstance(public_materialization_phase_timing, Mapping):
                public_materialization["phase_timing"] = sanitize_for_json(
                    dict(public_materialization_phase_timing)
                )
            company["public_materialization"] = public_materialization
        self._refresh_downstream_company_drain_timing(company)
        updated_at = public_finished_at or public_started_at or final_wait_finished_at or ready_at or ordered_started_at
        downstream_drain["updated_at"] = updated_at
        self._refresh_downstream_drain_summary_fields(downstream_drain)
        next_payload[DOWNSTREAM_DRAIN_TELEMETRY_KEY] = downstream_drain
        next_payload = self._merge_runtime_surfaces_into_throughput_telemetry(
            next_payload,
            now_iso=updated_at,
        )
        self.run_metadata[THROUGHPUT_TELEMETRY_KEY] = next_payload
        self.summary[THROUGHPUT_TELEMETRY_KEY] = next_payload
        if updated_at:
            self.summary["updated_at"] = updated_at
            self.run_metadata["updated_at"] = updated_at
        self._persist_runtime_state()
        self._materialize_runtime_metadata()
        return True

    @staticmethod
    def _normalized_downstream_drain(payload: Any) -> dict[str, Any]:
        root = dict(payload) if isinstance(payload, Mapping) else {}
        companies_payload = root.get("companies")
        companies = {
            str(inn): dict(company)
            for inn, company in (companies_payload.items() if isinstance(companies_payload, Mapping) else [])
            if isinstance(company, Mapping)
        }
        return {
            "contract_version": DOWNSTREAM_DRAIN_CONTRACT_VERSION,
            "updated_at": _normalize_runtime_text(root.get("updated_at")),
            "companies": companies,
            "largest_stage_span": dict(root.get("largest_stage_span"))
            if isinstance(root.get("largest_stage_span"), Mapping)
            else {},
            "largest_company_drain": dict(root.get("largest_company_drain"))
            if isinstance(root.get("largest_company_drain"), Mapping)
            else {},
            "largest_ordered_ack_wait": dict(root.get("largest_ordered_ack_wait"))
            if isinstance(root.get("largest_ordered_ack_wait"), Mapping)
            else {},
            "largest_final_drain_wait": dict(root.get("largest_final_drain_wait"))
            if isinstance(root.get("largest_final_drain_wait"), Mapping)
            else {},
            "largest_public_materialization": dict(root.get("largest_public_materialization"))
            if isinstance(root.get("largest_public_materialization"), Mapping)
            else {},
            "slow_summary": dict(root.get("slow_summary"))
            if isinstance(root.get("slow_summary"), Mapping)
            else {},
        }

    @staticmethod
    def _downstream_drain_company(
        downstream_drain: dict[str, Any],
        *,
        inn: str,
        row_index: Any,
        company_name: Any,
    ) -> dict[str, Any]:
        companies = downstream_drain.setdefault("companies", {})
        company = dict(companies.get(inn)) if isinstance(companies.get(inn), Mapping) else {}
        company["inn"] = inn
        try:
            normalized_row_index = int(row_index or company.get("row_index") or 0)
        except (TypeError, ValueError):
            normalized_row_index = 0
        company["row_index"] = max(normalized_row_index, 0)
        normalized_company_name = _normalize_runtime_text(company_name or company.get("company_name"))
        if normalized_company_name:
            company["company_name"] = normalized_company_name
        company.setdefault("stages", {})
        companies[inn] = company
        return company

    @staticmethod
    def _downstream_company_stage_bounds(company: Mapping[str, Any]) -> tuple[str, str]:
        first_started_at = ""
        last_finished_at = ""
        stages = company.get("stages")
        if not isinstance(stages, Mapping):
            return first_started_at, last_finished_at
        for stage_payload in stages.values():
            if not isinstance(stage_payload, Mapping):
                continue
            for span in stage_payload.get("spans", []) or []:
                if not isinstance(span, Mapping):
                    continue
                started_at = _normalize_runtime_text(span.get("started_at"))
                finished_at = _normalize_runtime_text(span.get("finished_at"))
                if started_at and (not first_started_at or started_at < first_started_at):
                    first_started_at = started_at
                if finished_at and (not last_finished_at or finished_at > last_finished_at):
                    last_finished_at = finished_at
        return first_started_at, last_finished_at

    @classmethod
    def _downstream_stage_execution_summary(cls, company: Mapping[str, Any]) -> dict[str, Any]:
        first_started_at, last_finished_at = cls._downstream_company_stage_bounds(company)
        stages = company.get("stages")
        total_elapsed_seconds = 0.0
        span_count = 0
        stage_count = 0
        if isinstance(stages, Mapping):
            for stage_payload in stages.values():
                if not isinstance(stage_payload, Mapping):
                    continue
                stage_count += 1
                total_elapsed_seconds += _round_runtime_seconds(stage_payload.get("total_elapsed_seconds"))
                span_count += int(stage_payload.get("span_count", 0) or 0)
        return {
            "first_started_at": first_started_at,
            "last_finished_at": last_finished_at,
            "stage_count": stage_count,
            "span_count": span_count,
            "total_elapsed_seconds": _round_runtime_seconds(total_elapsed_seconds),
        }

    @classmethod
    def _refresh_downstream_company_drain_timing(cls, company: dict[str, Any]) -> None:
        stage_execution = cls._downstream_stage_execution_summary(company)
        company["stage_execution"] = stage_execution
        company["actual_stage_execution_seconds"] = stage_execution["total_elapsed_seconds"]
        last_finished_at = _normalize_runtime_text(stage_execution.get("last_finished_at"))
        ordered_drain = company.get("ordered_drain")
        ordered_drain_started_at = (
            _normalize_runtime_text(ordered_drain.get("started_at"))
            if isinstance(ordered_drain, Mapping)
            else ""
        )
        if last_finished_at and ordered_drain_started_at:
            company["ordered_ack_wait"] = {
                "started_at": last_finished_at,
                "finished_at": ordered_drain_started_at,
                "elapsed_seconds": _runtime_elapsed_seconds(last_finished_at, ordered_drain_started_at),
            }

    @classmethod
    def _refresh_downstream_drain_summary_fields(cls, downstream_drain: dict[str, Any]) -> None:
        downstream_drain["largest_stage_span"] = cls._largest_downstream_stage_span(downstream_drain)
        downstream_drain["largest_company_drain"] = cls._largest_downstream_company_drain(downstream_drain)
        downstream_drain["largest_ordered_ack_wait"] = cls._largest_downstream_phase_wait(
            downstream_drain,
            phase_key="ordered_ack_wait",
        )
        downstream_drain["largest_final_drain_wait"] = cls._largest_downstream_phase_wait(
            downstream_drain,
            phase_key="final_drain_wait",
        )
        downstream_drain["largest_public_materialization"] = cls._largest_downstream_phase_wait(
            downstream_drain,
            phase_key="public_materialization",
        )
        downstream_drain["slow_summary"] = cls._downstream_slow_summary(downstream_drain)

    @classmethod
    def _downstream_slow_summary(cls, downstream_drain: Mapping[str, Any]) -> dict[str, Any]:
        companies = downstream_drain.get("companies")
        top_company_stage_execution: list[dict[str, Any]] = []
        top_stage_spans: list[dict[str, Any]] = []
        top_phase_waits: list[dict[str, Any]] = []
        top_public_materialization_phases: list[dict[str, Any]] = []
        stage_totals: dict[str, dict[str, Any]] = {}
        phase_totals: dict[str, dict[str, Any]] = {}
        public_materialization_phase_totals: dict[str, dict[str, Any]] = {}
        phase_intervals: dict[str, list[tuple[datetime, datetime]]] = {}
        if isinstance(companies, Mapping):
            for inn, company in companies.items():
                if not isinstance(company, Mapping):
                    continue
                stage_execution = company.get("stage_execution")
                total_stage_seconds = _round_runtime_seconds(
                    stage_execution.get("total_elapsed_seconds")
                    if isinstance(stage_execution, Mapping)
                    else company.get("actual_stage_execution_seconds")
                )
                if total_stage_seconds > 0:
                    top_company_stage_execution.append(
                        {
                            "inn": str(inn),
                            "row_index": int(company.get("row_index", 0) or 0),
                            "company_name": _normalize_runtime_text(company.get("company_name")),
                            "total_elapsed_seconds": total_stage_seconds,
                            "stage_count": int(
                                stage_execution.get("stage_count", 0)
                                if isinstance(stage_execution, Mapping)
                                else 0
                            ),
                            "span_count": int(
                                stage_execution.get("span_count", 0)
                                if isinstance(stage_execution, Mapping)
                                else 0
                            ),
                            "dominant_stage": cls._dominant_downstream_stage(company),
                        }
                    )
                stages = company.get("stages")
                if isinstance(stages, Mapping):
                    for stage_name, stage_payload in stages.items():
                        if not isinstance(stage_payload, Mapping):
                            continue
                        normalized_stage_name = str(stage_name)
                        stage_elapsed = _round_runtime_seconds(stage_payload.get("total_elapsed_seconds"))
                        stage_total = stage_totals.setdefault(
                            normalized_stage_name,
                            {
                                "stage": normalized_stage_name,
                                "total_elapsed_seconds": 0.0,
                                "span_count": 0,
                                "company_count": 0,
                                "max_elapsed_seconds": 0.0,
                            },
                        )
                        stage_total["total_elapsed_seconds"] = _round_runtime_seconds(
                            float(stage_total.get("total_elapsed_seconds", 0.0) or 0.0) + stage_elapsed
                        )
                        stage_total["span_count"] = int(stage_total.get("span_count", 0) or 0) + int(
                            stage_payload.get("span_count", 0) or 0
                        )
                        if stage_elapsed > 0:
                            stage_total["company_count"] = int(stage_total.get("company_count", 0) or 0) + 1
                        stage_total["max_elapsed_seconds"] = max(
                            _round_runtime_seconds(stage_total.get("max_elapsed_seconds")),
                            _round_runtime_seconds(stage_payload.get("max_elapsed_seconds")),
                        )
                        for span in stage_payload.get("spans", []) or []:
                            if not isinstance(span, Mapping):
                                continue
                            span_elapsed = _round_runtime_seconds(span.get("elapsed_seconds"))
                            if span_elapsed <= 0:
                                continue
                            top_stage_spans.append(
                                {
                                    "inn": str(inn),
                                    "row_index": int(company.get("row_index", 0) or 0),
                                    "company_name": _normalize_runtime_text(company.get("company_name")),
                                    "stage": normalized_stage_name,
                                    "started_at": _normalize_runtime_text(span.get("started_at")),
                                    "finished_at": _normalize_runtime_text(span.get("finished_at")),
                                    "elapsed_seconds": span_elapsed,
                                }
                            )
                for phase_key in ("ordered_ack_wait", "final_drain_wait", "public_materialization"):
                    phase_payload = company.get(phase_key)
                    if not isinstance(phase_payload, Mapping):
                        continue
                    phase_elapsed = _round_runtime_seconds(phase_payload.get("elapsed_seconds"))
                    if phase_elapsed <= 0:
                        continue
                    phase_started_at = _normalize_runtime_text(phase_payload.get("started_at"))
                    phase_finished_at = _normalize_runtime_text(phase_payload.get("finished_at"))
                    top_phase_waits.append(
                        {
                            "inn": str(inn),
                            "row_index": int(company.get("row_index", 0) or 0),
                            "company_name": _normalize_runtime_text(company.get("company_name")),
                            "phase": phase_key,
                            "started_at": phase_started_at,
                            "finished_at": phase_finished_at,
                            "elapsed_seconds": phase_elapsed,
                        }
                    )
                    phase_total = phase_totals.setdefault(
                        phase_key,
                        {
                            "phase": phase_key,
                            "total_elapsed_seconds": 0.0,
                            "company_count": 0,
                            "max_elapsed_seconds": 0.0,
                            "aggregate_mode": "per_company_additive",
                            "elapsed_seconds_semantics": "sum_of_per_company_wait_intervals",
                            "wall_clock_aggregate_mode": "overlap_adjusted_interval_union",
                        },
                    )
                    phase_total["total_elapsed_seconds"] = _round_runtime_seconds(
                        float(phase_total.get("total_elapsed_seconds", 0.0) or 0.0) + phase_elapsed
                    )
                    phase_total["company_count"] = int(phase_total.get("company_count", 0) or 0) + 1
                    phase_total["max_elapsed_seconds"] = max(
                        _round_runtime_seconds(phase_total.get("max_elapsed_seconds")),
                        phase_elapsed,
                    )
                    started = _parse_runtime_iso(phase_started_at)
                    finished = _parse_runtime_iso(phase_finished_at)
                    if started is not None and finished is not None and finished >= started:
                        phase_intervals.setdefault(phase_key, []).append((started, finished))
                    if phase_key != "public_materialization":
                        continue
                    phase_timing = phase_payload.get("phase_timing")
                    phase_breakdown = (
                        phase_timing.get("phase_breakdown")
                        if isinstance(phase_timing, Mapping)
                        else []
                    )
                    if not isinstance(phase_breakdown, list):
                        continue
                    for measured_phase in phase_breakdown:
                        if not isinstance(measured_phase, Mapping):
                            continue
                        measured_phase_name = _normalize_runtime_text(measured_phase.get("phase"))
                        if not measured_phase_name:
                            continue
                        measured_phase_elapsed = _round_runtime_seconds(measured_phase.get("elapsed_seconds"))
                        top_public_materialization_phases.append(
                            {
                                "inn": str(inn),
                                "row_index": int(company.get("row_index", 0) or 0),
                                "company_name": _normalize_runtime_text(company.get("company_name")),
                                "phase": measured_phase_name,
                                "elapsed_seconds": measured_phase_elapsed,
                            }
                        )
                        measured_phase_total = public_materialization_phase_totals.setdefault(
                            measured_phase_name,
                            {
                                "phase": measured_phase_name,
                                "total_elapsed_seconds": 0.0,
                                "company_count": 0,
                                "sample_count": 0,
                                "max_elapsed_seconds": 0.0,
                                "aggregate_mode": "per_company_phase_additive",
                                "elapsed_seconds_semantics": "sum_of_measured_non_overlapping_public_materialization_subphases",
                            },
                        )
                        measured_phase_total["total_elapsed_seconds"] = _round_runtime_seconds(
                            float(measured_phase_total.get("total_elapsed_seconds", 0.0) or 0.0)
                            + measured_phase_elapsed
                        )
                        measured_phase_total["company_count"] = int(
                            measured_phase_total.get("company_count", 0) or 0
                        ) + 1
                        measured_phase_total["sample_count"] = int(
                            measured_phase_total.get("sample_count", 0) or 0
                        ) + 1
                        measured_phase_total["max_elapsed_seconds"] = max(
                            _round_runtime_seconds(measured_phase_total.get("max_elapsed_seconds")),
                            measured_phase_elapsed,
                        )
        for phase_key, phase_total in phase_totals.items():
            wall_clock_elapsed_seconds = cls._runtime_interval_union_seconds(
                phase_intervals.get(phase_key, [])
            )
            phase_total["wall_clock_elapsed_seconds"] = wall_clock_elapsed_seconds
            phase_total["additive_overlap_seconds"] = _round_runtime_seconds(
                _round_runtime_seconds(phase_total.get("total_elapsed_seconds"))
                - wall_clock_elapsed_seconds
            )
            phase_total["interval_count"] = len(phase_intervals.get(phase_key, []))
            phase_total["measurement_note"] = (
                "total_elapsed_seconds sums per-company wait intervals; "
                "wall_clock_elapsed_seconds is the overlap-adjusted interval union"
            )
        return {
            "limit": SLOW_ROW_SUMMARY_LIMIT,
            "top_company_stage_execution": cls._top_slow_items(
                top_company_stage_execution,
                key_name="total_elapsed_seconds",
            ),
            "top_stage_spans": cls._top_slow_items(top_stage_spans, key_name="elapsed_seconds"),
            "stage_totals_by_stage": cls._top_slow_items(
                list(stage_totals.values()),
                key_name="total_elapsed_seconds",
                limit=None,
            ),
            "top_phase_waits": cls._top_slow_items(top_phase_waits, key_name="elapsed_seconds"),
            "phase_totals_by_phase": cls._top_slow_items(
                list(phase_totals.values()),
                key_name="total_elapsed_seconds",
                limit=None,
            ),
            "top_public_materialization_phases": cls._top_slow_items(
                top_public_materialization_phases,
                key_name="elapsed_seconds",
            ),
            "public_materialization_phase_totals_by_phase": cls._top_slow_items(
                list(public_materialization_phase_totals.values()),
                key_name="total_elapsed_seconds",
                limit=None,
            ),
        }

    @staticmethod
    def _dominant_downstream_stage(company: Mapping[str, Any]) -> dict[str, Any]:
        stages = company.get("stages")
        if not isinstance(stages, Mapping):
            return {}
        dominant: dict[str, Any] = {}
        for stage_name, stage_payload in stages.items():
            if not isinstance(stage_payload, Mapping):
                continue
            elapsed = _round_runtime_seconds(stage_payload.get("total_elapsed_seconds"))
            if elapsed <= _round_runtime_seconds(dominant.get("total_elapsed_seconds")):
                continue
            dominant = {
                "stage": str(stage_name),
                "total_elapsed_seconds": elapsed,
                "span_count": int(stage_payload.get("span_count", 0) or 0),
            }
        return dominant

    @classmethod
    def _largest_downstream_stage_span(cls, downstream_drain: Mapping[str, Any]) -> dict[str, Any]:
        largest: dict[str, Any] = {}
        companies = downstream_drain.get("companies")
        if not isinstance(companies, Mapping):
            return largest
        for inn, company in companies.items():
            if not isinstance(company, Mapping):
                continue
            stages = company.get("stages")
            if not isinstance(stages, Mapping):
                continue
            for stage_name, stage_payload in stages.items():
                if not isinstance(stage_payload, Mapping):
                    continue
                for span in stage_payload.get("spans", []) or []:
                    if not isinstance(span, Mapping):
                        continue
                    elapsed_seconds = _round_runtime_seconds(span.get("elapsed_seconds"))
                    if elapsed_seconds <= _round_runtime_seconds(largest.get("elapsed_seconds")):
                        continue
                    largest = {
                        "inn": str(inn),
                        "row_index": int(company.get("row_index", 0) or 0),
                        "company_name": _normalize_runtime_text(company.get("company_name")),
                        "stage": str(stage_name),
                        "started_at": _normalize_runtime_text(span.get("started_at")),
                        "finished_at": _normalize_runtime_text(span.get("finished_at")),
                        "elapsed_seconds": elapsed_seconds,
                    }
        return largest

    @classmethod
    def _largest_downstream_company_drain(cls, downstream_drain: Mapping[str, Any]) -> dict[str, Any]:
        largest: dict[str, Any] = {}
        companies = downstream_drain.get("companies")
        if not isinstance(companies, Mapping):
            return largest
        for inn, company in companies.items():
            if not isinstance(company, Mapping):
                continue
            first_started_at, _last_finished_at = cls._downstream_company_stage_bounds(company)
            final_ack = company.get("final_ack")
            acknowledged_at = (
                _normalize_runtime_text(final_ack.get("acknowledged_at"))
                if isinstance(final_ack, Mapping)
                else ""
            )
            elapsed_seconds = _runtime_elapsed_seconds(first_started_at, acknowledged_at)
            if elapsed_seconds <= _round_runtime_seconds(largest.get("elapsed_seconds")):
                continue
            largest = {
                "inn": str(inn),
                "row_index": int(company.get("row_index", 0) or 0),
                "company_name": _normalize_runtime_text(company.get("company_name")),
                "started_at": first_started_at,
                "finished_at": acknowledged_at,
                "elapsed_seconds": elapsed_seconds,
            }
        return largest

    @classmethod
    def _largest_downstream_phase_wait(
        cls,
        downstream_drain: Mapping[str, Any],
        *,
        phase_key: str,
    ) -> dict[str, Any]:
        largest: dict[str, Any] = {}
        companies = downstream_drain.get("companies")
        if not isinstance(companies, Mapping):
            return largest
        for inn, company in companies.items():
            if not isinstance(company, Mapping):
                continue
            phase_payload = company.get(phase_key)
            if not isinstance(phase_payload, Mapping):
                continue
            elapsed_seconds = _round_runtime_seconds(phase_payload.get("elapsed_seconds"))
            if elapsed_seconds <= _round_runtime_seconds(largest.get("elapsed_seconds")):
                continue
            largest = {
                "inn": str(inn),
                "row_index": int(company.get("row_index", 0) or 0),
                "company_name": _normalize_runtime_text(company.get("company_name")),
                "phase": phase_key,
                "started_at": _normalize_runtime_text(phase_payload.get("started_at")),
                "finished_at": _normalize_runtime_text(phase_payload.get("finished_at")),
                "elapsed_seconds": elapsed_seconds,
            }
        return largest

    @staticmethod
    def _stage_work_unit_for_ack(
        work_units: Mapping[str, Any],
        *,
        inn: str,
        handoff_fingerprint: str,
    ) -> dict[str, Any] | None:
        for surface_key in STAGE_WORK_UNIT_SURFACE_KEYS:
            surface = work_units.get(surface_key)
            companies = surface.get("companies") if isinstance(surface, Mapping) else {}
            company = companies.get(inn) if isinstance(companies, Mapping) else None
            if not isinstance(company, Mapping):
                continue
            if _normalize_runtime_text(company.get("handoff_fingerprint")) == handoff_fingerprint:
                return sanitize_for_json(dict(company))
        return None

    def emit_stage_message(
        self,
        *,
        message_type: str,
        stage: str,
        inn: str,
        row_index: int,
        payload: Any,
        ts: str | None = None,
    ) -> dict[str, Any]:
        core = _core()
        message = append_stage_message_to_outbox(
            self.output_dir,
            build_stage_message(
                run_id=self._ensure_run_id(),
                ts=ts or core.utc_now_iso(),
                message_type=message_type,
                stage=stage,
                inn=inn,
                row_index=row_index,
                payload=payload,
            ),
        )
        if message["message_type"] == "source_result_ready":
            self._apply_source_collection_timing_message(message)
        return message

    def consume_unread_stage_messages(self) -> list[dict[str, Any]]:
        unread_messages, current_cursor, next_cursor = self._read_unread_stage_messages()
        if next_cursor != current_cursor:
            self.run_metadata["stage_outbox_cursor"] = next_cursor
            self._persist_runtime_state()
        return unread_messages

    def materialize_unread_stage_handoffs(self) -> list[dict[str, Any]]:
        unread_messages, current_cursor, next_cursor = self._read_unread_stage_messages()
        current_handoffs = self._stage_handoffs()
        current_pickups = normalize_stage_handoff_pickup_state(self.run_metadata.get("stage_pickups"))
        next_handoffs = apply_stage_messages_to_handoff_state(current_handoffs, unread_messages)
        next_pickups = synchronize_stage_handoff_pickup_state(current_pickups, next_handoffs)
        if (
            next_cursor != current_cursor
            or next_handoffs != current_handoffs
            or next_pickups != current_pickups
        ):
            self.run_metadata["stage_outbox_cursor"] = next_cursor
            self.run_metadata["stage_handoffs"] = next_handoffs
            self.run_metadata["stage_pickups"] = next_pickups
            self._persist_runtime_state()
        return unread_messages

    def sync_stage_handoffs_to_work_units(self) -> list[dict[str, Any]]:
        self.materialize_unread_stage_handoffs()
        # Sequential runner materializes pending work units only; downstream ack remains explicit.
        return self.consume_pickup_ready_stage_handoffs()

    def materialize_stage_work_unit(
        self,
        *,
        inn: str,
        row_index: int,
        work_unit_payload: Mapping[str, Any],
        execution_boundary: str = AGGREGATOR_SITE_EXECUTION_BOUNDARY,
        last_message_ts: str | None = None,
    ) -> dict[str, Any]:
        current_work_units = self._stage_work_units()
        current_execution_evidence = self._stage_execution_evidence()
        updated, next_work_units, materialized_work_unit = upsert_stage_work_unit(
            current_work_units,
            inn=inn,
            row_index=row_index,
            work_unit_payload=work_unit_payload,
            execution_boundary=execution_boundary,
            last_message_ts=last_message_ts or "",
            fingerprint_scope=self._ensure_run_id(),
        )
        evidence_updated, next_execution_evidence = upsert_explicit_stage_execution_work_unit(
            current_execution_evidence,
            materialized_work_unit,
        )
        if updated or evidence_updated:
            self.run_metadata["stage_work_units"] = next_work_units
            self.run_metadata[STAGE_EXECUTION_EVIDENCE_KEY] = next_execution_evidence
            self._persist_runtime_state()
        return materialized_work_unit

    def pending_stage_work_units(
        self,
        *,
        execution_boundary: str | None = None,
        inns: list[str] | tuple[str, ...] | None = None,
    ) -> list[dict[str, Any]]:
        return pending_stage_work_units(
            self._stage_work_units(),
            execution_boundary=execution_boundary,
            inns=inns,
        )

    def consume_pending_stage_work_units(
        self,
        *,
        execution_boundary: str | None = None,
        inns: list[str] | tuple[str, ...] | None = None,
    ) -> list[dict[str, Any]]:
        stored_handoffs = self.run_metadata.get("stage_handoffs")
        stored_pickups = self.run_metadata.get("stage_pickups")
        stored_execution_evidence = self.run_metadata.get(STAGE_EXECUTION_EVIDENCE_KEY)
        stored_work_units = self.run_metadata.get("stage_work_units")
        current_handoffs = normalize_stage_handoff_state(stored_handoffs)
        current_pickups = synchronize_stage_handoff_pickup_state(stored_pickups, current_handoffs)
        current_execution_evidence = normalize_explicit_stage_execution_state(stored_execution_evidence)
        current_work_units = synchronize_stage_work_unit_state(
            stored_work_units,
            current_handoffs,
            current_pickups,
            explicit_execution_state=current_execution_evidence,
        )
        self.run_metadata["stage_handoffs"] = current_handoffs
        self.run_metadata["stage_pickups"] = current_pickups
        self.run_metadata[STAGE_EXECUTION_EVIDENCE_KEY] = current_execution_evidence
        self.run_metadata["stage_work_units"] = current_work_units
        if (
            current_handoffs != stored_handoffs
            or current_pickups != stored_pickups
            or current_execution_evidence != stored_execution_evidence
            or current_work_units != stored_work_units
        ):
            self._persist_runtime_state()
        return pending_stage_work_units(
            current_work_units,
            execution_boundary=execution_boundary,
            inns=inns,
        )

    def merge_stage_work_unit_private_state(
        self,
        *,
        inn: str,
        handoff_fingerprint: str,
        private_state_patch: Mapping[str, Any],
        last_message_ts: str | None = None,
    ) -> dict[str, Any]:
        current_work_units = self._stage_work_units()
        current_execution_evidence = self._stage_execution_evidence()
        updated, next_work_units, merged_work_unit = merge_stage_work_unit_private_state(
            current_work_units,
            inn=inn,
            handoff_fingerprint=handoff_fingerprint,
            private_state_patch=private_state_patch,
            last_message_ts=last_message_ts,
        )
        evidence_updated, next_execution_evidence, evidence_work_unit = merge_stage_work_unit_private_state(
            current_execution_evidence,
            inn=inn,
            handoff_fingerprint=handoff_fingerprint,
            private_state_patch=private_state_patch,
            last_message_ts=last_message_ts,
        )
        if updated or evidence_updated:
            self.run_metadata["stage_work_units"] = next_work_units
            self.run_metadata[STAGE_EXECUTION_EVIDENCE_KEY] = next_execution_evidence
            self._persist_runtime_state()
        return merged_work_unit or evidence_work_unit

    def pickup_ready_stage_handoffs(self) -> list[dict[str, Any]]:
        current_handoffs = self._stage_handoffs()
        current_pickups = normalize_stage_handoff_pickup_state(self.run_metadata.get("stage_pickups"))
        picked_up_handoffs, next_pickups = pickup_ready_stage_handoffs_from_state(
            current_pickups,
            current_handoffs,
        )
        if next_pickups != current_pickups:
            self.run_metadata["stage_pickups"] = next_pickups
            self._persist_runtime_state()
        return picked_up_handoffs

    def stage_handoff_company(self, inn: str) -> dict[str, Any] | None:
        normalized_inn = str(inn or "").strip()
        if not normalized_inn:
            return None
        companies = self._stage_handoffs()[AGGREGATOR_SITE_HANDOFF_KEY]["companies"]
        company = companies.get(normalized_inn)
        if not isinstance(company, Mapping):
            return None
        return sanitize_for_json(company)

    def consume_pickup_ready_stage_handoffs(self) -> list[dict[str, Any]]:
        stored_handoffs = self.run_metadata.get("stage_handoffs")
        stored_pickups = self.run_metadata.get("stage_pickups")
        stored_execution_evidence = self.run_metadata.get(STAGE_EXECUTION_EVIDENCE_KEY)
        stored_work_units = self.run_metadata.get("stage_work_units")
        current_handoffs = normalize_stage_handoff_state(stored_handoffs)
        current_pickups = synchronize_stage_handoff_pickup_state(stored_pickups, current_handoffs)
        current_execution_evidence = normalize_explicit_stage_execution_state(stored_execution_evidence)
        current_work_units = synchronize_stage_work_unit_state(
            stored_work_units,
            current_handoffs,
            current_pickups,
            explicit_execution_state=current_execution_evidence,
        )
        picked_up_handoffs, next_pickups = pickup_ready_stage_handoffs_from_state(
            current_pickups,
            current_handoffs,
        )
        _, next_work_units = materialize_pickup_ready_stage_work_units(
            current_work_units,
            picked_up_handoffs,
        )
        self.run_metadata["stage_handoffs"] = current_handoffs
        self.run_metadata["stage_pickups"] = next_pickups
        self.run_metadata[STAGE_EXECUTION_EVIDENCE_KEY] = current_execution_evidence
        self.run_metadata["stage_work_units"] = next_work_units
        if (
            current_handoffs != stored_handoffs
            or next_pickups != stored_pickups
            or current_execution_evidence != stored_execution_evidence
            or next_work_units != stored_work_units
        ):
            self._persist_runtime_state()
        return pending_stage_work_units(next_work_units)

    def ack_stage_handoff_work_unit(
        self,
        *,
        inn: str,
        handoff_fingerprint: str,
        acknowledged_at: str | None = None,
    ) -> bool:
        core = _core()
        resolved_acknowledged_at = acknowledged_at or core.utc_now_iso()
        current_work_units = self._stage_work_units()
        current_execution_evidence = self._stage_execution_evidence()
        updated, next_work_units = acknowledge_stage_work_unit(
            current_work_units,
            inn=inn,
            handoff_fingerprint=handoff_fingerprint,
            acknowledged_at=resolved_acknowledged_at,
        )
        evidence_updated, next_execution_evidence = acknowledge_stage_work_unit(
            current_execution_evidence,
            inn=inn,
            handoff_fingerprint=handoff_fingerprint,
            acknowledged_at=resolved_acknowledged_at,
        )
        acked_work_unit = self._stage_work_unit_for_ack(
            next_work_units,
            inn=str(inn or "").strip(),
            handoff_fingerprint=str(handoff_fingerprint or "").strip(),
        ) or self._stage_work_unit_for_ack(
            next_execution_evidence,
            inn=str(inn or "").strip(),
            handoff_fingerprint=str(handoff_fingerprint or "").strip(),
        )
        downstream_ack_updated = (
            self._apply_downstream_ack_to_telemetry(
                acked_work_unit,
                acknowledged_at=resolved_acknowledged_at,
            )
            if updated or evidence_updated
            else False
        )
        if updated or evidence_updated or downstream_ack_updated:
            self.run_metadata["stage_work_units"] = next_work_units
            self.run_metadata[STAGE_EXECUTION_EVIDENCE_KEY] = next_execution_evidence
            self._persist_runtime_state()
        return updated or evidence_updated

    @staticmethod
    def _normalize_host_event_text(value: Any) -> str:
        return str(value or "").strip()

    @staticmethod
    def _host_event_numeric_value(value: Any) -> int | float | None:
        if isinstance(value, bool):
            return None
        if isinstance(value, (int, float)):
            return value
        return None

    @classmethod
    def _build_host_event_payload(cls, event: dict[str, Any]) -> dict[str, Any] | None:
        event_type = cls._normalize_host_event_text(event.get("type"))
        host = cls._normalize_host_event_text(event.get("host")).lower()
        if not event_type or not host:
            return None
        if event_type in {"run_started", "run_finished"}:
            return None

        payload: dict[str, Any] = {
            "event_type": event_type,
            "host": host,
        }
        source = cls._normalize_host_event_text(event.get("source"))
        if source:
            payload["source"] = source
        status = cls._normalize_host_event_text(event.get("status") or event.get("request_status"))
        if status:
            payload["status"] = status
        cooldown_seconds = cls._host_event_numeric_value(event.get("cooldown_seconds"))
        if cooldown_seconds is not None:
            payload["cooldown_seconds"] = cooldown_seconds
        interval_seconds = cls._host_event_numeric_value(
            event.get("interval_seconds", event.get("since_previous_request_seconds"))
        )
        if interval_seconds is not None:
            payload["interval_seconds"] = interval_seconds
        proxy_label = cls._normalize_host_event_text(event.get("proxy_label") or event.get("proxy_label_or_id"))
        if proxy_label:
            payload["proxy_label"] = proxy_label
            payload["proxy_label_or_id"] = proxy_label
        anti_bot_reason = cls._normalize_host_event_text(event.get("anti_bot_reason"))
        if anti_bot_reason:
            payload["anti_bot_reason"] = anti_bot_reason
        block_class = cls._normalize_host_event_text(event.get("block_class"))
        if block_class:
            payload["block_class"] = block_class
        http_status = cls._host_event_numeric_value(event.get("http_status"))
        if isinstance(http_status, (int, float)):
            payload["http_status"] = int(http_status)
        if isinstance(event.get("challenge_detected"), bool):
            payload["challenge_detected"] = bool(event.get("challenge_detected"))
        if isinstance(event.get("blocked_by_policy"), bool):
            payload["blocked_by_policy"] = bool(event.get("blocked_by_policy"))
        access_state = cls._normalize_host_event_text(event.get("access_state"))
        if access_state:
            payload["access_state"] = access_state
        transport_selected = cls._normalize_host_event_text(event.get("transport_selected"))
        if transport_selected:
            payload["transport_selected"] = transport_selected
        transport_final = cls._normalize_host_event_text(event.get("transport_final"))
        if transport_final:
            payload["transport_final"] = transport_final
        return payload

    @classmethod
    def _host_event_inn(cls, event: dict[str, Any], *, host: str) -> str:
        # Host-scoped runtime events may not carry company identity, but the outbox envelope still requires it.
        inn = cls._normalize_host_event_text(event.get("inn"))
        return inn or f"host:{host}"

    @classmethod
    def _host_event_row_index(cls, event: dict[str, Any]) -> int:
        try:
            row_index = int(event.get("row_index"))
        except (TypeError, ValueError):
            return HOST_EVENT_FALLBACK_ROW_INDEX
        return row_index if row_index > 0 else HOST_EVENT_FALLBACK_ROW_INDEX

    @classmethod
    def _stage_host_event_payload(cls, host_event_payload: dict[str, Any], *, event: dict[str, Any] | None = None) -> dict[str, Any]:
        stage_payload: dict[str, Any] = {
            "event_type": host_event_payload["event_type"],
            "host": host_event_payload["host"],
        }
        for field_name in ("source", "status", "cooldown_seconds", "interval_seconds", "proxy_label"):
            if field_name in host_event_payload:
                stage_payload[field_name] = host_event_payload[field_name]
        if host_event_payload["event_type"] == "route_fetch_breaker_pause" and isinstance(event, dict):
            breaker_mode = cls._normalize_host_event_text(event.get("breaker_mode"))
            if breaker_mode:
                stage_payload["breaker_mode"] = breaker_mode
        return stage_payload

    def _emit_host_stage_message(
        self,
        event: dict[str, Any],
        *,
        host_event_payload: dict[str, Any] | None = None,
        ts: str | None = None,
    ) -> None:
        core = _core()
        payload = host_event_payload or self._build_host_event_payload(event)
        if payload is None:
            return
        host = str(payload["host"])
        self.emit_stage_message(
            message_type="host_event",
            stage=HOST_EVENT_STAGE,
            inn=self._host_event_inn(event, host=host),
            row_index=self._host_event_row_index(event),
            payload=self._stage_host_event_payload(payload, event=event),
            ts=ts or self._normalize_host_event_text(event.get("ts")) or core.utc_now_iso(),
        )

    @staticmethod
    def _normalize_llm_cost_value(value: Any, *, default: float = 0.0) -> float | None:
        if value is None:
            return None
        if isinstance(value, (int, float)):
            return round(float(value), 8)
        return round(default, 8)

    @classmethod
    def _ensure_llm_summary_fields(cls, summary: dict[str, Any]) -> None:
        if not isinstance(summary.get("llm_cost_by_stage"), dict):
            summary["llm_cost_by_stage"] = {}
        if not isinstance(summary.get("llm_cost_by_model"), dict):
            summary["llm_cost_by_model"] = {}
        summary["llm_total_cost_usd"] = cls._resolve_llm_summary_total(summary)

    @classmethod
    def _rollup_llm_cost_breakdown(cls, breakdown: Any) -> tuple[bool, float | None]:
        if not isinstance(breakdown, dict):
            return False, None
        total = 0.0
        has_numeric_value = False
        for value in breakdown.values():
            normalized_value = cls._normalize_llm_cost_value(value, default=0.0)
            if normalized_value is None:
                return True, None
            if isinstance(value, (int, float)):
                total += float(normalized_value)
                has_numeric_value = True
        if not has_numeric_value:
            return False, None
        return True, round(total, 8)

    @classmethod
    def _resolve_llm_summary_total(cls, summary: dict[str, Any]) -> float | None:
        breakdown_rollups: list[float] = []
        for field_name in ("llm_cost_by_stage", "llm_cost_by_model"):
            has_values, rolled_up_total = cls._rollup_llm_cost_breakdown(summary.get(field_name))
            if not has_values:
                continue
            if rolled_up_total is None:
                return None
            breakdown_rollups.append(rolled_up_total)
        if breakdown_rollups:
            return breakdown_rollups[0]
        return cls._normalize_llm_cost_value(summary.get("llm_total_cost_usd"), default=0.0)

    @staticmethod
    def _ensure_benchmark_capture_summary_fields(summary: dict[str, Any]) -> None:
        core = _core()
        for count_field, company_count_field in core.LLM_BENCHMARK_CAPTURE_SUMMARY_FIELDS.values():
            summary[count_field] = int(summary.get(count_field, 0) or 0)
            summary[company_count_field] = int(summary.get(company_count_field, 0) or 0)

    @classmethod
    def _merge_llm_cost_total(cls, current: Any, delta: Any, *, mark_unknown: bool) -> float | None:
        if mark_unknown:
            return None
        if current is None:
            return None
        normalized_current = cls._normalize_llm_cost_value(current, default=0.0)
        if normalized_current is None:
            return None
        if isinstance(delta, (int, float)):
            return round(normalized_current + float(delta), 8)
        return normalized_current

    @classmethod
    def _apply_llm_event_to_summary_payload(cls, summary: dict[str, Any], event: dict[str, Any]) -> bool:
        core = _core()
        if not str(event.get("type", "")).startswith("llm_"):
            return False
        stage = core.normalize_whitespace(str(event.get("stage", "") or ""))
        model = core.normalize_whitespace(str(event.get("model", "") or ""))
        if not stage or not model:
            return False
        cls._ensure_llm_summary_fields(summary)
        total_cost = event.get("total_cost_usd")
        cost_unknown = bool(event.get("cost_unknown"))
        stage_costs = summary["llm_cost_by_stage"]
        stage_costs[stage] = cls._merge_llm_cost_total(stage_costs.get(stage, 0.0), total_cost, mark_unknown=cost_unknown)
        model_costs = summary["llm_cost_by_model"]
        model_costs[model] = cls._merge_llm_cost_total(model_costs.get(model, 0.0), total_cost, mark_unknown=cost_unknown)
        summary["llm_total_cost_usd"] = cls._resolve_llm_summary_total(summary)
        return True

    def _apply_llm_event_to_summary(self, event: dict[str, Any]) -> bool:
        return self._apply_llm_event_to_summary_payload(self.summary, event)

    def _apply_benchmark_capture_event_to_summary_payload(self, summary: dict[str, Any], event: dict[str, Any]) -> bool:
        core = _core()
        if core.normalize_whitespace(str(event.get("type", "") or "")) != "llm_benchmark_capture":
            return False
        stage = core.normalize_whitespace(str(event.get("stage", "") or ""))
        fields = core.LLM_BENCHMARK_CAPTURE_SUMMARY_FIELDS.get(stage)
        if not fields:
            return False
        count_field, company_count_field = fields
        self._ensure_benchmark_capture_summary_fields(summary)
        summary[count_field] = int(summary.get(count_field, 0) or 0) + 1
        inn = core.normalize_whitespace(str(event.get("inn", "") or ""))
        if inn:
            inn_bucket = self._benchmark_capture_inns_by_stage.setdefault(stage, set())
            inn_bucket.add(inn)
            summary[company_count_field] = len(inn_bucket)
        return True

    def _apply_benchmark_capture_event_to_summary(self, event: dict[str, Any]) -> bool:
        return self._apply_benchmark_capture_event_to_summary_payload(self.summary, event)

    def _rebuild_llm_summary_from_events(self) -> None:
        llm_summary: dict[str, Any] = {
            "llm_total_cost_usd": 0.0,
            "llm_cost_by_stage": {},
            "llm_cost_by_model": {},
        }
        if self.events_jsonl.exists():
            try:
                for raw_line in self.events_jsonl.read_text(encoding="utf-8").splitlines():
                    if not raw_line.strip():
                        continue
                    payload = json.loads(raw_line)
                    if isinstance(payload, dict):
                        self._apply_llm_event_to_summary_payload(llm_summary, payload)
            except Exception:
                llm_summary = {
                    "llm_total_cost_usd": 0.0,
                    "llm_cost_by_stage": {},
                    "llm_cost_by_model": {},
                }
        self.summary.update(llm_summary)

    def _rebuild_benchmark_capture_summary_from_events(self) -> None:
        core = _core()
        benchmark_summary: dict[str, Any] = {}
        self._benchmark_capture_inns_by_stage = {
            stage: set() for stage in core.LLM_BENCHMARK_CAPTURE_SUMMARY_FIELDS
        }
        self._ensure_benchmark_capture_summary_fields(benchmark_summary)
        if self.events_jsonl.exists():
            try:
                for raw_line in self.events_jsonl.read_text(encoding="utf-8").splitlines():
                    if not raw_line.strip():
                        continue
                    payload = json.loads(raw_line)
                    if isinstance(payload, dict):
                        self._apply_benchmark_capture_event_to_summary_payload(benchmark_summary, payload)
            except Exception:
                benchmark_summary = {}
                self._benchmark_capture_inns_by_stage = {
                    stage: set() for stage in core.LLM_BENCHMARK_CAPTURE_SUMMARY_FIELDS
                }
                self._ensure_benchmark_capture_summary_fields(benchmark_summary)
        self.summary.update(benchmark_summary)

    def _reconcile_summary_after_load(self, *, promoted_checkpoint_count: int = 0) -> None:
        for field_name in RUNTIME_SUMMARY_DERIVED_ONLY_FIELDS:
            self.summary.pop(field_name, None)
        if not self.summary and not self.results:
            return
        rows_selected = max(int(self.summary.get("rows_selected", 0) or 0), 0)
        processed_rows = max(int(self.summary.get("processed_rows", 0) or 0), 0) + max(
            int(promoted_checkpoint_count or 0),
            0,
        )
        resume_skipped_rows = max(int(self.summary.get("resume_skipped_rows", 0) or 0), 0)
        if rows_selected:
            resume_skipped_rows = min(resume_skipped_rows, rows_selected)
        remaining_default = max(rows_selected - resume_skipped_rows - processed_rows, 0)
        self.summary["processed_rows"] = processed_rows
        self.summary["completed_rows"] = len(self.results)
        if promoted_checkpoint_count:
            self.summary["remaining_rows"] = remaining_default
        else:
            self.summary["remaining_rows"] = max(int(self.summary.get("remaining_rows", remaining_default) or 0), 0)
        self.summary["resume_skipped_rows"] = resume_skipped_rows
        self.summary["run_status"] = str(self.summary.get("run_status", "") or "")
        self.summary["finish_reason"] = str(self.summary.get("finish_reason", "") or "")
        self.summary["finished_at"] = str(self.summary.get("finished_at", "") or "")
        self.summary["stop_requested_at"] = str(self.summary.get("stop_requested_at", "") or "")
        self.summary["stop_reason"] = str(self.summary.get("stop_reason", "") or "")
        self.summary["terminal_checkpoint"] = str(self.summary.get("terminal_checkpoint", "") or "")
        self.summary["terminal_inn"] = str(self.summary.get("terminal_inn", "") or "")
        self.summary["terminal_boundary"] = str(self.summary.get("terminal_boundary", "") or "")
        self.summary["terminal_source"] = str(self.summary.get("terminal_source", "") or "")
        self.summary["terminal_source_status"] = str(self.summary.get("terminal_source_status", "") or "")
        self.summary["terminal_source_access_mode"] = str(self.summary.get("terminal_source_access_mode", "") or "")
        self.summary["terminal_error_type"] = str(self.summary.get("terminal_error_type", "") or "")
        self.summary["terminal_error_message"] = str(self.summary.get("terminal_error_message", "") or "")
        self.summary["stop_reason"] = _canonical_stop_reason(
            stop_reason=self.summary["stop_reason"],
            run_status=self.summary["run_status"],
            finish_reason=self.summary["finish_reason"],
            terminal_error_type=self.summary["terminal_error_type"],
        )
        self.summary[THROUGHPUT_TELEMETRY_KEY] = (
            sanitize_for_json(dict(self.summary.get(THROUGHPUT_TELEMETRY_KEY)))
            if isinstance(self.summary.get(THROUGHPUT_TELEMETRY_KEY), Mapping)
            else {}
        )

    def _reconcile_run_metadata_after_load(self) -> None:
        if not self.run_metadata and not self.summary:
            return
        stage_handoffs = normalize_stage_handoff_state(self.run_metadata.get("stage_handoffs"))
        stage_pickups = synchronize_stage_handoff_pickup_state(
            self.run_metadata.get("stage_pickups"),
            stage_handoffs,
        )
        stage_execution_evidence = normalize_explicit_stage_execution_state(
            self.run_metadata.get(STAGE_EXECUTION_EVIDENCE_KEY),
        )
        stage_work_units = synchronize_stage_work_unit_state(
            self.run_metadata.get("stage_work_units"),
            stage_handoffs,
            stage_pickups,
            explicit_execution_state=stage_execution_evidence,
        )
        required_source_deferred_state_payload = normalize_required_source_deferred_state(
            self.run_metadata.get(DEFERRED_REQUIRED_SOURCES_KEY, self.summary.get(DEFERRED_REQUIRED_SOURCES_KEY))
        )
        required_source_deferred_summary = build_required_source_deferred_summary_fields(
            required_source_deferred_state_payload
        )
        normalized_run_metadata = {
            "run_id": str(self.run_metadata.get("run_id", "") or ""),
            "input_path": str(self.run_metadata.get("input_path", "") or ""),
            "total_rows": max(int(self.run_metadata.get("total_rows", self.summary.get("total_rows", 0)) or 0), 0),
            "rows_selected": max(
                int(self.run_metadata.get("rows_selected", self.summary.get("rows_selected", 0)) or 0),
                0,
            ),
            "selection_mode": str(self.run_metadata.get("selection_mode", self.summary.get("selection_mode", "")) or ""),
            "selected_ordinals": list(
                self.run_metadata.get("selected_ordinals", self.summary.get("selected_ordinals", [])) or []
            ),
            "start_from": max(int(self.run_metadata.get("start_from", self.summary.get("start_from", 1)) or 1), 1),
            "end_at": self.run_metadata.get("end_at", self.summary.get("end_at")),
            "active_sources": list(
                self.run_metadata.get("active_sources", self.summary.get("active_sources", [])) or []
            ),
            "retry_blocked_source": str(self.run_metadata.get("retry_blocked_source", "") or ""),
            "resume_skipped_rows": max(
                int(
                    self.run_metadata.get(
                        "resume_skipped_rows",
                        self.summary.get("resume_skipped_rows", 0),
                    )
                    or 0
                ),
                0,
            ),
            "source_lane_scheduler": sanitize_for_json(dict(self.run_metadata.get("source_lane_scheduler")))
            if isinstance(self.run_metadata.get("source_lane_scheduler"), Mapping)
            else {},
            "downstream_worker_pools": sanitize_for_json(dict(self.run_metadata.get("downstream_worker_pools")))
            if isinstance(self.run_metadata.get("downstream_worker_pools"), Mapping)
            else {},
            THROUGHPUT_TELEMETRY_KEY: sanitize_for_json(
                dict(
                    self.run_metadata.get(
                        THROUGHPUT_TELEMETRY_KEY,
                        self.summary.get(THROUGHPUT_TELEMETRY_KEY, {}),
                    )
                )
            )
            if isinstance(
                self.run_metadata.get(
                    THROUGHPUT_TELEMETRY_KEY,
                    self.summary.get(THROUGHPUT_TELEMETRY_KEY, {}),
                ),
                Mapping,
            )
            else {},
            "stage_outbox_cursor": normalize_stage_outbox_cursor(self.run_metadata.get("stage_outbox_cursor")),
            "stage_handoffs": stage_handoffs,
            "stage_pickups": stage_pickups,
            STAGE_EXECUTION_EVIDENCE_KEY: stage_execution_evidence,
            "stage_work_units": stage_work_units,
            DEFERRED_REQUIRED_SOURCES_KEY: required_source_deferred_state_payload,
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
            "started_at": str(self.run_metadata.get("started_at", "") or ""),
            "updated_at": str(self.run_metadata.get("updated_at", self.summary.get("updated_at", "")) or ""),
            "run_status": str(self.run_metadata.get("run_status", self.summary.get("run_status", "")) or ""),
            "finish_reason": str(self.run_metadata.get("finish_reason", self.summary.get("finish_reason", "")) or ""),
            "finished_at": str(self.run_metadata.get("finished_at", self.summary.get("finished_at", "")) or ""),
            "stop_requested_at": str(
                self.run_metadata.get("stop_requested_at", self.summary.get("stop_requested_at", "")) or ""
            ),
            "stop_reason": str(self.run_metadata.get("stop_reason", self.summary.get("stop_reason", "")) or ""),
            "terminal_checkpoint": str(
                self.run_metadata.get("terminal_checkpoint", self.summary.get("terminal_checkpoint", "")) or ""
            ),
            "terminal_inn": str(self.run_metadata.get("terminal_inn", self.summary.get("terminal_inn", "")) or ""),
            "terminal_boundary": str(
                self.run_metadata.get("terminal_boundary", self.summary.get("terminal_boundary", "")) or ""
            ),
            "terminal_source": str(
                self.run_metadata.get("terminal_source", self.summary.get("terminal_source", "")) or ""
            ),
            "terminal_source_status": str(
                self.run_metadata.get("terminal_source_status", self.summary.get("terminal_source_status", "")) or ""
            ),
            "terminal_source_access_mode": str(
                self.run_metadata.get(
                    "terminal_source_access_mode",
                    self.summary.get("terminal_source_access_mode", ""),
                )
                or ""
            ),
            "terminal_error_type": str(
                self.run_metadata.get("terminal_error_type", self.summary.get("terminal_error_type", "")) or ""
            ),
            "terminal_error_message": str(
                self.run_metadata.get(
                    "terminal_error_message",
                    self.summary.get("terminal_error_message", ""),
                )
                or ""
            ),
        }
        normalized_run_metadata["stop_reason"] = _canonical_stop_reason(
            stop_reason=normalized_run_metadata["stop_reason"],
            run_status=normalized_run_metadata["run_status"],
            finish_reason=normalized_run_metadata["finish_reason"],
            terminal_error_type=normalized_run_metadata["terminal_error_type"],
        )
        self.run_metadata.clear()
        self.run_metadata.update(normalized_run_metadata)
        self.summary[THROUGHPUT_TELEMETRY_KEY] = sanitize_for_json(
            dict(normalized_run_metadata.get(THROUGHPUT_TELEMETRY_KEY) or {})
        )

    def _build_availability_summary(
        self,
        ordered_results: list[dict[str, Any]] | None = None,
    ) -> dict[str, Any]:
        core = _core()
        summary: dict[str, Any] = {"updated_at": core.utc_now_iso(), "sources": {}}
        for result in self._public_results(ordered_results)[0]:
            for source_name, source_payload in (result.get("sources") or {}).items():
                source_summary = summary["sources"].setdefault(source_name, {})
                availability = source_payload.get("availability") or {}
                for field_name, field_payload in availability.items():
                    if field_name not in core.IMPORTANT_FIELDS:
                        continue
                    field_summary = source_summary.setdefault(field_name, core.make_source_availability_counts())
                    status = core.normalize_source_availability_status(str(field_payload.get("status", "")))
                    field_summary[status] = field_summary.get(status, 0) + 1
        return summary

    def _ordered_results(self) -> list[dict[str, Any]]:
        return self.state.ordered_results()

    def _checkpointed_completed_results_by_inn(
        self,
        *,
        include_evidence_mirrored_pending: bool = False,
    ) -> dict[str, dict[str, Any]]:
        checkpointed_results: dict[str, dict[str, Any]] = {}
        stage_work_units = normalize_stage_work_unit_state(self.run_metadata.get("stage_work_units"))
        stage_execution_evidence = normalize_explicit_stage_execution_state(
            self.run_metadata.get(STAGE_EXECUTION_EVIDENCE_KEY),
        )
        for surface_key in STAGE_WORK_UNIT_SURFACE_KEYS:
            companies = stage_work_units[surface_key]["companies"]
            evidence_companies = stage_execution_evidence[surface_key]["companies"]
            for company in companies.values():
                if not isinstance(company, Mapping):
                    continue
                private_state = company.get("private_state")
                if not isinstance(private_state, Mapping):
                    continue
                normalized = self._normalize_public_result_payload(
                    private_state.get(COMPLETED_COMPANY_RESULT_CHECKPOINT_KEY)
                )
                if normalized is None:
                    continue
                inn = str(normalized.get("inn", "") or "").strip()
                if not inn:
                    continue
                if not self._completed_checkpoint_is_public_restore_safe(
                    company=company,
                    evidence_company=evidence_companies.get(inn),
                    normalized=normalized,
                    include_evidence_mirrored_pending=include_evidence_mirrored_pending,
                ):
                    continue
                checkpointed_results[inn] = normalized
        return checkpointed_results

    def _completed_checkpoint_is_public_restore_safe(
        self,
        *,
        company: Mapping[str, Any],
        evidence_company: Mapping[str, Any] | None,
        normalized: Mapping[str, Any],
        include_evidence_mirrored_pending: bool = False,
    ) -> bool:
        if str(company.get("work_status") or "") == WORK_STATUS_ACKED:
            return True
        if not include_evidence_mirrored_pending:
            return False
        if not isinstance(evidence_company, Mapping):
            return False
        if str(evidence_company.get("handoff_fingerprint") or "") != str(company.get("handoff_fingerprint") or ""):
            return False
        private_state = evidence_company.get("private_state")
        if not isinstance(private_state, Mapping):
            return False
        evidence_normalized = self._normalize_public_result_payload(
            private_state.get(COMPLETED_COMPANY_RESULT_CHECKPOINT_KEY)
        )
        return evidence_normalized == dict(normalized)

    def _converge_checkpointed_completed_results_after_load(self) -> int:
        promoted_count = 0
        for inn, normalized in self._checkpointed_completed_results_by_inn(
            include_evidence_mirrored_pending=not self._summary_is_terminal(),
        ).items():
            if inn in self.results:
                continue
            self.results[inn] = normalized
            promoted_count += 1
        self._cleanup_checkpointed_completed_result_duplicates_after_load()
        return promoted_count

    def _cleanup_checkpointed_completed_result_duplicates_after_load(self) -> None:
        self._cleanup_checkpointed_completed_result_duplicates()

    def _cleanup_checkpointed_completed_result_duplicates(self) -> bool:
        canonical_completed_inns = {
            str(normalized.get("inn", "") or "").strip()
            for payload in self.results.values()
            for normalized in (self._normalize_public_result_payload(payload),)
            if normalized is not None
        }
        if not canonical_completed_inns:
            return False

        stage_work_units = normalize_stage_work_unit_state(self.run_metadata.get("stage_work_units"))
        stage_execution_evidence = normalize_explicit_stage_execution_state(
            self.run_metadata.get(STAGE_EXECUTION_EVIDENCE_KEY),
        )
        work_units_updated = self._drop_completed_result_checkpoints_from_state(
            stage_work_units,
            canonical_completed_inns=canonical_completed_inns,
        )
        execution_evidence_updated = self._drop_completed_result_checkpoints_from_state(
            stage_execution_evidence,
            canonical_completed_inns=canonical_completed_inns,
        )
        if work_units_updated:
            self.run_metadata["stage_work_units"] = stage_work_units
        if execution_evidence_updated:
            self.run_metadata[STAGE_EXECUTION_EVIDENCE_KEY] = stage_execution_evidence
        return work_units_updated or execution_evidence_updated

    def _drop_completed_result_checkpoints_from_state(
        self,
        state: dict[str, Any],
        *,
        canonical_completed_inns: set[str],
    ) -> bool:
        changed = False
        for surface_key in STAGE_WORK_UNIT_SURFACE_KEYS:
            companies = state[surface_key]["companies"]
            for company in companies.values():
                private_state = company.get("private_state")
                if not isinstance(private_state, Mapping):
                    continue
                normalized = self._normalize_public_result_payload(
                    private_state.get(COMPLETED_COMPANY_RESULT_CHECKPOINT_KEY)
                )
                if normalized is None:
                    continue
                inn = str(normalized.get("inn", "") or company.get("inn") or "").strip()
                if inn not in canonical_completed_inns:
                    continue
                next_private_state = dict(private_state)
                next_private_state.pop(COMPLETED_COMPANY_RESULT_CHECKPOINT_KEY, None)
                if next_private_state == private_state:
                    continue
                company["private_state"] = next_private_state
                changed = True
        return changed

    def _normalize_public_result_payload(self, payload: Mapping[str, Any] | None) -> dict[str, Any] | None:
        if not isinstance(payload, Mapping):
            return None
        core = _core()
        normalized = core.normalize_company_result_payload(dict(payload))
        inn = str(normalized.get("inn", "") or "").strip()
        status = str(normalized.get("status", "") or "").strip().lower()
        if not inn or status != COMPLETED_COMPANY_RESULT_STATUS:
            return None
        return normalized

    def _public_results(
        self,
        ordered_results: list[dict[str, Any]] | None = None,
    ) -> tuple[list[dict[str, Any]], int]:
        public_results_by_inn: dict[str, dict[str, Any]] = {}
        ordered = self._ordered_results() if ordered_results is None else ordered_results
        for result in ordered:
            normalized = self._normalize_public_result_payload(result)
            if normalized is None:
                continue
            public_results_by_inn[str(normalized.get("inn", "") or "")] = normalized

        checkpoint_only_count = 0
        for inn, normalized in self._checkpointed_completed_results_by_inn().items():
            if inn in public_results_by_inn:
                continue
            public_results_by_inn[inn] = normalized
            checkpoint_only_count += 1

        return ordered_runtime_results(public_results_by_inn), checkpoint_only_count

    def _summary_is_terminal(self) -> bool:
        run_status = str(self.summary.get("run_status", "") or self.run_metadata.get("run_status", "") or "")
        if run_status and run_status != RUN_STATUS_RUNNING:
            return True
        finished_at = str(self.summary.get("finished_at", "") or self.run_metadata.get("finished_at", "") or "")
        return bool(finished_at)

    def _restore_rematerialized_artifacts_from_runtime_state(self) -> None:
        # `runtime_state.json` is canonical; jsonl traces remain append-only evidence and are not backfilled on reload.
        self._persist_runtime_state()
        public_results, checkpoint_only_count = self._public_results()
        full_output_surface = self._has_full_public_output_surface(
            public_results=public_results,
            checkpoint_only_count=checkpoint_only_count,
        )
        if public_results or full_output_surface:
            self._materialize_company_outputs(full_output_surface=full_output_surface)
        else:
            self._cleanup_company_outputs()
            self._reset_public_publish_state()
        if not public_results and (self.summary or self.host_stats):
            self._materialize_runtime_metadata(
                public_results=public_results,
                checkpoint_only_count=checkpoint_only_count,
            )

    def _persist_runtime_state(self, ordered_results: list[dict[str, Any]] | None = None) -> None:
        self._ensure_run_id()
        atomic_write_json(self.runtime_state_json, self.state.build_payload(ordered_results=ordered_results))

    def _materialize_runtime_metadata(
        self,
        *,
        public_results: list[dict[str, Any]] | None = None,
        checkpoint_only_count: int | None = None,
        summary_surface: dict[str, Any] | None = None,
    ) -> None:
        summary_surface = (
            summary_surface
            if summary_surface is not None
            else self._build_summary_surface(
                public_results=public_results,
                checkpoint_only_count=checkpoint_only_count,
            )
        )
        atomic_write_json(
            self._public_output_path(self.summary_json),
            summary_surface,
        )
        self._sync_committed_public_summary_snapshot(summary_surface=summary_surface)
        host_stats_path = self._public_output_path(self.host_stats_json)
        if self.host_stats:
            atomic_write_host_stats_json_best_effort(host_stats_path, self.host_stats)
        else:
            try:
                host_stats_path.unlink()
            except FileNotFoundError:
                return
            except OSError:
                return

    def _materialize_company_outputs(
        self,
        ordered_results: list[dict[str, Any]] | None = None,
        *,
        changed_result_payload: Mapping[str, Any] | None = None,
        full_output_surface: bool = True,
    ) -> dict[str, Any]:
        started_monotonic_at = time.monotonic()
        phase_breakdown: list[dict[str, Any]] = []
        phase_started_at = time.monotonic()
        ordered, checkpoint_only_count = self._public_results(ordered_results)
        summary_surface = self._build_summary_surface(
            public_results=ordered,
            checkpoint_only_count=checkpoint_only_count,
        )
        availability_summary = self._build_availability_summary(ordered)
        phase_breakdown.append(
            _runtime_phase_payload(
                "prepare_public_payloads",
                phase_started_at,
                public_result_count=len(ordered),
                checkpoint_only_count=checkpoint_only_count,
            )
        )
        phase_started_at = time.monotonic()
        staging_dir = self._prepare_public_publish_staging_dir()
        self._active_public_publish_dir = staging_dir
        phase_breakdown.append(_runtime_phase_payload("prepare_public_staging", phase_started_at))
        try:
            phase_started_at = time.monotonic()
            atomic_write_json(self._public_output_path(self.results_json), ordered)
            phase_breakdown.append(_runtime_phase_payload("write_results_json", phase_started_at))
            phase_started_at = time.monotonic()
            atomic_write_json(self._public_output_path(self.leads_json), self._collect_leads(ordered))
            phase_breakdown.append(_runtime_phase_payload("write_leads_json", phase_started_at))
            phase_started_at = time.monotonic()
            self._materialize_runtime_metadata(
                summary_surface=summary_surface,
            )
            phase_breakdown.append(_runtime_phase_payload("write_runtime_metadata", phase_started_at))
            phase_started_at = time.monotonic()
            atomic_write_json(self._public_output_path(self.availability_summary_json), availability_summary)
            phase_breakdown.append(_runtime_phase_payload("write_availability_summary", phase_started_at))
            phase_started_at = time.monotonic()
            if full_output_surface:
                self._write_markdown_reports(ordered)
                phase_breakdown.append(_runtime_phase_payload("write_full_reports", phase_started_at))
            else:
                self._write_incremental_live_reports(
                    ordered,
                    changed_result_payload=changed_result_payload,
                    summary_surface=summary_surface,
                    availability_summary=availability_summary,
                )
                phase_breakdown.append(_runtime_phase_payload("write_incremental_reports", phase_started_at))
        finally:
            self._active_public_publish_dir = None

        phase_started_at = time.monotonic()
        try:
            self._commit_staged_company_outputs(
                staging_dir,
                full_output_surface=full_output_surface,
            )
            phase_breakdown.append(_runtime_phase_payload("commit_public_generation", phase_started_at))
        finally:
            phase_started_at = time.monotonic()
            self._cleanup_public_publish_staging_dir(staging_dir)
            phase_breakdown.append(_runtime_phase_payload("cleanup_public_staging", phase_started_at))
        return sanitize_for_json(
            {
                "contract_version": 1,
                "full_output_surface": bool(full_output_surface),
                "total_elapsed_seconds": _round_runtime_seconds(time.monotonic() - started_monotonic_at),
                "phase_breakdown": phase_breakdown,
            }
        )

    def _reset_for_fresh_run(self) -> None:
        core = _core()
        self.state.reset()
        self._benchmark_capture_inns_by_stage = {
            stage: set() for stage in core.LLM_BENCHMARK_CAPTURE_SUMMARY_FIELDS
        }
        self._cleanup_append_only_logs()
        self._cleanup_company_outputs()
        self._reset_public_publish_state()

    def _cleanup_append_only_logs(self) -> None:
        for path in (
            self.results_jsonl,
            self.events_jsonl,
            stage_message_outbox_path(self.output_dir),
        ):
            self._unlink_if_exists(path)

    def _cleanup_company_outputs(self) -> None:
        for path in (
            self.results_json,
            self.leads_json,
            self.availability_summary_json,
            self.report_md,
            self.leads_md,
            self.insights_md,
            self.final_results_csv,
            self.final_results_xlsx,
        ):
            self._unlink_if_exists(path)
        for existing in self.company_reports_dir.glob("*.md"):
            self._unlink_if_exists(existing)
        self._cleanup_public_publish_staging_dir()

    def _cleanup_final_export_outputs(self) -> None:
        self._unlink_if_exists(self.final_results_csv)
        self._unlink_if_exists(self.final_results_xlsx)

    def _public_generation_snapshot_dir(self, generation_id: str) -> Path | None:
        normalized_generation_id = self._normalize_public_publish_generation_id(generation_id)
        if not normalized_generation_id:
            return None
        return self.public_generation_snapshots_dir / normalized_generation_id

    def _clear_public_generation_snapshots(self) -> None:
        try:
            shutil.rmtree(self.public_generation_snapshots_dir)
        except FileNotFoundError:
            return
        except OSError:
            return

    def _cleanup_stale_public_generation_snapshots(self, *, keep_generation_ids: set[str]) -> None:
        normalized_keep = {
            generation_id
            for generation_id in (
                self._normalize_public_publish_generation_id(item) for item in keep_generation_ids
            )
            if generation_id
        }
        try:
            existing_snapshots = list(self.public_generation_snapshots_dir.iterdir())
        except FileNotFoundError:
            return
        except OSError:
            return
        for snapshot_path in existing_snapshots:
            if snapshot_path.name in normalized_keep:
                continue
            if snapshot_path.is_dir():
                try:
                    shutil.rmtree(snapshot_path)
                except FileNotFoundError:
                    continue
                except OSError:
                    continue
                continue
            self._unlink_if_exists(snapshot_path)

    def _prepare_public_publish_staging_dir(self) -> Path:
        staging_root = self.output_dir / PUBLIC_OUTPUT_STAGING_DIRNAME
        self._cleanup_public_publish_staging_dir(staging_root)
        ensure_dir(staging_root)
        generation_id = _generate_run_id()
        staging_dir = staging_root / generation_id
        ensure_dir(staging_dir)
        self._persist_public_publish_state(active_generation_id=generation_id)
        return staging_dir

    def _cleanup_public_publish_staging_dir(self, staging_dir: Path | None = None) -> None:
        target = self.output_dir / PUBLIC_OUTPUT_STAGING_DIRNAME if staging_dir is None else staging_dir
        try:
            shutil.rmtree(target)
        except FileNotFoundError:
            return
        except OSError:
            return

    def _public_output_path(self, path: Path) -> Path:
        if self._active_public_publish_dir is None:
            return path
        return self._staged_public_output_path(self._active_public_publish_dir, path)

    def _staged_public_output_path(self, staging_dir: Path, path: Path) -> Path:
        return staging_dir / path.relative_to(self.output_dir)

    def _publish_public_output_file(self, staged_path: Path, final_path: Path) -> None:
        ensure_dir(final_path.parent)
        staged_path.replace(final_path)

    def _apply_public_generation_to_root_outputs(
        self,
        generation_dir: Path,
        *,
        full_output_surface: bool = True,
    ) -> None:
        output_paths = [
            self.results_json,
            self.leads_json,
            self.summary_json,
            self.host_stats_json,
            self.availability_summary_json,
            self.report_md,
            self.leads_md,
            self.insights_md,
        ]
        if full_output_surface:
            output_paths.extend(
                [
                    self.final_results_csv,
                    self.final_results_xlsx,
                ]
            )
        for path in output_paths:
            self._commit_staged_public_output(generation_dir, path)
        self._commit_staged_company_reports(
            generation_dir,
            prune_missing=full_output_surface,
        )
        if not full_output_surface:
            self._cleanup_final_export_outputs()

    def _persist_committed_public_generation_snapshot(self, staging_dir: Path) -> None:
        snapshot_dir = self._public_generation_snapshot_dir(staging_dir.name)
        if snapshot_dir is None:
            return
        ensure_dir(snapshot_dir.parent)
        try:
            shutil.rmtree(snapshot_dir)
        except FileNotFoundError:
            pass
        except OSError:
            pass
        shutil.copytree(staging_dir, snapshot_dir)

    def _restore_root_public_outputs_from_committed_generation(self) -> bool:
        snapshot_dir = self._public_generation_snapshot_dir(
            self._public_publish_state.get("committed_generation_id", "")
        )
        if snapshot_dir is None or not snapshot_dir.exists():
            return False
        public_results, checkpoint_only_count = self._public_results()
        full_output_surface = self._has_full_public_output_surface(
            public_results=public_results,
            checkpoint_only_count=checkpoint_only_count,
        )
        if not full_output_surface:
            self._cleanup_final_export_outputs()
        self._apply_public_generation_to_root_outputs(
            snapshot_dir,
            full_output_surface=full_output_surface,
        )
        return True

    def _commit_staged_public_output(self, staging_dir: Path, final_path: Path) -> None:
        staged_path = self._staged_public_output_path(staging_dir, final_path)
        if staged_path.exists():
            self._publish_public_output_file(staged_path, final_path)
            return
        self._unlink_if_exists(final_path)

    def _commit_staged_company_reports(
        self,
        staging_dir: Path,
        *,
        prune_missing: bool = True,
    ) -> None:
        staged_reports_dir = self._staged_public_output_path(staging_dir, self.company_reports_dir)
        expected_names: set[str] = set()
        if staged_reports_dir.exists():
            ensure_dir(self.company_reports_dir)
            for staged_report in sorted(staged_reports_dir.glob("*.md"), key=lambda path: path.name):
                expected_names.add(staged_report.name)
                self._publish_public_output_file(staged_report, self.company_reports_dir / staged_report.name)
        if prune_missing:
            for existing in self.company_reports_dir.glob("*.md"):
                if existing.name not in expected_names:
                    self._unlink_if_exists(existing)

    def _commit_staged_company_outputs(
        self,
        staging_dir: Path,
        *,
        full_output_surface: bool = True,
    ) -> None:
        generation_id = self._normalize_public_publish_generation_id(staging_dir.name)
        self._persist_committed_public_generation_snapshot(staging_dir)
        if not full_output_surface:
            self._cleanup_final_export_outputs()
        self._apply_public_generation_to_root_outputs(
            staging_dir,
            full_output_surface=full_output_surface,
        )
        self._persist_public_publish_state(
            active_generation_id=generation_id,
            committed_generation_id=generation_id,
        )
        self._cleanup_stale_public_generation_snapshots(keep_generation_ids={generation_id})

    @staticmethod
    def _unlink_if_exists(path: Path) -> None:
        try:
            path.unlink()
        except FileNotFoundError:
            return
        except OSError:
            return

    def _sync_committed_public_summary_snapshot(
        self,
        *,
        summary_surface: Mapping[str, Any],
    ) -> None:
        if self._active_public_publish_dir is not None or not self._has_full_public_output_surface():
            return
        snapshot_dir = self._public_generation_snapshot_dir(
            self._public_publish_state.get("committed_generation_id", "")
        )
        if snapshot_dir is None or not snapshot_dir.exists():
            return
        atomic_write_json(
            self._staged_public_output_path(snapshot_dir, self.summary_json),
            dict(summary_surface),
        )

    def _write_markdown_reports(self, ordered_results: list[dict[str, Any]] | None = None) -> None:
        core = _core()
        ordered, checkpoint_only_count = self._public_results(ordered_results)
        summary_surface = self._build_summary_surface(
            public_results=ordered,
            checkpoint_only_count=checkpoint_only_count,
        )
        company_reports_dir = self._public_output_path(self.company_reports_dir)
        ensure_dir(company_reports_dir)
        expected_names: set[str] = set()
        for result in ordered:
            report_name = core.report_file_name(
                int(result.get("row_index", 0) or 0),
                str(result.get("inn", "") or ""),
                str(result.get("company_name", "") or ""),
            )
            expected_names.add(report_name)
            atomic_write_text(company_reports_dir / report_name, core.render_company_report_markdown(result))
        for existing in company_reports_dir.glob("*.md"):
            if existing.name not in expected_names:
                existing.unlink(missing_ok=True)
        availability_summary = self._build_availability_summary(ordered)
        atomic_write_text(
            self._public_output_path(self.report_md),
            core.render_index_report_markdown(
                ordered,
                summary=summary_surface,
                availability_summary=availability_summary,
                host_stats=self.host_stats,
            ),
        )
        atomic_write_text(
            self._public_output_path(self.leads_md),
            core.render_leads_report_markdown(ordered, summary=summary_surface),
        )
        atomic_write_text(
            self._public_output_path(self.insights_md),
            core.render_run_insights_markdown(
                ordered,
                summary=summary_surface,
                availability_summary=availability_summary,
                host_stats=self.host_stats,
            ),
        )
        flat_rows = [core.flatten_company_result_for_export(item) for item in ordered]
        core.write_flat_csv(self._public_output_path(self.final_results_csv), flat_rows)
        core.write_flat_xlsx(self._public_output_path(self.final_results_xlsx), flat_rows)

    def _write_incremental_live_reports(
        self,
        ordered_results: list[dict[str, Any]],
        *,
        changed_result_payload: Mapping[str, Any] | None,
        summary_surface: Mapping[str, Any],
        availability_summary: Mapping[str, Any],
    ) -> None:
        core = _core()
        company_reports_dir = self._public_output_path(self.company_reports_dir)
        ensure_dir(company_reports_dir)
        changed_result = (
            self._normalize_public_result_payload(changed_result_payload)
            if isinstance(changed_result_payload, Mapping)
            else None
        )
        if changed_result is not None:
            report_name = core.report_file_name(
                int(changed_result.get("row_index", 0) or 0),
                str(changed_result.get("inn", "") or ""),
                str(changed_result.get("company_name", "") or ""),
            )
            atomic_write_text(
                company_reports_dir / report_name,
                core.render_company_report_markdown(changed_result),
            )
        atomic_write_text(
            self._public_output_path(self.report_md),
            core.render_index_report_markdown(
                ordered_results,
                summary=dict(summary_surface),
                availability_summary=dict(availability_summary),
                host_stats=self.host_stats,
            ),
        )
        atomic_write_text(
            self._public_output_path(self.leads_md),
            core.render_leads_report_markdown(ordered_results, summary=dict(summary_surface)),
        )
        atomic_write_text(
            self._public_output_path(self.insights_md),
            core.render_run_insights_markdown(
                ordered_results,
                summary=dict(summary_surface),
                availability_summary=dict(availability_summary),
                host_stats=self.host_stats,
            ),
        )

    def _collect_leads(self, ordered_results: list[dict[str, Any]]) -> list[dict[str, Any]]:
        leads: list[dict[str, Any]] = []
        for result in ordered_results:
            for item in result.get("lead_cards") or []:
                lead_payload = dict(item)
                lead_payload["company_row_index"] = result.get("row_index")
                leads.append(lead_payload)
        return leads

    @staticmethod
    def _normalize_public_publish_generation_id(value: Any) -> str:
        return str(value or "").strip()

    def _default_public_publish_state(self) -> dict[str, str]:
        return {
            "active_generation_id": "",
            "committed_generation_id": "",
        }

    def _normalize_public_publish_state(self, payload: Any) -> dict[str, str]:
        state = dict(payload) if isinstance(payload, Mapping) else {}
        return {
            "active_generation_id": self._normalize_public_publish_generation_id(
                state.get("active_generation_id")
            ),
            "committed_generation_id": self._normalize_public_publish_generation_id(
                state.get("committed_generation_id")
            ),
        }

    def _load_public_publish_state(self) -> dict[str, str]:
        try:
            payload = json.loads(self.public_publish_state_json.read_text(encoding="utf-8"))
        except FileNotFoundError:
            return self._default_public_publish_state()
        except (OSError, json.JSONDecodeError):
            return self._default_public_publish_state()
        return self._normalize_public_publish_state(payload)

    def _persist_public_publish_state(
        self,
        *,
        active_generation_id: str | None = None,
        committed_generation_id: str | None = None,
    ) -> None:
        next_state = dict(self._public_publish_state)
        if active_generation_id is not None:
            next_state["active_generation_id"] = self._normalize_public_publish_generation_id(
                active_generation_id
            )
        if committed_generation_id is not None:
            next_state["committed_generation_id"] = self._normalize_public_publish_generation_id(
                committed_generation_id
            )
        ensure_dir(self.public_publish_state_json.parent)
        atomic_write_json(self.public_publish_state_json, next_state)
        self._public_publish_state = next_state

    def _reset_public_publish_state(self) -> None:
        self._persist_public_publish_state(
            active_generation_id="",
            committed_generation_id="",
        )
        self._clear_public_generation_snapshots()

    def _has_incomplete_public_publish_generation(self) -> bool:
        active_generation_id = self._public_publish_state.get("active_generation_id", "")
        committed_generation_id = self._public_publish_state.get("committed_generation_id", "")
        return bool(active_generation_id) and active_generation_id != committed_generation_id

    def _update_host_stats(self, event: dict[str, Any]) -> None:
        host = event.get("host")
        if not host:
            return
        bucket = self.host_stats.setdefault(
            host,
            {
                "first_seen": event.get("ts"),
                "last_seen": event.get("ts"),
                "total_events": 0,
                "event_types": {},
                "sources": {},
                "elapsed_seconds": {"count": 0, "sum": 0.0, "max": 0.0},
                "interval_seconds": {"count": 0, "sum": 0.0, "min": None, "max": 0.0},
                "cooldown_seconds": {"count": 0, "sum": 0.0, "max": 0.0},
            },
        )
        bucket["last_seen"] = event.get("ts")
        bucket["total_events"] += 1
        event_type = event.get("type", "unknown")
        bucket["event_types"][event_type] = bucket["event_types"].get(event_type, 0) + 1
        source = event.get("source", "unknown")
        bucket["sources"][source] = bucket["sources"].get(source, 0) + 1

        elapsed = event.get("elapsed_seconds")
        if isinstance(elapsed, (int, float)):
            elapsed_bucket = bucket["elapsed_seconds"]
            elapsed_bucket["count"] += 1
            elapsed_bucket["sum"] += float(elapsed)
            elapsed_bucket["max"] = max(elapsed_bucket["max"], float(elapsed))
            elapsed_bucket["avg"] = round(elapsed_bucket["sum"] / elapsed_bucket["count"], 4)

        interval = event.get("since_previous_request_seconds")
        if isinstance(interval, (int, float)):
            interval_bucket = bucket["interval_seconds"]
            interval_bucket["count"] += 1
            interval_bucket["sum"] += float(interval)
            interval_bucket["max"] = max(interval_bucket["max"], float(interval))
            interval_bucket["min"] = float(interval) if interval_bucket["min"] is None else min(interval_bucket["min"], float(interval))
            interval_bucket["avg"] = round(interval_bucket["sum"] / interval_bucket["count"], 4)

        cooldown = event.get("cooldown_seconds")
        if isinstance(cooldown, (int, float)) and float(cooldown) > 0:
            cooldown_bucket = bucket["cooldown_seconds"]
            cooldown_bucket["count"] += 1
            cooldown_bucket["sum"] += float(cooldown)
            cooldown_bucket["max"] = max(cooldown_bucket["max"], float(cooldown))
            cooldown_bucket["avg"] = round(cooldown_bucket["sum"] / cooldown_bucket["count"], 4)

    def _update_host_memory(self, host_event_payload: dict[str, Any] | None, *, ts: str) -> bool:
        return update_host_memory_from_event_payload(self.host_memory, host_event_payload, ts=ts)

    def recent_host_proxy_outcomes(
        self,
        host: str,
        *,
        signal_tags: list[str] | tuple[str, ...] | set[str] | frozenset[str] | None = None,
        proxy_label_or_id: str | None = None,
        limit: int | None = None,
    ) -> list[dict[str, Any]]:
        return recent_host_proxy_outcomes_from_memory(
            self.host_memory,
            host,
            signal_tags=signal_tags,
            proxy_label_or_id=proxy_label_or_id,
            limit=limit,
        )

    def recent_governor_signal_proxy_labels(
        self,
        host: str,
        *,
        limit: int | None = None,
    ) -> list[str]:
        return recent_governor_signal_proxy_labels_from_memory(self.host_memory, host, limit=limit)

    @property
    def governor_signal_tags(self) -> frozenset[str]:
        return HOST_MEMORY_GOVERNOR_SIGNAL_TAGS

    def _stage_outbox_cursor(self) -> dict[str, Any]:
        cursor = normalize_stage_outbox_cursor(self.run_metadata.get("stage_outbox_cursor"))
        self.run_metadata["stage_outbox_cursor"] = cursor
        return dict(cursor)

    def _stage_handoffs(self) -> dict[str, Any]:
        handoffs = normalize_stage_handoff_state(self.run_metadata.get("stage_handoffs"))
        self.run_metadata["stage_handoffs"] = handoffs
        return handoffs

    def _stage_pickups(self) -> dict[str, Any]:
        handoffs = self._stage_handoffs()
        pickups = synchronize_stage_handoff_pickup_state(self.run_metadata.get("stage_pickups"), handoffs)
        self.run_metadata["stage_pickups"] = pickups
        return pickups

    def _stage_execution_evidence(self) -> dict[str, Any]:
        execution_evidence = normalize_explicit_stage_execution_state(
            self.run_metadata.get(STAGE_EXECUTION_EVIDENCE_KEY),
        )
        self.run_metadata[STAGE_EXECUTION_EVIDENCE_KEY] = execution_evidence
        return execution_evidence

    def _stage_work_units(self) -> dict[str, Any]:
        handoffs = self._stage_handoffs()
        pickups = self._stage_pickups()
        execution_evidence = self._stage_execution_evidence()
        work_units = synchronize_stage_work_unit_state(
            self.run_metadata.get("stage_work_units"),
            handoffs,
            pickups,
            explicit_execution_state=execution_evidence,
        )
        self.run_metadata["stage_work_units"] = work_units
        return work_units

    def _read_unread_stage_messages(self) -> tuple[list[dict[str, Any]], dict[str, Any], dict[str, Any]]:
        current_cursor = self._stage_outbox_cursor()
        unread_messages, next_cursor = read_unconsumed_stage_messages(
            self.output_dir,
            cursor=current_cursor,
        )
        return unread_messages, current_cursor, next_cursor

    def _ensure_run_id(self) -> str:
        run_id = str(self.run_metadata.get("run_id", "") or "")
        if run_id:
            return run_id
        run_id = _generate_run_id()
        self.run_metadata["run_id"] = run_id
        return run_id

    def _build_summary_surface(
        self,
        *,
        public_results: list[dict[str, Any]] | None = None,
        checkpoint_only_count: int | None = None,
    ) -> dict[str, Any]:
        payload = dict(self.summary)
        if public_results is None or checkpoint_only_count is None:
            public_results, checkpoint_only_count = self._public_results(public_results)
        rows_selected = max(int(payload.get("rows_selected", 0) or 0), 0)
        processed_rows = max(int(payload.get("processed_rows", 0) or 0), 0) + checkpoint_only_count
        resume_skipped_rows = max(int(payload.get("resume_skipped_rows", 0) or 0), 0)
        if rows_selected:
            resume_skipped_rows = min(resume_skipped_rows, rows_selected)
            payload["remaining_rows"] = max(rows_selected - resume_skipped_rows - processed_rows, 0)
        else:
            payload["remaining_rows"] = max(int(payload.get("remaining_rows", 0) or 0), 0)
        payload["processed_rows"] = processed_rows
        payload["completed_rows"] = len(public_results)
        payload["resume_skipped_rows"] = resume_skipped_rows
        contract_status = self._public_output_contract_status(
            payload=payload,
            public_results=public_results,
            checkpoint_only_count=checkpoint_only_count,
        )
        terminal_run = bool(contract_status["terminal_run"])
        full_output_surface = bool(contract_status["full_output_surface"])
        run_status = str(payload.get("run_status", self.run_metadata.get("run_status", "")) or "")
        finish_reason = str(payload.get("finish_reason", self.run_metadata.get("finish_reason", "")) or "")
        if not full_output_surface:
            final_exports_state = FINAL_EXPORTS_STATE_SUPPRESSED_NON_TERMINAL
        elif bool(contract_status["all_selected_completed"]) and not terminal_run:
            final_exports_state = FINAL_EXPORTS_STATE_ALL_SELECTED_COMPLETED
        elif run_status == RUN_STATUS_COMPLETED and (
            not finish_reason or finish_reason == RUN_FINISH_REASON_NORMAL_COMPLETION
        ):
            final_exports_state = FINAL_EXPORTS_STATE_TERMINAL_COMPLETED
        else:
            final_exports_state = FINAL_EXPORTS_STATE_TERMINAL_PARTIAL
        payload["public_output_contract"] = {
            "contract_version": PUBLIC_OUTPUT_CONTRACT_VERSION,
            "terminal_run": terminal_run,
            "all_selected_completed": bool(contract_status["all_selected_completed"]),
            "full_output_surface": full_output_surface,
            "full_output_surface_reason": str(contract_status["full_output_surface_reason"]),
            "run_finished_required_for_final_exports": not full_output_surface,
            "run_status": run_status,
            "finish_reason": finish_reason,
            "public_result_count": len(public_results),
            "checkpoint_only_count": checkpoint_only_count,
            "remaining_rows": payload["remaining_rows"],
            "final_exports": {
                "state": final_exports_state,
                "available": full_output_surface,
                "row_count": len(public_results) if full_output_surface else 0,
                "paths": [
                    self.final_results_csv.name,
                    self.final_results_xlsx.name,
                ],
            },
            "warning": ""
            if full_output_surface
            else (
                "Non-terminal run: final_results.csv and final_results.xlsx "
                "are suppressed until run_finished is recorded or all selected rows are completed."
            ),
        }
        run_id = str(self.run_metadata.get("run_id", "") or "")
        if run_id:
            payload["run_id"] = run_id
        payload[THROUGHPUT_TELEMETRY_KEY] = sanitize_for_json(
            dict(self.run_metadata.get(THROUGHPUT_TELEMETRY_KEY))
        ) if isinstance(self.run_metadata.get(THROUGHPUT_TELEMETRY_KEY), Mapping) else {}
        return payload

    def _public_output_contract_status(
        self,
        *,
        payload: Mapping[str, Any],
        public_results: list[dict[str, Any]],
        checkpoint_only_count: int,
    ) -> dict[str, Any]:
        rows_selected = max(int(payload.get("rows_selected", 0) or 0), 0)
        processed_rows = max(int(payload.get("processed_rows", 0) or 0), 0)
        resume_skipped_rows = max(int(payload.get("resume_skipped_rows", 0) or 0), 0)
        if rows_selected:
            resume_skipped_rows = min(resume_skipped_rows, rows_selected)
        remaining_rows = (
            max(rows_selected - resume_skipped_rows - processed_rows, 0)
            if rows_selected
            else max(int(payload.get("remaining_rows", 0) or 0), 0)
        )
        expected_public_result_count = max(rows_selected - resume_skipped_rows, 0)
        all_selected_completed = (
            rows_selected > 0
            and remaining_rows == 0
            and len(public_results) >= expected_public_result_count
        )
        terminal_run = self._summary_is_terminal()
        full_output_surface = terminal_run or all_selected_completed
        if terminal_run:
            reason = "terminal_run"
        elif all_selected_completed:
            reason = FINAL_EXPORTS_STATE_ALL_SELECTED_COMPLETED
        else:
            reason = FINAL_EXPORTS_STATE_SUPPRESSED_NON_TERMINAL
        return {
            "terminal_run": terminal_run,
            "all_selected_completed": all_selected_completed,
            "full_output_surface": full_output_surface,
            "full_output_surface_reason": reason,
            "remaining_rows": remaining_rows,
            "expected_public_result_count": expected_public_result_count,
            "checkpoint_only_count": max(int(checkpoint_only_count or 0), 0),
        }

    def _has_full_public_output_surface(
        self,
        *,
        public_results: list[dict[str, Any]] | None = None,
        checkpoint_only_count: int | None = None,
    ) -> bool:
        if public_results is None or checkpoint_only_count is None:
            public_results, checkpoint_only_count = self._public_results(public_results)
        return bool(
            self._public_output_contract_status(
                payload=self.summary,
                public_results=public_results,
                checkpoint_only_count=checkpoint_only_count,
            )["full_output_surface"]
        )


__all__ = ["ProgressStore"]

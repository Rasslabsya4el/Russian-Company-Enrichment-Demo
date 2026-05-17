from __future__ import annotations

import argparse
from datetime import datetime, timezone
import os
import sys
import time
from collections import deque
from concurrent.futures import CancelledError, Future, ThreadPoolExecutor
from contextlib import contextmanager, nullcontext
from dataclasses import asdict, dataclass
from pathlib import Path
from threading import Event
from time import monotonic as _runtime_monotonic
from urllib.parse import urlparse

import company_enrichment_core as core
from app.dossier import build_and_store_company_dossier
from app.discovery import build_domain_resolution, choose_candidate_sites
from app.llm.benchmark_capture import (
    BenchmarkAwareSiteAuthenticityAnalyzer,
    LLMBenchmarkCaptureConfig,
    LLMBenchmarkCaptureWriter,
    describe_content_review_prod_skip_reason,
    parse_llm_benchmark_force_stages,
)
from app.runtime import ProgressStore, ProxyPool
from app.runtime.bounded_executor import (
    DOWNSTREAM_EXECUTION_PHASE_KEY,
    PrefetchedCompanySourceBatch,
    RollingCompanySourceBatchExecutor,
    build_company_source_batch_key,
    open_company_source_search_executor,
    plan_direct_default_bounded_executor,
)
from app.runtime.concurrency import build_source_execution_guardrails
from app.runtime.host_governor import HostGovernorLedger
from app.runtime.proxy6 import (
    PROXY_PROVIDER_INVENTORY_EMPTY_OR_EXPIRED,
    diagnose_proxy6_inventory_from_env,
)
from app.runtime.required_source_deferred import (
    OUTCOME_DEFERRED_ROW_TRANSIENT,
    OUTCOME_NON_DEFER_FAIL_FAST,
    OUTCOME_SYSTEMIC_STOP,
    RUN_FINISH_REASON_DEFERRED_REQUIRED_SOURCES,
    RUN_STATUS_COMPLETED_WITH_DEFERRED_REQUIRED_SOURCES,
    build_required_source_deferred_record,
    classify_required_source_outcome,
    first_unresolved_record_for_source,
)
from app.runtime.queue_families import (
    CANDIDATE_SITE_STAGE_NAME,
    DEEP_PARSE_STAGE_NAME,
    EXTRA_CHECK_STAGE_NAME,
    FACTORY_SITE_STAGE_NAME,
    LLM_STAGE_NAME,
    OCR_STAGE_NAME,
    build_aggregator_site_queue_family_contour,
    build_downstream_worker_pool_contour,
    build_deep_parse_queue_family_contour,
)
from app.runtime.row_selection import parse_ordinals, resolve_row_selection, select_rows_for_window
from app.runtime.stage_pools import (
    SourceLaneTelemetryLedger,
    StagePoolGovernor,
    build_throughput_telemetry_payload,
)
from app.runtime.work_units import (
    AGGREGATOR_SITE_EXECUTION_BOUNDARY,
    DEEP_PARSE_EXECUTION_BOUNDARY,
    EXPLICIT_EXECUTION_BOUNDARY,
)
from app.site_intelligence import (
    SiteAuthHelpers,
    classify_content_record,
    should_use_llm_record_review,
)
from app.site_intelligence.factory_site_parser import FactorySiteParser, FactorySiteParserCompany
from app.site_intelligence.factory_site_parser.documents import FactorySiteDocumentsStage
from app.site_intelligence.geo_admission import (
    evaluate_geo_deep_parse_admission,
    parse_allowed_geo_buckets,
)
from app.site_intelligence.preparse_trust_gate import gate_candidate_sites_before_deep_parse
from app.sources import CheckoSource, ListOrgSource, RusprofileSource, SparkSource, ZachestnyBiznesSource


LIST_ORG_OFFLINE_MODE = "offline_snapshot"
LIST_ORG_OFFLINE_STALE_NOTE = "List-Org offline snapshot reused from prior payload; current run did not refresh this source"
COMPLETED_RESULT_CHECKPOINT_KEY = "completed_company_result"
NO_CANDIDATE_SITE_NOTE = "Не нашел кандидатов на сайт из агрегаторов и доменов почты"
SiteAuthenticityAnalyzer = BenchmarkAwareSiteAuthenticityAnalyzer
RUN_STATUS_CANCELLED = "cancelled"
RUN_STATUS_ABORTED = "aborted"
RUN_STATUS_FAILED_REQUIRED_SOURCE = "failed_required_source"
RUN_FINISH_REASON_CANCELLED = "cancelled"
RUN_FINISH_REASON_ABORTED = "aborted"
RUN_FINISH_REASON_REQUIRED_SOURCE = "required_source_not_operational"
REQUIRED_SOURCE_TERMINAL_ERROR_TYPE = "required_source_not_operational"
DOWNSTREAM_PREFETCH_PENDING_QUEUE_MIN = 8
DOWNSTREAM_PREFETCH_PENDING_QUEUE_PER_WORKER = 8
DOWNSTREAM_PREFETCH_PENDING_QUEUE_MAX = 32
DOWNSTREAM_PREFETCH_SELECTED_SURFACE_QUEUE_MAX = 128
READY_IDLE_SOURCE_WAIT_DRAIN_SECONDS = 120.0
READY_IDLE_SOURCE_WAIT_DRAIN_MAX_ROWS = 1
SOURCE_REQUEST_EVENT_TYPES = frozenset(
    {
        "request_ok",
        "request_ok_insecure_tls",
        "http_error",
        "rate_limited",
        "bot_gate",
        "request_error",
        "request_blocked_by_policy",
        "cooldown_skip",
    }
)
DOWNSTREAM_STAGE_SPAN_EVENT_TYPE = "downstream_stage_span"


def _allowed_geo_buckets_for_deep_parse() -> tuple[str, ...]:
    return parse_allowed_geo_buckets(
        os.getenv("PARSER_ALLOWED_GEO_BUCKETS", "") or os.getenv("PARSER_ALLOWED_GEO", "")
    )


def _geo_deep_parse_admission_note(
    *,
    source_results: dict[str, core.SourceResult],
    row: core.RowInput,
    merged_contacts: dict[str, list[str]],
) -> str:
    allowed_buckets = _allowed_geo_buckets_for_deep_parse()
    if not allowed_buckets:
        return ""
    result_payload = {"merged_contacts": merged_contacts}
    geo_signal = core.build_geo_signal_payload(result_payload)
    decision = evaluate_geo_deep_parse_admission(
        geo_signal=geo_signal,
        allowed_buckets=allowed_buckets,
    )
    return decision.note if decision.skip_deep_parse else ""


@dataclass(frozen=True, slots=True)
class PrefetchedAggregatorSiteExecution:
    candidate_site_payloads: tuple[dict[str, object], ...]
    site_gate_decision_payloads: tuple[dict[str, object], ...]
    deep_parse_sites: tuple[str, ...]
    gate_notes: tuple[str, ...]
    known_contacts: dict[str, list[str]]


@dataclass(frozen=True, slots=True)
class PrefetchedCompanyDownstreamExecution:
    aggregator_execution: PrefetchedAggregatorSiteExecution
    completed_result_payload: dict[str, object] | None = None


@dataclass(slots=True)
class PendingExplicitRuntimeRow:
    row: core.RowInput
    existing_payload: dict[str, object] | None
    resume_recovery: bool
    refreshed_source_names: tuple[str, ...]
    active_source_names: tuple[str, ...]
    deferred_required_source_retry_names: tuple[str, ...] = ()
    prefetched_aggregator_execution: PrefetchedAggregatorSiteExecution | None = None
    prefetched_completed_result_payload: dict[str, object] | None = None
    prefetched_runtime_events: tuple[dict[str, object], ...] = ()
    downstream_future: Future[PrefetchedCompanyDownstreamExecution] | None = None
    downstream_ready_at: str = ""
    ordered_drain_started_at: str = ""
    final_drain_wait_started_at: str = ""
    final_drain_wait_finished_at: str = ""
    final_drain_wait_seconds: float = 0.0


class RequiredSourceOperationalError(RuntimeError):
    def __init__(
        self,
        *,
        source_name: str,
        source_status: str,
        source_access_mode: str,
        reason: str,
    ) -> None:
        self.source_name = core.normalize_whitespace(source_name)
        self.source_status = core.normalize_whitespace(source_status)
        self.source_access_mode = core.normalize_whitespace(source_access_mode)
        self.reason = core.normalize_whitespace(reason)
        super().__init__(self.reason or self.source_status or self.source_name or "required_source_not_operational")


def _consume_controlled_stop_request(
    *,
    progress: ProgressStore,
    logger,
    checkpoint: str,
    row: core.RowInput | None = None,
    execution_boundary: str = "",
) -> dict[str, str] | None:
    stop_request = progress.consume_controlled_stop_request()
    if stop_request is None:
        return None
    logger.info(
        "Controlled stop requested: checkpoint=%s inn=%s boundary=%s requested_at=%s reason=%s",
        checkpoint,
        row.inn if row is not None else "",
        execution_boundary,
        stop_request.get("requested_at", ""),
        stop_request.get("reason", ""),
    )
    return stop_request


def _terminal_context(
    *,
    checkpoint: str,
    row: core.RowInput | None = None,
    execution_boundary: str = "",
    source_name: str = "",
    source_status: str = "",
    source_access_mode: str = "",
) -> dict[str, str]:
    return {
        "checkpoint": core.normalize_whitespace(str(checkpoint or "")),
        "inn": core.normalize_whitespace(str(row.inn if row is not None else "")),
        "execution_boundary": core.normalize_whitespace(str(execution_boundary or "")),
        "source": core.normalize_whitespace(str(source_name or "")),
        "source_status": core.normalize_whitespace(str(source_status or "")),
        "source_access_mode": core.normalize_whitespace(str(source_access_mode or "")),
    }


def _is_cancelled_terminal_exception(exc: BaseException) -> bool:
    return isinstance(exc, (KeyboardInterrupt, CancelledError))


def _is_required_source_terminal_exception(exc: BaseException) -> bool:
    return isinstance(exc, RequiredSourceOperationalError)


def _terminal_error_payload(exc: BaseException) -> dict[str, str]:
    if isinstance(exc, RequiredSourceOperationalError):
        return {
            "type": REQUIRED_SOURCE_TERMINAL_ERROR_TYPE,
            "message": exc.reason,
        }
    error_type = type(exc).__name__
    message = core.normalize_whitespace(str(exc))
    if not message and isinstance(exc, KeyboardInterrupt):
        message = "Run interrupted by operator"
    elif not message and isinstance(exc, CancelledError):
        message = "Run cancelled before completion"
    return {
        "type": error_type,
        "message": message,
    }


def _required_source_stop_request(exc: RequiredSourceOperationalError) -> dict[str, str]:
    return {
        "requested_at": core.utc_now_iso(),
        "reason": exc.reason,
    }


def _cleanup_runtime_resources(
    *,
    logger,
    source_lane_executors: tuple[RollingCompanySourceBatchExecutor | None, ...],
    downstream_prefetch_executor: ThreadPoolExecutor | None,
    downstream_prefetch_shutdown: Event | None,
    http_client: object,
) -> None:
    if downstream_prefetch_shutdown is not None:
        downstream_prefetch_shutdown.set()
    if downstream_prefetch_executor is not None:
        downstream_prefetch_executor.shutdown(wait=True, cancel_futures=True)
    for source_lane_executor in source_lane_executors:
        if source_lane_executor is not None:
            source_lane_executor.close()
    session = getattr(http_client, "session", None)
    session_close = getattr(session, "close", None)
    if callable(session_close):
        session_close()


def _buffered_task_key_for_row(row: core.RowInput) -> str:
    row_index, identity = build_company_source_batch_key(row)
    return f"{row_index}:{identity}"


def _resolve_downstream_prefetch_queue_limit(
    worker_count: int,
    *,
    selected_rows_count: int | None = None,
) -> int:
    resolved_worker_count = max(int(worker_count or 0), 1)
    target_depth = max(
        resolved_worker_count + 1,
        resolved_worker_count * DOWNSTREAM_PREFETCH_PENDING_QUEUE_PER_WORKER,
        DOWNSTREAM_PREFETCH_PENDING_QUEUE_MIN,
    )
    selected_depth = max(int(selected_rows_count or 0), 0)
    if selected_depth:
        target_depth = max(
            target_depth,
            min(selected_depth, DOWNSTREAM_PREFETCH_SELECTED_SURFACE_QUEUE_MAX),
        )
    bounded_depth = max(DOWNSTREAM_PREFETCH_PENDING_QUEUE_MAX, resolved_worker_count + 1)
    if selected_depth:
        bounded_depth = max(
            bounded_depth,
            min(selected_depth, DOWNSTREAM_PREFETCH_SELECTED_SURFACE_QUEUE_MAX),
        )
    return min(target_depth, bounded_depth)


def _resolve_downstream_prefetch_ready_drain_limit(
    worker_count: int,
    *,
    pending_queue_limit: int,
) -> int:
    resolved_pending_queue_limit = max(int(pending_queue_limit or 0), 0)
    if resolved_pending_queue_limit <= 0:
        return 0
    resolved_worker_count = max(int(worker_count or 0), 1)
    target_depth = max(
        DOWNSTREAM_PREFETCH_PENDING_QUEUE_MAX,
        resolved_worker_count + 1,
    )
    return min(target_depth, resolved_pending_queue_limit)


def _resolve_downstream_prefetch_ready_drain_low_watermark(
    *,
    ready_drain_limit: int,
) -> int:
    resolved_ready_drain_limit = max(int(ready_drain_limit or 0), 0)
    if resolved_ready_drain_limit <= 1:
        return 0
    return max(resolved_ready_drain_limit // 2, 1)


def _resolve_checko_worker_lane_budget(
    *,
    active_source_names: list[str],
    source_lane_scheduler: dict[str, object],
) -> int:
    if "checko" not in active_source_names:
        return 0
    worker_lane_budget_map = source_lane_scheduler.get("per_source_worker_lane_budget_map")
    if not isinstance(worker_lane_budget_map, dict):
        return 0
    try:
        return max(int(worker_lane_budget_map.get("checko", 0) or 0), 0)
    except (TypeError, ValueError):
        return 0


def _prepare_prefetched_company_downstream_batch(
    *,
    row: core.RowInput,
    source_results: dict[str, core.SourceResult],
    active_source_names: list[str],
    analyzer: BenchmarkAwareSiteAuthenticityAnalyzer,
    factory_site_parser_factory,
    buffered_progress_store,
    shutdown_requested,
    stage_governor: StagePoolGovernor | None = None,
    telemetry_tick: object = None,
    stage_span_recorder: object = None,
) -> PrefetchedCompanyDownstreamExecution:
    task_key = _buffered_task_key_for_row(row)
    with buffered_progress_store.bind(task_key, phase_key=DOWNSTREAM_EXECUTION_PHASE_KEY):
        if callable(shutdown_requested) and shutdown_requested():
            raise CancelledError(f"downstream prefetch cancelled before execution for {task_key}")
        return _prepare_prefetched_company_downstream_execution(
            row=row,
            source_results=source_results,
            active_source_names=active_source_names,
            analyzer=analyzer,
            factory_site_parser_factory=factory_site_parser_factory,
            stage_governor=stage_governor,
            shutdown_requested=shutdown_requested,
            telemetry_tick=telemetry_tick,
            stage_span_recorder=stage_span_recorder,
        )


def _submit_prefetched_company_downstream_batch(
    *,
    executor: ThreadPoolExecutor | None,
    row: core.RowInput,
    source_results: dict[str, core.SourceResult],
    active_source_names: list[str],
    analyzer: BenchmarkAwareSiteAuthenticityAnalyzer,
    factory_site_parser_factory,
    buffered_progress_store,
    shutdown_requested,
    stage_governor: StagePoolGovernor | None = None,
    telemetry_tick: object = None,
    stage_span_recorder: object = None,
) -> Future[PrefetchedCompanyDownstreamExecution] | None:
    if executor is None:
        return None
    return executor.submit(
        _prepare_prefetched_company_downstream_batch,
        row=row,
        source_results=dict(source_results),
        active_source_names=list(active_source_names),
        analyzer=analyzer,
        factory_site_parser_factory=factory_site_parser_factory,
        buffered_progress_store=buffered_progress_store,
        shutdown_requested=shutdown_requested,
        stage_governor=stage_governor,
        telemetry_tick=telemetry_tick,
        stage_span_recorder=stage_span_recorder,
    )


def _resolve_pending_prefetched_downstream_batch(
    *,
    pending_row: PendingExplicitRuntimeRow,
    buffered_progress_store,
    telemetry_tick: object = None,
) -> tuple[
    PrefetchedAggregatorSiteExecution | None,
    dict[str, object] | None,
    tuple[dict[str, object], ...],
]:
    prefetched_aggregator_execution = pending_row.prefetched_aggregator_execution
    prefetched_completed_result_payload = pending_row.prefetched_completed_result_payload
    prefetched_runtime_events = tuple(pending_row.prefetched_runtime_events)
    future = pending_row.downstream_future
    if future is None:
        if not pending_row.downstream_ready_at:
            pending_row.downstream_ready_at = core.utc_now_iso()
        return (
            prefetched_aggregator_execution,
            prefetched_completed_result_payload,
            prefetched_runtime_events,
        )
    wait_started_monotonic: float | None = None
    while not future.done():
        if not pending_row.final_drain_wait_started_at:
            pending_row.final_drain_wait_started_at = core.utc_now_iso()
            wait_started_monotonic = _runtime_monotonic()
        if callable(telemetry_tick):
            telemetry_tick()
        time.sleep(0.05)
    if pending_row.final_drain_wait_started_at and not pending_row.final_drain_wait_finished_at:
        pending_row.final_drain_wait_finished_at = core.utc_now_iso()
        if wait_started_monotonic is not None:
            pending_row.final_drain_wait_seconds = round(
                max(_runtime_monotonic() - wait_started_monotonic, 0.0),
                4,
            )
    if not pending_row.downstream_ready_at:
        pending_row.downstream_ready_at = pending_row.final_drain_wait_finished_at or core.utc_now_iso()
    prefetched_execution = future.result()
    task_key = _buffered_task_key_for_row(pending_row.row)
    runtime_events = tuple(
        buffered_progress_store.take(task_key, phase_key=DOWNSTREAM_EXECUTION_PHASE_KEY)
    )
    pending_row.downstream_future = None
    pending_row.prefetched_runtime_events = runtime_events
    pending_row.prefetched_aggregator_execution = prefetched_execution.aggregator_execution
    pending_row.prefetched_completed_result_payload = (
        _clone_json_like(prefetched_execution.completed_result_payload)
        if isinstance(prefetched_execution.completed_result_payload, dict)
        else None
    )
    return (
        pending_row.prefetched_aggregator_execution,
        pending_row.prefetched_completed_result_payload,
        pending_row.prefetched_runtime_events,
    )


def _mark_pending_downstream_ready(pending_row: PendingExplicitRuntimeRow) -> None:
    future = pending_row.downstream_future
    if future is None:
        return

    def mark_ready(_future: Future[PrefetchedCompanyDownstreamExecution]) -> None:
        if not pending_row.downstream_ready_at:
            pending_row.downstream_ready_at = core.utc_now_iso()

    future.add_done_callback(mark_ready)


def _pending_downstream_is_ready(pending_row: PendingExplicitRuntimeRow) -> bool:
    future = pending_row.downstream_future
    return future is not None and future.done()


def _pending_downstream_ready_count(pending_rows: deque[PendingExplicitRuntimeRow]) -> int:
    return sum(1 for pending_row in pending_rows if _pending_downstream_is_ready(pending_row))


def _parse_runtime_iso(value: object) -> datetime | None:
    text = core.normalize_whitespace(str(value or ""))
    if not text:
        return None
    if text.endswith("Z"):
        text = f"{text[:-1]}+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed


def _runtime_iso_elapsed_seconds(started_at: object, finished_at: object) -> float:
    started = _parse_runtime_iso(started_at)
    finished = _parse_runtime_iso(finished_at)
    if started is None or finished is None:
        return 0.0
    return round(max((finished - started).total_seconds(), 0.0), 4)


def _oldest_ready_pending_downstream_idle_seconds(
    pending_rows: deque[PendingExplicitRuntimeRow],
    *,
    now_iso: str,
) -> float:
    oldest_idle_seconds = 0.0
    for pending_row in pending_rows:
        if not _pending_downstream_is_ready(pending_row):
            continue
        idle_seconds = _runtime_iso_elapsed_seconds(pending_row.downstream_ready_at, now_iso)
        oldest_idle_seconds = max(oldest_idle_seconds, idle_seconds)
    return oldest_idle_seconds


def _pop_pending_explicit_runtime_row_at(
    pending_rows: deque[PendingExplicitRuntimeRow],
    index: int,
) -> PendingExplicitRuntimeRow:
    pending_rows.rotate(-index)
    try:
        return pending_rows.popleft()
    finally:
        pending_rows.rotate(index)


def _pop_next_pending_explicit_runtime_row(
    pending_rows: deque[PendingExplicitRuntimeRow],
    *,
    prefer_ready: bool,
) -> PendingExplicitRuntimeRow:
    if prefer_ready:
        for index, pending_row in enumerate(pending_rows):
            if _pending_downstream_is_ready(pending_row):
                return _pop_pending_explicit_runtime_row_at(pending_rows, index)
    return pending_rows.popleft()


def _finalization_timing_from_pending_row(pending_row: PendingExplicitRuntimeRow) -> dict[str, object]:
    return {
        "ordered_drain_started_at": pending_row.ordered_drain_started_at,
        "downstream_ready_at": pending_row.downstream_ready_at,
        "final_drain_wait_started_at": pending_row.final_drain_wait_started_at,
        "final_drain_wait_finished_at": pending_row.final_drain_wait_finished_at,
        "final_drain_wait_seconds": pending_row.final_drain_wait_seconds,
    }


def _persist_completed_company_result_with_finalization_timing(
    *,
    progress: ProgressStore,
    result: core.CompanyResult,
    total_rows: int,
    processed_rows: int,
    work_unit: dict[str, object],
    finalization_timing: dict[str, object] | None,
) -> None:
    public_materialization_started_at = core.utc_now_iso()
    progress.persist_completed_company_result(
        result,
        total_rows=total_rows,
        processed_rows=processed_rows,
        dossier_builder=build_and_store_company_dossier,
    )
    public_materialization_finished_at = core.utc_now_iso()
    progress.record_downstream_finalization_timing(
        inn=result.inn,
        row_index=result.row_index,
        company_name=result.company_name,
        handoff_fingerprint=str(work_unit.get("handoff_fingerprint") or ""),
        ordered_drain_started_at=str((finalization_timing or {}).get("ordered_drain_started_at") or ""),
        downstream_ready_at=str((finalization_timing or {}).get("downstream_ready_at") or ""),
        final_drain_wait_started_at=str((finalization_timing or {}).get("final_drain_wait_started_at") or ""),
        final_drain_wait_finished_at=str((finalization_timing or {}).get("final_drain_wait_finished_at") or ""),
        final_drain_wait_seconds=(finalization_timing or {}).get("final_drain_wait_seconds"),
        public_materialization_started_at=public_materialization_started_at,
        public_materialization_finished_at=public_materialization_finished_at,
    )


def _drain_pending_explicit_runtime_row(
    *,
    pending_row: PendingExplicitRuntimeRow,
    progress: ProgressStore,
    controlled_stop_request: dict[str, str] | None,
    active_source_names: list[str],
    analyzer: BenchmarkAwareSiteAuthenticityAnalyzer,
    factory_site_parser: FactorySiteParser,
    downstream_stage_governor: StagePoolGovernor | None,
    total_rows: int,
    processed_rows: int,
    logger,
    buffered_progress_store=None,
    telemetry_tick: object = None,
    ) -> tuple[int, dict[str, str] | None, dict[str, str]]:
    row = pending_row.row
    row_active_source_names = list(pending_row.active_source_names or tuple(active_source_names))
    if not pending_row.ordered_drain_started_at:
        pending_row.ordered_drain_started_at = core.utc_now_iso()
    terminal_context_payload = _terminal_context(
        checkpoint="before_explicit_boundary",
        row=row,
        execution_boundary=AGGREGATOR_SITE_EXECUTION_BOUNDARY,
    )
    try:
        (
            prefetched_aggregator_execution,
            prefetched_completed_result_payload,
            prefetched_runtime_events,
        ) = _resolve_pending_prefetched_downstream_batch(
            pending_row=pending_row,
            buffered_progress_store=buffered_progress_store,
            telemetry_tick=telemetry_tick,
        ) if buffered_progress_store is not None else (
            pending_row.prefetched_aggregator_execution,
            pending_row.prefetched_completed_result_payload,
            tuple(pending_row.prefetched_runtime_events),
        )
        finalization_timing = _finalization_timing_from_pending_row(pending_row)
        pending_work_unit = _pending_selected_explicit_stage_work_units(
            progress=progress,
            rows=[row],
        ).get(row.inn)
        if not pending_work_unit:
            raise RuntimeError(f"Missing pending explicit runtime work unit for INN {row.inn}")
        while pending_work_unit:
            execution_boundary = str(pending_work_unit.get("execution_boundary") or "")
            terminal_context_payload = _terminal_context(
                checkpoint="before_explicit_boundary",
                row=row,
                execution_boundary=execution_boundary,
            )
            if controlled_stop_request is None:
                controlled_stop_request = _consume_controlled_stop_request(
                    progress=progress,
                    logger=logger,
                    checkpoint="before_explicit_boundary",
                    row=row,
                    execution_boundary=execution_boundary,
                )
            if controlled_stop_request is not None:
                break
            terminal_context_payload = _terminal_context(
                checkpoint="explicit_boundary",
                row=row,
                execution_boundary=execution_boundary,
            )
            if execution_boundary == AGGREGATOR_SITE_EXECUTION_BOUNDARY:
                processed_rows, row_completed = _consume_aggregator_site_work_unit_v2(
                    progress=progress,
                    row=row,
                    existing_payload=pending_row.existing_payload,
                    aggregator_site_work_unit=pending_work_unit,
                    resume_recovery=pending_row.resume_recovery,
                    active_source_names=row_active_source_names,
                    analyzer=analyzer,
                    total_rows=total_rows,
                    processed_rows=processed_rows,
                    logger=logger,
                    prefetched_aggregator_execution=prefetched_aggregator_execution,
                    prefetched_completed_result_payload=prefetched_completed_result_payload,
                    prefetched_runtime_events=prefetched_runtime_events,
                    downstream_stage_governor=downstream_stage_governor,
                    telemetry_tick=telemetry_tick,
                    finalization_timing=finalization_timing,
                )
                if callable(telemetry_tick):
                    telemetry_tick()
                prefetched_aggregator_execution = None
                prefetched_completed_result_payload = None
                prefetched_runtime_events = ()
                if row_completed:
                    _mark_deferred_required_source_retry_targets_resolved(
                        progress=progress,
                        row=row,
                        source_results=_source_results_from_stage_work_unit(pending_work_unit),
                        source_names=pending_row.deferred_required_source_retry_names,
                        logger=logger,
                    )
                    break
            elif execution_boundary == DEEP_PARSE_EXECUTION_BOUNDARY:
                processed_rows = _consume_deep_parse_work_unit(
                    progress=progress,
                    row=row,
                    existing_payload=pending_row.existing_payload,
                    deep_parse_work_unit=pending_work_unit,
                    resume_recovery=pending_row.resume_recovery,
                    active_source_names=row_active_source_names,
                    analyzer=analyzer,
                    factory_site_parser=factory_site_parser,
                    downstream_stage_governor=downstream_stage_governor,
                    total_rows=total_rows,
                    processed_rows=processed_rows,
                    logger=logger,
                    telemetry_tick=telemetry_tick,
                    finalization_timing=finalization_timing,
                )
                _mark_deferred_required_source_retry_targets_resolved(
                    progress=progress,
                    row=row,
                    source_results=_source_results_from_stage_work_unit(pending_work_unit),
                    source_names=pending_row.deferred_required_source_retry_names,
                    logger=logger,
                )
                if callable(telemetry_tick):
                    telemetry_tick()
                break
            else:
                raise RuntimeError(
                    f"Unsupported explicit execution boundary '{execution_boundary}' for INN {row.inn}"
                )
            pending_work_unit = _pending_selected_explicit_stage_work_units(
                progress=progress,
                rows=[row],
            ).get(row.inn)
            if not pending_work_unit:
                raise RuntimeError(f"Missing pending deep_parse queue work unit for INN {row.inn}")
    except BaseException as exc:
        setattr(exc, "_terminal_context_payload", dict(terminal_context_payload))
        raise
    return processed_rows, controlled_stop_request, terminal_context_payload


def _option_was_provided(argv: list[str], option: str) -> bool:
    return any(token == option or token.startswith(f"{option}=") for token in argv)


def _resolve_requested_company_concurrency(raw_value: object) -> int:
    try:
        requested = int(str(raw_value).strip())
    except (TypeError, ValueError) as exc:
        raise ValueError("--company-concurrency должен быть целым числом >= 1") from exc
    if requested < 1:
        raise ValueError("--company-concurrency должен быть >= 1")
    return requested


def _resolve_usable_proxy_pool_count(proxy_pool: object, *, source_name: str = "") -> int:
    usable_count = getattr(proxy_pool, "usable_count", None)
    if callable(usable_count):
        try:
            if source_name:
                return max(int(usable_count(source_name=source_name)), 0)
            return max(int(usable_count()), 0)
        except TypeError:
            try:
                return max(int(usable_count()), 0)
            except Exception:
                return 0
        except Exception:
            return 0
    entries = getattr(proxy_pool, "entries", None)
    if isinstance(entries, list):
        return len(entries)
    return 0


def _checko_proxy_provider_startup_stop(
    *,
    active_source_names: list[str],
    source_search_rows: list[core.RowInput],
    proxy_pool: object,
) -> tuple[str, dict[str, object]] | None:
    if "checko" not in active_source_names or not source_search_rows or proxy_pool is None:
        return None
    usable_checko_proxy_count = _resolve_usable_proxy_pool_count(proxy_pool, source_name="checko")
    if usable_checko_proxy_count > 0:
        provider_diagnostic = getattr(proxy_pool, "proxy_provider_diagnostic", None)
        if callable(provider_diagnostic):
            try:
                diagnostic = provider_diagnostic()
            except Exception:
                diagnostic = diagnose_proxy6_inventory_from_env()
        else:
            diagnostic = diagnose_proxy6_inventory_from_env()
        if diagnostic.provider_status != PROXY_PROVIDER_INVENTORY_EMPTY_OR_EXPIRED:
            return None
    else:
        diagnostic = diagnose_proxy6_inventory_from_env()
    detail = diagnostic.operator_message()
    reason = (
        f"Checko proxy-bound required source cannot start: {core.REQUEST_BLOCKED_NO_PROXY_NO_POOL_REASON}; "
        f"{detail}"
    )
    return reason, diagnostic.as_event_fields()


def _iter_candidate_site_stage_payloads(
    candidate_sites: list[str],
    domain_resolution: core.DomainResolution | None,
) -> list[dict[str, object]]:
    candidates_by_url: dict[str, core.DomainCandidate] = {}
    resolution_status = ""
    if domain_resolution:
        resolution_status = core.normalize_whitespace(str(domain_resolution.status or ""))
        for candidate in domain_resolution.candidates or []:
            candidate_url = core.sanitize_website_url(candidate.url)
            if candidate_url and candidate_url not in candidates_by_url:
                candidates_by_url[candidate_url] = candidate

    payloads: list[dict[str, object]] = []
    selection_rank = 0
    for site_url in candidate_sites or []:
        normalized_site_url = core.sanitize_website_url(site_url)
        if not normalized_site_url:
            continue
        selection_rank += 1
        candidate = candidates_by_url.get(normalized_site_url)
        payload: dict[str, object] = {
            "site_url": normalized_site_url,
            "selection_rank": selection_rank,
            "selection_source": (
                core.normalize_whitespace(str(candidate.source or ""))
                if candidate and core.normalize_whitespace(str(candidate.source or ""))
                else "merged_contacts"
            ),
        }
        if resolution_status:
            payload["resolution_status"] = resolution_status
        if candidate:
            candidate_status = core.normalize_whitespace(str(candidate.status or ""))
            if candidate_status:
                payload["candidate_status"] = candidate_status
            payload["confidence"] = round(float(candidate.confidence), 3)
        payloads.append(payload)
    return payloads


def _normalized_site_hosts(site_urls: list[str]) -> tuple[str, ...]:
    seen_hosts: set[str] = set()
    hosts: list[str] = []
    for raw_site_url in site_urls or []:
        normalized_site_url = core.sanitize_website_url(raw_site_url) or core.normalize_whitespace(str(raw_site_url or ""))
        if not normalized_site_url:
            continue
        parsed = urlparse(normalized_site_url)
        host = core.normalize_whitespace(parsed.hostname or "").lower()
        if not host or host in seen_hosts:
            continue
        seen_hosts.add(host)
        hosts.append(host)
    return tuple(sorted(hosts))


def _resolve_prefetched_aggregator_site_hosts(
    *,
    row: core.RowInput,
    source_results: dict[str, core.SourceResult],
) -> tuple[str, ...]:
    merged_contacts = core.merge_contacts(source_results, row)
    domain_resolution = build_domain_resolution(row, source_results, merged_contacts)
    candidate_sites = choose_candidate_sites(row, merged_contacts, domain_resolution)
    return _normalized_site_hosts(candidate_sites)


def _iter_aggregator_site_source_payloads(
    source_results: dict[str, core.SourceResult],
) -> list[dict[str, object]]:
    payloads: list[dict[str, object]] = []
    for source_name in sorted(source_results.keys()):
        source_result = source_results[source_name]
        payload = asdict(source_result)
        payload["source"] = source_result.source or source_name
        payload["status"] = source_result.status
        payloads.append(payload)
    return payloads


def _clone_known_contacts_payload(known_contacts: dict[str, list[str]] | None) -> dict[str, list[str]]:
    normalized_contacts: dict[str, list[str]] = {}
    for raw_key, raw_values in (known_contacts or {}).items():
        normalized_key = core.normalize_whitespace(str(raw_key or ""))
        if not normalized_key:
            continue
        values: list[str] = []
        for item in raw_values or []:
            normalized_value = core.normalize_whitespace(str(item or ""))
            if normalized_value and normalized_value not in values:
                values.append(normalized_value)
        normalized_contacts[normalized_key] = values
    return normalized_contacts


def _clone_json_like(value: object) -> object:
    if isinstance(value, dict):
        return {str(key): _clone_json_like(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_clone_json_like(item) for item in value]
    if isinstance(value, tuple):
        return [_clone_json_like(item) for item in value]
    return value


def _non_negative_float(value: object) -> float | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        normalized = float(value)
    except (TypeError, ValueError):
        return None
    if normalized != normalized:
        return None
    return max(normalized, 0.0)


def _source_budget_pressure_payload(
    *,
    source_name: str,
    duration_seconds: float,
    budget_seconds: float,
    runtime_events: list[dict[str, object]] | tuple[dict[str, object], ...],
) -> dict[str, object]:
    normalized_source_name = core.normalize_whitespace(str(source_name or ""))
    request_event_count = 0
    request_elapsed_seconds = 0.0
    cooldown_seconds = 0.0
    max_since_previous_request_seconds = 0.0
    has_since_previous_request = False

    for event in runtime_events or ():
        if not isinstance(event, dict):
            continue
        event_source = core.normalize_whitespace(str(event.get("source") or ""))
        if normalized_source_name and event_source and event_source != normalized_source_name:
            continue
        event_type = core.normalize_whitespace(str(event.get("type") or ""))
        if event_type not in SOURCE_REQUEST_EVENT_TYPES:
            continue
        request_event_count += 1
        elapsed = _non_negative_float(event.get("elapsed_seconds"))
        if elapsed is not None:
            request_elapsed_seconds += elapsed
        cooldown = _non_negative_float(event.get("cooldown_seconds"))
        if cooldown is not None:
            cooldown_seconds += cooldown
        since_previous = _non_negative_float(event.get("since_previous_request_seconds"))
        if since_previous is not None:
            has_since_previous_request = True
            max_since_previous_request_seconds = max(max_since_previous_request_seconds, since_previous)

    duration = max(float(duration_seconds or 0.0), 0.0)
    budget = max(float(budget_seconds or 0.0), 0.0)
    non_request_wait_seconds = max(duration - request_elapsed_seconds, 0.0)
    if request_event_count <= 0:
        pressure_class = "source_call_duration_unclassified"
    elif cooldown_seconds > 0.0:
        pressure_class = "host_cooldown_or_policy_wait"
    elif budget > 0.0 and request_elapsed_seconds > budget:
        pressure_class = "http_elapsed_over_budget"
    elif budget > 0.0 and (
        non_request_wait_seconds > budget
        or (duration > 0.0 and non_request_wait_seconds / duration >= 0.75)
    ):
        pressure_class = "client_or_scheduler_wait_over_budget"
    else:
        pressure_class = "mixed_source_duration_over_budget"

    payload: dict[str, object] = {
        "budget_seconds": round(budget, 3),
        "budget_pressure_class": pressure_class,
        "request_event_count": request_event_count,
        "request_elapsed_seconds": round(request_elapsed_seconds, 3),
        "non_request_wait_seconds": round(non_request_wait_seconds, 3),
    }
    if cooldown_seconds > 0.0:
        payload["cooldown_seconds"] = round(cooldown_seconds, 3)
    if has_since_previous_request:
        payload["max_since_previous_request_seconds"] = round(max_since_previous_request_seconds, 3)
    return payload


def _clone_source_lane_scheduler_payload(payload: object) -> dict[str, object]:
    return dict(_clone_json_like(payload)) if isinstance(payload, dict) else {}


def _clone_downstream_worker_pool_payload(payload: object) -> dict[str, object]:
    return dict(_clone_json_like(payload)) if isinstance(payload, dict) else {}


def _raise_if_required_source_red_flag(
    *,
    progress: ProgressStore,
    row: core.RowInput,
    source_result: core.SourceResult,
    source_access_mode: str,
    classification_outcome: str = "",
    classification_reason: str = "",
    reason_override: str = "",
) -> None:
    source_name = core.normalize_whitespace(str(source_result.source or ""))
    source_status = core.normalize_whitespace(str(source_result.status or ""))
    if not core.source_result_requires_run_fail_fast(
        source_name,
        source_status,
        access_mode=source_access_mode,
    ):
        return
    detail = core.resolve_source_block_reason(source_result)
    reason = core.normalize_whitespace(reason_override) or core.build_required_source_fail_fast_reason(
        source_name,
        source_status,
        access_mode=source_access_mode,
        detail=detail,
    )
    progress.append_event(
        {
            "ts": core.utc_now_iso(),
            "type": "required_source_red_flag",
            "source": source_name,
            "source_status": source_status,
            "source_access_mode": core.normalize_whitespace(source_access_mode),
            "inn": row.inn,
            "row_index": row.row_index,
            "reason": reason,
            "required_source_outcome": core.normalize_whitespace(classification_outcome)
            or OUTCOME_NON_DEFER_FAIL_FAST,
            "classification_reason": core.normalize_whitespace(classification_reason),
        }
    )
    raise RequiredSourceOperationalError(
        source_name=source_name,
        source_status=source_status,
        source_access_mode=source_access_mode,
        reason=reason,
    )


def _handle_required_source_runtime_outcome(
    *,
    progress: ProgressStore,
    row: core.RowInput,
    source_result: core.SourceResult,
    source_access_mode: str,
    defer_required_source_transients: bool,
    selected_rows: int,
    retry_existing_deferred: bool = False,
) -> bool:
    source_name = core.normalize_whitespace(str(source_result.source or ""))
    source_status = core.normalize_whitespace(str(source_result.status or ""))
    if not core.source_result_requires_run_fail_fast(
        source_name,
        source_status,
        access_mode=source_access_mode,
    ):
        if defer_required_source_transients and source_name in core.CANONICAL_REQUIRED_SOURCE_NAMES:
            progress.mark_deferred_required_source_success(source_name=source_name)
        return False

    detail = core.resolve_source_block_reason(source_result)
    classification = classify_required_source_outcome(
        source=source_name,
        access_mode=source_access_mode,
        status=source_status,
        detail=detail,
        defer_enabled=defer_required_source_transients,
        selected_rows=selected_rows,
        deferred_state=progress.required_source_deferred_state(),
        retry_existing_deferred=retry_existing_deferred,
    )
    if classification.outcome == OUTCOME_DEFERRED_ROW_TRANSIENT:
        now_iso = core.utc_now_iso()
        record = build_required_source_deferred_record(
            source=source_name,
            access_mode=source_access_mode,
            status=source_status,
            error=detail,
            row_index=row.row_index,
            inn=row.inn,
            company_name=row.company_name,
            run_id=str(progress.run_metadata.get("run_id", "") or ""),
            now_iso=now_iso,
        )
        progress.record_deferred_required_source(record)
        progress.append_event(
            {
                "ts": now_iso,
                "type": "required_source_deferred",
                "required_source_outcome": classification.outcome,
                "classification_reason": classification.reason,
                "source": source_name,
                "source_status": source_status,
                "source_access_mode": core.normalize_whitespace(source_access_mode),
                "inn": row.inn,
                "row_index": row.row_index,
                "reason": classification.reason,
                "detail": detail,
            }
        )
        return True

    _raise_if_required_source_red_flag(
        progress=progress,
        row=row,
        source_result=source_result,
        source_access_mode=source_access_mode,
        classification_outcome=classification.outcome,
        classification_reason=classification.reason,
    )
    return False


def _deferred_required_source_final_stop(
    *,
    progress: ProgressStore,
) -> tuple[RequiredSourceOperationalError, dict[str, str]] | None:
    missing_success_sources = progress.deferred_required_sources_without_later_success()
    if not missing_success_sources:
        return None
    source_name = missing_success_sources[0]
    record = first_unresolved_record_for_source(progress.required_source_deferred_state(), source_name) or {}
    source_status = core.normalize_whitespace(str(record.get("status", "") or "unknown_status"))
    source_access_mode = core.normalize_whitespace(str(record.get("access_mode", "") or ""))
    row = core.RowInput(
        row_index=int(record.get("row_index", 0) or 0),
        inn=str(record.get("inn", "") or ""),
        company_name=str(record.get("company_name", "") or ""),
    )
    reason = (
        f"Deferred required source `{source_name}` never proved later success before finalization; "
        f"status=`{source_status}`"
    )
    progress.append_event(
        {
            "ts": core.utc_now_iso(),
            "type": "required_source_red_flag",
            "required_source_outcome": OUTCOME_SYSTEMIC_STOP,
            "classification_reason": "deferred required-source final circuit breaker: missing later success",
            "source": source_name,
            "source_status": source_status,
            "source_access_mode": source_access_mode,
            "inn": row.inn,
            "row_index": row.row_index,
            "reason": reason,
        }
    )
    return (
        RequiredSourceOperationalError(
            source_name=source_name,
            source_status=source_status,
            source_access_mode=source_access_mode,
            reason=reason,
        ),
        _terminal_context(
            checkpoint="required_source_deferred_final_circuit_breaker",
            row=row,
            source_name=source_name,
            source_status=source_status,
            source_access_mode=source_access_mode,
        ),
    )


def _deferred_required_source_retry_plan(
    *,
    unresolved_records: list[dict[str, object]],
    source_order: list[str],
    selected_sources: set[str] | None,
) -> tuple[list[str], dict[str, tuple[str, ...]]]:
    known_sources = set(source_order)
    if selected_sources is not None:
        unknown_selected_sources = selected_sources - known_sources
        if unknown_selected_sources:
            raise ValueError(f"Неизвестные источники: {', '.join(sorted(unknown_selected_sources))}")
    unknown_manifest_sources = sorted(
        {
            source
            for record in unresolved_records
            for source in (core.normalize_whitespace(str(record.get("source") or "")),)
            if source and source not in known_sources
        }
    )
    if unknown_manifest_sources:
        raise ValueError(
            "deferred_required_sources содержит неизвестные источники: "
            + ", ".join(unknown_manifest_sources)
        )

    allowed_sources = selected_sources if selected_sources is not None else known_sources
    source_sets_by_inn: dict[str, set[str]] = {}
    for record in unresolved_records:
        inn = core.normalize_whitespace(str(record.get("inn") or ""))
        source = core.normalize_whitespace(str(record.get("source") or ""))
        if not inn or not source or source not in allowed_sources:
            continue
        source_sets_by_inn.setdefault(inn, set()).add(source)

    sources_by_inn = {
        inn: tuple(source for source in source_order if source in source_set)
        for inn, source_set in source_sets_by_inn.items()
    }
    active_sources = [
        source
        for source in source_order
        if any(source in source_names for source_names in sources_by_inn.values())
    ]
    return active_sources, sources_by_inn


def _mark_deferred_required_source_retry_targets_resolved(
    *,
    progress: ProgressStore,
    row: core.RowInput,
    source_results: dict[str, core.SourceResult],
    source_names: tuple[str, ...],
    logger,
) -> None:
    for source_name in source_names:
        source_result = source_results.get(source_name)
        if source_result is None:
            logger.warning(
                "  deferred required-source retry did not resolve INN=%s source=%s: missing source result",
                row.inn,
                source_name,
            )
            continue
        source_status = core.normalize_whitespace(str(source_result.status or ""))
        if core.source_result_requires_run_fail_fast(source_name, source_status):
            logger.warning(
                "  deferred required-source retry did not resolve INN=%s source=%s status=%s",
                row.inn,
                source_name,
                source_status,
            )
            continue
        if not progress.mark_deferred_required_source_resolved(
            inn=row.inn,
            source_name=source_name,
            source_status=source_status,
            detail="retry promoted completed company result through explicit runtime ack",
        ):
            continue
        progress.append_event(
            {
                "ts": core.utc_now_iso(),
                "type": "required_source_deferred_resolved",
                "source": source_name,
                "source_status": source_status,
                "inn": row.inn,
                "row_index": row.row_index,
                "reason": "retry promoted completed company result through explicit runtime ack",
            }
        )


def _source_search_rows_for_run(
    *,
    rows: list[core.RowInput],
    progress: ProgressStore,
    active_source_names: list[str],
    args,
    resume_pending_explicit_inns: set[str],
) -> list[core.RowInput]:
    if bool(getattr(args, "retry_deferred_required_sources", False)):
        return []
    source_rows: list[core.RowInput] = []
    for row in rows:
        existing_payload = progress.get(row.inn)
        resume_recovery = args.resume and row.inn in resume_pending_explicit_inns
        if (
            args.resume
            and not resume_recovery
            and core.should_skip_on_resume(
                existing_payload,
                active_source_names,
                retry_blocked_source=args.retry_blocked_source,
            )
        ):
            continue
        if resume_recovery:
            continue
        source_rows.append(row)
    return source_rows


def _build_runtime_backpressure_policy(
    *,
    active_source_names: list[str],
    source_lane_scheduler: dict[str, object],
    downstream_worker_pools: dict[str, object],
    direct_default_executor_plan: DirectDefaultBoundedExecutorPlan,
    source_pending_rows: int,
    downstream_prefetch_queue_limit: int = 0,
    downstream_prefetch_ready_drain_limit: int = 0,
    ready_idle_source_wait_drain_seconds: float = 0.0,
    ready_idle_source_wait_drain_max_rows: int = 0,
) -> dict[str, object]:
    direct_default_full_source_contour = (
        direct_default_executor_plan.enabled
        and tuple(direct_default_executor_plan.active_sources) == tuple(active_source_names)
    )
    resolved_downstream_prefetch_queue_limit = max(int(downstream_prefetch_queue_limit or 0), 0)
    ready_queue_limit = (
        max(resolved_downstream_prefetch_queue_limit, 1)
        if direct_default_full_source_contour
        else max(int(direct_default_executor_plan.max_workers or 0), 1)
    )
    downstream_prefetch_enabled = (
        bool(direct_default_executor_plan.enabled)
        and not direct_default_full_source_contour
        and max(int(direct_default_executor_plan.max_workers or 0), 1) > 1
        and resolved_downstream_prefetch_queue_limit > 0
    )
    return {
        "policy_name": "full_source_contour_backpressure_v1",
        "intake_mode": "observed_intake_regulation",
        "source_set": list(active_source_names),
        "drops_sources": False,
        "safe_only_fallback": False,
        "source_pending_rows": max(int(source_pending_rows or 0), 0),
        "effective_company_concurrency_cap": int(
            source_lane_scheduler.get("effective_company_concurrency_cap", 0) or 0
        ),
        "direct_default_prefetch": {
            "enabled": bool(direct_default_executor_plan.enabled),
            "active_sources": list(direct_default_executor_plan.active_sources),
            "full_source_contour": tuple(direct_default_executor_plan.active_sources) == tuple(active_source_names),
            "max_workers": max(int(direct_default_executor_plan.max_workers or 0), 1),
            "ready_queue_limit": ready_queue_limit,
            "reason": "pause source intake when prefetched ready queue reaches the explicit runtime limit",
            "active_workers": 0,
        },
        "downstream_prefetch": {
            "enabled": downstream_prefetch_enabled,
            "pending_queue_limit": (
                resolved_downstream_prefetch_queue_limit
                if downstream_prefetch_enabled
                else 0
            ),
            "ready_drain_limit": (
                max(int(downstream_prefetch_ready_drain_limit or 0), 0)
                if downstream_prefetch_enabled
                else 0
            ),
            "ready_drain_low_watermark": (
                _resolve_downstream_prefetch_ready_drain_low_watermark(
                    ready_drain_limit=downstream_prefetch_ready_drain_limit,
                )
                if downstream_prefetch_enabled
                else 0
            ),
            "ready_idle_source_wait_drain_seconds": (
                max(float(ready_idle_source_wait_drain_seconds or 0.0), 0.0)
                if downstream_prefetch_enabled
                else 0.0
            ),
            "ready_idle_source_wait_drain_max_rows": (
                max(int(ready_idle_source_wait_drain_max_rows or 0), 0)
                if downstream_prefetch_enabled
                else 0
            ),
            "reason": (
                "keep source intake ahead of ordered downstream drain while preserving bounded memory"
            ),
        },
        "source_lane_budget_map": _clone_json_like(source_lane_scheduler.get("per_source_lane_budget_map") or {}),
        "downstream_stage_budget_map": _clone_json_like(
            downstream_worker_pools.get("per_stage_budget_map") or {}
        ),
    }


def _capture_runtime_throughput(
    *,
    progress: ProgressStore,
    source_lane_scheduler: dict[str, object],
    downstream_worker_pools: dict[str, object],
    source_lane_telemetry: SourceLaneTelemetryLedger,
    source_stage_governor: StagePoolGovernor,
    downstream_stage_governor: StagePoolGovernor,
    backpressure_policy: dict[str, object],
    direct_default_host_ledger: HostGovernorLedger | None = None,
) -> dict[str, object]:
    stage_backlog = {
        CANDIDATE_SITE_STAGE_NAME: len(
            progress.pending_stage_work_units(execution_boundary=AGGREGATOR_SITE_EXECUTION_BOUNDARY)
        ),
        DEEP_PARSE_STAGE_NAME: len(
            progress.pending_stage_work_units(execution_boundary=DEEP_PARSE_EXECUTION_BOUNDARY)
        ),
    }
    rows_completed = int(progress.summary.get("processed_rows", 0) or 0)
    return progress.update_throughput_telemetry(
        build_throughput_telemetry_payload(
            source_lane_scheduler=source_lane_scheduler,
            downstream_worker_pools=downstream_worker_pools,
            source_lane_runtime=source_lane_telemetry.snapshot(),
            source_stage_runtime=source_stage_governor.snapshot(),
            downstream_stage_runtime=downstream_stage_governor.snapshot(),
            stage_backlog=stage_backlog,
            host_governor_runtime=(
                direct_default_host_ledger.snapshot() if direct_default_host_ledger is not None else None
            ),
            backpressure_policy=backpressure_policy,
            rows_completed=rows_completed,
        )
    )


def _downstream_worker_pools_from_stage_work_unit(work_unit: dict[str, object]) -> dict[str, object]:
    payload = work_unit.get("work_unit")
    if not isinstance(payload, dict):
        return {}
    return _clone_downstream_worker_pool_payload(payload.get("downstream_worker_pools"))


def _site_decisions_from_payloads(
    decision_payloads: list[dict[str, object]] | tuple[dict[str, object], ...],
) -> list[object]:
    decisions: list[object] = []
    for payload in decision_payloads:
        if not isinstance(payload, dict):
            continue
        decisions.append(core.site_decision_from_dict(payload))
    return decisions


def _build_aggregator_site_work_unit_payload(
    *,
    row: core.RowInput,
    source_results: dict[str, core.SourceResult],
    candidate_site_payloads: list[dict[str, object]],
    updated_source_names: list[str],
    source_lane_scheduler: dict[str, object],
    downstream_worker_pools: dict[str, object],
) -> dict[str, object]:
    return {
        "inn": row.inn,
        "row_index": row.row_index,
        "company_name": row.company_name,
        "source_results": _iter_aggregator_site_source_payloads(source_results),
        "candidate_sites": [dict(payload) for payload in candidate_site_payloads],
        "updated_sources": list(updated_source_names),
        "queue_family_contour": build_aggregator_site_queue_family_contour().as_payload(),
        "source_lane_scheduler": _clone_source_lane_scheduler_payload(source_lane_scheduler),
        "downstream_worker_pools": _clone_downstream_worker_pool_payload(downstream_worker_pools),
    }


def _build_deep_parse_work_unit_payload(
    *,
    row: core.RowInput,
    source_results: dict[str, core.SourceResult],
    candidate_site_payloads: list[dict[str, object]],
    updated_source_names: list[str],
    known_contacts: dict[str, list[str]],
    site_gate_decisions: list[object],
    deep_parse_sites: list[str],
    gate_notes: list[str],
    source_lane_scheduler: dict[str, object],
    downstream_worker_pools: dict[str, object],
) -> dict[str, object]:
    return {
        "inn": row.inn,
        "row_index": row.row_index,
        "company_name": row.company_name,
        "source_results": _iter_aggregator_site_source_payloads(source_results),
        "candidate_sites": [dict(payload) for payload in candidate_site_payloads],
        "updated_sources": list(updated_source_names),
        "queue_family_contour": build_deep_parse_queue_family_contour().as_payload(),
        "known_contacts": {
            str(key): [core.normalize_whitespace(str(item or "")) for item in values or [] if core.normalize_whitespace(str(item or ""))]
            for key, values in (known_contacts or {}).items()
        },
        "site_gate_decisions": [asdict(decision) for decision in site_gate_decisions or []],
        "deep_parse_sites": list(deep_parse_sites),
        "gate_notes": [core.normalize_whitespace(str(item or "")) for item in gate_notes or [] if core.normalize_whitespace(str(item or ""))],
        "source_lane_scheduler": _clone_source_lane_scheduler_payload(source_lane_scheduler),
        "downstream_worker_pools": _clone_downstream_worker_pool_payload(downstream_worker_pools),
    }


def _candidate_sites_from_stage_work_unit(work_unit: dict[str, object]) -> list[str]:
    payload = work_unit.get("work_unit")
    candidate_site_items = payload.get("candidate_sites") if isinstance(payload, dict) else []
    candidate_sites: list[str] = []
    for item in candidate_site_items or []:
        if not isinstance(item, dict):
            continue
        normalized_site_url = core.sanitize_website_url(item.get("site_url"))
        if not normalized_site_url:
            normalized_site_url = core.normalize_whitespace(str(item.get("site_url") or ""))
        if normalized_site_url and normalized_site_url not in candidate_sites:
            candidate_sites.append(normalized_site_url)
    return candidate_sites


def _source_results_from_stage_work_unit(work_unit: dict[str, object]) -> dict[str, core.SourceResult]:
    payload = work_unit.get("work_unit")
    source_payloads = payload.get("source_results") if isinstance(payload, dict) else []
    source_results: dict[str, core.SourceResult] = {}
    for source_payload in source_payloads or []:
        if not isinstance(source_payload, dict):
            continue
        source_result = core.source_result_from_dict(source_payload)
        source_name = core.normalize_whitespace(
            str(source_result.source or source_payload.get("source") or "")
        )
        if not source_name:
            continue
        source_results[source_name] = source_result
    return source_results


def _updated_source_names_from_stage_work_unit(work_unit: dict[str, object]) -> list[str]:
    payload = work_unit.get("work_unit")
    updated_sources_payload = payload.get("updated_sources") if isinstance(payload, dict) else []
    updated_source_names: list[str] = []
    for item in updated_sources_payload or []:
        source_name = core.normalize_whitespace(str(item or ""))
        if source_name and source_name not in updated_source_names:
            updated_source_names.append(source_name)
    if updated_source_names:
        return updated_source_names
    return list(_source_results_from_stage_work_unit(work_unit).keys())


def _known_contacts_from_stage_work_unit(work_unit: dict[str, object]) -> dict[str, list[str]]:
    payload = work_unit.get("work_unit")
    known_contacts_payload = payload.get("known_contacts") if isinstance(payload, dict) else {}
    known_contacts: dict[str, list[str]] = {}
    if not isinstance(known_contacts_payload, dict):
        return known_contacts
    for raw_key, raw_values in known_contacts_payload.items():
        normalized_key = core.normalize_whitespace(str(raw_key or ""))
        if not normalized_key:
            continue
        values: list[str] = []
        for item in raw_values or []:
            normalized_value = core.normalize_whitespace(str(item or ""))
            if normalized_value and normalized_value not in values:
                values.append(normalized_value)
        known_contacts[normalized_key] = values
    return known_contacts


def _site_gate_decisions_from_stage_work_unit(work_unit: dict[str, object]) -> list[object]:
    payload = work_unit.get("work_unit")
    decision_payloads = payload.get("site_gate_decisions") if isinstance(payload, dict) else []
    decisions: list[object] = []
    for item in decision_payloads or []:
        if not isinstance(item, dict):
            continue
        decisions.append(core.site_decision_from_dict(item))
    return decisions


def _deep_parse_sites_from_stage_work_unit(work_unit: dict[str, object]) -> list[str]:
    payload = work_unit.get("work_unit")
    site_payloads = payload.get("deep_parse_sites") if isinstance(payload, dict) else []
    deep_parse_sites: list[str] = []
    for item in site_payloads or []:
        normalized_site = _normalize_deep_parse_site_url(item)
        if normalized_site and normalized_site not in deep_parse_sites:
            deep_parse_sites.append(normalized_site)
    return deep_parse_sites


def _gate_notes_from_stage_work_unit(work_unit: dict[str, object]) -> list[str]:
    payload = work_unit.get("work_unit")
    note_payloads = payload.get("gate_notes") if isinstance(payload, dict) else []
    notes: list[str] = []
    for item in note_payloads or []:
        normalized_note = core.normalize_whitespace(str(item or ""))
        if normalized_note:
            notes.append(normalized_note)
    return notes


def _completed_result_checkpoint_from_stage_work_unit(work_unit: dict[str, object] | None) -> dict[str, object] | None:
    private_state = work_unit.get("private_state") if isinstance(work_unit, dict) else None
    if not isinstance(private_state, dict):
        return None
    completed_result = private_state.get(COMPLETED_RESULT_CHECKPOINT_KEY)
    return dict(completed_result) if isinstance(completed_result, dict) else None


def _completed_result_checkpoint_patch(
    work_unit: dict[str, object],
    *,
    result: core.CompanyResult,
) -> dict[str, object]:
    private_state = (
        dict(work_unit.get("private_state"))
        if isinstance(work_unit.get("private_state"), dict)
        else {}
    )
    private_state[COMPLETED_RESULT_CHECKPOINT_KEY] = core.serialize_company_result(result)
    return private_state


def _completed_result_checkpoint_patch_from_payload(
    work_unit: dict[str, object],
    *,
    result_payload: dict[str, object] | None,
) -> dict[str, object]:
    private_state = (
        dict(work_unit.get("private_state"))
        if isinstance(work_unit.get("private_state"), dict)
        else {}
    )
    if isinstance(result_payload, dict):
        private_state[COMPLETED_RESULT_CHECKPOINT_KEY] = _clone_json_like(result_payload)
    return private_state


def _drop_stale_no_candidate_site_note(result: core.CompanyResult) -> None:
    result.notes = [
        note
        for note in result.notes
        if core.normalize_whitespace(str(note or "")) != NO_CANDIDATE_SITE_NOTE
    ]


def _stage_pool_context(
    stage_governor: StagePoolGovernor | None,
    *,
    stage_name: str,
    shutdown_requested: object = None,
    telemetry_tick: object = None,
    stage_span_recorder: object = None,
):
    pool_context = (
        nullcontext()
        if stage_governor is None
        else stage_governor.lease(
            stage_name,
            cancel_requested=shutdown_requested,
            wait_callback=telemetry_tick,
        )
    )
    if not callable(stage_span_recorder):
        return pool_context
    span_context = stage_span_recorder(stage_name=stage_name)
    return _combined_runtime_context(pool_context, span_context)


@contextmanager
def _combined_runtime_context(*contexts):
    if not contexts:
        yield
        return
    with contexts[0]:
        with _combined_runtime_context(*contexts[1:]):
            yield


def _make_downstream_stage_span_recorder(
    *,
    event_sink,
    row: core.RowInput,
    execution_boundary: str = "",
    handoff_fingerprint: str = "",
):
    def recorder(*, stage_name: str):
        return _downstream_stage_wallclock_span(
            event_sink=event_sink,
            row=row,
            stage_name=stage_name,
            execution_boundary=execution_boundary,
            handoff_fingerprint=handoff_fingerprint,
        )

    return recorder


@contextmanager
def _downstream_stage_wallclock_span(
    *,
    event_sink,
    row: core.RowInput,
    stage_name: str,
    execution_boundary: str = "",
    handoff_fingerprint: str = "",
):
    started_at = core.utc_now_iso()
    started_monotonic = _runtime_monotonic()
    status = "completed"
    error_type = ""
    raised: BaseException | None = None
    try:
        yield
    except BaseException as exc:
        raised = exc
        status = "cancelled" if isinstance(exc, CancelledError) else "failed"
        error_type = exc.__class__.__name__
        raise
    finally:
        finished_at = core.utc_now_iso()
        event: dict[str, object] = {
            "ts": finished_at,
            "type": DOWNSTREAM_STAGE_SPAN_EVENT_TYPE,
            "inn": row.inn,
            "row_index": row.row_index,
            "company_name": row.company_name,
            "stage": stage_name,
            "execution_boundary": execution_boundary,
            "handoff_fingerprint": handoff_fingerprint,
            "started_at": started_at,
            "finished_at": finished_at,
            "elapsed_seconds": round(max(_runtime_monotonic() - started_monotonic, 0.0), 4),
            "status": status,
        }
        if error_type:
            event["error_type"] = error_type
        try:
            event_sink(event)
        except Exception:
            if raised is None:
                raise


def _bind_factory_parser_ocr_context(
    *,
    parser: FactorySiteParser,
    stage_governor: StagePoolGovernor | None,
    shutdown_requested: object = None,
    telemetry_tick: object = None,
    stage_span_recorder: object = None,
) -> None:
    documents_stage = getattr(parser, "documents_stage", None)
    if documents_stage is not None and hasattr(documents_stage, "ocr_execution_context"):
        documents_stage.ocr_execution_context = (
            (
                lambda: _stage_pool_context(
                    stage_governor,
                    stage_name=OCR_STAGE_NAME,
                    shutdown_requested=shutdown_requested,
                    telemetry_tick=telemetry_tick,
                    stage_span_recorder=stage_span_recorder,
                )
            )
            if stage_governor is not None or callable(stage_span_recorder)
            else None
        )


def _build_prefetched_factory_site_parser(
    *,
    http_client: object,
    output_dir: Path,
    stage_governor: StagePoolGovernor | None,
    shutdown_requested: object = None,
    telemetry_tick: object = None,
    stage_span_recorder: object = None,
) -> FactorySiteParser:
    parser = FactorySiteParser(http_client, attachments_root=output_dir / "site_attachments")
    _bind_factory_parser_ocr_context(
        parser=parser,
        stage_governor=stage_governor,
        shutdown_requested=shutdown_requested,
        telemetry_tick=telemetry_tick,
        stage_span_recorder=stage_span_recorder,
    )
    return parser


def _prepare_prefetched_company_downstream_execution(
    *,
    row: core.RowInput,
    source_results: dict[str, core.SourceResult],
    active_source_names: list[str],
    analyzer: BenchmarkAwareSiteAuthenticityAnalyzer,
    factory_site_parser_factory,
    stage_governor: StagePoolGovernor | None = None,
    shutdown_requested: object = None,
    telemetry_tick: object = None,
    stage_span_recorder: object = None,
) -> PrefetchedCompanyDownstreamExecution:
    with _stage_pool_context(
        stage_governor,
        stage_name=CANDIDATE_SITE_STAGE_NAME,
        shutdown_requested=shutdown_requested,
        telemetry_tick=telemetry_tick,
        stage_span_recorder=stage_span_recorder,
    ):
        aggregator_execution = _prepare_prefetched_aggregator_site_execution(
            row=row,
            source_results=source_results,
            analyzer=analyzer,
        )
    result, _ = _build_company_result_for_stage_work(
        row=row,
        existing_payload=None,
        source_results=source_results,
        active_source_names=active_source_names,
        mark_running=False,
    )
    result.candidate_sites = [
        core.sanitize_website_url(payload.get("site_url")) or core.normalize_whitespace(str(payload.get("site_url") or ""))
        for payload in aggregator_execution.candidate_site_payloads
        if isinstance(payload, dict)
        and (
            core.sanitize_website_url(payload.get("site_url"))
            or core.normalize_whitespace(str(payload.get("site_url") or ""))
        )
    ]
    result.notes.extend(list(aggregator_execution.gate_notes))
    gate_decisions = _site_decisions_from_payloads(list(aggregator_execution.site_gate_decision_payloads))
    if not aggregator_execution.deep_parse_sites:
        result.validated_sites = list(gate_decisions)
        result.trusted_contacts = core.build_trusted_contacts(
            row,
            source_results,
            result.merged_contacts,
            result.validated_sites,
        )
        result.lead_cards = core.build_lead_cards(
            row,
            result.domain_resolution,
            result.trusted_contacts,
            result.merged_contacts,
            result.content_records,
        )
        result.site_refresh_plans = core.build_site_refresh_plans(result.candidate_sites, result.site_probes)
        result.finished_at = core.utc_now_iso()
        result.status = "completed"
        _drop_stale_no_candidate_site_note(result)
        return PrefetchedCompanyDownstreamExecution(
            aggregator_execution=aggregator_execution,
            completed_result_payload=core.serialize_company_result(result),
        )

    if callable(shutdown_requested) and shutdown_requested():
        raise CancelledError(f"downstream preparation cancelled before deep_parse for {row.inn}")

    deep_parse_sites = list(aggregator_execution.deep_parse_sites)
    known_contacts = _clone_known_contacts_payload(aggregator_execution.known_contacts)
    deep_parse_site_keys = {
        normalized_key
        for normalized_key in (
            _normalize_deep_parse_site_key(analyzer=analyzer, site_url=site_url)
            for site_url in deep_parse_sites
        )
        if normalized_key
    }
    validated_sites: list[object] = []
    pending_surface: dict[str, object] = {}
    for decision in gate_decisions:
        site_key = _normalize_deep_parse_site_key(
            analyzer=analyzer,
            site_url=getattr(decision, "final_url", "") or getattr(decision, "url", ""),
        )
        if site_key and site_key in deep_parse_site_keys:
            pending_surface[site_key] = decision
            continue
        validated_sites.append(decision)

    with _stage_pool_context(
        stage_governor,
        stage_name=DEEP_PARSE_STAGE_NAME,
        shutdown_requested=shutdown_requested,
        telemetry_tick=telemetry_tick,
        stage_span_recorder=stage_span_recorder,
    ):
        parser_company = FactorySiteParserCompany.from_row(
            row,
            candidate_sites=deep_parse_sites,
            source_results=source_results,
        )
        with _stage_pool_context(
            stage_governor,
            stage_name=FACTORY_SITE_STAGE_NAME,
            shutdown_requested=shutdown_requested,
            telemetry_tick=telemetry_tick,
            stage_span_recorder=stage_span_recorder,
        ):
            parser = factory_site_parser_factory(
                shutdown_requested=shutdown_requested,
                stage_span_recorder=stage_span_recorder,
            )
            parsed_factory_sites = parser.parse(parser_company)
        result.site_probes = parsed_factory_sites.site_probes
        result.route_strategies = parsed_factory_sites.route_strategies
        result.content_records = parsed_factory_sites.content_records
        result.notes.extend(parsed_factory_sites.notes)

        with _stage_pool_context(
            stage_governor,
            stage_name=EXTRA_CHECK_STAGE_NAME,
            shutdown_requested=shutdown_requested,
            telemetry_tick=telemetry_tick,
            stage_span_recorder=stage_span_recorder,
        ):
            for site_plan in parsed_factory_sites.plans:
                site_key = _normalize_deep_parse_site_key(
                    analyzer=analyzer,
                    site_url=getattr(site_plan, "site_url", ""),
                )
                surface_decision = pending_surface.pop(site_key, None)
                if not getattr(site_plan, "allows_deep_check", False):
                    if surface_decision is not None:
                        _append_site_decision_reason(
                            surface_decision,
                            "planner/probe blocked deep parse after cheap trust gate",
                        )
                        validated_sites.append(surface_decision)
                    continue
                validated_sites.append(
                    analyzer.analyze(
                        row,
                        getattr(site_plan, "site_url", ""),
                        known_contacts,
                        source_results,
                    )
                )

    for surface_decision in pending_surface.values():
        _append_site_decision_reason(
            surface_decision,
            "planner returned no deep-check site plan after cheap trust gate",
        )
        validated_sites.append(surface_decision)

    result.validated_sites = validated_sites
    result.trusted_contacts = core.build_trusted_contacts(
        row,
        source_results,
        result.merged_contacts,
        result.validated_sites,
    )
    primary_site = result.domain_resolution.selected_primary_domain if result.domain_resolution else ""
    with _stage_pool_context(
        stage_governor,
        stage_name=LLM_STAGE_NAME,
        shutdown_requested=shutdown_requested,
        telemetry_tick=telemetry_tick,
        stage_span_recorder=stage_span_recorder,
    ):
        for record in result.content_records:
            classify_content_record(record)
            if should_use_llm_record_review(record):
                llm_result = analyzer.llm.judge_content_record(
                    row,
                    record,
                    primary_site,
                )
                if llm_result:
                    record.llm_result = llm_result
                    llm_confidence = float(llm_result.get("confidence", 0.0) or 0.0)
                    if llm_confidence >= 0.6:
                        record.relevance_label = str(llm_result.get("relevance_label", record.relevance_label))
                        record.relevance_score = max(record.relevance_score, llm_confidence)
                        summary = core.normalize_whitespace(str(llm_result.get("summary", "")))
                        if summary:
                            record.relevance_reasons = core.dedupe_preserve_order(record.relevance_reasons + [summary])[:8]
            elif analyzer.llm.should_force_benchmark_stage("content_review"):
                analyzer.llm.capture_forced_content_review_fixture(
                    row=row,
                    record=record,
                    primary_site=primary_site,
                    prod_skip_reason=describe_content_review_prod_skip_reason(record),
                )

    result.lead_cards = core.build_lead_cards(
        row,
        result.domain_resolution,
        result.trusted_contacts,
        result.merged_contacts,
        result.content_records,
    )
    result.site_refresh_plans = core.build_site_refresh_plans(result.candidate_sites, result.site_probes)
    result.finished_at = core.utc_now_iso()
    result.status = "completed"
    _drop_stale_no_candidate_site_note(result)
    return PrefetchedCompanyDownstreamExecution(
        aggregator_execution=aggregator_execution,
        completed_result_payload=core.serialize_company_result(result),
    )


def _prepare_prefetched_aggregator_site_execution(
    *,
    row: core.RowInput,
    source_results: dict[str, core.SourceResult],
    analyzer: BenchmarkAwareSiteAuthenticityAnalyzer,
) -> PrefetchedAggregatorSiteExecution:
    analysis_contacts = core.build_analysis_contacts(source_results, row)
    merged_contacts = core.merge_contacts(source_results, row)
    domain_resolution = build_domain_resolution(row, source_results, merged_contacts)
    candidate_site_payloads = _iter_candidate_site_stage_payloads(
        choose_candidate_sites(row, merged_contacts, domain_resolution),
        domain_resolution,
    )
    gate_result = gate_candidate_sites_before_deep_parse(
        row=row,
        candidate_sites=[payload["site_url"] for payload in candidate_site_payloads if payload.get("site_url")],
        known_contacts=analysis_contacts,
        source_results=source_results,
        analyzer=analyzer,
    )
    gate_validated_sites = list(gate_result.surface_only_decisions)
    gate_validated_sites.extend(list(gate_result.trusted_surface_decisions_by_site.values()))
    deep_parse_sites: list[str] = []
    for site_url in gate_result.deep_parse_sites:
        normalized_site = _normalize_deep_parse_site_url(site_url)
        if normalized_site and normalized_site not in deep_parse_sites:
            deep_parse_sites.append(normalized_site)
    gate_notes = [
        normalized_note
        for normalized_note in (
            core.normalize_whitespace(str(item or ""))
            for item in gate_result.notes or []
        )
        if normalized_note
    ]
    geo_skip_note = _geo_deep_parse_admission_note(
        source_results=source_results,
        row=row,
        merged_contacts=merged_contacts,
    )
    if geo_skip_note:
        gate_notes.append(geo_skip_note)
        deep_parse_sites = []
    return PrefetchedAggregatorSiteExecution(
        candidate_site_payloads=tuple(dict(payload) for payload in candidate_site_payloads),
        site_gate_decision_payloads=tuple(asdict(decision) for decision in gate_validated_sites),
        deep_parse_sites=tuple(deep_parse_sites),
        gate_notes=tuple(gate_notes),
        known_contacts=_clone_known_contacts_payload(analysis_contacts),
    )


def _pending_selected_explicit_stage_work_units(
    *,
    progress: ProgressStore,
    rows: list[core.RowInput],
) -> dict[str, dict[str, object]]:
    selected_inns = {
        core.normalize_whitespace(str(row.inn or ""))
        for row in rows
        if core.normalize_whitespace(str(row.inn or ""))
    }
    pending_work_units: dict[str, dict[str, object]] = {}
    for execution_boundary in (AGGREGATOR_SITE_EXECUTION_BOUNDARY, DEEP_PARSE_EXECUTION_BOUNDARY):
        for work_unit in progress.consume_pending_stage_work_units(
            execution_boundary=execution_boundary,
            inns=sorted(selected_inns),
        ):
            inn = core.normalize_whitespace(str(work_unit.get("inn") or ""))
            if not inn or inn not in selected_inns:
                continue
            pending_work_units[inn] = work_unit
    return pending_work_units


def _build_company_result_for_stage_work(
    *,
    row: core.RowInput,
    existing_payload: dict[str, object] | None,
    source_results: dict[str, core.SourceResult],
    active_source_names: list[str],
    mark_running: bool,
) -> tuple[core.CompanyResult, object]:
    if existing_payload:
        result = core.company_result_from_dict(existing_payload)
        _drop_stale_no_candidate_site_note(result)
        result.input_site = row.xlsx_site
        result.input_phone = row.xlsx_phone
        result.input_comment = row.comment
        if mark_running:
            result.status = "running"
        result.notes.append(f"Результат обновлен {core.utc_now_iso()} для источников: {', '.join(active_source_names)}")
    else:
        result = core.build_company_result(row)
    _drop_stale_no_candidate_site_note(result)
    result.sources = source_results
    analysis_contacts = core.build_analysis_contacts(source_results, row)
    result.merged_contacts = core.merge_contacts(source_results, row)
    result.domain_resolution = build_domain_resolution(row, source_results, result.merged_contacts)
    return result, analysis_contacts


def _consume_aggregator_site_work_unit(
    *,
    progress: ProgressStore,
    row: core.RowInput,
    existing_payload: dict[str, object] | None,
    aggregator_site_work_unit: dict[str, object],
    resume_recovery: bool,
    active_source_names: list[str],
    analyzer: BenchmarkAwareSiteAuthenticityAnalyzer,
    factory_site_parser: FactorySiteParser,
    total_rows: int,
    processed_rows: int,
    logger,
) -> int:
    next_processed_rows, _ = _consume_aggregator_site_work_unit_v2(
        progress=progress,
        row=row,
        existing_payload=existing_payload,
        aggregator_site_work_unit=aggregator_site_work_unit,
        resume_recovery=resume_recovery,
        active_source_names=active_source_names,
        analyzer=analyzer,
        total_rows=total_rows,
        processed_rows=processed_rows,
        logger=logger,
    )
    return next_processed_rows

    source_results = _source_results_from_stage_work_unit(aggregator_site_work_unit)
    if not source_results:
        raise RuntimeError(f"Missing explicit source_results for runtime queue work unit INN {row.inn}")
    updated_source_names = _updated_source_names_from_stage_work_unit(aggregator_site_work_unit)
    result, analysis_contacts = _build_company_result_for_stage_work(
        row=row,
        existing_payload=existing_payload,
        source_results=source_results,
        active_source_names=active_source_names,
        mark_running=not resume_recovery,
    )
    result.candidate_sites = _candidate_sites_from_stage_work_unit(aggregator_site_work_unit)
    logger.info(
        "  aggregator_site_queue consumer fingerprint=%s status=%s candidate_sites=%s",
        aggregator_site_work_unit.get("handoff_fingerprint", ""),
        aggregator_site_work_unit.get("work_status", ""),
        len(result.candidate_sites),
    )
    handoff_company = progress.stage_handoff_company(row.inn) if resume_recovery else None
    resume_has_deep_parse_done = _handoff_has_stage_payload(handoff_company, "deep_parse_done")
    resume_has_company_completed = _handoff_has_stage_payload(handoff_company, "company_completed")
    completed_result_checkpoint = (
        _completed_result_checkpoint_from_stage_work_unit(aggregator_site_work_unit)
        if resume_recovery
        else None
    )
    if (
        resume_recovery
        and existing_payload
        and str(result.status or "") == "completed"
        and resume_has_deep_parse_done
    ):
        if not resume_has_company_completed:
            progress.emit_stage_message(
                message_type="company_completed",
                stage="finalize_company",
                inn=result.inn,
                row_index=result.row_index,
                payload={
                    "status": result.status,
                    "updated_sources": updated_source_names,
                },
            )
            progress.materialize_unread_stage_handoffs()
        progress.sync_stage_handoffs_to_work_units()
        if not progress.ack_stage_handoff_work_unit(
            inn=result.inn,
            handoff_fingerprint=str(aggregator_site_work_unit.get("handoff_fingerprint") or ""),
        ):
            raise RuntimeError(f"Failed to ack aggregator/site work unit for INN {result.inn}")
        next_processed_rows = processed_rows + 1
        progress.mark_existing_result_processed(
            total_rows=total_rows,
            processed_rows=next_processed_rows,
        )
        logger.info(
            "  aggregator_site_queue resume acked persisted explicit work unit: candidate_sites=%s, validated=%s",
            len(result.candidate_sites),
            len([site for site in result.validated_sites if site.belongs_to_company]),
        )
        return next_processed_rows
    if (
        resume_recovery
        and str(result.status or "") != "completed"
        and resume_has_deep_parse_done
        and completed_result_checkpoint is not None
    ):
        result = core.company_result_from_dict(completed_result_checkpoint)
        _drop_stale_no_candidate_site_note(result)
        result.input_site = row.xlsx_site
        result.input_phone = row.xlsx_phone
        result.input_comment = row.comment
        next_processed_rows = processed_rows + 1
        progress.persist_completed_company_result(
            result,
            total_rows=total_rows,
            processed_rows=next_processed_rows,
            dossier_builder=build_and_store_company_dossier,
        )
        if not resume_has_company_completed:
            progress.emit_stage_message(
                message_type="company_completed",
                stage="finalize_company",
                inn=result.inn,
                row_index=result.row_index,
                payload={
                    "status": result.status,
                    "updated_sources": updated_source_names,
                },
            )
            progress.materialize_unread_stage_handoffs()
        progress.sync_stage_handoffs_to_work_units()
        if not progress.ack_stage_handoff_work_unit(
            inn=result.inn,
            handoff_fingerprint=str(aggregator_site_work_unit.get("handoff_fingerprint") or ""),
        ):
            raise RuntimeError(f"Failed to ack aggregator/site work unit for INN {result.inn}")
        logger.info(
            "  aggregator_site_queue resume restored completed result from explicit checkpoint: candidate_sites=%s, validated=%s",
            len(result.candidate_sites),
            len([site for site in result.validated_sites if site.belongs_to_company]),
        )
        return next_processed_rows
    core.refresh_company_result_profile(result)
    result.site_probes = []
    result.route_strategies = []
    result.content_records = []
    gated_parse = run_gated_factory_site_parse(
        row=row,
        candidate_sites=result.candidate_sites,
        known_contacts=analysis_contacts,
        source_results=source_results,
        analyzer=analyzer,
        factory_site_parser=factory_site_parser,
    )
    parsed_factory_sites = gated_parse.parsed_factory_sites
    result.site_probes = parsed_factory_sites.site_probes
    result.route_strategies = parsed_factory_sites.route_strategies
    result.content_records = parsed_factory_sites.content_records
    result.validated_sites = gated_parse.validated_sites
    result.notes.extend(gated_parse.notes)
    result.notes.extend(parsed_factory_sites.notes)
    existing_site_gate_urls = _existing_site_gate_decision_urls(handoff_company)
    for payload in _iter_site_gate_stage_payloads(result.validated_sites):
        site_url = core.normalize_whitespace(str(payload.get("site_url") or ""))
        if site_url and site_url in existing_site_gate_urls:
            continue
        progress.emit_stage_message(
            message_type="site_gate_decision",
            stage="site_gate",
            inn=row.inn,
            row_index=row.row_index,
            payload=payload,
        )
    progress.materialize_unread_stage_handoffs()

    result.trusted_contacts = core.build_trusted_contacts(row, source_results, result.merged_contacts, result.validated_sites)
    primary_site = result.domain_resolution.selected_primary_domain if result.domain_resolution else ""
    for record in result.content_records:
        classify_content_record(record)
        if should_use_llm_record_review(record):
            llm_result = analyzer.llm.judge_content_record(
                row,
                record,
                primary_site,
            )
            if llm_result:
                record.llm_result = llm_result
                llm_confidence = float(llm_result.get("confidence", 0.0) or 0.0)
                if llm_confidence >= 0.6:
                    record.relevance_label = str(llm_result.get("relevance_label", record.relevance_label))
                    record.relevance_score = max(record.relevance_score, llm_confidence)
                    summary = core.normalize_whitespace(str(llm_result.get("summary", "")))
                    if summary:
                        record.relevance_reasons = core.dedupe_preserve_order(record.relevance_reasons + [summary])[:8]
        elif analyzer.llm.should_force_benchmark_stage("content_review"):
            analyzer.llm.capture_forced_content_review_fixture(
                row=row,
                record=record,
                primary_site=primary_site,
                prod_skip_reason=describe_content_review_prod_skip_reason(record),
            )

    result.lead_cards = core.build_lead_cards(
        row,
        result.domain_resolution,
        result.trusted_contacts,
        result.merged_contacts,
        result.content_records,
    )
    result.site_refresh_plans = core.build_site_refresh_plans(result.candidate_sites, result.site_probes)
    result.finished_at = core.utc_now_iso()
    result.status = "completed"
    _drop_stale_no_candidate_site_note(result)
    aggregator_site_work_unit = progress.merge_stage_work_unit_private_state(
        inn=result.inn,
        handoff_fingerprint=str(aggregator_site_work_unit.get("handoff_fingerprint") or ""),
        private_state_patch=_completed_result_checkpoint_patch(aggregator_site_work_unit, result=result),
    )
    if not aggregator_site_work_unit:
        raise RuntimeError(f"Failed to checkpoint aggregator/site work unit for INN {result.inn}")
    if not resume_has_deep_parse_done:
        progress.emit_stage_message(
            message_type="deep_parse_done",
            stage="deep_site_parse",
            inn=result.inn,
            row_index=result.row_index,
            payload=_build_deep_parse_stage_payload(
                gated_parse=gated_parse,
                candidate_sites=result.candidate_sites,
                validated_sites=result.validated_sites,
                site_probes=result.site_probes,
                route_strategies=result.route_strategies,
                content_records=result.content_records,
                lead_cards=result.lead_cards,
            ),
        )
        progress.materialize_unread_stage_handoffs()
    next_processed_rows = processed_rows + 1
    progress.persist_completed_company_result(
        result,
        total_rows=total_rows,
        processed_rows=next_processed_rows,
        dossier_builder=build_and_store_company_dossier,
    )
    if not resume_has_company_completed:
        progress.emit_stage_message(
            message_type="company_completed",
            stage="finalize_company",
            inn=result.inn,
            row_index=result.row_index,
            payload={
                "status": result.status,
                "updated_sources": updated_source_names,
            },
        )
        progress.materialize_unread_stage_handoffs()
    progress.sync_stage_handoffs_to_work_units()
    if not progress.ack_stage_handoff_work_unit(
        inn=result.inn,
        handoff_fingerprint=str(aggregator_site_work_unit.get("handoff_fingerprint") or ""),
    ):
        raise RuntimeError(f"Failed to ack aggregator/site work unit for INN {result.inn}")
    logger.info(
        "  aggregator_site_queue consumer acked result: candidate_sites=%s, validated=%s",
        len(result.candidate_sites),
        len([site for site in result.validated_sites if site.belongs_to_company]),
    )
    return next_processed_rows


def _emit_company_completed_if_needed(
    *,
    progress: ProgressStore,
    result: core.CompanyResult,
    updated_source_names: list[str],
    already_emitted: bool,
) -> None:
    if already_emitted:
        return
    progress.emit_stage_message(
        message_type="company_completed",
        stage="finalize_company",
        inn=result.inn,
        row_index=result.row_index,
        payload={
            "status": result.status,
            "updated_sources": updated_source_names,
        },
    )
    progress.materialize_unread_stage_handoffs()


def _normalize_deep_parse_site_key(*, analyzer: BenchmarkAwareSiteAuthenticityAnalyzer, site_url: object) -> str:
    normalize_url = getattr(getattr(analyzer, "h", None), "normalize_url", None)
    if callable(normalize_url):
        normalized = normalize_url(site_url)
        if normalized:
            return str(normalized)
    return _normalize_deep_parse_site_url(site_url) or _normalize_optional_stage_text(site_url)


def _append_site_decision_reason(decision: object, reason: str) -> None:
    reasons = getattr(decision, "reasons", None)
    if isinstance(reasons, list):
        if reason not in reasons:
            reasons.append(reason)
        return
    setattr(decision, "reasons", [reason])


def _consume_aggregator_site_work_unit_v2(
    *,
    progress: ProgressStore,
    row: core.RowInput,
    existing_payload: dict[str, object] | None,
    aggregator_site_work_unit: dict[str, object],
    resume_recovery: bool,
    active_source_names: list[str],
    analyzer: BenchmarkAwareSiteAuthenticityAnalyzer,
    total_rows: int,
    processed_rows: int,
    logger,
    prefetched_aggregator_execution: PrefetchedAggregatorSiteExecution | None = None,
    prefetched_completed_result_payload: dict[str, object] | None = None,
    prefetched_runtime_events: tuple[dict[str, object], ...] = (),
    downstream_stage_governor: StagePoolGovernor | None = None,
    telemetry_tick: object = None,
    finalization_timing: dict[str, object] | None = None,
) -> tuple[int, bool]:
    source_results = _source_results_from_stage_work_unit(aggregator_site_work_unit)
    if not source_results:
        raise RuntimeError(f"Missing explicit source_results for runtime queue work unit INN {row.inn}")
    updated_source_names = _updated_source_names_from_stage_work_unit(aggregator_site_work_unit)
    result, analysis_contacts = _build_company_result_for_stage_work(
        row=row,
        existing_payload=existing_payload,
        source_results=source_results,
        active_source_names=active_source_names,
        mark_running=not resume_recovery,
    )
    result.candidate_sites = _candidate_sites_from_stage_work_unit(aggregator_site_work_unit)
    logger.info(
        "  aggregator_site_queue consumer fingerprint=%s status=%s candidate_sites=%s",
        aggregator_site_work_unit.get("handoff_fingerprint", ""),
        aggregator_site_work_unit.get("work_status", ""),
        len(result.candidate_sites),
    )
    handoff_company = progress.stage_handoff_company(row.inn) if resume_recovery else None
    resume_has_deep_parse_done = _handoff_has_stage_payload(handoff_company, "deep_parse_done")
    resume_has_company_completed = _handoff_has_stage_payload(handoff_company, "company_completed")
    completed_result_checkpoint = (
        _completed_result_checkpoint_from_stage_work_unit(aggregator_site_work_unit)
        if resume_recovery
        else None
    )
    if (
        resume_recovery
        and existing_payload
        and str(result.status or "") == "completed"
        and resume_has_deep_parse_done
    ):
        _emit_company_completed_if_needed(
            progress=progress,
            result=result,
            updated_source_names=updated_source_names,
            already_emitted=resume_has_company_completed,
        )
        progress.sync_stage_handoffs_to_work_units()
        if not progress.ack_stage_handoff_work_unit(
            inn=result.inn,
            handoff_fingerprint=str(aggregator_site_work_unit.get("handoff_fingerprint") or ""),
        ):
            raise RuntimeError(f"Failed to ack aggregator/site work unit for INN {result.inn}")
        next_processed_rows = processed_rows + 1
        progress.mark_existing_result_processed(
            total_rows=total_rows,
            processed_rows=next_processed_rows,
        )
        logger.info(
            "  aggregator_site_queue resume acked persisted explicit work unit: candidate_sites=%s validated=%s",
            len(result.candidate_sites),
            len([site for site in result.validated_sites if site.belongs_to_company]),
        )
        return next_processed_rows, True
    if (
        resume_recovery
        and str(result.status or "") != "completed"
        and resume_has_deep_parse_done
        and completed_result_checkpoint is not None
    ):
        result = core.company_result_from_dict(completed_result_checkpoint)
        _drop_stale_no_candidate_site_note(result)
        result.input_site = row.xlsx_site
        result.input_phone = row.xlsx_phone
        result.input_comment = row.comment
        next_processed_rows = processed_rows + 1
        _emit_company_completed_if_needed(
            progress=progress,
            result=result,
            updated_source_names=updated_source_names,
            already_emitted=resume_has_company_completed,
        )
        progress.sync_stage_handoffs_to_work_units()
        if not progress.ack_stage_handoff_work_unit(
            inn=result.inn,
            handoff_fingerprint=str(aggregator_site_work_unit.get("handoff_fingerprint") or ""),
        ):
            raise RuntimeError(f"Failed to ack aggregator/site work unit for INN {result.inn}")
        _persist_completed_company_result_with_finalization_timing(
            progress=progress,
            result=result,
            total_rows=total_rows,
            processed_rows=next_processed_rows,
            work_unit=aggregator_site_work_unit,
            finalization_timing=finalization_timing,
        )
        logger.info(
            "  aggregator_site_queue resume restored completed result from explicit checkpoint: candidate_sites=%s validated=%s",
            len(result.candidate_sites),
            len([site for site in result.validated_sites if site.belongs_to_company]),
        )
        return next_processed_rows, True

    core.refresh_company_result_profile(result)
    result.site_probes = []
    result.route_strategies = []
    result.content_records = []
    stage_span_recorder = _make_downstream_stage_span_recorder(
        event_sink=progress.append_event,
        row=row,
        execution_boundary=AGGREGATOR_SITE_EXECUTION_BOUNDARY,
        handoff_fingerprint=str(aggregator_site_work_unit.get("handoff_fingerprint") or ""),
    )
    if prefetched_aggregator_execution is not None and not resume_recovery:
        for runtime_event in prefetched_runtime_events:
            progress.append_event(dict(runtime_event))
        gate_validated_sites = _site_decisions_from_payloads(
            list(prefetched_aggregator_execution.site_gate_decision_payloads)
        )
        gate_notes = list(prefetched_aggregator_execution.gate_notes)
        deep_parse_sites = list(prefetched_aggregator_execution.deep_parse_sites)
        known_contacts = _clone_known_contacts_payload(prefetched_aggregator_execution.known_contacts)
    else:
        with _stage_pool_context(
            downstream_stage_governor,
            stage_name=CANDIDATE_SITE_STAGE_NAME,
            telemetry_tick=telemetry_tick,
            stage_span_recorder=stage_span_recorder,
        ):
            gate_result = gate_candidate_sites_before_deep_parse(
                row=row,
                candidate_sites=result.candidate_sites,
                known_contacts=analysis_contacts,
                source_results=source_results,
                analyzer=analyzer,
            )
        gate_validated_sites = list(gate_result.surface_only_decisions)
        gate_validated_sites.extend(list(gate_result.trusted_surface_decisions_by_site.values()))
        gate_notes = [
            normalized_note
            for normalized_note in (
                core.normalize_whitespace(str(item or ""))
                for item in gate_result.notes or []
            )
            if normalized_note
        ]
        deep_parse_sites = [
            normalized_site
            for normalized_site in (
                _normalize_deep_parse_site_url(site_url)
                for site_url in gate_result.deep_parse_sites
            )
            if normalized_site
        ]
        geo_skip_note = _geo_deep_parse_admission_note(
            source_results=source_results,
            row=row,
            merged_contacts=result.merged_contacts,
        )
        if geo_skip_note:
            gate_notes.append(geo_skip_note)
            deep_parse_sites = []
        known_contacts = analysis_contacts
    result.validated_sites = list(gate_validated_sites)
    result.notes.extend(gate_notes)

    existing_site_gate_urls = _existing_site_gate_decision_urls(handoff_company)
    for payload in _iter_site_gate_stage_payloads(gate_validated_sites):
        site_url = core.normalize_whitespace(str(payload.get("site_url") or ""))
        if site_url and site_url in existing_site_gate_urls:
            continue
        progress.emit_stage_message(
            message_type="site_gate_decision",
            stage="site_gate",
            inn=row.inn,
            row_index=row.row_index,
            payload=payload,
        )
    progress.materialize_unread_stage_handoffs()

    if deep_parse_sites:
        deep_parse_work_unit = progress.materialize_stage_work_unit(
            inn=row.inn,
            row_index=row.row_index,
            execution_boundary=DEEP_PARSE_EXECUTION_BOUNDARY,
            work_unit_payload=_build_deep_parse_work_unit_payload(
                row=row,
                source_results=source_results,
                candidate_site_payloads=[
                    dict(payload)
                    for payload in (aggregator_site_work_unit.get("work_unit") or {}).get("candidate_sites", [])
                    if isinstance(payload, dict)
                ],
                updated_source_names=updated_source_names,
                known_contacts=known_contacts,
                site_gate_decisions=gate_validated_sites,
                deep_parse_sites=deep_parse_sites,
                gate_notes=gate_notes,
                source_lane_scheduler=_clone_source_lane_scheduler_payload(
                    (aggregator_site_work_unit.get("work_unit") or {}).get("source_lane_scheduler")
                ),
                downstream_worker_pools=_downstream_worker_pools_from_stage_work_unit(
                    aggregator_site_work_unit
                ),
            ),
        )
        if prefetched_completed_result_payload is not None and not resume_recovery:
            deep_parse_work_unit = progress.merge_stage_work_unit_private_state(
                inn=row.inn,
                handoff_fingerprint=str(deep_parse_work_unit.get("handoff_fingerprint") or ""),
                private_state_patch=_completed_result_checkpoint_patch_from_payload(
                    deep_parse_work_unit,
                    result_payload=prefetched_completed_result_payload,
                ),
            )
            if not deep_parse_work_unit:
                raise RuntimeError(f"Failed to checkpoint prefetched deep_parse work unit for INN {row.inn}")
        logger.info(
            "  deep_parse_queue materialized fingerprint=%s status=%s sites=%s",
            deep_parse_work_unit.get("handoff_fingerprint", ""),
            deep_parse_work_unit.get("work_status", ""),
            len(_deep_parse_sites_from_stage_work_unit(deep_parse_work_unit)),
        )
        if not progress.ack_stage_handoff_work_unit(
            inn=row.inn,
            handoff_fingerprint=str(aggregator_site_work_unit.get("handoff_fingerprint") or ""),
        ):
            raise RuntimeError(f"Failed to ack aggregator/site work unit after deep_parse handoff for INN {row.inn}")
        return processed_rows, False

    if prefetched_completed_result_payload is not None and not resume_recovery:
        result = core.company_result_from_dict(prefetched_completed_result_payload)
        _drop_stale_no_candidate_site_note(result)
        result.input_site = row.xlsx_site
        result.input_phone = row.xlsx_phone
        result.input_comment = row.comment
    else:
        result.trusted_contacts = core.build_trusted_contacts(
            row,
            source_results,
            result.merged_contacts,
            result.validated_sites,
        )
        result.lead_cards = core.build_lead_cards(
            row,
            result.domain_resolution,
            result.trusted_contacts,
            result.merged_contacts,
            result.content_records,
        )
        result.site_refresh_plans = core.build_site_refresh_plans(result.candidate_sites, result.site_probes)
        result.finished_at = core.utc_now_iso()
        result.status = "completed"
        _drop_stale_no_candidate_site_note(result)
    aggregator_site_work_unit = progress.merge_stage_work_unit_private_state(
        inn=result.inn,
        handoff_fingerprint=str(aggregator_site_work_unit.get("handoff_fingerprint") or ""),
        private_state_patch=(
            _completed_result_checkpoint_patch_from_payload(
                aggregator_site_work_unit,
                result_payload=prefetched_completed_result_payload,
            )
            if prefetched_completed_result_payload is not None and not resume_recovery
            else _completed_result_checkpoint_patch(aggregator_site_work_unit, result=result)
        ),
    )
    if not aggregator_site_work_unit:
        raise RuntimeError(f"Failed to checkpoint aggregator/site work unit for INN {result.inn}")
    if not resume_has_deep_parse_done:
        progress.emit_stage_message(
            message_type="deep_parse_done",
            stage="deep_site_parse",
            inn=result.inn,
            row_index=result.row_index,
            payload=_build_deep_parse_stage_payload(
                candidate_sites=result.candidate_sites,
                validated_sites=result.validated_sites,
                site_probes=result.site_probes,
                route_strategies=result.route_strategies,
                content_records=result.content_records,
                lead_cards=result.lead_cards,
            ),
        )
        progress.materialize_unread_stage_handoffs()
    next_processed_rows = processed_rows + 1
    _emit_company_completed_if_needed(
        progress=progress,
        result=result,
        updated_source_names=updated_source_names,
        already_emitted=resume_has_company_completed,
    )
    progress.sync_stage_handoffs_to_work_units()
    if not progress.ack_stage_handoff_work_unit(
        inn=result.inn,
        handoff_fingerprint=str(aggregator_site_work_unit.get("handoff_fingerprint") or ""),
    ):
        raise RuntimeError(f"Failed to ack aggregator/site work unit for INN {result.inn}")
    _persist_completed_company_result_with_finalization_timing(
        progress=progress,
        result=result,
        total_rows=total_rows,
        processed_rows=next_processed_rows,
        work_unit=aggregator_site_work_unit,
        finalization_timing=finalization_timing,
    )
    logger.info(
        "  aggregator_site_queue consumer acked result without deep_parse queue: candidate_sites=%s validated=%s",
        len(result.candidate_sites),
        len([site for site in result.validated_sites if site.belongs_to_company]),
    )
    return next_processed_rows, True


def _consume_deep_parse_work_unit(
    *,
    progress: ProgressStore,
    row: core.RowInput,
    existing_payload: dict[str, object] | None,
    deep_parse_work_unit: dict[str, object],
    resume_recovery: bool,
    active_source_names: list[str],
    analyzer: BenchmarkAwareSiteAuthenticityAnalyzer,
    factory_site_parser: FactorySiteParser,
    downstream_stage_governor: StagePoolGovernor | None = None,
    total_rows: int,
    processed_rows: int,
    logger,
    telemetry_tick: object = None,
    finalization_timing: dict[str, object] | None = None,
) -> int:
    source_results = _source_results_from_stage_work_unit(deep_parse_work_unit)
    if not source_results:
        raise RuntimeError(f"Missing explicit source_results for deep_parse work unit INN {row.inn}")
    updated_source_names = _updated_source_names_from_stage_work_unit(deep_parse_work_unit)
    result, _ = _build_company_result_for_stage_work(
        row=row,
        existing_payload=existing_payload,
        source_results=source_results,
        active_source_names=active_source_names,
        mark_running=not resume_recovery,
    )
    result.candidate_sites = _candidate_sites_from_stage_work_unit(deep_parse_work_unit)
    logger.info(
        "  deep_parse_queue consumer fingerprint=%s status=%s candidate_sites=%s",
        deep_parse_work_unit.get("handoff_fingerprint", ""),
        deep_parse_work_unit.get("work_status", ""),
        len(result.candidate_sites),
    )
    handoff_company = progress.stage_handoff_company(row.inn) if resume_recovery else None
    resume_has_deep_parse_done = _handoff_has_stage_payload(handoff_company, "deep_parse_done")
    resume_has_company_completed = _handoff_has_stage_payload(handoff_company, "company_completed")
    completed_result_checkpoint = _completed_result_checkpoint_from_stage_work_unit(deep_parse_work_unit)
    if (
        resume_recovery
        and existing_payload
        and str(result.status or "") == "completed"
        and resume_has_deep_parse_done
    ):
        _emit_company_completed_if_needed(
            progress=progress,
            result=result,
            updated_source_names=updated_source_names,
            already_emitted=resume_has_company_completed,
        )
        progress.sync_stage_handoffs_to_work_units()
        if not progress.ack_stage_handoff_work_unit(
            inn=result.inn,
            handoff_fingerprint=str(deep_parse_work_unit.get("handoff_fingerprint") or ""),
        ):
            raise RuntimeError(f"Failed to ack deep_parse work unit for INN {result.inn}")
        next_processed_rows = processed_rows + 1
        progress.mark_existing_result_processed(
            total_rows=total_rows,
            processed_rows=next_processed_rows,
        )
        logger.info(
            "  deep_parse_queue resume acked persisted explicit work unit: candidate_sites=%s validated=%s",
            len(result.candidate_sites),
            len([site for site in result.validated_sites if site.belongs_to_company]),
        )
        return next_processed_rows
    if (
        resume_recovery
        and str(result.status or "") != "completed"
        and resume_has_deep_parse_done
        and completed_result_checkpoint is not None
    ):
        result = core.company_result_from_dict(completed_result_checkpoint)
        _drop_stale_no_candidate_site_note(result)
        result.input_site = row.xlsx_site
        result.input_phone = row.xlsx_phone
        result.input_comment = row.comment
        next_processed_rows = processed_rows + 1
        _emit_company_completed_if_needed(
            progress=progress,
            result=result,
            updated_source_names=updated_source_names,
            already_emitted=resume_has_company_completed,
        )
        progress.sync_stage_handoffs_to_work_units()
        if not progress.ack_stage_handoff_work_unit(
            inn=result.inn,
            handoff_fingerprint=str(deep_parse_work_unit.get("handoff_fingerprint") or ""),
        ):
            raise RuntimeError(f"Failed to ack deep_parse work unit for INN {result.inn}")
        _persist_completed_company_result_with_finalization_timing(
            progress=progress,
            result=result,
            total_rows=total_rows,
            processed_rows=next_processed_rows,
            work_unit=deep_parse_work_unit,
            finalization_timing=finalization_timing,
        )
        logger.info(
            "  deep_parse_queue resume restored completed result from explicit checkpoint: candidate_sites=%s validated=%s",
            len(result.candidate_sites),
            len([site for site in result.validated_sites if site.belongs_to_company]),
        )
        return next_processed_rows
    if not resume_recovery and completed_result_checkpoint is not None:
        result = core.company_result_from_dict(completed_result_checkpoint)
        _drop_stale_no_candidate_site_note(result)
        result.input_site = row.xlsx_site
        result.input_phone = row.xlsx_phone
        result.input_comment = row.comment
        if not resume_has_deep_parse_done:
            progress.emit_stage_message(
                message_type="deep_parse_done",
                stage="deep_site_parse",
                inn=result.inn,
                row_index=result.row_index,
                payload=_build_deep_parse_stage_payload(
                    candidate_sites=result.candidate_sites,
                    validated_sites=result.validated_sites,
                    site_probes=result.site_probes,
                    route_strategies=result.route_strategies,
                    content_records=result.content_records,
                    lead_cards=result.lead_cards,
                ),
            )
            progress.materialize_unread_stage_handoffs()
        next_processed_rows = processed_rows + 1
        _emit_company_completed_if_needed(
            progress=progress,
            result=result,
            updated_source_names=updated_source_names,
            already_emitted=resume_has_company_completed,
        )
        progress.sync_stage_handoffs_to_work_units()
        if not progress.ack_stage_handoff_work_unit(
            inn=result.inn,
            handoff_fingerprint=str(deep_parse_work_unit.get("handoff_fingerprint") or ""),
        ):
            raise RuntimeError(f"Failed to ack prefetched deep_parse work unit for INN {result.inn}")
        _persist_completed_company_result_with_finalization_timing(
            progress=progress,
            result=result,
            total_rows=total_rows,
            processed_rows=next_processed_rows,
            work_unit=deep_parse_work_unit,
            finalization_timing=finalization_timing,
        )
        logger.info(
            "  deep_parse_queue consumer restored prefetched completed result: candidate_sites=%s validated=%s",
            len(result.candidate_sites),
            len([site for site in result.validated_sites if site.belongs_to_company]),
        )
        return next_processed_rows

    deep_parse_sites = _deep_parse_sites_from_stage_work_unit(deep_parse_work_unit)
    if not deep_parse_sites:
        raise RuntimeError(f"Missing explicit deep_parse_sites for deep_parse work unit INN {row.inn}")
    known_contacts = _known_contacts_from_stage_work_unit(deep_parse_work_unit)
    result.notes.extend(_gate_notes_from_stage_work_unit(deep_parse_work_unit))
    site_gate_decisions = _site_gate_decisions_from_stage_work_unit(deep_parse_work_unit)
    deep_parse_site_keys = {
        normalized_key
        for normalized_key in (
            _normalize_deep_parse_site_key(analyzer=analyzer, site_url=site_url)
            for site_url in deep_parse_sites
        )
        if normalized_key
    }
    validated_sites: list[object] = []
    pending_surface: dict[str, object] = {}
    for decision in site_gate_decisions:
        site_key = _normalize_deep_parse_site_key(
            analyzer=analyzer,
            site_url=getattr(decision, "final_url", "") or getattr(decision, "url", ""),
        )
        if site_key and site_key in deep_parse_site_keys:
            pending_surface[site_key] = decision
            continue
        validated_sites.append(decision)

    parser_company = FactorySiteParserCompany.from_row(
        row,
        candidate_sites=deep_parse_sites,
        source_results=source_results,
    )
    stage_span_recorder = _make_downstream_stage_span_recorder(
        event_sink=progress.append_event,
        row=row,
        execution_boundary=DEEP_PARSE_EXECUTION_BOUNDARY,
        handoff_fingerprint=str(deep_parse_work_unit.get("handoff_fingerprint") or ""),
    )
    _bind_factory_parser_ocr_context(
        parser=factory_site_parser,
        stage_governor=downstream_stage_governor,
        telemetry_tick=telemetry_tick,
        stage_span_recorder=stage_span_recorder,
    )
    with _stage_pool_context(
        downstream_stage_governor,
        stage_name=DEEP_PARSE_STAGE_NAME,
        telemetry_tick=telemetry_tick,
        stage_span_recorder=stage_span_recorder,
    ):
        with _stage_pool_context(
            downstream_stage_governor,
            stage_name=FACTORY_SITE_STAGE_NAME,
            telemetry_tick=telemetry_tick,
            stage_span_recorder=stage_span_recorder,
        ):
            parsed_factory_sites = factory_site_parser.parse(parser_company)
        result.site_probes = parsed_factory_sites.site_probes
        result.route_strategies = parsed_factory_sites.route_strategies
        result.content_records = parsed_factory_sites.content_records
        result.notes.extend(parsed_factory_sites.notes)

        with _stage_pool_context(
            downstream_stage_governor,
            stage_name=EXTRA_CHECK_STAGE_NAME,
            telemetry_tick=telemetry_tick,
            stage_span_recorder=stage_span_recorder,
        ):
            for site_plan in parsed_factory_sites.plans:
                site_key = _normalize_deep_parse_site_key(analyzer=analyzer, site_url=getattr(site_plan, "site_url", ""))
                surface_decision = pending_surface.pop(site_key, None)
                if not getattr(site_plan, "allows_deep_check", False):
                    if surface_decision is not None:
                        _append_site_decision_reason(
                            surface_decision,
                            "planner/probe blocked deep parse after cheap trust gate",
                        )
                        validated_sites.append(surface_decision)
                    continue
                validated_sites.append(
                    analyzer.analyze(
                        row,
                        getattr(site_plan, "site_url", ""),
                        known_contacts,
                        source_results,
                    )
                )

    for surface_decision in pending_surface.values():
        _append_site_decision_reason(
            surface_decision,
            "planner returned no deep-check site plan after cheap trust gate",
        )
        validated_sites.append(surface_decision)

    result.validated_sites = validated_sites
    result.trusted_contacts = core.build_trusted_contacts(
        row,
        source_results,
        result.merged_contacts,
        result.validated_sites,
    )
    primary_site = result.domain_resolution.selected_primary_domain if result.domain_resolution else ""
    with _stage_pool_context(
        downstream_stage_governor,
        stage_name=LLM_STAGE_NAME,
        telemetry_tick=telemetry_tick,
        stage_span_recorder=stage_span_recorder,
    ):
        for record in result.content_records:
            classify_content_record(record)
            if should_use_llm_record_review(record):
                llm_result = analyzer.llm.judge_content_record(
                    row,
                    record,
                    primary_site,
                )
                if llm_result:
                    record.llm_result = llm_result
                    llm_confidence = float(llm_result.get("confidence", 0.0) or 0.0)
                    if llm_confidence >= 0.6:
                        record.relevance_label = str(llm_result.get("relevance_label", record.relevance_label))
                        record.relevance_score = max(record.relevance_score, llm_confidence)
                        summary = core.normalize_whitespace(str(llm_result.get("summary", "")))
                        if summary:
                            record.relevance_reasons = core.dedupe_preserve_order(record.relevance_reasons + [summary])[:8]
            elif analyzer.llm.should_force_benchmark_stage("content_review"):
                analyzer.llm.capture_forced_content_review_fixture(
                    row=row,
                    record=record,
                    primary_site=primary_site,
                    prod_skip_reason=describe_content_review_prod_skip_reason(record),
                )

    result.lead_cards = core.build_lead_cards(
        row,
        result.domain_resolution,
        result.trusted_contacts,
        result.merged_contacts,
        result.content_records,
    )
    result.site_refresh_plans = core.build_site_refresh_plans(result.candidate_sites, result.site_probes)
    result.finished_at = core.utc_now_iso()
    result.status = "completed"
    _drop_stale_no_candidate_site_note(result)
    deep_parse_work_unit = progress.merge_stage_work_unit_private_state(
        inn=result.inn,
        handoff_fingerprint=str(deep_parse_work_unit.get("handoff_fingerprint") or ""),
        private_state_patch=_completed_result_checkpoint_patch(deep_parse_work_unit, result=result),
    )
    if not deep_parse_work_unit:
        raise RuntimeError(f"Failed to checkpoint deep_parse work unit for INN {result.inn}")
    if not resume_has_deep_parse_done:
        progress.emit_stage_message(
            message_type="deep_parse_done",
            stage="deep_site_parse",
            inn=result.inn,
            row_index=result.row_index,
            payload=_build_deep_parse_stage_payload(
                candidate_sites=result.candidate_sites,
                validated_sites=result.validated_sites,
                site_probes=result.site_probes,
                route_strategies=result.route_strategies,
                content_records=result.content_records,
                lead_cards=result.lead_cards,
            ),
        )
        progress.materialize_unread_stage_handoffs()
    next_processed_rows = processed_rows + 1
    _emit_company_completed_if_needed(
        progress=progress,
        result=result,
        updated_source_names=updated_source_names,
        already_emitted=resume_has_company_completed,
    )
    progress.sync_stage_handoffs_to_work_units()
    if not progress.ack_stage_handoff_work_unit(
        inn=result.inn,
        handoff_fingerprint=str(deep_parse_work_unit.get("handoff_fingerprint") or ""),
    ):
        raise RuntimeError(f"Failed to ack deep_parse work unit for INN {result.inn}")
    _persist_completed_company_result_with_finalization_timing(
        progress=progress,
        result=result,
        total_rows=total_rows,
        processed_rows=next_processed_rows,
        work_unit=deep_parse_work_unit,
        finalization_timing=finalization_timing,
    )
    logger.info(
        "  deep_parse_queue consumer acked result: candidate_sites=%s validated=%s",
        len(result.candidate_sites),
        len([site for site in result.validated_sites if site.belongs_to_company]),
    )
    return next_processed_rows


def _handoff_has_stage_payload(handoff_company: dict[str, object] | None, field_name: str) -> bool:
    payload = handoff_company.get(field_name) if isinstance(handoff_company, dict) else None
    return isinstance(payload, dict) and any(key not in {"stage", "ts"} for key in payload)


def _existing_site_gate_decision_urls(handoff_company: dict[str, object] | None) -> set[str]:
    existing_urls: set[str] = set()
    payloads = handoff_company.get("site_gate_decisions") if isinstance(handoff_company, dict) else []
    for payload in payloads or []:
        if not isinstance(payload, dict):
            continue
        site_url = core.normalize_whitespace(str(payload.get("site_url") or ""))
        if site_url:
            existing_urls.add(site_url)
    return existing_urls


def _rounded_site_decision_score(value: object) -> float:
    try:
        return round(float(value or 0.0), 3)
    except (TypeError, ValueError):
        return 0.0


def _iter_site_gate_stage_payloads(validated_sites: list[object]) -> list[dict[str, object]]:
    payloads: list[dict[str, object]] = []
    for decision in validated_sites or []:
        final_url = core.sanitize_website_url(getattr(decision, "final_url", ""))
        site_url = final_url or core.sanitize_website_url(getattr(decision, "url", ""))
        if not site_url:
            fallback_url = core.normalize_whitespace(
                str(getattr(decision, "final_url", "") or getattr(decision, "url", ""))
            )
            if not fallback_url:
                continue
            site_url = fallback_url
        payload: dict[str, object] = {
            "site_url": site_url,
            "decision_status": core.normalize_whitespace(str(getattr(decision, "decision_status", "") or "")),
            "authenticity_score": _rounded_site_decision_score(getattr(decision, "authenticity_score", 0.0)),
            "identity_score": _rounded_site_decision_score(getattr(decision, "identity_score", 0.0)),
            "viability_score": _rounded_site_decision_score(getattr(decision, "viability_score", 0.0)),
        }
        belongs_to_company = getattr(decision, "belongs_to_company", None)
        if isinstance(belongs_to_company, bool):
            payload["belongs_to_company"] = belongs_to_company
        payloads.append(payload)
    return payloads


def _normalize_optional_stage_text(value: object) -> str:
    return core.normalize_whitespace(str(value or ""))


def _normalize_deep_parse_site_url(site_url: object) -> str | None:
    normalized_url = core.sanitize_website_url(site_url)
    if normalized_url:
        return normalized_url
    fallback_url = _normalize_optional_stage_text(site_url)
    return fallback_url or None


def _select_deep_parse_site_decision(validated_sites: list[object]) -> object | None:
    decisions = list(validated_sites or [])
    for decision in decisions:
        if getattr(decision, "belongs_to_company", None) is True:
            return decision
    for decision in decisions:
        if _normalize_deep_parse_site_url(getattr(decision, "final_url", "") or getattr(decision, "url", "")):
            return decision
    return None


def _resolve_deep_parse_status(
    *,
    selected_decision: object | None,
    candidate_sites: list[str],
) -> str:
    if selected_decision is not None:
        normalized = _normalize_optional_stage_text(getattr(selected_decision, "decision_status", ""))
        if normalized:
            return normalized
    return "no_candidate_site" if not candidate_sites else "no_validated_site"


def _build_deep_parse_stage_payload(
    *,
    candidate_sites: list[str],
    validated_sites: list[object],
    site_probes: list[object],
    route_strategies: list[object],
    content_records: list[object],
    lead_cards: list[object] | None,
) -> dict[str, object]:
    selected_decision = _select_deep_parse_site_decision(validated_sites)
    site_url = None
    if selected_decision is not None:
        site_url = _normalize_deep_parse_site_url(
            getattr(selected_decision, "final_url", "") or getattr(selected_decision, "url", "")
        )
    if site_url is None:
        for candidate_site in candidate_sites or []:
            site_url = _normalize_deep_parse_site_url(candidate_site)
            if site_url is not None:
                break

    payload: dict[str, object] = {
        "site_url": site_url,
        "decision_status": _resolve_deep_parse_status(
            selected_decision=selected_decision,
            candidate_sites=candidate_sites,
        ),
        "content_records_count": len(content_records or []),
        "site_probes_count": len(site_probes or []),
        "route_strategies_count": len(route_strategies or []),
    }
    if lead_cards is not None:
        payload["lead_cards_count"] = len(lead_cards)
    return payload


def _parse_selection_ordinals(
    parser: argparse.ArgumentParser,
    argv: list[str],
    raw_value: str,
) -> list[int] | None:
    if not _option_was_provided(argv, "--ordinals"):
        return None
    if any(_option_was_provided(argv, option) for option in ("--start-from", "--count", "--limit")):
        parser.error("--ordinals conflicts with --start-from/--count/--limit")
    try:
        return parse_ordinals(raw_value)
    except ValueError as exc:
        parser.error(str(exc))
    return None


def build_list_org_offline_note(snapshot_path: Path) -> str:
    return f"List-Org mode={LIST_ORG_OFFLINE_MODE} file={snapshot_path.resolve()}; live_requests=0"


def normalize_list_org_offline_result(
    source_result: core.SourceResult,
    *,
    snapshot_path: Path,
    refreshed: bool,
) -> core.SourceResult:
    snapshot_ref = str(snapshot_path.resolve())
    filtered_notes = [
        note
        for note in source_result.notes
        if not note.startswith("List-Org mode=") and note != LIST_ORG_OFFLINE_STALE_NOTE
    ]
    normalized_notes = [build_list_org_offline_note(snapshot_path)]
    if not refreshed:
        normalized_notes.append(LIST_ORG_OFFLINE_STALE_NOTE)
    normalized_notes.extend(filtered_notes)
    source_result.notes = core.dedupe_preserve_order(normalized_notes)
    source_result.search_url = ""
    source_result.listing_url = snapshot_ref
    source_result.entity_url = ""
    source_result.links = []
    for field_name in core.SHARED_CONTACT_FIELDS:
        normalized_items = [
            core.ContactItem(
                value=item.value,
                source_url=snapshot_ref,
                kind=item.kind,
                masked=item.masked,
                note=item.note,
            )
            for item in getattr(source_result, field_name)
        ]
        setattr(source_result, field_name, normalized_items)
    return source_result


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass
    argv_list = list(sys.argv[1:] if argv is None else argv)

    parser = argparse.ArgumentParser(
        description="Полный pipeline company enrichment: агрегаторы, candidate sites, site validation, content sampling и итоговые артефакты."
    )
    parser.add_argument("--env-file", default=core.DEFAULT_ENV_FILE, help="Локальный .env файл с секретами и настройками.")
    parser.add_argument("--input", help="Путь до XLSX файла. По умолчанию берется первый .xlsx в текущей папке.")
    parser.add_argument("--output-dir", default="output", help="Папка для JSON, логов, прогресса и финальных артефактов.")
    parser.add_argument("--count", default="all", help="Сколько компаний обработать: all или число, например 10.")
    parser.add_argument("--limit", type=int, default=0, help="Совместимость со старым флагом. Если задан, заменяет --count.")
    parser.add_argument(
        "--resume",
        action="store_true",
        help="Пропускать ИНН, которые уже есть в canonical runtime state текущего output-dir.",
    )
    parser.add_argument(
        "--sources",
        default="all",
        help="Какие источники запускать: all или список через запятую, например spark,rusprofile.",
    )
    parser.add_argument(
        "--retry-blocked-source",
        default="",
        help="Переобработать только компании, где этот source раньше упал по IP-блоку: rate_limited, bot_gate, cooldown_active.",
    )
    parser.add_argument(
        "--defer-required-source-transients",
        action="store_true",
        help="Продолжать следующие строки при bounded per-row transient required-source failures; системные сбои остаются fail-closed.",
    )
    parser.add_argument(
        "--retry-deferred-required-sources",
        action="store_true",
        help="Переобработать только unresolved row/source пары из deferred_required_sources текущего output-dir.",
    )
    parser.add_argument(
        "--source-budget-seconds",
        type=float,
        default=12.0,
        help="Порог предупреждения, если один источник отвечает слишком долго.",
    )
    parser.add_argument(
        "--listorg-bootstrap",
        action="store_true",
        help="Legacy-флаг; сейчас игнорируется, потому что List-Org в pipeline работает только от offline search.json.",
    )
    parser.add_argument(
        "--listorg-bootstrap-on-block",
        action="store_true",
        help="Legacy-флаг; сейчас игнорируется, потому что List-Org в pipeline работает только от offline search.json.",
    )
    parser.add_argument(
        "--bootstrap-only",
        action="store_true",
        help="Legacy-флаг для устаревшего live bootstrap; при offline-only List-Org эффекта не дает.",
    )
    parser.add_argument(
        "--bootstrap-headless",
        action="store_true",
        help="Legacy-флаг для устаревшего live bootstrap; при offline-only List-Org эффекта не дает.",
    )
    parser.add_argument(
        "--listorg-session-file",
        default="",
        help="Legacy-параметр для устаревшей live session profile; offline-only адаптер List-Org его не использует.",
    )
    parser.add_argument(
        "--listorg-json",
        default="",
        help="Путь к offline-выгрузке List-Org search.json. В текущем pipeline List-Org работает только от этого snapshot.",
    )
    parser.add_argument(
        "--start-from",
        type=int,
        default=1,
        help="Start from 1-based company ordinal in XLSX order after the header row.",
    )
    parser.add_argument(
        "--company-concurrency",
        default=os.getenv("PARSER_COMPANY_CONCURRENCY", "1"),
        help=(
            "Запрошенный global company concurrency cap. "
            "Сейчас используется только как config/guardrail surface для future bounded concurrency; "
            "serial execution не включает multithreading."
        ),
    )
    parser.add_argument(
        "--ordinals",
        default="",
        help="Exact 1-based XLSX ordinals after the header row, for example 31,38,52,55,80.",
    )
    parser.add_argument(
        "--llm-benchmark-capture-dir",
        default="",
        help="Directory for benchmark-only LLM fixtures.",
    )
    parser.add_argument(
        "--llm-benchmark-force-stages",
        default="",
        help="Force fixture capture for skipped LLM stages: site_decision,content_review.",
    )
    parser.add_argument(
        "--llm-benchmark-capture-only",
        action="store_true",
        help="Capture benchmark fixtures without live OpenAI requests.",
    )
    args = parser.parse_args(argv_list)
    try:
        args.company_concurrency = _resolve_requested_company_concurrency(args.company_concurrency)
    except ValueError as exc:
        parser.error(str(exc))
    if args.retry_deferred_required_sources and args.retry_blocked_source:
        parser.error("--retry-deferred-required-sources conflicts with --retry-blocked-source")
    if args.retry_deferred_required_sources and args.resume:
        parser.error("--retry-deferred-required-sources resumes deferred state explicitly and conflicts with --resume")
    args.ordinals = _parse_selection_ordinals(parser, argv_list, args.ordinals)
    if (args.llm_benchmark_force_stages or args.llm_benchmark_capture_only) and not args.llm_benchmark_capture_dir:
        parser.error(
            "--llm-benchmark-capture-dir is required when using --llm-benchmark-force-stages or --llm-benchmark-capture-only"
        )
    try:
        args.llm_benchmark_force_stages = parse_llm_benchmark_force_stages(args.llm_benchmark_force_stages)
    except ValueError as exc:
        parser.error(str(exc))
    return args


def select_rows_for_run(rows: list[core.RowInput], *, start_from: int, count: int | None) -> list[core.RowInput]:
    return select_rows_for_window(rows, start_from=start_from, count=count)


def run(args: argparse.Namespace) -> int:
    if args.env_file:
        core.load_env_file(Path(args.env_file))

    output_dir = Path(args.output_dir)
    core.ensure_dir(output_dir)
    logger = core.configure_logger(output_dir / "run.log")
    progress = ProgressStore(output_dir)
    list_org_session_file = Path(args.listorg_session_file) if args.listorg_session_file else output_dir / core.DEFAULT_LISTORG_SESSION_RELATIVE_PATH
    list_org_json_file = (
        Path(args.listorg_json)
        if args.listorg_json
        else Path(os.getenv("LISTORG_JSON_FILE", "")).expanduser()
        if os.getenv("LISTORG_JSON_FILE", "").strip()
        else Path.cwd() / core.DEFAULT_LISTORG_JSON_FILE
    )
    list_org_offline_note = build_list_org_offline_note(list_org_json_file)

    if args.listorg_bootstrap:
        logger.info("List-Org работает только от offline search.json; --listorg-bootstrap пропущен. %s", list_org_offline_note)
        if args.bootstrap_only:
            logger.info("Bootstrap-only mode завершен без live-действий: List-Org offline-only.")
            return 0

    input_path = Path(args.input) if args.input else core.find_default_xlsx(Path.cwd())
    rows = core.load_rows_from_xlsx(input_path)
    count = args.limit if args.limit else core.parse_count(args.count)
    if args.retry_blocked_source == "list_org":
        raise ValueError("retry-blocked-source=list_org не поддерживается: List-Org работает только от offline search.json")

    proxy_pool = ProxyPool(os.getenv("PARSER_PROXIES"))
    min_delay_by_host = {
        "checko.ru": float(os.getenv("DELAY_CHECKO_SECONDS", "5.0")),
        "www.checko.ru": float(os.getenv("DELAY_CHECKO_SECONDS", "5.0")),
        "spark-interfax.ru": float(os.getenv("DELAY_SPARK_SECONDS", "4.0")),
        "zachestnyibiznes.ru": float(os.getenv("DELAY_ZB_SECONDS", "4.0")),
        "www.rusprofile.ru": float(os.getenv("DELAY_RUSPROFILE_SECONDS", "6.0")),
        "rusprofile.ru": float(os.getenv("DELAY_RUSPROFILE_SECONDS", "6.0")),
        "www.list-org.com": float(os.getenv("DELAY_LISTORG_SECONDS", "3.0")),
    }
    http_client = core.RateLimitedHttpClient(
        logger=logger,
        progress_store=progress,
        min_delay_by_host=min_delay_by_host,
        request_timeout=int(os.getenv("REQUEST_TIMEOUT_SECONDS", "18")),
        cooldown_on_429=int(os.getenv("COOLDOWN_429_SECONDS", str(60 * 60))),
        cooldown_on_bot=int(os.getenv("COOLDOWN_BOT_SECONDS", str(90 * 60))),
        proxy_pool=proxy_pool,
        list_org_session_file=Path(os.getenv("LISTORG_SESSION_FILE", "")).expanduser()
        if os.getenv("LISTORG_SESSION_FILE", "").strip()
        else list_org_session_file,
    )

    source_registry = {
        "spark": SparkSource(http_client),
        "zachestnyibiznes": ZachestnyBiznesSource(http_client),
        "rusprofile": RusprofileSource(http_client),
        "checko": CheckoSource(http_client),
        "list_org": ListOrgSource(http_client, list_org_json_file),
    }
    source_modes = {
        "spark": "live",
        "zachestnyibiznes": "live",
        "rusprofile": "live",
        "checko": "live",
        "list_org": LIST_ORG_OFFLINE_MODE,
    }
    selected_sources = core.parse_source_names(args.sources)
    retry_deferred_source_names_by_inn: dict[str, tuple[str, ...]] = {}
    if args.retry_deferred_required_sources:
        (
            retry_deferred_active_source_names,
            retry_deferred_source_names_by_inn,
        ) = _deferred_required_source_retry_plan(
            unresolved_records=progress.unresolved_required_source_deferred_records(),
            source_order=list(source_modes.keys()),
            selected_sources=selected_sources,
        )
        selected_sources = set(retry_deferred_active_source_names)
    elif args.retry_blocked_source:
        selected_sources = {args.retry_blocked_source}
    if selected_sources is None:
        active_source_names = list(source_modes.keys())
    else:
        unknown = selected_sources - set(source_modes.keys())
        if unknown:
            raise ValueError(f"Неизвестные источники: {', '.join(sorted(unknown))}")
        active_source_names = [name for name in source_modes.keys() if name in selected_sources]
    execution_guardrails = build_source_execution_guardrails(
        active_sources=active_source_names,
        requested_company_concurrency=args.company_concurrency,
        usable_proxy_pool_count=_resolve_usable_proxy_pool_count(proxy_pool),
    )
    downstream_worker_pool_contour = build_downstream_worker_pool_contour(
        company_concurrency_cap=execution_guardrails.company_concurrency_cap,
    )
    downstream_worker_pool_payload = downstream_worker_pool_contour.as_payload()
    source_lane_scheduler = execution_guardrails.as_dict()
    source_lane_scheduler["downstream_worker_pools"] = _clone_downstream_worker_pool_payload(
        downstream_worker_pool_payload
    )
    logger.info(
        "Execution guardrails: company_cap=%s effective_company_cap=%s proxy_pool=%s per_source_cap=%s per_source_lane_budget=%s per_source_worker_lane_budget=%s per_host_cap=%s transport_policy=%s",
        execution_guardrails.company_concurrency_cap,
        execution_guardrails.effective_company_concurrency_cap,
        execution_guardrails.usable_proxy_pool_count,
        execution_guardrails.per_source_cap_map,
        execution_guardrails.per_source_lane_budget_map,
        execution_guardrails.per_source_worker_lane_budget_map,
        execution_guardrails.per_host_cap_map,
        execution_guardrails.source_transport_policy,
    )
    logger.info(
        "Source lane contour resolved: %s",
        source_lane_scheduler.get("source_lane_contour", []),
    )
    logger.info(
        "Downstream worker pool contour resolved: %s",
        downstream_worker_pool_payload.get("lanes", []),
    )
    active_sources = [source_registry[name] for name in active_source_names]
    offline_only_source_names = {
        name for name in active_source_names if source_modes.get(name) == LIST_ORG_OFFLINE_MODE
    }
    if "list_org" in offline_only_source_names:
        logger.info("List-Org закреплен как offline-only источник: %s", list_org_offline_note)
        if args.listorg_bootstrap_on_block:
            logger.info("List-Org offline-only: --listorg-bootstrap-on-block будет проигнорирован.")

    filtered_rows = rows
    if args.retry_deferred_required_sources:
        filtered_rows = [
            row
            for row in rows
            if core.normalize_whitespace(str(row.inn or "")) in retry_deferred_source_names_by_inn
        ]
        logger.info(
            "Фильтр retry-deferred-required-sources оставил строк: %s, источники: %s",
            len(filtered_rows),
            ", ".join(active_source_names) if active_source_names else "none",
        )
    elif args.retry_blocked_source:
        retry_source = args.retry_blocked_source
        filtered_rows = []
        for row in rows:
            existing = progress.get(row.inn)
            if not existing:
                continue
            source_payload = (existing.get("sources") or {}).get(retry_source) or {}
            if core.is_retryable_block_status(source_payload.get("status", "")):
                filtered_rows.append(row)
        logger.info("Фильтр retry-blocked-source=%s оставил строк: %s", retry_source, len(filtered_rows))

    selection = resolve_row_selection(
        filtered_rows,
        start_from=args.start_from,
        count=count,
        ordinals=args.ordinals,
    )
    filtered_rows = selection.rows

    logger.info("Загружено строк из XLSX: %s", len(rows))
    logger.info("В текущем запуске будет обработано строк: %s", len(filtered_rows))
    logger.info("Активные источники: %s", ", ".join(active_source_names))
    resume_pending_explicit_work_units = (
        _pending_selected_explicit_stage_work_units(progress=progress, rows=filtered_rows)
        if args.resume
        else {}
    )
    resume_pending_explicit_inns = set(resume_pending_explicit_work_units)
    resume_skipped_rows = 0
    if args.resume:
        resume_skipped_rows = sum(
            1
            for row in filtered_rows
            if row.inn not in resume_pending_explicit_work_units
            and core.should_skip_on_resume(
                progress.get(row.inn),
                active_source_names,
                retry_blocked_source=args.retry_blocked_source,
            )
        )
        logger.info(
            "Resume contract: skip-ready=%s pending=%s",
            resume_skipped_rows,
            max(len(filtered_rows) - resume_skipped_rows, 0),
        )
    if selection.mode == "ordinals":
        logger.info(
            "Run selection: mode=ordinals ordinals=%s selected=%s",
            ",".join(str(ordinal) for ordinal in selection.selected_ordinals),
            len(filtered_rows),
        )
    else:
        logger.info(
            "Run window: start_from=%s end_at=%s selected=%s",
            selection.start_from,
            selection.end_at if selection.end_at is not None else "none",
            len(filtered_rows),
        )
    source_search_rows = _source_search_rows_for_run(
        rows=filtered_rows,
        progress=progress,
        active_source_names=active_source_names,
        args=args,
        resume_pending_explicit_inns=resume_pending_explicit_inns,
    )
    direct_default_executor_plan = plan_direct_default_bounded_executor(
        active_sources=active_source_names,
        company_concurrency_cap=execution_guardrails.company_concurrency_cap,
        per_source_lane_budget_map=execution_guardrails.per_source_lane_budget_map,
    )
    downstream_prefetch_queue_limit = (
        _resolve_downstream_prefetch_queue_limit(
            direct_default_executor_plan.max_workers,
            selected_rows_count=len(filtered_rows),
        )
        if direct_default_executor_plan.enabled and direct_default_executor_plan.max_workers > 1
        else 0
    )
    downstream_prefetch_ready_drain_limit = (
        _resolve_downstream_prefetch_ready_drain_limit(
            direct_default_executor_plan.max_workers,
            pending_queue_limit=downstream_prefetch_queue_limit,
        )
        if downstream_prefetch_queue_limit > 0
        else 0
    )
    downstream_prefetch_ready_drain_low_watermark = (
        _resolve_downstream_prefetch_ready_drain_low_watermark(
            ready_drain_limit=downstream_prefetch_ready_drain_limit,
        )
        if downstream_prefetch_ready_drain_limit > 0
        else 0
    )
    checko_worker_lane_budget = _resolve_checko_worker_lane_budget(
        active_source_names=active_source_names,
        source_lane_scheduler=source_lane_scheduler,
    )
    backpressure_policy = _build_runtime_backpressure_policy(
        active_source_names=active_source_names,
        source_lane_scheduler=source_lane_scheduler,
        downstream_worker_pools=downstream_worker_pool_payload,
        direct_default_executor_plan=direct_default_executor_plan,
        source_pending_rows=len(source_search_rows),
        downstream_prefetch_queue_limit=downstream_prefetch_queue_limit,
        downstream_prefetch_ready_drain_limit=downstream_prefetch_ready_drain_limit,
        ready_idle_source_wait_drain_seconds=READY_IDLE_SOURCE_WAIT_DRAIN_SECONDS,
        ready_idle_source_wait_drain_max_rows=READY_IDLE_SOURCE_WAIT_DRAIN_MAX_ROWS,
    )
    source_lane_telemetry = SourceLaneTelemetryLedger(source_lane_scheduler=source_lane_scheduler)
    source_lane_telemetry.seed_queue_depths(
        {
            source_name: len(source_search_rows)
            for source_name in active_source_names
        }
    )
    source_stage_governor = StagePoolGovernor(
        per_stage_budget_map=execution_guardrails.per_source_lane_budget_map,
    )
    downstream_stage_governor = StagePoolGovernor(
        per_stage_budget_map=downstream_worker_pool_contour.per_stage_budget_map(),
    )
    initial_throughput_telemetry = build_throughput_telemetry_payload(
        source_lane_scheduler=source_lane_scheduler,
        downstream_worker_pools=downstream_worker_pool_payload,
        source_lane_runtime=source_lane_telemetry.snapshot(),
        source_stage_runtime=source_stage_governor.snapshot(),
        downstream_stage_runtime=downstream_stage_governor.snapshot(),
        backpressure_policy=backpressure_policy,
        rows_completed=0,
    )
    progress.run_started(
        input_path=input_path,
        total_rows=len(rows),
        selected_rows=len(filtered_rows),
        selection_mode=selection.mode,
        selected_ordinals=list(selection.selected_ordinals),
        start_from=selection.start_from,
        end_at=selection.end_at,
        active_sources=active_source_names,
        retry_blocked_source=args.retry_blocked_source,
        resume_skipped_rows=resume_skipped_rows,
        continue_existing_run=bool(args.resume or args.retry_blocked_source or args.retry_deferred_required_sources),
        source_lane_scheduler=source_lane_scheduler,
        downstream_worker_pools=downstream_worker_pool_payload,
        throughput_telemetry=initial_throughput_telemetry,
    )
    progress.materialize_unread_stage_handoffs()
    benchmark_capture = (
        LLMBenchmarkCaptureWriter(
            LLMBenchmarkCaptureConfig(
                capture_dir=Path(args.llm_benchmark_capture_dir),
                force_stages=args.llm_benchmark_force_stages,
                capture_only=bool(args.llm_benchmark_capture_only),
                source_run_selection={
                    "mode": selection.mode,
                    "selected_ordinals": list(selection.selected_ordinals),
                    "start_from": selection.start_from,
                    "end_at": selection.end_at,
                },
            )
        )
        if args.llm_benchmark_capture_dir
        else None
    )
    if benchmark_capture:
        logger.info(
            "LLM benchmark capture enabled: dir=%s force_stages=%s capture_only=%s",
            Path(args.llm_benchmark_capture_dir).resolve(),
            ",".join(sorted(args.llm_benchmark_force_stages)) or "none",
            bool(args.llm_benchmark_capture_only),
        )

    direct_default_executor: RollingCompanySourceBatchExecutor | None = None
    checko_worker_lane_executor: RollingCompanySourceBatchExecutor | None = None
    processed = 0
    controlled_stop_request: dict[str, str] | None = None
    terminal_context_payload = _terminal_context(checkpoint="setup_runtime_resources")
    terminal_exception: BaseException | None = None
    terminal_traceback = None
    direct_default_host_ledger: HostGovernorLedger | None = None
    downstream_prefetch_executor: ThreadPoolExecutor | None = None
    downstream_prefetch_shutdown = Event()
    downstream_prefetch_buffered_progress = None
    pending_explicit_runtime_rows: deque[PendingExplicitRuntimeRow] = deque()
    source_wait_tail_service_active = False

    def capture_runtime_throughput() -> dict[str, object]:
        return _capture_runtime_throughput(
            progress=progress,
            source_lane_scheduler=source_lane_scheduler,
            downstream_worker_pools=downstream_worker_pool_payload,
            source_lane_telemetry=source_lane_telemetry,
            source_stage_governor=source_stage_governor,
            downstream_stage_governor=downstream_stage_governor,
            backpressure_policy=backpressure_policy,
            direct_default_host_ledger=direct_default_host_ledger,
        )

    def drain_ready_pending_explicit_runtime_rows(*, max_rows: int | None = None) -> bool:
        nonlocal processed, controlled_stop_request, terminal_context_payload
        drained = False
        drained_count = 0
        while (
            pending_explicit_runtime_rows
            and _pending_downstream_ready_count(pending_explicit_runtime_rows) > 0
            and (max_rows is None or drained_count < max_rows)
        ):
            pending_row = _pop_next_pending_explicit_runtime_row(
                pending_explicit_runtime_rows,
                prefer_ready=True,
            )
            processed, controlled_stop_request, terminal_context_payload = _drain_pending_explicit_runtime_row(
                pending_row=pending_row,
                progress=progress,
                controlled_stop_request=controlled_stop_request,
                active_source_names=active_source_names,
                analyzer=analyzer,
                factory_site_parser=factory_site_parser,
                downstream_stage_governor=downstream_stage_governor,
                total_rows=len(filtered_rows),
                processed_rows=processed,
                logger=logger,
                buffered_progress_store=downstream_prefetch_buffered_progress,
                telemetry_tick=capture_runtime_throughput,
            )
            drained = True
            drained_count += 1
            if controlled_stop_request is not None:
                break
        return drained

    def service_runtime_tail_during_source_wait(*, max_rows: int | None = None) -> None:
        nonlocal source_wait_tail_service_active
        if source_wait_tail_service_active:
            capture_runtime_throughput()
            return
        source_wait_tail_service_active = True
        drained_count = 0
        try:
            while (
                controlled_stop_request is None
                and pending_explicit_runtime_rows
                and _pending_downstream_ready_count(pending_explicit_runtime_rows) > 0
            ):
                if max_rows is not None and drained_count >= max_rows:
                    break
                drain_limit = 1 if max_rows is not None else None
                if not drain_ready_pending_explicit_runtime_rows(max_rows=drain_limit):
                    break
                if max_rows is not None:
                    drained_count += 1
                capture_runtime_throughput()
            capture_runtime_throughput()
        finally:
            source_wait_tail_service_active = False

    def service_stale_ready_tail_before_inline_source_wait() -> None:
        if not pending_explicit_runtime_rows:
            return
        oldest_ready_idle_seconds = _oldest_ready_pending_downstream_idle_seconds(
            pending_explicit_runtime_rows,
            now_iso=core.utc_now_iso(),
        )
        if oldest_ready_idle_seconds < READY_IDLE_SOURCE_WAIT_DRAIN_SECONDS:
            return
        service_runtime_tail_during_source_wait(max_rows=READY_IDLE_SOURCE_WAIT_DRAIN_MAX_ROWS)

    try:
        checko_startup_stop = _checko_proxy_provider_startup_stop(
            active_source_names=active_source_names,
            source_search_rows=source_search_rows,
            proxy_pool=proxy_pool,
        )
        if checko_startup_stop is not None:
            reason, proxy_provider_fields = checko_startup_stop
            terminal_context_payload = _terminal_context(
                checkpoint="checko_proxy_provider_startup_guardrail",
                source_name="checko",
                source_status=core.REQUEST_STATUS_BLOCKED_NO_PROXY,
                source_access_mode="proxy-bound",
            )
            progress.append_event(
                {
                    "ts": core.utc_now_iso(),
                    "type": "required_source_proxy_provider_stopper",
                    "source": "checko",
                    "source_status": core.REQUEST_STATUS_BLOCKED_NO_PROXY,
                    "source_access_mode": "proxy-bound",
                    "reason": reason,
                    **proxy_provider_fields,
                }
            )
            raise RequiredSourceOperationalError(
                source_name="checko",
                source_status=core.REQUEST_STATUS_BLOCKED_NO_PROXY,
                source_access_mode="proxy-bound",
                reason=reason,
            )

        factory_site_parser = _build_prefetched_factory_site_parser(
            http_client=http_client,
            output_dir=output_dir,
            stage_governor=downstream_stage_governor,
            telemetry_tick=capture_runtime_throughput,
        )
        site_auth_helpers = SiteAuthHelpers(
            normalize_url=core.normalize_url,
            normalize_whitespace=core.normalize_whitespace,
            parse_title_and_meta=core.parse_title_and_meta,
            dedupe_preserve_order=core.dedupe_preserve_order,
            extract_emails=core.extract_emails,
            extract_phones=core.extract_phones,
            extract_probable_addresses=core.extract_probable_addresses,
            normalize_phone_values=core.normalize_phone_values,
            normalize_address_values=core.normalize_address_values,
            normalize_phone_candidate=core.normalize_phone_candidate,
            company_tokens=core.company_tokens,
            normalized_phone_digits=core.normalized_phone_digits,
            guess_registered_domain=core.guess_registered_domain,
            address_identity_tokens=core.address_identity_tokens,
            is_valid_russian_inn=core.is_valid_russian_inn,
            keyword_found_in_text=core.keyword_found_in_text,
            compact_text=core.compact_text,
            summarize_source_context=core.summarize_source_context,
            looks_like_bot_gate=core.looks_like_bot_gate,
            contact_path_hints=core.CONTACT_PATH_HINTS,
            contact_link_text_hints=core.CONTACT_LINK_TEXT_HINTS,
            industrial_positive_keywords=core.INDUSTRIAL_POSITIVE_KEYWORDS,
            industrial_negative_keywords=core.INDUSTRIAL_NEGATIVE_KEYWORDS,
            generic_email_domains=core.GENERIC_EMAIL_DOMAINS,
            company_token_stopwords=core.COMPANY_TOKEN_STOPWORDS,
            activity_token_stopwords=core.ACTIVITY_TOKEN_STOPWORDS,
            non_corporate_domains=core.NON_CORPORATE_DOMAINS,
        )
        analyzer = BenchmarkAwareSiteAuthenticityAnalyzer(
            http_client,
            core.OpenAIDecider(logger, progress, benchmark_capture=benchmark_capture),
            site_auth_helpers,
        )
        disabled_sources_for_run: dict[str, str] = {}
        direct_default_prefetch_sources = [
            source_registry[source_name]
            for source_name in direct_default_executor_plan.active_sources
            if source_name in source_registry
        ]
        prefetch_covers_full_source_contour = tuple(direct_default_executor_plan.active_sources) == tuple(active_source_names)
        if direct_default_executor_plan.enabled:
            direct_default_executor_rows = list(source_search_rows)
            if direct_default_executor_rows and direct_default_prefetch_sources:
                worker_count = min(direct_default_executor_plan.max_workers, len(direct_default_executor_rows))
                direct_default_prefetch_policy = dict(backpressure_policy.get("direct_default_prefetch") or {})
                direct_default_prefetch_policy["active_workers"] = worker_count
                backpressure_policy["direct_default_prefetch"] = direct_default_prefetch_policy
                if prefetch_covers_full_source_contour:
                    direct_default_host_ledger = HostGovernorLedger(
                        persisted_host_memory=lambda: getattr(http_client.progress_store, "host_memory", {}),
                        now_fn=time.time,
                        sleep_fn=time.sleep,
                    )
                logger.info(
                    "Direct-default bounded executor activated: workers=%s active_sources=%s companies=%s full_contour=%s",
                    worker_count,
                    ",".join(direct_default_executor_plan.active_sources),
                    len(direct_default_executor_rows),
                    "yes" if prefetch_covers_full_source_contour else "no",
                )
                direct_default_executor = open_company_source_search_executor(
                    rows=direct_default_executor_rows,
                    sources=direct_default_prefetch_sources,
                    shared_client=http_client,
                    worker_count=worker_count,
                    prepare_downstream=(
                        (
                            lambda *, row, source_results: _prepare_prefetched_company_downstream_execution(
                                row=row,
                                source_results=source_results,
                                active_source_names=active_source_names,
                                analyzer=analyzer,
                                factory_site_parser_factory=(
                                    lambda *, shutdown_requested=None, stage_span_recorder=None: _build_prefetched_factory_site_parser(
                                        http_client=http_client,
                                        output_dir=output_dir,
                                        stage_governor=downstream_stage_governor,
                                        shutdown_requested=shutdown_requested,
                                        stage_span_recorder=stage_span_recorder,
                                    )
                                ),
                                stage_governor=downstream_stage_governor,
                                stage_span_recorder=_make_downstream_stage_span_recorder(
                                    event_sink=http_client.progress_store.append_event,
                                    row=row,
                                ),
                            )
                        )
                        if prefetch_covers_full_source_contour
                        else None
                    ),
                    prepare_downstream_host_resolver=(
                        _resolve_prefetched_aggregator_site_hosts
                        if prefetch_covers_full_source_contour
                        else None
                    ),
                    downstream_host_ledger=direct_default_host_ledger if prefetch_covers_full_source_contour else None,
                    source_stage_governor=source_stage_governor,
                    source_lane_telemetry=source_lane_telemetry,
                    max_ready_queue_depth=int(
                        (backpressure_policy.get("direct_default_prefetch") or {}).get("ready_queue_limit", 1) or 1
                    ),
                    wait_callback=service_runtime_tail_during_source_wait,
                )
                capture_runtime_throughput()
                if not prefetch_covers_full_source_contour and worker_count > 1:
                    downstream_prefetch_buffered_progress = direct_default_executor.buffered_progress_store
                    downstream_prefetch_executor = ThreadPoolExecutor(
                        max_workers=worker_count,
                        thread_name_prefix="company-downstream",
                    )
                    logger.info(
                        "Company downstream prefetch activated: workers=%s queue_limit=%s ready_drain_limit=%s ready_drain_low_watermark=%s",
                        worker_count,
                        downstream_prefetch_queue_limit,
                        downstream_prefetch_ready_drain_limit,
                        downstream_prefetch_ready_drain_low_watermark,
                    )
        if checko_worker_lane_budget > 0 and "checko" in source_registry:
            checko_worker_lane_rows = list(source_search_rows)
            if checko_worker_lane_rows:
                checko_worker_count = min(checko_worker_lane_budget, len(checko_worker_lane_rows))
                logger.info(
                    "Checko worker lane activated: workers=%s companies=%s",
                    checko_worker_count,
                    len(checko_worker_lane_rows),
                )
                checko_worker_lane_executor = open_company_source_search_executor(
                    rows=checko_worker_lane_rows,
                    sources=[source_registry["checko"]],
                    shared_client=http_client,
                    worker_count=checko_worker_count,
                    source_stage_governor=source_stage_governor,
                    source_lane_telemetry=source_lane_telemetry,
                    max_ready_queue_depth=checko_worker_count,
                    wait_callback=service_runtime_tail_during_source_wait,
                )
                capture_runtime_throughput()

        for idx, row in enumerate(filtered_rows, start=1):
            terminal_context_payload = _terminal_context(checkpoint="before_company_row", row=row)
            if controlled_stop_request is None:
                controlled_stop_request = _consume_controlled_stop_request(
                    progress=progress,
                    logger=logger,
                    checkpoint="before_company_row",
                    row=row,
                )
            if controlled_stop_request is not None:
                break
            existing_payload = progress.get(row.inn)
            retry_deferred_source_names = (
                retry_deferred_source_names_by_inn.get(core.normalize_whitespace(str(row.inn or "")), ())
                if args.retry_deferred_required_sources
                else ()
            )
            row_active_source_names = (
                list(retry_deferred_source_names)
                if args.retry_deferred_required_sources
                else list(active_source_names)
            )
            resume_recovery = args.resume and row.inn in resume_pending_explicit_inns
            if (
                args.resume
                and not resume_recovery
                and core.should_skip_on_resume(
                    existing_payload,
                    active_source_names,
                    retry_blocked_source=args.retry_blocked_source,
                )
            ):
                logger.info(
                    "[%s/%s] Пропускаю ИНН %s, выбранные источники уже есть в runtime state",
                    idx,
                    len(filtered_rows),
                    row.inn,
                )
                continue

            logger.info("[%s/%s] Обработка ИНН=%s | %s", idx, len(filtered_rows), row.inn, row.company_name)
            if existing_payload:
                result = core.company_result_from_dict(existing_payload)
                result.input_site = row.xlsx_site
                result.input_phone = row.xlsx_phone
                result.input_comment = row.comment
                result.status = "running"
                result.notes.append(f"Результат обновлен {core.utc_now_iso()} для источников: {', '.join(row_active_source_names)}")
            else:
                result = core.build_company_result(row)

            source_results = dict(result.sources)
            refreshed_source_names: list[str] = []
            prefetched_source_batches: list[PrefetchedCompanySourceBatch] = []
            prefetched_aggregator_execution: PrefetchedAggregatorSiteExecution | None = None
            prefetched_company_downstream_execution: PrefetchedCompanyDownstreamExecution | None = None
            if direct_default_executor is not None and direct_default_executor.contains(row):
                terminal_context_payload = _terminal_context(checkpoint="prefetched_source_handoff", row=row)
                prefetched_source_batches.append(direct_default_executor.take(row))
            if checko_worker_lane_executor is not None and checko_worker_lane_executor.contains(row):
                terminal_context_payload = _terminal_context(checkpoint="prefetched_checko_handoff", row=row)
                prefetched_source_batches.append(checko_worker_lane_executor.take(row))
            for prefetched_source_batch in prefetched_source_batches:
                for runtime_event in prefetched_source_batch.runtime_events:
                    progress.append_event(runtime_event)
                if isinstance(prefetched_source_batch.prepared_downstream, PrefetchedCompanyDownstreamExecution):
                    prefetched_company_downstream_execution = prefetched_source_batch.prepared_downstream
                    prefetched_aggregator_execution = (
                        prefetched_company_downstream_execution.aggregator_execution
                    )
                elif isinstance(prefetched_source_batch.prepared_downstream, PrefetchedAggregatorSiteExecution):
                    prefetched_aggregator_execution = prefetched_source_batch.prepared_downstream
            deferred_required_source_for_row = False
            prefetched_source_results: dict[str, core.SourceResult] = {}
            prefetched_source_durations: dict[str, float] = {}
            prefetched_source_runtime_events: dict[str, list[dict[str, object]]] = {}
            prefetched_downstream_runtime_events: list[dict[str, object]] = []
            for prefetched_source_batch in prefetched_source_batches:
                prefetched_source_results.update(prefetched_source_batch.source_results)
                prefetched_source_durations.update(prefetched_source_batch.source_durations)
                for runtime_event in prefetched_source_batch.runtime_events:
                    runtime_event_source = core.normalize_whitespace(str(runtime_event.get("source") or ""))
                    if not runtime_event_source:
                        continue
                    prefetched_source_runtime_events.setdefault(runtime_event_source, []).append(dict(runtime_event))
                prefetched_downstream_runtime_events.extend(prefetched_source_batch.downstream_runtime_events)
            capture_runtime_throughput()
            terminal_context_payload = _terminal_context(checkpoint="source_collect", row=row)
            for source in ([] if resume_recovery else active_sources):
                if args.retry_deferred_required_sources and source.source_name not in retry_deferred_source_names:
                    continue
                source_mode = source_modes.get(source.source_name, "")
                if source.source_name in disabled_sources_for_run:
                    source_results[source.source_name] = core.make_blocked_source_result(
                        source.source_name,
                        f"Источник отключен до конца текущего прогона: {disabled_sources_for_run[source.source_name]}",
                    )
                    logger.info("  источник=%s пропущен до конца прогона", source.source_name)
                    continue

                if source.source_name in prefetched_source_results:
                    source_result = prefetched_source_results[source.source_name]
                    duration = prefetched_source_durations[source.source_name]
                else:
                    service_stale_ready_tail_before_inline_source_wait()
                    if controlled_stop_request is not None:
                        break
                    with _stage_pool_context(
                        source_stage_governor,
                        stage_name=source.source_name,
                        telemetry_tick=capture_runtime_throughput,
                    ):
                        source_lane_telemetry.mark_started(source.source_name)
                        started = time.time()
                        source_result = source.search(row)
                        duration = round(time.time() - started, 2)
                if not core.normalize_whitespace(str(source_result.source or "")):
                    source_result.source = source.source_name
                source_results[source.source_name] = source_result
                refreshed_source_names.append(source.source_name)
                logger.info("  источник=%s статус=%s длительность=%.2fs", source.source_name, source_result.status, duration)
                progress.emit_stage_message(
                    message_type="source_result_ready",
                    stage="source_collect",
                    inn=row.inn,
                    row_index=row.row_index,
                    payload={
                        "source": source_result.source or source.source_name,
                        "status": source_result.status,
                        "duration_seconds": duration,
                    },
                )
                progress.materialize_unread_stage_handoffs()
                if duration > args.source_budget_seconds:
                    logger.warning("  источник=%s превысил бюджет %.1fs", source.source_name, args.source_budget_seconds)
                    progress.append_event(
                        {
                            "ts": core.utc_now_iso(),
                            "type": "source_budget_warning",
                            "source": source.source_name,
                            "inn": row.inn,
                            "duration_seconds": duration,
                            **_source_budget_pressure_payload(
                                source_name=source.source_name,
                                duration_seconds=duration,
                                budget_seconds=float(args.source_budget_seconds),
                                runtime_events=prefetched_source_runtime_events.get(source.source_name, []),
                            ),
                        }
                    )
                source_access_mode = execution_guardrails.source_transport_policy.get(source.source_name, "")
                if core.source_result_requires_run_fail_fast(
                    source.source_name,
                    source_result.status,
                    access_mode=source_access_mode,
                ):
                    terminal_context_payload = _terminal_context(
                        checkpoint="source_collect",
                        row=row,
                        source_name=source.source_name,
                        source_status=source_result.status,
                        source_access_mode=source_access_mode,
                    )
                deferred_required_source_for_row = _handle_required_source_runtime_outcome(
                    progress=progress,
                    row=row,
                    source_result=source_result,
                    source_access_mode=source_access_mode,
                    defer_required_source_transients=bool(
                        args.defer_required_source_transients or args.retry_deferred_required_sources
                    ),
                    selected_rows=len(filtered_rows),
                    retry_existing_deferred=bool(
                        args.retry_deferred_required_sources
                        and source.source_name in retry_deferred_source_names
                    ),
                )
                if deferred_required_source_for_row:
                    logger.warning(
                        "  источник=%s deferred required-source transient for INN=%s status=%s",
                        source.source_name,
                        row.inn,
                        source_result.status,
                    )
                    break
                if core.should_disable_source_for_run(
                    source_result.status,
                    live_mode=source_mode == "live",
                    offline_mode=source_mode == LIST_ORG_OFFLINE_MODE,
                ):
                    disabled_sources_for_run[source.source_name] = core.resolve_source_block_reason(source_result)
                    logger.warning(
                        "  источник=%s отключен до конца прогона после статуса=%s",
                        source.source_name,
                        source_result.status,
                    )
                    progress.append_event(
                        {
                            "ts": core.utc_now_iso(),
                            "type": "source_disabled_for_run",
                            "source": source.source_name,
                            "inn": row.inn,
                            "reason": disabled_sources_for_run[source.source_name],
                        }
                    )

            if controlled_stop_request is not None:
                break
            if deferred_required_source_for_row:
                capture_runtime_throughput()
                continue
            if "list_org" in source_results:
                source_results["list_org"] = normalize_list_org_offline_result(
                    source_results["list_org"],
                    snapshot_path=list_org_json_file,
                    refreshed="list_org" in refreshed_source_names,
                )
            capture_runtime_throughput()
            result.sources = source_results
            result.merged_contacts = core.merge_contacts(source_results, row)
            result.domain_resolution = build_domain_resolution(row, source_results, result.merged_contacts)
            if resume_recovery:
                candidate_site_stage_payloads = []
            elif prefetched_aggregator_execution is not None:
                candidate_site_stage_payloads = [
                    dict(payload) for payload in prefetched_aggregator_execution.candidate_site_payloads
                ]
            else:
                with _stage_pool_context(
                    downstream_stage_governor,
                    stage_name=CANDIDATE_SITE_STAGE_NAME,
                    telemetry_tick=capture_runtime_throughput,
                    stage_span_recorder=_make_downstream_stage_span_recorder(
                        event_sink=progress.append_event,
                        row=row,
                        execution_boundary=AGGREGATOR_SITE_EXECUTION_BOUNDARY,
                    ),
                ):
                    candidate_site_stage_payloads = _iter_candidate_site_stage_payloads(
                        choose_candidate_sites(row, result.merged_contacts, result.domain_resolution),
                        result.domain_resolution,
                    )
            for payload in candidate_site_stage_payloads:
                progress.emit_stage_message(
                    message_type="candidate_site_found",
                    stage="candidate_site_selection",
                    inn=row.inn,
                    row_index=row.row_index,
                    payload=payload,
                )
            if not resume_recovery:
                progress.materialize_unread_stage_handoffs()
                terminal_context_payload = _terminal_context(
                    checkpoint="materialize_explicit_work_unit",
                    row=row,
                    execution_boundary=AGGREGATOR_SITE_EXECUTION_BOUNDARY,
                )
                aggregator_site_work_unit = progress.materialize_stage_work_unit(
                    inn=row.inn,
                    row_index=row.row_index,
                    execution_boundary=AGGREGATOR_SITE_EXECUTION_BOUNDARY,
                    work_unit_payload=_build_aggregator_site_work_unit_payload(
                        row=row,
                        source_results=source_results,
                        candidate_site_payloads=candidate_site_stage_payloads,
                        updated_source_names=refreshed_source_names,
                        source_lane_scheduler=source_lane_scheduler,
                        downstream_worker_pools=downstream_worker_pool_payload,
                    ),
                )
                if not aggregator_site_work_unit:
                    raise RuntimeError(f"Failed to materialize aggregator/site work unit for INN {row.inn}")
                logger.info(
                    "  aggregator_site_queue materialized fingerprint=%s status=%s candidate_sites=%s",
                    aggregator_site_work_unit.get("handoff_fingerprint", ""),
                    aggregator_site_work_unit.get("work_status", ""),
                    len(_candidate_sites_from_stage_work_unit(aggregator_site_work_unit)),
                )
                capture_runtime_throughput()
            else:
                pending_work_unit = _pending_selected_explicit_stage_work_units(progress=progress, rows=[row]).get(row.inn)
                logger.info(
                    "  resume_recovery uses pending runtime work unit boundary=%s",
                    str((pending_work_unit or {}).get("execution_boundary") or ""),
                )
            pending_row = PendingExplicitRuntimeRow(
                row=row,
                existing_payload=existing_payload,
                resume_recovery=resume_recovery,
                refreshed_source_names=tuple(refreshed_source_names),
                active_source_names=tuple(row_active_source_names),
                deferred_required_source_retry_names=tuple(retry_deferred_source_names),
                prefetched_aggregator_execution=prefetched_aggregator_execution,
                prefetched_completed_result_payload=(
                    _clone_json_like(prefetched_company_downstream_execution.completed_result_payload)
                    if prefetched_company_downstream_execution is not None
                    and isinstance(prefetched_company_downstream_execution.completed_result_payload, dict)
                    else None
                ),
                prefetched_runtime_events=(
                    tuple(prefetched_downstream_runtime_events)
                ),
                downstream_ready_at=core.utc_now_iso() if prefetched_company_downstream_execution is not None else "",
            )
            if (
                downstream_prefetch_executor is not None
                and downstream_prefetch_buffered_progress is not None
                and not resume_recovery
                and prefetched_company_downstream_execution is None
            ):
                pending_row.downstream_future = _submit_prefetched_company_downstream_batch(
                    executor=downstream_prefetch_executor,
                    row=row,
                    source_results=source_results,
                    active_source_names=row_active_source_names,
                    analyzer=analyzer,
                    factory_site_parser_factory=(
                        lambda *, shutdown_requested=None, stage_span_recorder=None: _build_prefetched_factory_site_parser(
                            http_client=http_client,
                            output_dir=output_dir,
                            stage_governor=downstream_stage_governor,
                            shutdown_requested=shutdown_requested,
                            telemetry_tick=capture_runtime_throughput,
                            stage_span_recorder=stage_span_recorder,
                        )
                    ),
                    buffered_progress_store=downstream_prefetch_buffered_progress,
                    shutdown_requested=downstream_prefetch_shutdown.is_set,
                    stage_governor=downstream_stage_governor,
                    telemetry_tick=capture_runtime_throughput,
                    stage_span_recorder=_make_downstream_stage_span_recorder(
                        event_sink=downstream_prefetch_buffered_progress.append_event,
                        row=row,
                    ),
                )
                _mark_pending_downstream_ready(pending_row)

            if downstream_prefetch_executor is not None and not resume_recovery:
                pending_explicit_runtime_rows.append(pending_row)
                ready_downstream_count = _pending_downstream_ready_count(pending_explicit_runtime_rows)
                ready_drain_limit_reached = (
                    downstream_prefetch_ready_drain_limit > 0
                    and ready_downstream_count > downstream_prefetch_ready_drain_limit
                )
                post_materialized_ready_drain_reached = (
                    processed > 0
                    and downstream_prefetch_ready_drain_low_watermark > 0
                    and ready_downstream_count >= downstream_prefetch_ready_drain_low_watermark
                )
                ready_drain_requested = ready_drain_limit_reached or post_materialized_ready_drain_reached
                if (
                    len(pending_explicit_runtime_rows) < downstream_prefetch_queue_limit
                    and not ready_drain_requested
                ):
                    capture_runtime_throughput()
                    continue
                if ready_drain_requested or ready_downstream_count > 0:
                    while drain_ready_pending_explicit_runtime_rows():
                        if controlled_stop_request is not None:
                            break
                        capture_runtime_throughput()
                    if controlled_stop_request is not None:
                        break
                    capture_runtime_throughput()
                    continue
                pending_row = _pop_next_pending_explicit_runtime_row(
                    pending_explicit_runtime_rows,
                    prefer_ready=True,
                )

            processed, controlled_stop_request, terminal_context_payload = _drain_pending_explicit_runtime_row(
                pending_row=pending_row,
                progress=progress,
                controlled_stop_request=controlled_stop_request,
                active_source_names=active_source_names,
                analyzer=analyzer,
                factory_site_parser=factory_site_parser,
                downstream_stage_governor=downstream_stage_governor,
                total_rows=len(filtered_rows),
                processed_rows=processed,
                logger=logger,
                buffered_progress_store=downstream_prefetch_buffered_progress,
                telemetry_tick=capture_runtime_throughput,
            )
            if controlled_stop_request is not None:
                break

        while pending_explicit_runtime_rows and controlled_stop_request is None:
            pending_row = _pop_next_pending_explicit_runtime_row(
                pending_explicit_runtime_rows,
                prefer_ready=True,
            )
            processed, controlled_stop_request, terminal_context_payload = _drain_pending_explicit_runtime_row(
                pending_row=pending_row,
                progress=progress,
                controlled_stop_request=controlled_stop_request,
                active_source_names=active_source_names,
                analyzer=analyzer,
                factory_site_parser=factory_site_parser,
                downstream_stage_governor=downstream_stage_governor,
                total_rows=len(filtered_rows),
                processed_rows=processed,
                logger=logger,
                buffered_progress_store=downstream_prefetch_buffered_progress,
                telemetry_tick=capture_runtime_throughput,
            )

        if direct_default_executor is not None and controlled_stop_request is None:
            terminal_context_payload = _terminal_context(checkpoint="final_executor_drain")
            direct_default_executor.ensure_drained()
            capture_runtime_throughput()
        if checko_worker_lane_executor is not None and controlled_stop_request is None:
            terminal_context_payload = _terminal_context(checkpoint="final_checko_lane_drain")
            checko_worker_lane_executor.ensure_drained()
            capture_runtime_throughput()
    except BaseException as exc:
        terminal_exception = exc
        terminal_traceback = exc.__traceback__
        terminal_context_from_exc = getattr(exc, "_terminal_context_payload", None)
        if isinstance(terminal_context_from_exc, dict):
            terminal_context_payload = dict(terminal_context_from_exc)
    finally:
        try:
            _cleanup_runtime_resources(
                logger=logger,
                source_lane_executors=(direct_default_executor, checko_worker_lane_executor),
                downstream_prefetch_executor=downstream_prefetch_executor,
                downstream_prefetch_shutdown=downstream_prefetch_shutdown,
                http_client=http_client,
            )
        except BaseException as cleanup_exc:
            if terminal_exception is None:
                terminal_exception = cleanup_exc
                terminal_traceback = cleanup_exc.__traceback__
            else:
                logger.exception("Runtime cleanup failed after terminal exception")

        if (
            terminal_exception is None
            and bool(args.defer_required_source_transients)
            and not bool(args.retry_deferred_required_sources)
        ):
            final_deferred_stop = _deferred_required_source_final_stop(progress=progress)
            if final_deferred_stop is not None:
                terminal_exception, terminal_context_payload = final_deferred_stop
                terminal_traceback = terminal_exception.__traceback__

        if terminal_exception is None:
            unresolved_deferred_required_sources = (
                progress.unresolved_deferred_required_source_count()
                if bool(args.defer_required_source_transients or args.retry_deferred_required_sources)
                else 0
            )
            progress.run_finished(
                processed_rows=processed,
                controlled_stop=controlled_stop_request is not None,
                stop_request=controlled_stop_request,
                run_status=(
                    RUN_STATUS_COMPLETED_WITH_DEFERRED_REQUIRED_SOURCES
                    if controlled_stop_request is None and unresolved_deferred_required_sources
                    else None
                ),
                finish_reason=(
                    RUN_FINISH_REASON_DEFERRED_REQUIRED_SOURCES
                    if controlled_stop_request is None and unresolved_deferred_required_sources
                    else None
                ),
                terminal_context=terminal_context_payload,
            )
            capture_runtime_throughput()
        else:
            if _is_required_source_terminal_exception(terminal_exception):
                progress.run_finished(
                    processed_rows=processed,
                    run_status=RUN_STATUS_FAILED_REQUIRED_SOURCE,
                    finish_reason=RUN_FINISH_REASON_REQUIRED_SOURCE,
                    stop_request=_required_source_stop_request(terminal_exception),
                    terminal_context=terminal_context_payload,
                    terminal_error=_terminal_error_payload(terminal_exception),
                )
            else:
                cancelled_terminal = _is_cancelled_terminal_exception(terminal_exception)
                progress.run_finished(
                    processed_rows=processed,
                    run_status=RUN_STATUS_CANCELLED if cancelled_terminal else RUN_STATUS_ABORTED,
                    finish_reason=RUN_FINISH_REASON_CANCELLED if cancelled_terminal else RUN_FINISH_REASON_ABORTED,
                    terminal_context=terminal_context_payload,
                    terminal_error=_terminal_error_payload(terminal_exception),
                )
            capture_runtime_throughput()

    if terminal_exception is not None:
        if _is_required_source_terminal_exception(terminal_exception):
            logger.error("Required source fail-fast triggered: %s", terminal_exception)
            return 1
        raise terminal_exception.with_traceback(terminal_traceback)
    if controlled_stop_request is not None:
        logger.info("Controlled stop completed. Processed rows: %s", processed)
        return 0
    logger.info("Готово. Обработано строк: %s", processed)
    return 0


def main() -> int:
    return run(parse_args())


if __name__ == "__main__":
    raise SystemExit(main())

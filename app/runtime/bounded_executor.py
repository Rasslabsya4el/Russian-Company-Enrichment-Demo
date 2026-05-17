from __future__ import annotations

import time
from collections import deque
from collections.abc import Mapping, Sequence
from concurrent.futures import CancelledError, Future, ThreadPoolExecutor
from contextlib import contextmanager
from dataclasses import dataclass
from threading import Condition, Event, Lock, RLock, local
from typing import Any

import company_enrichment_core as core

from .concurrency import (
    DIRECT_DEFAULT_BOUNDED_SOURCE_NAMES,
    resolve_effective_company_concurrency_cap,
)

DIRECT_DEFAULT_EXECUTOR_SOURCE_NAMES = DIRECT_DEFAULT_BOUNDED_SOURCE_NAMES
SOURCE_SEARCH_PHASE_KEY = "source_search"
DOWNSTREAM_EXECUTION_PHASE_KEY = "downstream_execution"


@dataclass(frozen=True, slots=True)
class DirectDefaultBoundedExecutorPlan:
    enabled: bool
    max_workers: int
    active_sources: tuple[str, ...]
    reason: str = ""


@dataclass(frozen=True, slots=True)
class PrefetchedCompanySourceBatch:
    row_index: int
    inn: str
    source_results: dict[str, core.SourceResult]
    source_durations: dict[str, float]
    source_started_at: dict[str, str]
    source_finished_at: dict[str, str]
    runtime_events: tuple[dict[str, Any], ...]
    downstream_runtime_events: tuple[dict[str, Any], ...] = ()
    prepared_downstream: Any | None = None


class _ThreadBoundEventBuffer:
    def __init__(self, fallback_progress_store: Any) -> None:
        self._events_by_task: dict[tuple[str, str], list[dict[str, Any]]] = {}
        self._fallback_progress_store = fallback_progress_store
        self._lock = Lock()
        self._local = local()

    @contextmanager
    def bind(self, task_key: str, *, phase_key: str = ""):
        previous_task_key = getattr(self._local, "task_key", "")
        previous_phase_key = getattr(self._local, "phase_key", "")
        self._local.task_key = task_key
        self._local.phase_key = str(phase_key or "")
        try:
            yield
        finally:
            self._local.task_key = previous_task_key
            self._local.phase_key = previous_phase_key

    def append_event(self, event: dict[str, Any]) -> None:
        task_key = str(getattr(self._local, "task_key", "") or "").strip()
        if not task_key:
            fallback_append = getattr(self._fallback_progress_store, "append_event", None)
            if not callable(fallback_append):
                raise RuntimeError("Buffered bounded executor fallback progress store has no append_event")
            fallback_append(dict(event))
            return
        phase_key = str(getattr(self._local, "phase_key", "") or "")
        with self._lock:
            bucket = self._events_by_task.setdefault((task_key, phase_key), [])
            bucket.append(dict(event))

    def __getattr__(self, name: str) -> Any:
        return getattr(self._fallback_progress_store, name)

    def take(self, task_key: str, *, phase_key: str = "") -> tuple[dict[str, Any], ...]:
        with self._lock:
            events = tuple(self._events_by_task.pop((task_key, str(phase_key or "")), []))
        return events

    def ensure_empty(self) -> None:
        with self._lock:
            if self._events_by_task:
                raise RuntimeError("Buffered bounded executor left unconsumed runtime events behind")

    def clear(self) -> None:
        with self._lock:
            self._events_by_task.clear()


def build_company_source_batch_key(row: core.RowInput) -> tuple[int, str]:
    normalized_identity = core.normalize_whitespace(str(row.inn or ""))
    if not normalized_identity:
        normalized_identity = core.normalize_whitespace(str(row.company_name or ""))
    return int(row.row_index or 0), normalized_identity


def plan_direct_default_bounded_executor(
    *,
    active_sources: Sequence[str],
    company_concurrency_cap: int,
    per_source_lane_budget_map: Mapping[str, int] | None = None,
) -> DirectDefaultBoundedExecutorPlan:
    normalized_sources = tuple(str(source_name or "").strip() for source_name in active_sources if str(source_name or "").strip())
    eligible_sources = tuple(
        source_name
        for source_name in normalized_sources
        if source_name in DIRECT_DEFAULT_EXECUTOR_SOURCE_NAMES
    )
    if company_concurrency_cap <= 1:
        return DirectDefaultBoundedExecutorPlan(
            enabled=False,
            max_workers=1,
            active_sources=eligible_sources,
            reason="company_concurrency <= 1",
        )
    if not eligible_sources:
        return DirectDefaultBoundedExecutorPlan(
            enabled=False,
            max_workers=1,
            active_sources=eligible_sources,
            reason="no direct-default sources in contour",
        )
    effective_worker_cap = resolve_effective_company_concurrency_cap(
        active_sources=eligible_sources,
        requested_company_concurrency=company_concurrency_cap,
        per_source_lane_budget_map=dict(per_source_lane_budget_map or {}),
    )
    if effective_worker_cap <= 1:
        return DirectDefaultBoundedExecutorPlan(
            enabled=False,
            max_workers=1,
            active_sources=eligible_sources,
            reason="effective lane budget <= 1",
        )
    return DirectDefaultBoundedExecutorPlan(
        enabled=True,
        max_workers=max(int(effective_worker_cap), 1),
        active_sources=eligible_sources,
    )


class RollingCompanySourceBatchExecutor:
    def __init__(
        self,
        *,
        rows: Sequence[core.RowInput],
        sources: Sequence[Any],
        shared_client: Any,
        worker_count: int,
        prepare_downstream: Any | None = None,
        prepare_downstream_host_resolver: Any | None = None,
        downstream_host_ledger: Any | None = None,
        source_stage_governor: Any | None = None,
        source_lane_telemetry: Any | None = None,
        max_ready_queue_depth: int | None = None,
        wait_callback: Any | None = None,
    ) -> None:
        if worker_count < 1:
            raise ValueError("worker_count must be >= 1")
        if not hasattr(shared_client, "progress_store"):
            raise ValueError("shared_client must expose progress_store for bounded source-search buffering")

        self._rows = tuple(rows)
        self._sources = tuple(sources)
        row_keys = tuple(build_company_source_batch_key(row) for row in self._rows)
        if len(set(row_keys)) != len(row_keys):
            raise ValueError("rows must resolve to unique bounded executor batch keys")
        self._row_key_set = set(row_keys)
        self._remaining_rows = deque(self._rows)
        self._worker_count = worker_count
        self._shared_client = shared_client
        self._prepare_downstream = prepare_downstream
        self._prepare_downstream_host_resolver = prepare_downstream_host_resolver
        self._downstream_host_ledger = downstream_host_ledger
        self._source_stage_governor = source_stage_governor
        self._source_lane_telemetry = source_lane_telemetry
        self._active_source_names = tuple(
            str(getattr(source, "source_name", "") or "").strip()
            for source in self._sources
            if str(getattr(source, "source_name", "") or "").strip()
        )
        self._max_ready_queue_depth = max(int(max_ready_queue_depth or worker_count), 1)
        self._wait_callback = wait_callback
        self._wait_poll_seconds = 0.05
        self._original_progress_store = shared_client.progress_store
        self._buffered_progress = _ThreadBoundEventBuffer(self._original_progress_store)
        self._condition = Condition(RLock())
        self._ready_batches: dict[tuple[int, str], PrefetchedCompanySourceBatch] = {}
        self._future_to_row_key: dict[Future[PrefetchedCompanySourceBatch], tuple[int, str]] = {}
        self._consumed_row_keys: set[tuple[int, str]] = set()
        self._error: BaseException | None = None
        self._completed = False
        self._closed = False
        self._shutdown_requested = Event()
        self._shared_client.progress_store = self._buffered_progress
        try:
            self._executor = ThreadPoolExecutor(
                max_workers=worker_count,
                thread_name_prefix="direct-default-source",
            )
            with self._condition:
                while len(self._future_to_row_key) < self._worker_count and self._remaining_rows:
                    self._submit_next_locked()
                if not self._future_to_row_key and not self._remaining_rows:
                    self._completed = True
                self._update_backpressure_locked()
        except Exception:
            self._shared_client.progress_store = self._original_progress_store
            raise

    def contains(self, row: core.RowInput) -> bool:
        return build_company_source_batch_key(row) in self._row_key_set

    @property
    def buffered_progress_store(self) -> Any:
        return self._buffered_progress

    def take(self, row: core.RowInput) -> PrefetchedCompanySourceBatch:
        row_key = build_company_source_batch_key(row)
        if row_key not in self._row_key_set:
            raise KeyError(f"Direct-default bounded executor has no scheduled batch for row {row_key}")
        with self._condition:
            if row_key in self._consumed_row_keys:
                raise RuntimeError(f"Direct-default bounded executor batch already consumed for row {row_key}")
            while True:
                if self._error is not None:
                    raise RuntimeError("Direct-default bounded executor failed during rolling handoff") from self._error
                batch = self._ready_batches.pop(row_key, None)
                if batch is not None:
                    self._consumed_row_keys.add(row_key)
                    while (
                        len(self._future_to_row_key) < self._worker_count
                        and self._remaining_rows
                        and len(self._ready_batches) < self._max_ready_queue_depth
                        and not self._closed
                        and self._error is None
                    ):
                        self._submit_next_locked()
                    self._update_backpressure_locked()
                    return batch
                if self._completed:
                    raise RuntimeError(f"Missing direct-default bounded executor batch for row {row_key}")
                wait_callback = self._wait_callback if callable(self._wait_callback) else None
                wait_timeout = self._wait_poll_seconds if wait_callback is not None else None
                self._condition.wait(timeout=wait_timeout)
                if wait_callback is not None:
                    self._condition.release()
                    try:
                        wait_callback()
                    finally:
                        self._condition.acquire()

    def take_ready(
        self,
        *,
        exclude_rows: Sequence[core.RowInput] | None = None,
    ) -> PrefetchedCompanySourceBatch | None:
        exclude_keys = {
            build_company_source_batch_key(row)
            for row in (exclude_rows or ())
        }
        with self._condition:
            if self._error is not None:
                raise RuntimeError("Direct-default bounded executor failed during rolling handoff") from self._error
            ready_key: tuple[int, str] | None = None
            for row_key in self._ready_batches:
                if row_key in exclude_keys:
                    continue
                ready_key = row_key
                break
            if ready_key is None:
                return None
            batch = self._ready_batches.pop(ready_key)
            self._consumed_row_keys.add(ready_key)
            while (
                len(self._future_to_row_key) < self._worker_count
                and self._remaining_rows
                and len(self._ready_batches) < self._max_ready_queue_depth
                and not self._closed
                and self._error is None
            ):
                self._submit_next_locked()
            self._update_backpressure_locked()
            return batch

    def ensure_drained(self) -> None:
        with self._condition:
            while not self._completed and self._error is None:
                self._condition.wait()
            if self._error is not None:
                raise RuntimeError("Direct-default bounded executor failed before all batches were consumed") from self._error
            if self._ready_batches:
                raise RuntimeError(
                    f"Direct-default bounded executor left unconsumed source batches: {len(self._ready_batches)}"
                )
            if len(self._consumed_row_keys) != len(self._row_key_set):
                raise RuntimeError(
                    "Direct-default bounded executor closed before all scheduled rows were consumed"
                )
        self._buffered_progress.ensure_empty()

    def close(self) -> None:
        if self._closed:
            return
        with self._condition:
            self._closed = True
            self._shutdown_requested.set()
            self._ready_batches.clear()
            self._update_backpressure_locked()
            self._condition.notify_all()
        try:
            self._executor.shutdown(wait=True, cancel_futures=True)
        finally:
            self._buffered_progress.clear()
            self._shared_client.progress_store = self._original_progress_store

    def _submit_next_locked(self) -> None:
        row = self._remaining_rows.popleft()
        future = self._executor.submit(
            _search_company_sources,
            row=row,
            sources=self._sources,
            buffered_progress=self._buffered_progress,
            prepare_downstream=self._prepare_downstream,
            prepare_downstream_host_resolver=self._prepare_downstream_host_resolver,
            downstream_host_ledger=self._downstream_host_ledger,
            source_stage_governor=self._source_stage_governor,
            source_lane_telemetry=self._source_lane_telemetry,
            shutdown_requested=self._shutdown_requested.is_set,
        )
        self._future_to_row_key[future] = build_company_source_batch_key(row)
        future.add_done_callback(self._handle_future_completion)
        self._update_backpressure_locked()

    def _handle_future_completion(self, future: Future[PrefetchedCompanySourceBatch]) -> None:
        with self._condition:
            row_key = self._future_to_row_key.pop(future, None)
            if row_key is None:
                return
            try:
                batch = future.result()
            except CancelledError:
                if not self._closed and self._error is None:
                    self._error = RuntimeError(
                        f"Direct-default bounded executor task cancelled unexpectedly for row {row_key}"
                    )
            except BaseException as exc:
                if self._error is None:
                    self._error = exc
            else:
                if not self._closed:
                    self._ready_batches[row_key] = batch
            if self._error is None and not self._closed:
                while (
                    len(self._future_to_row_key) < self._worker_count
                    and self._remaining_rows
                    and len(self._ready_batches) < self._max_ready_queue_depth
                ):
                    self._submit_next_locked()
                if self._remaining_rows and len(self._ready_batches) >= self._max_ready_queue_depth:
                    self._update_backpressure_locked(
                        active=True,
                        reason="ready_queue_limit",
                        blocked_submissions_delta=1,
                    )
            if not self._future_to_row_key and not self._remaining_rows:
                self._completed = True
            self._update_backpressure_locked()
            self._condition.notify_all()

    def _update_backpressure_locked(
        self,
        *,
        active: bool | None = None,
        reason: str = "",
        blocked_submissions_delta: int = 0,
    ) -> None:
        if self._source_lane_telemetry is None or not self._active_source_names:
            return
        ready_queue_depth = len(self._ready_batches)
        is_active = ready_queue_depth >= self._max_ready_queue_depth if active is None else bool(active)
        resolved_reason = reason
        if is_active and not resolved_reason:
            resolved_reason = "ready_queue_limit" if ready_queue_depth >= self._max_ready_queue_depth else ""
        self._source_lane_telemetry.update_backpressure(
            list(self._active_source_names),
            active=is_active,
            reason=resolved_reason if is_active else "",
            ready_queue_depth=ready_queue_depth,
            ready_queue_limit=self._max_ready_queue_depth,
            blocked_submissions_delta=blocked_submissions_delta,
        )


def open_company_source_search_executor(
    *,
    rows: Sequence[core.RowInput],
    sources: Sequence[Any],
    shared_client: Any,
    worker_count: int,
    prepare_downstream: Any | None = None,
    prepare_downstream_host_resolver: Any | None = None,
    downstream_host_ledger: Any | None = None,
    source_stage_governor: Any | None = None,
    source_lane_telemetry: Any | None = None,
    max_ready_queue_depth: int | None = None,
    wait_callback: Any | None = None,
) -> RollingCompanySourceBatchExecutor:
    return RollingCompanySourceBatchExecutor(
        rows=rows,
        sources=sources,
        shared_client=shared_client,
        worker_count=worker_count,
        prepare_downstream=prepare_downstream,
        prepare_downstream_host_resolver=prepare_downstream_host_resolver,
        downstream_host_ledger=downstream_host_ledger,
        source_stage_governor=source_stage_governor,
        source_lane_telemetry=source_lane_telemetry,
        max_ready_queue_depth=max_ready_queue_depth,
        wait_callback=wait_callback,
    )


def execute_company_source_search_batch(
    *,
    rows: Sequence[core.RowInput],
    sources: Sequence[Any],
    shared_client: Any,
    worker_count: int,
    prepare_downstream: Any | None = None,
    prepare_downstream_host_resolver: Any | None = None,
    downstream_host_ledger: Any | None = None,
    source_stage_governor: Any | None = None,
    source_lane_telemetry: Any | None = None,
    max_ready_queue_depth: int | None = None,
    wait_callback: Any | None = None,
) -> dict[tuple[int, str], PrefetchedCompanySourceBatch]:
    if not rows:
        return {}
    rolling_executor = open_company_source_search_executor(
        rows=rows,
        sources=sources,
        shared_client=shared_client,
        worker_count=worker_count,
        prepare_downstream=prepare_downstream,
        prepare_downstream_host_resolver=prepare_downstream_host_resolver,
        downstream_host_ledger=downstream_host_ledger,
        source_stage_governor=source_stage_governor,
        source_lane_telemetry=source_lane_telemetry,
        max_ready_queue_depth=max_ready_queue_depth,
        wait_callback=wait_callback,
    )
    try:
        source_batch_by_row = {
            build_company_source_batch_key(row): rolling_executor.take(row)
            for row in rows
        }
        rolling_executor.ensure_drained()
        return source_batch_by_row
    finally:
        rolling_executor.close()


def _search_company_sources(
    *,
    row: core.RowInput,
    sources: Sequence[Any],
    buffered_progress: _ThreadBoundEventBuffer,
    prepare_downstream: Any | None = None,
    prepare_downstream_host_resolver: Any | None = None,
    downstream_host_ledger: Any | None = None,
    source_stage_governor: Any | None = None,
    source_lane_telemetry: Any | None = None,
    shutdown_requested: Any | None = None,
) -> PrefetchedCompanySourceBatch:
    task_key = _task_key_for_row(row)
    source_results: dict[str, core.SourceResult] = {}
    source_durations: dict[str, float] = {}
    source_started_at: dict[str, str] = {}
    source_finished_at: dict[str, str] = {}
    with buffered_progress.bind(task_key, phase_key=SOURCE_SEARCH_PHASE_KEY):
        for source in sources:
            if callable(shutdown_requested) and shutdown_requested():
                raise CancelledError(f"bounded executor shutdown requested before source search for {task_key}")
            if source_stage_governor is None:
                if source_lane_telemetry is not None:
                    source_lane_telemetry.mark_started(source.source_name)
                source_started_at[source.source_name] = core.utc_now_iso()
                started = time.time()
                source_results[source.source_name] = source.search(row)
                source_durations[source.source_name] = round(time.time() - started, 2)
                source_finished_at[source.source_name] = core.utc_now_iso()
                continue
            with source_stage_governor.lease(
                str(getattr(source, "source_name", "") or ""),
                cancel_requested=shutdown_requested,
            ):
                if source_lane_telemetry is not None:
                    source_lane_telemetry.mark_started(source.source_name)
                source_started_at[source.source_name] = core.utc_now_iso()
                started = time.time()
                source_results[source.source_name] = source.search(row)
                source_durations[source.source_name] = round(time.time() - started, 2)
                source_finished_at[source.source_name] = core.utc_now_iso()
    prepared_downstream = None
    downstream_runtime_events: tuple[dict[str, Any], ...] = ()
    if prepare_downstream is not None:
        if callable(shutdown_requested) and shutdown_requested():
            raise CancelledError(f"bounded executor shutdown requested before downstream prep for {task_key}")
        downstream_hosts: tuple[str, ...] = ()
        if prepare_downstream_host_resolver is not None:
            resolved_hosts = prepare_downstream_host_resolver(
                row=row,
                source_results=dict(source_results),
            )
            if isinstance(resolved_hosts, Sequence) and not isinstance(resolved_hosts, (str, bytes)):
                downstream_hosts = tuple(
                    sorted(
                        {
                            str(item or "").strip().lower()
                            for item in resolved_hosts
                            if str(item or "").strip()
                        }
                    )
                )
        acquired_hosts: tuple[str, ...] = ()
        if downstream_host_ledger is not None and downstream_hosts:
            acquired_hosts = downstream_host_ledger.acquire(
                downstream_hosts,
                cancel_requested=shutdown_requested,
            )
            if callable(shutdown_requested) and shutdown_requested():
                raise CancelledError(f"bounded executor shutdown requested during host-governor wait for {task_key}")
        try:
            with buffered_progress.bind(task_key, phase_key=DOWNSTREAM_EXECUTION_PHASE_KEY):
                if callable(shutdown_requested) and shutdown_requested():
                    raise CancelledError(f"bounded executor shutdown requested before downstream execution for {task_key}")
                prepared_downstream = prepare_downstream(
                    row=row,
                    source_results=dict(source_results),
                )
        finally:
            downstream_runtime_events = buffered_progress.take(task_key, phase_key=DOWNSTREAM_EXECUTION_PHASE_KEY)
            if downstream_host_ledger is not None and acquired_hosts:
                downstream_host_ledger.release(
                    acquired_hosts,
                    runtime_events=downstream_runtime_events,
                )
    return PrefetchedCompanySourceBatch(
        row_index=row.row_index,
        inn=row.inn,
        source_results=source_results,
        source_durations=source_durations,
        source_started_at=source_started_at,
        source_finished_at=source_finished_at,
        runtime_events=buffered_progress.take(task_key, phase_key=SOURCE_SEARCH_PHASE_KEY),
        downstream_runtime_events=downstream_runtime_events,
        prepared_downstream=prepared_downstream,
    )


def _task_key_for_row(row: core.RowInput) -> str:
    row_index, identity = build_company_source_batch_key(row)
    return f"{row_index}:{identity}"


__all__ = [
    "DIRECT_DEFAULT_EXECUTOR_SOURCE_NAMES",
    "DOWNSTREAM_EXECUTION_PHASE_KEY",
    "DirectDefaultBoundedExecutorPlan",
    "PrefetchedCompanySourceBatch",
    "RollingCompanySourceBatchExecutor",
    "build_company_source_batch_key",
    "execute_company_source_search_batch",
    "open_company_source_search_executor",
    "plan_direct_default_bounded_executor",
]

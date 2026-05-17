from __future__ import annotations

import time
from concurrent.futures import CancelledError
from contextlib import contextmanager
from datetime import datetime, timezone
from threading import BoundedSemaphore, Lock
from typing import Any


THROUGHPUT_TELEMETRY_CONTRACT_VERSION = 1


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _normalize_int(value: Any, *, default: int = 0) -> int:
    try:
        return max(int(value), 0)
    except (TypeError, ValueError):
        return default


def _normalize_float(value: Any, *, default: float = 0.0) -> float:
    try:
        return max(float(value), 0.0)
    except (TypeError, ValueError):
        return default


def _default_wait_pressure() -> dict[str, Any]:
    return {
        "active": False,
        "waiters": 0,
        "events": 0,
        "total_seconds": 0.0,
        "max_seconds": 0.0,
        "last_seconds": 0.0,
        "last_reason": "",
        "reasons": {},
    }


def _normalize_wait_pressure(payload: Any) -> dict[str, Any]:
    root = dict(payload) if isinstance(payload, dict) else {}
    reason_payload = root.get("reasons")
    reasons = {}
    if isinstance(reason_payload, dict):
        for raw_name, raw_count in reason_payload.items():
            reason_name = str(raw_name or "").strip()
            if not reason_name:
                continue
            reasons[reason_name] = _normalize_int(raw_count)
    waiters = _normalize_int(root.get("waiters"))
    return {
        "active": bool(root.get("active")) or waiters > 0,
        "waiters": waiters,
        "events": _normalize_int(root.get("events")),
        "total_seconds": round(_normalize_float(root.get("total_seconds")), 4),
        "max_seconds": round(_normalize_float(root.get("max_seconds")), 4),
        "last_seconds": round(_normalize_float(root.get("last_seconds")), 4),
        "last_reason": str(root.get("last_reason", "") or ""),
        "reasons": reasons,
    }


def _default_backpressure_state() -> dict[str, Any]:
    return {
        "active": False,
        "reason": "",
        "ready_queue_depth": 0,
        "ready_queue_limit": 0,
        "blocked_submissions": 0,
        "updated_at": "",
    }


def _normalize_backpressure_state(payload: Any) -> dict[str, Any]:
    root = dict(payload) if isinstance(payload, dict) else {}
    return {
        "active": bool(root.get("active")),
        "reason": str(root.get("reason", "") or ""),
        "ready_queue_depth": _normalize_int(root.get("ready_queue_depth")),
        "ready_queue_limit": _normalize_int(root.get("ready_queue_limit")),
        "blocked_submissions": _normalize_int(root.get("blocked_submissions")),
        "updated_at": str(root.get("updated_at", "") or ""),
    }


def _default_stage_pool_snapshot(*, stage_name: str, worker_budget: int) -> dict[str, Any]:
    return {
        "stage_name": stage_name,
        "worker_budget": max(int(worker_budget or 0), 1),
        "queue_depth": 0,
        "inflight": 0,
        "completed": 0,
        "wait_pressure": _default_wait_pressure(),
        "last_started_at": "",
        "last_completed_at": "",
    }


def _normalize_stage_pool_snapshot(
    stage_name: str,
    payload: Any,
    *,
    worker_budget: int,
) -> dict[str, Any]:
    root = dict(payload) if isinstance(payload, dict) else {}
    return {
        "stage_name": stage_name,
        "worker_budget": max(_normalize_int(root.get("worker_budget"), default=worker_budget), 1),
        "queue_depth": _normalize_int(root.get("queue_depth")),
        "inflight": _normalize_int(root.get("inflight")),
        "completed": _normalize_int(root.get("completed")),
        "wait_pressure": _normalize_wait_pressure(root.get("wait_pressure")),
        "last_started_at": str(root.get("last_started_at", "") or ""),
        "last_completed_at": str(root.get("last_completed_at", "") or ""),
    }


class SourceLaneTelemetryLedger:
    def __init__(
        self,
        *,
        source_lane_scheduler: dict[str, Any] | None = None,
    ) -> None:
        contour_payload = source_lane_scheduler.get("source_lane_contour") if isinstance(source_lane_scheduler, dict) else []
        self._lock = Lock()
        self._lanes: dict[str, dict[str, Any]] = {}
        for item in contour_payload or []:
            if not isinstance(item, dict):
                continue
            source_name = str(item.get("source_name", "") or "").strip()
            if not source_name:
                continue
            self._lanes[source_name] = {
                "source_name": source_name,
                "queue_depth": 0,
                "backpressure": _default_backpressure_state(),
            }

    def seed_queue_depths(self, queue_depth_by_source: dict[str, int] | None = None) -> None:
        with self._lock:
            for raw_source_name, raw_queue_depth in (queue_depth_by_source or {}).items():
                source_name = str(raw_source_name or "").strip()
                if not source_name:
                    continue
                lane = self._lanes.setdefault(
                    source_name,
                    {
                        "source_name": source_name,
                        "queue_depth": 0,
                        "backpressure": _default_backpressure_state(),
                    },
                )
                lane["queue_depth"] = _normalize_int(raw_queue_depth)

    def mark_started(self, source_name: str) -> None:
        normalized_source_name = str(source_name or "").strip()
        if not normalized_source_name:
            return
        with self._lock:
            lane = self._lanes.setdefault(
                normalized_source_name,
                {
                    "source_name": normalized_source_name,
                    "queue_depth": 0,
                    "backpressure": _default_backpressure_state(),
                },
            )
            lane["queue_depth"] = max(int(lane.get("queue_depth", 0) or 0) - 1, 0)

    def update_backpressure(
        self,
        source_names: list[str] | tuple[str, ...],
        *,
        active: bool,
        reason: str = "",
        ready_queue_depth: int | None = None,
        ready_queue_limit: int | None = None,
        blocked_submissions_delta: int = 0,
    ) -> None:
        normalized_source_names = [
            str(source_name or "").strip()
            for source_name in (source_names or [])
            if str(source_name or "").strip()
        ]
        if not normalized_source_names:
            return
        normalized_reason = str(reason or "").strip()
        updated_at = _utc_now_iso()
        with self._lock:
            for source_name in normalized_source_names:
                lane = self._lanes.setdefault(
                    source_name,
                    {
                        "source_name": source_name,
                        "queue_depth": 0,
                        "backpressure": _default_backpressure_state(),
                    },
                )
                backpressure = _normalize_backpressure_state(lane.get("backpressure"))
                backpressure["active"] = bool(active)
                backpressure["reason"] = normalized_reason if active else ""
                if ready_queue_depth is not None:
                    backpressure["ready_queue_depth"] = _normalize_int(ready_queue_depth)
                if ready_queue_limit is not None:
                    backpressure["ready_queue_limit"] = _normalize_int(ready_queue_limit)
                if blocked_submissions_delta:
                    backpressure["blocked_submissions"] = max(
                        int(backpressure.get("blocked_submissions", 0) or 0) + int(blocked_submissions_delta),
                        0,
                    )
                backpressure["updated_at"] = updated_at
                lane["backpressure"] = backpressure

    def snapshot(self) -> dict[str, Any]:
        with self._lock:
            snapshot: dict[str, Any] = {}
            for source_name in sorted(self._lanes.keys()):
                lane = self._lanes[source_name]
                snapshot[source_name] = {
                    "source_name": source_name,
                    "queue_depth": _normalize_int(lane.get("queue_depth")),
                    "backpressure": _normalize_backpressure_state(lane.get("backpressure")),
                }
            return snapshot


class StagePoolGovernor:
    def __init__(
        self,
        *,
        per_stage_budget_map: dict[str, int] | None = None,
        active_poll_seconds: float = 0.01,
    ) -> None:
        normalized_budget_map = {
            str(stage_name or "").strip(): max(int(stage_budget or 0), 1)
            for stage_name, stage_budget in (per_stage_budget_map or {}).items()
            if str(stage_name or "").strip()
        }
        self._active_poll_seconds = max(float(active_poll_seconds), 0.001)
        self._semaphores = {
            stage_name: BoundedSemaphore(stage_budget)
            for stage_name, stage_budget in normalized_budget_map.items()
        }
        self._stats_lock = Lock()
        self._stats = {
            stage_name: _default_stage_pool_snapshot(
                stage_name=stage_name,
                worker_budget=stage_budget,
            )
            for stage_name, stage_budget in normalized_budget_map.items()
        }

    @contextmanager
    def lease(
        self,
        stage_name: str,
        *,
        cancel_requested: Any | None = None,
        wait_callback: Any | None = None,
    ):
        normalized_stage_name = str(stage_name or "").strip()
        semaphore = self._semaphores.get(normalized_stage_name)
        if semaphore is None:
            yield
            return
        wait_started_at = 0.0
        wait_registered = False
        while True:
            if callable(cancel_requested) and cancel_requested():
                if wait_registered:
                    self._finish_wait(
                        normalized_stage_name,
                        wait_started_at=wait_started_at,
                        acquired=False,
                    )
                raise CancelledError(f"stage-pool wait cancelled for {normalized_stage_name}")
            if semaphore.acquire(timeout=self._active_poll_seconds):
                break
            if not wait_registered:
                wait_started_at = time.monotonic()
                wait_registered = True
                self._begin_wait(normalized_stage_name)
            if callable(wait_callback):
                wait_callback()
        self._finish_wait(
            normalized_stage_name,
            wait_started_at=wait_started_at,
            acquired=wait_registered,
        )
        self._mark_started(normalized_stage_name)
        try:
            yield
        finally:
            self._mark_completed(normalized_stage_name)
            semaphore.release()

    def snapshot(self) -> dict[str, Any]:
        with self._stats_lock:
            snapshot: dict[str, Any] = {}
            for stage_name in sorted(self._stats.keys()):
                stats = self._stats[stage_name]
                snapshot[stage_name] = _normalize_stage_pool_snapshot(
                    stage_name,
                    stats,
                    worker_budget=int(stats.get("worker_budget", 1) or 1),
                )
            return snapshot

    def _begin_wait(self, stage_name: str) -> None:
        with self._stats_lock:
            stats = self._stats[stage_name]
            stats["queue_depth"] = max(int(stats.get("queue_depth", 0) or 0) + 1, 0)
            wait_pressure = _normalize_wait_pressure(stats.get("wait_pressure"))
            wait_pressure["active"] = True
            wait_pressure["waiters"] = max(int(wait_pressure.get("waiters", 0) or 0) + 1, 0)
            wait_pressure["events"] = max(int(wait_pressure.get("events", 0) or 0) + 1, 0)
            wait_pressure["last_reason"] = "stage_budget_saturated"
            reason_counts = dict(wait_pressure.get("reasons") or {})
            reason_counts["stage_budget_saturated"] = max(
                int(reason_counts.get("stage_budget_saturated", 0) or 0) + 1,
                0,
            )
            wait_pressure["reasons"] = reason_counts
            stats["wait_pressure"] = wait_pressure

    def _finish_wait(
        self,
        stage_name: str,
        *,
        wait_started_at: float,
        acquired: bool,
    ) -> None:
        if wait_started_at <= 0.0:
            return
        wait_seconds = max(time.monotonic() - wait_started_at, 0.0)
        with self._stats_lock:
            stats = self._stats[stage_name]
            stats["queue_depth"] = max(int(stats.get("queue_depth", 0) or 0) - 1, 0)
            wait_pressure = _normalize_wait_pressure(stats.get("wait_pressure"))
            wait_pressure["waiters"] = max(int(wait_pressure.get("waiters", 0) or 0) - 1, 0)
            wait_pressure["active"] = wait_pressure["waiters"] > 0
            wait_pressure["last_seconds"] = round(wait_seconds, 4)
            wait_pressure["total_seconds"] = round(
                float(wait_pressure.get("total_seconds", 0.0) or 0.0) + wait_seconds,
                4,
            )
            wait_pressure["max_seconds"] = round(
                max(float(wait_pressure.get("max_seconds", 0.0) or 0.0), wait_seconds),
                4,
            )
            if not acquired and wait_pressure["waiters"] <= 0:
                wait_pressure["active"] = False
            stats["wait_pressure"] = wait_pressure

    def _mark_started(self, stage_name: str) -> None:
        with self._stats_lock:
            stats = self._stats[stage_name]
            stats["inflight"] = max(int(stats.get("inflight", 0) or 0) + 1, 0)
            stats["last_started_at"] = _utc_now_iso()

    def _mark_completed(self, stage_name: str) -> None:
        with self._stats_lock:
            stats = self._stats[stage_name]
            stats["inflight"] = max(int(stats.get("inflight", 0) or 0) - 1, 0)
            stats["completed"] = max(int(stats.get("completed", 0) or 0) + 1, 0)
            stats["last_completed_at"] = _utc_now_iso()


def build_throughput_telemetry_payload(
    *,
    source_lane_scheduler: dict[str, Any] | None = None,
    downstream_worker_pools: dict[str, Any] | None = None,
    source_lane_runtime: dict[str, Any] | None = None,
    source_stage_runtime: dict[str, Any] | None = None,
    downstream_stage_runtime: dict[str, Any] | None = None,
    stage_backlog: dict[str, int] | None = None,
    host_governor_runtime: dict[str, Any] | None = None,
    backpressure_policy: dict[str, Any] | None = None,
    rows_completed: int = 0,
) -> dict[str, Any]:
    source_lane_scheduler = dict(source_lane_scheduler) if isinstance(source_lane_scheduler, dict) else {}
    downstream_worker_pools = dict(downstream_worker_pools) if isinstance(downstream_worker_pools, dict) else {}
    source_lane_runtime = dict(source_lane_runtime) if isinstance(source_lane_runtime, dict) else {}
    source_stage_runtime = dict(source_stage_runtime) if isinstance(source_stage_runtime, dict) else {}
    downstream_stage_runtime = dict(downstream_stage_runtime) if isinstance(downstream_stage_runtime, dict) else {}
    stage_backlog = dict(stage_backlog) if isinstance(stage_backlog, dict) else {}
    backpressure_policy = dict(backpressure_policy) if isinstance(backpressure_policy, dict) else {}

    source_lanes: dict[str, Any] = {}
    source_contour_payload = source_lane_scheduler.get("source_lane_contour")
    if isinstance(source_contour_payload, list):
        for item in source_contour_payload:
            if not isinstance(item, dict):
                continue
            source_name = str(item.get("source_name", "") or "").strip()
            if not source_name:
                continue
            source_runtime = dict(source_lane_runtime.get(source_name) or {})
            source_stage = dict(source_stage_runtime.get(source_name) or {})
            source_lanes[source_name] = {
                "source_name": source_name,
                "transport_policy": str(item.get("transport_policy", "") or ""),
                "scheduler_lane": str(item.get("scheduler_lane", "") or ""),
                "network_surface": str(item.get("network_surface", "") or ""),
                "contour_state": str(item.get("contour_state", "") or ""),
                "capacity_boundary": str(item.get("capacity_boundary", "") or ""),
                "reason": str(item.get("reason", "") or ""),
                "requested_company_concurrency": _normalize_int(item.get("requested_company_concurrency")),
                "source_capacity_cap": _normalize_int(item.get("source_capacity_cap")),
                "source_lane_budget": _normalize_int(item.get("source_lane_budget"), default=1),
                "worker_lane_budget": _normalize_int(item.get("worker_lane_budget")),
                "host_cap": _normalize_int(item.get("host_cap")),
                "host_aliases": [
                    str(host or "").strip()
                    for host in (item.get("host_aliases") or [])
                    if str(host or "").strip()
                ],
                "queue_depth": _normalize_int(source_runtime.get("queue_depth")),
                "inflight": _normalize_int(source_stage.get("inflight")),
                "completed": _normalize_int(source_stage.get("completed")),
                "wait_pressure": _normalize_wait_pressure(source_stage.get("wait_pressure")),
                "backpressure": _normalize_backpressure_state(source_runtime.get("backpressure")),
            }
    for source_name, source_runtime_payload in source_lane_runtime.items():
        normalized_source_name = str(source_name or "").strip()
        if not normalized_source_name or normalized_source_name in source_lanes:
            continue
        source_stage = dict(source_stage_runtime.get(normalized_source_name) or {})
        source_lanes[normalized_source_name] = {
            "source_name": normalized_source_name,
            "transport_policy": "",
            "scheduler_lane": "",
            "network_surface": "",
            "contour_state": "",
            "capacity_boundary": "",
            "reason": "",
            "requested_company_concurrency": 0,
            "source_capacity_cap": 0,
            "source_lane_budget": 1,
            "worker_lane_budget": 0,
            "host_cap": 0,
            "host_aliases": [],
            "queue_depth": _normalize_int((source_runtime_payload or {}).get("queue_depth")),
            "inflight": _normalize_int(source_stage.get("inflight")),
            "completed": _normalize_int(source_stage.get("completed")),
            "wait_pressure": _normalize_wait_pressure(source_stage.get("wait_pressure")),
            "backpressure": _normalize_backpressure_state((source_runtime_payload or {}).get("backpressure")),
        }

    downstream_stage_pools: dict[str, Any] = {}
    downstream_lanes_payload = downstream_worker_pools.get("lanes")
    if isinstance(downstream_lanes_payload, list):
        for item in downstream_lanes_payload:
            if not isinstance(item, dict):
                continue
            stage_name = str(item.get("stage_name", "") or "").strip()
            if not stage_name:
                continue
            stage_runtime = dict(downstream_stage_runtime.get(stage_name) or {})
            queue_depth = max(
                _normalize_int(stage_runtime.get("queue_depth")),
                _normalize_int(stage_backlog.get(stage_name)),
            )
            downstream_stage_pools[stage_name] = {
                "stage_name": stage_name,
                "queue_family": str(item.get("queue_family", "") or ""),
                "runtime_identity": str(item.get("runtime_identity", "") or ""),
                "priority_class": str(item.get("priority_class", "") or ""),
                "worker_budget": max(_normalize_int(item.get("worker_budget"), default=1), 1),
                "queue_depth": queue_depth,
                "inflight": _normalize_int(stage_runtime.get("inflight")),
                "completed": _normalize_int(stage_runtime.get("completed")),
                "wait_pressure": _normalize_wait_pressure(stage_runtime.get("wait_pressure")),
            }
    for stage_name, stage_runtime_payload in downstream_stage_runtime.items():
        normalized_stage_name = str(stage_name or "").strip()
        if not normalized_stage_name or normalized_stage_name in downstream_stage_pools:
            continue
        queue_depth = max(
            _normalize_int((stage_runtime_payload or {}).get("queue_depth")),
            _normalize_int(stage_backlog.get(normalized_stage_name)),
        )
        downstream_stage_pools[normalized_stage_name] = {
            "stage_name": normalized_stage_name,
            "queue_family": "",
            "runtime_identity": "",
            "priority_class": "",
            "worker_budget": max(_normalize_int((stage_runtime_payload or {}).get("worker_budget"), default=1), 1),
            "queue_depth": queue_depth,
            "inflight": _normalize_int((stage_runtime_payload or {}).get("inflight")),
            "completed": _normalize_int((stage_runtime_payload or {}).get("completed")),
            "wait_pressure": _normalize_wait_pressure((stage_runtime_payload or {}).get("wait_pressure")),
        }

    normalized_host_governor_runtime = _normalize_host_governor_runtime(host_governor_runtime)
    blocked_on = _blocked_on_snapshot(
        source_lanes=source_lanes,
        downstream_stage_pools=downstream_stage_pools,
        host_governor_runtime=normalized_host_governor_runtime,
    )
    source_queue_depth = sum(_normalize_int(item.get("queue_depth")) for item in source_lanes.values())
    source_inflight = sum(_normalize_int(item.get("inflight")) for item in source_lanes.values())
    source_completed = sum(_normalize_int(item.get("completed")) for item in source_lanes.values())
    downstream_queue_depth = sum(_normalize_int(item.get("queue_depth")) for item in downstream_stage_pools.values())
    downstream_inflight = sum(_normalize_int(item.get("inflight")) for item in downstream_stage_pools.values())
    downstream_completed = sum(_normalize_int(item.get("completed")) for item in downstream_stage_pools.values())
    return {
        "contract_version": THROUGHPUT_TELEMETRY_CONTRACT_VERSION,
        "updated_at": "",
        "source_lanes": source_lanes,
        "downstream_stage_pools": downstream_stage_pools,
        "boundary_waits": {
            "host_governor": normalized_host_governor_runtime,
        },
        "backpressure_policy": backpressure_policy,
        "snapshot": {
            "rows_completed": _normalize_int(rows_completed),
            "source_queue_depth": source_queue_depth,
            "source_inflight": source_inflight,
            "source_completed": source_completed,
            "downstream_queue_depth": downstream_queue_depth,
            "downstream_inflight": downstream_inflight,
            "downstream_completed": downstream_completed,
            "boundary_queue_depth": _normalize_int(normalized_host_governor_runtime.get("queue_depth")),
            "blocked_on": blocked_on,
        },
    }


def _normalize_host_governor_runtime(payload: Any) -> dict[str, Any]:
    root = dict(payload) if isinstance(payload, dict) else {}
    hosts_payload = root.get("hosts")
    hosts: dict[str, Any] = {}
    if isinstance(hosts_payload, dict):
        for raw_host, raw_snapshot in hosts_payload.items():
            host = str(raw_host or "").strip().lower()
            if not host or not isinstance(raw_snapshot, dict):
                continue
            hosts[host] = {
                "active_leases": _normalize_int(raw_snapshot.get("active_leases")),
                "cooldown_remaining_seconds": round(
                    _normalize_float(raw_snapshot.get("cooldown_remaining_seconds")),
                    4,
                ),
            }
    return {
        "queue_depth": _normalize_int(root.get("queue_depth")),
        "inflight": _normalize_int(root.get("inflight")),
        "completed": _normalize_int(root.get("completed")),
        "active_hosts": _normalize_int(root.get("active_hosts")),
        "cooldown_hosts": _normalize_int(root.get("cooldown_hosts")),
        "wait_pressure": _normalize_wait_pressure(root.get("wait_pressure")),
        "hosts": hosts,
    }


def _blocked_on_snapshot(
    *,
    source_lanes: dict[str, Any],
    downstream_stage_pools: dict[str, Any],
    host_governor_runtime: dict[str, Any],
) -> list[dict[str, Any]]:
    blocked_on: list[dict[str, Any]] = []
    for source_name, payload in source_lanes.items():
        backpressure = _normalize_backpressure_state(payload.get("backpressure"))
        if backpressure["active"]:
            blocked_on.append(
                {
                    "scope": "source_lane",
                    "name": source_name,
                    "reason": backpressure["reason"] or "intake_backpressure_active",
                }
            )
            continue
        queue_depth = _normalize_int(payload.get("queue_depth"))
        worker_lane_budget = _normalize_int(payload.get("worker_lane_budget"))
        capacity_boundary = str(payload.get("capacity_boundary", "") or "")
        if queue_depth > 0 and worker_lane_budget <= 0 and capacity_boundary:
            blocked_on.append(
                {
                    "scope": "source_lane",
                    "name": source_name,
                    "reason": capacity_boundary,
                }
            )
    for stage_name, payload in downstream_stage_pools.items():
        queue_depth = _normalize_int(payload.get("queue_depth"))
        inflight = _normalize_int(payload.get("inflight"))
        worker_budget = max(_normalize_int(payload.get("worker_budget"), default=1), 1)
        wait_pressure = _normalize_wait_pressure(payload.get("wait_pressure"))
        if queue_depth > 0 and inflight >= worker_budget:
            blocked_on.append(
                {
                    "scope": "downstream_stage_pool",
                    "name": stage_name,
                    "reason": "stage_pool_saturated",
                }
            )
            continue
        if wait_pressure["active"]:
            blocked_on.append(
                {
                    "scope": "downstream_stage_pool",
                    "name": stage_name,
                    "reason": wait_pressure["last_reason"] or "stage_pool_wait_active",
                }
            )
    host_wait_pressure = _normalize_wait_pressure(host_governor_runtime.get("wait_pressure"))
    if host_wait_pressure["active"] or _normalize_int(host_governor_runtime.get("cooldown_hosts")) > 0:
        blocked_on.append(
            {
                "scope": "boundary_wait",
                "name": "host_governor",
                "reason": host_wait_pressure["last_reason"] or "cooldown_or_host_lease_wait",
            }
        )
    return blocked_on


__all__ = [
    "SourceLaneTelemetryLedger",
    "StagePoolGovernor",
    "THROUGHPUT_TELEMETRY_CONTRACT_VERSION",
    "build_throughput_telemetry_payload",
]

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass

DIRECT_DEFAULT_TRANSPORT = "direct-default"
OFFLINE_ONLY_TRANSPORT = "offline-only"
SESSION_BOUND_TRANSPORT = "session-bound"
PROXY_BOUND_TRANSPORT = "proxy-bound"
PROXY_REQUIRED_TRANSPORT = PROXY_BOUND_TRANSPORT
SERIAL_ONLY_LANE_BUDGET = 1
SOURCE_WORKER_LANE_SOURCE_NAMES = frozenset({"spark", "zachestnyibiznes", "checko"})
DIRECT_DEFAULT_BOUNDED_SOURCE_NAMES = frozenset({"spark", "zachestnyibiznes"})

DEFAULT_SOURCE_TRANSPORT_POLICY = {
    "bicotender": PROXY_BOUND_TRANSPORT,
    "checko": PROXY_BOUND_TRANSPORT,
    "spark": DIRECT_DEFAULT_TRANSPORT,
    "zachestnyibiznes": DIRECT_DEFAULT_TRANSPORT,
    "rusprofile": SESSION_BOUND_TRANSPORT,
    "list_org": OFFLINE_ONLY_TRANSPORT,
}

SOURCE_HOST_ALIASES = {
    "bicotender": ("www.bicotender.ru", "bicotender.ru"),
    "checko": ("checko.ru", "www.checko.ru"),
    "spark": ("spark-interfax.ru",),
    "zachestnyibiznes": ("zachestnyibiznes.ru",),
    "rusprofile": ("rusprofile.ru", "www.rusprofile.ru"),
    "list_org": ("www.list-org.com",),
}


@dataclass(frozen=True)
class SourceLaneContourEntry:
    source_name: str
    transport_policy: str
    scheduler_lane: str
    network_surface: str
    contour_state: str
    capacity_boundary: str
    reason: str
    requested_company_concurrency: int
    source_capacity_cap: int
    source_lane_budget: int
    worker_lane_budget: int
    host_cap: int
    host_aliases: tuple[str, ...] = ()

    def as_dict(self) -> dict[str, object]:
        return {
            "source_name": self.source_name,
            "transport_policy": self.transport_policy,
            "scheduler_lane": self.scheduler_lane,
            "network_surface": self.network_surface,
            "contour_state": self.contour_state,
            "capacity_boundary": self.capacity_boundary,
            "reason": self.reason,
            "requested_company_concurrency": self.requested_company_concurrency,
            "source_capacity_cap": self.source_capacity_cap,
            "source_lane_budget": self.source_lane_budget,
            "worker_lane_budget": self.worker_lane_budget,
            "host_cap": self.host_cap,
            "host_aliases": list(self.host_aliases),
        }


@dataclass(frozen=True)
class SourceExecutionGuardrails:
    company_concurrency_cap: int
    requested_company_concurrency: int
    effective_company_concurrency_cap: int
    usable_proxy_pool_count: int
    per_source_cap_map: dict[str, int]
    per_source_lane_budget_map: dict[str, int]
    per_source_worker_lane_budget_map: dict[str, int]
    per_host_cap_map: dict[str, int]
    source_transport_policy: dict[str, str]
    source_lane_contour: tuple[SourceLaneContourEntry, ...]

    def as_dict(self) -> dict[str, object]:
        return {
            "company_concurrency_cap": self.company_concurrency_cap,
            "requested_company_concurrency": self.requested_company_concurrency,
            "effective_company_concurrency_cap": self.effective_company_concurrency_cap,
            "usable_proxy_pool_count": self.usable_proxy_pool_count,
            "per_source_cap_map": dict(self.per_source_cap_map),
            "per_source_lane_budget_map": dict(self.per_source_lane_budget_map),
            "per_source_worker_lane_budget_map": dict(self.per_source_worker_lane_budget_map),
            "per_host_cap_map": dict(self.per_host_cap_map),
            "source_transport_policy": dict(self.source_transport_policy),
            "source_lane_contour": [entry.as_dict() for entry in self.source_lane_contour],
        }


def resolve_effective_company_concurrency_cap(
    *,
    active_sources: Sequence[str],
    requested_company_concurrency: int,
    per_source_lane_budget_map: dict[str, int],
) -> int:
    normalized_requested = int(requested_company_concurrency or 0)
    if normalized_requested < 1:
        raise ValueError("company concurrency must be >= 1")

    effective_cap = normalized_requested
    for source_name in active_sources:
        normalized_source_name = str(source_name or "").strip()
        if not normalized_source_name:
            continue
        lane_budget = max(int(per_source_lane_budget_map.get(normalized_source_name, normalized_requested) or 0), 1)
        effective_cap = min(effective_cap, lane_budget)
    return max(effective_cap, 1)


def build_source_execution_guardrails(
    *,
    active_sources: list[str],
    requested_company_concurrency: int,
    usable_proxy_pool_count: int,
) -> SourceExecutionGuardrails:
    normalized_requested = int(requested_company_concurrency or 0)
    if normalized_requested < 1:
        raise ValueError("company concurrency must be >= 1")

    normalized_proxy_count = max(int(usable_proxy_pool_count or 0), 0)
    source_transport_policy = {
        source_name: DEFAULT_SOURCE_TRANSPORT_POLICY.get(source_name, DIRECT_DEFAULT_TRANSPORT)
        for source_name in active_sources
    }

    per_source_cap_map: dict[str, int] = {}
    per_source_lane_budget_map: dict[str, int] = {}
    per_source_worker_lane_budget_map: dict[str, int] = {}
    per_host_cap_map: dict[str, int] = {}
    source_lane_contour: list[SourceLaneContourEntry] = []
    for source_name in active_sources:
        contour_entry = _build_source_lane_contour_entry(
            source_name=source_name,
            transport_policy=source_transport_policy[source_name],
            requested_company_concurrency=normalized_requested,
            usable_proxy_pool_count=normalized_proxy_count,
        )
        source_lane_contour.append(contour_entry)
        per_source_cap_map[source_name] = contour_entry.source_capacity_cap
        per_source_lane_budget_map[source_name] = contour_entry.source_lane_budget
        per_source_worker_lane_budget_map[source_name] = contour_entry.worker_lane_budget
        for host in contour_entry.host_aliases:
            per_host_cap_map[host] = contour_entry.host_cap

    effective_source_names = [
        entry.source_name
        for entry in source_lane_contour
        if entry.worker_lane_budget > 0
    ]
    effective_budget_map = (
        per_source_worker_lane_budget_map
        if effective_source_names
        else per_source_lane_budget_map
    )
    if not effective_source_names:
        effective_source_names = list(active_sources)

    effective_company_concurrency_cap = resolve_effective_company_concurrency_cap(
        active_sources=effective_source_names,
        requested_company_concurrency=normalized_requested,
        per_source_lane_budget_map=effective_budget_map,
    )

    return SourceExecutionGuardrails(
        company_concurrency_cap=normalized_requested,
        requested_company_concurrency=normalized_requested,
        effective_company_concurrency_cap=effective_company_concurrency_cap,
        usable_proxy_pool_count=normalized_proxy_count,
        per_source_cap_map=per_source_cap_map,
        per_source_lane_budget_map=per_source_lane_budget_map,
        per_source_worker_lane_budget_map=per_source_worker_lane_budget_map,
        per_host_cap_map=per_host_cap_map,
        source_transport_policy=source_transport_policy,
        source_lane_contour=tuple(source_lane_contour),
    )


def _build_source_lane_contour_entry(
    *,
    source_name: str,
    transport_policy: str,
    requested_company_concurrency: int,
    usable_proxy_pool_count: int,
) -> SourceLaneContourEntry:
    normalized_source_name = str(source_name or "").strip()
    host_aliases = tuple(SOURCE_HOST_ALIASES.get(normalized_source_name, ()))

    if transport_policy == DIRECT_DEFAULT_TRANSPORT:
        source_capacity_cap = requested_company_concurrency
        source_lane_budget = max(source_capacity_cap, 1)
        worker_lane_budget = source_lane_budget if requested_company_concurrency > 1 else 0
        if worker_lane_budget > 0:
            contour_state = "worker_lane_active"
            capacity_boundary = "direct_default_worker_lane"
            scheduler_lane = "direct_default_worker"
            reason = "direct-default source is eligible for the bounded worker lane"
        else:
            contour_state = "serial_inline_only"
            capacity_boundary = "company_concurrency_serial_only"
            scheduler_lane = "direct_default_serial_inline"
            reason = "requested company concurrency keeps the direct-default lane serial"
        return SourceLaneContourEntry(
            source_name=normalized_source_name,
            transport_policy=transport_policy,
            scheduler_lane=scheduler_lane,
            network_surface="live-network",
            contour_state=contour_state,
            capacity_boundary=capacity_boundary,
            reason=reason,
            requested_company_concurrency=requested_company_concurrency,
            source_capacity_cap=source_capacity_cap,
            source_lane_budget=source_lane_budget,
            worker_lane_budget=worker_lane_budget,
            host_cap=source_capacity_cap,
            host_aliases=host_aliases,
        )

    if transport_policy == SESSION_BOUND_TRANSPORT:
        return SourceLaneContourEntry(
            source_name=normalized_source_name,
            transport_policy=transport_policy,
            scheduler_lane="session_serial_inline",
            network_surface="live-network",
            contour_state="serial_inline_only",
            capacity_boundary="session_bound_serial_lane",
            reason="session-bound source remains on the runner-owned serial lane",
            requested_company_concurrency=requested_company_concurrency,
            source_capacity_cap=1,
            source_lane_budget=SERIAL_ONLY_LANE_BUDGET,
            worker_lane_budget=0,
            host_cap=1,
            host_aliases=host_aliases,
        )

    if transport_policy == PROXY_BOUND_TRANSPORT:
        source_capacity_cap = min(requested_company_concurrency, usable_proxy_pool_count)
        if requested_company_concurrency > 1:
            source_lane_budget = max(source_capacity_cap, SERIAL_ONLY_LANE_BUDGET)
            worker_lane_budget = source_capacity_cap if source_capacity_cap > 1 else 0
            if worker_lane_budget > 0:
                contour_state = "worker_lane_active"
                capacity_boundary = "proxy_bound_worker_lane"
                scheduler_lane = "proxy_bound_worker"
                reason = "proxy-bound source is eligible for the bounded worker lane"
            elif usable_proxy_pool_count <= 0:
                contour_state = "worker_lane_unavailable"
                capacity_boundary = "proxy_capacity_unavailable"
                scheduler_lane = "proxy_bound_worker_unavailable"
                reason = (
                    "proxy-bound source is worker-lane eligible, but usable_proxy_pool_count == 0; "
                    "direct outbound disabled"
                )
            else:
                contour_state = "worker_lane_capacity_limited"
                capacity_boundary = "proxy_bound_single_capacity"
                scheduler_lane = "proxy_bound_worker_capacity_limited"
                reason = "proxy-bound worker lane requires at least two usable proxy slots for bounded parallelism"
        else:
            source_lane_budget = SERIAL_ONLY_LANE_BUDGET
            worker_lane_budget = 0
            if usable_proxy_pool_count <= 0:
                contour_state = "serial_inline_only"
                capacity_boundary = "proxy_capacity_unavailable"
                scheduler_lane = "proxy_serial_inline"
                reason = (
                    "proxy-bound source has no usable proxy pool in the current runtime (usable_proxy_pool_count == 0); "
                    "direct outbound disabled"
                )
            else:
                contour_state = "serial_inline_only"
                capacity_boundary = "proxy_bound_serial_lane"
                scheduler_lane = "proxy_serial_inline"
                reason = "proxy-bound source remains on the runner-owned serial lane"
        return SourceLaneContourEntry(
            source_name=normalized_source_name,
            transport_policy=transport_policy,
            scheduler_lane=scheduler_lane,
            network_surface="live-network",
            contour_state=contour_state,
            capacity_boundary=capacity_boundary,
            reason=reason,
            requested_company_concurrency=requested_company_concurrency,
            source_capacity_cap=source_capacity_cap,
            source_lane_budget=source_lane_budget,
            worker_lane_budget=worker_lane_budget,
            host_cap=source_capacity_cap,
            host_aliases=host_aliases,
        )

    return SourceLaneContourEntry(
        source_name=normalized_source_name,
        transport_policy=OFFLINE_ONLY_TRANSPORT,
        scheduler_lane="offline_surface",
        network_surface="offline-snapshot",
        contour_state="offline_only_surface",
        capacity_boundary="offline_only_surface",
        reason="offline-only source has no live network worker lane",
        requested_company_concurrency=requested_company_concurrency,
        source_capacity_cap=0,
        source_lane_budget=SERIAL_ONLY_LANE_BUDGET,
        worker_lane_budget=0,
        host_cap=0,
        host_aliases=host_aliases,
    )


__all__ = [
    "DIRECT_DEFAULT_BOUNDED_SOURCE_NAMES",
    "DIRECT_DEFAULT_TRANSPORT",
    "OFFLINE_ONLY_TRANSPORT",
    "PROXY_BOUND_TRANSPORT",
    "PROXY_REQUIRED_TRANSPORT",
    "SERIAL_ONLY_LANE_BUDGET",
    "SESSION_BOUND_TRANSPORT",
    "SOURCE_HOST_ALIASES",
    "SOURCE_WORKER_LANE_SOURCE_NAMES",
    "SourceLaneContourEntry",
    "SourceExecutionGuardrails",
    "build_source_execution_guardrails",
    "resolve_effective_company_concurrency_cap",
]

from __future__ import annotations

import time
from dataclasses import dataclass
from datetime import datetime
from threading import Lock
from collections.abc import Callable
from typing import Any

from .host_memory import (
    HOST_MEMORY_SIGNAL_BOT_GATE,
    HOST_MEMORY_SIGNAL_CHALLENGE,
    HOST_MEMORY_SIGNAL_COOLDOWN,
    HOST_MEMORY_SIGNAL_HTTP_403,
    HOST_MEMORY_SIGNAL_HTTP_429,
    active_governor_signal_outcomes,
)


HOST_GOVERNOR_PROXY_AVOID_SIGNAL_TAGS = frozenset(
    {
        HOST_MEMORY_SIGNAL_HTTP_429,
        HOST_MEMORY_SIGNAL_HTTP_403,
        HOST_MEMORY_SIGNAL_BOT_GATE,
        HOST_MEMORY_SIGNAL_CHALLENGE,
    }
)


@dataclass(frozen=True, slots=True)
class HostGovernorPreflight:
    host: str
    cooldown_active: bool = False
    cooldown_remaining_seconds: int = 0
    avoid_proxy_labels_or_ids: tuple[str, ...] = ()
    relevant_signal_tags: tuple[str, ...] = ()


@dataclass(slots=True)
class _HostGovernorLedgerEntry:
    active_leases: int = 0
    cooldown_until_ts: float = 0.0
    bootstrapped_from_runtime: bool = False


class HostGovernorLedger:
    def __init__(
        self,
        *,
        persisted_host_memory: dict[str, Any] | Callable[[], dict[str, Any] | None] | None = None,
        now_fn: Any | None = None,
        sleep_fn: Any | None = None,
        active_poll_seconds: float = 0.01,
    ) -> None:
        self._persisted_host_memory = persisted_host_memory
        self._now_fn = now_fn or time.time
        self._sleep_fn = sleep_fn or time.sleep
        self._active_poll_seconds = max(float(active_poll_seconds), 0.001)
        self._entries: dict[str, _HostGovernorLedgerEntry] = {}
        self._lock = Lock()
        self._waiters = 0
        self._completed_acquisitions = 0
        self._wait_events = 0
        self._wait_seconds_total = 0.0
        self._wait_seconds_max = 0.0
        self._last_wait_seconds = 0.0
        self._blocked_reason_counts: dict[str, int] = {}
        self._last_blocked_reasons: tuple[str, ...] = ()

    def acquire(
        self,
        hosts: Any,
        *,
        cancel_requested: Callable[[], bool] | None = None,
    ) -> tuple[str, ...]:
        normalized_hosts = _normalize_hosts(hosts)
        if not normalized_hosts:
            return ()
        wait_started_at = 0.0
        wait_registered = False
        while True:
            if callable(cancel_requested) and cancel_requested():
                if wait_registered:
                    with self._lock:
                        self._waiters = max(self._waiters - 1, 0)
                return ()
            with self._lock:
                now_ts = float(self._now_fn())
                wait_seconds, blocked_reasons = self._blocked_wait_seconds_locked(normalized_hosts, now_ts=now_ts)
                if wait_seconds <= 0.0:
                    if wait_registered:
                        elapsed_seconds = max(float(self._now_fn()) - wait_started_at, 0.0)
                        self._waiters = max(self._waiters - 1, 0)
                        self._wait_seconds_total += elapsed_seconds
                        self._wait_seconds_max = max(self._wait_seconds_max, elapsed_seconds)
                        self._last_wait_seconds = round(elapsed_seconds, 4)
                    for host in normalized_hosts:
                        entry = self._entry_locked(host, now_ts=now_ts)
                        entry.active_leases += 1
                    self._completed_acquisitions += 1
                    return normalized_hosts
                if not wait_registered:
                    wait_registered = True
                    wait_started_at = float(self._now_fn())
                    self._waiters += 1
                    self._wait_events += 1
                    self._last_blocked_reasons = tuple(sorted(blocked_reasons))
                    for reason in blocked_reasons:
                        self._blocked_reason_counts[reason] = self._blocked_reason_counts.get(reason, 0) + 1
            if callable(cancel_requested):
                self._sleep_fn(min(wait_seconds, self._active_poll_seconds))
            else:
                self._sleep_fn(wait_seconds)

    def release(
        self,
        hosts: Any,
        *,
        runtime_events: Any = (),
    ) -> None:
        normalized_hosts = _normalize_hosts(hosts)
        with self._lock:
            now_ts = float(self._now_fn())
            cooldowns_by_host = _cooldown_untils_from_runtime_events(runtime_events, now_ts=now_ts)
            for host in normalized_hosts:
                entry = self._entry_locked(host, now_ts=now_ts)
                if entry.active_leases > 0:
                    entry.active_leases -= 1
            for host, cooldown_until_ts in cooldowns_by_host.items():
                entry = self._entry_locked(host, now_ts=now_ts)
                entry.cooldown_until_ts = max(entry.cooldown_until_ts, cooldown_until_ts)

    def snapshot(self) -> dict[str, Any]:
        with self._lock:
            now_ts = float(self._now_fn())
            snapshot: dict[str, dict[str, Any]] = {}
            inflight = 0
            cooldown_hosts = 0
            active_hosts = 0
            for host in sorted(self._entries.keys()):
                entry = self._entry_locked(host, now_ts=now_ts)
                inflight += max(entry.active_leases, 0)
                if entry.active_leases > 0:
                    active_hosts += 1
                cooldown_remaining_seconds = round(max(entry.cooldown_until_ts - now_ts, 0.0), 4)
                if cooldown_remaining_seconds > 0:
                    cooldown_hosts += 1
                snapshot[host] = {
                    "active_leases": entry.active_leases,
                    "cooldown_remaining_seconds": cooldown_remaining_seconds,
                }
            payload: dict[str, Any] = {
                "queue_depth": self._waiters,
                "inflight": inflight,
                "completed": self._completed_acquisitions,
                "active_hosts": active_hosts,
                "cooldown_hosts": cooldown_hosts,
                "wait_pressure": {
                    "active": self._waiters > 0,
                    "waiters": self._waiters,
                    "events": self._wait_events,
                    "total_seconds": round(self._wait_seconds_total, 4),
                    "max_seconds": round(self._wait_seconds_max, 4),
                    "last_seconds": round(self._last_wait_seconds, 4),
                    "last_reason": self._last_blocked_reasons[0] if self._last_blocked_reasons else "",
                    "reasons": dict(sorted(self._blocked_reason_counts.items())),
                },
                "hosts": snapshot,
            }
            for host, host_snapshot in snapshot.items():
                payload.setdefault(host, host_snapshot)
            return payload

    def _blocked_wait_seconds_locked(self, hosts: tuple[str, ...], *, now_ts: float) -> tuple[float, tuple[str, ...]]:
        wait_candidates: list[float] = []
        blocked_reasons: set[str] = set()
        for host in hosts:
            entry = self._entry_locked(host, now_ts=now_ts)
            if entry.active_leases > 0:
                wait_candidates.append(self._active_poll_seconds)
                blocked_reasons.add("active_host_lease")
            cooldown_remaining = max(entry.cooldown_until_ts - now_ts, 0.0)
            if cooldown_remaining > 0.0:
                wait_candidates.append(cooldown_remaining)
                blocked_reasons.add("cooldown_active")
        return (min(wait_candidates) if wait_candidates else 0.0), tuple(sorted(blocked_reasons))

    def _entry_locked(self, host: str, *, now_ts: float) -> _HostGovernorLedgerEntry:
        entry = self._entries.setdefault(host, _HostGovernorLedgerEntry())
        if not entry.bootstrapped_from_runtime:
            preflight = resolve_host_governor_preflight(
                _resolve_persisted_host_memory(self._persisted_host_memory),
                host,
                now_ts=now_ts,
            )
            if preflight.cooldown_active and preflight.cooldown_remaining_seconds > 0:
                entry.cooldown_until_ts = max(
                    entry.cooldown_until_ts,
                    now_ts + float(preflight.cooldown_remaining_seconds),
                )
            entry.bootstrapped_from_runtime = True
        if entry.cooldown_until_ts <= now_ts:
            entry.cooldown_until_ts = 0.0
        return entry


def resolve_host_governor_preflight(
    state: dict[str, Any] | None,
    host: str,
    *,
    now_ts: float | None = None,
) -> HostGovernorPreflight:
    normalized_host = str(host or "").strip().lower()
    if not normalized_host:
        return HostGovernorPreflight(host="")
    active_items = active_governor_signal_outcomes(state, normalized_host)
    if not active_items:
        return HostGovernorPreflight(host=normalized_host)

    now = time.time() if now_ts is None else float(now_ts)
    seen_proxy_labels: set[str] = set()
    avoid_proxy_labels: list[str] = []
    seen_tags: set[str] = set()
    relevant_signal_tags: list[str] = []
    max_cooldown_remaining = 0.0

    for item in active_items:
        signal_tags = tuple(str(tag or "").strip() for tag in item.get("signal_tags", []) if str(tag or "").strip())
        if not signal_tags:
            continue
        cooldown_remaining = _cooldown_remaining_seconds(item, now_ts=now)
        if cooldown_remaining > max_cooldown_remaining:
            max_cooldown_remaining = cooldown_remaining
        if cooldown_remaining > 0:
            _extend_signal_tags(
                relevant_signal_tags,
                seen_tags,
                signal_tags,
                include_cooldown=True,
            )

        proxy_label = str(item.get("proxy_label_or_id") or "").strip()
        if not proxy_label or proxy_label in seen_proxy_labels:
            continue
        seen_proxy_labels.add(proxy_label)
        if HOST_GOVERNOR_PROXY_AVOID_SIGNAL_TAGS.isdisjoint(signal_tags):
            continue
        avoid_proxy_labels.append(proxy_label)
        _extend_signal_tags(
            relevant_signal_tags,
            seen_tags,
            signal_tags,
            include_cooldown=cooldown_remaining > 0,
        )

    cooldown_remaining_seconds = int(max(0.0, round(max_cooldown_remaining)))
    return HostGovernorPreflight(
        host=normalized_host,
        cooldown_active=cooldown_remaining_seconds > 0,
        cooldown_remaining_seconds=cooldown_remaining_seconds,
        avoid_proxy_labels_or_ids=tuple(avoid_proxy_labels),
        relevant_signal_tags=tuple(relevant_signal_tags),
    )


def _extend_signal_tags(
    bucket: list[str],
    seen_tags: set[str],
    signal_tags: tuple[str, ...],
    *,
    include_cooldown: bool,
) -> None:
    for tag in signal_tags:
        if tag == HOST_MEMORY_SIGNAL_COOLDOWN and not include_cooldown:
            continue
        if tag in seen_tags:
            continue
        seen_tags.add(tag)
        bucket.append(tag)


def _cooldown_remaining_seconds(item: dict[str, Any], *, now_ts: float) -> float:
    cooldown_seconds = _normalize_non_negative_float(item.get("cooldown_seconds"))
    if cooldown_seconds <= 0.0:
        return 0.0
    event_ts = _parse_event_timestamp(item.get("ts"))
    if event_ts is None:
        return 0.0
    return max(0.0, (event_ts + cooldown_seconds) - now_ts)


def _normalize_non_negative_float(value: Any) -> float:
    if value in (None, "") or isinstance(value, bool):
        return 0.0
    try:
        return max(float(value), 0.0)
    except (TypeError, ValueError):
        return 0.0


def _parse_event_timestamp(value: Any) -> float | None:
    text = str(value or "").strip()
    if not text:
        return None
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        return datetime.fromisoformat(text).timestamp()
    except ValueError:
        return None


def _normalize_hosts(value: Any) -> tuple[str, ...]:
    seen: set[str] = set()
    hosts: list[str] = []
    for item in value or ():
        normalized = str(item or "").strip().lower()
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        hosts.append(normalized)
    return tuple(sorted(hosts))


def _resolve_persisted_host_memory(
    value: dict[str, Any] | Callable[[], dict[str, Any] | None] | None,
) -> dict[str, Any] | None:
    if callable(value):
        resolved = value()
        return resolved if isinstance(resolved, dict) else None
    return value if isinstance(value, dict) else None


def _cooldown_untils_from_runtime_events(runtime_events: Any, *, now_ts: float) -> dict[str, float]:
    cooldowns_by_host: dict[str, float] = {}
    for event in runtime_events or ():
        if not isinstance(event, dict):
            continue
        host = str(event.get("host") or "").strip().lower()
        cooldown_seconds = _normalize_non_negative_float(event.get("cooldown_seconds"))
        if not host or cooldown_seconds <= 0.0:
            continue
        event_ts = _parse_event_timestamp(event.get("ts"))
        if event_ts is None:
            event_ts = now_ts
        else:
            event_ts = max(event_ts, now_ts)
        cooldowns_by_host[host] = max(
            cooldowns_by_host.get(host, 0.0),
            event_ts + cooldown_seconds,
        )
    return cooldowns_by_host


__all__ = [
    "HOST_GOVERNOR_PROXY_AVOID_SIGNAL_TAGS",
    "HostGovernorLedger",
    "HostGovernorPreflight",
    "resolve_host_governor_preflight",
]

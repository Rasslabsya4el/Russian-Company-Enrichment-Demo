from __future__ import annotations

import json
import os
import time
from dataclasses import dataclass, field
from pathlib import Path
from threading import Lock
from urllib.parse import quote, unquote, urlparse

from app.runtime.proxy6 import (
    PROXY_PROVIDER_INVENTORY_EMPTY_OR_EXPIRED,
    PROXY_PROVIDER_INVENTORY_HEALTHY,
    PROXY_PROVIDER_STATUS_UNKNOWN,
    Proxy6InventoryDiagnostic,
    RUNTIME_PROXY_POOL_DEPLETED_PROVIDER_HEALTHY,
    diagnose_proxy6_inventory_from_env,
    load_active_proxy6_parser_proxies_from_env,
)


@dataclass
class ProxyEntry:
    url: str
    source: str
    proxy_id: str = ""
    host: str = ""
    port: str = ""
    country: str = ""
    descr: str = ""
    failures: int = 0
    cooldown_until: float = 0.0
    recovered_sources: set[str] = field(default_factory=set)

    @property
    def label(self) -> str:
        if self.host and self.port:
            return f"{self.host}:{self.port}"
        parsed = urlparse(self.url)
        host = parsed.hostname or ""
        port = parsed.port or ""
        return f"{host}:{port}" if host or port else self.url


@dataclass(frozen=True)
class ProxySelection:
    url: str = ""
    source: str = ""
    proxy_id: str = ""
    label: str = ""
    host: str = ""
    port: str = ""
    country: str = ""
    via_proxy: bool = False

    @property
    def proxy_label_or_id(self) -> str:
        return self.proxy_id or self.label

    @property
    def requests_proxies(self) -> dict[str, str] | None:
        if not self.via_proxy or not self.url:
            return None
        return {"http": self.url, "https": self.url}

    @property
    def browser_proxy(self) -> dict[str, str] | None:
        if not self.via_proxy or not self.url:
            return None
        parsed = urlparse(self.url)
        if not parsed.hostname:
            return None
        server = f"{parsed.scheme or 'http'}://{parsed.hostname}"
        if parsed.port:
            server = f"{server}:{parsed.port}"
        payload: dict[str, str] = {"server": server}
        if parsed.username:
            payload["username"] = unquote(parsed.username)
        if parsed.password:
            payload["password"] = unquote(parsed.password)
        return payload


@dataclass(frozen=True)
class ProxyProviderSyncResult:
    attempted: bool
    status: str
    reason: str
    added_count: int = 0
    refreshed_count: int = 0
    provider_proxy_count: int = 0
    usable_count_before: int = 0
    usable_count_after: int = 0
    provider_status: str = ""
    provider_stop_class: str = ""
    operator_action: str = ""

    @property
    def restored(self) -> bool:
        return self.usable_count_after > self.usable_count_before and self.usable_count_after > 0

    def as_event_fields(self) -> dict[str, object]:
        return {
            "proxy_sync_attempted": self.attempted,
            "proxy_sync_status": self.status,
            "proxy_sync_reason": self.reason,
            "proxy_sync_added_count": self.added_count,
            "proxy_sync_refreshed_count": self.refreshed_count,
            "proxy_sync_provider_proxy_count": self.provider_proxy_count,
            "proxy_sync_usable_count_before": self.usable_count_before,
            "proxy_sync_usable_count_after": self.usable_count_after,
            "proxy_sync_restored": self.restored,
            "proxy_provider_status": self.provider_status,
            "proxy_provider_stop_class": self.provider_stop_class,
            "operator_action": self.operator_action,
        }


PROXY_LIFECYCLE_MISSING_RUNTIME_CONFIG = "missing_runtime_proxy_config"
PROXY_LIFECYCLE_PROVIDER_UNKNOWN = PROXY_PROVIDER_STATUS_UNKNOWN
PROXY_LIFECYCLE_PROVIDER_EMPTY_OR_EXPIRED = PROXY_PROVIDER_INVENTORY_EMPTY_OR_EXPIRED
PROXY_LIFECYCLE_RUNTIME_SYNCED_USABLE = "runtime_proxy_pool_synced_usable"
PROXY_LIFECYCLE_RUNTIME_DEPLETED_PROVIDER_HEALTHY = RUNTIME_PROXY_POOL_DEPLETED_PROVIDER_HEALTHY
PROXY_LIFECYCLE_SOURCE_CAPACITY_BLOCKED = "source_proxy_capacity_reserved_or_ineligible"
PROXY_LIFECYCLE_TRUE_HARD_BLOCK = "target_site_or_proxy_hard_block"
PROXY_LIFECYCLE_PARSER_PROVEN_RECOVERY = "parser_proven_source_recovery"
PROXY_LIFECYCLE_ACTION_NONE = "none"
PROXY_LIFECYCLE_ACTION_FAIL_CLOSED = "fail_closed_precise_stop"
PROXY_LIFECYCLE_ACTION_SOURCE_SCOPED_RECOVERY = "source_scoped_parser_proven_recovery"
PROXY_LIFECYCLE_ACTION_PRESERVE_RESERVED_CAPACITY = (
    "preserve_required_source_reserved_capacity_or_add_source_scoped_recovery"
)
PROXY_LIFECYCLE_HARD_FAILURE_CLASSES = frozenset(
    {"http_403", "bot_gate", "rate_limited", "proxy_timeout", "proxy_tunnel_error", "proxy_connection_error"}
)


@dataclass(frozen=True)
class ProxyLifecycleSnapshot:
    source_name: str
    state: str
    subreason: str
    operator_action: str
    failure_class: str
    recovery_class: str
    provider_status: str = ""
    provider_stop_class: str = ""
    sync_status: str = ""
    sync_reason: str = ""
    runtime_entry_count: int = 0
    runtime_global_usable_count: int = 0
    runtime_source_usable_count: int = 0
    runtime_cooldown_active_count: int = 0
    runtime_reserved_capacity: int = 0
    runtime_recovered_source_count: int = 0

    def as_event_fields(self) -> dict[str, object]:
        payload: dict[str, object] = {
            "proxy_lifecycle_state": self.state,
            "proxy_lifecycle_subreason": self.subreason,
            "proxy_lifecycle_operator_action": self.operator_action,
            "proxy_lifecycle_failure_class": self.failure_class,
            "proxy_lifecycle_recovery_class": self.recovery_class,
            "proxy_lifecycle_runtime_entry_count": self.runtime_entry_count,
            "proxy_lifecycle_global_usable_count": self.runtime_global_usable_count,
            "proxy_lifecycle_source_usable_count": self.runtime_source_usable_count,
            "proxy_lifecycle_cooldown_active_count": self.runtime_cooldown_active_count,
            "proxy_lifecycle_reserved_capacity": self.runtime_reserved_capacity,
            "proxy_lifecycle_recovered_source_count": self.runtime_recovered_source_count,
        }
        if self.source_name:
            payload["proxy_lifecycle_source"] = self.source_name
        if self.provider_status:
            payload["proxy_lifecycle_provider_status"] = self.provider_status
        if self.provider_stop_class:
            payload["proxy_lifecycle_provider_stop_class"] = self.provider_stop_class
        if self.sync_status:
            payload["proxy_lifecycle_sync_status"] = self.sync_status
        if self.sync_reason:
            payload["proxy_lifecycle_sync_reason"] = self.sync_reason
        return payload

    def operator_message(self) -> str:
        parts = [
            f"proxy_lifecycle_state={self.state}",
            f"proxy_lifecycle_subreason={self.subreason}",
            f"proxy_lifecycle_operator_action={self.operator_action}",
        ]
        if self.sync_status:
            parts.append(f"proxy_lifecycle_sync_status={self.sync_status}")
        if self.runtime_entry_count or self.runtime_global_usable_count or self.runtime_source_usable_count:
            parts.append(f"runtime_entries={self.runtime_entry_count}")
            parts.append(f"global_usable={self.runtime_global_usable_count}")
            parts.append(f"source_usable={self.runtime_source_usable_count}")
        return " ".join(parts)


def proxy_lifecycle_hard_block_event_fields(*, failure_class: str, recovery_class: str = "") -> dict[str, object]:
    normalized_failure_class = _normalize_proxy_failure_reason(failure_class) or "hard_block"
    return {
        "proxy_lifecycle_state": PROXY_LIFECYCLE_TRUE_HARD_BLOCK,
        "proxy_lifecycle_subreason": normalized_failure_class,
        "proxy_lifecycle_operator_action": PROXY_LIFECYCLE_ACTION_FAIL_CLOSED,
        "proxy_lifecycle_failure_class": normalized_failure_class,
        "proxy_lifecycle_recovery_class": recovery_class or "hard_block_not_globally_recoverable",
    }


def proxy_lifecycle_parser_recovery_event_fields(
    *,
    source_name: str,
    proxy_recovered: bool,
) -> dict[str, object]:
    return {
        "proxy_lifecycle_state": PROXY_LIFECYCLE_PARSER_PROVEN_RECOVERY,
        "proxy_lifecycle_subreason": "parser_proven_source_specific_soft_gate_recovery",
        "proxy_lifecycle_operator_action": PROXY_LIFECYCLE_ACTION_SOURCE_SCOPED_RECOVERY,
        "proxy_lifecycle_failure_class": "soft_gate_false_positive",
        "proxy_lifecycle_recovery_class": "source_scoped_recovered_proxy_eligibility"
        if proxy_recovered
        else "source_scoped_host_recovery_only",
        "proxy_lifecycle_source": str(source_name or "").strip().lower(),
    }


def _normalize_proxy_url(value: str) -> str:
    raw = (value or "").strip()
    if not raw:
        return ""
    if "://" not in raw:
        raw = "http://" + raw
    parsed = urlparse(raw)
    if not parsed.netloc:
        return ""
    scheme = parsed.scheme or "http"
    return parsed._replace(scheme=scheme, params="", query="", fragment="").geturl()


def _proxy_url_from_dict(payload: dict[str, object]) -> str:
    raw_url = _normalize_proxy_url(str(payload.get("url", "")).strip())
    if raw_url:
        return raw_url
    host = str(payload.get("host", "")).strip()
    port = str(payload.get("port", "")).strip()
    if not host or not port:
        return ""
    proxy_type = str(payload.get("type", "") or payload.get("proxy_type", "")).strip().lower()
    scheme = "socks5" if proxy_type == "socks" else "http"
    user = str(payload.get("user", "")).strip()
    password = str(payload.get("pass", "") or payload.get("password", "")).strip()
    auth = f"{quote(user, safe='')}:{quote(password, safe='')}@" if user or password else ""
    return _normalize_proxy_url(f"{scheme}://{auth}{host}:{port}")


DEFAULT_PRIORITY_SOURCE_NAMES = frozenset({"checko"})
PROXY_ASSISTED_SOURCE_NAMES = frozenset(
    {"bicotender", "checko", "company_site", "route_fetch", "factory_site_fetch"}
)
PRIORITY_SOURCE_OBSERVATION_ONLY_FAILURE_REASONS = frozenset({"timeout", "request_error"})
RESERVED_CAPACITY_PROTECTED_FAILURE_SOURCE_NAMES = frozenset(
    {"company_site", "route_fetch", "factory_site_fetch"}
)


def _parse_source_names(raw_value: str | None) -> frozenset[str]:
    normalized_names = {
        str(item).strip().lower()
        for item in str(raw_value or "").split(",")
        if str(item).strip()
    }
    return frozenset(normalized_names)


def _normalize_proxy_failure_reason(reason: str | None) -> str:
    return str(reason or "").strip().lower()


class ProxyPool:
    def __init__(
        self,
        raw_value: str | None,
        *,
        proxy_file: str | Path | None = None,
        strategy: str | None = None,
        sticky_ttl_seconds: float = 900.0,
        ban_cooldown_seconds: float = 300.0,
    ) -> None:
        self.index = 0
        self.lock = Lock()
        self.strategy = (strategy or os.getenv("PARSER_PROXY_STRATEGY", "round_robin")).strip().lower()
        sticky_env = os.getenv("PARSER_PROXY_STICKY_TTL_SECONDS", "")
        cooldown_env = os.getenv("PARSER_PROXY_BAN_COOLDOWN_SECONDS", "")
        self.sticky_ttl_seconds = max(float(sticky_env or sticky_ttl_seconds), 1.0)
        self.ban_cooldown_seconds = max(float(cooldown_env or ban_cooldown_seconds), 5.0)
        priority_sources_env = os.getenv("PARSER_PROXY_PRIORITY_SOURCE_NAMES", "")
        reserved_capacity_env = os.getenv("PARSER_PROXY_PRIORITY_RESERVED_CAPACITY", "")
        self.priority_source_names = _parse_source_names(priority_sources_env) or DEFAULT_PRIORITY_SOURCE_NAMES
        try:
            configured_reserved_capacity = int(reserved_capacity_env or 1)
        except ValueError:
            configured_reserved_capacity = 1
        self.priority_reserved_capacity = max(configured_reserved_capacity, 0)
        self._sticky_by_host: dict[str, tuple[int, float]] = {}
        configured_file = proxy_file or os.getenv("PARSER_PROXIES_FILE", "").strip()
        entries: list[ProxyEntry] = []
        entries.extend(self._entries_from_raw(raw_value, source="env:PARSER_PROXIES"))
        entries.extend(self._entries_from_file(configured_file))
        self.entries = self._dedupe(entries)

    def _entries_from_raw(self, raw_value: str | None, *, source: str) -> list[ProxyEntry]:
        entries: list[ProxyEntry] = []
        for item in (raw_value or "").split(","):
            url = _normalize_proxy_url(item)
            if not url:
                continue
            parsed = urlparse(url)
            entries.append(
                ProxyEntry(
                    url=url,
                    source=source,
                    host=parsed.hostname or "",
                    port=str(parsed.port or ""),
                )
            )
        return entries

    def _entries_from_file(self, configured_file: str | Path | None) -> list[ProxyEntry]:
        if not configured_file:
            return []
        file_path = Path(configured_file).expanduser()
        if not file_path.exists():
            return []
        try:
            if file_path.suffix.lower() == ".json":
                payload = json.loads(file_path.read_text(encoding="utf-8"))
                return self._entries_from_json_payload(payload, source=f"file:{file_path}")
            return self._entries_from_text(file_path.read_text(encoding="utf-8"), source=f"file:{file_path}")
        except Exception:
            return []

    def _entries_from_text(self, payload: str, *, source: str) -> list[ProxyEntry]:
        entries: list[ProxyEntry] = []
        for raw_line in payload.splitlines():
            line = raw_line.strip()
            if not line or line.startswith("#"):
                continue
            url = _normalize_proxy_url(line)
            if not url:
                continue
            parsed = urlparse(url)
            entries.append(
                ProxyEntry(
                    url=url,
                    source=source,
                    host=parsed.hostname or "",
                    port=str(parsed.port or ""),
                )
            )
        return entries

    def _entries_from_json_payload(self, payload: object, *, source: str) -> list[ProxyEntry]:
        if isinstance(payload, dict):
            proxies_payload = payload.get("proxies")
            if not isinstance(proxies_payload, list):
                proxies_payload = payload.get("list")
            if isinstance(proxies_payload, list):
                items = proxies_payload
            else:
                items = [payload]
        elif isinstance(payload, list):
            items = payload
        else:
            items = []

        entries: list[ProxyEntry] = []
        for item in items:
            if isinstance(item, str):
                url = _normalize_proxy_url(item)
                if not url:
                    continue
                parsed = urlparse(url)
                entries.append(
                    ProxyEntry(
                        url=url,
                        source=source,
                        host=parsed.hostname or "",
                        port=str(parsed.port or ""),
                    )
                )
                continue
            if not isinstance(item, dict):
                continue
            url = _proxy_url_from_dict(item)
            if not url:
                continue
            parsed = urlparse(url)
            entries.append(
                ProxyEntry(
                    url=url,
                    source=source,
                    proxy_id=str(item.get("id", "")).strip(),
                    host=str(item.get("host", parsed.hostname or "")).strip(),
                    port=str(item.get("port", parsed.port or "")).strip(),
                    country=str(item.get("country", "")).strip().lower(),
                    descr=str(item.get("descr", "")).strip(),
                )
            )
        return entries

    def _dedupe(self, entries: list[ProxyEntry]) -> list[ProxyEntry]:
        result: list[ProxyEntry] = []
        seen: set[str] = set()
        for entry in entries:
            key = entry.url
            if key in seen:
                continue
            seen.add(key)
            result.append(entry)
        return result

    def _normalize_source_name(self, source_name: str | None) -> str:
        return str(source_name or "").strip().lower()

    def _is_priority_source(self, source_name: str | None) -> bool:
        normalized_source_name = self._normalize_source_name(source_name)
        return bool(normalized_source_name) and normalized_source_name in self.priority_source_names

    def _is_proxy_assisted_source(self, source_name: str | None) -> bool:
        normalized_source_name = self._normalize_source_name(source_name)
        if not normalized_source_name:
            return False
        return (
            normalized_source_name in PROXY_ASSISTED_SOURCE_NAMES
            or normalized_source_name in self.priority_source_names
        )

    def proxy_provider_diagnostic(
        self,
        *,
        force_refresh: bool = False,
    ) -> Proxy6InventoryDiagnostic:
        return diagnose_proxy6_inventory_from_env(force_refresh=force_refresh)

    def proxy_provider_attempt_guard(
        self,
        *,
        source_name: str | None = None,
    ) -> Proxy6InventoryDiagnostic | None:
        if not self._is_proxy_assisted_source(source_name):
            return None
        diagnostic = self.proxy_provider_diagnostic()
        if diagnostic.provider_status == PROXY_PROVIDER_INVENTORY_EMPTY_OR_EXPIRED:
            return diagnostic
        return None

    def sync_from_proxy_provider(
        self,
        *,
        source_name: str | None = None,
        diagnostic: Proxy6InventoryDiagnostic | None = None,
    ) -> ProxyProviderSyncResult:
        normalized_source_name = self._normalize_source_name(source_name)
        if not self._is_proxy_assisted_source(normalized_source_name):
            return ProxyProviderSyncResult(
                attempted=False,
                status="skipped_non_proxy_assisted_source",
                reason="source is not configured for proxy-provider sync",
            )

        usable_before = self.usable_count(source_name=normalized_source_name or None)
        provider_diagnostic = diagnostic or self.proxy_provider_diagnostic(force_refresh=True)
        if provider_diagnostic.provider_status != PROXY_PROVIDER_INVENTORY_HEALTHY:
            return ProxyProviderSyncResult(
                attempted=False,
                status="provider_not_healthy",
                reason=provider_diagnostic.reason,
                usable_count_before=usable_before,
                usable_count_after=usable_before,
                provider_status=provider_diagnostic.provider_status,
                provider_stop_class=provider_diagnostic.runtime_stop_class,
                operator_action=provider_diagnostic.operator_action,
            )

        sync_diagnostic, active_proxies = load_active_proxy6_parser_proxies_from_env(force_refresh=True)
        if sync_diagnostic.provider_status != PROXY_PROVIDER_INVENTORY_HEALTHY:
            return ProxyProviderSyncResult(
                attempted=True,
                status="provider_sync_failed",
                reason=sync_diagnostic.reason,
                usable_count_before=usable_before,
                usable_count_after=usable_before,
                provider_status=sync_diagnostic.provider_status,
                provider_stop_class=sync_diagnostic.runtime_stop_class,
                operator_action=sync_diagnostic.operator_action,
            )

        provider_entries: list[ProxyEntry] = []
        for proxy in active_proxies:
            url = _normalize_proxy_url(proxy.as_url())
            if not url:
                continue
            provider_entries.append(
                ProxyEntry(
                    url=url,
                    source="proxy6:runtime_sync",
                    proxy_id=proxy.id,
                    host=proxy.host,
                    port=proxy.port,
                    country=proxy.country,
                    descr=proxy.descr,
                )
            )

        added_count = 0
        refreshed_count = 0
        with self.lock:
            entries_by_url = {entry.url: entry for entry in self.entries}
            entries_by_id = {entry.proxy_id: entry for entry in self.entries if entry.proxy_id}
            for provider_entry in provider_entries:
                existing = entries_by_id.get(provider_entry.proxy_id) if provider_entry.proxy_id else None
                if existing is None:
                    existing = entries_by_url.get(provider_entry.url)
                if existing is None:
                    self.entries.append(provider_entry)
                    entries_by_url[provider_entry.url] = provider_entry
                    if provider_entry.proxy_id:
                        entries_by_id[provider_entry.proxy_id] = provider_entry
                    added_count += 1
                    continue
                if existing.url != provider_entry.url:
                    old_url = existing.url
                    existing.url = provider_entry.url
                    existing.source = provider_entry.source
                    existing.host = provider_entry.host
                    existing.port = provider_entry.port
                    existing.country = provider_entry.country
                    existing.descr = provider_entry.descr
                    existing.cooldown_until = 0.0
                    existing.failures = 0
                    existing.recovered_sources.clear()
                    entries_by_url.pop(old_url, None)
                    entries_by_url[existing.url] = existing
                    refreshed_count += 1
            now = time.time()
            usable_after = self._usable_count_for_source_locked(now, normalized_source_name or None)

        if usable_after > usable_before and usable_after > 0:
            status = "restored"
            reason = "provider active parser proxies materialized into runtime proxy pool"
        elif provider_entries:
            status = "no_new_usable_proxy"
            reason = "provider active parser proxies were already known and remain unavailable in runtime"
        else:
            status = "provider_returned_no_materializable_proxy"
            reason = "provider inventory is healthy but active parser proxies could not be materialized"

        return ProxyProviderSyncResult(
            attempted=True,
            status=status,
            reason=reason,
            added_count=added_count,
            refreshed_count=refreshed_count,
            provider_proxy_count=len(provider_entries),
            usable_count_before=usable_before,
            usable_count_after=usable_after,
            provider_status=sync_diagnostic.provider_status,
            provider_stop_class=sync_diagnostic.runtime_stop_class,
            operator_action=sync_diagnostic.operator_action,
        )

    def _reserved_capacity_floor(self) -> int:
        if not self.priority_source_names:
            return 0
        return min(self.priority_reserved_capacity, len(self.entries))

    def _usable_count_locked(self, now: float) -> int:
        return sum(1 for item in self.entries if item.cooldown_until <= now)

    def _entry_usable_for_source_locked(
        self,
        entry: ProxyEntry,
        now: float,
        source_name: str | None,
    ) -> bool:
        if entry.cooldown_until <= now:
            return True
        normalized_source_name = self._normalize_source_name(source_name)
        return bool(normalized_source_name) and normalized_source_name in entry.recovered_sources

    def _usable_count_for_source_locked(self, now: float, source_name: str | None) -> int:
        if source_name is None:
            usable_count = self._usable_count_locked(now)
        else:
            usable_count = sum(
                1 for item in self.entries if self._entry_usable_for_source_locked(item, now, source_name)
            )
        if source_name is None or self._is_priority_source(source_name):
            return usable_count
        return max(usable_count - self._reserved_capacity_floor(), 0)

    def _cooldown_active_count_locked(self, now: float) -> int:
        return sum(1 for item in self.entries if item.cooldown_until > now)

    def _recovered_source_count_locked(self, source_name: str | None) -> int:
        normalized_source_name = self._normalize_source_name(source_name)
        if not normalized_source_name:
            return sum(1 for item in self.entries if item.recovered_sources)
        return sum(1 for item in self.entries if normalized_source_name in item.recovered_sources)

    def lifecycle_snapshot(
        self,
        *,
        source_name: str | None = None,
        diagnostic: Proxy6InventoryDiagnostic | None = None,
        sync_fields: dict[str, object] | None = None,
        failure_class: str = "",
        recovery_class: str = "",
    ) -> ProxyLifecycleSnapshot:
        normalized_source_name = self._normalize_source_name(source_name)
        provider_diagnostic = diagnostic or self.proxy_provider_diagnostic()
        sync_payload = dict(sync_fields or {})
        sync_status = str(sync_payload.get("proxy_sync_status", "") or "").strip()
        sync_reason = str(sync_payload.get("proxy_sync_reason", "") or "").strip()
        with self.lock:
            now = time.time()
            runtime_entry_count = len(self.entries)
            global_usable_count = self._usable_count_locked(now)
            source_usable_count = self._usable_count_for_source_locked(now, normalized_source_name or None)
            cooldown_active_count = self._cooldown_active_count_locked(now)
            reserved_capacity = self._reserved_capacity_floor()
            recovered_source_count = self._recovered_source_count_locked(normalized_source_name or None)

        provider_status = provider_diagnostic.provider_status or PROXY_PROVIDER_STATUS_UNKNOWN
        provider_stop_class = provider_diagnostic.runtime_stop_class
        operator_action = provider_diagnostic.operator_action or PROXY_LIFECYCLE_ACTION_FAIL_CLOSED
        normalized_failure_class = _normalize_proxy_failure_reason(failure_class)
        normalized_recovery_class = str(recovery_class or "").strip()

        if provider_status == PROXY_PROVIDER_INVENTORY_EMPTY_OR_EXPIRED:
            state = PROXY_LIFECYCLE_PROVIDER_EMPTY_OR_EXPIRED
            subreason = "provider_has_no_active_usable_parser_proxy"
            recovery = normalized_recovery_class or "operator_top_up_or_renew_required"
        elif provider_status == PROXY_PROVIDER_STATUS_UNKNOWN:
            if runtime_entry_count == 0 and provider_diagnostic.missing_config:
                state = PROXY_LIFECYCLE_MISSING_RUNTIME_CONFIG
                subreason = "runtime_proxy_config_missing_and_provider_config_missing"
            else:
                state = PROXY_LIFECYCLE_PROVIDER_UNKNOWN
                subreason = "provider_status_unknown_or_api_unavailable"
            recovery = normalized_recovery_class or "operator_configure_or_sync_required"
        elif source_usable_count > 0:
            state = PROXY_LIFECYCLE_RUNTIME_SYNCED_USABLE
            subreason = "runtime_pool_has_source_eligible_proxy"
            operator_action = PROXY_LIFECYCLE_ACTION_NONE
            recovery = normalized_recovery_class or "selection_allowed"
        elif global_usable_count > 0:
            state = PROXY_LIFECYCLE_SOURCE_CAPACITY_BLOCKED
            subreason = "reserved_capacity_floor_or_source_specific_eligibility_prevents_selection"
            operator_action = PROXY_LIFECYCLE_ACTION_PRESERVE_RESERVED_CAPACITY
            recovery = normalized_recovery_class or "no_global_clear_without_source_specific_proof"
        elif runtime_entry_count > 0:
            state = PROXY_LIFECYCLE_RUNTIME_DEPLETED_PROVIDER_HEALTHY
            if sync_status == "no_new_usable_proxy":
                subreason = "same_url_provider_sync_noop_runtime_known_but_ineligible"
            else:
                subreason = "runtime_known_but_cooled_or_quarantined"
            recovery = normalized_recovery_class or "fail_closed_until_safe_runtime_repair_or_source_proof"
        else:
            state = PROXY_LIFECYCLE_RUNTIME_DEPLETED_PROVIDER_HEALTHY
            subreason = "provider_healthy_but_runtime_pool_empty"
            recovery = normalized_recovery_class or "sync_runtime_pool_or_fail_closed"

        return ProxyLifecycleSnapshot(
            source_name=normalized_source_name,
            state=state,
            subreason=subreason,
            operator_action=operator_action,
            failure_class=normalized_failure_class or state,
            recovery_class=recovery,
            provider_status=provider_status,
            provider_stop_class=provider_stop_class,
            sync_status=sync_status,
            sync_reason=sync_reason,
            runtime_entry_count=runtime_entry_count,
            runtime_global_usable_count=global_usable_count,
            runtime_source_usable_count=source_usable_count,
            runtime_cooldown_active_count=cooldown_active_count,
            runtime_reserved_capacity=reserved_capacity,
            runtime_recovered_source_count=recovered_source_count,
        )

    def _source_can_select_locked(self, now: float, source_name: str | None) -> bool:
        if source_name is None:
            return bool(self.entries)
        return self._usable_count_for_source_locked(now, source_name) > 0

    def _choose_next_index(self, now: float, *, source_name: str | None = None) -> int | None:
        if not self.entries:
            return None
        if not self._source_can_select_locked(now, source_name):
            return None
        count = len(self.entries)
        for offset in range(count):
            idx = (self.index + offset) % count
            entry = self.entries[idx]
            if not self._entry_usable_for_source_locked(entry, now, source_name):
                continue
            self.index = (idx + 1) % count
            return idx
        idx = self.index % count
        self.index = (idx + 1) % count
        return idx

    def _selection_from_entry(self, entry: ProxyEntry | None) -> ProxySelection:
        if entry is None:
            return ProxySelection()
        return ProxySelection(
            url=entry.url,
            source=entry.source,
            proxy_id=entry.proxy_id,
            label=entry.label,
            host=entry.host,
            port=entry.port,
            country=entry.country,
            via_proxy=True,
        )

    def select(self, host: str | None = None, *, source_name: str | None = None) -> ProxySelection:
        if not self.entries:
            return ProxySelection()
        if self.proxy_provider_attempt_guard(source_name=source_name) is not None:
            return ProxySelection()
        host_key = (host or "").strip().lower()
        now = time.time()
        with self.lock:
            if not self._source_can_select_locked(now, source_name):
                return ProxySelection()
            idx: int | None = None
            if self.strategy == "sticky_by_host" and host_key:
                sticky = self._sticky_by_host.get(host_key)
                if sticky and sticky[1] > now and 0 <= sticky[0] < len(self.entries):
                    sticky_entry = self.entries[sticky[0]]
                    if sticky_entry.cooldown_until <= now:
                        idx = sticky[0]
                if idx is None:
                    idx = self._choose_next_index(now, source_name=source_name)
                    if idx is not None:
                        self._sticky_by_host[host_key] = (idx, now + self.sticky_ttl_seconds)
            else:
                idx = self._choose_next_index(now, source_name=source_name)
            if idx is None:
                return ProxySelection()
            return self._selection_from_entry(self.entries[idx])

    def next_for_host(self, host: str | None = None, *, source_name: str | None = None) -> dict[str, str] | None:
        selection = self.select(host, source_name=source_name)
        return selection.requests_proxies

    def next(self) -> dict[str, str] | None:
        return self.next_for_host(None)

    def mark_bad(self, proxy_url: str | None, *, reason: str = "", source_name: str | None = None) -> None:
        value = _normalize_proxy_url(proxy_url or "")
        if not value:
            return
        reason_label = _normalize_proxy_failure_reason(reason)
        normalized_source_name = self._normalize_source_name(source_name)
        with self.lock:
            now = time.time()
            protected_capacity = self._reserved_capacity_floor()
            for entry in self.entries:
                if entry.url != value:
                    continue
                if (
                    self._is_priority_source(source_name)
                    and reason_label in PRIORITY_SOURCE_OBSERVATION_ONLY_FAILURE_REASONS
                ):
                    entry.descr = (entry.descr + f" | observed:{reason_label}")[:150]
                    break
                if (
                    source_name is not None
                    and not self._is_priority_source(source_name)
                    and normalized_source_name in RESERVED_CAPACITY_PROTECTED_FAILURE_SOURCE_NAMES
                    and protected_capacity > 0
                    and entry.cooldown_until <= now
                    and self._usable_count_locked(now) <= protected_capacity
                ):
                    if reason:
                        entry.descr = (entry.descr + f" | preserved:{reason}")[:150]
                    break
                if normalized_source_name:
                    entry.recovered_sources.discard(normalized_source_name)
                else:
                    entry.recovered_sources.clear()
                entry.failures += 1
                cooldown_multiplier = min(entry.failures, 4)
                entry.cooldown_until = max(entry.cooldown_until, now + self.ban_cooldown_seconds * cooldown_multiplier)
                if reason:
                    entry.descr = (entry.descr + f" | bad:{reason}")[:150]
                break

    def mark_ok(self, proxy_url: str | None, *, source_name: str | None = None) -> None:
        value = _normalize_proxy_url(proxy_url or "")
        if not value:
            return
        with self.lock:
            for entry in self.entries:
                if entry.url != value:
                    continue
                entry.cooldown_until = 0.0
                entry.failures = max(entry.failures - 1, 0)
                break

    def mark_ok_by_label_or_id(self, proxy_label_or_id: str | None, *, source_name: str | None = None) -> bool:
        value = str(proxy_label_or_id or "").strip()
        if not value:
            return False
        normalized_url = _normalize_proxy_url(value)
        normalized_source_name = self._normalize_source_name(source_name)
        with self.lock:
            for entry in self.entries:
                if (
                    value == entry.proxy_id
                    or value == entry.label
                    or value == entry.url
                    or (normalized_url and normalized_url == entry.url)
                ):
                    entry.cooldown_until = 0.0
                    entry.failures = max(entry.failures - 1, 0)
                    if normalized_source_name:
                        entry.recovered_sources.add(normalized_source_name)
                    return True
        return False

    def enabled(self) -> bool:
        return bool(self.entries)

    def usable_count(self, *, source_name: str | None = None) -> int:
        with self.lock:
            now = time.time()
            return self._usable_count_for_source_locked(now, source_name)

    def describe(self) -> dict[str, object]:
        with self.lock:
            now = time.time()
            cooldown_active = sum(1 for item in self.entries if item.cooldown_until > now)
            return {
                "enabled": bool(self.entries),
                "count": len(self.entries),
                "strategy": self.strategy,
                "sticky_ttl_seconds": self.sticky_ttl_seconds,
                "ban_cooldown_seconds": self.ban_cooldown_seconds,
                "priority_source_names": sorted(self.priority_source_names),
                "priority_reserved_capacity": self._reserved_capacity_floor(),
                "cooldown_active": cooldown_active,
                "items": [
                    {
                        "label": item.label,
                        "proxy_id": item.proxy_id,
                        "country": item.country,
                        "failures": item.failures,
                        "cooldown_until": item.cooldown_until,
                        "recovered_sources": sorted(item.recovered_sources),
                        "source": item.source,
                    }
                    for item in self.entries
                ],
            }


__all__ = [
    "PROXY_ASSISTED_SOURCE_NAMES",
    "PROXY_LIFECYCLE_ACTION_FAIL_CLOSED",
    "PROXY_LIFECYCLE_ACTION_NONE",
    "PROXY_LIFECYCLE_ACTION_PRESERVE_RESERVED_CAPACITY",
    "PROXY_LIFECYCLE_ACTION_SOURCE_SCOPED_RECOVERY",
    "PROXY_LIFECYCLE_HARD_FAILURE_CLASSES",
    "PROXY_LIFECYCLE_MISSING_RUNTIME_CONFIG",
    "PROXY_LIFECYCLE_PARSER_PROVEN_RECOVERY",
    "PROXY_LIFECYCLE_PROVIDER_EMPTY_OR_EXPIRED",
    "PROXY_LIFECYCLE_PROVIDER_UNKNOWN",
    "PROXY_LIFECYCLE_RUNTIME_DEPLETED_PROVIDER_HEALTHY",
    "PROXY_LIFECYCLE_RUNTIME_SYNCED_USABLE",
    "PROXY_LIFECYCLE_SOURCE_CAPACITY_BLOCKED",
    "PROXY_LIFECYCLE_TRUE_HARD_BLOCK",
    "ProxyEntry",
    "ProxyLifecycleSnapshot",
    "ProxyPool",
    "ProxyProviderSyncResult",
    "ProxySelection",
    "proxy_lifecycle_hard_block_event_fields",
    "proxy_lifecycle_parser_recovery_event_fields",
]

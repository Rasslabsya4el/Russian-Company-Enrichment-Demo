from __future__ import annotations

import os
import time
from dataclasses import asdict, dataclass, field, replace
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

import requests
from bs4 import BeautifulSoup

from app.runtime import ProxyPool, ProxySelection
from app.runtime.proxy6 import (
    PROXY_PROVIDER_INVENTORY_EMPTY_OR_EXPIRED,
    PROXY_PROVIDER_INVENTORY_HEALTHY,
    PROXY_PROVIDER_STATUS_UNKNOWN,
    diagnose_proxy6_inventory_from_env,
)
from app.runtime.host_memory import normalize_host_memory_state
from app.runtime.host_governor import HostGovernorPreflight, resolve_host_governor_preflight

from .antibot import (
    ACCESS_STATE_BLOCKED,
    ACCESS_STATE_COMPLETED_WITH_CONTENT,
    ACCESS_STATE_MANUAL_HANDOFF_REQUIRED,
    ACCESS_STATE_PAUSED_BY_BREAKER,
    ACCESS_STATE_RECOVERED,
    BLOCK_CLASS_CHALLENGE_LOOP,
    BLOCK_CLASS_HARD_BAN,
    BLOCK_CLASS_RATE_LIMIT,
    BLOCK_CLASS_SUCCESS,
    BREAKER_MODE_PAUSED,
    BREAKER_MODE_SURVIVAL,
    DEFAULT_RETRY_BUDGET,
    DomainBreakerRegistry,
    FactorySiteSessionStore,
    SessionProfile,
    TRANSPORT_PLAYWRIGHT,
    TRANSPORT_REQUESTS,
    apply_session_profile_to_requests_session,
    classify_fetch_attempt,
    derive_access_state,
    resolve_route_transport_policy,
    route_is_high_value,
)
from .common import normalize_whitespace


@dataclass
class FetchTelemetry:
    host: str
    url: str
    fetch_mode: str
    route_family: str = ""
    section_name: str = ""
    proxy_mode: str = "direct"
    proxy_label_or_id: str = ""
    proxy_id: str = ""
    timeout: bool = False
    blocked: bool = False
    playwright_fallback_used: bool = False
    status: str = ""
    http_status: int | None = None
    block_class: str = ""
    anti_bot_reason: str = ""
    attempt_no: int = 0
    escalated_from: str = ""
    escalated_to: str = ""
    session_reused: bool = False
    breaker_mode: str = "normal"
    manual_handoff_required: bool = False
    playwright_used: bool = False
    challenge_detected: bool = False
    access_state: str = ""
    transport_selected: str = ""
    transport_final: str = ""
    escalation_reason: str = ""
    blocked_by_policy: bool = False
    cooldown_seconds: int = 0
    retry_budget: int = 0

    def to_trace(self) -> dict[str, Any]:
        return asdict(self)


@dataclass
class FetchResult:
    response: requests.Response | None
    status: str
    notes: list[str]
    access_state: str = ACCESS_STATE_BLOCKED
    block_class: str = ""
    anti_bot_reason: str = ""
    breaker_mode: str = "normal"
    manual_handoff_required: bool = False
    challenge_detected: bool = False
    session_reused: bool = False
    completed_with_content: bool = False
    route_family: str = ""
    section_name: str = ""
    transport_selected: str = ""
    transport_final: str = ""
    escalation_reason: str = ""
    blocked_by_policy: bool = False
    attempts: list[FetchTelemetry] = field(default_factory=list)


@dataclass
class _AttemptExecution:
    attempt_mode: str
    response: requests.Response | None
    status: str
    notes: list[str]
    proxy_selection: ProxySelection
    timeout: bool = False
    blocked: bool = False
    session_reused: bool = False
    cooldown_seconds: int = 0
    playwright_used: bool = False
    storage_payload: dict[str, Any] | None = None
    browser_user_agent: str = ""
    final_url: str = ""


@dataclass(frozen=True)
class _PolicyAction:
    action: str
    next_transport: str = ""
    reason: str = ""
    blocked_by_policy: bool = False
    note: str = ""
    cooldown_seconds: int = 0
    retry_budget: int = 0
    transport_final: str = ""


BROWSER_ESCALATION_ROUTE_FAMILIES = frozenset(
    {
        "homepage",
        "about",
        "company/about",
        "contacts",
        "products",
        "production/products",
        "services",
        "files",
        "search",
        "documents",
        "docs/certificates",
        "procurement",
        "purchases",
        "tenders",
        "surplus/realization",
    }
)
BROWSER_ESCALATION_SECTIONS = frozenset({"homepage", "about", "contacts", "products", "services", "files", "search", "documents", "procurement", "sales"})
SOFT_CONTENT_PROXY_REUSE_REASONS = frozenset(
    {
        "thin_html",
        "suspiciously_thin_html",
        "empty_js_shell",
        "browser_required",
        "redirect_loop",
    }
)


class Fetcher:
    def __init__(self, client: Any, *, proxy_pool: ProxyPool | None = None) -> None:
        self.client = client
        self.proxy_pool = self._resolve_proxy_pool(client, proxy_pool)
        self.playwright_enabled = os.getenv("ENABLE_PLAYWRIGHT_SITE_FETCH", "0").strip() in {"1", "true", "True"}
        self.playwright_timeout_ms = int(os.getenv("PLAYWRIGHT_SITE_FETCH_TIMEOUT_MS", "20000"))
        self.playwright_headless = os.getenv("PLAYWRIGHT_SITE_FETCH_HEADLESS", "1").strip() not in {"0", "false", "False"}
        self.max_attempts = max(1, int(os.getenv("FACTORY_SITE_FETCH_MAX_ATTEMPTS", "2")))
        self.max_rate_limit_backoff = max(float(os.getenv("FACTORY_SITE_RATE_LIMIT_MAX_BACKOFF_SECONDS", "5")), 0.0)
        self.default_rate_limit_cooldown_seconds = max(int(os.getenv("FACTORY_SITE_RATE_LIMIT_DEFAULT_COOLDOWN_SECONDS", "30") or 30), 0)
        self.default_block_cooldown_seconds = max(int(os.getenv("FACTORY_SITE_BLOCK_DEFAULT_COOLDOWN_SECONDS", "45") or 45), 0)
        self.session_store = FactorySiteSessionStore()
        self.breakers = DomainBreakerRegistry()
        self._host_cooldowns: dict[str, float] = {}
        self._host_proxy_rotation_hints: dict[str, str] = {}
        self.last_fetch_telemetry: FetchTelemetry | None = None
        self.last_fetch_result: FetchResult | None = None

    def fetch(
        self,
        url: str,
        mode: str,
        *,
        route_family: str = "",
        section_name: str = "",
    ) -> tuple[requests.Response | None, str, list[str]]:
        host = self._host_from_url(url)
        requested_mode = normalize_whitespace(mode).lower() or "requests"
        normalized_route_family = self._normalize_policy_value(route_family)
        normalized_section_name = self._normalize_policy_value(section_name)
        self.last_fetch_telemetry = None
        self.last_fetch_result = None

        if requested_mode == "skip":
            telemetry = FetchTelemetry(
                host=host,
                url=url,
                fetch_mode="skip",
                route_family=normalized_route_family,
                section_name=normalized_section_name,
                status="skipped",
                access_state=ACCESS_STATE_BLOCKED,
                transport_selected="skip",
                transport_final="skip",
                escalation_reason="requested_skip",
                blocked_by_policy=True,
            )
            self.last_fetch_telemetry = telemetry
            self.last_fetch_result = FetchResult(
                response=None,
                status="skipped",
                notes=["route strategy пометила маршрут как skip"],
                access_state=ACCESS_STATE_BLOCKED,
                route_family=normalized_route_family,
                section_name=normalized_section_name,
                transport_selected="skip",
                transport_final="skip",
                escalation_reason="requested_skip",
                blocked_by_policy=True,
                attempts=[telemetry],
            )
            return None, "skipped", ["route strategy пометила маршрут как skip"]

        persisted_session_profile = self.session_store.load(host)
        preflight = self._runtime_host_governor_preflight(host)
        runtime_blocked_proxy_labels = set(preflight.avoid_proxy_labels_or_ids)
        session_profile = persisted_session_profile
        session_profile = self._runtime_guard_session_profile(
            session_profile=session_profile,
            blocked_proxy_labels=runtime_blocked_proxy_labels,
        )
        self._seed_host_cooldown_from_preflight(host, preflight)
        self._bootstrap_breaker_from_runtime_host_memory(host)
        breaker_state = self.breakers.state_for_host(host)
        survival_bootstrap_proxy: ProxySelection | None = None
        if (
            breaker_state.mode == BREAKER_MODE_SURVIVAL
            and persisted_session_profile is not None
            and session_profile is None
        ):
            survival_bootstrap_proxy = self._select_safe_survival_bootstrap_proxy(
                host,
                blocked_proxy_labels=runtime_blocked_proxy_labels,
            )
        if (
            breaker_state.mode == BREAKER_MODE_PAUSED
            or (
                breaker_state.mode == BREAKER_MODE_SURVIVAL
                and session_profile is None
                and survival_bootstrap_proxy is None
            )
        ):
            paused_result = self._paused_by_breaker_result(
                url=url,
                host=host,
                requested_mode=requested_mode,
                breaker_mode=breaker_state.mode,
                route_family=normalized_route_family,
                section_name=normalized_section_name,
            )
            return paused_result.response, paused_result.status, paused_result.notes

        initial_policy = self._select_initial_transport(
            requested_mode=requested_mode,
            route_family=normalized_route_family,
            section_name=normalized_section_name,
            breaker_mode=breaker_state.mode,
            session_profile=session_profile,
            allow_fresh_session_bootstrap=survival_bootstrap_proxy is not None,
        )
        attempt_mode = initial_policy.next_transport or "requests"

        attempts: list[FetchTelemetry] = []
        aggregated_notes: list[str] = []

        first_attempt = self._execute_attempt(
            url=url,
            host=host,
            attempt_mode=attempt_mode,
            attempt_no=1,
            requested_mode=requested_mode,
            session_profile=session_profile,
            blocked_proxy_labels=runtime_blocked_proxy_labels,
            escalated_from="",
            escalated_to="",
            route_family=normalized_route_family,
            section_name=normalized_section_name,
            transport_selected=attempt_mode,
            escalation_reason=initial_policy.reason,
            retry_budget=initial_policy.retry_budget,
            proxy_selection=survival_bootstrap_proxy,
        )
        attempts.append(first_attempt["telemetry"])
        aggregated_notes.extend(first_attempt["notes"])

        first_result = self._finalize_from_attempt(first_attempt, attempts=attempts, notes=aggregated_notes)
        if first_result.completed_with_content:
            return first_result.response, first_result.status, first_result.notes

        if self.max_attempts < 2:
            return first_result.response, first_result.status, first_result.notes

        followup = self._resolve_followup_action(
            host=host,
            requested_mode=requested_mode,
            route_family=normalized_route_family,
            section_name=normalized_section_name,
            current_attempt_mode=attempt_mode,
            attempt_no=1,
            execution=first_attempt["execution"],
            decision=first_attempt["decision"],
            response=first_attempt["execution"].response,
        )
        self._apply_followup_to_telemetry(first_attempt["telemetry"], followup)
        if followup.note:
            aggregated_notes.append(followup.note)

        if followup.action == "stop":
            if followup.blocked_by_policy:
                self._append_terminal_progress_event(
                    first_attempt["telemetry"],
                    status="blocked_by_policy",
                    access_state=first_attempt["telemetry"].access_state or ACCESS_STATE_BLOCKED,
                )
                blocked_result = self._blocked_result(
                    response=first_attempt["execution"].response,
                    status="blocked_by_policy",
                    notes=aggregated_notes,
                    access_state=ACCESS_STATE_BLOCKED,
                    block_class=first_attempt["decision"].block_class,
                    anti_bot_reason=first_attempt["decision"].anti_bot_reason or "blocked_by_policy",
                    breaker_mode=first_attempt["telemetry"].breaker_mode,
                    session_reused=first_attempt["execution"].session_reused,
                    challenge_detected=first_attempt["decision"].challenge_detected,
                    route_family=normalized_route_family,
                    section_name=normalized_section_name,
                    attempts=attempts,
                )
                return blocked_result.response, blocked_result.status, blocked_result.notes
            return first_result.response, first_result.status, first_result.notes

        if followup.action == "retry":
            followup_cooldown_remaining = self._host_cooldown_remaining(host)
            if followup_cooldown_remaining > 0:
                self._apply_followup_to_telemetry(
                    first_attempt["telemetry"],
                    _PolicyAction(
                        action="stop",
                        blocked_by_policy=True,
                        note=f"host cooldown active for {followup_cooldown_remaining}s",
                        transport_final=first_attempt["telemetry"].transport_final or first_attempt["telemetry"].fetch_mode,
                    ),
                )
                aggregated_notes.append(f"host cooldown active for {followup_cooldown_remaining}s")
                self._append_terminal_progress_event(
                    first_attempt["telemetry"],
                    status="blocked_by_policy",
                    access_state=first_attempt["telemetry"].access_state or ACCESS_STATE_BLOCKED,
                )
                blocked_result = self._blocked_result(
                    response=None,
                    status="blocked_by_policy",
                    notes=aggregated_notes,
                    access_state=first_attempt["telemetry"].access_state,
                    block_class=first_attempt["decision"].block_class,
                    anti_bot_reason=first_attempt["decision"].anti_bot_reason or "blocked_by_policy",
                    breaker_mode=first_attempt["telemetry"].breaker_mode,
                    session_reused=first_attempt["execution"].session_reused,
                    challenge_detected=first_attempt["decision"].challenge_detected,
                    route_family=normalized_route_family,
                    section_name=normalized_section_name,
                    attempts=attempts,
                )
                return blocked_result.response, blocked_result.status, blocked_result.notes
            self._sleep_backoff(followup.cooldown_seconds or first_attempt["execution"].cooldown_seconds)
            retry_attempt = self._execute_attempt(
                url=url,
                host=host,
                attempt_mode=followup.next_transport or "requests",
                attempt_no=2,
                requested_mode=requested_mode,
                session_profile=self._load_runtime_guarded_session_profile(
                    host,
                    blocked_proxy_labels=runtime_blocked_proxy_labels,
                ),
                blocked_proxy_labels=runtime_blocked_proxy_labels,
                escalated_from=first_attempt["telemetry"].transport_selected or first_attempt["telemetry"].fetch_mode,
                escalated_to=followup.next_transport or "requests",
                route_family=normalized_route_family,
                section_name=normalized_section_name,
                transport_selected=followup.next_transport or "requests",
                escalation_reason=followup.reason,
                retry_budget=followup.retry_budget,
            )
            attempts.append(retry_attempt["telemetry"])
            aggregated_notes.extend(retry_attempt["notes"])
            retry_result = self._finalize_from_attempt(retry_attempt, attempts=attempts, notes=aggregated_notes)
            return retry_result.response, retry_result.status, retry_result.notes

        blocked_proxy_labels: set[str] = set(runtime_blocked_proxy_labels)
        allow_same_proxy_for_browser_escalation = self._allow_same_proxy_for_browser_escalation(
            followup=followup,
            decision=first_attempt["decision"],
        )
        if (
            not allow_same_proxy_for_browser_escalation
            and first_attempt["decision"].block_class != BLOCK_CLASS_RATE_LIMIT
            and first_attempt["execution"].proxy_selection.via_proxy
        ):
            blocked_proxy_labels.add(first_attempt["execution"].proxy_selection.proxy_label_or_id)

        followup_cooldown_remaining = self._host_cooldown_remaining(host)
        if followup_cooldown_remaining > 0 and not blocked_proxy_labels:
            self._apply_followup_to_telemetry(
                first_attempt["telemetry"],
                _PolicyAction(
                    action="stop",
                    blocked_by_policy=True,
                    note=f"host cooldown active for {followup_cooldown_remaining}s",
                    transport_final=first_attempt["telemetry"].transport_final or first_attempt["telemetry"].fetch_mode,
                ),
            )
            aggregated_notes.append(f"host cooldown active for {followup_cooldown_remaining}s")
            self._append_terminal_progress_event(
                first_attempt["telemetry"],
                status="blocked_by_policy",
                access_state=first_attempt["telemetry"].access_state or ACCESS_STATE_BLOCKED,
            )
            blocked_result = self._blocked_result(
                response=None,
                status="blocked_by_policy",
                notes=aggregated_notes,
                access_state=first_attempt["telemetry"].access_state,
                block_class=first_attempt["decision"].block_class,
                anti_bot_reason=first_attempt["decision"].anti_bot_reason or "blocked_by_policy",
                breaker_mode=first_attempt["telemetry"].breaker_mode,
                session_reused=first_attempt["execution"].session_reused,
                challenge_detected=first_attempt["decision"].challenge_detected,
                route_family=normalized_route_family,
                section_name=normalized_section_name,
                attempts=attempts,
            )
            return blocked_result.response, blocked_result.status, blocked_result.notes

        second_attempt = self._execute_attempt(
            url=url,
            host=host,
            attempt_mode=followup.next_transport or "playwright",
            attempt_no=2,
            requested_mode=requested_mode,
            session_profile=self._load_runtime_guarded_session_profile(
                host,
                blocked_proxy_labels=blocked_proxy_labels,
            ),
            blocked_proxy_labels=blocked_proxy_labels,
            escalated_from=first_attempt["telemetry"].transport_selected or first_attempt["telemetry"].fetch_mode,
            escalated_to=followup.next_transport or "playwright",
            route_family=normalized_route_family,
            section_name=normalized_section_name,
            transport_selected=followup.next_transport or "playwright",
            escalation_reason=followup.reason,
            retry_budget=followup.retry_budget,
        )
        attempts.append(second_attempt["telemetry"])
        aggregated_notes.extend(second_attempt["notes"])
        self._apply_followup_to_telemetry(
            second_attempt["telemetry"],
            _PolicyAction(
                action="stop",
                reason=followup.reason,
                transport_final=second_attempt["telemetry"].fetch_mode,
            ),
        )
        second_result = self._finalize_from_attempt(second_attempt, attempts=attempts, notes=aggregated_notes)
        return second_result.response, second_result.status, second_result.notes

    def _execute_attempt(
        self,
        *,
        url: str,
        host: str,
        attempt_mode: str,
        attempt_no: int,
        requested_mode: str,
        session_profile: SessionProfile | None,
        blocked_proxy_labels: set[str],
        escalated_from: str,
        escalated_to: str,
        route_family: str,
        section_name: str,
        transport_selected: str,
        escalation_reason: str,
        retry_budget: int,
        proxy_selection: ProxySelection | None = None,
    ) -> dict[str, Any]:
        cooldown_remaining = self._host_cooldown_remaining(host)
        proxy_selection = proxy_selection or self._select_proxy(host, blocked_proxy_labels=blocked_proxy_labels)
        proxy_provider_fields = self._proxy_provider_event_fields(proxy_selection)
        if (
            blocked_proxy_labels
            and proxy_selection.via_proxy
            and proxy_selection.proxy_label_or_id in blocked_proxy_labels
        ):
            execution = _AttemptExecution(
                attempt_mode=attempt_mode,
                response=None,
                status="blocked_no_alternative_proxy",
                notes=["recovery stopped: same proxy would be reused after prior block"],
                proxy_selection=proxy_selection,
                blocked=True,
            )
        elif cooldown_remaining > 0:
            execution = _AttemptExecution(
                attempt_mode=attempt_mode,
                response=None,
                status="cooldown_active",
                notes=[f"host cooldown active for {cooldown_remaining}s"],
                proxy_selection=proxy_selection,
                blocked=True,
                cooldown_seconds=cooldown_remaining,
            )
        elif attempt_mode == "playwright":
            execution = self._playwright_fetch(
                url,
                proxy_selection=proxy_selection,
                session_profile=session_profile,
            )
        else:
            execution = self._requests_fetch(
                url,
                proxy_selection=proxy_selection,
                session_profile=session_profile,
            )

        breaker_before = self.breakers.state_for_host(host)
        cooldown_active, cooldown_remaining = self.breakers.cooldown_status(host)
        usable_content = bool(execution.response) and self._response_has_usable_html(execution.response)
        decision = classify_fetch_attempt(
            status=execution.status,
            response=execution.response,
            usable_content=usable_content,
            browser_attempt=attempt_mode == "playwright",
            session_reused=execution.session_reused,
            host=host,
            requested_mode=requested_mode,
            route_family=route_family,
            section_name=section_name,
            attempt_no=attempt_no,
            retry_budget=max(retry_budget, DEFAULT_RETRY_BUDGET),
            current_transport=transport_selected or attempt_mode,
            breaker_mode=breaker_before.mode,
            cooldown_active=execution.status == "cooldown_active" or cooldown_active,
            cooldown_remaining_seconds=cooldown_remaining,
            response_text=(execution.response.text if execution.response is not None else ""),
        )
        if execution.status == "blocked_no_alternative_proxy":
            decision.anti_bot_reason = "blocked_no_alternative_proxy"
            decision.should_escalate = False
            decision.should_retry = False
            decision.blocked_by_policy = True
            decision.transport_final = attempt_mode
            decision.escalation_reason = "proxy_reuse_policy_denied"

        applied_cooldown_seconds = self._update_host_cooldown(
            host,
            block_class=decision.block_class,
            cooldown_seconds=max(execution.cooldown_seconds, int(decision.cooldown_seconds or 0)),
        )
        breaker_state = self.breakers.record(
            host,
            decision,
            proxy_label_or_id=proxy_selection.proxy_label_or_id,
        )
        access_state = derive_access_state(
            decision=decision,
            breaker_mode=breaker_state.mode,
            attempt_no=attempt_no,
            session_reused=execution.session_reused,
        )
        status = execution.status
        if decision.manual_handoff_required:
            status = ACCESS_STATE_MANUAL_HANDOFF_REQUIRED
        elif decision.usable_content:
            status = "success"
        elif status == "success":
            status = decision.anti_bot_reason or "blocked"

        telemetry = self._build_telemetry(
            url=url,
            fetch_mode=attempt_mode,
            route_family=route_family,
            section_name=section_name,
            proxy_selection=proxy_selection,
            status=status,
            response=execution.response,
            timeout=execution.timeout,
            blocked=decision.block_class != BLOCK_CLASS_SUCCESS,
            playwright_fallback_used=attempt_no > 1 and execution.playwright_used,
            proxy_mode="proxy" if proxy_selection.via_proxy else "direct",
            proxy_label_or_id=proxy_selection.proxy_label_or_id,
            proxy_id=proxy_selection.proxy_id,
            block_class=decision.block_class,
            anti_bot_reason=decision.anti_bot_reason,
            attempt_no=attempt_no,
            escalated_from=escalated_from,
            escalated_to=escalated_to,
            session_reused=execution.session_reused,
            breaker_mode=breaker_state.mode,
            manual_handoff_required=decision.manual_handoff_required,
            playwright_used=execution.playwright_used,
            challenge_detected=decision.challenge_detected,
            access_state=access_state,
            transport_selected=decision.transport_selected or transport_selected or attempt_mode,
            transport_final=decision.transport_final or attempt_mode,
            escalation_reason=decision.escalation_reason or escalation_reason,
            blocked_by_policy=decision.blocked_by_policy or execution.status in {"cooldown_active", "blocked_no_alternative_proxy"},
            cooldown_seconds=max(applied_cooldown_seconds, int(decision.cooldown_seconds or 0)),
            retry_budget=max(decision.retry_budget_remaining, retry_budget),
        )
        self._append_progress_event(
            "route_fetch_attempt" if attempt_mode == "requests" else "route_fetch_playwright",
            telemetry,
            **proxy_provider_fields,
        )
        return {
            "execution": execution,
            "decision": decision,
            "telemetry": telemetry,
            "notes": execution.notes,
            "access_state": access_state,
            "requested_mode": requested_mode,
        }

    def _finalize_from_attempt(
        self,
        attempt: dict[str, Any],
        *,
        attempts: list[FetchTelemetry],
        notes: list[str],
    ) -> FetchResult:
        execution: _AttemptExecution = attempt["execution"]
        decision = attempt["decision"]
        telemetry: FetchTelemetry = attempt["telemetry"]
        result_notes = [note for note in notes if note]
        access_state = telemetry.access_state or ACCESS_STATE_BLOCKED
        completed = access_state in {ACCESS_STATE_COMPLETED_WITH_CONTENT, ACCESS_STATE_RECOVERED}
        status = execution.status
        if decision.manual_handoff_required:
            status = ACCESS_STATE_MANUAL_HANDOFF_REQUIRED
        elif completed:
            status = "success"
        elif access_state == ACCESS_STATE_PAUSED_BY_BREAKER:
            status = ACCESS_STATE_PAUSED_BY_BREAKER
        elif telemetry.blocked_by_policy:
            status = "blocked_by_policy"
        result = FetchResult(
            response=execution.response,
            status=status,
            notes=result_notes,
            access_state=access_state,
            block_class=telemetry.block_class,
            anti_bot_reason=telemetry.anti_bot_reason,
            breaker_mode=telemetry.breaker_mode,
            manual_handoff_required=telemetry.manual_handoff_required,
            challenge_detected=telemetry.challenge_detected,
            session_reused=telemetry.session_reused,
            completed_with_content=completed,
            route_family=telemetry.route_family,
            section_name=telemetry.section_name,
            transport_selected=(attempts[0].transport_selected if attempts else telemetry.transport_selected),
            transport_final=telemetry.transport_final or telemetry.fetch_mode,
            escalation_reason=telemetry.escalation_reason,
            blocked_by_policy=any(item.blocked_by_policy for item in attempts),
            attempts=list(attempts),
        )
        self.last_fetch_telemetry = attempts[-1] if attempts else None
        self.last_fetch_result = result
        return result

    def _paused_by_breaker_result(
        self,
        *,
        url: str,
        host: str,
        requested_mode: str,
        breaker_mode: str,
        route_family: str,
        section_name: str,
    ) -> FetchResult:
        telemetry = FetchTelemetry(
            host=host,
            url=url,
            fetch_mode=requested_mode,
            route_family=route_family,
            section_name=section_name,
            status=ACCESS_STATE_PAUSED_BY_BREAKER,
            block_class=BLOCK_CLASS_HARD_BAN,
            anti_bot_reason="paused_by_breaker",
            breaker_mode=breaker_mode,
            blocked=True,
            access_state=ACCESS_STATE_PAUSED_BY_BREAKER,
            transport_selected=requested_mode,
            transport_final=requested_mode,
            escalation_reason="paused_by_breaker",
            blocked_by_policy=True,
        )
        self._append_progress_event("route_fetch_breaker_pause", telemetry)
        result = FetchResult(
            response=None,
            status=ACCESS_STATE_PAUSED_BY_BREAKER,
            notes=[f"route paused by {breaker_mode} breaker"],
            access_state=ACCESS_STATE_PAUSED_BY_BREAKER,
            block_class=BLOCK_CLASS_HARD_BAN,
            anti_bot_reason="paused_by_breaker",
            breaker_mode=breaker_mode,
            route_family=route_family,
            section_name=section_name,
            transport_selected=requested_mode,
            transport_final=requested_mode,
            escalation_reason="paused_by_breaker",
            blocked_by_policy=True,
            attempts=[telemetry],
        )
        self.last_fetch_telemetry = telemetry
        self.last_fetch_result = result
        return result

    def _blocked_result(
        self,
        *,
        response: requests.Response | None,
        status: str,
        notes: list[str],
        access_state: str,
        block_class: str,
        anti_bot_reason: str,
        breaker_mode: str,
        session_reused: bool,
        challenge_detected: bool,
        route_family: str,
        section_name: str,
        attempts: list[FetchTelemetry],
    ) -> FetchResult:
        latest = attempts[-1] if attempts else None
        result = FetchResult(
            response=response,
            status=status,
            notes=notes,
            access_state=access_state,
            block_class=block_class,
            anti_bot_reason=anti_bot_reason,
            breaker_mode=breaker_mode,
            manual_handoff_required=access_state == ACCESS_STATE_MANUAL_HANDOFF_REQUIRED,
            challenge_detected=challenge_detected,
            session_reused=session_reused,
            completed_with_content=False,
            route_family=route_family,
            section_name=section_name,
            transport_selected=(attempts[0].transport_selected if attempts else ""),
            transport_final=(latest.transport_final if latest is not None else ""),
            escalation_reason=(latest.escalation_reason if latest is not None else ""),
            blocked_by_policy=any(item.blocked_by_policy for item in attempts),
            attempts=list(attempts),
        )
        self.last_fetch_telemetry = attempts[-1] if attempts else None
        self.last_fetch_result = result
        return result

    def _select_initial_transport(
        self,
        *,
        requested_mode: str,
        route_family: str,
        section_name: str,
        breaker_mode: str,
        session_profile: SessionProfile | None,
        allow_fresh_session_bootstrap: bool = False,
    ) -> _PolicyAction:
        retry_budget = max(0, self.max_attempts - 1)
        route_policy = resolve_route_transport_policy(
            requested_mode=requested_mode,
            route_family=route_family,
            section_name=section_name,
        )
        if route_policy.transport_selected == "skip":
            return _PolicyAction(action="start", next_transport="skip", reason=route_policy.route_policy_reason, retry_budget=retry_budget)
        if breaker_mode == BREAKER_MODE_SURVIVAL and (session_profile is not None or allow_fresh_session_bootstrap):
            return _PolicyAction(
                action="start",
                next_transport=TRANSPORT_PLAYWRIGHT,
                reason="survival_session_bootstrap",
                retry_budget=retry_budget,
            )
        return _PolicyAction(
            action="start",
            next_transport=route_policy.transport_selected or TRANSPORT_REQUESTS,
            reason=route_policy.route_policy_reason,
            retry_budget=retry_budget,
        )

    def _resolve_followup_action(
        self,
        *,
        host: str,
        requested_mode: str,
        route_family: str,
        section_name: str,
        current_attempt_mode: str,
        attempt_no: int,
        execution: _AttemptExecution,
        decision: Any,
        response: requests.Response | None,
    ) -> _PolicyAction:
        remaining_budget = max(0, self.max_attempts - attempt_no)
        if decision.manual_handoff_required or execution.status in {ACCESS_STATE_MANUAL_HANDOFF_REQUIRED, ACCESS_STATE_PAUSED_BY_BREAKER}:
            return _PolicyAction(
                action="stop",
                reason=decision.escalation_reason or execution.status or decision.anti_bot_reason,
                transport_final=decision.transport_final or current_attempt_mode,
            )
        if decision.usable_content:
            return _PolicyAction(action="stop", reason="content_obtained", transport_final=decision.transport_final or current_attempt_mode)
        if execution.status == "blocked_no_alternative_proxy":
            return _PolicyAction(
                action="stop",
                reason=decision.escalation_reason or "proxy_reuse_policy_denied",
                blocked_by_policy=True,
                note="browser escalation denied because the same blocked proxy would be reused",
                transport_final=decision.transport_final or current_attempt_mode,
            )
        if execution.status == "cooldown_active":
            return _PolicyAction(
                action="stop",
                reason=decision.escalation_reason or "host_cooldown_active",
                blocked_by_policy=True,
                note=f"host cooldown active for {self._host_cooldown_remaining(host)}s",
                transport_final=decision.transport_final or current_attempt_mode,
            )
        if remaining_budget > 0 and decision.should_retry:
            cooldown_seconds = execution.cooldown_seconds or self._host_cooldown_remaining(host)
            return _PolicyAction(
                action="retry",
                next_transport=TRANSPORT_REQUESTS,
                reason=(decision.anti_bot_reason or "http_429") if decision.block_class == BLOCK_CLASS_RATE_LIMIT else (decision.anti_bot_reason or decision.escalation_reason or "retry"),
                note="limited retry after cooldown/backoff",
                cooldown_seconds=cooldown_seconds,
                retry_budget=max(decision.retry_budget_remaining, remaining_budget - 1),
                transport_final=TRANSPORT_REQUESTS,
            )
        if decision.should_escalate and (decision.transport_final or "") == TRANSPORT_PLAYWRIGHT:
            if not self.playwright_enabled:
                return _PolicyAction(
                    action="stop",
                    reason=decision.escalation_reason or "playwright_disabled",
                    blocked_by_policy=True,
                    note="browser escalation required by policy, but Playwright is disabled",
                    transport_final=current_attempt_mode,
                )
            return _PolicyAction(
                action="escalate",
                next_transport=TRANSPORT_PLAYWRIGHT,
                reason=decision.escalation_reason or decision.anti_bot_reason or "browser_escalation",
                note="policy escalated requests path to playwright",
                retry_budget=max(decision.retry_budget_remaining, remaining_budget - 1),
                transport_final=TRANSPORT_PLAYWRIGHT,
            )
        if decision.blocked_by_policy:
            return _PolicyAction(
                action="stop",
                reason=decision.escalation_reason or decision.anti_bot_reason or "blocked_by_policy",
                blocked_by_policy=True,
                note=f"browser escalation denied by route policy for {route_family or section_name or 'unknown_route'}",
                transport_final=decision.transport_final or current_attempt_mode,
            )
        return _PolicyAction(
            action="stop",
            reason=decision.escalation_reason or decision.anti_bot_reason or execution.status or "policy_terminal",
            transport_final=decision.transport_final or current_attempt_mode,
        )

    def _apply_followup_to_telemetry(self, telemetry: FetchTelemetry, followup: _PolicyAction) -> None:
        telemetry.transport_final = followup.transport_final or followup.next_transport or telemetry.fetch_mode
        if followup.reason:
            telemetry.escalation_reason = followup.reason
        telemetry.blocked_by_policy = telemetry.blocked_by_policy or followup.blocked_by_policy
        if followup.cooldown_seconds > telemetry.cooldown_seconds:
            telemetry.cooldown_seconds = followup.cooldown_seconds
        if followup.retry_budget > telemetry.retry_budget:
            telemetry.retry_budget = followup.retry_budget

    def _allow_same_proxy_for_browser_escalation(self, *, followup: _PolicyAction, decision: Any) -> bool:
        if (followup.next_transport or "") != TRANSPORT_PLAYWRIGHT:
            return False
        if getattr(decision, "block_class", "") in {BLOCK_CLASS_HARD_BAN, BLOCK_CLASS_CHALLENGE_LOOP}:
            return False
        return self._normalize_policy_value(followup.reason) in SOFT_CONTENT_PROXY_REUSE_REASONS

    def _normalize_policy_value(self, value: str) -> str:
        return normalize_whitespace(value).lower()

    def _route_allows_browser_escalation(
        self,
        *,
        requested_mode: str,
        route_family: str,
        section_name: str,
    ) -> bool:
        if requested_mode == "playwright":
            return True
        return (
            route_is_high_value(route_family=route_family, section_name=section_name)
            or route_family in BROWSER_ESCALATION_ROUTE_FAMILIES
            or section_name in BROWSER_ESCALATION_SECTIONS
        )

    def _derive_browser_escalation_reason(
        self,
        *,
        decision: Any,
        current_attempt_mode: str,
        response: requests.Response | None,
    ) -> str:
        if current_attempt_mode == "playwright":
            return ""
        if decision.anti_bot_reason == "rate_limited" or (response is not None and response.status_code == 429):
            return ""
        if self._response_has_redirect_loop(response):
            return "redirect_loop"
        if decision.challenge_detected or decision.anti_bot_reason in {"challenge_detected", "challenge_loop"}:
            return "challenge_page"
        if decision.anti_bot_reason == "bot_gate":
            return "bot_gate"
        if decision.anti_bot_reason == "js_wall":
            return "empty_js_shell"
        if decision.anti_bot_reason == "http_403" or (response is not None and response.status_code == 403):
            return "http_403"
        if self._response_has_suspiciously_thin_html(response):
            return "suspiciously_thin_html"
        if decision.block_class in {BLOCK_CLASS_HARD_BAN, BLOCK_CLASS_CHALLENGE_LOOP}:
            return decision.anti_bot_reason or decision.block_class.lower()
        if decision.block_class != BLOCK_CLASS_SUCCESS:
            return decision.anti_bot_reason or "browser_escalation"
        return ""

    def _update_host_cooldown(self, host: str, *, block_class: str, cooldown_seconds: int) -> int:
        normalized_cooldown = max(int(cooldown_seconds or 0), 0)
        if block_class == BLOCK_CLASS_SUCCESS:
            self._host_cooldowns.pop(host, None)
            return 0
        if normalized_cooldown <= 0 and block_class == BLOCK_CLASS_RATE_LIMIT:
            normalized_cooldown = self.default_rate_limit_cooldown_seconds
        elif normalized_cooldown <= 0 and block_class in {BLOCK_CLASS_HARD_BAN, BLOCK_CLASS_CHALLENGE_LOOP}:
            normalized_cooldown = self.default_block_cooldown_seconds
        if normalized_cooldown <= 0:
            return self._host_cooldown_remaining(host)
        self._host_cooldowns[host] = max(self._host_cooldowns.get(host, 0.0), time.time() + float(normalized_cooldown))
        return self._host_cooldown_remaining(host)

    def _host_cooldown_remaining(self, host: str) -> int:
        until = float(self._host_cooldowns.get(host, 0.0) or 0.0)
        if until <= 0.0:
            return 0
        remaining = int(max(0.0, round(until - time.time())))
        if remaining <= 0:
            self._host_cooldowns.pop(host, None)
            return 0
        return remaining

    def _response_has_redirect_loop(self, response: requests.Response | None) -> bool:
        if response is None:
            return False
        history = list(getattr(response, "history", []) or [])
        return len(history) >= 4

    def _response_has_suspiciously_thin_html(self, response: requests.Response | None) -> bool:
        if response is None:
            return False
        text = response.text or ""
        content_type = response.headers.get("Content-Type", "").lower()
        if "html" not in content_type and "<html" not in text[:800].lower():
            return False
        cleaned = normalize_whitespace(BeautifulSoup(text, "html.parser").get_text(" ", strip=True))
        return 0 < len(cleaned) < 250

    def _requests_fetch(
        self,
        url: str,
        *,
        proxy_selection: ProxySelection,
        session_profile: SessionProfile | None,
    ) -> _AttemptExecution:
        request_kwargs: dict[str, Any] = {
            "source": "route_fetch",
            "timeout": 15,
            "proxy_selection": proxy_selection,
        }
        session_reused = self._apply_session_profile_to_client(session_profile)
        try:
            outcome = self.client.request(url, **request_kwargs)
        except TypeError:
            request_kwargs.pop("proxy_selection", None)
            outcome = self.client.request(url, **request_kwargs)

        response = getattr(outcome, "response", None)
        if not getattr(outcome, "ok", False) or not response:
            status = str(getattr(outcome, "status", "request_error") or "request_error")
            return _AttemptExecution(
                attempt_mode="requests",
                response=response,
                status=status,
                notes=[str(getattr(outcome, "error", "") or status)],
                proxy_selection=proxy_selection,
                timeout=bool(getattr(outcome, "timeout", False)),
                blocked=bool(getattr(outcome, "blocked", False)),
                session_reused=session_reused,
                cooldown_seconds=int(getattr(outcome, "cooldown_seconds", 0) or 0),
            )

        return _AttemptExecution(
            attempt_mode="requests",
            response=response,
            status="success",
            notes=["страница fetched через requests"],
            proxy_selection=proxy_selection,
            timeout=bool(getattr(outcome, "timeout", False)),
            blocked=bool(getattr(outcome, "blocked", False)),
            session_reused=session_reused,
            cooldown_seconds=int(getattr(outcome, "cooldown_seconds", 0) or 0),
        )

    def _playwright_fetch(
        self,
        url: str,
        *,
        proxy_selection: ProxySelection,
        session_profile: SessionProfile | None,
    ) -> _AttemptExecution:
        try:
            from playwright.sync_api import TimeoutError as PlaywrightTimeoutError
            from playwright.sync_api import sync_playwright
        except Exception:
            return _AttemptExecution(
                attempt_mode="playwright",
                response=None,
                status="browser_required",
                notes=["playwright не установлен или недоступен"],
                proxy_selection=proxy_selection,
                session_reused=bool(session_profile),
                playwright_used=True,
            )

        status_code: int | None = None
        html = ""
        final_url = url
        browser = None
        context = None
        page = None
        storage_payload: dict[str, Any] | None = None
        current_user_agent = ""
        session_reused = bool(session_profile)
        try:
            launch_kwargs: dict[str, Any] = {"headless": self.playwright_headless}
            browser_proxy = proxy_selection.browser_proxy
            if browser_proxy:
                launch_kwargs["proxy"] = browser_proxy
            with sync_playwright() as playwright:
                browser = playwright.chromium.launch(**launch_kwargs)
                context_kwargs: dict[str, Any] = {
                    "ignore_https_errors": True,
                    "locale": "ru-RU",
                    "viewport": {"width": 1440, "height": 960},
                }
                if session_profile and session_profile.storage_state_path and Path(session_profile.storage_state_path).exists():
                    context_kwargs["storage_state"] = session_profile.storage_state_path
                context = browser.new_context(**context_kwargs)
                page = context.new_page()
                goto_response = page.goto(url, wait_until="domcontentloaded", timeout=self.playwright_timeout_ms)
                status_code = goto_response.status if goto_response else 200
                html = page.content()
                final_url = page.url
                if hasattr(page, "evaluate"):
                    current_user_agent = str(page.evaluate("() => navigator.userAgent") or "")
                if hasattr(context, "storage_state"):
                    storage_payload = context.storage_state()
        except Exception as exc:
            timeout_error = isinstance(exc, PlaywrightTimeoutError) or self._is_timeout_error(exc)
            self._mark_proxy_bad(proxy_selection, reason="timeout" if timeout_error else "browser_error")
            return _AttemptExecution(
                attempt_mode="playwright",
                response=None,
                status="browser_error",
                notes=[str(exc)],
                proxy_selection=proxy_selection,
                timeout=timeout_error,
                blocked=True,
                session_reused=session_reused,
                playwright_used=True,
            )
        finally:
            try:
                if page is not None:
                    page.close()
            except Exception:
                pass
            try:
                if context is not None:
                    context.close()
            except Exception:
                pass
            try:
                if browser is not None:
                    browser.close()
            except Exception:
                pass

        effective_status = "success"
        blocked = False
        if status_code == 429:
            effective_status = "rate_limited"
            blocked = True
        elif status_code == 403:
            effective_status = "http_403"
            blocked = True
        elif status_code and status_code >= 400:
            effective_status = f"http_{status_code}"

        if proxy_selection.via_proxy:
            if effective_status == "success":
                self._mark_proxy_ok(proxy_selection)
            elif blocked:
                self._mark_proxy_bad(proxy_selection, reason=effective_status)

        fake_response = requests.Response()
        fake_response.status_code = status_code or 200
        fake_response.url = final_url
        fake_response._content = html.encode("utf-8", errors="ignore")
        fake_response.headers["Content-Type"] = "text/html; charset=utf-8"
        fake_response.encoding = "utf-8"

        if effective_status == "success" and storage_payload:
            self.session_store.save(
                host=self._host_from_url(url),
                storage_payload=storage_payload,
                final_url=final_url,
                user_agent=current_user_agent or session_profile.user_agent if session_profile else "",
                referer=url,
                proxy_label_or_id=proxy_selection.proxy_label_or_id,
                manual_bootstrap=False,
            )

        return _AttemptExecution(
            attempt_mode="playwright",
            response=fake_response,
            status=effective_status,
            notes=["страница fetched через playwright"] if effective_status == "success" else [f"playwright returned {effective_status}"],
            proxy_selection=proxy_selection,
            blocked=blocked,
            session_reused=session_reused,
            playwright_used=True,
            storage_payload=storage_payload,
            browser_user_agent=current_user_agent,
            final_url=final_url,
        )

    def _resolve_proxy_pool(self, client: Any, proxy_pool: ProxyPool | None) -> ProxyPool:
        if isinstance(proxy_pool, ProxyPool):
            return proxy_pool
        proxy_pool = getattr(client, "proxy_pool", None)
        if isinstance(proxy_pool, ProxyPool):
            return proxy_pool
        return ProxyPool(os.getenv("PARSER_PROXIES"))

    def _select_proxy_from_pool(self, host: str | None) -> ProxySelection:
        select_proxy = getattr(self.proxy_pool, "select", None)
        if not callable(select_proxy):
            return ProxySelection()
        try:
            selection = select_proxy(host, source_name="company_site")
        except TypeError:
            try:
                selection = select_proxy(host)
            except Exception:
                return ProxySelection()
        except Exception:
            return ProxySelection()
        if selection.via_proxy or not self._proxy_pool_has_entries():
            return selection
        if self._proxy_provider_attempt_blocked():
            return selection
        try:
            return select_proxy(host)
        except Exception:
            return selection

    def _proxy_pool_has_entries(self) -> bool:
        return bool(getattr(self.proxy_pool, "entries", None) or [])

    def _proxy_provider_attempt_blocked(self) -> bool:
        attempt_guard = getattr(self.proxy_pool, "proxy_provider_attempt_guard", None)
        if not callable(attempt_guard):
            return False
        try:
            return attempt_guard(source_name="company_site") is not None
        except TypeError:
            return False
        except Exception:
            return False

    def _proxy_provider_diagnostic(self):
        provider_diagnostic = getattr(self.proxy_pool, "proxy_provider_diagnostic", None)
        if callable(provider_diagnostic):
            try:
                return provider_diagnostic()
            except Exception:
                pass
        return diagnose_proxy6_inventory_from_env()

    def _proxy_provider_event_fields(self, proxy_selection: ProxySelection) -> dict[str, object]:
        if proxy_selection.via_proxy:
            return {}
        has_proxy_entries = self._proxy_pool_has_entries()
        if not has_proxy_entries and not os.getenv("PROXY6_API_KEY", "").strip():
            return {}
        diagnostic = self._proxy_provider_diagnostic()
        if diagnostic.provider_status == PROXY_PROVIDER_INVENTORY_EMPTY_OR_EXPIRED:
            return diagnostic.as_event_fields()
        if diagnostic.provider_status == PROXY_PROVIDER_INVENTORY_HEALTHY:
            return diagnostic.as_event_fields()
        if diagnostic.provider_status == PROXY_PROVIDER_STATUS_UNKNOWN and has_proxy_entries:
            return diagnostic.as_event_fields()
        return {}

    def _mark_proxy_bad(self, proxy_selection: ProxySelection, *, reason: str) -> None:
        if not proxy_selection.via_proxy:
            return
        try:
            self.proxy_pool.mark_bad(proxy_selection.url, reason=reason, source_name="company_site")
        except TypeError:
            self.proxy_pool.mark_bad(proxy_selection.url, reason=reason)

    def _mark_proxy_ok(self, proxy_selection: ProxySelection) -> None:
        if not proxy_selection.via_proxy:
            return
        try:
            self.proxy_pool.mark_ok(proxy_selection.url, source_name="company_site")
        except TypeError:
            self.proxy_pool.mark_ok(proxy_selection.url)

    def _select_proxy(self, host: str, *, blocked_proxy_labels: set[str]) -> ProxySelection:
        normalized_host = self._normalize_host_key(host)
        if not blocked_proxy_labels:
            self._host_proxy_rotation_hints.pop(normalized_host, None)
            return self._select_proxy_from_pool(host)
        selection = self._select_proxy_from_pool(host)
        if not selection.via_proxy or selection.proxy_label_or_id not in blocked_proxy_labels:
            self._host_proxy_rotation_hints.pop(normalized_host, None)
            return selection
        hinted_selection = self._select_host_proxy_rotation_hint(host, blocked_proxy_labels=blocked_proxy_labels)
        if hinted_selection is not None:
            return hinted_selection
        entry_count = max(len(getattr(self.proxy_pool, "entries", [])), 1)
        for _ in range(entry_count):
            candidate = self._select_proxy_from_pool(None)
            if not candidate.via_proxy or candidate.proxy_label_or_id not in blocked_proxy_labels:
                self._remember_host_proxy_rotation_hint(host, candidate)
                return candidate
        self._host_proxy_rotation_hints.pop(normalized_host, None)
        return selection

    def _normalize_host_key(self, host: str) -> str:
        return str(host or "").strip().lower()

    def _remember_host_proxy_rotation_hint(self, host: str, selection: ProxySelection) -> None:
        normalized_host = self._normalize_host_key(host)
        if not normalized_host:
            return
        proxy_label = normalize_whitespace(selection.proxy_label_or_id)
        if not selection.via_proxy or not proxy_label:
            self._host_proxy_rotation_hints.pop(normalized_host, None)
            return
        self._host_proxy_rotation_hints[normalized_host] = proxy_label

    def _select_host_proxy_rotation_hint(
        self,
        host: str,
        *,
        blocked_proxy_labels: set[str],
    ) -> ProxySelection | None:
        normalized_host = self._normalize_host_key(host)
        if not normalized_host:
            return None
        hinted_proxy_label = normalize_whitespace(self._host_proxy_rotation_hints.get(normalized_host, ""))
        if not hinted_proxy_label or hinted_proxy_label in blocked_proxy_labels:
            self._host_proxy_rotation_hints.pop(normalized_host, None)
            return None
        selection = self._resolve_proxy_selection_by_label_or_id(hinted_proxy_label)
        if selection is None or (selection.via_proxy and selection.proxy_label_or_id in blocked_proxy_labels):
            self._host_proxy_rotation_hints.pop(normalized_host, None)
            return None
        return selection

    def _resolve_proxy_selection_by_label_or_id(self, proxy_label_or_id: str) -> ProxySelection | None:
        proxy_label = normalize_whitespace(proxy_label_or_id)
        if not proxy_label:
            return None
        now = time.time()
        for entry in getattr(self.proxy_pool, "entries", []):
            entry_label = normalize_whitespace(entry.proxy_id or entry.label)
            if entry_label != proxy_label or entry.cooldown_until > now:
                continue
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
        return None

    def _select_safe_survival_bootstrap_proxy(
        self,
        host: str,
        *,
        blocked_proxy_labels: set[str],
    ) -> ProxySelection | None:
        if not blocked_proxy_labels:
            return None
        selection = self._select_proxy(host, blocked_proxy_labels=blocked_proxy_labels)
        if not selection.via_proxy:
            return selection
        proxy_label = normalize_whitespace(selection.proxy_label_or_id)
        if not proxy_label or proxy_label in blocked_proxy_labels:
            return None
        return selection

    def _runtime_host_governor_preflight(self, host: str) -> HostGovernorPreflight:
        progress_store = getattr(self.client, "progress_store", None)
        host_memory = getattr(progress_store, "host_memory", None)
        if not isinstance(host_memory, dict):
            return HostGovernorPreflight(host=host)
        return resolve_host_governor_preflight(host_memory, host)

    def _runtime_guard_session_profile(
        self,
        *,
        session_profile: SessionProfile | None,
        blocked_proxy_labels: set[str],
    ) -> SessionProfile | None:
        if session_profile is None or not blocked_proxy_labels:
            return session_profile
        session_proxy_label = normalize_whitespace(session_profile.proxy_label_or_id)
        if not session_proxy_label or session_proxy_label not in blocked_proxy_labels:
            return session_profile
        return None

    def _load_runtime_guarded_session_profile(
        self,
        host: str,
        *,
        blocked_proxy_labels: set[str],
    ) -> SessionProfile | None:
        return self._runtime_guard_session_profile(
            session_profile=self.session_store.load(host),
            blocked_proxy_labels=blocked_proxy_labels,
        )

    def _bootstrap_breaker_from_runtime_host_memory(self, host: str) -> None:
        progress_store = getattr(self.client, "progress_store", None)
        host_memory = getattr(progress_store, "host_memory", None)
        if not isinstance(host_memory, dict):
            return
        normalized_host = self._normalize_host_key(host)
        if not normalized_host:
            return
        recent_attempts = normalize_host_memory_state(host_memory).get(normalized_host, {}).get("recent_attempts", [])
        self.breakers.bootstrap_from_recent_attempts(host, recent_attempts)

    def _seed_host_cooldown_from_preflight(self, host: str, preflight: HostGovernorPreflight) -> None:
        if not preflight.cooldown_active or preflight.cooldown_remaining_seconds <= 0:
            return
        self._host_cooldowns[host] = max(
            self._host_cooldowns.get(host, 0.0),
            time.time() + float(preflight.cooldown_remaining_seconds),
        )

    def _build_telemetry(
        self,
        *,
        url: str,
        fetch_mode: str,
        route_family: str,
        section_name: str,
        proxy_selection: ProxySelection,
        status: str,
        response: requests.Response | None = None,
        timeout: bool = False,
        blocked: bool = False,
        playwright_fallback_used: bool = False,
        proxy_mode: str = "",
        proxy_label_or_id: str = "",
        proxy_id: str = "",
        block_class: str = "",
        anti_bot_reason: str = "",
        attempt_no: int = 0,
        escalated_from: str = "",
        escalated_to: str = "",
        session_reused: bool = False,
        breaker_mode: str = "normal",
        manual_handoff_required: bool = False,
        playwright_used: bool = False,
        challenge_detected: bool = False,
        access_state: str = "",
        transport_selected: str = "",
        transport_final: str = "",
        escalation_reason: str = "",
        blocked_by_policy: bool = False,
        cooldown_seconds: int = 0,
        retry_budget: int = 0,
    ) -> FetchTelemetry:
        return FetchTelemetry(
            host=self._host_from_url(url),
            url=url,
            fetch_mode=fetch_mode,
            route_family=route_family,
            section_name=section_name,
            proxy_mode=proxy_mode or ("proxy" if proxy_selection.via_proxy else "direct"),
            proxy_label_or_id=proxy_label_or_id or proxy_selection.proxy_label_or_id,
            proxy_id=proxy_id or proxy_selection.proxy_id,
            timeout=timeout,
            blocked=blocked,
            playwright_fallback_used=playwright_fallback_used,
            status=status,
            http_status=response.status_code if response is not None else None,
            block_class=block_class,
            anti_bot_reason=anti_bot_reason,
            attempt_no=attempt_no,
            escalated_from=escalated_from,
            escalated_to=escalated_to,
            session_reused=session_reused,
            breaker_mode=breaker_mode,
            manual_handoff_required=manual_handoff_required,
            playwright_used=playwright_used,
            challenge_detected=challenge_detected,
            access_state=access_state,
            transport_selected=transport_selected or fetch_mode,
            transport_final=transport_final or fetch_mode,
            escalation_reason=escalation_reason,
            blocked_by_policy=blocked_by_policy,
            cooldown_seconds=max(int(cooldown_seconds or 0), 0),
            retry_budget=max(int(retry_budget or 0), 0),
        )

    def _append_progress_event(self, event_type: str, telemetry: FetchTelemetry, **extra: Any) -> None:
        progress_store = getattr(self.client, "progress_store", None)
        append_event = getattr(progress_store, "append_event", None)
        if not callable(append_event):
            return
        payload = {
            "ts": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
            "type": event_type,
            "source": "factory_site_fetch",
            **telemetry.to_trace(),
        }
        payload.update(extra)
        append_event(payload)

    def _append_terminal_progress_event(
        self,
        telemetry: FetchTelemetry,
        *,
        status: str,
        access_state: str,
    ) -> None:
        # Keep host/stage tails honest for same-run short-circuits without emitting a synthetic second fetch attempt
        # or duplicating governor signal debt in host memory.
        terminal_telemetry = replace(
            telemetry,
            status=status,
            http_status=None,
            block_class="",
            anti_bot_reason="",
            manual_handoff_required=False,
            challenge_detected=False,
            access_state=access_state or ACCESS_STATE_BLOCKED,
            blocked_by_policy=True,
            cooldown_seconds=0,
        )
        self._append_progress_event("route_fetch_terminal", terminal_telemetry)

    def _apply_session_profile_to_client(self, session_profile: SessionProfile | None) -> bool:
        session = getattr(self.client, "session", None)
        return apply_session_profile_to_requests_session(session_profile, session) if session_profile else False

    def _sleep_backoff(self, cooldown_seconds: int) -> None:
        if cooldown_seconds <= 0:
            return
        wait_for = min(float(cooldown_seconds), self.max_rate_limit_backoff)
        if wait_for <= 0:
            return
        time.sleep(wait_for)

    def _host_from_url(self, url: str) -> str:
        return urlparse(url).netloc.lower()

    def _is_timeout_error(self, exc: BaseException) -> bool:
        error_text = normalize_whitespace(str(exc)).lower()
        return any(marker in error_text for marker in ("timed out", "timeout", "read timeout", "connect timeout"))

    def _response_has_usable_html(self, response: requests.Response) -> bool:
        text = response.text or ""
        if "html" not in response.headers.get("Content-Type", "").lower() and "<html" not in text[:800].lower():
            return False
        cleaned = normalize_whitespace(BeautifulSoup(text, "html.parser").get_text(" ", strip=True))
        return len(cleaned) >= 250


__all__ = ["FetchResult", "FetchTelemetry", "Fetcher"]

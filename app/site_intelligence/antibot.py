from __future__ import annotations

import json
import os
import time
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any

import requests

from .common import SPA_MARKERS, guess_registered_domain, normalize_whitespace

BLOCK_CLASS_SUCCESS = "SUCCESS"
BLOCK_CLASS_SOFT_BLOCK = "SOFT_BLOCK"
BLOCK_CLASS_HARD_BAN = "HARD_BAN"
BLOCK_CLASS_RATE_LIMIT = "RATE_LIMIT"
BLOCK_CLASS_AUTH_REQUIRED = "AUTH_REQUIRED"
BLOCK_CLASS_CHALLENGE_LOOP = "CHALLENGE_LOOP"
BLOCK_CLASS_SHADOW_THROTTLE = "SHADOW_THROTTLE"

TRANSPORT_SKIP = "skip"
TRANSPORT_REQUESTS = "requests"
TRANSPORT_PLAYWRIGHT = "playwright"
TRANSPORT_HYBRID = "hybrid"

ACCESS_STATE_COMPLETED_WITH_CONTENT = "completed_with_content"
ACCESS_STATE_RECOVERED = "recovered"
ACCESS_STATE_BLOCKED = "blocked"
ACCESS_STATE_MANUAL_HANDOFF_REQUIRED = "manual_handoff_required"
ACCESS_STATE_PAUSED_BY_BREAKER = "paused_by_breaker"

BREAKER_MODE_NORMAL = "normal"
BREAKER_MODE_CONSERVATIVE = "conservative"
BREAKER_MODE_SURVIVAL = "survival"
BREAKER_MODE_PAUSED = "paused"

HIGH_VALUE_ROUTE_SECTIONS = frozenset({"homepage", "about", "contacts", "products", "services", "files", "search"})
HIGH_VALUE_ROUTE_FAMILIES = frozenset(
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
    }
)

BROWSER_FIRST_ROUTE_FAMILIES = frozenset({"search", "files", "procurement", "tenders", "sales", "warehouse", "stock"})
NO_BROWSER_ROUTE_FAMILY_HINTS = (
    "api",
    "asset",
    "static",
    "image",
    "images",
    "img",
    "css",
    "js",
    "javascript",
    "feed",
    "robots",
    "sitemap",
)
ROUTE_FAMILY_ESCALATION_HINTS = (
    "about",
    "catalog",
    "company",
    "contact",
    "files",
    "news",
    "product",
    "production",
    "procurement",
    "sale",
    "search",
    "service",
    "stock",
    "surplus",
    "tender",
    "tmc",
    "mtr",
    "warehouse",
)

SYMPTOM_SUCCESS = "success"
SYMPTOM_HTTP_403 = "http_403"
SYMPTOM_HTTP_429 = "http_429"
SYMPTOM_CHALLENGE_PAGE = "challenge_page"
SYMPTOM_EMPTY_JS_SHELL = "empty_js_shell"
SYMPTOM_REDIRECT_LOOP = "redirect_loop"
SYMPTOM_BOT_GATE = "bot_gate"
SYMPTOM_THIN_HTML = "thin_html"
SYMPTOM_SHADOW_THROTTLE = "shadow_throttle"
SYMPTOM_AUTH_REQUIRED = "auth_required"
SYMPTOM_HTTP_5XX = "http_5xx"
SYMPTOM_REQUEST_ERROR = "request_error"
SYMPTOM_BROWSER_ERROR = "browser_error"
SYMPTOM_BROWSER_REQUIRED = "browser_required"

DEFAULT_RETRY_BUDGET = 2

CHALLENGE_MARKERS = (
    "captcha",
    "turnstile",
    "cloudflare",
    "verify you are human",
    "checking if the site connection is secure",
    "prove you are human",
    "security check",
    "attention required",
    "проверка, что вы не робот",
    "вы слишком часто обращались к сайту",
    "автоматическая активность",
    "капча",
    "я не робот",
)
AUTH_MARKERS = (
    "sign in",
    "login",
    "log in",
    "password",
    "вход",
    "авторизац",
    "личный кабинет",
    "требуется авторизация",
)
JS_WALL_MARKERS = (
    "enable javascript",
    "javascript required",
    "please enable javascript",
    "для работы сайта нужен javascript",
    "включите javascript",
)
SHADOW_THROTTLE_MARKERS = (
    "request blocked",
    "temporarily unavailable",
    "temporarily limited",
    "доступ временно ограничен",
    "временно недоступно",
)
BOT_GATE_MARKERS = (
    "access denied",
    "automated requests",
    "bot detection",
    "forbidden",
    "verify you are human",
    "deny access",
    "доступ ограничен",
    "защита от автоматических запросов",
)

_BREAKER_RANK = {
    BREAKER_MODE_NORMAL: 0,
    BREAKER_MODE_CONSERVATIVE: 1,
    BREAKER_MODE_SURVIVAL: 2,
    BREAKER_MODE_PAUSED: 3,
}


def normalize_route_name(value: str | None) -> str:
    return normalize_whitespace(value).lower()


def route_is_high_value(*, route_family: str | None = None, section_name: str | None = None) -> bool:
    normalized_family = normalize_route_name(route_family)
    normalized_section = normalize_route_name(section_name)
    return normalized_section in HIGH_VALUE_ROUTE_SECTIONS or normalized_family in HIGH_VALUE_ROUTE_FAMILIES


def breaker_mode_rank(mode: str | None) -> int:
    return _BREAKER_RANK.get(normalize_route_name(mode), 0)


def upgrade_breaker_mode(current: str, target: str) -> str:
    if breaker_mode_rank(target) > breaker_mode_rank(current):
        return target
    return current


def block_class_priority(block_class: str | None) -> int:
    normalized = normalize_whitespace(block_class).upper()
    priorities = {
        BLOCK_CLASS_SUCCESS: 0,
        BLOCK_CLASS_RATE_LIMIT: 1,
        BLOCK_CLASS_SHADOW_THROTTLE: 2,
        BLOCK_CLASS_SOFT_BLOCK: 3,
        BLOCK_CLASS_AUTH_REQUIRED: 4,
        BLOCK_CLASS_HARD_BAN: 5,
        BLOCK_CLASS_CHALLENGE_LOOP: 6,
    }
    return priorities.get(normalized, 0)


def response_text_head(response: requests.Response | None, *, limit: int = 8000) -> str:
    if response is None:
        return ""
    try:
        return response.text[:limit]
    except Exception:
        return ""


def challenge_detected_from_text(text: str) -> bool:
    lowered = normalize_whitespace(text).lower()
    return any(marker in lowered for marker in CHALLENGE_MARKERS)


def auth_required_from_text(text: str) -> bool:
    lowered = normalize_whitespace(text).lower()
    return any(marker in lowered for marker in AUTH_MARKERS)


def js_wall_detected_from_text(text: str) -> bool:
    lowered = normalize_whitespace(text).lower()
    if any(marker in lowered for marker in JS_WALL_MARKERS):
        return True
    if any(marker.lower() in lowered for marker in SPA_MARKERS) and len(lowered) < 400:
        return True
    return False


def shadow_throttle_detected_from_text(text: str) -> bool:
    lowered = normalize_whitespace(text).lower()
    return any(marker in lowered for marker in SHADOW_THROTTLE_MARKERS)


def bot_gate_detected_from_text(text: str) -> bool:
    lowered = normalize_whitespace(text).lower()
    return any(marker in lowered for marker in BOT_GATE_MARKERS)


def suspiciously_thin_html_from_text(text: str) -> bool:
    normalized = normalize_whitespace(text)
    if not normalized:
        return False
    lowered = normalized.lower()
    if challenge_detected_from_text(lowered) or auth_required_from_text(lowered) or js_wall_detected_from_text(lowered):
        return False
    if "<html" in text.lower() or "<body" in text.lower():
        return len(normalized) < 220
    return 0 < len(normalized) < 140


def redirect_loop_detected(
    *,
    response: requests.Response | None = None,
    redirect_chain: tuple[str, ...] | list[str] | None = None,
) -> tuple[bool, int]:
    chain: list[str] = []
    if response is not None:
        for item in list(response.history) + [response]:
            try:
                url = normalize_whitespace(str(getattr(item, "url", "") or ""))
            except Exception:
                url = ""
            if url:
                chain.append(url)
    if redirect_chain:
        chain.extend(normalize_whitespace(item) for item in redirect_chain if normalize_whitespace(item))
    if not chain:
        return False, 0
    normalized_chain = [item.lower() for item in chain if item]
    if len(normalized_chain) < 2:
        return False, max(0, len(normalized_chain) - 1)
    if len(normalized_chain) >= 4 and len(set(normalized_chain[-4:])) <= 2:
        return True, max(0, len(normalized_chain) - 1)
    seen: set[str] = set()
    for item in normalized_chain:
        if item in seen:
            return True, max(0, len(normalized_chain) - 1)
        seen.add(item)
    return False, max(0, len(normalized_chain) - 1)


def normalize_fetch_symptoms(
    *,
    status: str,
    response: requests.Response | None,
    usable_content: bool,
    response_text: str = "",
    redirect_chain: tuple[str, ...] | list[str] | None = None,
) -> "FetchSymptoms":
    normalized_status = normalize_whitespace(status).lower()
    http_status = response.status_code if response is not None else None
    text = response_text or response_text_head(response)
    normalized_text = normalize_whitespace(text)
    challenge_page = challenge_detected_from_text(normalized_text)
    auth_required = auth_required_from_text(normalized_text)
    empty_js_shell = not usable_content and js_wall_detected_from_text(normalized_text)
    redirect_loop, redirect_count = redirect_loop_detected(response=response, redirect_chain=redirect_chain)
    bot_gate = normalized_status == SYMPTOM_BOT_GATE or bot_gate_detected_from_text(normalized_text)
    suspiciously_thin_html = not usable_content and suspiciously_thin_html_from_text(text)
    shadow_throttle = (
        not usable_content
        and not challenge_page
        and not empty_js_shell
        and not bot_gate
        and bool(normalized_text)
        and (shadow_throttle_detected_from_text(normalized_text) or suspiciously_thin_html)
    )
    rate_limited = normalized_status in {"rate_limited", "cooldown_active"} or http_status == 429
    hard_block = normalized_status == SYMPTOM_HTTP_403 or http_status == 403
    server_error = normalized_status.startswith("http_5")
    request_error = normalized_status == SYMPTOM_REQUEST_ERROR
    browser_error = normalized_status == SYMPTOM_BROWSER_ERROR
    browser_required = normalized_status == SYMPTOM_BROWSER_REQUIRED

    symptom_codes: list[str] = []
    if usable_content and normalized_status in {"success", "ok"}:
        symptom_codes.append(SYMPTOM_SUCCESS)
    if rate_limited:
        symptom_codes.append(SYMPTOM_HTTP_429)
    if challenge_page:
        symptom_codes.append(SYMPTOM_CHALLENGE_PAGE)
    if auth_required or normalized_status in {"auth_required", "http_401", "http_407"}:
        symptom_codes.append(SYMPTOM_AUTH_REQUIRED)
    if empty_js_shell:
        symptom_codes.append(SYMPTOM_EMPTY_JS_SHELL)
    if redirect_loop:
        symptom_codes.append(SYMPTOM_REDIRECT_LOOP)
    if bot_gate:
        symptom_codes.append(SYMPTOM_BOT_GATE)
    if suspiciously_thin_html:
        symptom_codes.append(SYMPTOM_THIN_HTML)
    if shadow_throttle:
        symptom_codes.append(SYMPTOM_SHADOW_THROTTLE)
    if hard_block:
        symptom_codes.append(SYMPTOM_HTTP_403)
    if server_error:
        symptom_codes.append(SYMPTOM_HTTP_5XX)
    if request_error:
        symptom_codes.append(SYMPTOM_REQUEST_ERROR)
    if browser_error:
        symptom_codes.append(SYMPTOM_BROWSER_ERROR)
    if browser_required:
        symptom_codes.append(SYMPTOM_BROWSER_REQUIRED)
    primary_reason = symptom_codes[0] if symptom_codes else (normalized_status or SYMPTOM_REQUEST_ERROR)

    return FetchSymptoms(
        normalized_status=normalized_status,
        http_status=http_status,
        usable_content=usable_content,
        challenge_page=challenge_page,
        auth_required=auth_required or normalized_status in {"auth_required", "http_401", "http_407"},
        empty_js_shell=empty_js_shell,
        redirect_loop=redirect_loop,
        bot_gate=bot_gate,
        suspiciously_thin_html=suspiciously_thin_html,
        shadow_throttle=shadow_throttle,
        rate_limited=rate_limited,
        hard_block=hard_block,
        server_error=server_error,
        request_error=request_error,
        browser_error=browser_error,
        browser_required=browser_required,
        symptom_codes=tuple(symptom_codes),
        primary_reason=primary_reason,
        content_length=len(normalized_text),
        redirect_count=redirect_count,
    )


@dataclass
class AntiBotDecision:
    block_class: str
    anti_bot_reason: str = ""
    challenge_detected: bool = False
    should_retry: bool = False
    should_escalate: bool = False
    manual_handoff_required: bool = False
    usable_content: bool = False
    transport_selected: str = ""
    transport_current: str = ""
    transport_final: str = ""
    escalation_reason: str = ""
    retry_reason: str = ""
    blocked_reason: str = ""
    blocked_by_policy: bool = False
    retry_budget_remaining: int = 0
    cooldown_key: str = ""
    cooldown_seconds: float = 0.0
    cooldown_active: bool = False
    route_family: str = ""
    section_name: str = ""
    requested_mode: str = ""
    symptoms: "FetchSymptoms | None" = None

    def to_policy_snapshot(self) -> dict[str, Any]:
        snapshot = {
            "block_class": self.block_class,
            "anti_bot_reason": self.anti_bot_reason,
            "challenge_detected": self.challenge_detected,
            "should_retry": self.should_retry,
            "should_escalate": self.should_escalate,
            "manual_handoff_required": self.manual_handoff_required,
            "usable_content": self.usable_content,
            "transport_selected": self.transport_selected,
            "transport_current": self.transport_current,
            "transport_final": self.transport_final,
            "escalation_reason": self.escalation_reason,
            "retry_reason": self.retry_reason,
            "blocked_reason": self.blocked_reason,
            "blocked_by_policy": self.blocked_by_policy,
            "retry_budget_remaining": self.retry_budget_remaining,
            "cooldown_key": self.cooldown_key,
            "cooldown_seconds": self.cooldown_seconds,
            "cooldown_active": self.cooldown_active,
            "route_family": self.route_family,
            "section_name": self.section_name,
            "requested_mode": self.requested_mode,
        }
        if self.symptoms is not None:
            snapshot["symptom_codes"] = list(self.symptoms.symptom_codes)
            snapshot["primary_reason"] = self.symptoms.primary_reason
        return snapshot


@dataclass(frozen=True)
class FetchSymptoms:
    normalized_status: str
    http_status: int | None = None
    usable_content: bool = False
    challenge_page: bool = False
    auth_required: bool = False
    empty_js_shell: bool = False
    redirect_loop: bool = False
    bot_gate: bool = False
    suspiciously_thin_html: bool = False
    shadow_throttle: bool = False
    rate_limited: bool = False
    hard_block: bool = False
    server_error: bool = False
    request_error: bool = False
    browser_error: bool = False
    browser_required: bool = False
    symptom_codes: tuple[str, ...] = field(default_factory=tuple)
    primary_reason: str = ""
    content_length: int = 0
    redirect_count: int = 0


@dataclass(frozen=True)
class RouteTransportPolicy:
    requested_mode: str
    route_family: str
    section_name: str
    transport_selected: str
    browser_allowed: bool
    browser_first: bool
    route_policy_reason: str

    def to_runtime_fields(self) -> dict[str, Any]:
        return {
            "requested_mode": self.requested_mode,
            "route_family": self.route_family,
            "section_name": self.section_name,
            "transport_selected": self.transport_selected,
            "browser_allowed": self.browser_allowed,
            "browser_first": self.browser_first,
            "route_policy_reason": self.route_policy_reason,
        }


@dataclass(frozen=True)
class TransportPolicyContext:
    host: str
    requested_mode: str
    route_family: str = ""
    section_name: str = ""
    status: str = ""
    response: requests.Response | None = None
    response_text: str = ""
    usable_content: bool = False
    attempt_no: int = 1
    retry_budget: int = DEFAULT_RETRY_BUDGET
    current_transport: str = ""
    browser_attempt: bool = False
    session_reused: bool = False
    breaker_mode: str = BREAKER_MODE_NORMAL
    cooldown_active: bool = False
    cooldown_remaining_seconds: float = 0.0
    redirect_chain: tuple[str, ...] = field(default_factory=tuple)


@dataclass(frozen=True)
class TransportPolicyDecision:
    route_policy: RouteTransportPolicy
    symptoms: FetchSymptoms
    transport_selected: str
    transport_current: str
    transport_final: str
    escalation_reason: str = ""
    retry_reason: str = ""
    blocked_reason: str = ""
    blocked_by_policy: bool = False
    should_retry: bool = False
    should_escalate: bool = False
    retry_allowed: bool = False
    retry_budget_remaining: int = 0
    cooldown_key: str = ""
    cooldown_seconds: float = 0.0
    cooldown_active: bool = False
    manual_handoff_required: bool = False
    challenge_detected: bool = False

    def to_runtime_fields(self) -> dict[str, Any]:
        return {
            **self.route_policy.to_runtime_fields(),
            "transport_current": self.transport_current,
            "transport_final": self.transport_final,
            "escalation_reason": self.escalation_reason,
            "retry_reason": self.retry_reason,
            "blocked_reason": self.blocked_reason,
            "blocked_by_policy": self.blocked_by_policy,
            "should_retry": self.should_retry,
            "should_escalate": self.should_escalate,
            "retry_allowed": self.retry_allowed,
            "retry_budget_remaining": self.retry_budget_remaining,
            "cooldown_key": self.cooldown_key,
            "cooldown_seconds": self.cooldown_seconds,
            "cooldown_active": self.cooldown_active,
            "manual_handoff_required": self.manual_handoff_required,
            "challenge_detected": self.challenge_detected,
            "symptom_codes": list(self.symptoms.symptom_codes),
            "primary_reason": self.symptoms.primary_reason,
        }


@dataclass
class BreakerState:
    domain: str
    mode: str = BREAKER_MODE_NORMAL
    soft_block_events: int = 0
    hard_block_events: int = 0
    challenge_loop_events: int = 0
    rate_limit_events: int = 0
    shadow_throttle_events: int = 0
    last_block_class: str = ""
    last_reason: str = ""
    last_proxy_label_or_id: str = ""
    cooldown_until: float = 0.0
    updated_at: float = field(default_factory=time.time)

    @property
    def breaker_open(self) -> bool:
        return self.mode in {BREAKER_MODE_SURVIVAL, BREAKER_MODE_PAUSED}

    @property
    def cooldown_remaining_seconds(self) -> float:
        return max(0.0, self.cooldown_until - time.time())

    @property
    def cooldown_active(self) -> bool:
        return self.cooldown_remaining_seconds > 0.0


def resolve_route_transport_policy(
    *,
    requested_mode: str,
    route_family: str | None = None,
    section_name: str | None = None,
) -> RouteTransportPolicy:
    normalized_mode = normalize_route_name(requested_mode) or TRANSPORT_REQUESTS
    normalized_family = normalize_route_name(route_family)
    normalized_section = normalize_route_name(section_name)
    family_tokens = " ".join(token for token in (normalized_family, normalized_section) if token)
    browser_allowed = True
    browser_first = False
    route_policy_reason = "route_default_requests"

    if any(token in family_tokens for token in NO_BROWSER_ROUTE_FAMILY_HINTS):
        browser_allowed = False
        route_policy_reason = "route_family_no_browser"
    elif normalized_family in BROWSER_FIRST_ROUTE_FAMILIES:
        browser_first = True
        route_policy_reason = "route_family_browser_first"
    elif route_is_high_value(route_family=normalized_family, section_name=normalized_section):
        route_policy_reason = "route_family_high_value"
    elif any(token in family_tokens for token in ROUTE_FAMILY_ESCALATION_HINTS):
        route_policy_reason = "route_family_browser_allowed"
    else:
        browser_allowed = False
        route_policy_reason = "route_family_requests_only"

    if normalized_mode == TRANSPORT_SKIP:
        transport_selected = TRANSPORT_SKIP
    elif normalized_mode == TRANSPORT_PLAYWRIGHT:
        transport_selected = TRANSPORT_PLAYWRIGHT
        browser_allowed = True
        route_policy_reason = "requested_playwright"
    elif normalized_mode == TRANSPORT_HYBRID and browser_first and browser_allowed:
        transport_selected = TRANSPORT_PLAYWRIGHT
    else:
        transport_selected = TRANSPORT_REQUESTS

    return RouteTransportPolicy(
        requested_mode=normalized_mode,
        route_family=normalized_family,
        section_name=normalized_section,
        transport_selected=transport_selected,
        browser_allowed=browser_allowed,
        browser_first=browser_first,
        route_policy_reason=route_policy_reason,
    )


def recommended_cooldown_seconds(
    *,
    block_class: str,
    reason: str = "",
    event_count: int = 1,
) -> float:
    normalized_block = normalize_whitespace(block_class).upper()
    normalized_reason = normalize_route_name(reason)
    event_count = max(1, event_count)
    if normalized_block == BLOCK_CLASS_RATE_LIMIT:
        return float(30 * (2 ** min(event_count - 1, 3)))
    if normalized_block == BLOCK_CLASS_CHALLENGE_LOOP:
        return float(300 * event_count)
    if normalized_block == BLOCK_CLASS_HARD_BAN:
        return float(180 * (2 ** min(event_count - 1, 3)))
    if normalized_block == BLOCK_CLASS_SHADOW_THROTTLE:
        return float(45 * (2 ** min(event_count - 1, 3)))
    if normalized_reason == SYMPTOM_BOT_GATE:
        return float(20 * event_count)
    return 0.0


def cooldown_key_for_host(host: str) -> str:
    normalized_host = normalize_whitespace(host).lower()
    return guess_registered_domain(normalized_host) or normalized_host


def remaining_retry_budget(*, attempt_no: int, retry_budget: int) -> int:
    # `retry_budget` is treated as max attempts for the host/route scope.
    return max(0, max(1, retry_budget) - max(1, attempt_no))


def browser_escalation_reason_for_symptoms(symptoms: FetchSymptoms) -> str:
    if symptoms.challenge_page:
        return SYMPTOM_CHALLENGE_PAGE
    if symptoms.redirect_loop:
        return SYMPTOM_REDIRECT_LOOP
    if symptoms.bot_gate:
        return SYMPTOM_BOT_GATE
    if symptoms.empty_js_shell:
        return SYMPTOM_EMPTY_JS_SHELL
    if symptoms.hard_block:
        return SYMPTOM_HTTP_403
    if symptoms.shadow_throttle:
        return SYMPTOM_SHADOW_THROTTLE
    if symptoms.suspiciously_thin_html:
        return SYMPTOM_THIN_HTML
    if symptoms.browser_required:
        return SYMPTOM_BROWSER_REQUIRED
    if symptoms.auth_required:
        return SYMPTOM_AUTH_REQUIRED
    return ""


def retry_reason_for_symptoms(symptoms: FetchSymptoms) -> str:
    if symptoms.rate_limited:
        return SYMPTOM_HTTP_429
    if symptoms.server_error or symptoms.request_error or symptoms.browser_error:
        return symptoms.primary_reason
    return ""


def resolve_transport_policy(context: TransportPolicyContext) -> TransportPolicyDecision:
    route_policy = resolve_route_transport_policy(
        requested_mode=context.requested_mode,
        route_family=context.route_family,
        section_name=context.section_name,
    )
    symptoms = normalize_fetch_symptoms(
        status=context.status,
        response=context.response,
        usable_content=context.usable_content,
        response_text=context.response_text,
        redirect_chain=context.redirect_chain,
    )
    transport_selected = route_policy.transport_selected
    transport_current = normalize_route_name(context.current_transport) or transport_selected
    transport_final = transport_current
    if context.browser_attempt and transport_final != TRANSPORT_PLAYWRIGHT:
        transport_current = TRANSPORT_PLAYWRIGHT
        transport_final = TRANSPORT_PLAYWRIGHT
    escalation_reason = ""
    retry_reason = ""
    blocked_reason = ""
    retry_allowed = False
    manual_handoff_required = False
    blocked_by_policy = False
    cooldown_seconds = 0.0

    browser_trigger_reason = ""
    retry_trigger_reason = ""
    if symptoms.challenge_page:
        browser_trigger_reason = SYMPTOM_CHALLENGE_PAGE
    elif symptoms.redirect_loop:
        browser_trigger_reason = SYMPTOM_REDIRECT_LOOP
    elif symptoms.bot_gate:
        browser_trigger_reason = SYMPTOM_BOT_GATE
    elif symptoms.empty_js_shell:
        browser_trigger_reason = SYMPTOM_EMPTY_JS_SHELL
    elif symptoms.hard_block:
        browser_trigger_reason = SYMPTOM_HTTP_403
    elif symptoms.suspiciously_thin_html or symptoms.shadow_throttle:
        browser_trigger_reason = SYMPTOM_THIN_HTML if symptoms.suspiciously_thin_html else SYMPTOM_SHADOW_THROTTLE
    elif symptoms.auth_required or symptoms.browser_required:
        browser_trigger_reason = SYMPTOM_BROWSER_REQUIRED if symptoms.browser_required else SYMPTOM_AUTH_REQUIRED

    if symptoms.rate_limited:
        retry_trigger_reason = SYMPTOM_HTTP_429
    elif symptoms.server_error or symptoms.request_error or symptoms.browser_error:
        retry_trigger_reason = symptoms.primary_reason

    max_attempts = max(1, context.retry_budget)
    retry_budget_remaining = max(0, max_attempts - max(1, context.attempt_no))
    if retry_trigger_reason and retry_budget_remaining > 0 and not context.cooldown_active:
        retry_allowed = True
        retry_reason = retry_trigger_reason
        escalation_reason = retry_trigger_reason

    if browser_trigger_reason and transport_final != TRANSPORT_PLAYWRIGHT:
        escalation_reason = browser_trigger_reason
        if route_policy.browser_allowed and not context.cooldown_active and context.breaker_mode != BREAKER_MODE_PAUSED:
            transport_final = TRANSPORT_PLAYWRIGHT
        else:
            blocked_by_policy = True
            blocked_reason = route_policy.route_policy_reason or browser_trigger_reason

    if symptoms.challenge_page and (context.browser_attempt or context.session_reused):
        manual_handoff_required = True
        transport_final = TRANSPORT_PLAYWRIGHT if route_policy.browser_allowed else transport_final
        escalation_reason = SYMPTOM_CHALLENGE_PAGE
        blocked_by_policy = blocked_by_policy or not route_policy.browser_allowed
        if blocked_by_policy and not blocked_reason:
            blocked_reason = route_policy.route_policy_reason or SYMPTOM_CHALLENGE_PAGE

    if context.cooldown_active:
        blocked_by_policy = blocked_by_policy or bool(browser_trigger_reason) or transport_final == TRANSPORT_PLAYWRIGHT
        cooldown_seconds = max(context.cooldown_remaining_seconds, 0.0)
        retry_allowed = False
        if not escalation_reason:
            escalation_reason = "cooldown_active"
        if not blocked_reason:
            blocked_reason = "cooldown_active"
    elif symptoms.rate_limited:
        cooldown_seconds = recommended_cooldown_seconds(
            block_class=BLOCK_CLASS_RATE_LIMIT,
            reason=SYMPTOM_HTTP_429,
            event_count=max(1, context.attempt_no),
        )
    elif browser_trigger_reason:
        cooldown_seconds = recommended_cooldown_seconds(
            block_class=BLOCK_CLASS_HARD_BAN if symptoms.hard_block else BLOCK_CLASS_SOFT_BLOCK,
            reason=browser_trigger_reason,
            event_count=max(1, context.attempt_no),
        )

    if route_policy.transport_selected == TRANSPORT_SKIP:
        transport_current = TRANSPORT_SKIP
        transport_final = TRANSPORT_SKIP
        retry_allowed = False
        blocked_by_policy = True
        escalation_reason = escalation_reason or "requested_skip"
        blocked_reason = blocked_reason or "requested_skip"

    should_escalate = (
        not blocked_by_policy
        and transport_current != TRANSPORT_PLAYWRIGHT
        and transport_final == TRANSPORT_PLAYWRIGHT
    )

    return TransportPolicyDecision(
        route_policy=route_policy,
        symptoms=symptoms,
        transport_selected=transport_selected,
        transport_current=transport_current,
        transport_final=transport_final,
        escalation_reason=escalation_reason,
        retry_reason=retry_reason,
        blocked_reason=blocked_reason,
        blocked_by_policy=blocked_by_policy,
        should_retry=retry_allowed,
        should_escalate=should_escalate,
        retry_allowed=retry_allowed,
        retry_budget_remaining=retry_budget_remaining,
        cooldown_key=guess_registered_domain(normalize_whitespace(context.host).lower())
        or normalize_whitespace(context.host).lower(),
        cooldown_seconds=cooldown_seconds,
        cooldown_active=context.cooldown_active,
        manual_handoff_required=manual_handoff_required,
        challenge_detected=symptoms.challenge_page,
    )


def anti_bot_decision_from_policy(
    policy: TransportPolicyDecision,
    *,
    browser_attempt: bool = False,
    session_reused: bool = False,
) -> AntiBotDecision:
    symptoms = policy.symptoms
    if symptoms.usable_content and symptoms.normalized_status in {"success", "ok"}:
        block_class = BLOCK_CLASS_SUCCESS
        anti_bot_reason = ""
    elif symptoms.rate_limited:
        block_class = BLOCK_CLASS_RATE_LIMIT
        anti_bot_reason = SYMPTOM_HTTP_429
    elif symptoms.auth_required:
        block_class = BLOCK_CLASS_AUTH_REQUIRED
        anti_bot_reason = SYMPTOM_AUTH_REQUIRED
    elif symptoms.challenge_page and (browser_attempt or session_reused or policy.manual_handoff_required):
        block_class = BLOCK_CLASS_CHALLENGE_LOOP
        anti_bot_reason = SYMPTOM_CHALLENGE_PAGE
    elif symptoms.challenge_page:
        block_class = BLOCK_CLASS_SOFT_BLOCK
        anti_bot_reason = SYMPTOM_CHALLENGE_PAGE
    elif symptoms.hard_block:
        block_class = BLOCK_CLASS_HARD_BAN
        anti_bot_reason = SYMPTOM_HTTP_403
    elif symptoms.shadow_throttle:
        block_class = BLOCK_CLASS_SHADOW_THROTTLE
        anti_bot_reason = SYMPTOM_SHADOW_THROTTLE
    elif symptoms.bot_gate:
        block_class = BLOCK_CLASS_SOFT_BLOCK
        anti_bot_reason = SYMPTOM_BOT_GATE
    elif symptoms.redirect_loop:
        block_class = BLOCK_CLASS_SOFT_BLOCK
        anti_bot_reason = SYMPTOM_REDIRECT_LOOP
    elif symptoms.empty_js_shell:
        block_class = BLOCK_CLASS_SOFT_BLOCK
        anti_bot_reason = SYMPTOM_EMPTY_JS_SHELL
    elif symptoms.suspiciously_thin_html:
        block_class = BLOCK_CLASS_SOFT_BLOCK
        anti_bot_reason = SYMPTOM_THIN_HTML
    elif symptoms.server_error or symptoms.request_error or symptoms.browser_error or symptoms.browser_required:
        block_class = BLOCK_CLASS_SOFT_BLOCK
        anti_bot_reason = symptoms.primary_reason
    else:
        block_class = BLOCK_CLASS_SOFT_BLOCK
        anti_bot_reason = policy.escalation_reason or symptoms.primary_reason

    current_transport = TRANSPORT_PLAYWRIGHT if browser_attempt else (policy.transport_selected or TRANSPORT_REQUESTS)
    should_escalate = (
        not policy.blocked_by_policy
        and current_transport != TRANSPORT_PLAYWRIGHT
        and policy.transport_final == TRANSPORT_PLAYWRIGHT
    )

    return AntiBotDecision(
        block_class=block_class,
        anti_bot_reason=anti_bot_reason,
        challenge_detected=policy.challenge_detected,
        should_retry=policy.retry_allowed,
        should_escalate=should_escalate,
        manual_handoff_required=policy.manual_handoff_required,
        usable_content=symptoms.usable_content and block_class == BLOCK_CLASS_SUCCESS,
        transport_selected=policy.transport_selected,
        transport_final=policy.transport_final,
        escalation_reason=policy.escalation_reason,
        blocked_by_policy=policy.blocked_by_policy,
        retry_budget_remaining=policy.retry_budget_remaining,
        cooldown_key=policy.cooldown_key,
        cooldown_seconds=policy.cooldown_seconds,
        route_family=policy.route_policy.route_family,
        section_name=policy.route_policy.section_name,
        requested_mode=policy.route_policy.requested_mode,
        symptoms=symptoms,
    )


def classify_fetch_attempt_with_policy(
    *,
    status: str,
    response: requests.Response | None,
    usable_content: bool,
    browser_attempt: bool,
    session_reused: bool,
    host: str = "",
    requested_mode: str = TRANSPORT_REQUESTS,
    route_family: str = "",
    section_name: str = "",
    attempt_no: int = 1,
    retry_budget: int = DEFAULT_RETRY_BUDGET,
    current_transport: str = "",
    breaker_mode: str = BREAKER_MODE_NORMAL,
    cooldown_active: bool = False,
    cooldown_remaining_seconds: float = 0.0,
    redirect_chain: tuple[str, ...] | list[str] | None = None,
    response_text: str = "",
) -> tuple[AntiBotDecision, TransportPolicyDecision]:
    active_transport = normalize_route_name(current_transport)
    if not active_transport:
        active_transport = TRANSPORT_PLAYWRIGHT if browser_attempt else normalize_route_name(requested_mode) or TRANSPORT_REQUESTS
    policy = resolve_transport_policy(
        TransportPolicyContext(
            host=host,
            requested_mode=requested_mode,
            route_family=route_family,
            section_name=section_name,
            status=status,
            response=response,
            response_text=response_text,
            usable_content=usable_content,
            attempt_no=max(1, attempt_no),
            retry_budget=max(1, retry_budget),
            current_transport=active_transport,
            browser_attempt=browser_attempt,
            session_reused=session_reused,
            breaker_mode=breaker_mode,
            cooldown_active=cooldown_active,
            cooldown_remaining_seconds=max(cooldown_remaining_seconds, 0.0),
            redirect_chain=tuple(redirect_chain or ()),
        )
    )
    return anti_bot_decision_from_policy(
        policy,
        browser_attempt=browser_attempt,
        session_reused=session_reused,
    ), policy


class DomainBreakerRegistry:
    def __init__(self) -> None:
        self._states: dict[str, BreakerState] = {}

    def key_for_host(self, host: str) -> str:
        normalized_host = normalize_whitespace(host).lower()
        return guess_registered_domain(normalized_host) or normalized_host

    def state_for_host(self, host: str) -> BreakerState:
        key = self.key_for_host(host)
        state = self._states.get(key)
        if state is None:
            state = BreakerState(domain=key)
            self._states[key] = state
        return state

    def cooldown_status(self, host: str) -> tuple[bool, float]:
        state = self.state_for_host(host)
        return state.cooldown_active, state.cooldown_remaining_seconds

    def bootstrap_from_recent_attempts(
        self,
        host: str,
        recent_attempts: list[dict[str, Any]] | tuple[dict[str, Any], ...] | None,
        *,
        now_ts: float | None = None,
    ) -> BreakerState:
        key = self.key_for_host(host)
        state = self._states.get(key)
        if state is None:
            state = BreakerState(domain=key)
            self._states[key] = state
        elif not _breaker_state_is_pristine(state):
            return state

        if not recent_attempts:
            return state

        replay_now = time.time() if now_ts is None else float(now_ts)
        for item in recent_attempts:
            if not isinstance(item, dict):
                continue
            decision = _breaker_decision_from_runtime_attempt(item)
            if decision is None:
                continue
            recorded_at = _parse_breaker_event_timestamp(item.get("ts")) or replay_now
            persisted_cooldown = _normalize_breaker_cooldown_seconds(item.get("cooldown_seconds"))
            if persisted_cooldown > 0.0:
                cooldown_seconds = persisted_cooldown
            elif _parse_breaker_event_timestamp(item.get("ts")) is not None:
                cooldown_seconds = None
            else:
                cooldown_seconds = 0.0
            _apply_breaker_decision(
                state,
                decision,
                proxy_label_or_id=normalize_whitespace(item.get("proxy_label_or_id")),
                recorded_at=recorded_at,
                cooldown_seconds=cooldown_seconds,
            )
        return state

    def record(self, host: str, decision: AntiBotDecision, *, proxy_label_or_id: str = "") -> BreakerState:
        state = self.state_for_host(host)
        return _apply_breaker_decision(
            state,
            decision,
            proxy_label_or_id=proxy_label_or_id,
            recorded_at=time.time(),
        )


def _apply_breaker_decision(
    state: BreakerState,
    decision: AntiBotDecision,
    *,
    proxy_label_or_id: str,
    recorded_at: float,
    cooldown_seconds: float | None = None,
) -> BreakerState:
    normalized_block_class = normalize_whitespace(decision.block_class).upper()
    state.last_block_class = normalized_block_class
    state.last_reason = decision.anti_bot_reason
    state.last_proxy_label_or_id = proxy_label_or_id
    state.updated_at = recorded_at

    if normalized_block_class == BLOCK_CLASS_SUCCESS:
        state.mode = BREAKER_MODE_NORMAL
        state.soft_block_events = 0
        state.hard_block_events = 0
        state.challenge_loop_events = 0
        state.rate_limit_events = 0
        state.shadow_throttle_events = 0
        state.cooldown_until = 0.0
        return state

    target_mode = state.mode
    effective_cooldown_seconds = cooldown_seconds
    if normalized_block_class == BLOCK_CLASS_RATE_LIMIT:
        state.rate_limit_events += 1
        target_mode = BREAKER_MODE_CONSERVATIVE if state.rate_limit_events == 1 else BREAKER_MODE_SURVIVAL
        if effective_cooldown_seconds is None:
            effective_cooldown_seconds = recommended_cooldown_seconds(
                block_class=normalized_block_class,
                reason=decision.anti_bot_reason,
                event_count=state.rate_limit_events,
            )
    elif normalized_block_class == BLOCK_CLASS_SHADOW_THROTTLE:
        state.shadow_throttle_events += 1
        target_mode = BREAKER_MODE_CONSERVATIVE if state.shadow_throttle_events == 1 else BREAKER_MODE_SURVIVAL
        if effective_cooldown_seconds is None:
            effective_cooldown_seconds = recommended_cooldown_seconds(
                block_class=normalized_block_class,
                reason=decision.anti_bot_reason,
                event_count=state.shadow_throttle_events,
            )
    elif normalized_block_class in {BLOCK_CLASS_SOFT_BLOCK, BLOCK_CLASS_AUTH_REQUIRED}:
        state.soft_block_events += 1
        target_mode = BREAKER_MODE_CONSERVATIVE if state.soft_block_events == 1 else BREAKER_MODE_SURVIVAL
        if effective_cooldown_seconds is None:
            effective_cooldown_seconds = recommended_cooldown_seconds(
                block_class=normalized_block_class,
                reason=decision.anti_bot_reason,
                event_count=state.soft_block_events,
            )
    elif normalized_block_class == BLOCK_CLASS_HARD_BAN:
        state.hard_block_events += 1
        if state.hard_block_events == 1:
            target_mode = BREAKER_MODE_CONSERVATIVE
        elif state.hard_block_events == 2:
            target_mode = BREAKER_MODE_SURVIVAL
        else:
            target_mode = BREAKER_MODE_PAUSED
        if effective_cooldown_seconds is None:
            effective_cooldown_seconds = recommended_cooldown_seconds(
                block_class=normalized_block_class,
                reason=decision.anti_bot_reason,
                event_count=state.hard_block_events,
            )
    elif normalized_block_class == BLOCK_CLASS_CHALLENGE_LOOP:
        state.challenge_loop_events += 1
        target_mode = BREAKER_MODE_SURVIVAL if state.challenge_loop_events == 1 else BREAKER_MODE_PAUSED
        if effective_cooldown_seconds is None:
            effective_cooldown_seconds = recommended_cooldown_seconds(
                block_class=normalized_block_class,
                reason=decision.anti_bot_reason,
                event_count=state.challenge_loop_events,
            )

    state.mode = upgrade_breaker_mode(state.mode, target_mode)
    normalized_cooldown_seconds = _normalize_breaker_cooldown_seconds(effective_cooldown_seconds)
    if normalized_cooldown_seconds > 0.0:
        state.cooldown_until = max(state.cooldown_until, recorded_at + normalized_cooldown_seconds)
    return state


def _breaker_state_is_pristine(state: BreakerState) -> bool:
    return (
        state.mode == BREAKER_MODE_NORMAL
        and state.soft_block_events == 0
        and state.hard_block_events == 0
        and state.challenge_loop_events == 0
        and state.rate_limit_events == 0
        and state.shadow_throttle_events == 0
        and not state.last_block_class
        and not state.last_reason
        and not state.last_proxy_label_or_id
        and state.cooldown_until <= 0.0
    )


def _breaker_decision_from_runtime_attempt(item: dict[str, Any]) -> AntiBotDecision | None:
    block_class = normalize_whitespace(item.get("block_class")).upper()
    if not block_class:
        normalized_status = normalize_route_name(item.get("status"))
        if normalized_status == "success":
            block_class = BLOCK_CLASS_SUCCESS
        else:
            return None
    return AntiBotDecision(
        block_class=block_class,
        anti_bot_reason=normalize_whitespace(item.get("anti_bot_reason")),
        challenge_detected=bool(item.get("challenge_detected")),
    )


def _parse_breaker_event_timestamp(value: Any) -> float | None:
    text = normalize_whitespace(value)
    if not text:
        return None
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        return datetime.fromisoformat(text).timestamp()
    except ValueError:
        return None


def _normalize_breaker_cooldown_seconds(value: Any) -> float:
    if value in (None, "") or isinstance(value, bool):
        return 0.0
    try:
        return max(float(value), 0.0)
    except (TypeError, ValueError):
        return 0.0


@dataclass
class SessionPaths:
    domain: str
    session_file: Path
    storage_state_file: Path


@dataclass
class SessionProfile:
    domain: str
    host: str
    created_at: float
    expires_at: float
    final_url: str = ""
    user_agent: str = ""
    referer: str = ""
    storage_state_path: str = ""
    proxy_label_or_id: str = ""
    cookies: list[dict[str, Any]] = field(default_factory=list)
    manual_bootstrap: bool = False

    @property
    def expired(self) -> bool:
        return self.expires_at <= time.time()


class FactorySiteSessionStore:
    def __init__(self, *, root_dir: str | Path | None = None, ttl_seconds: float | None = None) -> None:
        configured_root = root_dir or os.getenv("FACTORY_SITE_SESSION_ROOT", "").strip()
        if configured_root:
            self.root_dir = Path(configured_root).expanduser()
        else:
            self.root_dir = Path("runtime_local") / "browser_sessions" / "factory_site"
        configured_ttl = os.getenv("FACTORY_SITE_SESSION_TTL_SECONDS", "").strip()
        ttl_value = float(configured_ttl or ttl_seconds or 21_600)
        self.ttl_seconds = max(ttl_value, 300.0)

    def domain_for_host(self, host: str) -> str:
        normalized_host = normalize_whitespace(host).lower()
        return guess_registered_domain(normalized_host) or normalized_host

    def resolve(self, host: str) -> SessionPaths:
        domain = self.domain_for_host(host)
        session_dir = self.root_dir / domain
        return SessionPaths(
            domain=domain,
            session_file=session_dir / "session_profile.json",
            storage_state_file=session_dir / "storage_state.json",
        )

    def load(self, host: str) -> SessionProfile | None:
        paths = self.resolve(host)
        if not paths.session_file.exists():
            return None
        try:
            payload = json.loads(paths.session_file.read_text(encoding="utf-8"))
        except Exception:
            return None
        if not isinstance(payload, dict):
            return None
        profile = SessionProfile(
            domain=str(payload.get("domain", paths.domain) or paths.domain),
            host=str(payload.get("host", "") or ""),
            created_at=float(payload.get("created_at", 0.0) or 0.0),
            expires_at=float(payload.get("expires_at", 0.0) or 0.0),
            final_url=str(payload.get("final_url", "") or ""),
            user_agent=normalize_whitespace(str(payload.get("user_agent", "") or "")),
            referer=normalize_whitespace(str(payload.get("referer", "") or "")),
            storage_state_path=str(payload.get("storage_state_path", "") or ""),
            proxy_label_or_id=str(payload.get("proxy_label_or_id", "") or ""),
            cookies=list(payload.get("cookies") or []),
            manual_bootstrap=bool(payload.get("manual_bootstrap", False)),
        )
        storage_state_path = Path(profile.storage_state_path) if profile.storage_state_path else paths.storage_state_file
        if profile.expired or not storage_state_path.exists():
            return None
        return profile

    def save(
        self,
        host: str,
        *,
        storage_payload: dict[str, Any],
        final_url: str,
        user_agent: str,
        referer: str = "",
        proxy_label_or_id: str = "",
        manual_bootstrap: bool = False,
    ) -> SessionProfile:
        paths = self.resolve(host)
        paths.session_file.parent.mkdir(parents=True, exist_ok=True)
        paths.storage_state_file.write_text(json.dumps(storage_payload, ensure_ascii=False, indent=2), encoding="utf-8")
        created_at = time.time()
        profile_payload = {
            "domain": paths.domain,
            "host": normalize_whitespace(host).lower(),
            "created_at": created_at,
            "expires_at": created_at + self.ttl_seconds,
            "final_url": normalize_whitespace(final_url),
            "user_agent": normalize_whitespace(user_agent),
            "referer": normalize_whitespace(referer),
            "storage_state_path": str(paths.storage_state_file),
            "proxy_label_or_id": normalize_whitespace(proxy_label_or_id),
            "cookies": list(storage_payload.get("cookies") or []),
            "manual_bootstrap": manual_bootstrap,
        }
        paths.session_file.write_text(json.dumps(profile_payload, ensure_ascii=False, indent=2), encoding="utf-8")
        return SessionProfile(
            domain=paths.domain,
            host=normalize_whitespace(host).lower(),
            created_at=created_at,
            expires_at=created_at + self.ttl_seconds,
            final_url=normalize_whitespace(final_url),
            user_agent=normalize_whitespace(user_agent),
            referer=normalize_whitespace(referer),
            storage_state_path=str(paths.storage_state_file),
            proxy_label_or_id=normalize_whitespace(proxy_label_or_id),
            cookies=list(storage_payload.get("cookies") or []),
            manual_bootstrap=manual_bootstrap,
        )


def apply_session_profile_to_requests_session(profile: SessionProfile, session: requests.Session | None) -> bool:
    if session is None:
        return False
    applied = False
    if profile.user_agent:
        session.headers["User-Agent"] = profile.user_agent
        applied = True
    if profile.referer:
        session.headers["Referer"] = profile.referer
        applied = True
    for cookie in profile.cookies:
        try:
            name = str(cookie.get("name", "")).strip()
            value = str(cookie.get("value", ""))
            domain = str(cookie.get("domain", "")).strip() or None
            path = str(cookie.get("path", "/")).strip() or "/"
            if not name:
                continue
            session.cookies.set(name, value, domain=domain, path=path)
            applied = True
        except Exception:
            continue
    return applied


def classify_fetch_attempt(
    *,
    status: str,
    response: requests.Response | None,
    usable_content: bool,
    browser_attempt: bool,
    session_reused: bool,
    host: str = "",
    requested_mode: str = TRANSPORT_REQUESTS,
    route_family: str = "",
    section_name: str = "",
    attempt_no: int = 1,
    retry_budget: int = DEFAULT_RETRY_BUDGET,
    current_transport: str = "",
    breaker_mode: str = BREAKER_MODE_NORMAL,
    cooldown_active: bool = False,
    cooldown_remaining_seconds: float = 0.0,
    redirect_chain: tuple[str, ...] | list[str] | None = None,
    response_text: str = "",
) -> AntiBotDecision:
    decision, _policy = classify_fetch_attempt_with_policy(
        status=status,
        response=response,
        usable_content=usable_content,
        browser_attempt=browser_attempt,
        session_reused=session_reused,
        host=host,
        requested_mode=requested_mode,
        route_family=route_family,
        section_name=section_name,
        attempt_no=attempt_no,
        retry_budget=retry_budget,
        current_transport=current_transport,
        breaker_mode=breaker_mode,
        cooldown_active=cooldown_active,
        cooldown_remaining_seconds=cooldown_remaining_seconds,
        redirect_chain=redirect_chain,
        response_text=response_text,
    )
    if decision.symptoms and decision.symptoms.normalized_status == ACCESS_STATE_PAUSED_BY_BREAKER:
        decision.block_class = BLOCK_CLASS_HARD_BAN
        decision.anti_bot_reason = "paused_by_breaker"
        decision.blocked_by_policy = True
        return decision
    if decision.symptoms and decision.symptoms.normalized_status == ACCESS_STATE_MANUAL_HANDOFF_REQUIRED:
        decision.block_class = BLOCK_CLASS_CHALLENGE_LOOP
        decision.anti_bot_reason = "manual_handoff_required"
        decision.challenge_detected = True
        decision.manual_handoff_required = True
        return decision
    return decision


def derive_access_state(
    *,
    decision: AntiBotDecision,
    breaker_mode: str,
    attempt_no: int,
    session_reused: bool,
    paused_by_breaker: bool = False,
) -> str:
    if paused_by_breaker or breaker_mode == BREAKER_MODE_PAUSED:
        return ACCESS_STATE_PAUSED_BY_BREAKER
    if decision.manual_handoff_required:
        return ACCESS_STATE_MANUAL_HANDOFF_REQUIRED
    if decision.usable_content:
        if attempt_no > 1 or session_reused:
            return ACCESS_STATE_RECOVERED
        return ACCESS_STATE_COMPLETED_WITH_CONTENT
    return ACCESS_STATE_BLOCKED


__all__ = [
    "ACCESS_STATE_BLOCKED",
    "ACCESS_STATE_COMPLETED_WITH_CONTENT",
    "ACCESS_STATE_MANUAL_HANDOFF_REQUIRED",
    "ACCESS_STATE_PAUSED_BY_BREAKER",
    "ACCESS_STATE_RECOVERED",
    "AntiBotDecision",
    "BLOCK_CLASS_AUTH_REQUIRED",
    "BLOCK_CLASS_CHALLENGE_LOOP",
    "BLOCK_CLASS_HARD_BAN",
    "BLOCK_CLASS_RATE_LIMIT",
    "BLOCK_CLASS_SHADOW_THROTTLE",
    "BLOCK_CLASS_SOFT_BLOCK",
    "BLOCK_CLASS_SUCCESS",
    "BREAKER_MODE_CONSERVATIVE",
    "BREAKER_MODE_NORMAL",
    "BREAKER_MODE_PAUSED",
    "BREAKER_MODE_SURVIVAL",
    "BOT_GATE_MARKERS",
    "BreakerState",
    "BROWSER_FIRST_ROUTE_FAMILIES",
    "DEFAULT_RETRY_BUDGET",
    "DomainBreakerRegistry",
    "FactorySiteSessionStore",
    "FetchSymptoms",
    "RouteTransportPolicy",
    "SessionProfile",
    "SessionPaths",
    "SYMPTOM_AUTH_REQUIRED",
    "SYMPTOM_BOT_GATE",
    "SYMPTOM_BROWSER_ERROR",
    "SYMPTOM_BROWSER_REQUIRED",
    "SYMPTOM_CHALLENGE_PAGE",
    "SYMPTOM_EMPTY_JS_SHELL",
    "SYMPTOM_HTTP_403",
    "SYMPTOM_HTTP_429",
    "SYMPTOM_REDIRECT_LOOP",
    "SYMPTOM_REQUEST_ERROR",
    "SYMPTOM_SHADOW_THROTTLE",
    "SYMPTOM_THIN_HTML",
    "TRANSPORT_HYBRID",
    "TRANSPORT_PLAYWRIGHT",
    "TRANSPORT_REQUESTS",
    "TRANSPORT_SKIP",
    "TransportPolicyContext",
    "TransportPolicyDecision",
    "apply_session_profile_to_requests_session",
    "anti_bot_decision_from_policy",
    "block_class_priority",
    "bot_gate_detected_from_text",
    "breaker_mode_rank",
    "classify_fetch_attempt",
    "classify_fetch_attempt_with_policy",
    "normalize_fetch_symptoms",
    "recommended_cooldown_seconds",
    "redirect_loop_detected",
    "derive_access_state",
    "resolve_route_transport_policy",
    "resolve_transport_policy",
    "route_is_high_value",
    "suspiciously_thin_html_from_text",
    "upgrade_breaker_mode",
]

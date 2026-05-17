from __future__ import annotations

import argparse
import json
import multiprocessing
import os
import queue
import shutil
import sys
import tempfile
import time
from contextlib import contextmanager
from dataclasses import asdict, dataclass
from pathlib import Path
from types import SimpleNamespace
from urllib.parse import urlparse

import requests

if __package__ in {None, ""}:
    sys.path.append(str(Path(__file__).resolve().parents[2]))

from app.runtime import ProxyPool
from app.site_intelligence.antibot import (
    ACCESS_STATE_MANUAL_HANDOFF_REQUIRED,
    ACCESS_STATE_PAUSED_BY_BREAKER,
    ACCESS_STATE_RECOVERED,
    BLOCK_CLASS_CHALLENGE_LOOP,
    BLOCK_CLASS_HARD_BAN,
    BLOCK_CLASS_RATE_LIMIT,
    BLOCK_CLASS_SOFT_BLOCK,
    BLOCK_CLASS_SUCCESS,
)
from app.site_intelligence.factory_site_parser import FactorySiteParser, FactorySiteParserCompany
from app.site_intelligence.fetcher import FetchResult, Fetcher
from company_enrichment_core import ProgressStore, RateLimitedHttpClient, RequestOutcome, configure_logger


DEFAULT_LIVE_URL = "https://metallprofil.ru/"
DEFAULT_SCENARIOS = (
    "requests_bot_gate_playwright_success",
    "policy_denied_browser_escalation",
    "rate_limit_retry_success",
    "challenge_loop_manual_handoff",
    "hard_blocks_breaker_pause",
    "proxy_not_reused_after_bot_gate",
    "seeded_session_reuse_success",
)
SESSION_ROOT_ENV = "FACTORY_SITE_SESSION_ROOT"
TERMINAL_ACCESS_STATES = frozenset({ACCESS_STATE_MANUAL_HANDOFF_REQUIRED, ACCESS_STATE_PAUSED_BY_BREAKER})
LIVE_TIMEOUT_REASON = "wall_clock_timeout"


@dataclass
class ScenarioSummary:
    name: str
    status: str
    access_state: str
    block_class: str
    content_records: int
    attempts: int
    manual_handoff_required: bool
    session_reused: bool
    breaker_mode: str
    challenge_detected: bool
    acceptance_reason: str
    transport_selected: str
    transport_final: str
    escalation_reason: str
    blocked_by_policy: bool | None
    proxy_labels: list[str]
    notes: list[str]


@dataclass
class PolicyTelemetrySnapshot:
    transport_selected: str = ""
    transport_selected_present: bool = False
    transport_final: str = ""
    transport_final_present: bool = False
    escalation_reason: str = ""
    escalation_reason_present: bool = False
    blocked_by_policy: bool | None = None
    blocked_by_policy_present: bool = False


class ScriptedClient:
    def __init__(self, outcomes: list[RequestOutcome]) -> None:
        self._outcomes = list(outcomes)
        self.session = requests.Session()
        self.progress_store = None
        self.proxy_pool = None

    def request(
        self,
        url: str,
        *,
        source: str,
        allow_redirects: bool = True,
        timeout: int | None = None,
        proxy_selection: object | None = None,
        **_: object,
    ) -> RequestOutcome:
        if not self._outcomes:
            raise RuntimeError(f"No scripted outcome left for {url} source={source}")
        return self._outcomes.pop(0)


class ScriptedFetcher(Fetcher):
    def __init__(self, client: ScriptedClient, *, proxy_pool: ProxyPool | None, browser_attempts: list[dict[str, object]]) -> None:
        super().__init__(client, proxy_pool=proxy_pool)
        self.browser_attempts = list(browser_attempts)
        self.playwright_enabled = True
        self.max_rate_limit_backoff = 0.0

    def _playwright_fetch(self, url: str, *, proxy_selection: object, session_profile: object | None) -> object:
        if not self.browser_attempts:
            raise RuntimeError(f"No scripted browser attempt left for {url}")
        payload = self.browser_attempts.pop(0)
        response = _build_response(
            str(payload.get("url") or url),
            status_code=int(payload.get("http_status", 200) or 200),
            html=str(payload.get("html") or ""),
        )
        status = str(payload.get("status") or "success")
        storage_payload = None
        if status == "success" and bool(payload.get("save_session", False)):
            storage_payload = {
                "cookies": list(payload.get("cookies") or []),
                "origins": list(payload.get("origins") or []),
            }
            self.session_store.save(
                host=self._host_from_url(url),
                storage_payload=storage_payload,
                final_url=response.url,
                user_agent=str(payload.get("user_agent") or "scripted-browser-agent"),
                referer=url,
                proxy_label_or_id=getattr(proxy_selection, "proxy_label_or_id", ""),
                manual_bootstrap=False,
            )
        return SimpleNamespace(
            attempt_mode="playwright",
            response=response,
            status=status,
            notes=list(payload.get("notes") or [f"scripted playwright {status}"]),
            proxy_selection=proxy_selection,
            timeout=bool(payload.get("timeout", False)),
            blocked=bool(payload.get("blocked", status != "success")),
            session_reused=bool(session_profile),
            cooldown_seconds=int(payload.get("cooldown_seconds", 0) or 0),
            playwright_used=True,
            storage_payload=storage_payload,
            browser_user_agent=str(payload.get("user_agent") or "scripted-browser-agent"),
            final_url=response.url,
        )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Factory-site anti-bot smoke CLI with offline and live modes.")
    parser.add_argument("--mode", choices=("offline", "live", "all"), default="offline")
    parser.add_argument(
        "--scenario",
        action="append",
        dest="scenarios",
        choices=sorted(DEFAULT_SCENARIOS),
        help="Offline scenario to run. Repeat to select multiple; default is all offline scenarios.",
    )
    parser.add_argument("--url", default=DEFAULT_LIVE_URL, help="Live smoke target URL.")
    parser.add_argument("--company-id", default="factory-site-antibot-smoke", help="Company id for live smoke.")
    parser.add_argument("--company-name", default="Factory Site Anti-Bot Smoke", help="Company name for live smoke.")
    parser.add_argument("--max-routes", type=int, default=3, help="Max routes per site for live parser run.")
    parser.add_argument(
        "--max-wall-clock-sec",
        type=float,
        default=0.0,
        help="Hard wall-clock limit for live parser.parse(...); 0 disables the guard.",
    )
    parser.add_argument("--dry-run", action="store_true", help="Run the live parser in dry-run mode.")
    parser.add_argument("--session-root", default="", help="Optional FACTORY_SITE_SESSION_ROOT override.")
    parser.add_argument("--enable-playwright", action="store_true", help="Force ENABLE_PLAYWRIGHT_SITE_FETCH=1 for live mode.")
    parser.add_argument("--playwright-headed", action="store_true", help="Force headed Playwright for live mode.")
    parser.add_argument(
        "--expect-session-reused",
        action="store_true",
        help="Fail live smoke unless the parser reports session_reused=true.",
    )
    parser.add_argument("--keep-dir", default="", help="Optional directory to keep live smoke artifacts.")
    parser.add_argument("--print-json", action="store_true", help="Print JSON summaries in addition to PASS lines.")
    return parser.parse_args()


def _configure_output_streams() -> None:
    for stream_name in ("stdout", "stderr"):
        stream = getattr(sys, stream_name, None)
        if stream is None or not hasattr(stream, "reconfigure"):
            continue
        try:
            stream.reconfigure(encoding="utf-8", errors="backslashreplace")
        except OSError:
            continue


def _assert(condition: bool, message: str) -> None:
    if not condition:
        raise RuntimeError(message)


def _print_header(title: str) -> None:
    print(f"=== {title} ===")


def _print_live_progress(stage: str, *, url: str, max_wall_clock_sec: float, artifact_root: Path) -> None:
    limit_text = f"{max_wall_clock_sec:.3f}" if max_wall_clock_sec > 0 else "disabled"
    print(
        "LIVE_PROGRESS "
        f"stage={stage} "
        f"url={url} "
        f"max_wall_clock_sec={limit_text} "
        f"artifact_root={artifact_root}",
        flush=True,
    )


@contextmanager
def _temporary_session_root(*, seed_from: str | Path = "", parent: str | Path | None = None, cleanup: bool = True):
    previous = os.environ.get(SESSION_ROOT_ENV)
    parent_path = Path(parent).expanduser() if parent else None
    if parent_path is not None:
        parent_path.mkdir(parents=True, exist_ok=True)
        session_root = Path(tempfile.mkdtemp(prefix="factory-site-antibot-session-", dir=str(parent_path)))
    else:
        session_root = Path(tempfile.mkdtemp(prefix="factory-site-antibot-session-"))
    seed_path = Path(seed_from).expanduser() if seed_from else None
    if seed_path and seed_path.exists():
        for item in seed_path.iterdir():
            target = session_root / item.name
            if item.is_dir():
                shutil.copytree(item, target, dirs_exist_ok=True)
            else:
                shutil.copy2(item, target)
    os.environ[SESSION_ROOT_ENV] = str(session_root)
    try:
        yield session_root
    finally:
        if previous is None:
            os.environ.pop(SESSION_ROOT_ENV, None)
        else:
            os.environ[SESSION_ROOT_ENV] = previous
        if cleanup:
            shutil.rmtree(session_root, ignore_errors=True)


def _long_html(title: str, body: str) -> str:
    repeated = " ".join([body] * 6)
    return (
        "<html><head><title>{title}</title></head>"
        "<body><main><h1>{title}</h1><p>{repeated}</p></main></body></html>"
    ).format(title=title, repeated=repeated)


def _challenge_html(title: str) -> str:
    return (
        "<html><head><title>{title}</title></head>"
        "<body><main><h1>{title}</h1><p>captcha verify you are human security check</p></main></body></html>"
    ).format(title=title)


def _build_response(url: str, *, status_code: int, html: str) -> requests.Response:
    response = requests.Response()
    response.status_code = status_code
    response.url = url
    response._content = html.encode("utf-8", errors="ignore")
    response.headers["Content-Type"] = "text/html; charset=utf-8"
    response.encoding = "utf-8"
    return response


def _outcome(
    *,
    url: str,
    status: str,
    ok: bool = False,
    html: str = "",
    http_status: int = 0,
    cooldown_seconds: int = 0,
    blocked: bool = False,
    timeout: bool = False,
) -> RequestOutcome:
    response = None
    if http_status:
        response = _build_response(url, status_code=http_status, html=html)
    return RequestOutcome(
        ok=ok,
        status=status,
        response=response,
        error="" if ok else status,
        host=urlparse(url).netloc.lower(),
        cooldown_seconds=cooldown_seconds,
        timeout=timeout,
        blocked=blocked,
    )


def _summary_from_result(name: str, result: FetchResult) -> ScenarioSummary:
    policy = _policy_snapshot(result)
    content_records = _content_records_from_result(result)
    return ScenarioSummary(
        name=name,
        status=result.status,
        access_state=result.access_state,
        block_class=result.block_class,
        content_records=content_records,
        attempts=len(result.attempts),
        manual_handoff_required=result.manual_handoff_required,
        session_reused=result.session_reused,
        breaker_mode=result.breaker_mode,
        challenge_detected=result.challenge_detected,
        acceptance_reason=_live_acceptance_reason(
            access_state=result.access_state,
            content_records=content_records,
            manual_handoff_required=result.manual_handoff_required,
            session_reused=result.session_reused,
            telemetry=result.attempts,
        ),
        transport_selected=policy.transport_selected,
        transport_final=policy.transport_final,
        escalation_reason=policy.escalation_reason,
        blocked_by_policy=policy.blocked_by_policy,
        proxy_labels=[attempt.proxy_label_or_id for attempt in result.attempts if attempt.proxy_label_or_id],
        notes=list(result.notes),
    )


def _content_records_from_result(result: FetchResult) -> int:
    if not result.completed_with_content or result.response is None:
        return 0
    return 1 if (result.response.text or "").strip() else 0


def _has_explicit_recovery_signal(attempts: list[object]) -> bool:
    if not attempts:
        return False
    prior_attempts = attempts[:-1] if len(attempts) > 1 else attempts
    for attempt in prior_attempts:
        if bool(getattr(attempt, "manual_handoff_required", False)) or bool(getattr(attempt, "challenge_detected", False)):
            return True
        block_class = str(getattr(attempt, "block_class", "")).strip().upper()
        if block_class and block_class != BLOCK_CLASS_SUCCESS:
            return True
        if str(getattr(attempt, "access_state", "")).strip() in TERMINAL_ACCESS_STATES:
            return True
    return False


def _session_reuse_success_detected(*, access_state: str, content_records: int, session_reused: bool) -> bool:
    return access_state == ACCESS_STATE_RECOVERED and content_records > 0 and session_reused


def _live_acceptance_reason(
    *,
    access_state: str,
    content_records: int,
    manual_handoff_required: bool,
    session_reused: bool,
    telemetry: list[object],
) -> str:
    if manual_handoff_required or access_state in TERMINAL_ACCESS_STATES:
        return "manual_handoff_required" if manual_handoff_required else "paused_by_breaker"
    if access_state != ACCESS_STATE_RECOVERED or content_records <= 0:
        return ""
    if _has_explicit_recovery_signal(telemetry):
        return "recovery_signal"
    if _session_reuse_success_detected(
        access_state=access_state,
        content_records=content_records,
        session_reused=session_reused,
    ):
        return "session_reused_seeded"
    return ""


def _live_acceptance_passed(
    *,
    access_state: str,
    content_records: int,
    manual_handoff_required: bool,
    session_reused: bool,
    telemetry: list[object],
) -> bool:
    return bool(
        _live_acceptance_reason(
            access_state=access_state,
            content_records=content_records,
            manual_handoff_required=manual_handoff_required,
            session_reused=session_reused,
            telemetry=telemetry,
        )
    )


def _string_field_with_presence(sources: list[object], field_name: str) -> tuple[str, bool]:
    present = False
    for source in sources:
        if source is None or not hasattr(source, field_name):
            continue
        present = True
        value = getattr(source, field_name)
        if value is None:
            continue
        text = str(value).strip()
        if text:
            return text, True
    return "", present


def _bool_field_with_presence(sources: list[object], field_name: str) -> tuple[bool | None, bool]:
    present = False
    for source in sources:
        if source is None or not hasattr(source, field_name):
            continue
        present = True
        value = getattr(source, field_name)
        if value is None:
            continue
        return bool(value), True
    return None, present


def _policy_snapshot(result: FetchResult | object) -> PolicyTelemetrySnapshot:
    attempts = list(getattr(result, "attempts", []) or [])
    selected_sources = [result, *attempts]
    final_sources = [result, *reversed(attempts)]
    reason_sources = [result, *reversed(attempts), *attempts]
    blocked_sources = [result, *reversed(attempts), *attempts]
    transport_selected, transport_selected_present = _string_field_with_presence(selected_sources, "transport_selected")
    transport_final, transport_final_present = _string_field_with_presence(final_sources, "transport_final")
    escalation_reason, escalation_reason_present = _string_field_with_presence(reason_sources, "escalation_reason")
    blocked_by_policy, blocked_by_policy_present = _bool_field_with_presence(blocked_sources, "blocked_by_policy")
    return PolicyTelemetrySnapshot(
        transport_selected=transport_selected,
        transport_selected_present=transport_selected_present,
        transport_final=transport_final,
        transport_final_present=transport_final_present,
        escalation_reason=escalation_reason,
        escalation_reason_present=escalation_reason_present,
        blocked_by_policy=blocked_by_policy,
        blocked_by_policy_present=blocked_by_policy_present,
    )


def _assert_requests_to_browser_policy(
    result: FetchResult,
    *,
    expected_reason_message: str,
) -> None:
    policy = _policy_snapshot(result)
    if policy.transport_selected_present:
        _assert(policy.transport_selected == "requests", "Expected transport_selected=requests for policy-driven browser escalation.")
    else:
        _assert(bool(result.attempts) and result.attempts[0].fetch_mode == "requests", "Expected requests to be the initial transport.")
    if policy.transport_final_present:
        _assert(policy.transport_final == "playwright", "Expected transport_final=playwright after policy-driven escalation.")
    else:
        _assert(len(result.attempts) >= 2 and result.attempts[-1].playwright_used, "Expected legacy playwright fallback signal on the final attempt.")
        _assert(
            getattr(result.attempts[-1], "escalated_to", "") == "playwright" or getattr(result.attempts[-1], "fetch_mode", "") == "playwright",
            "Expected legacy escalation markers to show browser fallback.",
        )
    if policy.escalation_reason_present:
        _assert(bool(policy.escalation_reason), expected_reason_message)
    if policy.blocked_by_policy_present:
        _assert(policy.blocked_by_policy is False, "Browser escalation should not be blocked by policy in recovery scenario.")


def _assert_policy_refusal_or_legacy_browser_required(result: FetchResult) -> None:
    policy = _policy_snapshot(result)
    if policy.transport_selected_present:
        _assert(policy.transport_selected == "requests", "Expected policy refusal path to begin with requests transport.")
    else:
        _assert(bool(result.attempts) and result.attempts[0].fetch_mode == "requests", "Expected requests attempt before refusal.")
    if policy.transport_final_present:
        _assert(policy.transport_final != "playwright", "Policy refusal path must not finish on browser transport.")
    else:
        _assert(len(result.attempts) == 1, "Legacy refusal path should stop before browser attempt.")
        _assert(not any(getattr(attempt, "playwright_used", False) for attempt in result.attempts), "Legacy refusal path must not use Playwright.")
    if policy.blocked_by_policy_present:
        _assert(policy.blocked_by_policy is True, "Expected blocked_by_policy=true when runtime exposes structured refusal telemetry.")
    else:
        _assert(result.status == "browser_required", "Expected legacy browser_required refusal when structured policy telemetry is absent.")
    if policy.escalation_reason_present:
        _assert(bool(policy.escalation_reason), "Expected escalation_reason for policy refusal when runtime exposes it.")


def _run_requests_bot_gate_playwright_success() -> ScenarioSummary:
    url = "https://offline-antibot.example/"
    client = ScriptedClient(
        [
            _outcome(url=url, status="bot_gate", http_status=403, html=_challenge_html("Bot gate"), blocked=True),
        ]
    )
    fetcher = ScriptedFetcher(
        client,
        proxy_pool=ProxyPool(""),
        browser_attempts=[
            {
                "status": "success",
                "html": _long_html("Recovered homepage", "Factory homepage content with contacts and products."),
                "save_session": True,
            }
        ],
    )
    fetcher.fetch(url, "hybrid", route_family="homepage", section_name="homepage")
    result = fetcher.last_fetch_result
    _assert(result is not None, "Missing fetch result for requests->bot_gate->playwright scenario.")
    _assert(result.access_state == ACCESS_STATE_RECOVERED, "Expected recovered access_state after playwright escalation.")
    _assert(result.block_class == BLOCK_CLASS_SUCCESS, "Expected SUCCESS block_class after recovery.")
    _assert(len(result.attempts) == 2, "Expected exactly 2 attempts for escalation scenario.")
    _assert_requests_to_browser_policy(
        result,
        expected_reason_message="Expected escalation_reason for requests->browser recovery when runtime exposes structured telemetry.",
    )
    _assert(_has_explicit_recovery_signal(result.attempts), "Expected explicit anti-bot recovery signal before recovered success.")
    return _summary_from_result("requests_bot_gate_playwright_success", result)


def _run_policy_denied_browser_escalation() -> ScenarioSummary:
    url = "https://offline-policy-denied.example/catalog"
    client = ScriptedClient(
        [
            _outcome(url=url, status="bot_gate", http_status=403, html=_challenge_html("Policy denied bot gate"), blocked=True),
        ]
    )
    fetcher = ScriptedFetcher(
        client,
        proxy_pool=ProxyPool(""),
        browser_attempts=[
            {
                "status": "success",
                "html": _long_html("Unexpected browser success", "This browser path should stay unused when policy refuses escalation."),
            }
        ],
    )
    fetcher.playwright_enabled = False
    fetcher.fetch(url, "hybrid", route_family="catalog", section_name="catalog")
    result = fetcher.last_fetch_result
    _assert(result is not None, "Missing fetch result for policy refusal scenario.")
    _assert(result.access_state != ACCESS_STATE_RECOVERED, "Policy refusal scenario must not recover through browser fallback.")
    _assert(result.block_class != BLOCK_CLASS_SUCCESS, "Policy refusal scenario must remain blocked.")
    _assert_policy_refusal_or_legacy_browser_required(result)
    return _summary_from_result("policy_denied_browser_escalation", result)


def _run_rate_limit_retry_success() -> ScenarioSummary:
    url = "https://offline-rate-limit.example/contacts"
    client = ScriptedClient(
        [
            _outcome(url=url, status="rate_limited", http_status=429, html="rate limited", cooldown_seconds=2, blocked=True),
            _outcome(
                url=url,
                status="success",
                ok=True,
                http_status=200,
                html=_long_html("Contacts", "Factory contacts and procurement department details."),
            ),
        ]
    )
    fetcher = ScriptedFetcher(client, proxy_pool=ProxyPool(""), browser_attempts=[])
    fetcher.fetch(url, "requests", route_family="contacts", section_name="contacts")
    result = fetcher.last_fetch_result
    _assert(result is not None, "Missing fetch result for rate-limit scenario.")
    _assert(result.access_state == ACCESS_STATE_RECOVERED, "Expected recovered access_state after limited retry.")
    _assert(len(result.attempts) == 2, "Expected a single limited retry after rate limit.")
    _assert(result.attempts[0].block_class == BLOCK_CLASS_RATE_LIMIT, "Expected RATE_LIMIT on first attempt.")
    _assert(_has_explicit_recovery_signal(result.attempts), "Expected explicit blocked-to-recovered signal after rate-limit retry.")
    return _summary_from_result("rate_limit_retry_success", result)


def _run_challenge_loop_manual_handoff() -> ScenarioSummary:
    url = "https://offline-challenge-loop.example/about"
    client = ScriptedClient(
        [
            _outcome(url=url, status="bot_gate", http_status=403, html=_challenge_html("Bot gate"), blocked=True),
        ]
    )
    fetcher = ScriptedFetcher(
        client,
        proxy_pool=ProxyPool(""),
        browser_attempts=[
            {
                "status": "success",
                "html": _challenge_html("Still blocked"),
                "save_session": False,
                "blocked": True,
            }
        ],
    )
    fetcher.fetch(url, "hybrid", route_family="about", section_name="about")
    result = fetcher.last_fetch_result
    _assert(result is not None, "Missing fetch result for challenge loop scenario.")
    _assert(
        result.access_state == ACCESS_STATE_MANUAL_HANDOFF_REQUIRED,
        "Expected manual_handoff_required when challenge persists in browser path.",
    )
    _assert(result.block_class == BLOCK_CLASS_CHALLENGE_LOOP, "Expected CHALLENGE_LOOP classification.")
    _assert(result.manual_handoff_required, "Expected manual_handoff_required flag.")
    _assert_requests_to_browser_policy(
        result,
        expected_reason_message="Expected escalation_reason for challenge-loop browser path when runtime exposes structured telemetry.",
    )
    _assert(
        _live_acceptance_passed(
            access_state=result.access_state,
            content_records=0,
            manual_handoff_required=result.manual_handoff_required,
            session_reused=result.session_reused,
            telemetry=result.attempts,
        ),
        "Expected terminal manual handoff outcome to satisfy smoke acceptance.",
    )
    return _summary_from_result("challenge_loop_manual_handoff", result)


def _run_hard_blocks_breaker_pause() -> ScenarioSummary:
    url = "https://offline-hard-ban.example/products"
    client = ScriptedClient(
        [
            _outcome(url=url, status="http_403", http_status=403, html="forbidden", blocked=True),
        ]
    )
    fetcher = ScriptedFetcher(
        client,
        proxy_pool=ProxyPool(""),
        browser_attempts=[
            {
                "status": "http_403",
                "http_status": 403,
                "html": "forbidden",
                "blocked": True,
                "notes": ["scripted playwright hard block"],
            }
        ],
    )
    fetcher.fetch(url, "requests", route_family="products", section_name="products")
    first = fetcher.last_fetch_result
    _assert(first is not None and first.block_class == BLOCK_CLASS_HARD_BAN, "First hard block should classify as HARD_BAN.")
    fetcher.fetch(url, "requests", route_family="products", section_name="products")
    paused = fetcher.last_fetch_result
    _assert(paused is not None, "Missing paused breaker result.")
    _assert(paused.access_state == ACCESS_STATE_PAUSED_BY_BREAKER, "Expected paused_by_breaker on third attempt.")
    _assert(paused.status == ACCESS_STATE_PAUSED_BY_BREAKER, "Expected paused status on third attempt.")
    _assert(
        _live_acceptance_passed(
            access_state=paused.access_state,
            content_records=0,
            manual_handoff_required=paused.manual_handoff_required,
            session_reused=paused.session_reused,
            telemetry=paused.attempts,
        ),
        "Expected paused_by_breaker outcome to satisfy smoke acceptance.",
    )
    return _summary_from_result("hard_blocks_breaker_pause", paused)


def _run_proxy_not_reused_after_bot_gate() -> ScenarioSummary:
    url = "https://offline-proxy-rotation.example/services"
    proxy_pool = ProxyPool(
        "http://proxy-one.example:8080,http://proxy-two.example:8080",
        strategy="sticky_by_host",
    )
    client = ScriptedClient(
        [
            _outcome(url=url, status="bot_gate", http_status=403, html=_challenge_html("Bot gate"), blocked=True),
        ]
    )
    fetcher = ScriptedFetcher(
        client,
        proxy_pool=proxy_pool,
        browser_attempts=[
            {
                "status": "success",
                "html": _long_html("Services", "Factory services, installation support, and maintenance."),
            }
        ],
    )
    fetcher.fetch(url, "hybrid", route_family="services", section_name="services")
    result = fetcher.last_fetch_result
    _assert(result is not None, "Missing fetch result for proxy reuse scenario.")
    _assert(len(result.attempts) == 2, "Expected exactly 2 attempts for proxy reuse scenario.")
    _assert(result.attempts[0].proxy_label_or_id, "Expected proxy label on first attempt.")
    _assert(result.attempts[1].proxy_label_or_id, "Expected proxy label on second attempt.")
    _assert(
        result.attempts[0].proxy_label_or_id != result.attempts[1].proxy_label_or_id,
        "Expected a different proxy after bot_gate instead of reusing the blocked proxy.",
    )
    _assert(result.access_state == ACCESS_STATE_RECOVERED, "Expected recovered access_state after proxy rotation.")
    _assert_requests_to_browser_policy(
        result,
        expected_reason_message="Expected escalation_reason for proxy-rotation browser recovery when runtime exposes structured telemetry.",
    )
    _assert(_has_explicit_recovery_signal(result.attempts), "Expected explicit anti-bot recovery signal before recovered proxy-rotation success.")
    return _summary_from_result("proxy_not_reused_after_bot_gate", result)


def _run_seeded_session_reuse_success() -> ScenarioSummary:
    url = "https://offline-seeded-session.example/contacts"
    client = ScriptedClient(
        [
            _outcome(
                url=url,
                status="success",
                ok=True,
                http_status=200,
                html=_long_html("Contacts", "Factory contacts with seeded browser session reuse."),
            ),
        ]
    )
    fetcher = ScriptedFetcher(client, proxy_pool=ProxyPool(""), browser_attempts=[])
    host = urlparse(url).netloc.lower()
    fetcher.session_store.save(
        host=host,
        storage_payload={"cookies": [], "origins": []},
        final_url=url,
        user_agent="seeded-session-agent",
        referer=url,
        proxy_label_or_id="",
        manual_bootstrap=True,
    )
    fetcher.fetch(url, "requests", route_family="contacts", section_name="contacts")
    result = fetcher.last_fetch_result
    _assert(result is not None, "Missing fetch result for seeded session scenario.")
    _assert(result.access_state == ACCESS_STATE_RECOVERED, "Expected recovered access_state on seeded-session rerun.")
    _assert(result.session_reused, "Expected session_reused=true on seeded-session rerun.")
    _assert(len(result.attempts) == 1, "Expected a single successful attempt on seeded-session rerun.")
    _assert(not _has_explicit_recovery_signal(result.attempts), "Seeded-session rerun should not need a current blocked/challenge attempt.")
    _assert(
        _live_acceptance_reason(
            access_state=result.access_state,
            content_records=_content_records_from_result(result),
            manual_handoff_required=result.manual_handoff_required,
            session_reused=result.session_reused,
            telemetry=result.attempts,
        )
        == "session_reused_seeded",
        "Expected seeded-session acceptance reason.",
    )
    _assert(
        _live_acceptance_passed(
            access_state=result.access_state,
            content_records=_content_records_from_result(result),
            manual_handoff_required=result.manual_handoff_required,
            session_reused=result.session_reused,
            telemetry=result.attempts,
        ),
        "Expected seeded-session rerun to satisfy smoke acceptance.",
    )
    return _summary_from_result("seeded_session_reuse_success", result)


def _offline_scenarios() -> dict[str, callable]:
    return {
        "requests_bot_gate_playwright_success": _run_requests_bot_gate_playwright_success,
        "policy_denied_browser_escalation": _run_policy_denied_browser_escalation,
        "rate_limit_retry_success": _run_rate_limit_retry_success,
        "challenge_loop_manual_handoff": _run_challenge_loop_manual_handoff,
        "hard_blocks_breaker_pause": _run_hard_blocks_breaker_pause,
        "proxy_not_reused_after_bot_gate": _run_proxy_not_reused_after_bot_gate,
        "seeded_session_reuse_success": _run_seeded_session_reuse_success,
    }


def _run_offline_smoke(selected: list[str], *, print_json: bool) -> list[ScenarioSummary]:
    summaries: list[ScenarioSummary] = []
    registry = _offline_scenarios()
    with _temporary_session_root():
        for name in selected:
            _print_header(name)
            summary = registry[name]()
            summaries.append(summary)
            if print_json:
                print(json.dumps(asdict(summary), ensure_ascii=False, indent=2))
            print(
                "PASS "
                f"name={summary.name} "
                f"status={summary.status} "
                f"access_state={summary.access_state} "
                f"block_class={summary.block_class} "
                f"content_records={summary.content_records} "
                f"attempts={summary.attempts} "
                f"acceptance_reason={summary.acceptance_reason or '-'} "
                f"transport_selected={summary.transport_selected or '-'} "
                f"transport_final={summary.transport_final or '-'} "
                f"escalation_reason={summary.escalation_reason or '-'} "
                f"blocked_by_policy={summary.blocked_by_policy if summary.blocked_by_policy is not None else '-'} "
                f"breaker_mode={summary.breaker_mode}"
            )
    return summaries


def _live_args_payload(args: argparse.Namespace) -> dict[str, object]:
    return {
        "url": args.url,
        "company_id": args.company_id,
        "company_name": args.company_name,
        "max_routes": max(1, int(args.max_routes)),
        "max_wall_clock_sec": max(0.0, float(args.max_wall_clock_sec or 0.0)),
        "dry_run": bool(args.dry_run),
        "session_root": args.session_root,
        "enable_playwright": bool(args.enable_playwright),
        "playwright_headed": bool(args.playwright_headed),
        "expect_session_reused": bool(args.expect_session_reused),
    }


def _live_summary_defaults(args_payload: dict[str, object], *, artifact_root: Path) -> dict[str, object]:
    return {
        "status": "pending",
        "completed": False,
        "timed_out": False,
        "wall_clock_sec": 0.0,
        "max_wall_clock_sec": float(args_payload.get("max_wall_clock_sec") or 0.0),
        "url": str(args_payload.get("url") or ""),
        "company_id": str(args_payload.get("company_id") or ""),
        "company_name": str(args_payload.get("company_name") or ""),
        "access_state": "",
        "block_class": "",
        "anti_bot_reason": "",
        "breaker_mode": "",
        "manual_handoff_required": False,
        "challenge_detected": False,
        "session_reused": False,
        "content_records": 0,
        "plans": 0,
        "fetch_attempts": 0,
        "sample_urls": [],
        "artifact_root": str(artifact_root),
        "proxy_pool_enabled": False,
        "proxy_pool_count": 0,
        "recovery_signal_detected": False,
        "session_reuse_success_detected": False,
        "acceptance_reason": "",
        "acceptance_outcome": False,
        "exception_type": "",
        "exception_message": "",
    }


def _live_timeout_summary(args_payload: dict[str, object], *, artifact_root: Path, wall_clock_sec: float) -> dict[str, object]:
    summary = _live_summary_defaults(args_payload, artifact_root=artifact_root)
    summary.update(
        {
            "status": "timeout",
            "timed_out": True,
            "wall_clock_sec": round(wall_clock_sec, 3),
            "acceptance_reason": LIVE_TIMEOUT_REASON,
            "acceptance_outcome": False,
            "exception_type": "TimeoutError",
            "exception_message": (
                "Live parser exceeded wall-clock limit "
                f"of {float(args_payload.get('max_wall_clock_sec') or 0.0):.3f} sec."
            ),
        }
    )
    return summary


def _run_live_smoke_once(args_payload: dict[str, object], *, artifact_root: Path) -> dict[str, object]:
    summary = _live_summary_defaults(args_payload, artifact_root=artifact_root)
    parse_started_at: float | None = None
    if bool(args_payload.get("enable_playwright")):
        os.environ["ENABLE_PLAYWRIGHT_SITE_FETCH"] = "1"
    if bool(args_payload.get("playwright_headed")):
        os.environ["PLAYWRIGHT_SITE_FETCH_HEADLESS"] = "0"

    artifact_root.mkdir(parents=True, exist_ok=True)
    session_root_raw = str(args_payload.get("session_root") or "").strip()
    session_seed = Path(session_root_raw).expanduser() if session_root_raw else None
    try:
        with _temporary_session_root(seed_from=session_seed, parent=artifact_root):
            logger = configure_logger(artifact_root / "live_smoke.log")
            progress_store = ProgressStore(artifact_root / "progress")
            proxy_pool = ProxyPool(os.getenv("PARSER_PROXIES"), proxy_file=os.getenv("PARSER_PROXIES_FILE", "").strip())
            summary["proxy_pool_enabled"] = proxy_pool.enabled()
            summary["proxy_pool_count"] = len(proxy_pool.entries)
            client = RateLimitedHttpClient(
                logger=logger,
                progress_store=progress_store,
                min_delay_by_host={},
                request_timeout=15,
                cooldown_on_429=30,
                cooldown_on_bot=120,
                proxy_pool=proxy_pool,
            )
            parser = FactorySiteParser(
                client,
                proxy_pool=proxy_pool,
                max_sites=1,
                max_routes_per_site=int(args_payload["max_routes"]),
            )
            company = FactorySiteParserCompany(
                company_id=str(args_payload["company_id"]),
                company_name=str(args_payload["company_name"]),
                input_site=str(args_payload["url"]),
                candidate_sites=[str(args_payload["url"])],
            )
            _print_live_progress(
                "parse_started",
                url=str(args_payload["url"]),
                max_wall_clock_sec=float(args_payload.get("max_wall_clock_sec") or 0.0),
                artifact_root=artifact_root,
            )
            parse_started_at = time.monotonic()
            result = parser.parse(company, dry_run=bool(args_payload["dry_run"]))
            telemetry = list(getattr(result, "fetch_telemetry", []))
            summary.update(
                {
                    "completed": True,
                    "wall_clock_sec": round(time.monotonic() - parse_started_at, 3),
                    "access_state": result.access_state,
                    "block_class": result.block_class,
                    "anti_bot_reason": result.anti_bot_reason,
                    "breaker_mode": result.breaker_mode,
                    "manual_handoff_required": result.manual_handoff_required,
                    "challenge_detected": result.challenge_detected,
                    "session_reused": result.session_reused,
                    "content_records": len(result.content_records),
                    "plans": len(result.plans),
                    "fetch_attempts": sum(len(plan.fetch_telemetry) for plan in result.plans),
                    "sample_urls": [record.url for record in result.content_records[:3]],
                    "recovery_signal_detected": _has_explicit_recovery_signal(telemetry),
                    "session_reuse_success_detected": _session_reuse_success_detected(
                        access_state=result.access_state,
                        content_records=len(result.content_records),
                        session_reused=result.session_reused,
                    ),
                    "acceptance_reason": _live_acceptance_reason(
                        access_state=result.access_state,
                        content_records=len(result.content_records),
                        manual_handoff_required=result.manual_handoff_required,
                        session_reused=result.session_reused,
                        telemetry=telemetry,
                    ),
                }
            )
            summary["acceptance_outcome"] = bool(summary["acceptance_reason"])
            if bool(args_payload.get("expect_session_reused")) and not bool(summary["session_reused"]):
                summary["status"] = "failed"
                summary["exception_type"] = "RuntimeError"
                summary["exception_message"] = "Live smoke expected session_reused=true."
                return summary
            if not bool(summary["acceptance_outcome"]):
                summary["status"] = "failed"
                summary["exception_type"] = "RuntimeError"
                summary["exception_message"] = (
                    "Live smoke requires explicit anti-bot recovery telemetry or terminal "
                    "manual_handoff_required/paused_by_breaker outcome."
                )
                return summary
            summary["status"] = "ok"
            return summary
    except Exception as exc:
        if parse_started_at is not None and not summary["wall_clock_sec"]:
            summary["wall_clock_sec"] = round(time.monotonic() - parse_started_at, 3)
        summary["status"] = "exception"
        summary["exception_type"] = type(exc).__name__
        summary["exception_message"] = str(exc)
        return summary


def _run_live_smoke_worker(
    args_payload: dict[str, object],
    artifact_root: str,
    result_queue: multiprocessing.queues.Queue,
) -> None:
    result_queue.put(_run_live_smoke_once(args_payload, artifact_root=Path(artifact_root)))


def _run_live_smoke_with_guard(
    args_payload: dict[str, object],
    *,
    artifact_root: Path,
    max_wall_clock_sec: float,
) -> dict[str, object]:
    ctx = multiprocessing.get_context("spawn")
    result_queue = ctx.Queue(maxsize=1)
    process = ctx.Process(
        target=_run_live_smoke_worker,
        args=(args_payload, str(artifact_root), result_queue),
    )
    started_at = time.monotonic()
    process.start()
    process.join(timeout=max_wall_clock_sec)
    elapsed = time.monotonic() - started_at
    try:
        if process.is_alive():
            _print_live_progress(
                "timeout_guard_triggered",
                url=str(args_payload["url"]),
                max_wall_clock_sec=max_wall_clock_sec,
                artifact_root=artifact_root,
            )
            process.terminate()
            process.join(timeout=5)
            if process.is_alive() and hasattr(process, "kill"):
                process.kill()
                process.join(timeout=5)
            return _live_timeout_summary(args_payload, artifact_root=artifact_root, wall_clock_sec=elapsed)
        try:
            return result_queue.get_nowait()
        except queue.Empty:
            summary = _live_summary_defaults(args_payload, artifact_root=artifact_root)
            summary.update(
                {
                    "status": "exception",
                    "wall_clock_sec": round(elapsed, 3),
                    "exception_type": "RuntimeError",
                    "exception_message": f"Live worker exited without summary. exitcode={process.exitcode}",
                }
            )
            return summary
    finally:
        result_queue.close()
        result_queue.join_thread()


def _run_live_smoke(args: argparse.Namespace) -> dict[str, object]:
    args_payload = _live_args_payload(args)
    temp_root = Path(args.keep_dir).expanduser() if args.keep_dir.strip() else Path(tempfile.mkdtemp(prefix="factory-site-antibot-live-"))
    cleanup = not args.keep_dir.strip()
    max_wall_clock_sec = float(args_payload["max_wall_clock_sec"])
    try:
        _print_live_progress(
            "live_run_configured",
            url=str(args_payload["url"]),
            max_wall_clock_sec=max_wall_clock_sec,
            artifact_root=temp_root,
        )
        if max_wall_clock_sec > 0:
            return _run_live_smoke_with_guard(
                args_payload,
                artifact_root=temp_root,
                max_wall_clock_sec=max_wall_clock_sec,
            )
        return _run_live_smoke_once(args_payload, artifact_root=temp_root)
    finally:
        if cleanup:
            shutil.rmtree(temp_root, ignore_errors=True)


def main() -> int:
    _configure_output_streams()
    args = parse_args()
    exit_code = 0
    selected_scenarios = args.scenarios or list(DEFAULT_SCENARIOS)
    if args.mode in {"offline", "all"}:
        _run_offline_smoke(selected_scenarios, print_json=bool(args.print_json))
    if args.mode in {"live", "all"}:
        _print_header("live_factory_site_antibot")
        summary = _run_live_smoke(args)
        if args.print_json:
            print(json.dumps(summary, ensure_ascii=False, indent=2))
        print(
            "LIVE_RESULT "
            f"url={summary['url']} "
            f"status={summary['status']} "
            f"timed_out={summary['timed_out']} "
            f"wall_clock_sec={summary['wall_clock_sec']} "
            f"max_wall_clock_sec={summary['max_wall_clock_sec'] if summary['max_wall_clock_sec'] else '-'} "
            f"access_state={summary['access_state']} "
            f"block_class={summary['block_class'] or '-'} "
            f"manual_handoff_required={summary['manual_handoff_required']} "
            f"session_reused={summary['session_reused']} "
            f"content_records={summary['content_records']} "
            f"acceptance_reason={summary['acceptance_reason'] or '-'} "
            f"breaker_mode={summary['breaker_mode']} "
            f"exception_type={summary['exception_type'] or '-'} "
            f"exception_message={json.dumps(summary['exception_message'], ensure_ascii=False) if summary['exception_message'] else '-'}"
        )
        if summary["status"] != "ok":
            exit_code = 1
    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())

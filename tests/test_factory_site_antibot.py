from __future__ import annotations

import json
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest import mock
from types import SimpleNamespace

import requests

from app.runtime import ProgressStore, load_stage_messages
from app.runtime.host_memory import normalize_host_memory_state, update_host_memory_from_event_payload
from app.runtime.proxies import ProxyPool, ProxySelection
from app.site_intelligence.antibot import (
    ACCESS_STATE_MANUAL_HANDOFF_REQUIRED,
    ACCESS_STATE_PAUSED_BY_BREAKER,
    ACCESS_STATE_RECOVERED,
    BLOCK_CLASS_CHALLENGE_LOOP,
    BLOCK_CLASS_HARD_BAN,
    BLOCK_CLASS_RATE_LIMIT,
    BLOCK_CLASS_SOFT_BLOCK,
    BLOCK_CLASS_SUCCESS,
    BREAKER_MODE_NORMAL,
    BREAKER_MODE_PAUSED,
    BREAKER_MODE_SURVIVAL,
    DomainBreakerRegistry,
    SessionProfile,
)
from app.site_intelligence.fetcher import Fetcher, _AttemptExecution
from app.site_intelligence.models import SiteProbe
from app.site_intelligence.serialization import site_probe_from_dict, site_probe_to_dict
from scripts.smoke.run_factory_site_antibot_smoke import _live_acceptance_passed


_MISSING = object()


class FakeClient:
    def __init__(self, *, progress_store: object | None = None) -> None:
        self.session = requests.Session()
        self.progress_store = progress_store

    def request(self, url: str, **kwargs: object) -> object:
        raise AssertionError(f"unexpected client.request call for {url!r} with {kwargs!r}")


def make_response(
    *,
    url: str,
    text: str,
    status_code: int = 200,
    content_type: str = "text/html; charset=utf-8",
) -> requests.Response:
    response = requests.Response()
    response.status_code = status_code
    response.url = url
    response.encoding = "utf-8"
    response._content = text.encode("utf-8", errors="ignore")
    response.headers["Content-Type"] = content_type
    return response


def make_usable_html(label: str = "factory") -> str:
    body = " ".join([label] * 320)
    return f"<html><body><main><h1>{label}</h1><p>{body}</p></main></body></html>"


def make_challenge_html() -> str:
    return "<html><body><h1>Security check</h1><p>Please verify you are human via captcha.</p></body></html>"


def make_execution(
    *,
    attempt_mode: str,
    status: str,
    proxy_selection: ProxySelection,
    response: requests.Response | None = None,
    notes: list[str] | None = None,
    cooldown_seconds: int = 0,
    session_reused: bool = False,
    blocked: bool | None = None,
) -> _AttemptExecution:
    return _AttemptExecution(
        attempt_mode=attempt_mode,
        response=response,
        status=status,
        notes=notes or [status],
        proxy_selection=proxy_selection,
        blocked=(status != "success") if blocked is None else blocked,
        session_reused=session_reused,
        cooldown_seconds=cooldown_seconds,
        playwright_used=attempt_mode == "playwright",
    )


def make_session_profile(*, proxy_label_or_id: str) -> SessionProfile:
    expires_at = (datetime.now(timezone.utc) + timedelta(minutes=5)).timestamp()
    return SessionProfile(
        domain="factory.example",
        host="factory.example",
        created_at=expires_at - 60,
        expires_at=expires_at,
        final_url="https://factory.example/session",
        user_agent="test-agent",
        referer="https://factory.example/",
        storage_state_path="",
        proxy_label_or_id=proxy_label_or_id,
        cookies=[],
        manual_bootstrap=False,
    )


def load_jsonl(path: Path) -> list[dict[str, object]]:
    if not path.exists():
        return []
    return [
        json.loads(line)
        for line in path.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]


def _attr_or_missing(target: object | None, name: str) -> object:
    if target is None:
        return _MISSING
    return getattr(target, name, _MISSING)


def _first_present(*values: object) -> object:
    for value in values:
        if value is not _MISSING:
            return value
    return _MISSING


class FactorySiteAntiBotTests(unittest.TestCase):
    def test_site_probe_serialization_round_trip_preserves_policy_snapshot(self) -> None:
        probe = SiteProbe(
            url="https://factory.example/",
            status="success",
            transport_selected="requests",
            transport_final="playwright",
            blocked_by_policy=True,
            escalation_reason="challenge_page",
        )

        payload = site_probe_to_dict(probe)
        expected_snapshot = {
            "transport_selected": "requests",
            "transport_final": "playwright",
            "blocked_by_policy": True,
            "escalation_reason": "challenge_page",
        }

        self.assertEqual({key: payload[key] for key in expected_snapshot}, expected_snapshot)
        self.assertIs(payload["blocked_by_policy"], True)

        restored = site_probe_from_dict(payload)

        self.assertEqual(restored.transport_selected, "requests")
        self.assertEqual(restored.transport_final, "playwright")
        self.assertIs(restored.blocked_by_policy, True)
        self.assertEqual(restored.escalation_reason, "challenge_page")

        round_trip_payload = site_probe_to_dict(restored)

        self.assertEqual({key: round_trip_payload[key] for key in expected_snapshot}, expected_snapshot)
        self.assertIs(round_trip_payload["blocked_by_policy"], True)

    def _assert_transport_contract(
        self,
        fetcher: Fetcher,
        *,
        transport_selected: str,
        transport_final: str,
        escalation_reason: str | None = None,
        blocked_by_policy: bool | None = None,
        fallback_blocked_by_policy: bool | object = _MISSING,
    ) -> None:
        result = fetcher.last_fetch_result
        self.assertIsNotNone(result)
        attempts = result.attempts
        self.assertTrue(attempts)
        first_attempt = attempts[0]
        final_attempt = attempts[-1]

        selected_value = _first_present(
            _attr_or_missing(result, "transport_selected"),
            _attr_or_missing(first_attempt, "transport_selected"),
            _attr_or_missing(first_attempt, "fetch_mode"),
        )
        final_value = _first_present(
            _attr_or_missing(result, "transport_final"),
            _attr_or_missing(final_attempt, "transport_final"),
            _attr_or_missing(final_attempt, "fetch_mode"),
        )

        self.assertEqual(selected_value, transport_selected)
        self.assertEqual(final_value, transport_final)

        if escalation_reason is not None:
            actual_reason = _first_present(
                _attr_or_missing(result, "escalation_reason"),
                _attr_or_missing(final_attempt, "escalation_reason"),
                _attr_or_missing(first_attempt, "escalation_reason"),
                _attr_or_missing(result, "anti_bot_reason"),
                _attr_or_missing(final_attempt, "anti_bot_reason"),
                _attr_or_missing(first_attempt, "anti_bot_reason"),
                _attr_or_missing(result, "block_class"),
                _attr_or_missing(final_attempt, "block_class"),
                _attr_or_missing(first_attempt, "block_class"),
            )
            self.assertEqual(actual_reason, escalation_reason)

        if blocked_by_policy is not None:
            actual_blocked = _first_present(
                _attr_or_missing(result, "blocked_by_policy"),
                _attr_or_missing(final_attempt, "blocked_by_policy"),
                _attr_or_missing(first_attempt, "blocked_by_policy"),
                fallback_blocked_by_policy,
            )
            self.assertIsNot(actual_blocked, _MISSING)
            self.assertEqual(bool(actual_blocked), blocked_by_policy)

    def make_fetcher(self, *, progress_store: object | None = None) -> Fetcher:
        fetcher = Fetcher(FakeClient(progress_store=progress_store))
        fetcher.playwright_enabled = True
        fetcher.max_attempts = 2
        fetcher.session_store.load = lambda host: None
        return fetcher

    def _assert_terminal_progress_surfaces(
        self,
        progress_store: ProgressStore,
        *,
        raw_status: str,
        transport_final: str,
        escalation_reason: str,
        raw_anti_bot_reason: str,
        raw_block_class: str,
        raw_http_status: int | None,
        raw_cooldown_seconds: int,
        expected_first_signal_tags: list[str],
        raw_blocked_by_policy: bool | None = None,
        expected_event_types: tuple[str, str] = ("route_fetch_attempt", "route_fetch_terminal"),
        expected_first_fetch_mode: str = "requests",
        expected_first_transport_selected: str | None = None,
        expected_first_transport_final: str | object = _MISSING,
        expected_first_playwright_used: bool = False,
        expected_first_playwright_fallback_used: bool = False,
        expected_terminal_fetch_mode: str = "requests",
        expected_terminal_transport_selected: str = "requests",
        expected_terminal_playwright_used: bool = False,
        expected_terminal_playwright_fallback_used: bool = False,
    ) -> None:
        expected_first_transport_selected = expected_first_transport_selected or expected_first_fetch_mode
        events = load_jsonl(progress_store.events_jsonl)
        self.assertEqual([event["type"] for event in events], list(expected_event_types))
        first_event = events[0]
        terminal_event = events[-1]

        self.assertEqual(first_event["status"], raw_status)
        if raw_blocked_by_policy is not None:
            self.assertIs(first_event["blocked_by_policy"], raw_blocked_by_policy)
        self.assertEqual(first_event["fetch_mode"], expected_first_fetch_mode)
        self.assertEqual(first_event["transport_selected"], expected_first_transport_selected)
        if expected_first_transport_final is not _MISSING:
            self.assertEqual(first_event["transport_final"], expected_first_transport_final)
        self.assertIs(first_event["playwright_used"], expected_first_playwright_used)
        self.assertIs(first_event["playwright_fallback_used"], expected_first_playwright_fallback_used)
        self.assertEqual(first_event["anti_bot_reason"], raw_anti_bot_reason)
        self.assertEqual(first_event["block_class"], raw_block_class)
        self.assertEqual(first_event["http_status"], raw_http_status)
        self.assertEqual(first_event["cooldown_seconds"], raw_cooldown_seconds)
        self.assertEqual(terminal_event["status"], "blocked_by_policy")
        self.assertIs(terminal_event["blocked_by_policy"], True)
        self.assertEqual(terminal_event["fetch_mode"], expected_terminal_fetch_mode)
        self.assertEqual(terminal_event["transport_selected"], expected_terminal_transport_selected)
        self.assertEqual(terminal_event["transport_final"], transport_final)
        self.assertEqual(terminal_event["escalation_reason"], escalation_reason)
        self.assertEqual(terminal_event["attempt_no"], 1)
        self.assertEqual(terminal_event["escalated_to"], "")
        self.assertIs(terminal_event["playwright_used"], expected_terminal_playwright_used)
        self.assertIs(terminal_event["playwright_fallback_used"], expected_terminal_playwright_fallback_used)
        self.assertEqual(terminal_event["anti_bot_reason"], "")
        self.assertEqual(terminal_event["block_class"], "")
        self.assertEqual(terminal_event["cooldown_seconds"], 0)
        self.assertIsNone(terminal_event["http_status"])
        self.assertFalse(progress_store.results_jsonl.exists())

        stage_messages = load_stage_messages(progress_store.output_dir)
        self.assertEqual(
            [message["payload"]["event_type"] for message in stage_messages],
            list(expected_event_types),
        )
        self.assertEqual(stage_messages[0]["payload"]["status"], raw_status)
        self.assertEqual(stage_messages[-1]["payload"]["status"], "blocked_by_policy")

        host_recent_attempts = normalize_host_memory_state(progress_store.host_memory)["factory.example"]["recent_attempts"][-2:]
        self.assertEqual(
            [attempt["event_type"] for attempt in host_recent_attempts],
            list(expected_event_types),
        )
        first_host_attempt = host_recent_attempts[0]
        terminal_host_attempt = host_recent_attempts[-1]
        self.assertEqual(first_host_attempt["status"], raw_status)
        if raw_blocked_by_policy is not None:
            self.assertIs(first_host_attempt["blocked_by_policy"], raw_blocked_by_policy)
        self.assertEqual(first_host_attempt["transport_selected"], expected_first_transport_selected)
        if expected_first_transport_final is not _MISSING:
            self.assertEqual(first_host_attempt["transport_final"], expected_first_transport_final)
        self.assertEqual(first_host_attempt["anti_bot_reason"], raw_anti_bot_reason)
        self.assertEqual(first_host_attempt["block_class"], raw_block_class)
        self.assertEqual(first_host_attempt["http_status"], raw_http_status)
        self.assertEqual(first_host_attempt["cooldown_seconds"], float(raw_cooldown_seconds))
        self.assertEqual(first_host_attempt["signal_tags"], expected_first_signal_tags)
        self.assertEqual(terminal_host_attempt["status"], "blocked_by_policy")
        self.assertIs(terminal_host_attempt["blocked_by_policy"], True)
        self.assertEqual(terminal_host_attempt["transport_selected"], expected_terminal_transport_selected)
        self.assertEqual(terminal_host_attempt["transport_final"], transport_final)
        self.assertEqual(terminal_host_attempt["anti_bot_reason"], "")
        self.assertEqual(terminal_host_attempt["block_class"], "")
        self.assertIsNone(terminal_host_attempt["http_status"])
        self.assertEqual(terminal_host_attempt["cooldown_seconds"], 0.0)
        self.assertEqual(terminal_host_attempt["signal_tags"], [])

    def test_requests_challenge_page_escalates_to_playwright_success_when_no_host_cooldown_is_created(self) -> None:
        fetcher = self.make_fetcher()
        direct = ProxySelection()
        first = make_execution(
            attempt_mode="requests",
            status="blocked",
            proxy_selection=direct,
            response=make_response(url="https://factory.example/", text=make_challenge_html(), status_code=200),
            notes=["challenge page"],
        )
        second = make_execution(
            attempt_mode="playwright",
            status="success",
            proxy_selection=direct,
            response=make_response(url="https://factory.example/", text=make_usable_html("recovered")),
            notes=["playwright success"],
        )
        fetcher._requests_fetch = mock.Mock(return_value=first)
        fetcher._playwright_fetch = mock.Mock(return_value=second)

        response, status, notes = fetcher.fetch("https://factory.example/", "hybrid", route_family="homepage")

        self.assertEqual(status, "success")
        self.assertIn("policy escalated requests path to playwright", notes)
        self.assertIsNotNone(response)
        self.assertEqual(fetcher.last_fetch_result.access_state, ACCESS_STATE_RECOVERED)
        self.assertTrue(fetcher.last_fetch_result.completed_with_content)
        self.assertEqual(len(fetcher.last_fetch_result.attempts), 2)
        self.assertEqual(fetcher.last_fetch_result.attempts[0].block_class, BLOCK_CLASS_SOFT_BLOCK)
        self.assertEqual(fetcher.last_fetch_result.attempts[0].anti_bot_reason, "challenge_page")
        self.assertEqual(fetcher.last_fetch_result.attempts[1].block_class, BLOCK_CLASS_SUCCESS)
        self.assertTrue(fetcher.last_fetch_result.attempts[1].playwright_used)
        self._assert_transport_contract(
            fetcher,
            transport_selected="requests",
            transport_final="playwright",
            escalation_reason="challenge_page",
            blocked_by_policy=False,
            fallback_blocked_by_policy=False,
        )

    def test_requests_bot_gate_escalation_second_attempt_respects_in_run_host_cooldown_before_playwright(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            progress_store = ProgressStore(Path(tmpdir))
            fetcher = self.make_fetcher(progress_store=progress_store)
            direct = ProxySelection()
            first = make_execution(
                attempt_mode="requests",
                status="bot_gate",
                proxy_selection=direct,
                response=make_response(url="https://factory.example/cooldown-browser", text="<html><body>bot gate</body></html>", status_code=200),
                notes=["bot gate"],
                cooldown_seconds=45,
            )
            fetcher._requests_fetch = mock.Mock(return_value=first)
            fetcher._playwright_fetch = mock.Mock(
                side_effect=AssertionError("playwright should not run when in-run host cooldown is active before second attempt")
            )

            response, status, notes = fetcher.fetch("https://factory.example/cooldown-browser", "hybrid", route_family="homepage")

            self.assertIsNone(response)
            self.assertEqual(status, "blocked_by_policy")
            self.assertIn("policy escalated requests path to playwright", notes)
            self.assertTrue(any("host cooldown active for" in note for note in notes))
            self.assertEqual(fetcher._requests_fetch.call_count, 1)
            fetcher._playwright_fetch.assert_not_called()
            self.assertEqual(fetcher.last_fetch_result.access_state, "blocked")
            self.assertEqual(len(fetcher.last_fetch_result.attempts), 1)
            self.assertEqual(fetcher.last_fetch_result.attempts[0].status, "bot_gate")
            self.assertFalse(fetcher.last_fetch_result.attempts[0].playwright_used)
            self.assertTrue(fetcher.last_fetch_result.attempts[0].blocked_by_policy)
            self._assert_transport_contract(
                fetcher,
                transport_selected="requests",
                transport_final="playwright",
                escalation_reason="bot_gate",
                blocked_by_policy=True,
            )
            self._assert_terminal_progress_surfaces(
                progress_store,
                raw_status="bot_gate",
                transport_final="playwright",
                escalation_reason="bot_gate",
                raw_anti_bot_reason="bot_gate",
                raw_block_class=BLOCK_CLASS_SOFT_BLOCK,
                raw_http_status=200,
                raw_cooldown_seconds=45,
                expected_first_signal_tags=["cooldown", "bot_gate"],
                raw_blocked_by_policy=False,
            )

    def test_requests_bot_gate_runtime_blocked_persisted_session_can_stop_followup_before_second_attempt(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            progress_store = ProgressStore(Path(tmpdir))
            event_ts = (datetime.now(timezone.utc) - timedelta(seconds=5)).isoformat().replace("+00:00", "Z")
            update_host_memory_from_event_payload(
                progress_store.host_memory,
                {
                    "ts": event_ts,
                    "event_type": "route_fetch_attempt",
                    "host": "factory.example",
                    "status": "bot_gate",
                    "proxy_label_or_id": "proxy-1",
                    "cooldown_seconds": 0,
                    "anti_bot_reason": "bot_gate",
                    "block_class": "SOFT_BLOCK",
                },
                ts=event_ts,
            )

            fetcher = self.make_fetcher(progress_store=progress_store)
            fetcher.session_store.load = mock.Mock(return_value=make_session_profile(proxy_label_or_id="proxy-1"))
            direct = ProxySelection()
            fetcher._select_proxy = mock.Mock(return_value=direct)
            fetcher._requests_fetch = mock.Mock(
                return_value=make_execution(
                    attempt_mode="requests",
                    status="bot_gate",
                    proxy_selection=direct,
                    response=make_response(url="https://factory.example/retry-browser", text="<html><body>bot gate</body></html>"),
                    notes=["bot gate"],
                )
            )
            fetcher._playwright_fetch = mock.Mock(
                return_value=make_execution(
                    attempt_mode="playwright",
                    status="success",
                    proxy_selection=direct,
                    response=make_response(url="https://factory.example/retry-browser", text=make_usable_html("recovered")),
                    notes=["playwright success"],
                    session_reused=False,
                )
            )

            response, status, notes = fetcher.fetch("https://factory.example/retry-browser", "hybrid", route_family="homepage")

            self.assertIsNotNone(response)
            self.assertEqual(status, "blocked_by_policy")
            self.assertIn("browser escalation denied by route policy for homepage", notes)
            self.assertEqual(fetcher.session_store.load.call_count, 1)
            self.assertIsNone(fetcher._requests_fetch.call_args.kwargs["session_profile"])
            fetcher._playwright_fetch.assert_not_called()
            self.assertEqual(fetcher._requests_fetch.call_count, 1)
            self.assertEqual(len(fetcher.last_fetch_result.attempts), 1)
            first_attempt = fetcher.last_fetch_result.attempts[0]
            self.assertEqual(first_attempt.status, "bot_gate")
            self.assertFalse(first_attempt.session_reused)
            self._assert_transport_contract(
                fetcher,
                transport_selected="requests",
                transport_final="requests",
                escalation_reason="bot_gate",
                blocked_by_policy=True,
            )
            self._assert_terminal_progress_surfaces(
                progress_store,
                raw_status="bot_gate",
                transport_final="requests",
                escalation_reason="bot_gate",
                raw_anti_bot_reason="bot_gate",
                raw_block_class=first_attempt.block_class,
                raw_http_status=200,
                raw_cooldown_seconds=first_attempt.cooldown_seconds,
                expected_first_signal_tags=(["cooldown"] if first_attempt.cooldown_seconds > 0 else []) + ["bot_gate"],
            )

    def test_request_error_gets_limited_retry_when_no_host_cooldown_is_created(self) -> None:
        fetcher = self.make_fetcher()
        direct = ProxySelection()
        first = make_execution(
            attempt_mode="requests",
            status="request_error",
            proxy_selection=direct,
            response=None,
            notes=["request error"],
        )
        second = make_execution(
            attempt_mode="requests",
            status="success",
            proxy_selection=direct,
            response=make_response(url="https://factory.example/retry", text=make_usable_html("limited retry")),
            notes=["retry success"],
        )
        fetcher._requests_fetch = mock.Mock(side_effect=[first, second])
        fetcher._sleep_backoff = mock.Mock()

        response, status, notes = fetcher.fetch("https://factory.example/retry", "requests", route_family="homepage")

        self.assertEqual(status, "success")
        self.assertIsNotNone(response)
        self.assertIn("limited retry after cooldown/backoff", notes)
        fetcher._sleep_backoff.assert_called_once_with(0)
        self.assertEqual(fetcher.last_fetch_result.access_state, ACCESS_STATE_RECOVERED)
        self.assertEqual(len(fetcher.last_fetch_result.attempts), 2)
        self.assertEqual(fetcher.last_fetch_result.attempts[0].block_class, BLOCK_CLASS_SOFT_BLOCK)
        self.assertEqual(fetcher.last_fetch_result.attempts[1].block_class, BLOCK_CLASS_SUCCESS)
        self._assert_transport_contract(
            fetcher,
            transport_selected="requests",
            transport_final="requests",
            escalation_reason="request_error",
            blocked_by_policy=False,
            fallback_blocked_by_policy=False,
        )

    def test_rate_limit_limited_retry_second_attempt_respects_in_run_host_cooldown_before_requests_retry(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            progress_store = ProgressStore(Path(tmpdir))
            fetcher = self.make_fetcher(progress_store=progress_store)
            direct = ProxySelection()
            first = make_execution(
                attempt_mode="requests",
                status="rate_limited",
                proxy_selection=direct,
                response=make_response(url="https://factory.example/cooldown-retry", text="<html><body>slow down</body></html>", status_code=429),
                notes=["429"],
                cooldown_seconds=7,
            )
            fetcher._requests_fetch = mock.Mock(return_value=first)
            fetcher._sleep_backoff = mock.Mock()

            response, status, notes = fetcher.fetch("https://factory.example/cooldown-retry", "requests", route_family="homepage")

            self.assertIsNone(response)
            self.assertEqual(status, "blocked_by_policy")
            self.assertIn("limited retry after cooldown/backoff", notes)
            self.assertTrue(any("host cooldown active for" in note for note in notes))
            fetcher._sleep_backoff.assert_not_called()
            self.assertEqual(fetcher._requests_fetch.call_count, 1)
            self.assertEqual(fetcher.last_fetch_result.access_state, "blocked")
            self.assertEqual(len(fetcher.last_fetch_result.attempts), 1)
            self.assertEqual(fetcher.last_fetch_result.attempts[0].status, "rate_limited")
            self.assertFalse(fetcher.last_fetch_result.attempts[0].playwright_used)
            self.assertTrue(fetcher.last_fetch_result.attempts[0].blocked_by_policy)
            self._assert_transport_contract(
                fetcher,
                transport_selected="requests",
                transport_final="requests",
                escalation_reason="http_429",
                blocked_by_policy=True,
            )
            self._assert_terminal_progress_surfaces(
                progress_store,
                raw_status="rate_limited",
                transport_final="requests",
                escalation_reason="http_429",
                raw_anti_bot_reason="http_429",
                raw_block_class=BLOCK_CLASS_RATE_LIMIT,
                raw_http_status=429,
                raw_cooldown_seconds=30,
                expected_first_signal_tags=["cooldown", "http_429"],
                raw_blocked_by_policy=False,
            )

    def test_rate_limit_limited_retry_second_attempt_respects_in_run_cooldown_with_runtime_blocked_persisted_session(self) -> None:
        host_memory: dict[str, object] = {}
        event_ts = (datetime.now(timezone.utc) - timedelta(seconds=5)).isoformat().replace("+00:00", "Z")
        update_host_memory_from_event_payload(
            host_memory,
            {
                "ts": event_ts,
                "event_type": "route_fetch_attempt",
                "host": "factory.example",
                "status": "challenge",
                "proxy_label_or_id": "proxy-1",
                "cooldown_seconds": 0,
                "anti_bot_reason": "challenge",
                "block_class": "SOFT_BLOCK",
            },
            ts=event_ts,
        )

        fetcher = Fetcher(FakeClient(progress_store=SimpleNamespace(host_memory=host_memory)))
        fetcher.playwright_enabled = True
        fetcher.max_attempts = 2
        fetcher.session_store.load = mock.Mock(return_value=make_session_profile(proxy_label_or_id="proxy-1"))
        direct = ProxySelection()
        fetcher._select_proxy = mock.Mock(return_value=direct)
        first = make_execution(
            attempt_mode="requests",
            status="rate_limited",
            proxy_selection=direct,
            response=make_response(url="https://factory.example/retry-requests", text="<html><body>slow down</body></html>", status_code=429),
            notes=["429"],
            cooldown_seconds=7,
        )
        second = make_execution(
            attempt_mode="requests",
            status="success",
            proxy_selection=direct,
            response=make_response(url="https://factory.example/retry-requests", text=make_usable_html("limited retry")),
            notes=["retry success"],
            session_reused=False,
        )
        fetcher._requests_fetch = mock.Mock(side_effect=[first, second])
        fetcher._sleep_backoff = mock.Mock()

        response, status, notes = fetcher.fetch("https://factory.example/retry-requests", "requests", route_family="homepage")

        self.assertIsNone(response)
        self.assertEqual(status, "blocked_by_policy")
        self.assertIn("limited retry after cooldown/backoff", notes)
        self.assertTrue(any("host cooldown active for" in note for note in notes))
        fetcher._sleep_backoff.assert_not_called()
        self.assertEqual(fetcher.session_store.load.call_count, 1)
        self.assertEqual(len(fetcher._requests_fetch.call_args_list), 1)
        self.assertIsNone(fetcher._requests_fetch.call_args.kwargs["session_profile"])
        self.assertEqual(len(fetcher.last_fetch_result.attempts), 1)
        self.assertEqual(fetcher.last_fetch_result.attempts[0].status, "rate_limited")
        self.assertFalse(fetcher.last_fetch_result.attempts[0].session_reused)
        self.assertTrue(fetcher.last_fetch_result.attempts[0].blocked_by_policy)

    def test_policy_denied_browser_escalation_is_traced_without_playwright_retry(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            progress_store = ProgressStore(Path(tmpdir))
            fetcher = self.make_fetcher(progress_store=progress_store)
            fetcher.playwright_enabled = False
            direct = ProxySelection()
            fetcher._requests_fetch = mock.Mock(
                return_value=make_execution(
                    attempt_mode="requests",
                    status="bot_gate",
                    proxy_selection=direct,
                    response=make_response(url="https://factory.example/policy-denied", text="<html><body>bot gate</body></html>"),
                    notes=["bot gate"],
                )
            )
            fetcher._playwright_fetch = mock.Mock(side_effect=AssertionError("playwright should stay disabled by policy"))

            response, status, notes = fetcher.fetch("https://factory.example/policy-denied", "hybrid", route_family="homepage")

            self.assertIsNotNone(response)
            self.assertEqual(status, "blocked_by_policy")
            self.assertIn("bot gate", notes)
            self.assertIn("Playwright is disabled", notes[-1])
            self.assertEqual(fetcher.last_fetch_result.access_state, "blocked")
            self.assertEqual(len(fetcher.last_fetch_result.attempts), 1)
            first_attempt = fetcher.last_fetch_result.attempts[0]
            self.assertEqual(first_attempt.anti_bot_reason, "bot_gate")
            self.assertEqual(fetcher._requests_fetch.call_count, 1)
            fetcher._playwright_fetch.assert_not_called()
            self._assert_transport_contract(
                fetcher,
                transport_selected="requests",
                transport_final="requests",
                escalation_reason="bot_gate",
                blocked_by_policy=True,
                fallback_blocked_by_policy=True,
            )
            self._assert_terminal_progress_surfaces(
                progress_store,
                raw_status="bot_gate",
                transport_final="requests",
                escalation_reason="bot_gate",
                raw_anti_bot_reason="bot_gate",
                raw_block_class=first_attempt.block_class,
                raw_http_status=200,
                raw_cooldown_seconds=first_attempt.cooldown_seconds,
                expected_first_signal_tags=(["cooldown"] if first_attempt.cooldown_seconds > 0 else []) + ["bot_gate"],
                raw_blocked_by_policy=False,
            )

    def test_browser_challenge_loop_requires_manual_handoff(self) -> None:
        fetcher = self.make_fetcher()
        direct = ProxySelection()
        fetcher._playwright_fetch = mock.Mock(
            return_value=make_execution(
                attempt_mode="playwright",
                status="success",
                proxy_selection=direct,
                response=make_response(url="https://factory.example/challenge", text=make_challenge_html()),
                notes=["challenge still present"],
                session_reused=True,
            )
        )

        response, status, notes = fetcher.fetch("https://factory.example/challenge", "playwright", route_family="homepage")

        self.assertIsNotNone(response)
        self.assertEqual(status, ACCESS_STATE_MANUAL_HANDOFF_REQUIRED)
        self.assertIn("challenge still present", notes)
        self.assertEqual(fetcher.last_fetch_result.access_state, ACCESS_STATE_MANUAL_HANDOFF_REQUIRED)
        self.assertEqual(fetcher.last_fetch_result.block_class, BLOCK_CLASS_CHALLENGE_LOOP)
        self.assertTrue(fetcher.last_fetch_result.manual_handoff_required)
        self.assertTrue(fetcher.last_fetch_result.challenge_detected)
        self.assertTrue(fetcher.last_fetch_result.session_reused)
        fetcher._playwright_fetch.assert_called_once()

    def test_playwright_manual_handoff_progress_surface_contract(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            progress_store = ProgressStore(Path(tmpdir))
            fetcher = self.make_fetcher(progress_store=progress_store)
            direct = ProxySelection()
            fetcher._playwright_fetch = mock.Mock(
                return_value=make_execution(
                    attempt_mode="playwright",
                    status="success",
                    proxy_selection=direct,
                    response=make_response(url="https://factory.example/challenge", text=make_challenge_html()),
                    notes=["challenge still present"],
                    session_reused=True,
                )
            )

            response, status, notes = fetcher.fetch(
                "https://factory.example/challenge",
                "playwright",
                route_family="homepage",
            )

            self.assertIsNotNone(response)
            self.assertEqual(status, ACCESS_STATE_MANUAL_HANDOFF_REQUIRED)
            self.assertIn("challenge still present", notes)
            self.assertEqual(fetcher.last_fetch_result.access_state, ACCESS_STATE_MANUAL_HANDOFF_REQUIRED)
            self.assertEqual(fetcher.last_fetch_result.block_class, BLOCK_CLASS_CHALLENGE_LOOP)
            self.assertTrue(fetcher.last_fetch_result.manual_handoff_required)
            self.assertTrue(fetcher.last_fetch_result.challenge_detected)
            self.assertTrue(fetcher.last_fetch_result.session_reused)
            self.assertEqual(len(fetcher.last_fetch_result.attempts), 1)
            first_attempt = fetcher.last_fetch_result.attempts[0]
            self.assertEqual(first_attempt.fetch_mode, "playwright")
            self.assertEqual(first_attempt.status, ACCESS_STATE_MANUAL_HANDOFF_REQUIRED)
            self.assertEqual(first_attempt.transport_selected, "playwright")
            self.assertEqual(first_attempt.transport_final, "playwright")
            self.assertEqual(first_attempt.escalation_reason, "challenge_page")
            self.assertTrue(first_attempt.manual_handoff_required)
            self.assertTrue(first_attempt.challenge_detected)
            fetcher._playwright_fetch.assert_called_once()
            self._assert_transport_contract(
                fetcher,
                transport_selected="playwright",
                transport_final="playwright",
                escalation_reason="challenge_page",
                blocked_by_policy=False,
                fallback_blocked_by_policy=False,
            )

            events = load_jsonl(progress_store.events_jsonl)
            self.assertEqual([event["type"] for event in events], ["route_fetch_playwright"])
            progress_event = events[0]
            self.assertEqual(progress_event["status"], ACCESS_STATE_MANUAL_HANDOFF_REQUIRED)
            self.assertIs(progress_event["manual_handoff_required"], True)
            self.assertIs(progress_event["challenge_detected"], True)
            self.assertIs(progress_event["blocked_by_policy"], False)
            self.assertEqual(progress_event["transport_selected"], "playwright")
            self.assertEqual(progress_event["transport_final"], "playwright")
            self.assertEqual(progress_event["escalation_reason"], "challenge_page")
            self.assertEqual(progress_event["attempt_no"], 1)
            self.assertEqual(progress_event["anti_bot_reason"], "challenge_page")
            self.assertEqual(progress_event["block_class"], BLOCK_CLASS_CHALLENGE_LOOP)
            self.assertEqual(progress_event["http_status"], 200)
            self.assertEqual(progress_event["fetch_mode"], "playwright")
            self.assertIs(progress_event["playwright_used"], True)
            self.assertIs(progress_event["playwright_fallback_used"], False)

            self.assertFalse(progress_store.results_jsonl.exists())

            stage_messages = load_stage_messages(progress_store.output_dir)
            self.assertEqual(
                [message["payload"]["event_type"] for message in stage_messages],
                ["route_fetch_playwright"],
            )
            self.assertEqual(
                stage_messages[0]["payload"]["status"],
                ACCESS_STATE_MANUAL_HANDOFF_REQUIRED,
            )

            host_recent_attempts = normalize_host_memory_state(progress_store.host_memory)["factory.example"][
                "recent_attempts"
            ]
            self.assertEqual(len(host_recent_attempts), 1)
            host_attempt = host_recent_attempts[0]
            self.assertEqual(host_attempt["event_type"], "route_fetch_playwright")
            self.assertEqual(host_attempt["status"], ACCESS_STATE_MANUAL_HANDOFF_REQUIRED)
            self.assertEqual(host_attempt["transport_selected"], "playwright")
            self.assertEqual(host_attempt["transport_final"], "playwright")
            self.assertEqual(host_attempt["anti_bot_reason"], "challenge_page")
            self.assertEqual(host_attempt["block_class"], BLOCK_CLASS_CHALLENGE_LOOP)
            self.assertEqual(host_attempt["http_status"], 200)
            self.assertEqual(host_attempt["cooldown_seconds"], float(first_attempt.cooldown_seconds))
            self.assertTrue(host_attempt["challenge_detected"])
            self.assertIs(host_attempt["blocked_by_policy"], False)
            self.assertEqual(host_attempt["signal_tags"], ["cooldown", "challenge"])
            self.assertNotIn("route_fetch_terminal", [attempt["event_type"] for attempt in host_recent_attempts])

    def test_open_breaker_pauses_route_before_retrying(self) -> None:
        fetcher = self.make_fetcher()
        fetcher.breakers.state_for_host("factory.example").mode = BREAKER_MODE_PAUSED

        response, status, notes = fetcher.fetch("https://factory.example/", "requests", route_family="homepage")

        self.assertIsNone(response)
        self.assertEqual(status, ACCESS_STATE_PAUSED_BY_BREAKER)
        self.assertIn("route paused by paused breaker", notes)
        self.assertEqual(fetcher.last_fetch_result.access_state, ACCESS_STATE_PAUSED_BY_BREAKER)
        self.assertEqual(fetcher.last_fetch_result.block_class, BLOCK_CLASS_HARD_BAN)
        self.assertEqual(fetcher.last_fetch_result.breaker_mode, BREAKER_MODE_PAUSED)
        self.assertEqual(len(fetcher.last_fetch_result.attempts), 1)

    def test_open_breaker_pause_progress_surface_contract(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            progress_store = ProgressStore(Path(tmpdir))
            initial_recent_attempts = list(
                normalize_host_memory_state(progress_store.host_memory)
                .get("factory.example", {})
                .get("recent_attempts", [])
            )
            fetcher = self.make_fetcher(progress_store=progress_store)
            fetcher.breakers.state_for_host("factory.example").mode = BREAKER_MODE_PAUSED
            fetcher._requests_fetch = mock.Mock(
                side_effect=AssertionError(
                    "requests should not run when the open breaker already paused the route before network"
                )
            )
            fetcher._playwright_fetch = mock.Mock(
                side_effect=AssertionError(
                    "playwright should not run when the open breaker already paused the route before network"
                )
            )

            response, status, notes = fetcher.fetch(
                "https://factory.example/",
                "requests",
                route_family="homepage",
            )

            self.assertIsNone(response)
            self.assertEqual(status, ACCESS_STATE_PAUSED_BY_BREAKER)
            self.assertIn("route paused by paused breaker", notes)
            self.assertEqual(fetcher.last_fetch_result.access_state, ACCESS_STATE_PAUSED_BY_BREAKER)
            self.assertEqual(fetcher.last_fetch_result.breaker_mode, BREAKER_MODE_PAUSED)
            self.assertEqual(fetcher.breakers.state_for_host("factory.example").mode, BREAKER_MODE_PAUSED)
            self.assertEqual(len(fetcher.last_fetch_result.attempts), 1)
            first_attempt = fetcher.last_fetch_result.attempts[0]
            self.assertEqual(first_attempt.fetch_mode, "requests")
            self.assertEqual(first_attempt.status, ACCESS_STATE_PAUSED_BY_BREAKER)
            self.assertTrue(first_attempt.blocked_by_policy)
            self.assertEqual(first_attempt.anti_bot_reason, "paused_by_breaker")
            self.assertEqual(first_attempt.block_class, BLOCK_CLASS_HARD_BAN)
            self.assertEqual(first_attempt.breaker_mode, BREAKER_MODE_PAUSED)
            self.assertEqual(first_attempt.transport_selected, "requests")
            self.assertEqual(first_attempt.transport_final, "requests")
            fetcher._requests_fetch.assert_not_called()
            fetcher._playwright_fetch.assert_not_called()
            self._assert_transport_contract(
                fetcher,
                transport_selected="requests",
                transport_final="requests",
                escalation_reason="paused_by_breaker",
                blocked_by_policy=True,
            )

            events = load_jsonl(progress_store.events_jsonl)
            self.assertEqual([event["type"] for event in events], ["route_fetch_breaker_pause"])
            breaker_event = events[0]
            self.assertEqual(breaker_event["status"], ACCESS_STATE_PAUSED_BY_BREAKER)
            self.assertIs(breaker_event["blocked_by_policy"], True)
            self.assertEqual(breaker_event["breaker_mode"], BREAKER_MODE_PAUSED)
            self.assertEqual(breaker_event["transport_selected"], "requests")
            self.assertEqual(breaker_event["transport_final"], "requests")
            self.assertEqual(breaker_event["anti_bot_reason"], "paused_by_breaker")
            self.assertEqual(breaker_event["block_class"], BLOCK_CLASS_HARD_BAN)
            self.assertFalse(progress_store.results_jsonl.exists())

            stage_messages = load_stage_messages(progress_store.output_dir)
            self.assertEqual(
                [message["payload"]["event_type"] for message in stage_messages],
                ["route_fetch_breaker_pause"],
            )
            self.assertEqual(stage_messages[0]["payload"]["status"], ACCESS_STATE_PAUSED_BY_BREAKER)
            self.assertEqual(stage_messages[0]["payload"]["breaker_mode"], BREAKER_MODE_PAUSED)

            host_recent_attempts = normalize_host_memory_state(progress_store.host_memory)["factory.example"][
                "recent_attempts"
            ]
            self.assertEqual(len(host_recent_attempts), len(initial_recent_attempts) + 1)
            self.assertEqual(host_recent_attempts[-1]["event_type"], "route_fetch_breaker_pause")
            self.assertNotIn("route_fetch_terminal", [attempt["event_type"] for attempt in host_recent_attempts[-1:]])
            breaker_host_attempt = host_recent_attempts[-1]
            self.assertEqual(breaker_host_attempt["status"], ACCESS_STATE_PAUSED_BY_BREAKER)
            self.assertIs(breaker_host_attempt["blocked_by_policy"], True)
            self.assertEqual(breaker_host_attempt["transport_selected"], "requests")
            self.assertEqual(breaker_host_attempt["transport_final"], "requests")
            self.assertEqual(breaker_host_attempt["anti_bot_reason"], "paused_by_breaker")
            self.assertEqual(breaker_host_attempt["block_class"], BLOCK_CLASS_HARD_BAN)
            self.assertEqual(breaker_host_attempt["signal_tags"], [])

    def test_open_breaker_playwright_pause_progress_surface_contract(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            progress_store = ProgressStore(Path(tmpdir))
            initial_recent_attempts = list(
                normalize_host_memory_state(progress_store.host_memory)
                .get("factory.example", {})
                .get("recent_attempts", [])
            )
            fetcher = self.make_fetcher(progress_store=progress_store)
            fetcher.breakers.state_for_host("factory.example").mode = BREAKER_MODE_PAUSED
            fetcher._requests_fetch = mock.Mock(
                side_effect=AssertionError(
                    "requests should not run when the paused breaker already short-circuited the playwright route"
                )
            )
            fetcher._playwright_fetch = mock.Mock(
                side_effect=AssertionError(
                    "playwright should not run when the paused breaker already short-circuited the playwright route"
                )
            )

            response, status, notes = fetcher.fetch(
                "https://factory.example/browser-first",
                "playwright",
                route_family="homepage",
            )

            self.assertIsNone(response)
            self.assertEqual(status, ACCESS_STATE_PAUSED_BY_BREAKER)
            self.assertIn("route paused by paused breaker", notes)
            self.assertEqual(fetcher.last_fetch_result.access_state, ACCESS_STATE_PAUSED_BY_BREAKER)
            self.assertEqual(fetcher.last_fetch_result.breaker_mode, BREAKER_MODE_PAUSED)
            self.assertEqual(fetcher.breakers.state_for_host("factory.example").mode, BREAKER_MODE_PAUSED)
            self.assertEqual(len(fetcher.last_fetch_result.attempts), 1)
            first_attempt = fetcher.last_fetch_result.attempts[0]
            self.assertEqual(first_attempt.fetch_mode, "playwright")
            self.assertEqual(first_attempt.status, ACCESS_STATE_PAUSED_BY_BREAKER)
            self.assertTrue(first_attempt.blocked_by_policy)
            self.assertEqual(first_attempt.anti_bot_reason, "paused_by_breaker")
            self.assertEqual(first_attempt.block_class, BLOCK_CLASS_HARD_BAN)
            self.assertEqual(first_attempt.breaker_mode, BREAKER_MODE_PAUSED)
            self.assertEqual(first_attempt.transport_selected, "playwright")
            self.assertEqual(first_attempt.transport_final, "playwright")
            fetcher._requests_fetch.assert_not_called()
            fetcher._playwright_fetch.assert_not_called()
            self._assert_transport_contract(
                fetcher,
                transport_selected="playwright",
                transport_final="playwright",
                escalation_reason="paused_by_breaker",
                blocked_by_policy=True,
            )

            events = load_jsonl(progress_store.events_jsonl)
            self.assertEqual([event["type"] for event in events], ["route_fetch_breaker_pause"])
            breaker_event = events[0]
            self.assertEqual(breaker_event["status"], ACCESS_STATE_PAUSED_BY_BREAKER)
            self.assertIs(breaker_event["blocked_by_policy"], True)
            self.assertEqual(breaker_event["breaker_mode"], BREAKER_MODE_PAUSED)
            self.assertEqual(breaker_event["transport_selected"], "playwright")
            self.assertEqual(breaker_event["transport_final"], "playwright")
            self.assertEqual(breaker_event["anti_bot_reason"], "paused_by_breaker")
            self.assertEqual(breaker_event["block_class"], BLOCK_CLASS_HARD_BAN)
            self.assertFalse(progress_store.results_jsonl.exists())

            stage_messages = load_stage_messages(progress_store.output_dir)
            self.assertEqual(
                [message["payload"]["event_type"] for message in stage_messages],
                ["route_fetch_breaker_pause"],
            )
            self.assertEqual(stage_messages[0]["payload"]["status"], ACCESS_STATE_PAUSED_BY_BREAKER)
            self.assertEqual(stage_messages[0]["payload"]["breaker_mode"], BREAKER_MODE_PAUSED)

            host_recent_attempts = normalize_host_memory_state(progress_store.host_memory)["factory.example"][
                "recent_attempts"
            ]
            self.assertEqual(len(host_recent_attempts), len(initial_recent_attempts) + 1)
            self.assertEqual(host_recent_attempts[-1]["event_type"], "route_fetch_breaker_pause")
            self.assertNotIn("route_fetch_terminal", [attempt["event_type"] for attempt in host_recent_attempts[-1:]])
            breaker_host_attempt = host_recent_attempts[-1]
            self.assertEqual(breaker_host_attempt["status"], ACCESS_STATE_PAUSED_BY_BREAKER)
            self.assertIs(breaker_host_attempt["blocked_by_policy"], True)
            self.assertEqual(breaker_host_attempt["transport_selected"], "playwright")
            self.assertEqual(breaker_host_attempt["transport_final"], "playwright")
            self.assertEqual(breaker_host_attempt["anti_bot_reason"], "paused_by_breaker")
            self.assertEqual(breaker_host_attempt["block_class"], BLOCK_CLASS_HARD_BAN)
            self.assertEqual(breaker_host_attempt["signal_tags"], [])

    def test_open_breaker_hybrid_pause_progress_surface_contract(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            progress_store = ProgressStore(Path(tmpdir))
            initial_recent_attempts = list(
                normalize_host_memory_state(progress_store.host_memory)
                .get("factory.example", {})
                .get("recent_attempts", [])
            )
            fetcher = self.make_fetcher(progress_store=progress_store)
            fetcher.breakers.state_for_host("factory.example").mode = BREAKER_MODE_PAUSED
            fetcher._requests_fetch = mock.Mock(
                side_effect=AssertionError(
                    "requests should not run when the paused breaker already short-circuited the hybrid route"
                )
            )
            fetcher._playwright_fetch = mock.Mock(
                side_effect=AssertionError(
                    "playwright should not run when the paused breaker already short-circuited the hybrid route"
                )
            )

            response, status, notes = fetcher.fetch(
                "https://factory.example/browser-first",
                "hybrid",
                route_family="homepage",
            )

            self.assertIsNone(response)
            self.assertEqual(status, ACCESS_STATE_PAUSED_BY_BREAKER)
            self.assertIn("route paused by paused breaker", notes)
            self.assertEqual(fetcher.last_fetch_result.access_state, ACCESS_STATE_PAUSED_BY_BREAKER)
            self.assertEqual(fetcher.last_fetch_result.breaker_mode, BREAKER_MODE_PAUSED)
            self.assertEqual(fetcher.breakers.state_for_host("factory.example").mode, BREAKER_MODE_PAUSED)
            self.assertEqual(len(fetcher.last_fetch_result.attempts), 1)
            first_attempt = fetcher.last_fetch_result.attempts[0]
            self.assertEqual(first_attempt.fetch_mode, "hybrid")
            self.assertEqual(first_attempt.status, ACCESS_STATE_PAUSED_BY_BREAKER)
            self.assertTrue(first_attempt.blocked_by_policy)
            self.assertEqual(first_attempt.anti_bot_reason, "paused_by_breaker")
            self.assertEqual(first_attempt.block_class, BLOCK_CLASS_HARD_BAN)
            self.assertEqual(first_attempt.breaker_mode, BREAKER_MODE_PAUSED)
            self.assertEqual(first_attempt.transport_selected, "hybrid")
            self.assertEqual(first_attempt.transport_final, "hybrid")
            fetcher._requests_fetch.assert_not_called()
            fetcher._playwright_fetch.assert_not_called()
            self._assert_transport_contract(
                fetcher,
                transport_selected="hybrid",
                transport_final="hybrid",
                escalation_reason="paused_by_breaker",
                blocked_by_policy=True,
            )

            events = load_jsonl(progress_store.events_jsonl)
            self.assertEqual([event["type"] for event in events], ["route_fetch_breaker_pause"])
            breaker_event = events[0]
            self.assertEqual(breaker_event["status"], ACCESS_STATE_PAUSED_BY_BREAKER)
            self.assertIs(breaker_event["blocked_by_policy"], True)
            self.assertEqual(breaker_event["breaker_mode"], BREAKER_MODE_PAUSED)
            self.assertEqual(breaker_event["transport_selected"], "hybrid")
            self.assertEqual(breaker_event["transport_final"], "hybrid")
            self.assertEqual(breaker_event["anti_bot_reason"], "paused_by_breaker")
            self.assertEqual(breaker_event["block_class"], BLOCK_CLASS_HARD_BAN)
            self.assertFalse(progress_store.results_jsonl.exists())

            stage_messages = load_stage_messages(progress_store.output_dir)
            self.assertEqual(
                [message["payload"]["event_type"] for message in stage_messages],
                ["route_fetch_breaker_pause"],
            )
            self.assertEqual(stage_messages[0]["payload"]["status"], ACCESS_STATE_PAUSED_BY_BREAKER)

            host_recent_attempts = normalize_host_memory_state(progress_store.host_memory)["factory.example"][
                "recent_attempts"
            ]
            self.assertEqual(len(host_recent_attempts), len(initial_recent_attempts) + 1)
            self.assertEqual(host_recent_attempts[-1]["event_type"], "route_fetch_breaker_pause")
            self.assertNotIn("route_fetch_terminal", [attempt["event_type"] for attempt in host_recent_attempts[-1:]])
            breaker_host_attempt = host_recent_attempts[-1]
            self.assertEqual(breaker_host_attempt["status"], ACCESS_STATE_PAUSED_BY_BREAKER)
            self.assertIs(breaker_host_attempt["blocked_by_policy"], True)
            self.assertEqual(breaker_host_attempt["transport_selected"], "hybrid")
            self.assertEqual(breaker_host_attempt["transport_final"], "hybrid")
            self.assertEqual(breaker_host_attempt["anti_bot_reason"], "paused_by_breaker")
            self.assertEqual(breaker_host_attempt["block_class"], BLOCK_CLASS_HARD_BAN)
            self.assertEqual(breaker_host_attempt["signal_tags"], [])

    def test_repeat_soft_block_does_not_reuse_same_proxy_blindly(self) -> None:
        fetcher = self.make_fetcher()
        blocked_proxy = ProxySelection(
            url="http://proxy1.example:8080",
            source="test",
            proxy_id="proxy-1",
            label="proxy-1",
            host="proxy1.example",
            port="8080",
            via_proxy=True,
        )
        fetcher._select_proxy = mock.Mock(return_value=blocked_proxy)
        fetcher._requests_fetch = mock.Mock(
            return_value=make_execution(
                attempt_mode="requests",
                status="bot_gate",
                proxy_selection=blocked_proxy,
                response=make_response(url="https://factory.example/proxy", text="<html><body>bot gate</body></html>"),
                notes=["bot gate on proxy"],
            )
        )
        fetcher._playwright_fetch = mock.Mock(side_effect=AssertionError("playwright should not run when only blocked proxy is available"))

        response, status, notes = fetcher.fetch("https://factory.example/proxy", "hybrid", route_family="homepage")

        self.assertIsNone(response)
        self.assertEqual(status, "blocked_by_policy")
        self.assertIn("recovery stopped: same proxy would be reused after prior block", notes)
        self.assertEqual(fetcher.last_fetch_result.access_state, "blocked")
        self.assertEqual(len(fetcher.last_fetch_result.attempts), 2)
        self.assertEqual(fetcher.last_fetch_result.attempts[0].proxy_label_or_id, "proxy-1")
        self.assertEqual(fetcher.last_fetch_result.attempts[1].proxy_label_or_id, "proxy-1")
        self.assertEqual(fetcher.last_fetch_result.attempts[1].anti_bot_reason, "blocked_no_alternative_proxy")
        self.assertEqual(fetcher.last_fetch_result.attempts[1].block_class, BLOCK_CLASS_SOFT_BLOCK)
        self.assertFalse(fetcher.last_fetch_result.attempts[1].playwright_used)
        fetcher._playwright_fetch.assert_not_called()

    def test_resume_preflight_proxy_reuse_policy_denied_appends_terminal_progress_tail(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            progress_store = ProgressStore(Path(tmpdir))
            event_ts = (datetime.now(timezone.utc) - timedelta(seconds=5)).isoformat().replace("+00:00", "Z")
            update_host_memory_from_event_payload(
                progress_store.host_memory,
                {
                    "ts": event_ts,
                    "event_type": "route_fetch_attempt",
                    "host": "factory.example",
                    "status": "bot_gate",
                    "proxy_label_or_id": "proxy-1",
                    "cooldown_seconds": 0,
                    "anti_bot_reason": "bot_gate",
                    "block_class": "SOFT_BLOCK",
                },
                ts=event_ts,
            )
            fetcher = self.make_fetcher(progress_store=progress_store)
            blocked_proxy = ProxySelection(
                url="http://proxy1.example:8080",
                source="test",
                proxy_id="proxy-1",
                label="proxy-1",
                host="proxy1.example",
                port="8080",
                via_proxy=True,
            )
            fetcher._select_proxy = mock.Mock(return_value=blocked_proxy)
            fetcher._requests_fetch = mock.Mock(
                side_effect=AssertionError("requests should not run when runtime preflight only sees the blocked proxy")
            )
            fetcher._playwright_fetch = mock.Mock(
                side_effect=AssertionError("playwright should not run when runtime preflight only sees the blocked proxy")
            )

            response, status, notes = fetcher.fetch("https://factory.example/proxy-denied", "requests", route_family="homepage")

            self.assertIsNone(response)
            self.assertEqual(status, "blocked_by_policy")
            self.assertIn("recovery stopped: same proxy would be reused after prior block", notes)
            self.assertIn("browser escalation denied because the same blocked proxy would be reused", notes)
            self.assertEqual(fetcher._requests_fetch.call_count, 0)
            fetcher._playwright_fetch.assert_not_called()
            self.assertEqual(len(fetcher.last_fetch_result.attempts), 1)
            first_attempt = fetcher.last_fetch_result.attempts[0]
            self.assertEqual(first_attempt.status, "blocked_no_alternative_proxy")
            self.assertEqual(first_attempt.anti_bot_reason, "blocked_no_alternative_proxy")
            self.assertEqual(first_attempt.proxy_label_or_id, "proxy-1")
            self._assert_transport_contract(
                fetcher,
                transport_selected="requests",
                transport_final="requests",
                escalation_reason="proxy_reuse_policy_denied",
                blocked_by_policy=True,
            )
            self._assert_terminal_progress_surfaces(
                progress_store,
                raw_status="blocked_no_alternative_proxy",
                transport_final="requests",
                escalation_reason="proxy_reuse_policy_denied",
                raw_anti_bot_reason="blocked_no_alternative_proxy",
                raw_block_class=first_attempt.block_class,
                raw_http_status=None,
                raw_cooldown_seconds=first_attempt.cooldown_seconds,
                expected_first_signal_tags=["cooldown"] if first_attempt.cooldown_seconds > 0 else [],
                raw_blocked_by_policy=True,
            )

    def test_resume_preflight_avoids_same_proxy_from_runtime_host_memory(self) -> None:
        host_memory: dict[str, object] = {}
        event_ts = (datetime.now(timezone.utc) - timedelta(seconds=5)).isoformat().replace("+00:00", "Z")
        update_host_memory_from_event_payload(
            host_memory,
            {
                "ts": event_ts,
                "event_type": "route_fetch_attempt",
                "host": "factory.example",
                "status": "bot_gate",
                "proxy_label_or_id": "proxy-1",
                "cooldown_seconds": 0,
                "anti_bot_reason": "bot_gate",
                "block_class": "SOFT_BLOCK",
            },
            ts=event_ts,
        )
        fetcher = Fetcher(FakeClient(progress_store=SimpleNamespace(host_memory=host_memory)))
        fetcher.playwright_enabled = True
        fetcher.max_attempts = 2
        fetcher.session_store.load = lambda host: None

        blocked_proxy = ProxySelection(
            url="http://proxy1.example:8080",
            source="test",
            proxy_id="proxy-1",
            label="proxy-1",
            host="proxy1.example",
            port="8080",
            via_proxy=True,
        )
        fetcher._select_proxy = mock.Mock(return_value=blocked_proxy)
        fetcher._playwright_fetch = mock.Mock(side_effect=AssertionError("playwright should not run when runtime preflight only sees the blocked proxy"))

        response, status, notes = fetcher.fetch("https://factory.example/proxy", "playwright", route_family="homepage")

        self.assertIsNone(response)
        self.assertEqual(status, "blocked_by_policy")
        self.assertTrue(any("recovery stopped: same proxy would be reused after prior block" in note for note in notes))
        self.assertEqual(len(fetcher.last_fetch_result.attempts), 1)
        self.assertEqual(fetcher.last_fetch_result.attempts[0].proxy_label_or_id, "proxy-1")
        self.assertEqual(fetcher.last_fetch_result.attempts[0].anti_bot_reason, "blocked_no_alternative_proxy")
        self.assertFalse(fetcher.last_fetch_result.attempts[0].playwright_used)
        fetcher._playwright_fetch.assert_not_called()

    def test_resume_preflight_rotates_to_safe_alternative_proxy_from_runtime_host_memory(self) -> None:
        host_memory: dict[str, object] = {}
        event_ts = (datetime.now(timezone.utc) - timedelta(seconds=5)).isoformat().replace("+00:00", "Z")
        update_host_memory_from_event_payload(
            host_memory,
            {
                "ts": event_ts,
                "event_type": "route_fetch_attempt",
                "host": "factory.example",
                "status": "bot_gate",
                "proxy_label_or_id": "proxy1.example:8080",
                "cooldown_seconds": 0,
                "anti_bot_reason": "bot_gate",
                "block_class": "SOFT_BLOCK",
            },
            ts=event_ts,
        )
        proxy_pool = ProxyPool(
            "http://proxy1.example:8080,http://proxy2.example:8080",
            strategy="sticky_by_host",
            sticky_ttl_seconds=900.0,
        )
        fetcher = Fetcher(
            FakeClient(progress_store=SimpleNamespace(host_memory=host_memory)),
            proxy_pool=proxy_pool,
        )
        fetcher.max_attempts = 1
        fetcher.session_store.load = lambda host: None

        def requests_success(*args: object, **kwargs: object) -> _AttemptExecution:
            proxy_selection = kwargs["proxy_selection"]
            assert isinstance(proxy_selection, ProxySelection)
            return make_execution(
                attempt_mode="requests",
                status="success",
                proxy_selection=proxy_selection,
                response=make_response(
                    url="https://factory.example/proxy",
                    text=make_usable_html("rotated"),
                ),
                notes=["requests success"],
            )

        fetcher._requests_fetch = mock.Mock(side_effect=requests_success)
        fetcher._playwright_fetch = mock.Mock(side_effect=AssertionError("playwright should not run for a successful requests attempt"))

        response, status, notes = fetcher.fetch("https://factory.example/proxy", "requests", route_family="homepage")

        self.assertEqual(status, "success")
        self.assertIsNotNone(response)
        self.assertIn("requests success", notes)
        self.assertEqual(fetcher._requests_fetch.call_count, 1)
        self.assertEqual(fetcher._requests_fetch.call_args.kwargs["proxy_selection"].proxy_label_or_id, "proxy2.example:8080")
        self.assertEqual(fetcher.last_fetch_result.attempts[0].proxy_label_or_id, "proxy2.example:8080")
        fetcher._playwright_fetch.assert_not_called()

    def test_resume_preflight_reuses_safe_alternative_proxy_for_repeated_same_host_fetches(self) -> None:
        host_memory: dict[str, object] = {}
        event_ts = (datetime.now(timezone.utc) - timedelta(seconds=5)).isoformat().replace("+00:00", "Z")
        update_host_memory_from_event_payload(
            host_memory,
            {
                "ts": event_ts,
                "event_type": "route_fetch_attempt",
                "host": "factory.example",
                "status": "bot_gate",
                "proxy_label_or_id": "proxy1.example:8080",
                "cooldown_seconds": 0,
                "anti_bot_reason": "bot_gate",
                "block_class": "SOFT_BLOCK",
            },
            ts=event_ts,
        )
        proxy_pool = ProxyPool(
            "http://proxy1.example:8080,http://proxy2.example:8080",
            strategy="sticky_by_host",
            sticky_ttl_seconds=900.0,
        )
        fetcher = Fetcher(
            FakeClient(progress_store=SimpleNamespace(host_memory=host_memory)),
            proxy_pool=proxy_pool,
        )
        fetcher.max_attempts = 1
        fetcher.session_store.load = lambda host: None
        seen_proxy_labels: list[str] = []

        def requests_success(*args: object, **kwargs: object) -> _AttemptExecution:
            proxy_selection = kwargs["proxy_selection"]
            assert isinstance(proxy_selection, ProxySelection)
            seen_proxy_labels.append(proxy_selection.proxy_label_or_id)
            return make_execution(
                attempt_mode="requests",
                status="success",
                proxy_selection=proxy_selection,
                response=make_response(
                    url="https://factory.example/proxy",
                    text=make_usable_html(f"rotated-{len(seen_proxy_labels)}"),
                ),
                notes=["requests success"],
            )

        fetcher._requests_fetch = mock.Mock(side_effect=requests_success)
        fetcher._playwright_fetch = mock.Mock(side_effect=AssertionError("playwright should not run for a successful requests attempt"))

        first_response, first_status, first_notes = fetcher.fetch("https://factory.example/proxy", "requests", route_family="homepage")
        second_response, second_status, second_notes = fetcher.fetch("https://factory.example/proxy", "requests", route_family="homepage")

        self.assertEqual(first_status, "success")
        self.assertEqual(second_status, "success")
        self.assertIsNotNone(first_response)
        self.assertIsNotNone(second_response)
        self.assertIn("requests success", first_notes)
        self.assertIn("requests success", second_notes)
        self.assertEqual(seen_proxy_labels, ["proxy2.example:8080", "proxy2.example:8080"])
        self.assertEqual(fetcher._requests_fetch.call_count, 2)
        self.assertEqual(fetcher._requests_fetch.call_args_list[0].kwargs["proxy_selection"].proxy_label_or_id, "proxy2.example:8080")
        self.assertEqual(fetcher._requests_fetch.call_args_list[1].kwargs["proxy_selection"].proxy_label_or_id, "proxy2.example:8080")
        self.assertEqual(fetcher.last_fetch_result.attempts[0].proxy_label_or_id, "proxy2.example:8080")
        fetcher._playwright_fetch.assert_not_called()

    def test_resume_preflight_clears_safe_alternative_proxy_hint_when_runtime_debt_disappears(self) -> None:
        host_memory: dict[str, object] = {}
        event_ts = (datetime.now(timezone.utc) - timedelta(seconds=5)).isoformat().replace("+00:00", "Z")
        update_host_memory_from_event_payload(
            host_memory,
            {
                "ts": event_ts,
                "event_type": "route_fetch_attempt",
                "host": "factory.example",
                "status": "bot_gate",
                "proxy_label_or_id": "proxy1.example:8080",
                "cooldown_seconds": 0,
                "anti_bot_reason": "bot_gate",
                "block_class": "SOFT_BLOCK",
            },
            ts=event_ts,
        )
        progress_store = SimpleNamespace(host_memory=host_memory)
        proxy_pool = ProxyPool(
            "http://proxy1.example:8080,http://proxy2.example:8080",
            strategy="sticky_by_host",
            sticky_ttl_seconds=900.0,
        )
        fetcher = Fetcher(FakeClient(progress_store=progress_store), proxy_pool=proxy_pool)
        fetcher.max_attempts = 1
        fetcher.session_store.load = lambda host: None
        seen_proxy_labels: list[str] = []

        def requests_success(*args: object, **kwargs: object) -> _AttemptExecution:
            proxy_selection = kwargs["proxy_selection"]
            assert isinstance(proxy_selection, ProxySelection)
            seen_proxy_labels.append(proxy_selection.proxy_label_or_id)
            return make_execution(
                attempt_mode="requests",
                status="success",
                proxy_selection=proxy_selection,
                response=make_response(
                    url="https://factory.example/proxy",
                    text=make_usable_html(f"fetch-{len(seen_proxy_labels)}"),
                ),
                notes=["requests success"],
            )

        fetcher._requests_fetch = mock.Mock(side_effect=requests_success)
        fetcher._playwright_fetch = mock.Mock(side_effect=AssertionError("playwright should not run for a successful requests attempt"))

        first_response, first_status, first_notes = fetcher.fetch("https://factory.example/proxy", "requests", route_family="homepage")
        host_memory.clear()
        second_response, second_status, second_notes = fetcher.fetch("https://factory.example/proxy", "requests", route_family="homepage")

        self.assertEqual(first_status, "success")
        self.assertEqual(second_status, "success")
        self.assertIsNotNone(first_response)
        self.assertIsNotNone(second_response)
        self.assertIn("requests success", first_notes)
        self.assertIn("requests success", second_notes)
        self.assertEqual(seen_proxy_labels, ["proxy2.example:8080", "proxy1.example:8080"])
        self.assertEqual(fetcher._requests_fetch.call_count, 2)
        self.assertEqual(fetcher._requests_fetch.call_args_list[0].kwargs["proxy_selection"].proxy_label_or_id, "proxy2.example:8080")
        self.assertEqual(fetcher._requests_fetch.call_args_list[1].kwargs["proxy_selection"].proxy_label_or_id, "proxy1.example:8080")
        self.assertEqual(fetcher.last_fetch_result.attempts[0].proxy_label_or_id, "proxy1.example:8080")
        fetcher._playwright_fetch.assert_not_called()

    def test_resume_preflight_blocks_playwright_first_when_runtime_host_memory_cooldown_is_active(self) -> None:
        host_memory: dict[str, object] = {}
        event_ts = (datetime.now(timezone.utc) - timedelta(seconds=5)).isoformat().replace("+00:00", "Z")
        update_host_memory_from_event_payload(
            host_memory,
            {
                "ts": event_ts,
                "event_type": "route_fetch_attempt",
                "host": "factory.example",
                "status": "http_429",
                "proxy_label_or_id": "",
                "cooldown_seconds": 45,
                "anti_bot_reason": "http_429",
                "block_class": "RATE_LIMIT",
                "http_status": 429,
            },
            ts=event_ts,
        )
        fetcher = Fetcher(FakeClient(progress_store=SimpleNamespace(host_memory=host_memory)))
        fetcher.playwright_enabled = True
        fetcher.max_attempts = 2
        fetcher.session_store.load = lambda host: None
        fetcher._select_proxy = mock.Mock(return_value=ProxySelection())
        fetcher._playwright_fetch = mock.Mock(side_effect=AssertionError("playwright should not run while persisted cooldown is active"))

        response, status, notes = fetcher.fetch("https://factory.example/browser-first", "playwright", route_family="homepage")

        self.assertIsNone(response)
        self.assertEqual(status, "blocked_by_policy")
        self.assertTrue(any("host cooldown active for" in note for note in notes))
        self.assertEqual(fetcher.last_fetch_result.access_state, "blocked")
        self.assertEqual(len(fetcher.last_fetch_result.attempts), 1)
        self.assertEqual(fetcher.last_fetch_result.attempts[0].status, "cooldown_active")
        self.assertEqual(fetcher.last_fetch_result.attempts[0].proxy_label_or_id, "")
        self.assertFalse(fetcher.last_fetch_result.attempts[0].playwright_used)
        fetcher._playwright_fetch.assert_not_called()

    def test_resume_preflight_playwright_first_cooldown_appends_terminal_progress_tail(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            progress_store = ProgressStore(Path(tmpdir))
            event_ts = (datetime.now(timezone.utc) - timedelta(seconds=5)).isoformat().replace("+00:00", "Z")
            update_host_memory_from_event_payload(
                progress_store.host_memory,
                {
                    "ts": event_ts,
                    "event_type": "route_fetch_attempt",
                    "host": "factory.example",
                    "status": "http_429",
                    "proxy_label_or_id": "",
                    "cooldown_seconds": 45,
                    "anti_bot_reason": "http_429",
                    "block_class": "RATE_LIMIT",
                    "http_status": 429,
                },
                ts=event_ts,
            )
            fetcher = self.make_fetcher(progress_store=progress_store)
            fetcher._select_proxy = mock.Mock(return_value=ProxySelection())
            fetcher._playwright_fetch = mock.Mock(
                side_effect=AssertionError("playwright should not run while persisted cooldown is active")
            )

            response, status, notes = fetcher.fetch(
                "https://factory.example/browser-first",
                "playwright",
                route_family="homepage",
            )

            self.assertIsNone(response)
            self.assertEqual(status, "blocked_by_policy")
            self.assertTrue(any("host cooldown active for" in note for note in notes))
            self.assertEqual(fetcher.last_fetch_result.access_state, "blocked")
            self.assertEqual(len(fetcher.last_fetch_result.attempts), 1)
            first_attempt = fetcher.last_fetch_result.attempts[0]
            self.assertEqual(first_attempt.status, "cooldown_active")
            self.assertEqual(first_attempt.proxy_label_or_id, "")
            self.assertFalse(first_attempt.playwright_used)
            fetcher._playwright_fetch.assert_not_called()
            self._assert_transport_contract(
                fetcher,
                transport_selected="playwright",
                transport_final="playwright",
                escalation_reason="cooldown_active",
                blocked_by_policy=True,
            )
            self._assert_terminal_progress_surfaces(
                progress_store,
                raw_status="cooldown_active",
                transport_final="playwright",
                escalation_reason="cooldown_active",
                raw_anti_bot_reason="http_429",
                raw_block_class=first_attempt.block_class,
                raw_http_status=None,
                raw_cooldown_seconds=first_attempt.cooldown_seconds,
                expected_first_signal_tags=["cooldown", "http_429"],
                raw_blocked_by_policy=True,
                expected_event_types=("route_fetch_playwright", "route_fetch_terminal"),
                expected_first_fetch_mode="playwright",
                expected_first_transport_selected="playwright",
                expected_first_transport_final="playwright",
                expected_terminal_fetch_mode="playwright",
                expected_terminal_transport_selected="playwright",
            )

    def test_resume_preflight_playwright_first_proxy_reuse_policy_denied_appends_terminal_progress_tail(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            progress_store = ProgressStore(Path(tmpdir))
            event_ts = (datetime.now(timezone.utc) - timedelta(seconds=5)).isoformat().replace("+00:00", "Z")
            update_host_memory_from_event_payload(
                progress_store.host_memory,
                {
                    "ts": event_ts,
                    "event_type": "route_fetch_attempt",
                    "host": "factory.example",
                    "status": "bot_gate",
                    "proxy_label_or_id": "proxy-1",
                    "cooldown_seconds": 0,
                    "anti_bot_reason": "bot_gate",
                    "block_class": "SOFT_BLOCK",
                },
                ts=event_ts,
            )
            fetcher = self.make_fetcher(progress_store=progress_store)
            blocked_proxy = ProxySelection(
                url="http://proxy1.example:8080",
                source="test",
                proxy_id="proxy-1",
                label="proxy-1",
                host="proxy1.example",
                port="8080",
                via_proxy=True,
            )
            fetcher._select_proxy = mock.Mock(return_value=blocked_proxy)
            fetcher._requests_fetch = mock.Mock(
                side_effect=AssertionError(
                    "requests should not run when playwright-first runtime preflight only sees the blocked proxy"
                )
            )
            fetcher._playwright_fetch = mock.Mock(
                side_effect=AssertionError(
                    "playwright should not run when playwright-first runtime preflight only sees the blocked proxy"
                )
            )

            response, status, notes = fetcher.fetch(
                "https://factory.example/browser-first-proxy-denied",
                "playwright",
                route_family="homepage",
            )

            self.assertIsNone(response)
            self.assertEqual(status, "blocked_by_policy")
            self.assertIn("recovery stopped: same proxy would be reused after prior block", notes)
            self.assertIn("browser escalation denied because the same blocked proxy would be reused", notes)
            self.assertEqual(fetcher.last_fetch_result.access_state, "blocked")
            self.assertEqual(len(fetcher.last_fetch_result.attempts), 1)
            first_attempt = fetcher.last_fetch_result.attempts[0]
            self.assertEqual(first_attempt.status, "blocked_no_alternative_proxy")
            self.assertEqual(first_attempt.anti_bot_reason, "blocked_no_alternative_proxy")
            self.assertEqual(first_attempt.proxy_label_or_id, "proxy-1")
            self.assertFalse(first_attempt.playwright_used)
            fetcher._requests_fetch.assert_not_called()
            fetcher._playwright_fetch.assert_not_called()
            self._assert_transport_contract(
                fetcher,
                transport_selected="playwright",
                transport_final="playwright",
                escalation_reason="proxy_reuse_policy_denied",
                blocked_by_policy=True,
            )
            self._assert_terminal_progress_surfaces(
                progress_store,
                raw_status="blocked_no_alternative_proxy",
                transport_final="playwright",
                escalation_reason="proxy_reuse_policy_denied",
                raw_anti_bot_reason="blocked_no_alternative_proxy",
                raw_block_class=first_attempt.block_class,
                raw_http_status=None,
                raw_cooldown_seconds=first_attempt.cooldown_seconds,
                expected_first_signal_tags=["cooldown"] if first_attempt.cooldown_seconds > 0 else [],
                raw_blocked_by_policy=True,
                expected_event_types=("route_fetch_playwright", "route_fetch_terminal"),
                expected_first_fetch_mode="playwright",
                expected_first_transport_selected="playwright",
                expected_first_transport_final="playwright",
                expected_terminal_fetch_mode="playwright",
                expected_terminal_transport_selected="playwright",
            )

    def test_resume_preflight_breaker_pause_progress_surface_contract(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            progress_store = ProgressStore(Path(tmpdir))
            first_ts = (datetime.now(timezone.utc) - timedelta(seconds=6)).isoformat().replace("+00:00", "Z")
            second_ts = (datetime.now(timezone.utc) - timedelta(seconds=3)).isoformat().replace("+00:00", "Z")
            for ts, proxy_label in ((first_ts, "proxy-a"), (second_ts, "proxy-b")):
                update_host_memory_from_event_payload(
                    progress_store.host_memory,
                    {
                        "ts": ts,
                        "event_type": "route_fetch_attempt",
                        "host": "factory.example",
                        "status": "http_429",
                        "proxy_label_or_id": proxy_label,
                        "cooldown_seconds": 45,
                        "anti_bot_reason": "http_429",
                        "block_class": "RATE_LIMIT",
                        "http_status": 429,
                    },
                    ts=ts,
                )
            initial_recent_attempts = list(
                normalize_host_memory_state(progress_store.host_memory)["factory.example"]["recent_attempts"]
            )

            fetcher = self.make_fetcher(progress_store=progress_store)
            fetcher._requests_fetch = mock.Mock(
                side_effect=AssertionError(
                    "requests should not run when the pre-network breaker pause contour already short-circuited the route"
                )
            )
            fetcher._playwright_fetch = mock.Mock(
                side_effect=AssertionError(
                    "playwright should not run when the pre-network breaker pause contour already short-circuited the route"
                )
            )

            response, status, notes = fetcher.fetch(
                "https://factory.example/browser-first",
                "hybrid",
                route_family="homepage",
            )

            self.assertIsNone(response)
            self.assertEqual(status, ACCESS_STATE_PAUSED_BY_BREAKER)
            self.assertIn("route paused by survival breaker", notes)
            self.assertEqual(fetcher.last_fetch_result.access_state, ACCESS_STATE_PAUSED_BY_BREAKER)
            self.assertEqual(fetcher.last_fetch_result.breaker_mode, BREAKER_MODE_SURVIVAL)
            self.assertEqual(len(fetcher.last_fetch_result.attempts), 1)
            first_attempt = fetcher.last_fetch_result.attempts[0]
            self.assertEqual(first_attempt.status, ACCESS_STATE_PAUSED_BY_BREAKER)
            self.assertTrue(first_attempt.blocked_by_policy)
            self.assertEqual(first_attempt.anti_bot_reason, "paused_by_breaker")
            self.assertEqual(first_attempt.block_class, BLOCK_CLASS_HARD_BAN)
            fetcher._requests_fetch.assert_not_called()
            fetcher._playwright_fetch.assert_not_called()
            self._assert_transport_contract(
                fetcher,
                transport_selected="hybrid",
                transport_final="hybrid",
                escalation_reason="paused_by_breaker",
                blocked_by_policy=True,
            )

            events = load_jsonl(progress_store.events_jsonl)
            self.assertEqual([event["type"] for event in events], ["route_fetch_breaker_pause"])
            breaker_event = events[0]
            self.assertEqual(breaker_event["status"], ACCESS_STATE_PAUSED_BY_BREAKER)
            self.assertIs(breaker_event["blocked_by_policy"], True)
            self.assertEqual(breaker_event["transport_selected"], "hybrid")
            self.assertEqual(breaker_event["transport_final"], "hybrid")
            self.assertEqual(breaker_event["anti_bot_reason"], "paused_by_breaker")
            self.assertEqual(breaker_event["block_class"], BLOCK_CLASS_HARD_BAN)
            self.assertFalse(progress_store.results_jsonl.exists())

            stage_messages = load_stage_messages(progress_store.output_dir)
            self.assertEqual(
                [message["payload"]["event_type"] for message in stage_messages],
                ["route_fetch_breaker_pause"],
            )
            self.assertEqual(stage_messages[0]["payload"]["status"], ACCESS_STATE_PAUSED_BY_BREAKER)

            host_recent_attempts = normalize_host_memory_state(progress_store.host_memory)["factory.example"][
                "recent_attempts"
            ]
            self.assertEqual(len(host_recent_attempts), len(initial_recent_attempts) + 1)
            self.assertEqual(host_recent_attempts[-1]["event_type"], "route_fetch_breaker_pause")
            self.assertNotIn("route_fetch_terminal", [attempt["event_type"] for attempt in host_recent_attempts[-2:]])
            breaker_host_attempt = host_recent_attempts[-1]
            self.assertEqual(breaker_host_attempt["status"], ACCESS_STATE_PAUSED_BY_BREAKER)
            self.assertIs(breaker_host_attempt["blocked_by_policy"], True)
            self.assertEqual(breaker_host_attempt["transport_selected"], "hybrid")
            self.assertEqual(breaker_host_attempt["transport_final"], "hybrid")
            self.assertEqual(breaker_host_attempt["anti_bot_reason"], "paused_by_breaker")
            self.assertEqual(breaker_host_attempt["block_class"], BLOCK_CLASS_HARD_BAN)
            self.assertEqual(breaker_host_attempt["signal_tags"], [])

    def test_resume_preflight_requests_breaker_pause_progress_surface_contract(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            progress_store = ProgressStore(Path(tmpdir))
            first_ts = (datetime.now(timezone.utc) - timedelta(seconds=6)).isoformat().replace("+00:00", "Z")
            second_ts = (datetime.now(timezone.utc) - timedelta(seconds=3)).isoformat().replace("+00:00", "Z")
            for ts, proxy_label in ((first_ts, "proxy-a"), (second_ts, "proxy-b")):
                update_host_memory_from_event_payload(
                    progress_store.host_memory,
                    {
                        "ts": ts,
                        "event_type": "route_fetch_attempt",
                        "host": "factory.example",
                        "status": "http_429",
                        "proxy_label_or_id": proxy_label,
                        "cooldown_seconds": 45,
                        "anti_bot_reason": "http_429",
                        "block_class": "RATE_LIMIT",
                        "http_status": 429,
                    },
                    ts=ts,
                )
            initial_recent_attempts = list(
                normalize_host_memory_state(progress_store.host_memory)["factory.example"]["recent_attempts"]
            )

            fetcher = self.make_fetcher(progress_store=progress_store)
            fetcher._requests_fetch = mock.Mock(
                side_effect=AssertionError(
                    "requests should not run when the requests pre-network breaker pause contour already short-circuited the route"
                )
            )
            fetcher._playwright_fetch = mock.Mock(
                side_effect=AssertionError(
                    "playwright should not run when the requests pre-network breaker pause contour already short-circuited the route"
                )
            )

            response, status, notes = fetcher.fetch(
                "https://factory.example/requests-first",
                "requests",
                route_family="homepage",
            )

            self.assertIsNone(response)
            self.assertEqual(status, ACCESS_STATE_PAUSED_BY_BREAKER)
            self.assertIn("route paused by survival breaker", notes)
            self.assertEqual(fetcher.last_fetch_result.access_state, ACCESS_STATE_PAUSED_BY_BREAKER)
            self.assertEqual(fetcher.last_fetch_result.breaker_mode, BREAKER_MODE_SURVIVAL)
            self.assertEqual(len(fetcher.last_fetch_result.attempts), 1)
            first_attempt = fetcher.last_fetch_result.attempts[0]
            self.assertEqual(first_attempt.fetch_mode, "requests")
            self.assertEqual(first_attempt.status, ACCESS_STATE_PAUSED_BY_BREAKER)
            self.assertTrue(first_attempt.blocked_by_policy)
            self.assertEqual(first_attempt.anti_bot_reason, "paused_by_breaker")
            self.assertEqual(first_attempt.block_class, BLOCK_CLASS_HARD_BAN)
            self.assertEqual(first_attempt.breaker_mode, BREAKER_MODE_SURVIVAL)
            self.assertEqual(first_attempt.transport_selected, "requests")
            self.assertEqual(first_attempt.transport_final, "requests")
            fetcher._requests_fetch.assert_not_called()
            fetcher._playwright_fetch.assert_not_called()
            self._assert_transport_contract(
                fetcher,
                transport_selected="requests",
                transport_final="requests",
                escalation_reason="paused_by_breaker",
                blocked_by_policy=True,
            )

            events = load_jsonl(progress_store.events_jsonl)
            self.assertEqual([event["type"] for event in events], ["route_fetch_breaker_pause"])
            breaker_event = events[0]
            self.assertEqual(breaker_event["status"], ACCESS_STATE_PAUSED_BY_BREAKER)
            self.assertIs(breaker_event["blocked_by_policy"], True)
            self.assertEqual(breaker_event["breaker_mode"], BREAKER_MODE_SURVIVAL)
            self.assertEqual(breaker_event["transport_selected"], "requests")
            self.assertEqual(breaker_event["transport_final"], "requests")
            self.assertEqual(breaker_event["anti_bot_reason"], "paused_by_breaker")
            self.assertEqual(breaker_event["block_class"], BLOCK_CLASS_HARD_BAN)
            self.assertFalse(progress_store.results_jsonl.exists())

            stage_messages = load_stage_messages(progress_store.output_dir)
            self.assertEqual(
                [message["payload"]["event_type"] for message in stage_messages],
                ["route_fetch_breaker_pause"],
            )
            self.assertEqual(stage_messages[0]["payload"]["status"], ACCESS_STATE_PAUSED_BY_BREAKER)

            host_recent_attempts = normalize_host_memory_state(progress_store.host_memory)["factory.example"][
                "recent_attempts"
            ]
            self.assertEqual(len(host_recent_attempts), len(initial_recent_attempts) + 1)
            self.assertEqual(host_recent_attempts[-1]["event_type"], "route_fetch_breaker_pause")
            self.assertNotIn("route_fetch_terminal", [attempt["event_type"] for attempt in host_recent_attempts[-2:]])
            breaker_host_attempt = host_recent_attempts[-1]
            self.assertEqual(breaker_host_attempt["status"], ACCESS_STATE_PAUSED_BY_BREAKER)
            self.assertIs(breaker_host_attempt["blocked_by_policy"], True)
            self.assertEqual(breaker_host_attempt["transport_selected"], "requests")
            self.assertEqual(breaker_host_attempt["transport_final"], "requests")
            self.assertEqual(breaker_host_attempt["anti_bot_reason"], "paused_by_breaker")
            self.assertEqual(breaker_host_attempt["block_class"], BLOCK_CLASS_HARD_BAN)
            self.assertEqual(breaker_host_attempt["signal_tags"], [])

    def test_resume_preflight_playwright_breaker_pause_progress_surface_contract(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            progress_store = ProgressStore(Path(tmpdir))
            first_ts = (datetime.now(timezone.utc) - timedelta(seconds=6)).isoformat().replace("+00:00", "Z")
            second_ts = (datetime.now(timezone.utc) - timedelta(seconds=3)).isoformat().replace("+00:00", "Z")
            for ts, proxy_label in ((first_ts, "proxy-a"), (second_ts, "proxy-b")):
                update_host_memory_from_event_payload(
                    progress_store.host_memory,
                    {
                        "ts": ts,
                        "event_type": "route_fetch_attempt",
                        "host": "factory.example",
                        "status": "http_429",
                        "proxy_label_or_id": proxy_label,
                        "cooldown_seconds": 45,
                        "anti_bot_reason": "http_429",
                        "block_class": "RATE_LIMIT",
                        "http_status": 429,
                    },
                    ts=ts,
                )
            initial_recent_attempts = list(
                normalize_host_memory_state(progress_store.host_memory)["factory.example"]["recent_attempts"]
            )

            fetcher = self.make_fetcher(progress_store=progress_store)
            fetcher._requests_fetch = mock.Mock(
                side_effect=AssertionError(
                    "requests should not run when the playwright pre-network breaker pause contour already short-circuited the route"
                )
            )
            fetcher._playwright_fetch = mock.Mock(
                side_effect=AssertionError(
                    "playwright should not run when the playwright pre-network breaker pause contour already short-circuited the route"
                )
            )

            response, status, notes = fetcher.fetch(
                "https://factory.example/browser-first",
                "playwright",
                route_family="homepage",
            )

            self.assertIsNone(response)
            self.assertEqual(status, ACCESS_STATE_PAUSED_BY_BREAKER)
            self.assertIn("route paused by survival breaker", notes)
            self.assertEqual(fetcher.last_fetch_result.access_state, ACCESS_STATE_PAUSED_BY_BREAKER)
            self.assertEqual(fetcher.last_fetch_result.breaker_mode, BREAKER_MODE_SURVIVAL)
            self.assertEqual(len(fetcher.last_fetch_result.attempts), 1)
            first_attempt = fetcher.last_fetch_result.attempts[0]
            self.assertEqual(first_attempt.fetch_mode, "playwright")
            self.assertEqual(first_attempt.status, ACCESS_STATE_PAUSED_BY_BREAKER)
            self.assertTrue(first_attempt.blocked_by_policy)
            self.assertEqual(first_attempt.anti_bot_reason, "paused_by_breaker")
            self.assertEqual(first_attempt.block_class, BLOCK_CLASS_HARD_BAN)
            self.assertEqual(first_attempt.breaker_mode, BREAKER_MODE_SURVIVAL)
            self.assertEqual(first_attempt.transport_selected, "playwright")
            self.assertEqual(first_attempt.transport_final, "playwright")
            fetcher._requests_fetch.assert_not_called()
            fetcher._playwright_fetch.assert_not_called()
            self._assert_transport_contract(
                fetcher,
                transport_selected="playwright",
                transport_final="playwright",
                escalation_reason="paused_by_breaker",
                blocked_by_policy=True,
            )

            events = load_jsonl(progress_store.events_jsonl)
            self.assertEqual([event["type"] for event in events], ["route_fetch_breaker_pause"])
            breaker_event = events[0]
            self.assertEqual(breaker_event["status"], ACCESS_STATE_PAUSED_BY_BREAKER)
            self.assertIs(breaker_event["blocked_by_policy"], True)
            self.assertEqual(breaker_event["breaker_mode"], BREAKER_MODE_SURVIVAL)
            self.assertEqual(breaker_event["transport_selected"], "playwright")
            self.assertEqual(breaker_event["transport_final"], "playwright")
            self.assertEqual(breaker_event["anti_bot_reason"], "paused_by_breaker")
            self.assertEqual(breaker_event["block_class"], BLOCK_CLASS_HARD_BAN)
            self.assertFalse(progress_store.results_jsonl.exists())

            stage_messages = load_stage_messages(progress_store.output_dir)
            self.assertEqual(
                [message["payload"]["event_type"] for message in stage_messages],
                ["route_fetch_breaker_pause"],
            )
            self.assertEqual(stage_messages[0]["payload"]["status"], ACCESS_STATE_PAUSED_BY_BREAKER)

            host_recent_attempts = normalize_host_memory_state(progress_store.host_memory)["factory.example"][
                "recent_attempts"
            ]
            self.assertEqual(len(host_recent_attempts), len(initial_recent_attempts) + 1)
            self.assertEqual(host_recent_attempts[-1]["event_type"], "route_fetch_breaker_pause")
            self.assertNotIn("route_fetch_terminal", [attempt["event_type"] for attempt in host_recent_attempts[-2:]])
            breaker_host_attempt = host_recent_attempts[-1]
            self.assertEqual(breaker_host_attempt["status"], ACCESS_STATE_PAUSED_BY_BREAKER)
            self.assertIs(breaker_host_attempt["blocked_by_policy"], True)
            self.assertEqual(breaker_host_attempt["transport_selected"], "playwright")
            self.assertEqual(breaker_host_attempt["transport_final"], "playwright")
            self.assertEqual(breaker_host_attempt["anti_bot_reason"], "paused_by_breaker")
            self.assertEqual(breaker_host_attempt["block_class"], BLOCK_CLASS_HARD_BAN)
            self.assertEqual(breaker_host_attempt["signal_tags"], [])

    def test_resume_bootstrap_pauses_survival_breaker_before_network_call(self) -> None:
        host_memory: dict[str, object] = {}
        first_ts = (datetime.now(timezone.utc) - timedelta(seconds=6)).isoformat().replace("+00:00", "Z")
        second_ts = (datetime.now(timezone.utc) - timedelta(seconds=3)).isoformat().replace("+00:00", "Z")
        for ts, proxy_label in ((first_ts, "proxy-a"), (second_ts, "proxy-b")):
            update_host_memory_from_event_payload(
                host_memory,
                {
                    "ts": ts,
                    "event_type": "route_fetch_attempt",
                    "host": "factory.example",
                    "status": "http_429",
                    "proxy_label_or_id": proxy_label,
                    "cooldown_seconds": 45,
                    "anti_bot_reason": "http_429",
                    "block_class": "RATE_LIMIT",
                    "http_status": 429,
                },
                ts=ts,
            )

        fetcher = Fetcher(FakeClient(progress_store=SimpleNamespace(host_memory=host_memory)))
        fetcher.playwright_enabled = True
        fetcher.max_attempts = 2
        fetcher.session_store.load = lambda host: None
        fetcher._requests_fetch = mock.Mock(side_effect=AssertionError("requests should not run when runtime breaker bootstrap already paused the route"))
        fetcher._playwright_fetch = mock.Mock(side_effect=AssertionError("playwright should not run when runtime breaker bootstrap already paused the route"))

        response, status, notes = fetcher.fetch("https://factory.example/browser-first", "hybrid", route_family="homepage")

        self.assertIsNone(response)
        self.assertEqual(status, ACCESS_STATE_PAUSED_BY_BREAKER)
        self.assertIn("route paused by survival breaker", notes)
        self.assertEqual(fetcher.last_fetch_result.breaker_mode, BREAKER_MODE_SURVIVAL)
        self.assertEqual(fetcher.breakers.state_for_host("factory.example").mode, BREAKER_MODE_SURVIVAL)
        fetcher._requests_fetch.assert_not_called()
        fetcher._playwright_fetch.assert_not_called()

    def test_survival_session_bootstrap_allows_fresh_browser_bootstrap_on_safe_alternative_proxy(self) -> None:
        host_memory: dict[str, object] = {}
        first_ts = (datetime.now(timezone.utc) - timedelta(seconds=6)).isoformat().replace("+00:00", "Z")
        second_ts = (datetime.now(timezone.utc) - timedelta(seconds=3)).isoformat().replace("+00:00", "Z")
        for ts in (first_ts, second_ts):
            update_host_memory_from_event_payload(
                host_memory,
                {
                    "ts": ts,
                    "event_type": "route_fetch_attempt",
                    "host": "factory.example",
                    "status": "bot_gate",
                    "proxy_label_or_id": "proxy-1",
                    "cooldown_seconds": 0,
                    "anti_bot_reason": "bot_gate",
                    "block_class": "SOFT_BLOCK",
                },
                ts=ts,
            )

        fetcher = Fetcher(FakeClient(progress_store=SimpleNamespace(host_memory=host_memory)))
        fetcher.playwright_enabled = True
        fetcher.max_attempts = 2
        fetcher.session_store.load = lambda host: make_session_profile(proxy_label_or_id="proxy-1")
        safe_proxy = ProxySelection(
            url="http://proxy2.example:8080",
            source="test",
            proxy_id="proxy-2",
            label="proxy-2",
            host="proxy2.example",
            port="8080",
            via_proxy=True,
        )
        fetcher._select_proxy = mock.Mock(return_value=safe_proxy)
        fetcher._requests_fetch = mock.Mock(
            side_effect=AssertionError("requests should not run when survival session bootstrap can switch to a safe alternative proxy")
        )
        fetcher._playwright_fetch = mock.Mock(
            return_value=make_execution(
                attempt_mode="playwright",
                status="success",
                proxy_selection=safe_proxy,
                response=make_response(url="https://factory.example/reload", text=make_usable_html("fresh bootstrap")),
                notes=["playwright success"],
                session_reused=False,
            )
        )

        response, status, notes = fetcher.fetch("https://factory.example/reload", "hybrid", route_family="homepage")

        self.assertEqual(status, "success")
        self.assertIsNotNone(response)
        self.assertIn("playwright success", notes)
        self.assertEqual(fetcher.last_fetch_result.breaker_mode, BREAKER_MODE_NORMAL)
        self.assertEqual(fetcher.breakers.state_for_host("factory.example").mode, BREAKER_MODE_NORMAL)
        fetcher._select_proxy.assert_called_once()
        fetcher._requests_fetch.assert_not_called()
        fetcher._playwright_fetch.assert_called_once()
        self.assertIsNone(fetcher._playwright_fetch.call_args.kwargs["session_profile"])
        self.assertEqual(fetcher._playwright_fetch.call_args.kwargs["proxy_selection"].proxy_label_or_id, "proxy-2")
        self.assertEqual(fetcher.last_fetch_result.transport_final, "playwright")
        self.assertEqual(fetcher.last_fetch_result.attempts[0].fetch_mode, "playwright")
        self.assertEqual(fetcher.last_fetch_result.attempts[0].proxy_label_or_id, "proxy-2")
        self.assertFalse(fetcher.last_fetch_result.attempts[0].session_reused)

    def test_survival_session_bootstrap_allows_fresh_browser_bootstrap_on_safe_direct_selection(self) -> None:
        host_memory: dict[str, object] = {}
        first_ts = (datetime.now(timezone.utc) - timedelta(seconds=6)).isoformat().replace("+00:00", "Z")
        second_ts = (datetime.now(timezone.utc) - timedelta(seconds=3)).isoformat().replace("+00:00", "Z")
        for ts in (first_ts, second_ts):
            update_host_memory_from_event_payload(
                host_memory,
                {
                    "ts": ts,
                    "event_type": "route_fetch_attempt",
                    "host": "factory.example",
                    "status": "bot_gate",
                    "proxy_label_or_id": "proxy-1",
                    "cooldown_seconds": 0,
                    "anti_bot_reason": "bot_gate",
                    "block_class": "SOFT_BLOCK",
                },
                ts=ts,
            )

        fetcher = Fetcher(FakeClient(progress_store=SimpleNamespace(host_memory=host_memory)))
        fetcher.playwright_enabled = True
        fetcher.max_attempts = 2
        fetcher.session_store.load = lambda host: make_session_profile(proxy_label_or_id="proxy-1")
        direct = ProxySelection()
        fetcher._select_proxy = mock.Mock(return_value=direct)
        fetcher._requests_fetch = mock.Mock(
            side_effect=AssertionError("requests should not run when survival session bootstrap can switch to direct/no-proxy")
        )
        fetcher._playwright_fetch = mock.Mock(
            return_value=make_execution(
                attempt_mode="playwright",
                status="success",
                proxy_selection=direct,
                response=make_response(url="https://factory.example/reload", text=make_usable_html("fresh direct bootstrap")),
                notes=["playwright success"],
                session_reused=False,
            )
        )

        response, status, notes = fetcher.fetch("https://factory.example/reload", "hybrid", route_family="homepage")

        self.assertEqual(status, "success")
        self.assertIsNotNone(response)
        self.assertIn("playwright success", notes)
        self.assertEqual(fetcher.last_fetch_result.breaker_mode, BREAKER_MODE_NORMAL)
        self.assertEqual(fetcher.breakers.state_for_host("factory.example").mode, BREAKER_MODE_NORMAL)
        fetcher._select_proxy.assert_called_once()
        fetcher._requests_fetch.assert_not_called()
        fetcher._playwright_fetch.assert_called_once()
        self.assertIsNone(fetcher._playwright_fetch.call_args.kwargs["session_profile"])
        self.assertEqual(fetcher._playwright_fetch.call_args.kwargs["proxy_selection"].proxy_label_or_id, "")
        self.assertFalse(fetcher._playwright_fetch.call_args.kwargs["proxy_selection"].via_proxy)
        self.assertEqual(fetcher.last_fetch_result.transport_final, "playwright")
        self.assertEqual(fetcher.last_fetch_result.attempts[0].fetch_mode, "playwright")
        self.assertEqual(fetcher.last_fetch_result.attempts[0].proxy_label_or_id, "")
        self.assertFalse(fetcher.last_fetch_result.attempts[0].session_reused)

    def test_survival_session_bootstrap_pauses_when_runtime_host_memory_blocks_session_proxy_without_safe_alternative_proxy(self) -> None:
        host_memory: dict[str, object] = {}
        first_ts = (datetime.now(timezone.utc) - timedelta(seconds=6)).isoformat().replace("+00:00", "Z")
        second_ts = (datetime.now(timezone.utc) - timedelta(seconds=3)).isoformat().replace("+00:00", "Z")
        for ts in (first_ts, second_ts):
            update_host_memory_from_event_payload(
                host_memory,
                {
                    "ts": ts,
                    "event_type": "route_fetch_attempt",
                    "host": "factory.example",
                    "status": "bot_gate",
                    "proxy_label_or_id": "proxy-1",
                    "cooldown_seconds": 0,
                    "anti_bot_reason": "bot_gate",
                    "block_class": "SOFT_BLOCK",
                },
                ts=ts,
            )

        fetcher = Fetcher(FakeClient(progress_store=SimpleNamespace(host_memory=host_memory)))
        fetcher.playwright_enabled = True
        fetcher.max_attempts = 2
        fetcher.session_store.load = lambda host: make_session_profile(proxy_label_or_id="proxy-1")
        blocked_proxy = ProxySelection(
            url="http://proxy1.example:8080",
            source="test",
            proxy_id="proxy-1",
            label="proxy-1",
            host="proxy1.example",
            port="8080",
            via_proxy=True,
        )
        fetcher._select_proxy = mock.Mock(return_value=blocked_proxy)
        fetcher._requests_fetch = mock.Mock(
            side_effect=AssertionError("requests should not run when survival session bootstrap is denied before network")
        )
        fetcher._playwright_fetch = mock.Mock(
            side_effect=AssertionError("playwright should not run when survival session bootstrap is denied before network")
        )

        response, status, notes = fetcher.fetch("https://factory.example/reload", "hybrid", route_family="homepage")

        self.assertIsNone(response)
        self.assertEqual(status, ACCESS_STATE_PAUSED_BY_BREAKER)
        self.assertIn("route paused by survival breaker", notes)
        self.assertEqual(fetcher.last_fetch_result.breaker_mode, BREAKER_MODE_SURVIVAL)
        self.assertEqual(fetcher.breakers.state_for_host("factory.example").mode, BREAKER_MODE_SURVIVAL)
        fetcher._select_proxy.assert_called_once()
        fetcher._requests_fetch.assert_not_called()
        fetcher._playwright_fetch.assert_not_called()

    def test_survival_session_bootstrap_stays_allowed_for_safe_session_proxy(self) -> None:
        host_memory: dict[str, object] = {}
        first_ts = (datetime.now(timezone.utc) - timedelta(seconds=6)).isoformat().replace("+00:00", "Z")
        second_ts = (datetime.now(timezone.utc) - timedelta(seconds=3)).isoformat().replace("+00:00", "Z")
        for ts in (first_ts, second_ts):
            update_host_memory_from_event_payload(
                host_memory,
                {
                    "ts": ts,
                    "event_type": "route_fetch_attempt",
                    "host": "factory.example",
                    "status": "bot_gate",
                    "proxy_label_or_id": "proxy-1",
                    "cooldown_seconds": 0,
                    "anti_bot_reason": "bot_gate",
                    "block_class": "SOFT_BLOCK",
                },
                ts=ts,
            )

        fetcher = Fetcher(FakeClient(progress_store=SimpleNamespace(host_memory=host_memory)))
        fetcher.playwright_enabled = True
        fetcher.max_attempts = 2
        fetcher.session_store.load = lambda host: make_session_profile(proxy_label_or_id="proxy-2")
        fetcher._select_proxy = mock.Mock(return_value=ProxySelection())
        fetcher._requests_fetch = mock.Mock(side_effect=AssertionError("requests should not run when survival session bootstrap stays allowed"))
        fetcher._playwright_fetch = mock.Mock(
            return_value=make_execution(
                attempt_mode="playwright",
                status="success",
                proxy_selection=ProxySelection(),
                response=make_response(url="https://factory.example/reload", text=make_usable_html("session bootstrap")),
                notes=["playwright success"],
                session_reused=True,
            )
        )

        response, status, notes = fetcher.fetch("https://factory.example/reload", "hybrid", route_family="homepage")

        self.assertEqual(status, "success")
        self.assertIsNotNone(response)
        self.assertIn("playwright success", notes)
        self.assertEqual(fetcher.last_fetch_result.breaker_mode, BREAKER_MODE_NORMAL)
        fetcher._requests_fetch.assert_not_called()
        fetcher._playwright_fetch.assert_called_once()
        self.assertEqual(fetcher._playwright_fetch.call_args.kwargs["session_profile"].proxy_label_or_id, "proxy-2")
        self.assertEqual(fetcher.last_fetch_result.transport_final, "playwright")
        self.assertEqual(fetcher.last_fetch_result.attempts[0].fetch_mode, "playwright")
        self.assertTrue(fetcher.last_fetch_result.attempts[0].session_reused)

    def test_breaker_bootstrap_latest_success_clears_stale_open_state(self) -> None:
        registry = DomainBreakerRegistry()
        recent_attempts = [
            {
                "ts": "2026-04-19T11:00:00Z",
                "host": "factory.example",
                "status": "http_429",
                "anti_bot_reason": "http_429",
                "block_class": "RATE_LIMIT",
                "proxy_label_or_id": "proxy-a",
                "cooldown_seconds": 45,
                "challenge_detected": False,
            },
            {
                "ts": "2026-04-19T11:00:05Z",
                "host": "factory.example",
                "status": "bot_gate",
                "anti_bot_reason": "bot_gate",
                "block_class": "SOFT_BLOCK",
                "proxy_label_or_id": "proxy-b",
                "cooldown_seconds": 20,
                "challenge_detected": False,
            },
            {
                "ts": "2026-04-19T11:00:10Z",
                "host": "factory.example",
                "status": "success",
                "anti_bot_reason": "",
                "block_class": "SUCCESS",
                "proxy_label_or_id": "proxy-c",
                "cooldown_seconds": 0,
                "challenge_detected": False,
            },
        ]

        state = registry.bootstrap_from_recent_attempts("factory.example", recent_attempts)

        self.assertEqual(state.mode, BREAKER_MODE_NORMAL)
        self.assertFalse(state.breaker_open)
        self.assertFalse(state.cooldown_active)
        self.assertEqual(state.last_block_class, BLOCK_CLASS_SUCCESS)


class FactorySiteAntiBotSmokeAcceptanceTests(unittest.TestCase):
    @staticmethod
    def make_acceptance_attempt(
        *,
        block_class: str = BLOCK_CLASS_SUCCESS,
        access_state: str = ACCESS_STATE_RECOVERED,
        session_reused: bool = False,
        challenge_detected: bool = False,
        manual_handoff_required: bool = False,
    ) -> SimpleNamespace:
        return SimpleNamespace(
            block_class=block_class,
            access_state=access_state,
            session_reused=session_reused,
            challenge_detected=challenge_detected,
            manual_handoff_required=manual_handoff_required,
        )

    def test_live_acceptance_rejects_content_without_recovery_signal_or_session_reuse(self) -> None:
        telemetry = [
            self.make_acceptance_attempt(
                block_class=BLOCK_CLASS_SUCCESS,
                access_state=ACCESS_STATE_RECOVERED,
                session_reused=False,
            )
        ]

        accepted = _live_acceptance_passed(
            access_state=ACCESS_STATE_RECOVERED,
            content_records=1,
            manual_handoff_required=False,
            session_reused=False,
            telemetry=telemetry,
        )

        self.assertFalse(accepted)

    def test_live_acceptance_allows_seeded_session_rerun_without_current_recovery_signal(self) -> None:
        telemetry = [
            self.make_acceptance_attempt(
                block_class=BLOCK_CLASS_SUCCESS,
                access_state=ACCESS_STATE_RECOVERED,
                session_reused=True,
            )
        ]

        accepted = _live_acceptance_passed(
            access_state=ACCESS_STATE_RECOVERED,
            content_records=1,
            manual_handoff_required=False,
            session_reused=True,
            telemetry=telemetry,
        )

        self.assertTrue(accepted)


if __name__ == "__main__":
    unittest.main()

from __future__ import annotations

import json
import logging
import threading
from pathlib import Path
from types import SimpleNamespace

import pytest
import requests

import company_enrichment_core as core
import app.runtime.proxies as runtime_proxies
import app.runtime.proxy6 as runtime_proxy6
from app.runtime import ProxyPool
from app.site_intelligence.site_authenticity import SiteAuthHelpers, SiteAuthenticityAnalyzer


@pytest.fixture(autouse=True)
def _clear_proxy6_inventory_cache(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("PROXY6_API_KEY", raising=False)
    monkeypatch.delenv("PROXY6_DIAGNOSTIC_CACHE_TTL_SECONDS", raising=False)
    runtime_proxy6.clear_proxy6_inventory_diagnostic_cache()
    yield
    runtime_proxy6.clear_proxy6_inventory_diagnostic_cache()


class _ProxyPoolStub:
    def __init__(self, selections: list[SimpleNamespace] | None = None) -> None:
        self.bad_marks: list[tuple[str, str]] = []
        self.ok_marks: list[str] = []
        self.select_calls: list[str | None] = []
        self.selections = list(selections or [])
        self.entries = [SimpleNamespace(url=selection.url) for selection in self.selections if getattr(selection, "url", "")]

    def select(self, host: str | None = None, *, source_name: str | None = None) -> SimpleNamespace:
        self.select_calls.append(host)
        if self.selections:
            return self.selections.pop(0)
        return SimpleNamespace(
            via_proxy=False,
            label="",
            proxy_id="",
            proxy_label_or_id="",
            requests_proxies={},
            url="",
        )

    def mark_bad(self, url: str, *, reason: str, source_name: str | None = None) -> None:
        self.bad_marks.append((url, reason))

    def mark_ok(self, url: str, source_name: str | None = None) -> None:
        self.ok_marks.append(url)


def _proxy_selection(url: str, *, label: str, proxy_id: str = "") -> SimpleNamespace:
    return SimpleNamespace(
        via_proxy=True,
        label=label,
        proxy_id=proxy_id,
        proxy_label_or_id=proxy_id or label,
        requests_proxies={"http": url, "https": url},
        url=url,
    )


def _proxy6_proxy(
    proxy_id: str,
    *,
    active: bool,
    country: str = "ru",
    descr: str = "",
) -> runtime_proxy6.Proxy6Proxy:
    return runtime_proxy6.Proxy6Proxy(
        id=proxy_id,
        host=f"proxy-{proxy_id}.example",
        port="8080",
        user="user",
        password="password",
        proxy_type="http",
        country=country,
        date="2026-01-01",
        date_end="2026-02-01",
        descr=descr,
        active=active,
    )


def _install_proxy6_diagnostic_client(
    monkeypatch: pytest.MonkeyPatch,
    *,
    active: list[runtime_proxy6.Proxy6Proxy] | None = None,
    expired: list[runtime_proxy6.Proxy6Proxy] | None = None,
    all_items: list[runtime_proxy6.Proxy6Proxy] | None = None,
) -> list[str]:
    calls: list[str] = []
    active_items = list(active or [])
    expired_items = list(expired or [])
    all_proxy_items = list(all_items or active_items + expired_items)

    class _Proxy6ClientStub:
        def __init__(self, *_args: object, **_kwargs: object) -> None:
            calls.append("init")

        def get_account(self) -> dict[str, object]:
            calls.append("get_account")
            return {"balance": "0.00", "currency": "USD"}

        def get_all_proxies(self, *, state: str, **_kwargs: object) -> list[runtime_proxy6.Proxy6Proxy]:
            calls.append(f"get_all_proxies:{state}")
            if state == "active":
                return list(active_items)
            if state == "expired":
                return list(expired_items)
            if state == "all":
                return list(all_proxy_items)
            return []

    runtime_proxy6.clear_proxy6_inventory_diagnostic_cache()
    monkeypatch.setenv("PROXY6_API_KEY", "test-proxy6-key")
    monkeypatch.setenv("PROXY6_DIAGNOSTIC_CACHE_TTL_SECONDS", "300")
    monkeypatch.setattr(runtime_proxy6, "Proxy6Client", _Proxy6ClientStub)
    return calls


def _build_response(url: str, *, status_code: int = 200, text: str = "ok") -> requests.Response:
    response = requests.Response()
    response.status_code = status_code
    response.url = url
    response._content = text.encode("utf-8")
    response.headers["Content-Type"] = "text/html; charset=utf-8"
    response.encoding = "utf-8"
    return response


def _build_client(
    tmp_path: Path,
    proxy_pool: _ProxyPoolStub | None = None,
) -> tuple[core.RateLimitedHttpClient, _ProxyPoolStub]:
    proxy_pool = proxy_pool or _ProxyPoolStub()
    client = core.RateLimitedHttpClient(
        logger=logging.getLogger("tests.request_boundary_urlguard"),
        progress_store=core.ProgressStore(tmp_path / "progress"),
        min_delay_by_host={},
        request_timeout=5,
        cooldown_on_429=60,
        cooldown_on_bot=60,
        proxy_pool=proxy_pool,
    )
    return client, proxy_pool


def _read_events(path: Path) -> list[dict[str, object]]:
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]


def test_proxy_lifecycle_state_matrix_distinguishes_provider_runtime_and_source_states(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("PARSER_PROXY_PRIORITY_RESERVED_CAPACITY", "1")
    missing_config = runtime_proxy6.Proxy6InventoryDiagnostic(
        provider_status=runtime_proxy6.PROXY_PROVIDER_STATUS_UNKNOWN,
        operator_action=runtime_proxy6.PROXY6_OPERATOR_ACTION_CONFIGURE_OR_SYNC,
        reason="PROXY6_API_KEY is not configured",
        missing_config="PROXY6_API_KEY",
    )
    api_unknown = runtime_proxy6.Proxy6InventoryDiagnostic(
        provider_status=runtime_proxy6.PROXY_PROVIDER_STATUS_UNKNOWN,
        operator_action=runtime_proxy6.PROXY6_OPERATOR_ACTION_CONFIGURE_OR_SYNC,
        reason="Proxy6 API unavailable",
        error="timeout",
    )
    expired = runtime_proxy6.Proxy6InventoryDiagnostic(
        provider_status=runtime_proxy6.PROXY_PROVIDER_INVENTORY_EMPTY_OR_EXPIRED,
        operator_action=runtime_proxy6.PROXY6_OPERATOR_ACTION_RENEW,
        reason="Proxy6 reports no active usable parser-pool proxies",
    )
    healthy = runtime_proxy6.Proxy6InventoryDiagnostic(
        provider_status=runtime_proxy6.PROXY_PROVIDER_INVENTORY_HEALTHY,
        operator_action=runtime_proxy6.PROXY6_OPERATOR_ACTION_REPAIR_RUNTIME_POOL,
        reason="Proxy6 reports active parser-pool proxies",
        active_usable_count=2,
        active_total_count=2,
    )

    assert (
        ProxyPool("").lifecycle_snapshot(source_name="checko", diagnostic=missing_config).state
        == runtime_proxies.PROXY_LIFECYCLE_MISSING_RUNTIME_CONFIG
    )
    assert (
        ProxyPool("http://proxy-1.local:8080")
        .lifecycle_snapshot(source_name="checko", diagnostic=api_unknown)
        .state
        == runtime_proxies.PROXY_LIFECYCLE_PROVIDER_UNKNOWN
    )
    assert (
        ProxyPool("http://proxy-1.local:8080")
        .lifecycle_snapshot(source_name="checko", diagnostic=expired)
        .state
        == runtime_proxies.PROXY_LIFECYCLE_PROVIDER_EMPTY_OR_EXPIRED
    )
    assert (
        ProxyPool("http://proxy-1.local:8080")
        .lifecycle_snapshot(source_name="checko", diagnostic=healthy)
        .state
        == runtime_proxies.PROXY_LIFECYCLE_RUNTIME_SYNCED_USABLE
    )

    cooled_pool = ProxyPool("http://proxy-1.local:8080", ban_cooldown_seconds=300)
    cooled_pool.mark_bad("http://proxy-1.local:8080", reason="proxy_tunnel_error", source_name="checko")
    cooled_snapshot = cooled_pool.lifecycle_snapshot(
        source_name="checko",
        diagnostic=healthy,
        sync_fields={"proxy_sync_status": "no_new_usable_proxy"},
    )
    assert cooled_snapshot.state == runtime_proxies.PROXY_LIFECYCLE_RUNTIME_DEPLETED_PROVIDER_HEALTHY
    assert cooled_snapshot.subreason == "same_url_provider_sync_noop_runtime_known_but_ineligible"

    reserved_pool = ProxyPool(
        "http://proxy-1.local:8080,http://proxy-2.local:8080",
        ban_cooldown_seconds=300,
    )
    reserved_pool.mark_bad("http://proxy-1.local:8080", reason="proxy_tunnel_error", source_name="company_site")
    reserved_snapshot = reserved_pool.lifecycle_snapshot(source_name="company_site", diagnostic=healthy)
    assert reserved_snapshot.state == runtime_proxies.PROXY_LIFECYCLE_SOURCE_CAPACITY_BLOCKED
    assert reserved_snapshot.runtime_global_usable_count == 1
    assert reserved_snapshot.runtime_source_usable_count == 0


def test_rate_limited_http_client_host_delay_does_not_block_unrelated_host(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    client, _proxy_pool = _build_client(tmp_path)
    client.min_delay_by_host.update({"slow.example": 30.0, "fast.example": 0.0})
    fake_now = {"value": 100.0}
    with client.lock:
        client.host_state["slow.example"].last_request_at = fake_now["value"]
    monkeypatch.setattr(core.time, "time", lambda: fake_now["value"])
    monkeypatch.setattr(core.random, "uniform", lambda _lower, _upper: 0.0)

    sleep_started = threading.Event()
    release_sleep = threading.Event()
    sleep_calls: list[float] = []

    def fake_sleep(seconds: float) -> None:
        sleep_calls.append(seconds)
        fake_now["value"] += seconds
        sleep_started.set()
        assert release_sleep.wait(timeout=2), "host delay sleep was not released"

    monkeypatch.setattr(core.time, "sleep", fake_sleep)

    request_urls: list[str] = []
    request_urls_lock = threading.Lock()

    def fake_get(url: str, **_kwargs: object) -> requests.Response:
        with request_urls_lock:
            request_urls.append(url)
        return _build_response(url)

    monkeypatch.setattr(client.session, "get", fake_get)
    outcomes: dict[str, core.RequestOutcome] = {}
    errors: list[BaseException] = []
    done = {
        "slow": threading.Event(),
        "same_host": threading.Event(),
        "fast": threading.Event(),
    }

    def run_request(name: str, url: str) -> None:
        try:
            outcomes[name] = client.request(url, source="company_site", timeout=20)
        except BaseException as exc:  # pragma: no cover - surfaced below with thread context
            errors.append(exc)
        finally:
            done[name].set()

    slow_thread = threading.Thread(target=run_request, args=("slow", "https://slow.example/first"))
    slow_thread.start()
    assert sleep_started.wait(timeout=2), "slow host request did not enter host delay sleep"

    same_host_thread = threading.Thread(target=run_request, args=("same_host", "https://slow.example/second"))
    same_host_thread.start()
    assert not done["same_host"].wait(timeout=0.1), "same-host request bypassed the host delay lock"

    fast_thread = threading.Thread(target=run_request, args=("fast", "https://fast.example/resource"))
    fast_thread.start()
    try:
        assert done["fast"].wait(timeout=1), "unrelated host request was blocked by another host delay sleep"
        assert not errors
        assert outcomes["fast"].ok
        with request_urls_lock:
            assert request_urls == ["https://fast.example/resource"]
    finally:
        release_sleep.set()
        slow_thread.join(timeout=2)
        same_host_thread.join(timeout=2)
        fast_thread.join(timeout=2)

    assert not slow_thread.is_alive()
    assert not same_host_thread.is_alive()
    assert not fast_thread.is_alive()
    assert not errors
    assert outcomes["slow"].ok
    assert outcomes["same_host"].ok
    assert sleep_calls[0] == pytest.approx(30.0)


def test_rate_limited_http_client_emits_host_min_delay_wait_event(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    client, _proxy_pool = _build_client(tmp_path)
    client.min_delay_by_host.update({"checko.ru": 5.0})
    fake_now = {"value": 100.0}
    with client.lock:
        client.host_state["checko.ru"].last_request_at = fake_now["value"]
    monkeypatch.setattr(core.time, "time", lambda: fake_now["value"])
    monkeypatch.setattr(core.random, "uniform", lambda _lower, _upper: 0.0)

    def fake_sleep(seconds: float) -> None:
        fake_now["value"] += seconds

    def fake_get(url: str, **_kwargs: object) -> requests.Response:
        return _build_response(url)

    monkeypatch.setattr(core.time, "sleep", fake_sleep)
    monkeypatch.setattr(client.session, "get", fake_get)

    outcome = client.request("https://checko.ru/search?query=7701234567", source="company_site", timeout=20)

    assert outcome.ok
    events = _read_events(client.progress_store.events_jsonl)
    assert [event["type"] for event in events] == ["host_min_delay_wait", "request_ok"]
    assert events[0]["host"] == "checko.ru"
    assert events[0]["source"] == "company_site"
    assert events[0]["wait_seconds"] == pytest.approx(5.0)
    assert events[0]["total_wait_seconds"] == pytest.approx(5.0)
    assert events[0]["min_delay_seconds"] == pytest.approx(5.0)


def test_rate_limited_http_client_marks_429_as_hard_lifecycle_block(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    client, _proxy_pool = _build_client(tmp_path)

    def fake_get(url: str, **_kwargs: object) -> requests.Response:
        return _build_response(url, status_code=429, text="Too Many Requests")

    monkeypatch.setattr(client.session, "get", fake_get)

    outcome = client.request("https://example.com/rate-limit", source="company_site", timeout=20)

    assert outcome.status == "rate_limited"
    events = _read_events(client.progress_store.events_jsonl)
    assert events[-1]["type"] == "rate_limited"
    assert events[-1]["proxy_lifecycle_state"] == runtime_proxies.PROXY_LIFECYCLE_TRUE_HARD_BLOCK
    assert events[-1]["proxy_lifecycle_failure_class"] == "rate_limited"
    assert events[-1]["proxy_lifecycle_recovery_class"] == "hard_block_not_globally_recoverable"


def _dedupe(values: object) -> list[str]:
    result: list[str] = []
    seen: set[str] = set()
    for value in values or []:
        text = str(value)
        if text in seen:
            continue
        seen.add(text)
        result.append(text)
    return result


def _build_site_auth_analyzer(client: object) -> SiteAuthenticityAnalyzer:
    helpers = SiteAuthHelpers(
        normalize_url=core.normalize_url,
        normalize_whitespace=core.normalize_whitespace,
        parse_title_and_meta=lambda soup: {"title": "", "description": ""},
        dedupe_preserve_order=_dedupe,
        extract_emails=lambda text: [],
        extract_phones=lambda text: [],
        extract_probable_addresses=lambda text: [],
        normalize_phone_values=lambda values: [],
        normalize_address_values=lambda values: [],
        normalize_phone_candidate=lambda value: "",
        company_tokens=lambda value: set(),
        normalized_phone_digits=lambda value: "",
        guess_registered_domain=lambda host: host,
        address_identity_tokens=lambda address: {"postals": set(), "tokens": set()},
        is_valid_russian_inn=lambda inn: True,
        keyword_found_in_text=lambda text, keyword: keyword.lower() in text.lower(),
        compact_text=lambda text, *args, **kwargs: str(text),
        summarize_source_context=lambda payload: payload,
        looks_like_bot_gate=lambda response, text: False,
        contact_path_hints=(),
        contact_link_text_hints=(),
        industrial_positive_keywords={},
        industrial_negative_keywords={},
        generic_email_domains=set(),
        company_token_stopwords=set(),
        activity_token_stopwords=set(),
        non_corporate_domains=set(),
    )
    return SiteAuthenticityAnalyzer(client=client, llm=object(), helpers=helpers)


@pytest.mark.parametrize(
    ("raised_exception", "expected_status", "expected_fragment"),
    [
        (AssertionError(b"ecolabteh.ru"), "request_error", "low-level URL/host assertion"),
        (ValueError("invalid host label"), "invalid_url", "invalid URL/host"),
    ],
)
def test_rate_limited_http_client_downgrades_low_level_url_boundary_errors(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    raised_exception: BaseException,
    expected_status: str,
    expected_fragment: str,
) -> None:
    client, proxy_pool = _build_client(tmp_path)

    def fake_get(*args, **kwargs):
        raise raised_exception

    monkeypatch.setattr(client.session, "get", fake_get)

    outcome = client.request("https://ecolabteh.ru", source="company_site", timeout=20)

    assert not outcome.ok
    assert outcome.status == expected_status
    assert expected_fragment in outcome.error
    events = _read_events(client.progress_store.events_jsonl)
    assert len(events) == 1
    assert events[0]["type"] == "request_error"
    assert events[0]["host"] == "ecolabteh.ru"
    assert events[0]["error"] == outcome.error
    if expected_status == "invalid_url":
        assert events[0]["request_status"] == "invalid_url"
    else:
        assert "request_status" not in events[0]
    assert proxy_pool.bad_marks == []


def test_rate_limited_http_client_retries_with_next_proxy_on_transient_proxy_failure(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    first_proxy = _proxy_selection("http://proxy-1.local:8080", label="proxy-1")
    second_proxy = _proxy_selection("http://proxy-2.local:8080", label="proxy-2")
    client, proxy_pool = _build_client(tmp_path, _ProxyPoolStub([first_proxy, second_proxy]))

    def fake_get(url: str, **kwargs: object) -> requests.Response:
        proxies = kwargs.get("proxies") or {}
        proxy_url = str((proxies or {}).get("https") or (proxies or {}).get("http") or "")
        if proxy_url == first_proxy.url:
            raise requests.exceptions.ProxyError("Cannot connect to proxy: connect timeout while opening tunnel")
        assert proxy_url == second_proxy.url
        return _build_response(url, text="ok via second proxy")

    monkeypatch.setattr(client.session, "get", fake_get)

    outcome = client.request("https://example.com/resource", source="company_site", timeout=20)

    assert outcome.ok
    assert outcome.status == "ok"
    assert outcome.proxy_mode == "proxy"
    assert outcome.proxy_label == second_proxy.label
    assert proxy_pool.bad_marks == [(first_proxy.url, "proxy_timeout")]
    assert proxy_pool.ok_marks == [second_proxy.url]
    assert proxy_pool.select_calls == ["example.com", "example.com"]
    events = _read_events(client.progress_store.events_jsonl)
    assert [event["type"] for event in events] == ["request_error", "request_ok"]
    assert events[0]["proxy_label"] == first_proxy.label
    assert events[1]["proxy_label"] == second_proxy.label


def test_rate_limited_http_client_returns_nonfatal_failure_when_all_proxies_fail(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    first_proxy = _proxy_selection("http://proxy-1.local:8080", label="proxy-1")
    second_proxy = _proxy_selection("http://proxy-2.local:8080", label="proxy-2")
    client, proxy_pool = _build_client(tmp_path, _ProxyPoolStub([first_proxy, second_proxy]))

    def fake_get(url: str, **kwargs: object) -> requests.Response:
        proxies = kwargs.get("proxies") or {}
        proxy_url = str((proxies or {}).get("https") or (proxies or {}).get("http") or "")
        if proxy_url == first_proxy.url:
            raise requests.exceptions.ProxyError("Cannot connect to proxy: connect timeout while opening tunnel")
        assert proxy_url == second_proxy.url
        raise requests.exceptions.ProxyError("Tunnel connection failed: 502 Bad Gateway")

    monkeypatch.setattr(client.session, "get", fake_get)

    outcome = client.request("https://example.com/resource", source="company_site", timeout=20)

    assert not outcome.ok
    assert outcome.status == "request_error"
    assert "Tunnel connection failed" in outcome.error
    assert outcome.proxy_mode == "proxy"
    assert outcome.proxy_label == second_proxy.label
    assert proxy_pool.bad_marks == [
        (first_proxy.url, "proxy_timeout"),
        (second_proxy.url, "proxy_tunnel_error"),
    ]
    assert proxy_pool.ok_marks == []
    assert proxy_pool.select_calls == ["example.com", "example.com"]
    events = _read_events(client.progress_store.events_jsonl)
    assert [event["type"] for event in events] == ["request_error", "request_error"]
    assert events[0]["proxy_label"] == first_proxy.label
    assert events[1]["proxy_label"] == second_proxy.label


def test_route_fetch_uses_direct_fallback_when_proxy6_provider_inventory_is_expired(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    expired_proxy = _proxy6_proxy("expired-1", active=False)
    proxy6_calls = _install_proxy6_diagnostic_client(
        monkeypatch,
        active=[],
        expired=[expired_proxy],
        all_items=[expired_proxy],
    )
    proxy_pool = ProxyPool("http://proxy-1.local:8080,http://proxy-2.local:8080")
    client, _proxy_pool = _build_client(tmp_path, proxy_pool)  # type: ignore[arg-type]
    session_get_calls: list[dict[str, object]] = []

    def fake_get(url: str, **kwargs: object) -> requests.Response:
        session_get_calls.append(dict(kwargs))
        assert kwargs.get("proxies") is None
        return _build_response(url, text="ok direct after provider guard")

    monkeypatch.setattr(client.session, "get", fake_get)

    first = client.request("https://factory.example/one", source="route_fetch", timeout=20)
    second = client.request("https://factory-two.example/two", source="route_fetch", timeout=20)

    assert first.ok
    assert second.ok
    assert first.proxy_mode == "direct"
    assert second.proxy_mode == "direct"
    assert len(session_get_calls) == 2
    assert all(entry.failures == 0 for entry in proxy_pool.entries)
    assert proxy6_calls.count("init") == 1
    events = _read_events(client.progress_store.events_jsonl)
    guard_events = [event for event in events if event["type"] == "request_proxy_provider_guardrail"]
    assert len(guard_events) == 2
    assert all(
        event["proxy_provider_status"] == runtime_proxy6.PROXY_PROVIDER_INVENTORY_EMPTY_OR_EXPIRED
        for event in guard_events
    )
    assert all(
        event["operator_action"] == runtime_proxy6.PROXY6_OPERATOR_ACTION_RENEW
        for event in guard_events
    )


def test_route_fetch_rotates_bad_proxy_when_proxy6_provider_inventory_has_active_proxy(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setenv("PARSER_PROXY_PRIORITY_RESERVED_CAPACITY", "0")
    active_proxy = _proxy6_proxy("active-1", active=True)
    expired_proxy = _proxy6_proxy("expired-1", active=False)
    _install_proxy6_diagnostic_client(
        monkeypatch,
        active=[active_proxy],
        expired=[expired_proxy],
        all_items=[active_proxy, expired_proxy],
    )
    proxy_pool = ProxyPool(
        "http://proxy-1.local:8080,http://proxy-2.local:8080",
        strategy="round_robin",
        ban_cooldown_seconds=60,
    )
    client, _proxy_pool = _build_client(tmp_path, proxy_pool)  # type: ignore[arg-type]
    attempted_proxy_urls: list[str] = []

    def fake_get(url: str, **kwargs: object) -> requests.Response:
        proxies = kwargs.get("proxies") or {}
        proxy_url = str((proxies or {}).get("https") or (proxies or {}).get("http") or "")
        attempted_proxy_urls.append(proxy_url)
        if proxy_url == "http://proxy-1.local:8080":
            raise requests.exceptions.ProxyError("Cannot connect to proxy: connect timeout while opening tunnel")
        assert proxy_url == "http://proxy-2.local:8080"
        return _build_response(url, text="ok via second proxy")

    monkeypatch.setattr(client.session, "get", fake_get)

    outcome = client.request("https://factory.example/resource", source="route_fetch", timeout=20)

    assert outcome.ok
    assert outcome.proxy_mode == "proxy"
    assert outcome.proxy_label == "proxy-2.local:8080"
    assert attempted_proxy_urls == ["http://proxy-1.local:8080", "http://proxy-2.local:8080"]
    events = _read_events(client.progress_store.events_jsonl)
    assert [event["type"] for event in events] == ["request_error", "request_ok"]
    assert not any(
        event.get("proxy_provider_status") == runtime_proxy6.PROXY_PROVIDER_INVENTORY_EMPTY_OR_EXPIRED
        for event in events
    )


def test_route_fetch_classifies_empty_runtime_pool_with_healthy_proxy6_provider_as_sync_repair(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    active_proxy = _proxy6_proxy("active-1", active=True)
    _install_proxy6_diagnostic_client(
        monkeypatch,
        active=[active_proxy],
        all_items=[active_proxy],
    )
    proxy_pool = ProxyPool("")
    client, _proxy_pool = _build_client(tmp_path, proxy_pool)  # type: ignore[arg-type]
    session_get_calls: list[dict[str, object]] = []

    def fake_get(url: str, **kwargs: object) -> requests.Response:
        session_get_calls.append(dict(kwargs))
        assert kwargs.get("proxies") is None
        return _build_response(url, text="ok direct while runtime pool awaits sync")

    monkeypatch.setattr(client.session, "get", fake_get)

    outcome = client.request("https://factory.example/resource", source="route_fetch", timeout=20)

    assert outcome.ok
    assert outcome.proxy_mode == "direct"
    assert len(session_get_calls) == 1
    events = _read_events(client.progress_store.events_jsonl)
    guard_event = next(event for event in events if event["type"] == "request_proxy_provider_guardrail")
    assert guard_event["proxy_provider_status"] == runtime_proxy6.PROXY_PROVIDER_INVENTORY_HEALTHY
    assert guard_event["proxy_provider_stop_class"] == runtime_proxy6.RUNTIME_PROXY_POOL_DEPLETED_PROVIDER_HEALTHY
    assert guard_event["operator_action"] == runtime_proxy6.PROXY6_OPERATOR_ACTION_REPAIR_RUNTIME_POOL


def test_bicotender_empty_runtime_pool_with_healthy_proxy6_provider_syncs_proxy_without_direct_fallback(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    active_proxy = _proxy6_proxy("active-1", active=True)
    reserve_proxy = _proxy6_proxy("active-2", active=True)
    _install_proxy6_diagnostic_client(
        monkeypatch,
        active=[active_proxy, reserve_proxy],
        all_items=[active_proxy, reserve_proxy],
    )
    proxy_pool = ProxyPool("")
    client, _proxy_pool = _build_client(tmp_path, proxy_pool)  # type: ignore[arg-type]
    session_get_calls: list[dict[str, object]] = []

    def fake_get(url: str, **kwargs: object) -> requests.Response:
        session_get_calls.append(dict(kwargs))
        proxies = kwargs.get("proxies") or {}
        proxy_url = str((proxies or {}).get("https") or (proxies or {}).get("http") or "")
        assert proxy_url == active_proxy.as_url()
        return _build_response(url, text="ok via synced proxy")

    monkeypatch.setattr(client.session, "get", fake_get)

    outcome = client.request(
        "https://www.bicotender.ru/tender/search/?company%5Binn%5D=7707083893",
        source="bicotender",
        timeout=20,
    )

    assert outcome.ok
    assert outcome.proxy_mode == "proxy"
    assert outcome.proxy_label == "proxy-active-1.example:8080"
    assert len(session_get_calls) == 1
    assert proxy_pool.usable_count(source_name="bicotender") == 1
    events = _read_events(client.progress_store.events_jsonl)
    assert [event["type"] for event in events] == ["request_proxy_pool_sync", "request_ok"]
    assert events[0]["source"] == "bicotender"
    assert events[0]["proxy_provider_status"] == runtime_proxy6.PROXY_PROVIDER_INVENTORY_HEALTHY
    assert events[0]["proxy_provider_stop_class"] == runtime_proxy6.RUNTIME_PROXY_POOL_DEPLETED_PROVIDER_HEALTHY
    assert events[0]["operator_action"] == runtime_proxy6.PROXY6_OPERATOR_ACTION_REPAIR_RUNTIME_POOL
    assert events[0]["proxy_sync_status"] == "restored"
    assert events[0]["proxy_sync_added_count"] == 2
    assert events[1]["proxy_mode"] == "proxy"


def test_bicotender_provider_sync_does_not_clear_existing_hard_proxy_cooldown(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setenv("PARSER_PROXY_PRIORITY_RESERVED_CAPACITY", "0")
    active_proxy = _proxy6_proxy("active-1", active=True)
    _install_proxy6_diagnostic_client(
        monkeypatch,
        active=[active_proxy],
        all_items=[active_proxy],
    )
    proxy_pool = ProxyPool(active_proxy.as_url(), strategy="round_robin", ban_cooldown_seconds=300)
    proxy_pool.mark_bad(active_proxy.as_url(), reason="http_403", source_name="bicotender")
    assert proxy_pool.usable_count(source_name="bicotender") == 0
    client, _proxy_pool = _build_client(tmp_path, proxy_pool)  # type: ignore[arg-type]
    session_get_calls: list[tuple[tuple[object, ...], dict[str, object]]] = []

    def fail_if_called(*args: object, **kwargs: object) -> requests.Response:
        session_get_calls.append((args, dict(kwargs)))
        raise RuntimeError("bicotender must not dispatch when provider sync cannot restore a usable proxy")

    monkeypatch.setattr(client.session, "get", fail_if_called)

    outcome = client.request(
        "https://www.bicotender.ru/tender/search/?company%5Binn%5D=7707083893",
        source="bicotender",
        timeout=20,
    )

    assert session_get_calls == []
    assert not outcome.ok
    assert outcome.status == core.REQUEST_STATUS_BLOCKED_NO_PROXY
    assert outcome.proxy_mode == "blocked_no_proxy"
    assert "proxy_sync_status=no_new_usable_proxy" in outcome.error
    assert "proxy_provider_status=proxy_provider_inventory_healthy" in outcome.error
    events = _read_events(client.progress_store.events_jsonl)
    assert [event["type"] for event in events] == ["request_proxy_pool_sync", "request_blocked_by_policy"]
    assert events[0]["proxy_sync_status"] == "no_new_usable_proxy"
    assert events[0]["proxy_sync_restored"] is False
    assert events[1]["proxy_sync_status"] == "no_new_usable_proxy"
    assert events[1]["proxy_provider_stop_class"] == runtime_proxy6.RUNTIME_PROXY_POOL_DEPLETED_PROVIDER_HEALTHY


def test_checko_provider_healthy_known_cooled_runtime_pool_fails_closed_with_lifecycle_state(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setenv("PARSER_PROXY_PRIORITY_RESERVED_CAPACITY", "0")
    active_proxy = _proxy6_proxy("active-1", active=True)
    _install_proxy6_diagnostic_client(
        monkeypatch,
        active=[active_proxy],
        all_items=[active_proxy],
    )
    proxy_pool = ProxyPool(active_proxy.as_url(), strategy="round_robin", ban_cooldown_seconds=300)
    proxy_pool.mark_bad(active_proxy.as_url(), reason="proxy_tunnel_error", source_name="checko")
    assert proxy_pool.usable_count(source_name="checko") == 0
    client, _proxy_pool = _build_client(tmp_path, proxy_pool)  # type: ignore[arg-type]
    session_get_calls: list[tuple[tuple[object, ...], dict[str, object]]] = []

    def fail_if_called(*args: object, **kwargs: object) -> requests.Response:
        session_get_calls.append((args, dict(kwargs)))
        raise RuntimeError("checko must fail closed before outbound when known proxies remain cooled")

    monkeypatch.setattr(client.session, "get", fail_if_called)

    outcome = client.request("https://checko.ru/search?query=7701234567", source="checko", timeout=20)

    assert session_get_calls == []
    assert not outcome.ok
    assert outcome.status == core.REQUEST_STATUS_BLOCKED_NO_PROXY
    assert "proxy_sync_status=no_new_usable_proxy" in outcome.error
    assert "proxy_lifecycle_state=runtime_proxy_pool_depleted_provider_healthy" in outcome.error
    assert "proxy_lifecycle_subreason=same_url_provider_sync_noop_runtime_known_but_ineligible" in outcome.error
    events = _read_events(client.progress_store.events_jsonl)
    assert [event["type"] for event in events] == ["request_proxy_pool_sync", "request_blocked_by_policy"]
    assert events[0]["proxy_sync_status"] == "no_new_usable_proxy"
    assert events[0]["proxy_lifecycle_state"] == runtime_proxies.PROXY_LIFECYCLE_RUNTIME_DEPLETED_PROVIDER_HEALTHY
    assert events[0]["proxy_lifecycle_subreason"] == "same_url_provider_sync_noop_runtime_known_but_ineligible"
    assert events[1]["source"] == "checko"
    assert events[1]["proxy_provider_status"] == runtime_proxy6.PROXY_PROVIDER_INVENTORY_HEALTHY
    assert events[1]["proxy_lifecycle_state"] == runtime_proxies.PROXY_LIFECYCLE_RUNTIME_DEPLETED_PROVIDER_HEALTHY
    assert events[1]["operator_action"] == runtime_proxy6.PROXY6_OPERATOR_ACTION_REPAIR_RUNTIME_POOL


def test_checko_stays_blocked_no_proxy_when_proxy6_provider_inventory_is_expired(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    expired_proxy = _proxy6_proxy("expired-1", active=False)
    _install_proxy6_diagnostic_client(
        monkeypatch,
        active=[],
        expired=[expired_proxy],
        all_items=[expired_proxy],
    )
    proxy_pool = ProxyPool("http://proxy-1.local:8080")
    client, _proxy_pool = _build_client(tmp_path, proxy_pool)  # type: ignore[arg-type]
    session_get_calls: list[tuple[tuple[object, ...], dict[str, object]]] = []

    def fail_if_called(*args: object, **kwargs: object) -> requests.Response:
        session_get_calls.append((args, dict(kwargs)))
        raise RuntimeError("checko must not fall back to direct transport")

    monkeypatch.setattr(client.session, "get", fail_if_called)

    outcome = client.request("https://checko.ru/search?query=7701234567", source="checko", timeout=20)

    assert session_get_calls == []
    assert not outcome.ok
    assert outcome.status == core.REQUEST_STATUS_BLOCKED_NO_PROXY
    assert "proxy_provider_status=proxy_provider_inventory_empty_or_expired" in outcome.error
    events = _read_events(client.progress_store.events_jsonl)
    assert len(events) == 1
    assert events[0]["type"] == "request_blocked_by_policy"
    assert events[0]["source"] == "checko"
    assert events[0]["proxy_provider_status"] == runtime_proxy6.PROXY_PROVIDER_INVENTORY_EMPTY_OR_EXPIRED
    assert events[0]["operator_action"] == runtime_proxy6.PROXY6_OPERATOR_ACTION_RENEW


def test_rate_limited_http_client_rehabilitates_successful_proxy_before_checko_failover(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    proxy_pool = ProxyPool(
        "http://proxy-1.local:8080,http://proxy-2.local:8080",
        strategy="round_robin",
        ban_cooldown_seconds=300,
    )
    client = core.RateLimitedHttpClient(
        logger=logging.getLogger("tests.request_boundary_urlguard"),
        progress_store=core.ProgressStore(tmp_path / "progress"),
        min_delay_by_host={"checko.ru": 0.0},
        request_timeout=5,
        cooldown_on_429=60,
        cooldown_on_bot=60,
        proxy_pool=proxy_pool,
    )
    fake_now = {"value": 0.0}
    monkeypatch.setattr(core.time, "time", lambda: fake_now["value"])
    monkeypatch.setattr(runtime_proxies.time, "time", lambda: fake_now["value"])

    first_entry = proxy_pool.entries[0]
    second_entry = proxy_pool.entries[1]
    first_proxy = _proxy_selection(first_entry.url, label=first_entry.label)
    second_proxy = _proxy_selection(second_entry.url, label=second_entry.label)

    fake_now["value"] = 0.0
    proxy_pool.mark_bad(first_proxy.url, reason="proxy_tunnel_error", source_name="company_site")
    fake_now["value"] = 60.0
    proxy_pool.mark_bad(first_proxy.url, reason="proxy_tunnel_error", source_name="company_site")
    fake_now["value"] = 61.0
    proxy_pool.mark_ok(first_proxy.url, source_name="zachestnyibiznes")

    assert proxy_pool.usable_count(source_name="checko") == 2

    def fake_get(url: str, **kwargs: object) -> requests.Response:
        proxies = kwargs.get("proxies") or {}
        proxy_url = str((proxies or {}).get("https") or (proxies or {}).get("http") or "")
        if proxy_url == second_proxy.url:
            raise requests.exceptions.ConnectionError(
                "HTTPSConnectionPool(host='checko.ru', port=443): Max retries exceeded with "
                "url: /search?query=7701234567 (Caused by ProxyError('Unable to connect to proxy', "
                "RemoteDisconnected('Remote end closed connection without response')))"
            )
        assert proxy_url == first_proxy.url
        return _build_response(url, text="ok via rehabilitated proxy")

    monkeypatch.setattr(client.session, "get", fake_get)
    fake_now["value"] = 100.0

    outcome = client.request(
        "https://checko.ru/search?query=7701234567",
        source="checko",
        timeout=20,
        proxy_selection=second_proxy,
    )

    assert outcome.ok
    assert outcome.status == "ok"
    assert outcome.proxy_mode == "proxy"
    assert outcome.proxy_label == first_proxy.label
    events = _read_events(client.progress_store.events_jsonl)
    assert [event["type"] for event in events] == ["request_error", "request_ok"]
    assert events[0]["proxy_label"] == second_proxy.label
    assert events[1]["proxy_label"] == first_proxy.label


def test_rate_limited_http_client_keeps_checko_request_error_when_all_proxies_remain_quarantined(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    proxy_pool = ProxyPool(
        "http://proxy-1.local:8080,http://proxy-2.local:8080",
        strategy="round_robin",
        ban_cooldown_seconds=300,
    )
    client = core.RateLimitedHttpClient(
        logger=logging.getLogger("tests.request_boundary_urlguard"),
        progress_store=core.ProgressStore(tmp_path / "progress"),
        min_delay_by_host={"checko.ru": 0.0},
        request_timeout=5,
        cooldown_on_429=60,
        cooldown_on_bot=60,
        proxy_pool=proxy_pool,
    )
    fake_now = {"value": 0.0}
    monkeypatch.setattr(core.time, "time", lambda: fake_now["value"])
    monkeypatch.setattr(runtime_proxies.time, "time", lambda: fake_now["value"])

    first_entry = proxy_pool.entries[0]
    second_entry = proxy_pool.entries[1]
    first_proxy = _proxy_selection(first_entry.url, label=first_entry.label)
    second_proxy = _proxy_selection(second_entry.url, label=second_entry.label)

    fake_now["value"] = 0.0
    proxy_pool.mark_bad(first_proxy.url, reason="proxy_tunnel_error", source_name="company_site")
    fake_now["value"] = 60.0
    proxy_pool.mark_bad(first_proxy.url, reason="proxy_tunnel_error", source_name="company_site")

    assert proxy_pool.usable_count(source_name="checko") == 1

    def fake_get(_url: str, **_kwargs: object) -> requests.Response:
        raise requests.exceptions.ConnectionError(
            "HTTPSConnectionPool(host='checko.ru', port=443): Max retries exceeded with "
            "url: /search?query=7701234567 (Caused by ProxyError('Unable to connect to proxy', "
            "RemoteDisconnected('Remote end closed connection without response')))"
        )

    monkeypatch.setattr(client.session, "get", fake_get)
    fake_now["value"] = 100.0

    outcome = client.request(
        "https://checko.ru/search?query=7701234567",
        source="checko",
        timeout=20,
        proxy_selection=second_proxy,
    )

    assert not outcome.ok
    assert outcome.status == "request_error"
    assert "Remote end closed connection without response" in outcome.error
    events = _read_events(client.progress_store.events_jsonl)
    assert [event["type"] for event in events] == ["request_error"]
    assert events[0]["proxy_label"] == second_proxy.label


def test_site_authenticity_surface_keeps_nonfatal_request_status_after_boundary_assertion(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    client, _proxy_pool = _build_client(tmp_path)

    def fake_get(*args, **kwargs):
        raise AssertionError(b"ecolabteh.ru")

    monkeypatch.setattr(client.session, "get", fake_get)
    analyzer = _build_site_auth_analyzer(client)

    decision = analyzer.analyze_surface(
        SimpleNamespace(company_name="АО Эколабтех", inn="1234567890"),
        "https://ecolabteh.ru",
        {"phones": [], "emails": [], "websites": [], "addresses": []},
        {},
    )

    assert decision.status == "request_error"
    assert decision.decision_status == "suspicious"
    assert any("low-level URL/host assertion" in error for error in decision.errors)

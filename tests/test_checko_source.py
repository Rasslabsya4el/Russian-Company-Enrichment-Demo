from __future__ import annotations

from collections.abc import Callable
import json
import logging
import threading
import time
from types import SimpleNamespace
from urllib.parse import urlparse

import pytest
import requests

import company_enrichment_core as core
import run_company_enrichment_pipeline as pipeline
import app.runtime.proxy6 as runtime_proxy6
from app.runtime import ProxyPool
from app.runtime.bounded_executor import plan_direct_default_bounded_executor
from app.runtime.concurrency import (
    DIRECT_DEFAULT_TRANSPORT,
    OFFLINE_ONLY_TRANSPORT,
    PROXY_REQUIRED_TRANSPORT,
    SESSION_BOUND_TRANSPORT,
    SourceExecutionGuardrails,
    build_source_execution_guardrails,
)
from app.runtime.queue_families import (
    build_aggregator_site_queue_family_contour,
    build_downstream_worker_pool_contour,
    build_deep_parse_queue_family_contour,
)
from app.sources import checko


@pytest.fixture(autouse=True)
def _clear_proxy6_inventory_cache(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("PROXY6_API_KEY", raising=False)
    monkeypatch.delenv("PROXY6_DIAGNOSTIC_CACHE_TTL_SECONDS", raising=False)
    runtime_proxy6.clear_proxy6_inventory_diagnostic_cache()
    yield
    runtime_proxy6.clear_proxy6_inventory_diagnostic_cache()


class _RecordingSource:
    def __init__(
        self,
        source_name: str,
        calls: list[str],
        result_factory: Callable[[core.RowInput, int], core.SourceResult] | None = None,
    ) -> None:
        self.source_name = source_name
        self._calls = calls
        self._result_factory = result_factory
        self._call_count = 0

    def search(self, row: core.RowInput) -> core.SourceResult:
        self._calls.append(self.source_name)
        self._call_count += 1
        if self._result_factory is not None:
            return self._result_factory(row, self._call_count)
        return core.SourceResult(source=self.source_name, status="success")


class _ConcurrencyTracker:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._inflight = 0
        self.max_inflight = 0

    def enter(self) -> None:
        with self._lock:
            self._inflight += 1
            self.max_inflight = max(self.max_inflight, self._inflight)

    def leave(self) -> None:
        with self._lock:
            self._inflight -= 1


class _ConcurrencyProbeSource:
    def __init__(
        self,
        source_name: str,
        tracker: _ConcurrencyTracker,
        *,
        sleep_by_row_index: dict[int, float],
        on_search_complete: Callable[[core.RowInput, str], None] | None = None,
    ) -> None:
        self.source_name = source_name
        self._tracker = tracker
        self._sleep_by_row_index = dict(sleep_by_row_index)
        self._on_search_complete = on_search_complete

    def search(self, row: core.RowInput) -> core.SourceResult:
        self._tracker.enter()
        try:
            time.sleep(self._sleep_by_row_index.get(row.row_index, 0.01))
            return core.SourceResult(source=self.source_name, status="success")
        finally:
            if self._on_search_complete is not None:
                self._on_search_complete(row, self.source_name)
            self._tracker.leave()


class _ThreadRecordingSource:
    def __init__(
        self,
        source_name: str,
        thread_names: dict[str, list[str]],
        *,
        sleep_by_row_index: dict[int, float] | None = None,
        tracker: _ConcurrencyTracker | None = None,
    ) -> None:
        self.source_name = source_name
        self._thread_names = thread_names
        self._sleep_by_row_index = dict(sleep_by_row_index or {})
        self._tracker = tracker

    def search(self, row: core.RowInput) -> core.SourceResult:
        if self._tracker is not None:
            self._tracker.enter()
        try:
            self._thread_names.setdefault(self.source_name, []).append(threading.current_thread().name)
            time.sleep(self._sleep_by_row_index.get(row.row_index, 0.01))
            return core.SourceResult(source=self.source_name, status="success")
        finally:
            if self._tracker is not None:
                self._tracker.leave()


class _BrokenProxyPool:
    def __init__(self, *, usable_count: int, selection: core.ProxySelection | None = None) -> None:
        self._usable_count = usable_count
        self._selection = selection if selection is not None else core.ProxySelection()
        self.entries = [SimpleNamespace(url="http://proxy1.example:8080")] if usable_count > 0 else []

    def usable_count(self, *, source_name: str | None = None) -> int:
        return self._usable_count

    def select(self, _host: str | None = None, *, source_name: str | None = None) -> core.ProxySelection:
        return self._selection

    def mark_bad(self, _proxy_url: str | None, *, reason: str = "", source_name: str | None = None) -> None:
        return None

    def mark_ok(self, _proxy_url: str | None, *, source_name: str | None = None) -> None:
        return None


def _proxy6_proxy(
    proxy_id: str,
    *,
    active: bool,
    country: str = "ru",
    descr: str = "parser_pool",
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


class _Proxy6DiagnosticClient:
    def __init__(
        self,
        *,
        active: list[runtime_proxy6.Proxy6Proxy] | None = None,
        expired: list[runtime_proxy6.Proxy6Proxy] | None = None,
        all_items: list[runtime_proxy6.Proxy6Proxy] | None = None,
        error: Exception | None = None,
    ) -> None:
        self.active = list(active or [])
        self.expired = list(expired or [])
        self.all_items = list(all_items or self.active + self.expired)
        self.error = error

    def get_account(self) -> dict[str, object]:
        if self.error is not None:
            raise self.error
        return {"balance": "0.00", "currency": "USD"}

    def get_all_proxies(self, *, state: str, **_kwargs: object) -> list[runtime_proxy6.Proxy6Proxy]:
        if state == "active":
            return list(self.active)
        if state == "expired":
            return list(self.expired)
        if state == "all":
            return list(self.all_items)
        return []


def _rows(count: int) -> list[core.RowInput]:
    return [
        core.RowInput(
            row_index=index + 1,
            inn=f"{index:010d}",
            company_name=f"Company {index}",
        )
        for index in range(1, count + 1)
    ]


def _install_lightweight_run_stubs(
    monkeypatch: pytest.MonkeyPatch,
    *,
    rows: list[core.RowInput],
    source_calls: list[str],
    client_kwargs: dict[str, object],
    source_result_factories: dict[str, Callable[[core.RowInput, int], core.SourceResult]] | None = None,
) -> None:
    def fake_rate_limited_http_client(**kwargs):
        client_kwargs.update(kwargs)
        client = SimpleNamespace(progress_store=kwargs["progress_store"], min_delay_by_host=kwargs["min_delay_by_host"])
        client_kwargs["client_instance"] = client
        return client

    def fake_gated_parse(**kwargs):
        return SimpleNamespace(
            validated_sites=[],
            notes=[],
            parsed_factory_sites=SimpleNamespace(
                site_probes=[],
                route_strategies=[],
                content_records=[],
                notes=[],
            ),
        )

    def make_source(source_name: str):
        result_factory = (source_result_factories or {}).get(source_name)
        return lambda *_args, **_kwargs: _RecordingSource(source_name, source_calls, result_factory)

    monkeypatch.setattr(pipeline.core, "load_env_file", lambda _path: None)
    monkeypatch.setattr(pipeline.core, "load_rows_from_xlsx", lambda _path: rows)
    monkeypatch.setattr(pipeline.core, "RateLimitedHttpClient", fake_rate_limited_http_client)
    monkeypatch.setattr(core.ProgressStore, "_write_markdown_reports", lambda self, ordered_results=None: None)
    monkeypatch.setattr(pipeline, "ProxyPool", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(pipeline, "SparkSource", make_source("spark"))
    monkeypatch.setattr(pipeline, "ZachestnyBiznesSource", make_source("zachestnyibiznes"))
    monkeypatch.setattr(pipeline, "RusprofileSource", make_source("rusprofile"))
    monkeypatch.setattr(pipeline, "CheckoSource", make_source("checko"))
    monkeypatch.setattr(pipeline, "ListOrgSource", lambda *_args, **_kwargs: _RecordingSource("list_org", source_calls))
    monkeypatch.setattr(pipeline, "FactorySiteParser", lambda *_args, **_kwargs: SimpleNamespace())
    monkeypatch.setattr(pipeline, "SiteAuthHelpers", lambda **_kwargs: SimpleNamespace())
    monkeypatch.setattr(
        pipeline,
        "BenchmarkAwareSiteAuthenticityAnalyzer",
        lambda *_args, **_kwargs: SimpleNamespace(llm=SimpleNamespace()),
    )
    monkeypatch.setattr(pipeline, "build_domain_resolution", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(pipeline, "choose_candidate_sites", lambda *_args, **_kwargs: [])
    monkeypatch.setattr(pipeline, "run_gated_factory_site_parse", fake_gated_parse, raising=False)
    monkeypatch.setattr(pipeline, "classify_content_record", lambda _record: None)
    monkeypatch.setattr(pipeline, "should_use_llm_record_review", lambda _record: False)
    monkeypatch.setattr(pipeline, "build_and_store_company_dossier", lambda **_kwargs: {})
    monkeypatch.setattr(pipeline.core, "build_analysis_contacts", lambda *_args, **_kwargs: {})
    monkeypatch.setattr(pipeline.core, "merge_contacts", lambda *_args, **_kwargs: {})
    monkeypatch.setattr(pipeline.core, "build_trusted_contacts", lambda *_args, **_kwargs: {})
    monkeypatch.setattr(pipeline.core, "build_lead_cards", lambda *_args, **_kwargs: [])
    monkeypatch.setattr(pipeline.core, "build_site_refresh_plans", lambda *_args, **_kwargs: [])


def test_proxy6_inventory_diagnostic_classifies_expired_or_empty_parser_pool() -> None:
    expired_proxy = _proxy6_proxy("expired-1", active=False)
    diagnostic = runtime_proxy6.diagnose_proxy6_inventory(
        _Proxy6DiagnosticClient(active=[], expired=[expired_proxy], all_items=[expired_proxy]),
        parser_country="ru",
        parser_descr="parser_pool",
    )

    assert diagnostic.provider_status == runtime_proxy6.PROXY_PROVIDER_INVENTORY_EMPTY_OR_EXPIRED
    assert diagnostic.runtime_stop_class == runtime_proxy6.PROXY_PROVIDER_INVENTORY_EMPTY_OR_EXPIRED
    assert diagnostic.operator_action == runtime_proxy6.PROXY6_OPERATOR_ACTION_RENEW
    assert diagnostic.active_usable_count == 0
    assert diagnostic.expired_count == 1
    assert "operator_action=top_up_or_renew_proxy_subscription" in diagnostic.operator_message()


def test_proxy6_inventory_diagnostic_marks_provider_unavailable_as_unknown() -> None:
    diagnostic = runtime_proxy6.diagnose_proxy6_inventory(
        _Proxy6DiagnosticClient(
            error=runtime_proxy6.Proxy6ApiError(
                "authentication failed",
                error_id=100,
                status_code=403,
            )
        ),
        parser_country="ru",
    )

    assert diagnostic.provider_status == runtime_proxy6.PROXY_PROVIDER_STATUS_UNKNOWN
    assert diagnostic.runtime_stop_class == runtime_proxy6.PROXY_PROVIDER_STATUS_UNKNOWN
    assert diagnostic.operator_action == runtime_proxy6.PROXY6_OPERATOR_ACTION_CONFIGURE_OR_SYNC
    assert diagnostic.error_id == 100
    assert diagnostic.status_code == 403


def test_proxy6_inventory_diagnostic_marks_healthy_provider_as_runtime_pool_depletion() -> None:
    active_proxy = _proxy6_proxy("active-1", active=True)
    diagnostic = runtime_proxy6.diagnose_proxy6_inventory(
        _Proxy6DiagnosticClient(active=[active_proxy], all_items=[active_proxy]),
        parser_country="ru",
    )

    assert diagnostic.provider_status == runtime_proxy6.PROXY_PROVIDER_INVENTORY_HEALTHY
    assert diagnostic.runtime_stop_class == runtime_proxy6.RUNTIME_PROXY_POOL_DEPLETED_PROVIDER_HEALTHY
    assert diagnostic.operator_action == runtime_proxy6.PROXY6_OPERATOR_ACTION_REPAIR_RUNTIME_POOL
    assert diagnostic.active_usable_count == 1


def test_detect_checko_access_boundary_marks_rate_limited_on_http_429() -> None:
    assert checko.detect_checko_access_boundary(response_status=429) == (
        "rate_limited",
        f"{checko.CHECKO_RF_PROXY_REQUIRED_REASON} (HTTP 429 Too Many Requests)",
    )


@pytest.mark.parametrize(
    ("kwargs", "expected_reason"),
    [
        (
            {"response_status": 403},
            f"{checko.CHECKO_RF_PROXY_REQUIRED_REASON} (HTTP 403 Forbidden)",
        ),
        (
            {
                "html": (
                    "<html><body>"
                    "\u0414\u043e\u0441\u0442\u0443\u043f \u043e\u0433\u0440\u0430\u043d\u0438\u0447\u0435\u043d, "
                    "\u0442\u043e\u043b\u044c\u043a\u043e \u0434\u043b\u044f "
                    "\u043f\u043e\u043b\u044c\u0437\u043e\u0432\u0430\u0442\u0435\u043b\u0435\u0439 "
                    "\u0438\u0437 \u0420\u043e\u0441\u0441\u0438\u0438"
                    "</body></html>"
                ),
            },
            checko.CHECKO_RF_PROXY_REQUIRED_REASON,
        ),
    ],
)
def test_detect_checko_access_boundary_marks_blocked_on_forbidden_or_access_gated_text(
    kwargs: dict[str, object],
    expected_reason: str,
) -> None:
    assert checko.detect_checko_access_boundary(**kwargs) == ("blocked", expected_reason)


def test_resolve_checko_listing_entity_handles_direct_card() -> None:
    row = core.RowInput(row_index=1, inn="7701234567", company_name="Test")
    html = (
        "<html><head><title>\u041e\u041e\u041e \"\u0422\u0415\u0421\u0422\" - \u0418\u041d\u041d 7701234567</title></head>"
        "<body>"
        "<h1>\u041e\u041e\u041e \"\u0422\u0415\u0421\u0422\"</h1>"
        "<div>\u0418\u041d\u041d 7701234567</div>"
        "<div>\u041e\u0413\u0420\u041d 1234567890123</div>"
        "<div>\u041a\u041f\u041f 770101001</div>"
        "<div>\u042e\u0440\u0438\u0434\u0438\u0447\u0435\u0441\u043a\u0438\u0439 \u0430\u0434\u0440\u0435\u0441: \u0433. \u041c\u043e\u0441\u043a\u0432\u0430</div>"
        "<div>\u041a\u043e\u043d\u0442\u0430\u043a\u0442\u044b</div>"
        "</body></html>"
    )

    resolution = checko.resolve_checko_listing_entity(
        row,
        listing_url="https://checko.ru/search?query=7701234567",
        html=html,
    )

    assert resolution.status == "resolved"
    assert resolution.entity_url == "https://checko.ru/search?query=7701234567"
    assert resolution.note.endswith("\u0418\u041d\u041d 7701234567")


def test_resolve_checko_listing_entity_handles_exact_inn_candidate() -> None:
    row = core.RowInput(row_index=1, inn="7701234567", company_name="Test")
    html = (
        "<html><body>"
        "<div><a href=\"/company/123\">\u041e\u041e\u041e \u0422\u0415\u0421\u0422</a>"
        "<span>\u0418\u041d\u041d 7701234567</span></div>"
        "<div><a href=\"/company/999\">\u041e\u041e\u041e \u0414\u0420\u0423\u0413\u041e\u0419</a>"
        "<span>\u0418\u041d\u041d 7707654321</span></div>"
        "</body></html>"
    )

    resolution = checko.resolve_checko_listing_entity(
        row,
        listing_url="https://checko.ru/search?query=7701234567",
        html=html,
    )

    assert resolution.status == "resolved"
    assert resolution.entity_url == "https://checko.ru/company/123"
    assert resolution.note.endswith("\u0418\u041d\u041d 7701234567")


def test_resolve_checko_listing_entity_handles_not_found_marker() -> None:
    row = core.RowInput(row_index=1, inn="7701234567", company_name="Test")
    html = (
        "<html><body>"
        "\u041f\u043e \u0432\u0430\u0448\u0435\u043c\u0443 \u0437\u0430\u043f\u0440\u043e\u0441\u0443 "
        "\u043d\u0438\u0447\u0435\u0433\u043e \u043d\u0435 \u043d\u0430\u0439\u0434\u0435\u043d\u043e"
        "</body></html>"
    )

    resolution = checko.resolve_checko_listing_entity(
        row,
        listing_url="https://checko.ru/search?query=7701234567",
        html=html,
    )

    assert resolution.status == "not_found"
    assert resolution.entity_url == ""
    assert "7701234567" in resolution.note


def test_parse_checko_company_html_extracts_contacts_okved_and_availability() -> None:
    html = (
        "<html><head>"
        "<title>\u041e\u041e\u041e \"\u0422\u0415\u0421\u0422\" - \u0418\u041d\u041d 7701234567</title>"
        "<meta name=\"description\" content=\"\u041f\u0440\u043e\u0444\u0438\u043b\u044c \u043a\u043e\u043c\u043f\u0430\u043d\u0438\u0438\" />"
        "</head><body>"
        "<h1>\u041e\u041e\u041e \"\u0422\u0415\u0421\u0422\"</h1>"
        "<div>\u041e\u0431\u0449\u0435\u0441\u0442\u0432\u043e \u0441 "
        "\u043e\u0433\u0440\u0430\u043d\u0438\u0447\u0435\u043d\u043d\u043e\u0439 "
        "\u043e\u0442\u0432\u0435\u0442\u0441\u0442\u0432\u0435\u043d\u043d\u043e\u0441\u0442\u044c\u044e "
        "\"\u0422\u0415\u0421\u0422\"</div>"
        "<div>\u042e\u0440\u0438\u0434\u0438\u0447\u0435\u0441\u043a\u0438\u0439 \u0430\u0434\u0440\u0435\u0441</div>"
        "<div>123456, \u0433. \u041c\u043e\u0441\u043a\u0432\u0430, \u0443\u043b. \u041b\u0435\u043d\u0438\u043d\u0430, \u0434. 1</div>"
        "<h2>\u041a\u043e\u043d\u0442\u0430\u043a\u0442\u044b</h2>"
        "<div>\u0422\u0435\u043b\u0435\u0444\u043e\u043d: +7 (495) 123-45-67</div>"
        "<div>E-mail: info@test.ru</div>"
        "<div>\u0421\u0430\u0439\u0442: www.test.ru</div>"
        "<h2>\u0412\u0438\u0434\u044b \u0434\u0435\u044f\u0442\u0435\u043b\u044c\u043d\u043e\u0441\u0442\u0438 \u041e\u041a\u0412\u042d\u0414</h2>"
        "<div>46.90 \u0422\u043e\u0440\u0433\u043e\u0432\u043b\u044f \u043e\u043f\u0442\u043e\u0432\u0430\u044f "
        "\u043d\u0435\u0441\u043f\u0435\u0446\u0438\u0430\u043b\u0438\u0437\u0438\u0440\u043e\u0432\u0430\u043d\u043d\u0430\u044f</div>"
        "<div>46.69 \u0422\u043e\u0440\u0433\u043e\u0432\u043b\u044f \u043e\u043f\u0442\u043e\u0432\u0430\u044f "
        "\u043f\u0440\u043e\u0447\u0438\u043c\u0438 \u043c\u0430\u0448\u0438\u043d\u0430\u043c\u0438 "
        "\u0438 \u043e\u0431\u043e\u0440\u0443\u0434\u043e\u0432\u0430\u043d\u0438\u0435\u043c</div>"
        "<h2>\u0413\u0435\u043d\u0435\u0440\u0430\u043b\u044c\u043d\u044b\u0439 \u0434\u0438\u0440\u0435\u043a\u0442\u043e\u0440</h2>"
        "<div>\u0418\u0432\u0430\u043d\u043e\u0432 \u0418\u0432\u0430\u043d \u0418\u0432\u0430\u043d\u043e\u0432\u0438\u0447</div>"
        "<h2>\u0423\u0447\u0440\u0435\u0434\u0438\u0442\u0435\u043b\u0438</h2>"
        "<div>\u041f\u0435\u0442\u0440\u043e\u0432 \u041f\u0435\u0442\u0440 \u041f\u0435\u0442\u0440\u043e\u0432\u0438\u0447</div>"
        "</body></html>"
    )

    payload = checko.parse_checko_company_html("https://checko.ru/company/test", html)

    assert payload["company_name"] == (
        "\u041e\u0431\u0449\u0435\u0441\u0442\u0432\u043e \u0441 "
        "\u043e\u0433\u0440\u0430\u043d\u0438\u0447\u0435\u043d\u043d\u043e\u0439 "
        "\u043e\u0442\u0432\u0435\u0442\u0441\u0442\u0432\u0435\u043d\u043d\u043e\u0441\u0442\u044c\u044e "
        "\"\u0422\u0415\u0421\u0422\""
    )
    assert [item.value for item in payload["phones"]] == ["+7 495 123-45-67"]
    assert [item.value for item in payload["emails"]] == ["info@test.ru"]
    assert [item.value for item in payload["websites"]] == ["https://www.test.ru"]
    assert [item.value for item in payload["addresses"]] == [
        "123456, \u0433. \u041c\u043e\u0441\u043a\u0432\u0430, \u0443\u043b. \u041b\u0435\u043d\u0438\u043d\u0430, \u0434. 1"
    ]
    assert payload["primary_okved"] == core.OkvedEntry(
        code="46.90",
        label="\u0422\u043e\u0440\u0433\u043e\u0432\u043b\u044f \u043e\u043f\u0442\u043e\u0432\u0430\u044f "
        "\u043d\u0435\u0441\u043f\u0435\u0446\u0438\u0430\u043b\u0438\u0437\u0438\u0440\u043e\u0432\u0430\u043d\u043d\u0430\u044f",
    )
    assert payload["additional_okveds"] == [
        core.OkvedEntry(
            code="46.69",
            label="\u0422\u043e\u0440\u0433\u043e\u0432\u043b\u044f \u043e\u043f\u0442\u043e\u0432\u0430\u044f "
            "\u043f\u0440\u043e\u0447\u0438\u043c\u0438 \u043c\u0430\u0448\u0438\u043d\u0430\u043c\u0438 "
            "\u0438 \u043e\u0431\u043e\u0440\u0443\u0434\u043e\u0432\u0430\u043d\u0438\u0435\u043c",
        )
    ]
    assert payload["availability"]["management"]["status"] == "open"
    assert payload["availability"]["management"]["open_count"] == 1
    assert payload["availability"]["founders"]["status"] == "open"
    assert payload["availability"]["founders"]["open_count"] == 1


def test_shared_core_contract_skips_run_disable_for_live_sources() -> None:
    assert core.SOURCE_DOMAINS["checko"] == "checko.ru"
    for status in ("blocked", "rate_limited", "bot_gate"):
        assert core.should_disable_source_for_run(status, live_mode=True) is False
    assert core.should_disable_source_for_run("blocked", offline_mode=True) is False
    assert core.should_disable_source_for_run("blocked") is True
    assert core.should_disable_source_for_run(core.REQUEST_STATUS_BLOCKED_NO_PROXY) is False
    assert core.is_retryable_block_status(core.REQUEST_STATUS_BLOCKED_NO_PROXY) is True


def test_resolve_source_block_reason_prefers_availability_reason_then_notes() -> None:
    availability_first = core.SourceResult(
        source="checko",
        status="blocked",
        notes=["fallback from notes"],
        availability={
            "phones": core.build_field_availability_payload(
                "blocked",
                reason="blocked by source availability",
            )
        },
    )
    notes_fallback = core.SourceResult(
        source="checko",
        status="blocked",
        notes=["fallback from notes"],
    )

    assert core.resolve_source_block_reason(availability_first) == "blocked by source availability"
    assert core.resolve_source_block_reason(notes_fallback) == "fallback from notes"


class _SequenceCheckoClient:
    def __init__(self, outcomes: list[core.RequestOutcome]) -> None:
        self.outcomes = list(outcomes)
        self.urls: list[str] = []
        self.sources: list[str] = []

    def request(self, url: str, *, source: str) -> core.RequestOutcome:
        self.urls.append(url)
        self.sources.append(source)
        if self.outcomes:
            return self.outcomes.pop(0)
        raise AssertionError("unexpected extra checko request")


def test_checko_retries_single_proxy_bound_connection_reset_before_failure() -> None:
    client = _SequenceCheckoClient(
        [
            core.RequestOutcome(
                ok=False,
                status="request_error",
                error="ConnectionResetError(10054), connection forcibly closed by remote host",
                proxy_mode="proxy",
                proxy_label="proxy1.example:8080",
            ),
            core.RequestOutcome(ok=True, status="ok", proxy_mode="proxy"),
        ]
    )

    outcome = checko.CheckoSource(client)._request_with_proxy_reset_retry(
        "https://checko.ru/search?query=7701234567"
    )

    assert outcome.ok is True
    assert client.urls == [
        "https://checko.ru/search?query=7701234567",
        "https://checko.ru/search?query=7701234567",
    ]
    assert client.sources == ["checko", "checko"]


def test_checko_exhausted_proxy_bound_connection_reset_stays_request_error() -> None:
    reset_error = "ConnectionResetError(10054), connection forcibly closed by remote host"
    client = _SequenceCheckoClient(
        [
            core.RequestOutcome(ok=False, status="request_error", error=reset_error, proxy_mode="proxy"),
            core.RequestOutcome(ok=False, status="request_error", error=reset_error, proxy_mode="proxy"),
        ]
    )

    result = checko.CheckoSource(client).search(
        core.RowInput(row_index=1, inn="7701234567", company_name="Test")
    )

    assert len(client.urls) == checko.CHECKO_PROXY_RESET_RETRY_ATTEMPTS
    assert result.status == "request_error"
    assert reset_error in result.errors
    assert any(reset_error in note for note in result.notes)


def test_checko_retries_single_proxy_bound_read_timeout_before_success() -> None:
    timeout_error = "HTTPSConnectionPool(host='checko.ru', port=443): Read timed out. (read timeout=18)"
    client = _SequenceCheckoClient(
        [
            core.RequestOutcome(
                ok=False,
                status="request_error",
                error=timeout_error,
                proxy_mode="proxy",
                timeout=True,
            ),
            core.RequestOutcome(ok=True, status="ok", proxy_mode="proxy"),
        ]
    )

    outcome = checko.CheckoSource(client)._request_with_proxy_reset_retry(
        "https://checko.ru/search?query=7701234567"
    )

    assert outcome.ok is True
    assert client.urls == [
        "https://checko.ru/search?query=7701234567",
        "https://checko.ru/search?query=7701234567",
    ]
    assert client.sources == ["checko", "checko"]


def test_checko_exhausted_proxy_bound_read_timeout_stays_request_error() -> None:
    timeout_error = "HTTPSConnectionPool(host='checko.ru', port=443): Read timed out. (read timeout=18)"
    client = _SequenceCheckoClient(
        [
            core.RequestOutcome(
                ok=False,
                status="request_error",
                error=timeout_error,
                proxy_mode="proxy-bound",
                timeout=True,
            ),
            core.RequestOutcome(
                ok=False,
                status="request_error",
                error=timeout_error,
                proxy_mode="proxy-bound",
                timeout=True,
            ),
        ]
    )

    result = checko.CheckoSource(client).search(
        core.RowInput(row_index=1, inn="7701234567", company_name="Test")
    )

    assert len(client.urls) == checko.CHECKO_PROXY_RESET_RETRY_ATTEMPTS
    assert result.status == "request_error"
    assert timeout_error in result.errors
    assert any(timeout_error in note for note in result.notes)


def test_checko_read_timeout_retry_does_not_apply_to_direct_path() -> None:
    timeout_error = "HTTPSConnectionPool(host='checko.ru', port=443): Read timed out. (read timeout=18)"
    client = _SequenceCheckoClient(
        [
            core.RequestOutcome(
                ok=False,
                status="request_error",
                error=timeout_error,
                proxy_mode="direct",
                timeout=True,
            ),
            core.RequestOutcome(ok=True, status="ok", proxy_mode="direct"),
        ]
    )

    outcome = checko.CheckoSource(client)._request_with_proxy_reset_retry(
        "https://checko.ru/search?query=7701234567"
    )

    assert outcome.ok is False
    assert outcome.status == "request_error"
    assert client.urls == ["https://checko.ru/search?query=7701234567"]
    assert client.sources == ["checko"]


def test_checko_source_blocks_before_outbound_when_proxy_selection_unavailable(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    events: list[dict[str, object]] = []
    session_get_calls: list[tuple[tuple[object, ...], dict[str, object]]] = []
    progress_store = SimpleNamespace(
        append_event=lambda event: events.append(dict(event)),
        host_memory={},
    )
    client = core.RateLimitedHttpClient(
        logger=logging.getLogger("test_checko_source"),
        progress_store=progress_store,
        min_delay_by_host={"checko.ru": 0.0},
        request_timeout=5,
        cooldown_on_429=1,
        cooldown_on_bot=1,
        proxy_pool=_BrokenProxyPool(usable_count=1),
    )

    def fail_if_called(*args, **kwargs):
        session_get_calls.append((args, dict(kwargs)))
        raise RuntimeError("session.get must stay unreachable for blocked_no_proxy")

    monkeypatch.setattr(client.session, "get", fail_if_called)

    result = checko.CheckoSource(client).search(
        core.RowInput(row_index=1, inn="7701234567", company_name="Test")
    )

    assert session_get_calls == []
    assert result.status == core.REQUEST_STATUS_BLOCKED_NO_PROXY
    assert result.http_status is None
    assert any(
        core.REQUEST_BLOCKED_NO_PROXY_SELECTION_UNAVAILABLE_REASON in error
        for error in result.errors
    )
    assert any(
        core.REQUEST_BLOCKED_NO_PROXY_SELECTION_UNAVAILABLE_REASON in note
        for note in result.notes
    )
    assert result.availability["phones"]["status"] == "blocked"
    assert core.REQUEST_BLOCKED_NO_PROXY_SELECTION_UNAVAILABLE_REASON in result.availability["phones"]["reason"]
    assert len(events) == 1
    assert events[0]["type"] == "request_blocked_by_policy"
    assert events[0]["source"] == "checko"
    assert events[0]["host"] == "checko.ru"
    assert events[0]["url"] == "https://checko.ru/search?query=7701234567"
    assert core.REQUEST_BLOCKED_NO_PROXY_SELECTION_UNAVAILABLE_REASON in events[0]["error"]
    assert events[0]["request_status"] == core.REQUEST_STATUS_BLOCKED_NO_PROXY
    assert events[0]["blocked_by_policy"] is True
    assert events[0]["access_state"] == "proxy_required"
    assert events[0]["transport_selected"] == "proxy-bound"
    assert events[0]["transport_final"] == core.REQUEST_STATUS_BLOCKED_NO_PROXY
    assert events[0]["since_previous_request_seconds"] is None
    assert events[0]["proxy_provider_status"] == runtime_proxy6.PROXY_PROVIDER_STATUS_UNKNOWN
    assert events[0]["operator_action"] == runtime_proxy6.PROXY6_OPERATOR_ACTION_CONFIGURE_OR_SYNC


def test_company_site_proxy_storm_preserves_reserved_checko_proxy_capacity(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    events: list[dict[str, object]] = []
    session_get_calls: list[dict[str, object]] = []
    progress_store = SimpleNamespace(
        append_event=lambda event: events.append(dict(event)),
        host_memory={},
    )
    proxy_pool = ProxyPool(
        "http://proxy1.example:8080,http://proxy2.example:8080,http://proxy3.example:8080",
        ban_cooldown_seconds=60,
    )
    client = core.RateLimitedHttpClient(
        logger=logging.getLogger("test_checko_source"),
        progress_store=progress_store,
        min_delay_by_host={"checko.ru": 0.0, "www.taganka-most.ru": 0.0},
        request_timeout=5,
        cooldown_on_429=1,
        cooldown_on_bot=1,
        proxy_pool=proxy_pool,
    )

    burned_proxy_urls: list[str] = []
    for _ in range(2):
        storm_selection = proxy_pool.select("www.taganka-most.ru", source_name="company_site")
        assert storm_selection.via_proxy is True
        burned_proxy_urls.append(storm_selection.url)
        proxy_pool.mark_bad(storm_selection.url, reason="proxy_tunnel_error", source_name="company_site")

    assert len(set(burned_proxy_urls)) == 2
    assert proxy_pool.usable_count() == 1
    assert proxy_pool.usable_count(source_name="company_site") == 0
    assert proxy_pool.select("www.taganka-most.ru", source_name="company_site").via_proxy is False

    reserved_selection = proxy_pool.select("checko.ru", source_name="checko")
    assert reserved_selection.via_proxy is True
    proxy_pool.mark_bad(reserved_selection.url, reason="proxy_tunnel_error", source_name="company_site")
    assert proxy_pool.usable_count() == 1
    assert proxy_pool.usable_count(source_name="checko") == 1
    assert proxy_pool.select("checko.ru", source_name="checko").url == reserved_selection.url

    def fake_get(url: str, **kwargs):
        session_get_calls.append(dict(kwargs))
        response = requests.Response()
        response.status_code = 200
        response.url = url
        response._content = b"<html><body>ok</body></html>"
        response.headers["Content-Type"] = "text/html; charset=utf-8"
        response.encoding = "utf-8"
        return response

    monkeypatch.setattr(client.session, "get", fake_get)

    outcome = client.request("https://checko.ru/search?query=7701234567", source="checko")

    assert outcome.ok is True
    assert outcome.proxy_mode == "proxy"
    assert session_get_calls == [{"timeout": 5, "allow_redirects": True, "proxies": reserved_selection.requests_proxies, "headers": None}]
    assert not any(event["type"] == "request_blocked_by_policy" for event in events)
    assert events[-1]["type"] == "request_ok"
    assert events[-1]["source"] == "checko"
    assert events[-1]["proxy_mode"] == "proxy"


def test_checko_plain_timeout_does_not_deplete_priority_proxy_pool(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("PARSER_PROXY_PRIORITY_SOURCE_NAMES", raising=False)
    monkeypatch.delenv("PARSER_PROXY_PRIORITY_RESERVED_CAPACITY", raising=False)
    proxy_pool = ProxyPool("http://proxy1.example:8080", ban_cooldown_seconds=60)
    selection = proxy_pool.select("checko.ru", source_name="checko")
    assert selection.via_proxy is True

    proxy_pool.mark_bad(selection.url, reason="timeout", source_name="checko")

    assert proxy_pool.usable_count(source_name="checko") == 1
    assert proxy_pool.describe()["cooldown_active"] == 0
    assert proxy_pool.entries[0].failures == 0
    assert "observed:timeout" in proxy_pool.entries[0].descr
    assert proxy_pool.select("checko.ru", source_name="checko").url == selection.url

    proxy_pool.mark_bad(selection.url, reason="proxy_timeout", source_name="checko")

    assert proxy_pool.usable_count(source_name="checko") == 0
    assert proxy_pool.describe()["cooldown_active"] == 1
    assert proxy_pool.entries[0].failures == 1


def test_checko_exhausts_its_own_reserved_proxy_capacity_and_still_blocks_before_outbound(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("PROXY6_API_KEY", raising=False)
    events: list[dict[str, object]] = []
    session_get_calls: list[tuple[tuple[object, ...], dict[str, object]]] = []
    progress_store = SimpleNamespace(
        append_event=lambda event: events.append(dict(event)),
        host_memory={},
    )
    proxy_pool = ProxyPool("http://proxy1.example:8080", ban_cooldown_seconds=60)
    initial_selection = proxy_pool.select("checko.ru", source_name="checko")
    assert initial_selection.via_proxy is True
    proxy_pool.mark_bad(initial_selection.url, reason="proxy_tunnel_error", source_name="checko")
    assert proxy_pool.usable_count(source_name="checko") == 0

    client = core.RateLimitedHttpClient(
        logger=logging.getLogger("test_checko_source"),
        progress_store=progress_store,
        min_delay_by_host={"checko.ru": 0.0},
        request_timeout=5,
        cooldown_on_429=1,
        cooldown_on_bot=1,
        proxy_pool=proxy_pool,
    )

    def fail_if_called(*args, **kwargs):
        session_get_calls.append((args, dict(kwargs)))
        raise RuntimeError("session.get must stay unreachable when checko exhausts its own proxy capacity")

    monkeypatch.setattr(client.session, "get", fail_if_called)

    outcome = client.request("https://checko.ru/search?query=7701234567", source="checko")

    assert session_get_calls == []
    assert outcome.status == core.REQUEST_STATUS_BLOCKED_NO_PROXY
    assert core.REQUEST_BLOCKED_NO_PROXY_NO_POOL_REASON in outcome.error
    assert "proxy_provider_status=proxy_provider_status_unknown" in outcome.error
    assert len(events) == 1
    assert events[0]["type"] == "request_blocked_by_policy"
    assert events[0]["source"] == "checko"
    assert events[0]["request_status"] == core.REQUEST_STATUS_BLOCKED_NO_PROXY
    assert events[0]["error"] == outcome.error
    assert events[0]["proxy_provider_status"] == runtime_proxy6.PROXY_PROVIDER_STATUS_UNKNOWN
    assert events[0]["proxy_provider_stop_class"] == runtime_proxy6.PROXY_PROVIDER_STATUS_UNKNOWN
    assert events[0]["operator_action"] == runtime_proxy6.PROXY6_OPERATOR_ACTION_CONFIGURE_OR_SYNC


def test_run_with_checko_source_uses_selected_source_path_and_default_delay(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    source_calls: list[str] = []
    client_kwargs: dict[str, object] = {}
    output_dir = tmp_path / "output"
    monkeypatch.delenv("DELAY_CHECKO_SECONDS", raising=False)
    _install_lightweight_run_stubs(
        monkeypatch,
        rows=_rows(1),
        source_calls=source_calls,
        client_kwargs=client_kwargs,
    )

    exit_code = pipeline.run(
        pipeline.parse_args(
            [
                "--input",
                "input.xlsx",
                "--output-dir",
                str(output_dir),
                "--sources=checko",
            ]
        )
    )

    assert exit_code == 0
    assert source_calls == ["checko"]
    assert client_kwargs["min_delay_by_host"]["checko.ru"] == 5.0
    assert client_kwargs["min_delay_by_host"]["www.checko.ru"] == 5.0

    results = json.loads((output_dir / "results.json").read_text(encoding="utf-8"))
    assert len(results) == 1
    assert set(results[0]["sources"]) == {"checko"}


def test_run_activates_direct_default_bounded_executor_rolls_handoff_and_keeps_input_order(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    tracker = _ConcurrencyTracker()
    source_calls: list[str] = []
    client_kwargs: dict[str, object] = {}
    aggregator_materialized_at: dict[int, float] = {}
    row_search_completed_at: dict[int, float] = {}
    output_dir = tmp_path / "output"
    rows = _rows(3)
    _install_lightweight_run_stubs(
        monkeypatch,
        rows=rows,
        source_calls=source_calls,
        client_kwargs=client_kwargs,
    )
    original_materialize_stage_work_unit = core.ProgressStore.materialize_stage_work_unit

    def record_materialize_stage_work_unit(self, **kwargs):
        row_index = int(kwargs["row_index"])
        execution_boundary = str(kwargs["execution_boundary"])
        if (
            execution_boundary == pipeline.AGGREGATOR_SITE_EXECUTION_BOUNDARY
            and row_index not in aggregator_materialized_at
        ):
            aggregator_materialized_at[row_index] = time.perf_counter()
        return original_materialize_stage_work_unit(self, **kwargs)

    def record_search_complete(row: core.RowInput, source_name: str) -> None:
        if source_name == "zachestnyibiznes":
            row_search_completed_at[row.row_index] = time.perf_counter()

    monkeypatch.setattr(core.ProgressStore, "materialize_stage_work_unit", record_materialize_stage_work_unit)
    sleep_by_row_index = {
        rows[0].row_index: 0.03,
        rows[1].row_index: 0.01,
        rows[2].row_index: 0.15,
    }
    monkeypatch.setattr(
        pipeline,
        "SparkSource",
        lambda _client: _ConcurrencyProbeSource(
            "spark",
            tracker,
            sleep_by_row_index=sleep_by_row_index,
            on_search_complete=record_search_complete,
        ),
    )
    monkeypatch.setattr(
        pipeline,
        "ZachestnyBiznesSource",
        lambda _client: _ConcurrencyProbeSource(
            "zachestnyibiznes",
            tracker,
            sleep_by_row_index=sleep_by_row_index,
            on_search_complete=record_search_complete,
        ),
    )

    exit_code = pipeline.run(
        pipeline.parse_args(
            [
                "--input",
                "input.xlsx",
                "--output-dir",
                str(output_dir),
                "--sources=spark,zachestnyibiznes",
                "--company-concurrency=2",
            ]
        )
    )

    assert exit_code == 0
    assert tracker.max_inflight > 1
    assert aggregator_materialized_at[rows[0].row_index] < row_search_completed_at[rows[2].row_index]
    assert aggregator_materialized_at[rows[1].row_index] < row_search_completed_at[rows[2].row_index]

    results = json.loads((output_dir / "results.json").read_text(encoding="utf-8"))
    assert [item["inn"] for item in results] == [row.inn for row in rows]

    run_log = (output_dir / "run.log").read_text(encoding="utf-8")
    assert "Direct-default bounded executor activated: workers=2 active_sources=spark,zachestnyibiznes companies=3 full_contour=yes" in run_log


def test_run_activates_direct_default_bounded_executor_with_effective_lane_budget_cap(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    tracker = _ConcurrencyTracker()
    source_calls: list[str] = []
    client_kwargs: dict[str, object] = {}
    output_dir = tmp_path / "output"
    rows = _rows(4)
    _install_lightweight_run_stubs(
        monkeypatch,
        rows=rows,
        source_calls=source_calls,
        client_kwargs=client_kwargs,
    )
    monkeypatch.setattr(
        pipeline,
        "build_source_execution_guardrails",
        lambda **_kwargs: SourceExecutionGuardrails(
            company_concurrency_cap=4,
            requested_company_concurrency=4,
            effective_company_concurrency_cap=2,
            usable_proxy_pool_count=0,
            per_source_cap_map={"spark": 4, "zachestnyibiznes": 4},
            per_source_lane_budget_map={"spark": 2, "zachestnyibiznes": 2},
            per_source_worker_lane_budget_map={"spark": 2, "zachestnyibiznes": 2},
            per_host_cap_map={
                "spark-interfax.ru": 4,
                "zachestnyibiznes.ru": 4,
            },
            source_transport_policy={
                "spark": DIRECT_DEFAULT_TRANSPORT,
                "zachestnyibiznes": DIRECT_DEFAULT_TRANSPORT,
            },
            source_lane_contour=(),
        ),
    )
    sleep_by_row_index = {row.row_index: 0.05 for row in rows}
    monkeypatch.setattr(
        pipeline,
        "SparkSource",
        lambda _client: _ConcurrencyProbeSource("spark", tracker, sleep_by_row_index=sleep_by_row_index),
    )
    monkeypatch.setattr(
        pipeline,
        "ZachestnyBiznesSource",
        lambda _client: _ConcurrencyProbeSource("zachestnyibiznes", tracker, sleep_by_row_index=sleep_by_row_index),
    )

    exit_code = pipeline.run(
        pipeline.parse_args(
            [
                "--input",
                "input.xlsx",
                "--output-dir",
                str(output_dir),
                "--sources=spark,zachestnyibiznes",
                "--company-concurrency=4",
            ]
        )
    )

    assert exit_code == 0
    assert 1 < tracker.max_inflight <= 2

    results = json.loads((output_dir / "results.json").read_text(encoding="utf-8"))
    assert [item["inn"] for item in results] == [row.inn for row in rows]

    run_log = (output_dir / "run.log").read_text(encoding="utf-8")
    assert "Direct-default bounded executor activated: workers=2 active_sources=spark,zachestnyibiznes companies=4 full_contour=yes" in run_log


def test_run_prefetches_next_direct_default_execution_slice_before_first_row_materialization(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    source_calls: list[str] = []
    client_kwargs: dict[str, object] = {}
    gate_completed_at: dict[int, float] = {}
    aggregator_materialized_at: dict[int, float] = {}
    output_dir = tmp_path / "output"
    rows = _rows(3)
    _install_lightweight_run_stubs(
        monkeypatch,
        rows=rows,
        source_calls=source_calls,
        client_kwargs=client_kwargs,
    )
    original_materialize_stage_work_unit = core.ProgressStore.materialize_stage_work_unit

    def record_materialize_stage_work_unit(self, **kwargs):
        row_index = int(kwargs["row_index"])
        execution_boundary = str(kwargs["execution_boundary"])
        if (
            execution_boundary == pipeline.AGGREGATOR_SITE_EXECUTION_BOUNDARY
            and row_index not in aggregator_materialized_at
        ):
            aggregator_materialized_at[row_index] = time.perf_counter()
        return original_materialize_stage_work_unit(self, **kwargs)

    def fake_gate_candidate_sites_before_deep_parse(*, row, **_kwargs):
        time.sleep({rows[0].row_index: 0.04, rows[1].row_index: 0.01, rows[2].row_index: 0.01}[row.row_index])
        gate_completed_at[row.row_index] = time.perf_counter()
        return SimpleNamespace(
            deep_parse_sites=[],
            surface_only_decisions=[],
            trusted_surface_decisions_by_site={},
            notes=[f"prefetched-gate-{row.row_index}"],
        )

    monkeypatch.setattr(core.ProgressStore, "materialize_stage_work_unit", record_materialize_stage_work_unit)
    monkeypatch.setattr(
        pipeline,
        "choose_candidate_sites",
        lambda row, *_args, **_kwargs: [f"https://{row.inn}.example"],
    )
    monkeypatch.setattr(pipeline, "build_domain_resolution", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(
        pipeline,
        "gate_candidate_sites_before_deep_parse",
        fake_gate_candidate_sites_before_deep_parse,
    )
    sleep_by_row_index = {
        rows[0].row_index: 0.12,
        rows[1].row_index: 0.01,
        rows[2].row_index: 0.01,
    }
    monkeypatch.setattr(
        pipeline,
        "SparkSource",
        lambda _client: _ConcurrencyProbeSource("spark", _ConcurrencyTracker(), sleep_by_row_index=sleep_by_row_index),
    )
    monkeypatch.setattr(
        pipeline,
        "ZachestnyBiznesSource",
        lambda _client: _ConcurrencyProbeSource("zachestnyibiznes", _ConcurrencyTracker(), sleep_by_row_index=sleep_by_row_index),
    )

    exit_code = pipeline.run(
        pipeline.parse_args(
            [
                "--input",
                "input.xlsx",
                "--output-dir",
                str(output_dir),
                "--sources=spark,zachestnyibiznes",
                "--company-concurrency=2",
            ]
        )
    )

    assert exit_code == 0
    assert gate_completed_at[rows[1].row_index] < aggregator_materialized_at[rows[0].row_index]
    results = json.loads((output_dir / "results.json").read_text(encoding="utf-8"))
    assert [item["inn"] for item in results] == [row.inn for row in rows]


def test_run_direct_default_dedupes_replayed_deep_parse_materialization_on_same_host(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    source_calls: list[str] = []
    client_kwargs: dict[str, object] = {}
    output_dir = tmp_path / "output"
    rows = _rows(1)
    parse_candidate_sites: list[list[str]] = []
    materialized_fingerprints: list[str] = []
    pending_snapshots: list[list[dict[str, object]]] = []
    replay_injected = {"done": False}
    _install_lightweight_run_stubs(
        monkeypatch,
        rows=rows,
        source_calls=source_calls,
        client_kwargs=client_kwargs,
    )
    original_materialize_stage_work_unit = core.ProgressStore.materialize_stage_work_unit

    def replay_same_identity_materialize_stage_work_unit(self, **kwargs):
        materialized = original_materialize_stage_work_unit(self, **kwargs)
        if (
            replay_injected["done"]
            or str(kwargs["execution_boundary"]) != pipeline.DEEP_PARSE_EXECUTION_BOUNDARY
        ):
            return materialized
        replay_injected["done"] = True
        materialized_fingerprints.append(str(materialized.get("handoff_fingerprint") or ""))
        replay_payload = dict(kwargs["work_unit_payload"])
        replay_payload["candidate_sites"] = [{"site_url": "https://alpha.example/about"}]
        replay_payload["site_gate_decisions"] = [{"site_url": "https://alpha.example/products"}]
        replay_payload["deep_parse_sites"] = ["https://alpha.example/products"]
        replayed = original_materialize_stage_work_unit(
            self,
            inn=kwargs["inn"],
            row_index=kwargs["row_index"],
            execution_boundary=kwargs["execution_boundary"],
            work_unit_payload=replay_payload,
        )
        materialized_fingerprints.append(str(replayed.get("handoff_fingerprint") or ""))
        pending_snapshots.append(
            self.pending_stage_work_units(
                execution_boundary=pipeline.DEEP_PARSE_EXECUTION_BOUNDARY,
                inns=[str(kwargs["inn"])],
            )
        )
        return replayed

    def fake_gate_candidate_sites_before_deep_parse(**_kwargs):
        return SimpleNamespace(
            deep_parse_sites=["https://alpha.example/about"],
            surface_only_decisions=[],
            trusted_surface_decisions_by_site={},
            notes=[],
        )

    def fake_analyzer_factory(*_args, **_kwargs):
        class _DecisionPayload(dict):
            __getattr__ = dict.get

        return SimpleNamespace(
            analyze=lambda _row, site_url, _known_contacts, _source_results: _DecisionPayload(
                {
                    "url": site_url,
                    "final_url": site_url,
                    "decision_status": "candidate",
                    "belongs_to_company": True,
                    "authenticity_score": 0.91,
                    "identity_score": 0.82,
                    "viability_score": 0.73,
                }
            ),
            h=SimpleNamespace(
                normalize_url=lambda value: core.sanitize_website_url(value) or str(value or "").strip()
            ),
            llm=SimpleNamespace(
                should_force_benchmark_stage=lambda _stage: False,
                judge_content_record=lambda *_args, **_kwargs: None,
                capture_forced_content_review_fixture=lambda **_kwargs: None,
            ),
        )

    def parse_company(company):
        parse_candidate_sites.append(list(company.candidate_sites))
        return SimpleNamespace(
            plans=[
                SimpleNamespace(
                    site_url=company.candidate_sites[0],
                    allows_deep_check=True,
                )
            ],
            site_probes=[],
            route_strategies=[],
            content_records=[],
            notes=[],
        )

    monkeypatch.setattr(core.ProgressStore, "materialize_stage_work_unit", replay_same_identity_materialize_stage_work_unit)
    monkeypatch.setattr(
        pipeline,
        "choose_candidate_sites",
        lambda *_args, **_kwargs: ["https://alpha.example/catalog"],
    )
    monkeypatch.setattr(pipeline, "build_domain_resolution", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(
        pipeline,
        "gate_candidate_sites_before_deep_parse",
        fake_gate_candidate_sites_before_deep_parse,
    )
    monkeypatch.setattr(pipeline, "BenchmarkAwareSiteAuthenticityAnalyzer", fake_analyzer_factory)
    monkeypatch.setattr(
        pipeline,
        "FactorySiteParser",
        lambda *_args, **_kwargs: SimpleNamespace(parse=parse_company),
    )

    exit_code = pipeline.run(
        pipeline.parse_args(
            [
                "--input",
                "input.xlsx",
                "--output-dir",
                str(output_dir),
                "--sources=spark,zachestnyibiznes",
                "--company-concurrency=2",
            ]
        )
    )

    assert exit_code == 0
    assert materialized_fingerprints[0] == materialized_fingerprints[1]
    assert len(pending_snapshots) == 1
    assert len(pending_snapshots[0]) == 1
    assert pending_snapshots[0][0]["handoff_fingerprint"] == materialized_fingerprints[0]
    assert pending_snapshots[0][0]["work_unit"]["deep_parse_sites"] == ["https://alpha.example/about"]
    assert parse_candidate_sites == [["https://alpha.example/about"]]

    results = json.loads((output_dir / "results.json").read_text(encoding="utf-8"))
    assert [item["inn"] for item in results] == [row.inn for row in rows]
    assert results[0]["candidate_sites"] == ["https://alpha.example/catalog"]

    runtime_state = json.loads((output_dir / "runtime_state.json").read_text(encoding="utf-8"))
    deep_parse_work_unit = runtime_state["run"]["metadata"]["stage_work_units"]["deep_parse"]["companies"]["0000000001"]
    assert deep_parse_work_unit["work_status"] == "acked"
    assert deep_parse_work_unit["handoff_fingerprint"] == materialized_fingerprints[0]

    run_log = (output_dir / "run.log").read_text(encoding="utf-8")
    assert "Direct-default bounded executor activated" in run_log


def test_run_host_governor_serializes_same_host_prefetch_and_preserves_result_order(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    source_calls: list[str] = []
    client_kwargs: dict[str, object] = {}
    output_dir = tmp_path / "output"
    rows = _rows(3)
    _install_lightweight_run_stubs(
        monkeypatch,
        rows=rows,
        source_calls=source_calls,
        client_kwargs=client_kwargs,
    )

    gate_lock = threading.Lock()
    gate_started_at: dict[int, float] = {}
    gate_finished_at: dict[int, float] = {}
    inflight_by_host: dict[str, int] = {}
    max_inflight_by_host: dict[str, int] = {}
    gate_sleep_by_row_index = {
        rows[0].row_index: 0.04,
        rows[1].row_index: 0.01,
        rows[2].row_index: 0.01,
    }
    candidate_sites_by_row_index = {
        rows[0].row_index: ["https://shared-host.example/company-a"],
        rows[1].row_index: ["https://shared-host.example/company-b"],
        rows[2].row_index: ["https://isolated-host.example/company-c"],
    }
    source_sleep_by_row_index = {
        rows[0].row_index: 0.01,
        rows[1].row_index: 0.02,
        rows[2].row_index: 0.02,
    }

    def fake_gate_candidate_sites_before_deep_parse(*, row, candidate_sites, **_kwargs):
        host = str(urlparse(candidate_sites[0]).hostname or "")
        with gate_lock:
            inflight_by_host[host] = inflight_by_host.get(host, 0) + 1
            max_inflight_by_host[host] = max(max_inflight_by_host.get(host, 0), inflight_by_host[host])
            gate_started_at[row.row_index] = time.perf_counter()
        try:
            time.sleep(gate_sleep_by_row_index[row.row_index])
            client_kwargs["client_instance"].progress_store.append_event(
                {
                    "ts": core.utc_now_iso(),
                    "type": "route_fetch_attempt",
                    "host": host,
                    "source": "company_site",
                    "status": "success",
                    "cooldown_seconds": 0.12 if row.row_index == rows[0].row_index else 0,
                }
            )
            return SimpleNamespace(
                deep_parse_sites=[],
                surface_only_decisions=[],
                trusted_surface_decisions_by_site={},
                notes=[],
            )
        finally:
            with gate_lock:
                gate_finished_at[row.row_index] = time.perf_counter()
                inflight_by_host[host] -= 1

    monkeypatch.setattr(
        pipeline,
        "choose_candidate_sites",
        lambda row, *_args, **_kwargs: list(candidate_sites_by_row_index[row.row_index]),
    )
    monkeypatch.setattr(pipeline, "build_domain_resolution", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(
        pipeline,
        "gate_candidate_sites_before_deep_parse",
        fake_gate_candidate_sites_before_deep_parse,
    )
    monkeypatch.setattr(
        pipeline,
        "SparkSource",
        lambda _client: _ConcurrencyProbeSource(
            "spark",
            _ConcurrencyTracker(),
            sleep_by_row_index=source_sleep_by_row_index,
        ),
    )
    monkeypatch.setattr(
        pipeline,
        "ZachestnyBiznesSource",
        lambda _client: _ConcurrencyProbeSource(
            "zachestnyibiznes",
            _ConcurrencyTracker(),
            sleep_by_row_index=source_sleep_by_row_index,
        ),
    )

    exit_code = pipeline.run(
        pipeline.parse_args(
            [
                "--input",
                "input.xlsx",
                "--output-dir",
                str(output_dir),
                "--sources=spark,zachestnyibiznes",
                "--company-concurrency=2",
            ]
        )
    )

    assert exit_code == 0
    assert max_inflight_by_host["shared-host.example"] == 1
    assert gate_started_at[rows[1].row_index] >= gate_finished_at[rows[0].row_index]
    assert gate_started_at[rows[1].row_index] - gate_finished_at[rows[0].row_index] >= 0.08

    results = json.loads((output_dir / "results.json").read_text(encoding="utf-8"))
    assert [item["inn"] for item in results] == [row.inn for row in rows]


def test_run_direct_default_keeps_low_priority_queue_family_slice_out_of_mainline_scheduler(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    tracker = _ConcurrencyTracker()
    source_calls: list[str] = []
    client_kwargs: dict[str, object] = {}
    output_dir = tmp_path / "output"
    rows = _rows(2)
    _install_lightweight_run_stubs(
        monkeypatch,
        rows=rows,
        source_calls=source_calls,
        client_kwargs=client_kwargs,
    )

    def fake_gate_candidate_sites_before_deep_parse(**_kwargs):
        return SimpleNamespace(
            deep_parse_sites=["https://alpha.example/about"],
            surface_only_decisions=[],
            trusted_surface_decisions_by_site={},
            notes=["prefetched-low-priority-slice"],
        )

    def fake_analyzer_factory(*_args, **_kwargs):
        class _DecisionPayload(dict):
            __getattr__ = dict.get

        return SimpleNamespace(
            analyze=lambda _row, site_url, _known_contacts, _source_results: _DecisionPayload(
                {
                    "url": site_url,
                    "final_url": site_url,
                    "decision_status": "candidate",
                    "belongs_to_company": True,
                    "authenticity_score": 0.91,
                    "identity_score": 0.82,
                    "viability_score": 0.73,
                }
            ),
            h=SimpleNamespace(
                normalize_url=lambda value: core.sanitize_website_url(value) or str(value or "").strip()
            ),
            llm=SimpleNamespace(
                should_force_benchmark_stage=lambda _stage: False,
                judge_content_record=lambda *_args, **_kwargs: None,
                capture_forced_content_review_fixture=lambda **_kwargs: None,
            ),
        )

    def parse_company(_company):
        return SimpleNamespace(
            plans=[SimpleNamespace(site_url="https://alpha.example/about", allows_deep_check=True)],
            site_probes=[],
            route_strategies=[],
            content_records=[],
            notes=[],
        )

    sleep_by_row_index = {
        rows[0].row_index: 0.06,
        rows[1].row_index: 0.02,
    }
    monkeypatch.setattr(
        pipeline,
        "SparkSource",
        lambda _client: _ConcurrencyProbeSource("spark", tracker, sleep_by_row_index=sleep_by_row_index),
    )
    monkeypatch.setattr(
        pipeline,
        "ZachestnyBiznesSource",
        lambda _client: _ConcurrencyProbeSource("zachestnyibiznes", tracker, sleep_by_row_index=sleep_by_row_index),
    )
    monkeypatch.setattr(
        pipeline,
        "choose_candidate_sites",
        lambda *_args, **_kwargs: ["https://alpha.example/catalog"],
    )
    monkeypatch.setattr(pipeline, "build_domain_resolution", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(
        pipeline,
        "gate_candidate_sites_before_deep_parse",
        fake_gate_candidate_sites_before_deep_parse,
    )
    monkeypatch.setattr(pipeline, "BenchmarkAwareSiteAuthenticityAnalyzer", fake_analyzer_factory)
    monkeypatch.setattr(
        pipeline,
        "FactorySiteParser",
        lambda *_args, **_kwargs: SimpleNamespace(parse=parse_company),
    )

    exit_code = pipeline.run(
        pipeline.parse_args(
            [
                "--input",
                "input.xlsx",
                "--output-dir",
                str(output_dir),
                "--sources=spark,zachestnyibiznes",
                "--company-concurrency=2",
            ]
        )
    )

    assert exit_code == 0
    assert tracker.max_inflight > 1

    runtime_state = json.loads((output_dir / "runtime_state.json").read_text(encoding="utf-8"))
    deep_parse_work_unit = runtime_state["run"]["metadata"]["stage_execution_evidence"]["deep_parse"]["companies"]["0000000001"]
    aggregator_work_unit = runtime_state["run"]["metadata"]["stage_execution_evidence"]["aggregator_site"]["companies"]["0000000001"]
    assert deep_parse_work_unit["execution_boundary"] == pipeline.DEEP_PARSE_EXECUTION_BOUNDARY
    assert deep_parse_work_unit["work_unit"]["queue_family_contour"] == build_deep_parse_queue_family_contour().as_payload()
    assert aggregator_work_unit["execution_boundary"] == pipeline.AGGREGATOR_SITE_EXECUTION_BOUNDARY
    assert aggregator_work_unit["work_unit"]["queue_family_contour"] == build_aggregator_site_queue_family_contour().as_payload()

    run_log = (output_dir / "run.log").read_text(encoding="utf-8")
    assert "Direct-default bounded executor activated: workers=2 active_sources=spark,zachestnyibiznes companies=2 full_contour=yes" in run_log


def test_run_cleanup_restores_progress_store_and_rethrows_exception_after_activation(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    tracker = _ConcurrencyTracker()
    source_calls: list[str] = []
    client_kwargs: dict[str, object] = {}
    cleanup_state = {
        "close_calls": 0,
        "store_swapped_before_close": False,
        "store_restored_after_close": False,
    }
    output_dir = tmp_path / "output"
    rows = _rows(3)
    _install_lightweight_run_stubs(
        monkeypatch,
        rows=rows,
        source_calls=source_calls,
        client_kwargs=client_kwargs,
    )
    original_materialize_stage_work_unit = core.ProgressStore.materialize_stage_work_unit
    original_open_executor = pipeline.open_company_source_search_executor

    def fail_after_activation(self, **kwargs):
        if (
            str(kwargs["execution_boundary"]) == pipeline.AGGREGATOR_SITE_EXECUTION_BOUNDARY
            and int(kwargs["row_index"]) == rows[0].row_index
        ):
            raise RuntimeError("boom after activation")
        return original_materialize_stage_work_unit(self, **kwargs)

    def open_executor_with_close_probe(**kwargs):
        executor = original_open_executor(**kwargs)
        original_close = executor.close

        def wrapped_close():
            cleanup_state["close_calls"] += 1
            cleanup_state["store_swapped_before_close"] = (
                kwargs["shared_client"].progress_store is not client_kwargs["progress_store"]
            )
            original_close()
            cleanup_state["store_restored_after_close"] = (
                kwargs["shared_client"].progress_store is client_kwargs["progress_store"]
            )

        executor.close = wrapped_close
        return executor

    sleep_by_row_index = {
        rows[0].row_index: 0.01,
        rows[1].row_index: 0.01,
        rows[2].row_index: 0.15,
    }
    monkeypatch.setattr(core.ProgressStore, "materialize_stage_work_unit", fail_after_activation)
    monkeypatch.setattr(pipeline, "open_company_source_search_executor", open_executor_with_close_probe)
    monkeypatch.setattr(
        pipeline,
        "SparkSource",
        lambda _client: _ConcurrencyProbeSource("spark", tracker, sleep_by_row_index=sleep_by_row_index),
    )
    monkeypatch.setattr(
        pipeline,
        "ZachestnyBiznesSource",
        lambda _client: _ConcurrencyProbeSource("zachestnyibiznes", tracker, sleep_by_row_index=sleep_by_row_index),
    )

    with pytest.raises(RuntimeError, match="boom after activation"):
        pipeline.run(
            pipeline.parse_args(
                [
                    "--input",
                    "input.xlsx",
                    "--output-dir",
                    str(output_dir),
                    "--sources=spark,zachestnyibiznes",
                    "--company-concurrency=2",
                ]
            )
        )

    assert cleanup_state["close_calls"] == 1
    assert cleanup_state["store_swapped_before_close"] is True
    assert cleanup_state["store_restored_after_close"] is True
    assert client_kwargs["client_instance"].progress_store is client_kwargs["progress_store"]


def test_run_keeps_mixed_source_lanes_isolated_and_materializes_full_source_contour(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    tracker = _ConcurrencyTracker()
    thread_names: dict[str, list[str]] = {}
    source_calls: list[str] = []
    client_kwargs: dict[str, object] = {}
    output_dir = tmp_path / "output"
    rows = _rows(2)
    _install_lightweight_run_stubs(
        monkeypatch,
        rows=rows,
        source_calls=source_calls,
        client_kwargs=client_kwargs,
    )
    monkeypatch.setattr(pipeline, "ProxyPool", lambda *_args, **_kwargs: _BrokenProxyPool(usable_count=2))
    sleep_by_row_index = {rows[0].row_index: 0.08, rows[1].row_index: 0.01}
    monkeypatch.setattr(
        pipeline,
        "SparkSource",
        lambda _client: _ThreadRecordingSource(
            "spark",
            thread_names,
            sleep_by_row_index=sleep_by_row_index,
            tracker=tracker,
        ),
    )
    monkeypatch.setattr(
        pipeline,
        "ZachestnyBiznesSource",
        lambda _client: _ThreadRecordingSource(
            "zachestnyibiznes",
            thread_names,
            sleep_by_row_index=sleep_by_row_index,
            tracker=tracker,
        ),
    )
    monkeypatch.setattr(
        pipeline,
        "RusprofileSource",
        lambda _client: _ThreadRecordingSource("rusprofile", thread_names),
    )
    monkeypatch.setattr(
        pipeline,
        "CheckoSource",
        lambda _client: _ThreadRecordingSource("checko", thread_names),
    )
    monkeypatch.setattr(
        pipeline,
        "ListOrgSource",
        lambda *_args, **_kwargs: _ThreadRecordingSource("list_org", thread_names),
    )

    exit_code = pipeline.run(
        pipeline.parse_args(
            [
                "--input",
                "input.xlsx",
                "--output-dir",
                str(output_dir),
                "--sources=spark,zachestnyibiznes,rusprofile,checko,list_org",
                "--company-concurrency=2",
            ]
        )
    )

    assert exit_code == 0
    assert tracker.max_inflight > 1
    assert all(name.startswith("direct-default-source") for name in thread_names["spark"])
    assert all(name.startswith("direct-default-source") for name in thread_names["zachestnyibiznes"])
    assert all(name == "MainThread" for name in thread_names["rusprofile"])
    assert all(name.startswith("direct-default-source") for name in thread_names["checko"])
    assert all(name == "MainThread" for name in thread_names["list_org"])

    results = json.loads((output_dir / "results.json").read_text(encoding="utf-8"))
    assert [item["inn"] for item in results] == [row.inn for row in rows]
    assert all(
        set(item["sources"]) == {"spark", "zachestnyibiznes", "rusprofile", "checko", "list_org"}
        for item in results
    )

    runtime_state = json.loads((output_dir / "runtime_state.json").read_text(encoding="utf-8"))
    scheduler = runtime_state["run"]["metadata"]["source_lane_scheduler"]
    downstream_pools = runtime_state["run"]["metadata"]["downstream_worker_pools"]
    contour_by_source = {
        item["source_name"]: item
        for item in scheduler["source_lane_contour"]
    }
    assert set(contour_by_source) == {"spark", "zachestnyibiznes", "rusprofile", "checko", "list_org"}
    assert contour_by_source["spark"]["scheduler_lane"] == "direct_default_worker"
    assert contour_by_source["zachestnyibiznes"]["scheduler_lane"] == "direct_default_worker"
    assert contour_by_source["rusprofile"]["capacity_boundary"] == "session_bound_serial_lane"
    assert contour_by_source["checko"]["scheduler_lane"] == "proxy_bound_worker"
    assert contour_by_source["checko"]["capacity_boundary"] == "proxy_bound_worker_lane"
    assert contour_by_source["list_org"]["capacity_boundary"] == "offline_only_surface"
    assert scheduler["per_source_worker_lane_budget_map"] == {
        "spark": 2,
        "zachestnyibiznes": 2,
        "rusprofile": 0,
        "checko": 2,
        "list_org": 0,
    }
    assert downstream_pools == build_downstream_worker_pool_contour(company_concurrency_cap=2).as_payload()
    aggregator_work_unit = runtime_state["run"]["metadata"]["stage_execution_evidence"]["aggregator_site"]["companies"]["0000000001"]["work_unit"]
    assert {
        item["source_name"]
        for item in aggregator_work_unit["source_lane_scheduler"]["source_lane_contour"]
    } == {"spark", "zachestnyibiznes", "rusprofile", "checko", "list_org"}
    assert aggregator_work_unit["source_lane_scheduler"]["downstream_worker_pools"] == downstream_pools
    assert aggregator_work_unit["downstream_worker_pools"] == downstream_pools

    run_log = (output_dir / "run.log").read_text(encoding="utf-8")
    assert "Direct-default bounded executor activated: workers=2 active_sources=spark,zachestnyibiznes companies=2 full_contour=no" in run_log
    assert "Checko worker lane activated: workers=2 companies=2" in run_log


def test_build_source_execution_guardrails_materializes_full_source_contour_with_capacity_boundaries() -> None:
    guardrails = build_source_execution_guardrails(
        active_sources=["list_org", "rusprofile", "spark", "zachestnyibiznes", "checko"],
        requested_company_concurrency=4,
        usable_proxy_pool_count=0,
    )

    assert guardrails.company_concurrency_cap == 4
    assert guardrails.effective_company_concurrency_cap == 4
    assert guardrails.source_transport_policy["list_org"] == OFFLINE_ONLY_TRANSPORT
    assert guardrails.source_transport_policy["rusprofile"] == SESSION_BOUND_TRANSPORT
    assert guardrails.source_transport_policy["spark"] == DIRECT_DEFAULT_TRANSPORT
    assert guardrails.source_transport_policy["zachestnyibiznes"] == DIRECT_DEFAULT_TRANSPORT
    assert guardrails.source_transport_policy["checko"] == PROXY_REQUIRED_TRANSPORT
    assert guardrails.per_source_cap_map["list_org"] == 0
    assert guardrails.per_source_cap_map["rusprofile"] == 1
    assert guardrails.per_source_cap_map["spark"] == 4
    assert guardrails.per_source_cap_map["zachestnyibiznes"] == 4
    assert guardrails.per_source_cap_map["checko"] == 0
    assert guardrails.per_source_lane_budget_map["list_org"] == 1
    assert guardrails.per_source_lane_budget_map["rusprofile"] == 1
    assert guardrails.per_source_lane_budget_map["spark"] == 4
    assert guardrails.per_source_lane_budget_map["zachestnyibiznes"] == 4
    assert guardrails.per_source_lane_budget_map["checko"] == 1
    assert guardrails.per_source_worker_lane_budget_map["list_org"] == 0
    assert guardrails.per_source_worker_lane_budget_map["rusprofile"] == 0
    assert guardrails.per_source_worker_lane_budget_map["spark"] == 4
    assert guardrails.per_source_worker_lane_budget_map["zachestnyibiznes"] == 4
    assert guardrails.per_source_worker_lane_budget_map["checko"] == 0
    assert guardrails.per_host_cap_map["www.list-org.com"] == 0
    assert guardrails.per_host_cap_map["rusprofile.ru"] == 1
    assert guardrails.per_host_cap_map["www.rusprofile.ru"] == 1
    assert guardrails.per_host_cap_map["spark-interfax.ru"] == 4
    assert guardrails.per_host_cap_map["zachestnyibiznes.ru"] == 4
    assert guardrails.per_host_cap_map["checko.ru"] == 0
    assert guardrails.per_host_cap_map["www.checko.ru"] == 0

    contour_by_source = {entry.source_name: entry for entry in guardrails.source_lane_contour}
    assert contour_by_source["list_org"].scheduler_lane == "offline_surface"
    assert contour_by_source["list_org"].capacity_boundary == "offline_only_surface"
    assert contour_by_source["rusprofile"].scheduler_lane == "session_serial_inline"
    assert contour_by_source["rusprofile"].capacity_boundary == "session_bound_serial_lane"
    assert contour_by_source["spark"].scheduler_lane == "direct_default_worker"
    assert contour_by_source["spark"].worker_lane_budget == 4
    assert contour_by_source["zachestnyibiznes"].scheduler_lane == "direct_default_worker"
    assert contour_by_source["checko"].scheduler_lane == "proxy_bound_worker_unavailable"
    assert contour_by_source["checko"].capacity_boundary == "proxy_capacity_unavailable"


def test_build_source_execution_guardrails_keeps_serial_checko_allowed_without_proxy_budget() -> None:
    guardrails = build_source_execution_guardrails(
        active_sources=["checko"],
        requested_company_concurrency=1,
        usable_proxy_pool_count=0,
    )

    assert guardrails.company_concurrency_cap == 1
    assert guardrails.effective_company_concurrency_cap == 1
    assert guardrails.source_transport_policy["checko"] == PROXY_REQUIRED_TRANSPORT
    assert guardrails.per_source_cap_map["checko"] == 0
    assert guardrails.per_source_lane_budget_map["checko"] == 1
    assert guardrails.per_source_worker_lane_budget_map["checko"] == 0
    assert guardrails.per_host_cap_map["checko.ru"] == 0
    assert guardrails.per_host_cap_map["www.checko.ru"] == 0
    assert guardrails.source_lane_contour[0].scheduler_lane == "proxy_serial_inline"
    assert guardrails.source_lane_contour[0].capacity_boundary == "proxy_capacity_unavailable"


def test_build_source_execution_guardrails_opens_checko_worker_lane_with_proxy_budget() -> None:
    guardrails = build_source_execution_guardrails(
        active_sources=["rusprofile", "checko"],
        requested_company_concurrency=4,
        usable_proxy_pool_count=2,
    )

    assert guardrails.effective_company_concurrency_cap == 2
    assert guardrails.per_source_cap_map["rusprofile"] == 1
    assert guardrails.per_source_cap_map["checko"] == 2
    assert guardrails.per_source_lane_budget_map["checko"] == 2
    assert guardrails.per_source_worker_lane_budget_map["rusprofile"] == 0
    assert guardrails.per_source_worker_lane_budget_map["checko"] == 2
    assert guardrails.per_host_cap_map["checko.ru"] == 2
    contour_by_source = {entry.source_name: entry for entry in guardrails.source_lane_contour}
    assert contour_by_source["rusprofile"].scheduler_lane == "session_serial_inline"
    assert contour_by_source["checko"].scheduler_lane == "proxy_bound_worker"
    assert contour_by_source["checko"].contour_state == "worker_lane_active"
    assert contour_by_source["checko"].capacity_boundary == "proxy_bound_worker_lane"


def test_plan_direct_default_bounded_executor_uses_effective_lane_budget_cap() -> None:
    plan = plan_direct_default_bounded_executor(
        active_sources=["spark", "rusprofile", "zachestnyibiznes"],
        company_concurrency_cap=4,
        per_source_lane_budget_map={
            "spark": 4,
            "rusprofile": 1,
            "zachestnyibiznes": 2,
        },
    )

    assert plan.enabled is True
    assert plan.max_workers == 2
    assert plan.active_sources == ("spark", "zachestnyibiznes")


def test_run_activates_checko_worker_lane_without_enabling_downstream_prefetch(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    thread_names: dict[str, list[str]] = {}
    source_calls: list[str] = []
    client_kwargs: dict[str, object] = {}
    output_dir = tmp_path / "output"
    _install_lightweight_run_stubs(
        monkeypatch,
        rows=_rows(2),
        source_calls=source_calls,
        client_kwargs=client_kwargs,
    )
    monkeypatch.setattr(pipeline, "ProxyPool", lambda *_args, **_kwargs: _BrokenProxyPool(usable_count=2))
    monkeypatch.setattr(
        pipeline,
        "CheckoSource",
        lambda _client: _ThreadRecordingSource("checko", thread_names),
    )

    exit_code = pipeline.run(
        pipeline.parse_args(
            [
                "--input",
                "input.xlsx",
                "--output-dir",
                str(output_dir),
                "--sources=checko",
                "--company-concurrency=2",
            ]
        )
    )

    assert exit_code == 0
    assert source_calls == []
    assert all(name.startswith("direct-default-source") for name in thread_names["checko"])

    runtime_state = json.loads((output_dir / "runtime_state.json").read_text(encoding="utf-8"))
    scheduler = runtime_state["run"]["metadata"]["source_lane_scheduler"]
    assert scheduler["per_source_worker_lane_budget_map"]["checko"] == 2
    assert scheduler["source_lane_contour"][0]["scheduler_lane"] == "proxy_bound_worker"

    run_log = (output_dir / "run.log").read_text(encoding="utf-8")
    assert "Checko worker lane activated: workers=2 companies=2" in run_log
    assert "Company downstream prefetch activated" not in run_log


def test_run_stops_at_proxy_provider_guardrail_without_checko_capacity(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    monkeypatch.delenv("PROXY6_API_KEY", raising=False)
    source_calls: list[str] = []
    client_kwargs: dict[str, object] = {}
    session_get_calls: list[tuple[tuple[object, ...], dict[str, object]]] = []
    original_rate_limited_http_client = core.RateLimitedHttpClient
    output_dir = tmp_path / "output"
    _install_lightweight_run_stubs(
        monkeypatch,
        rows=_rows(1),
        source_calls=source_calls,
        client_kwargs=client_kwargs,
    )

    def real_rate_limited_http_client(**kwargs):
        client_kwargs.update(kwargs)
        client = original_rate_limited_http_client(**kwargs)

        def fail_if_called(*args, **inner_kwargs):
            session_get_calls.append((args, dict(inner_kwargs)))
            raise RuntimeError("session.get must stay unreachable when checko has no usable proxy pool")

        monkeypatch.setattr(client.session, "get", fail_if_called)
        return client

    class _RecordingRealCheckoSource(checko.CheckoSource):
        def search(self, row: core.RowInput) -> core.SourceResult:
            source_calls.append(self.source_name)
            return super().search(row)

    monkeypatch.setattr(pipeline.core, "RateLimitedHttpClient", real_rate_limited_http_client)
    monkeypatch.setattr(pipeline, "CheckoSource", lambda client: _RecordingRealCheckoSource(client))
    monkeypatch.setattr(pipeline, "ProxyPool", lambda *_args, **_kwargs: ProxyPool(""))

    exit_code = pipeline.run(
        pipeline.parse_args(
            [
                "--input",
                "input.xlsx",
                "--output-dir",
                str(output_dir),
                "--sources=checko",
                "--company-concurrency=2",
            ]
        )
    )

    assert exit_code == 1
    assert source_calls == []
    assert session_get_calls == []

    summary = json.loads((output_dir / "summary.json").read_text(encoding="utf-8"))
    assert summary["run_status"] == pipeline.RUN_STATUS_FAILED_REQUIRED_SOURCE
    assert summary["finish_reason"] == pipeline.RUN_FINISH_REASON_REQUIRED_SOURCE
    assert summary["terminal_checkpoint"] == "checko_proxy_provider_startup_guardrail"
    assert summary["terminal_source"] == "checko"
    assert summary["terminal_source_status"] == core.REQUEST_STATUS_BLOCKED_NO_PROXY
    assert summary["terminal_source_access_mode"] == "proxy-bound"
    assert summary["stop_reason"] == "required_source_red_flag"
    assert core.REQUEST_BLOCKED_NO_PROXY_NO_POOL_REASON in summary["terminal_error_message"]
    assert "proxy_provider_status=proxy_provider_status_unknown" in summary["terminal_error_message"]
    assert runtime_proxy6.PROXY6_OPERATOR_ACTION_CONFIGURE_OR_SYNC in summary["terminal_error_message"]

    results = json.loads((output_dir / "results.json").read_text(encoding="utf-8"))
    assert results == []

    runtime_state = json.loads((output_dir / "runtime_state.json").read_text(encoding="utf-8"))
    scheduler = runtime_state["run"]["metadata"]["source_lane_scheduler"]
    assert scheduler["per_source_cap_map"]["checko"] == 0
    assert scheduler["per_source_lane_budget_map"]["checko"] == 1
    assert scheduler["per_source_worker_lane_budget_map"]["checko"] == 0
    assert scheduler["source_lane_contour"][0]["source_name"] == "checko"
    assert scheduler["source_lane_contour"][0]["capacity_boundary"] == "proxy_capacity_unavailable"
    assert "usable_proxy_pool_count == 0" in scheduler["source_lane_contour"][0]["reason"]
    assert runtime_state["run"]["summary"]["run_status"] == pipeline.RUN_STATUS_FAILED_REQUIRED_SOURCE
    assert runtime_state["run"]["summary"]["terminal_source_status"] == core.REQUEST_STATUS_BLOCKED_NO_PROXY
    assert runtime_state["company_entries"] == []

    events = [
        json.loads(line)
        for line in (output_dir / "events.jsonl").read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    assert [event["type"] for event in events] == [
        "run_started",
        "required_source_proxy_provider_stopper",
        "run_finished",
    ]
    stopper_event = next(event for event in events if event.get("type") == "required_source_proxy_provider_stopper")
    assert stopper_event["source"] == "checko"
    assert stopper_event["source_status"] == core.REQUEST_STATUS_BLOCKED_NO_PROXY
    assert stopper_event["proxy_provider_status"] == runtime_proxy6.PROXY_PROVIDER_STATUS_UNKNOWN
    assert stopper_event["proxy_provider_stop_class"] == runtime_proxy6.PROXY_PROVIDER_STATUS_UNKNOWN
    assert stopper_event["operator_action"] == runtime_proxy6.PROXY6_OPERATOR_ACTION_CONFIGURE_OR_SYNC


def test_run_does_not_disable_live_proxied_source_after_blocked_status(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    blocked_reason = "proxy timeout on first company"
    source_calls: list[str] = []
    client_kwargs: dict[str, object] = {}
    output_dir = tmp_path / "output"

    def checko_result_factory(_row: core.RowInput, call_count: int) -> core.SourceResult:
        if call_count == 1:
            return core.SourceResult(
                source="checko",
                status="blocked",
                notes=[blocked_reason],
                errors=[blocked_reason],
                availability={
                    "phones": core.build_field_availability_payload("blocked", reason=blocked_reason),
                },
            )
        return core.SourceResult(
            source="checko",
            status="success",
            notes=["second company still queried"],
        )

    _install_lightweight_run_stubs(
        monkeypatch,
        rows=_rows(2),
        source_calls=source_calls,
        client_kwargs=client_kwargs,
        source_result_factories={"checko": checko_result_factory},
    )

    exit_code = pipeline.run(
        pipeline.parse_args(
            [
                "--input",
                "input.xlsx",
                "--output-dir",
                str(output_dir),
                "--sources=checko",
            ]
        )
    )

    assert exit_code == 1
    assert source_calls == ["checko"]

    summary = json.loads((output_dir / "summary.json").read_text(encoding="utf-8"))
    assert summary["run_status"] == pipeline.RUN_STATUS_FAILED_REQUIRED_SOURCE
    assert summary["finish_reason"] == pipeline.RUN_FINISH_REASON_REQUIRED_SOURCE
    assert summary["terminal_source"] == "checko"
    assert summary["terminal_source_status"] == "blocked"
    assert summary["terminal_source_access_mode"] == "proxy-bound"
    assert summary["stop_reason"] == "required_source_red_flag"
    assert blocked_reason in summary["terminal_error_message"]

    results = json.loads((output_dir / "results.json").read_text(encoding="utf-8"))
    assert results == []

    runtime_state = json.loads((output_dir / "runtime_state.json").read_text(encoding="utf-8"))
    assert runtime_state["run"]["summary"]["run_status"] == pipeline.RUN_STATUS_FAILED_REQUIRED_SOURCE
    assert runtime_state["run"]["summary"]["terminal_source_status"] == "blocked"
    assert runtime_state["company_entries"] == []

    events = [
        json.loads(line)
        for line in (output_dir / "events.jsonl").read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    assert [event["type"] for event in events] == ["run_started", "required_source_red_flag", "run_finished"]
    assert all(event.get("type") != "source_disabled_for_run" for event in events)
    assert events[1]["source"] == "checko"
    assert events[1]["source_status"] == "blocked"
    assert blocked_reason in events[1]["reason"]

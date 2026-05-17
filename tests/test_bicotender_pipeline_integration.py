from __future__ import annotations

import logging

import requests

from app.runtime import concurrency, proxies
from app.dossier import build_company_dossier
from app.sources import bicotender

import company_enrichment_core as core


VALID_INN = "7707083893"
FOUND_ONE_TENDER = "\u041d\u0430\u0439\u0434\u0435\u043d\u043e 1 \u0442\u0435\u043d\u0434\u0435\u0440"
FOUND_ZERO_TENDERS = "\u041d\u0430\u0439\u0434\u0435\u043d\u043e 0 \u0442\u0435\u043d\u0434\u0435\u0440\u043e\u0432"


class _FakeHttpResponse:
    def __init__(self, html: str, *, status_code: int = 200, url: str = "") -> None:
        self.text = html
        self.status_code = status_code
        self.url = url or f"https://www.bicotender.ru/tender/search/?company%5Binn%5D={VALID_INN}"


def _requests_response(url: str, html: str, *, status_code: int = 200) -> requests.Response:
    response = requests.Response()
    response.status_code = status_code
    response.url = url
    response._content = html.encode("utf-8")
    response.headers["Content-Type"] = "text/html; charset=utf-8"
    response.encoding = "utf-8"
    return response


class _RecordingHttpClient:
    def __init__(self, outcome: core.RequestOutcome) -> None:
        self.outcome = outcome
        self.calls: list[dict[str, object]] = []

    def request(
        self,
        url: str,
        *,
        source: str,
        allow_redirects: bool = True,
        timeout: int | None = None,
        proxy_selection: core.ProxySelection | None = None,
    ) -> core.RequestOutcome:
        self.calls.append(
            {
                "url": url,
                "source": source,
                "allow_redirects": allow_redirects,
                "timeout": timeout,
                "proxy_selection": proxy_selection,
            }
        )
        return self.outcome


class _BotGateOutcomeHttpClient:
    def __init__(self, html_for_url) -> None:
        self.html_for_url = html_for_url
        self.calls: list[dict[str, object]] = []

    def request(
        self,
        url: str,
        *,
        source: str,
        allow_redirects: bool = True,
        timeout: int | None = None,
        proxy_selection: core.ProxySelection | None = None,
    ) -> core.RequestOutcome:
        self.calls.append(
            {
                "url": url,
                "source": source,
                "allow_redirects": allow_redirects,
                "timeout": timeout,
                "proxy_selection": proxy_selection,
            }
        )
        return core.RequestOutcome(
            ok=False,
            status="bot_gate",
            response=_FakeHttpResponse(self.html_for_url(url), url=url),
            error="Bot/captcha gate detected",
            blocked=True,
            proxy_mode="proxy",
        )


class _CooldownAfterBotGateHttpClient:
    def __init__(self, html_for_url) -> None:
        self.html_for_url = html_for_url
        self.calls: list[dict[str, object]] = []
        self.network_urls: list[str] = []
        self.cooldown_urls: list[str] = []
        self.clear_calls: list[tuple[str, ...]] = []
        self._cooldown_hosts: set[str] = set()

    def request(
        self,
        url: str,
        *,
        source: str,
        allow_redirects: bool = True,
        timeout: int | None = None,
        proxy_selection: core.ProxySelection | None = None,
    ) -> core.RequestOutcome:
        host = url.split("/", 3)[2].lower() if "://" in url else ""
        self.calls.append(
            {
                "url": url,
                "source": source,
                "allow_redirects": allow_redirects,
                "timeout": timeout,
                "proxy_selection": proxy_selection,
            }
        )
        if host in self._cooldown_hosts:
            self.cooldown_urls.append(url)
            return core.RequestOutcome(
                ok=False,
                status="cooldown_active",
                host=host,
                cooldown_seconds=60,
                error=f"Host {host} is in cooldown for 60s",
                blocked=True,
                proxy_mode="proxy",
            )

        self.network_urls.append(url)
        self._cooldown_hosts.add(host)
        return core.RequestOutcome(
            ok=False,
            status="bot_gate",
            response=_FakeHttpResponse(self.html_for_url(url), url=url),
            error="Bot/captcha gate detected",
            blocked=True,
            proxy_mode="proxy",
        )

    def clear_host_cooldown(self, *hosts: str) -> None:
        self.clear_calls.append(tuple(hosts))
        for host in hosts:
            self._cooldown_hosts.discard(host)


def _batch(index: int, keyword: str) -> bicotender.BicotenderKeywordBatch:
    return bicotender.BicotenderKeywordBatch(
        index=index,
        terms=(keyword,),
        keywords=keyword,
        char_count=len(keyword),
    )


def _source_result_from_fake_fetcher(
    fetcher,
    batches: tuple[bicotender.BicotenderKeywordBatch, ...] = (
        _batch(1, "scrap"),
        _batch(2, "pipe"),
        _batch(3, "stamp"),
    ),
) -> core.SourceResult:
    source = bicotender.BicotenderSource(
        object(),
        keyword_batches=batches,
        fetcher=fetcher,
    )
    return source.search(core.RowInput(row_index=1, inn=VALID_INN, company_name="Factory"))


def _result_payload_with_bicotender(source_result: core.SourceResult) -> dict[str, object]:
    row = core.RowInput(row_index=1, inn=VALID_INN, company_name="Factory")
    result = core.build_company_result(row)
    result.status = "completed"
    result.sources["bicotender"] = source_result
    return core.serialize_company_result(result)


def _signal_payload(result_payload: dict[str, object]) -> dict[str, object]:
    return (
        ((result_payload.get("profile") or {}).get("signals") or {}).get("bicotender_public_list")
        if isinstance(result_payload.get("profile"), dict)
        else {}
    )


def _read_events(path) -> list[dict[str, object]]:
    import json

    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]


def _assert_primary_statuses_hide_internal_labels(signal: dict[str, object]) -> None:
    statuses = [str(signal.get("primary_status", ""))]
    statuses.extend(
        str((batch or {}).get("primary_status", ""))
        for batch in signal.get("keyword_batches", [])
        if isinstance(batch, dict)
    )
    forbidden = ("has_relevant_trade_signal", "review", "source_error", "partial_source_error")
    for status in statuses:
        for marker in forbidden:
            assert marker not in status


def test_bicotender_flat_export_maps_legacy_visible_public_list_signal_without_false_zero() -> None:
    result_payload: dict[str, object] = {
        "row_index": 1,
        "inn": VALID_INN,
        "company_name": "Factory",
        "status": "completed",
        "sources": {},
        "profile": {
            "summary": {
                "inn": VALID_INN,
                "company_name": "Factory",
                "processing_status": "completed",
            },
            "signals": {
                "bicotender_public_list": {
                    "schema_version": "bicotender_public_list.v2",
                    "primary_status": "1 visible keyword item across 3 keyword batches",
                    "visible_count": 1,
                    "planned_keyword_batch_count": 3,
                    "executed_keyword_batch_count": 3,
                    "source_state": "ok",
                    "operator_summary": {
                        "primary_status": "1 visible keyword item across 3 keyword batches",
                        "visible_count": 1,
                    },
                    "keyword_batches": [
                        {
                            "primary_status": "batch 1: 1 visible item",
                            "visible_count": 1,
                            "items": [{"tender_id": "1234567"}],
                        }
                    ],
                }
            },
        },
    }

    flat = core.flatten_company_result_for_export(result_payload)

    assert flat["bicotender_visible_count"] == 1
    assert flat["bicotender_visible_public_list_count"] == 1
    assert flat["bicotender_raw_visible_public_list_count"] == 1
    assert flat["bicotender_relevant_count"] == 1
    assert flat["bicotender_relevant_count_semantics"] == (
        "deprecated_compat_equals_visible_public_list_count_no_relevance_filter"
    )
    assert flat["bicotender_relevance_mode"] == "no_filter_visible_public_list_passthrough"


def test_bicotender_runtime_transport_policy_is_proxy_bound() -> None:
    assert core.source_requires_proxy_bound_transport("bicotender") is True
    assert (
        concurrency.DEFAULT_SOURCE_TRANSPORT_POLICY["bicotender"]
        == concurrency.PROXY_BOUND_TRANSPORT
    )
    assert "www.bicotender.ru" in concurrency.SOURCE_HOST_ALIASES["bicotender"]
    assert "bicotender" in proxies.PROXY_ASSISTED_SOURCE_NAMES


def test_bicotender_live_fetch_defers_proxy_selection_to_runtime_policy() -> None:
    html = f"""
    <form><input name="company[inn]" value="{VALID_INN}"></form>
    <div>{FOUND_ZERO_TENDERS}</div>
    """
    client = _RecordingHttpClient(
        core.RequestOutcome(
            ok=True,
            status="ok",
            response=_FakeHttpResponse(html),
            proxy_mode="proxy",
        )
    )
    source = bicotender.BicotenderSource(client, keyword_batches=(_batch(1, "scrap"),))

    result = source.search(core.RowInput(row_index=1, inn=VALID_INN, company_name="Factory"))

    assert result.status == "success"
    assert client.calls
    assert client.calls[0]["source"] == "bicotender"
    assert client.calls[0]["proxy_selection"] is None


def test_bicotender_no_proxy_boundary_returns_optional_source_issue_without_direct_fallback() -> None:
    client = _RecordingHttpClient(
        core.RequestOutcome(
            ok=False,
            status=core.REQUEST_STATUS_BLOCKED_NO_PROXY,
            error=core.REQUEST_BLOCKED_NO_PROXY_NO_POOL_REASON,
            blocked=True,
            proxy_mode="blocked_no_proxy",
        )
    )
    source = bicotender.BicotenderSource(client, keyword_batches=(_batch(1, "scrap"),))

    result = source.search(core.RowInput(row_index=1, inn=VALID_INN, company_name="Factory"))
    signal = _signal_payload(_result_payload_with_bicotender(result))

    assert result.status == "source_issue"
    assert core.REQUEST_BLOCKED_NO_PROXY_NO_POOL_REASON in result.errors
    assert client.calls
    assert client.calls[0]["proxy_selection"] is None
    assert signal["source_state"] == "source_issue"
    assert core.REQUEST_BLOCKED_NO_PROXY_NO_POOL_REASON in signal["preflight"]["errors"]


def test_bicotender_bot_gate_static_marker_public_rows_continue_to_keyword_batches() -> None:
    batches = (_batch(1, "scrap"), _batch(2, "pipe"), _batch(3, "stamp"))

    def html_for_url(url: str) -> str:
        keyword = ""
        if "keywords=scrap" in url:
            keyword = "scrap"
        elif "keywords=pipe" in url:
            keyword = "pipe"
        elif "keywords=stamp" in url:
            keyword = "stamp"
        keyword_input = f'<input name="keywords" value="{keyword}">' if keyword else ""
        if keyword in {"pipe", "stamp"}:
            return f"""
            <script src="/assets/captcha-modal.js"></script>
            <div class="modal">captcha can be shown inside a dismissible login widget</div>
            <form>
              <input name="company[inn]" value="{VALID_INN}">
              {keyword_input}
            </form>
            <div>{FOUND_ZERO_TENDERS}</div>
            """
        title = "Sale of scrap metal" if keyword == "scrap" else "Office supplies"
        href = "/metals/sale-scrap-tender1234567.html" if keyword == "scrap" else "/tender/555555"
        return f"""
        <script src="/assets/captcha-modal.js"></script>
        <div class="modal">captcha can be shown inside a dismissible login widget</div>
        <form>
          <input name="company[inn]" value="{VALID_INN}">
          {keyword_input}
        </form>
        <div>{FOUND_ONE_TENDER}</div>
        <article class="tender-card"><a href="{href}">{title}</a></article>
        """

    client = _BotGateOutcomeHttpClient(html_for_url)
    source = bicotender.BicotenderSource(client, keyword_batches=batches)

    source_result = source.search(core.RowInput(row_index=1, inn=VALID_INN, company_name="Factory"))
    signal = _signal_payload(_result_payload_with_bicotender(source_result))

    assert source_result.status == "success"
    assert len(client.calls) == 4
    assert signal["planned_keyword_batch_count"] == 3
    assert signal["executed_keyword_batch_count"] == 3
    assert signal["preflight"]["source_state"] == "ok"
    assert signal["preflight"]["errors"] == []
    assert signal["preflight"]["access_reason"] == "static_access_marker_present_but_public_rows_usable"
    assert signal["keyword_batches"][0]["source_state"] == "ok"
    assert signal["keyword_batches"][0]["errors"] == []
    assert signal["keyword_batches"][0]["access_reason"] == "static_access_marker_present_but_public_rows_usable"
    assert signal["keyword_batches"][1]["access_reason"] == "static_access_marker_present_but_query_applied_zero_results"
    assert signal["technical_internal"]["classification_status"] == "visible_public_items"


def test_bicotender_clears_usable_static_marker_bot_gate_cooldown_before_keyword_batches() -> None:
    batches = (_batch(1, "scrap"), _batch(2, "pipe"), _batch(3, "stamp"))

    def html_for_url(url: str) -> str:
        keyword = ""
        if "keywords=scrap" in url:
            keyword = "scrap"
        elif "keywords=pipe" in url:
            keyword = "pipe"
        elif "keywords=stamp" in url:
            keyword = "stamp"
        keyword_input = f'<input name="keywords" value="{keyword}">' if keyword else ""
        if keyword == "pipe":
            return f"""
            <script src="/assets/captcha-modal.js"></script>
            <div class="modal">captcha can be shown inside a dismissible login widget</div>
            <form>
              <input name="company[inn]" value="{VALID_INN}">
              {keyword_input}
            </form>
            <div>{FOUND_ZERO_TENDERS}</div>
            """
        title = "Sale of scrap metal" if keyword == "scrap" else "Office supplies"
        href = "/metals/sale-scrap-tender1234567.html" if keyword == "scrap" else "/tender/555555"
        return f"""
        <script src="/assets/captcha-modal.js"></script>
        <div class="modal">captcha can be shown inside a dismissible login widget</div>
        <form>
          <input name="company[inn]" value="{VALID_INN}">
          {keyword_input}
        </form>
        <div>{FOUND_ONE_TENDER}</div>
        <article class="tender-card"><a href="{href}">{title}</a></article>
        """

    client = _CooldownAfterBotGateHttpClient(html_for_url)
    source = bicotender.BicotenderSource(client, keyword_batches=batches)

    source_result = source.search(core.RowInput(row_index=1, inn=VALID_INN, company_name="Factory"))
    signal = _signal_payload(_result_payload_with_bicotender(source_result))

    assert source_result.status == "success"
    assert len(client.network_urls) == 4
    assert client.cooldown_urls == []
    assert len(client.clear_calls) == 4
    assert all("www.bicotender.ru" in hosts for hosts in client.clear_calls)
    assert signal["planned_keyword_batch_count"] == 3
    assert signal["executed_keyword_batch_count"] == 3
    assert signal["preflight"]["source_state"] == "ok"
    assert signal["preflight"]["errors"] == []
    assert signal["keyword_batches"][0]["source_state"] == "ok"
    assert signal["keyword_batches"][1]["access_reason"] == "static_access_marker_present_but_query_applied_zero_results"
    assert signal["technical_internal"]["classification_status"] == "visible_public_items"


def test_bicotender_recovers_proxy_quarantine_after_usable_static_marker_bot_gate(
    monkeypatch,
    tmp_path,
) -> None:
    monkeypatch.setenv("PARSER_PROXY_PRIORITY_RESERVED_CAPACITY", "0")
    proxy_pool = proxies.ProxyPool(
        "http://proxy-1.local:8080",
        strategy="round_robin",
        ban_cooldown_seconds=60,
    )
    client = core.RateLimitedHttpClient(
        logger=logging.getLogger("tests.bicotender.runtime_proxy_recovery"),
        progress_store=core.ProgressStore(tmp_path / "progress"),
        min_delay_by_host={"www.bicotender.ru": 0.0},
        request_timeout=5,
        cooldown_on_429=60,
        cooldown_on_bot=60,
        proxy_pool=proxy_pool,
    )
    batches = (_batch(1, "scrap"), _batch(2, "pipe"), _batch(3, "stamp"))
    session_get_calls: list[dict[str, object]] = []

    def html_for_url(url: str) -> str:
        keyword = ""
        if "keywords=scrap" in url:
            keyword = "scrap"
        elif "keywords=pipe" in url:
            keyword = "pipe"
        elif "keywords=stamp" in url:
            keyword = "stamp"
        keyword_input = f'<input name="keywords" value="{keyword}">' if keyword else ""
        count_text = FOUND_ONE_TENDER if keyword in {"", "scrap"} else FOUND_ZERO_TENDERS
        article = (
            '<article class="tender-card"><a href="/metals/sale-scrap-tender1234567.html">Sale of scrap metal</a></article>'
            if keyword in {"", "scrap"}
            else ""
        )
        return f"""
        <script src="/assets/captcha-modal.js"></script>
        <div class="modal">captcha can be shown inside a dismissible login widget</div>
        <form>
          <input name="company[inn]" value="{VALID_INN}">
          {keyword_input}
        </form>
        <div>{count_text}</div>
        {article}
        """

    def fake_get(url: str, **kwargs: object) -> requests.Response:
        session_get_calls.append(dict(kwargs))
        assert kwargs.get("proxies") == {
            "http": "http://proxy-1.local:8080",
            "https": "http://proxy-1.local:8080",
        }
        return _requests_response(url, html_for_url(url))

    monkeypatch.setattr(client.session, "get", fake_get)
    source = bicotender.BicotenderSource(client, keyword_batches=batches)

    source_result = source.search(core.RowInput(row_index=1, inn=VALID_INN, company_name="Factory"))
    signal = _signal_payload(_result_payload_with_bicotender(source_result))
    events = _read_events(client.progress_store.events_jsonl)

    assert source_result.status == "success"
    assert len(session_get_calls) == 4
    assert proxy_pool.usable_count(source_name="bicotender") == 1
    assert proxy_pool.entries[0].failures == 0
    assert not any(event.get("type") == "request_blocked_by_policy" for event in events)
    assert [event.get("type") for event in events].count("bot_gate") == 4
    assert [event.get("type") for event in events].count("request_soft_gate_recovered") == 4
    recovery_events = [event for event in events if event.get("type") == "request_soft_gate_recovered"]
    assert {
        event.get("proxy_lifecycle_state")
        for event in recovery_events
    } == {proxies.PROXY_LIFECYCLE_PARSER_PROVEN_RECOVERY}
    assert {
        event.get("proxy_lifecycle_recovery_class")
        for event in recovery_events
    } == {"source_scoped_recovered_proxy_eligibility"}
    assert signal["planned_keyword_batch_count"] == 3
    assert signal["executed_keyword_batch_count"] == 3
    assert signal["technical_internal"]["classification_status"] == "visible_public_items"


def test_bicotender_recovered_proxy_stays_eligible_after_other_source_proxy_errors(
    monkeypatch,
    tmp_path,
) -> None:
    monkeypatch.delenv("PARSER_PROXY_PRIORITY_RESERVED_CAPACITY", raising=False)
    proxy_pool = proxies.ProxyPool(
        ",".join(f"http://proxy-{index}.local:8080" for index in range(1, 5)),
        strategy="round_robin",
        ban_cooldown_seconds=300,
    )
    client = core.RateLimitedHttpClient(
        logger=logging.getLogger("tests.bicotender.runtime_proxy_recovery"),
        progress_store=core.ProgressStore(tmp_path / "progress"),
        min_delay_by_host={"www.bicotender.ru": 0.0},
        request_timeout=5,
        cooldown_on_429=60,
        cooldown_on_bot=60,
        proxy_pool=proxy_pool,
    )
    batches = (_batch(1, "scrap"), _batch(2, "pipe"), _batch(3, "stamp"))
    session_get_calls: list[dict[str, object]] = []

    def html_for_url(url: str) -> str:
        keyword = ""
        if "keywords=scrap" in url:
            keyword = "scrap"
        elif "keywords=pipe" in url:
            keyword = "pipe"
        elif "keywords=stamp" in url:
            keyword = "stamp"
        keyword_input = f'<input name="keywords" value="{keyword}">' if keyword else ""
        count_text = FOUND_ONE_TENDER if keyword in {"", "scrap"} else FOUND_ZERO_TENDERS
        article = (
            '<article class="tender-card"><a href="/metals/sale-scrap-tender1234567.html">Sale of scrap metal</a></article>'
            if keyword in {"", "scrap"}
            else ""
        )
        return f"""
        <script src="/assets/captcha-modal.js"></script>
        <div class="modal">captcha can be shown inside a dismissible login widget</div>
        <form>
          <input name="company[inn]" value="{VALID_INN}">
          {keyword_input}
        </form>
        <div>{count_text}</div>
        {article}
        """

    def fake_get(url: str, **kwargs: object) -> requests.Response:
        proxies_arg = kwargs.get("proxies") or {}
        proxy_url = str((proxies_arg or {}).get("https") or (proxies_arg or {}).get("http") or "")
        session_get_calls.append({"url": url, "proxy_url": proxy_url})
        assert proxy_url
        return _requests_response(url, html_for_url(url))

    monkeypatch.setattr(client.session, "get", fake_get)
    source = bicotender.BicotenderSource(client, keyword_batches=batches)

    source_result = source.search(core.RowInput(row_index=1, inn=VALID_INN, company_name="Factory"))

    assert source_result.status == "success"
    recovered_proxy_urls = [str(call["proxy_url"]) for call in session_get_calls]
    assert len(recovered_proxy_urls) == 4
    assert len(set(recovered_proxy_urls)) == 4
    assert proxy_pool.usable_count(source_name="bicotender") == 3

    for proxy_url in recovered_proxy_urls:
        proxy_pool.mark_bad(proxy_url, reason="proxy_tunnel_error", source_name="company_site")

    assert proxy_pool.usable_count(source_name="company_site") == 0
    assert proxy_pool.usable_count(source_name="bicotender") == 3

    session_get_calls.clear()
    later_outcome = client.request(
        "https://www.bicotender.ru/tender/search/?company%5Binn%5D=5011037650",
        source="bicotender",
        timeout=20,
    )
    events = _read_events(client.progress_store.events_jsonl)

    assert later_outcome.status == "bot_gate"
    assert len(session_get_calls) == 1
    assert str(session_get_calls[0]["proxy_url"]) in set(recovered_proxy_urls)
    selected_entry = next(entry for entry in proxy_pool.entries if entry.url == str(session_get_calls[0]["proxy_url"]))
    assert "bicotender" not in selected_entry.recovered_sources
    assert not any(event.get("type") == "request_blocked_by_policy" for event in events)
    assert not any(event.get("type") == "request_proxy_pool_sync" for event in events)


def test_bicotender_bot_gate_without_public_rows_stays_source_issue() -> None:
    def challenge_html(_url: str) -> str:
        return """
        <html><body>
          <h1>access denied</h1>
          <div>captcha</div>
        </body></html>
        """

    client = _CooldownAfterBotGateHttpClient(challenge_html)
    source = bicotender.BicotenderSource(client, keyword_batches=(_batch(1, "scrap"),))

    source_result = source.search(core.RowInput(row_index=1, inn=VALID_INN, company_name="Factory"))
    signal = _signal_payload(_result_payload_with_bicotender(source_result))

    assert source_result.status == "source_issue"
    assert len(client.calls) == 1
    assert len(client.network_urls) == 1
    assert client.cooldown_urls == []
    assert client.clear_calls == []
    assert signal["executed_keyword_batch_count"] == 0
    assert signal["preflight"]["source_state"] == "source_issue"
    assert "Bot/captcha gate detected" in signal["preflight"]["errors"]
    assert "hard_challenge_or_access_denied_without_usable_public_rows" in signal["preflight"]["errors"]
    assert signal["technical_internal"]["classification_status"] == "source_error"


def test_bicotender_positive_signal_reaches_profile_report_and_dossier_payload() -> None:
    def fetcher(query: bicotender.BicotenderSearchQuery) -> bicotender.BicotenderFetchResponse:
        if not query.keywords:
            return bicotender.BicotenderFetchResponse(
                html=f"""
                <form><input name="company[inn]" value="{VALID_INN}"></form>
                <div>{FOUND_ONE_TENDER}</div>
                <article class="tender-card"><a href="/tender/555555">Office supplies</a></article>
                """
            )
        if query.keywords == "scrap":
            return bicotender.BicotenderFetchResponse(
                html=f"""
                <form>
                  <input name="company[inn]" value="{VALID_INN}">
                  <input name="keywords" value="{query.keywords}">
                </form>
                <div>{FOUND_ONE_TENDER}</div>
                <article class="tender-card">
                  <span>Tender #1234567</span>
                  <a href="/metals/sale-scrap-tender1234567.html">Sale of scrap metal</a>
                  <span>Region: Moscow Category: Metallurgy Date: 12.05.2026 Price: 100 000 rub. Procedure: sale</span>
                </article>
                """
            )
        if query.keywords == "pipe":
            return bicotender.BicotenderFetchResponse(
                html=f"""
                <form>
                  <input name="company[inn]" value="{VALID_INN}">
                  <input name="keywords" value="{query.keywords}">
                </form>
                <div>{FOUND_ZERO_TENDERS}</div>
                """
            )
        return bicotender.BicotenderFetchResponse(html="", http_status=500, error="source_timeout")

    source_result = _source_result_from_fake_fetcher(fetcher)
    result_payload = _result_payload_with_bicotender(source_result)
    signal = _signal_payload(result_payload)

    assert source_result.status == "partial_success"
    assert signal["schema_version"] == "bicotender_public_list.v2"
    assert signal["relevance_mode"] == "no_filter_visible_public_list_passthrough"
    assert signal["visible_public_list_count"] == 1
    assert signal["raw_visible_public_list_count"] == 1
    assert signal["relevant_count"] == 1
    assert signal["relevant_count_semantics"] == (
        "deprecated_compat_equals_visible_public_list_count_no_relevance_filter"
    )
    assert signal["primary_status"] == "partial source error with 1 visible keyword item across 3 keyword batches"
    assert signal["planned_keyword_batch_count"] == 3
    assert signal["executed_keyword_batch_count"] == 3
    assert signal["preflight"]["primary_status"] == "INN preflight: 1 visible public item of 1 total"
    assert len(signal["keyword_batches"]) == 3
    first_item = signal["keyword_batches"][0]["items"][0]
    assert first_item["tender_id"] == "1234567"
    assert first_item["item_url"].endswith("/metals/sale-scrap-tender1234567.html")
    assert first_item["detail_url"].endswith("/metals/sale-scrap-tender1234567.html")
    assert first_item["matched_positive_terms"] == ["scrap"]
    assert first_item["evidence_quality"] == "list_page_only"
    assert first_item["detail_fetched"] is False
    assert first_item["documents_accessed"] is False
    assert signal["keyword_batches"][2]["source_state"] == "source_issue"
    assert signal["technical_internal"]["classification_status"] == "partial_source_error"
    _assert_primary_statuses_hide_internal_labels(signal)

    report = core.render_company_report_markdown(result_payload)
    assert "## Bicotender Public List Evidence" in report
    assert "tender detail pages and documents were not fetched" in report
    assert "partial source error with 1 visible keyword item across 3 keyword batches" in report
    assert "Counts: relevant=" not in report
    assert "relevant results" not in report
    assert "has_relevant_trade_signal" not in report
    assert "no_filter_visible_public_list_passthrough" in report
    assert "Public-list counts: visible=" in report

    flat = core.flatten_company_result_for_export(result_payload)
    assert flat["bicotender_public_list_schema_version"] == "bicotender_public_list.v2"
    assert flat["bicotender_relevance_mode"] == "no_filter_visible_public_list_passthrough"
    assert flat["bicotender_visible_count"] == 1
    assert flat["bicotender_visible_public_list_count"] == 1
    assert flat["bicotender_raw_visible_public_list_count"] == 1
    assert flat["bicotender_relevant_count"] == 1
    assert flat["bicotender_relevant_count_semantics"] == (
        "deprecated_compat_equals_visible_public_list_count_no_relevance_filter"
    )

    dossier_payload = build_company_dossier(result=result_payload).to_dict()
    dossier_signal = dossier_payload["company_metadata"]["profile"]["signals"]["bicotender_public_list"]
    assert dossier_signal["primary_status"] == "partial source error with 1 visible keyword item across 3 keyword batches"
    assert dossier_signal["visible_public_list_count"] == 1
    assert dossier_signal["raw_visible_public_list_count"] == 1
    assert dossier_signal["relevant_count"] == 1
    assert dossier_signal["keyword_batches"][0]["items"][0]["detail_fetched"] is False


def test_bicotender_zero_inn_public_items_are_profiled_as_clean_skip() -> None:
    calls: list[str] = []

    def fetcher(query: bicotender.BicotenderSearchQuery) -> bicotender.BicotenderFetchResponse:
        calls.append(query.keywords or "preflight")
        return bicotender.BicotenderFetchResponse(
            html=f"""
            <form><input name="company[inn]" value="{VALID_INN}"></form>
            <div>{FOUND_ZERO_TENDERS}</div>
            """
        )

    source_result = _source_result_from_fake_fetcher(fetcher)
    signal = _signal_payload(_result_payload_with_bicotender(source_result))

    assert calls == ["preflight"]
    assert source_result.status == "success"
    assert signal["primary_status"] == "0 visible keyword items across 3 keyword batches"
    assert signal["preflight"]["source_state"] == "ok"
    assert signal["preflight"]["primary_status"] == "no public items by INN"
    assert signal["keyword_batches"] == []
    assert signal["keyword_batches_skipped_count"] == 3
    assert signal["keyword_batches_skipped"][0]["skip_reason"] == "inn_only_preflight_zero_applied_results"
    assert signal["technical_internal"]["classification_status"] == "no_public_items_by_inn"
    _assert_primary_statuses_hide_internal_labels(signal)


def test_bicotender_no_keyword_items_after_preflight_is_distinct_from_source_issue() -> None:
    def fetcher(query: bicotender.BicotenderSearchQuery) -> bicotender.BicotenderFetchResponse:
        if not query.keywords:
            return bicotender.BicotenderFetchResponse(
                html=f"""
                <form><input name="company[inn]" value="{VALID_INN}"></form>
                <div>{FOUND_ONE_TENDER}</div>
                <article class="tender-card"><a href="/tender/555555">Office supplies</a></article>
                """
            )
        return bicotender.BicotenderFetchResponse(
            html=f"""
            <form>
              <input name="company[inn]" value="{VALID_INN}">
              <input name="keywords" value="{query.keywords}">
            </form>
            <div>{FOUND_ZERO_TENDERS}</div>
            """
        )

    signal = _signal_payload(_result_payload_with_bicotender(_source_result_from_fake_fetcher(fetcher)))

    assert signal["primary_status"] == "0 visible keyword items across 3 keyword batches"
    assert signal["source_state"] == "ok"
    assert signal["technical_internal"]["classification_status"] == "no_keyword_items_after_inn_preflight"
    assert [batch["primary_status"] for batch in signal["keyword_batches"]] == [
        "batch 1: 0 visible items",
        "batch 2: 0 visible items",
        "batch 3: 0 visible items",
    ]
    _assert_primary_statuses_hide_internal_labels(signal)


def test_bicotender_partial_source_issue_preserves_counts_and_diagnostics_separately() -> None:
    batches = (_batch(1, "scrap"), _batch(2, "stamp"))

    def fetcher(query: bicotender.BicotenderSearchQuery) -> bicotender.BicotenderFetchResponse:
        if not query.keywords:
            return bicotender.BicotenderFetchResponse(
                html=f"""
                <form><input name="company[inn]" value="{VALID_INN}"></form>
                <div>{FOUND_ONE_TENDER}</div>
                <article class="tender-card"><a href="/tender/555555">Office supplies</a></article>
                """
            )
        if query.keywords == "scrap":
            return bicotender.BicotenderFetchResponse(
                html=f"""
                <form>
                  <input name="company[inn]" value="{VALID_INN}">
                  <input name="keywords" value="{query.keywords}">
                </form>
                <div>{FOUND_ZERO_TENDERS}</div>
                """
            )
        return bicotender.BicotenderFetchResponse(html="", http_status=500, error="source_timeout")

    source_result = _source_result_from_fake_fetcher(fetcher, batches=batches)
    signal = _signal_payload(_result_payload_with_bicotender(source_result))

    assert source_result.status == "partial_success"
    assert signal["primary_status"] == "partial source error with 0 visible keyword items across 2 keyword batches"
    assert signal["source_state"] == "source_issue"
    assert signal["technical_internal"]["classification_status"] == "partial_source_error"
    assert signal["keyword_batches"][1]["source_state"] == "source_issue"
    assert "source_timeout" in signal["keyword_batches"][1]["access_note"]
    _assert_primary_statuses_hide_internal_labels(signal)

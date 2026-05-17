from __future__ import annotations

import logging
from pathlib import Path

import pytest
import requests

import company_enrichment_core as core
from app.sources.rusprofile import RUSPROFILE_ORIGIN, RusprofileSource


class _ProxyPoolStub:
    entries: list[object] = []

    def select(self, host: str | None = None, *, source_name: str | None = None) -> object:
        return object()

    def mark_bad(self, proxy_url: str | None, *, reason: str = "", source_name: str | None = None) -> None:
        return None

    def mark_ok(self, proxy_url: str | None, *, source_name: str | None = None) -> None:
        return None


def _build_client(tmp_path: Path) -> core.RateLimitedHttpClient:
    return core.RateLimitedHttpClient(
        logger=logging.getLogger("tests.rusprofile_session_resilience"),
        progress_store=core.ProgressStore(tmp_path / "progress"),
        min_delay_by_host={},
        request_timeout=5,
        cooldown_on_429=60,
        cooldown_on_bot=60,
        proxy_pool=_ProxyPoolStub(),
    )


def _build_response(url: str, *, status_code: int = 200, text: str = "ok") -> requests.Response:
    response = requests.Response()
    response.status_code = status_code
    response.url = url
    response._content = text.encode("utf-8")
    response.headers["Content-Type"] = "text/html; charset=utf-8"
    response.encoding = "utf-8"
    return response


def _remote_disconnected_outcome() -> core.RequestOutcome:
    return core.RequestOutcome(
        ok=False,
        status="request_error",
        error="('Connection aborted.', RemoteDisconnected('Remote end closed connection without response'))",
        host="www.rusprofile.ru",
    )


def _read_timeout_outcome() -> core.RequestOutcome:
    return core.RequestOutcome(
        ok=False,
        status="request_error",
        error="HTTPSConnectionPool(host='www.rusprofile.ru', port=443): Read timed out. (read timeout=18)",
        host="www.rusprofile.ru",
        timeout=True,
    )


def _connect_timeout_outcome() -> core.RequestOutcome:
    return core.RequestOutcome(
        ok=False,
        status="request_error",
        error=(
            "HTTPSConnectionPool(host='www.rusprofile.ru', port=443): "
            "Max retries exceeded with url: /search?query=7804309200 "
            "(Caused by ConnectTimeoutError("
            "'Connection to www.rusprofile.ru timed out. (connect timeout=18)'))"
        ),
        host="www.rusprofile.ru",
        timeout=True,
    )


def _dns_name_resolution_outcome() -> core.RequestOutcome:
    return core.RequestOutcome(
        ok=False,
        status="request_error",
        error=(
            "HTTPSConnectionPool(host='www.rusprofile.ru', port=443): "
            "Max retries exceeded with url: /search?query=7804309200 "
            "(Caused by NameResolutionError(\"<urllib3.connection.HTTPSConnection object>: "
            "Failed to resolve 'www.rusprofile.ru' ([Errno 11001] getaddrinfo failed)\"))"
        ),
        host="www.rusprofile.ru",
    )


def _ssl_eof_outcome() -> core.RequestOutcome:
    return core.RequestOutcome(
        ok=False,
        status="request_error",
        error=(
            "HTTPSConnectionPool(host='www.rusprofile.ru', port=443): "
            "SSLError(SSLEOFError(8, '[SSL: UNEXPECTED_EOF_WHILE_READING] "
            "EOF occurred in violation of protocol (_ssl.c:1000)'))"
        ),
        host="www.rusprofile.ru",
    )


def _http_503_outcome(url: str) -> core.RequestOutcome:
    return core.RequestOutcome(
        ok=False,
        status="http_503",
        response=_build_response(url, status_code=503, text="Service unavailable"),
        error="HTTP 503",
        host="www.rusprofile.ru",
    )


def _non_retryable_request_error_outcome() -> core.RequestOutcome:
    return core.RequestOutcome(
        ok=False,
        status="request_error",
        error="invalid URL/host: bad rusprofile search URL",
        host="www.rusprofile.ru",
    )


def _authorized_entity_html(inn: str) -> str:
    return f"""
    <html>
      <head>
        <title>Test Company</title>
        <meta name="description" content="Company INN {inn}">
      </head>
      <body>
        <div id="main_info" data-user="true"></div>
        <h1>Test Company</h1>
        <dl>
          <dt>INN</dt>
          <dd>{inn}</dd>
        </dl>
      </body>
    </html>
    """


def _build_source(client: core.RateLimitedHttpClient) -> RusprofileSource:
    source = RusprofileSource(client)
    source._auth_checked = True
    source._auth_ok = True
    source._auth_method = "existing_or_profile_session"
    return source


def test_rusprofile_search_recovers_from_single_remote_disconnected_after_auth(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    client = _build_client(tmp_path)
    source = _build_source(client)
    row = core.RowInput(row_index=58, inn="7703770101", company_name="Test Company")
    search_url = f"{RUSPROFILE_ORIGIN}/search?query={row.inn}"
    entity_url = f"{RUSPROFILE_ORIGIN}/id/123456"
    entity_response = _build_response(entity_url, text=_authorized_entity_html(row.inn))
    outcomes = [
        _remote_disconnected_outcome(),
        core.RequestOutcome(ok=True, status="ok", response=entity_response, host="www.rusprofile.ru"),
    ]
    requested_urls: list[str] = []

    def fake_request(url: str, *, source: str, **kwargs: object) -> core.RequestOutcome:
        assert source == "rusprofile"
        requested_urls.append(url)
        return outcomes.pop(0)

    monkeypatch.setattr(client, "request", fake_request)

    result = source.search(row)

    assert result.status == "success"
    assert result.entity_url == entity_url
    assert result.links == [search_url, entity_url]
    assert result.company_name_found == "Test Company"
    assert requested_urls == [search_url, search_url]


def test_rusprofile_search_recovers_from_single_http_503_after_auth(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    client = _build_client(tmp_path)
    source = _build_source(client)
    row = core.RowInput(row_index=276, inn="7743238020", company_name="Test Company")
    search_url = f"{RUSPROFILE_ORIGIN}/search?query={row.inn}"
    entity_url = f"{RUSPROFILE_ORIGIN}/id/123456"
    entity_response = _build_response(entity_url, text=_authorized_entity_html(row.inn))
    outcomes = [
        _http_503_outcome(search_url),
        core.RequestOutcome(ok=True, status="ok", response=entity_response, host="www.rusprofile.ru"),
    ]
    requested_urls: list[str] = []

    def fake_request(url: str, *, source: str, **kwargs: object) -> core.RequestOutcome:
        assert source == "rusprofile"
        requested_urls.append(url)
        return outcomes.pop(0)

    monkeypatch.setattr(client, "request", fake_request)

    result = source.search(row)

    assert result.status == "success"
    assert result.entity_url == entity_url
    assert requested_urls == [search_url, search_url]
    retry_events = [
        event
        for event in client.progress_store.events_jsonl.read_text(encoding="utf-8").splitlines()
        if "rusprofile_transient_request_retry" in event
    ]
    assert len(retry_events) == 1
    assert '"request_status": "http_503"' in retry_events[0]
    assert "HTTP 503" in retry_events[0]


def test_rusprofile_search_recovers_from_single_ssl_eof_after_auth(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    client = _build_client(tmp_path)
    source = _build_source(client)
    row = core.RowInput(row_index=58, inn="7804309200", company_name="Test Company")
    search_url = f"{RUSPROFILE_ORIGIN}/search?query={row.inn}"
    entity_url = f"{RUSPROFILE_ORIGIN}/id/123456"
    entity_response = _build_response(entity_url, text=_authorized_entity_html(row.inn))
    outcomes = [
        _ssl_eof_outcome(),
        core.RequestOutcome(ok=True, status="ok", response=entity_response, host="www.rusprofile.ru"),
    ]
    requested_urls: list[str] = []

    def fake_request(url: str, *, source: str, **kwargs: object) -> core.RequestOutcome:
        assert source == "rusprofile"
        requested_urls.append(url)
        return outcomes.pop(0)

    monkeypatch.setattr(client, "request", fake_request)

    result = source.search(row)

    assert result.status == "success"
    assert result.entity_url == entity_url
    assert requested_urls == [search_url, search_url]
    retry_events = [
        event
        for event in client.progress_store.events_jsonl.read_text(encoding="utf-8").splitlines()
        if "rusprofile_transient_request_retry" in event
    ]
    assert len(retry_events) == 1
    assert "UNEXPECTED_EOF_WHILE_READING" in retry_events[0]


def test_rusprofile_search_keeps_request_error_when_remote_disconnected_repeats(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    client = _build_client(tmp_path)
    source = _build_source(client)
    row = core.RowInput(row_index=58, inn="7703770101", company_name="Test Company")
    search_url = f"{RUSPROFILE_ORIGIN}/search?query={row.inn}"
    outcomes = [_remote_disconnected_outcome(), _remote_disconnected_outcome()]
    requested_urls: list[str] = []

    def fake_request(url: str, *, source: str, **kwargs: object) -> core.RequestOutcome:
        assert source == "rusprofile"
        requested_urls.append(url)
        return outcomes.pop(0)

    monkeypatch.setattr(client, "request", fake_request)

    result = source.search(row)

    assert result.status == "request_error"
    assert requested_urls == [search_url, search_url]
    assert any("Remote end closed connection without response" in error for error in result.errors)
    assert "Remote end closed connection without response" in core.resolve_source_block_reason(result)
    assert core.source_result_requires_run_fail_fast(
        "rusprofile",
        result.status,
        access_mode=core.SESSION_BOUND_TRANSPORT,
    )


def test_rusprofile_search_keeps_request_error_when_connect_timeout_repeats(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    client = _build_client(tmp_path)
    source = _build_source(client)
    row = core.RowInput(row_index=58, inn="7804309200", company_name="Test Company")
    search_url = f"{RUSPROFILE_ORIGIN}/search?query={row.inn}"
    outcomes = [_connect_timeout_outcome(), _connect_timeout_outcome()]
    requested_urls: list[str] = []

    def fake_request(url: str, *, source: str, **kwargs: object) -> core.RequestOutcome:
        assert source == "rusprofile"
        requested_urls.append(url)
        return outcomes.pop(0)

    monkeypatch.setattr(client, "request", fake_request)

    result = source.search(row)

    assert result.status == "request_error"
    assert requested_urls == [search_url, search_url]
    assert any("ConnectTimeoutError" in error for error in result.errors)
    assert "connect timeout=18" in core.resolve_source_block_reason(result)
    assert core.source_result_requires_run_fail_fast(
        "rusprofile",
        result.status,
        access_mode=core.SESSION_BOUND_TRANSPORT,
    )


def test_rusprofile_search_keeps_request_error_when_ssl_eof_repeats(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    client = _build_client(tmp_path)
    source = _build_source(client)
    row = core.RowInput(row_index=58, inn="7804309200", company_name="Test Company")
    search_url = f"{RUSPROFILE_ORIGIN}/search?query={row.inn}"
    outcomes = [_ssl_eof_outcome(), _ssl_eof_outcome()]
    requested_urls: list[str] = []

    def fake_request(url: str, *, source: str, **kwargs: object) -> core.RequestOutcome:
        assert source == "rusprofile"
        requested_urls.append(url)
        return outcomes.pop(0)

    monkeypatch.setattr(client, "request", fake_request)

    result = source.search(row)

    assert result.status == "request_error"
    assert requested_urls == [search_url, search_url]
    assert any("UNEXPECTED_EOF_WHILE_READING" in error for error in result.errors)
    assert "UNEXPECTED_EOF_WHILE_READING" in core.resolve_source_block_reason(result)
    assert core.source_result_requires_run_fail_fast(
        "rusprofile",
        result.status,
        access_mode=core.SESSION_BOUND_TRANSPORT,
    )


def test_rusprofile_search_keeps_http_503_when_http_503_repeats(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    client = _build_client(tmp_path)
    source = _build_source(client)
    row = core.RowInput(row_index=276, inn="7743238020", company_name="Test Company")
    search_url = f"{RUSPROFILE_ORIGIN}/search?query={row.inn}"
    outcomes = [_http_503_outcome(search_url), _http_503_outcome(search_url)]
    requested_urls: list[str] = []

    def fake_request(url: str, *, source: str, **kwargs: object) -> core.RequestOutcome:
        assert source == "rusprofile"
        requested_urls.append(url)
        return outcomes.pop(0)

    monkeypatch.setattr(client, "request", fake_request)

    result = source.search(row)

    assert result.status == "http_503"
    assert requested_urls == [search_url, search_url]
    assert result.errors == ["HTTP 503"]
    assert core.resolve_source_block_reason(result) == "HTTP 503"
    assert core.source_result_requires_run_fail_fast(
        "rusprofile",
        result.status,
        access_mode=core.SESSION_BOUND_TRANSPORT,
    )


def test_rusprofile_search_recovers_from_single_read_timeout_after_auth(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    client = _build_client(tmp_path)
    source = _build_source(client)
    row = core.RowInput(row_index=58, inn="7804309200", company_name="Test Company")
    search_url = f"{RUSPROFILE_ORIGIN}/search?query={row.inn}"
    entity_url = f"{RUSPROFILE_ORIGIN}/id/123456"
    entity_response = _build_response(entity_url, text=_authorized_entity_html(row.inn))
    outcomes = [
        _read_timeout_outcome(),
        core.RequestOutcome(ok=True, status="ok", response=entity_response, host="www.rusprofile.ru"),
    ]
    requested_urls: list[str] = []

    def fake_request(url: str, *, source: str, **kwargs: object) -> core.RequestOutcome:
        assert source == "rusprofile"
        requested_urls.append(url)
        return outcomes.pop(0)

    monkeypatch.setattr(client, "request", fake_request)

    result = source.search(row)

    assert result.status == "success"
    assert result.entity_url == entity_url
    assert requested_urls == [search_url, search_url]
    retry_events = [
        event
        for event in client.progress_store.events_jsonl.read_text(encoding="utf-8").splitlines()
        if "rusprofile_transient_request_retry" in event
    ]
    assert len(retry_events) == 1
    assert "Read timed out" in retry_events[0]


def test_rusprofile_search_recovers_from_single_connect_timeout_after_auth(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    client = _build_client(tmp_path)
    source = _build_source(client)
    row = core.RowInput(row_index=58, inn="7804309200", company_name="Test Company")
    search_url = f"{RUSPROFILE_ORIGIN}/search?query={row.inn}"
    entity_url = f"{RUSPROFILE_ORIGIN}/id/123456"
    entity_response = _build_response(entity_url, text=_authorized_entity_html(row.inn))
    outcomes = [
        _connect_timeout_outcome(),
        core.RequestOutcome(ok=True, status="ok", response=entity_response, host="www.rusprofile.ru"),
    ]
    requested_urls: list[str] = []

    def fake_request(url: str, *, source: str, **kwargs: object) -> core.RequestOutcome:
        assert source == "rusprofile"
        requested_urls.append(url)
        return outcomes.pop(0)

    monkeypatch.setattr(client, "request", fake_request)

    result = source.search(row)

    assert result.status == "success"
    assert result.entity_url == entity_url
    assert requested_urls == [search_url, search_url]
    retry_events = [
        event
        for event in client.progress_store.events_jsonl.read_text(encoding="utf-8").splitlines()
        if "rusprofile_transient_request_retry" in event
    ]
    assert len(retry_events) == 1
    assert "ConnectTimeoutError" in retry_events[0]
    assert "connect timeout=18" in retry_events[0]


def test_rusprofile_search_recovers_from_single_dns_name_resolution_after_auth(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    client = _build_client(tmp_path)
    source = _build_source(client)
    row = core.RowInput(row_index=58, inn="7804309200", company_name="Test Company")
    search_url = f"{RUSPROFILE_ORIGIN}/search?query={row.inn}"
    entity_url = f"{RUSPROFILE_ORIGIN}/id/123456"
    entity_response = _build_response(entity_url, text=_authorized_entity_html(row.inn))
    outcomes = [
        _dns_name_resolution_outcome(),
        core.RequestOutcome(ok=True, status="ok", response=entity_response, host="www.rusprofile.ru"),
    ]
    requested_urls: list[str] = []

    def fake_request(url: str, *, source: str, **kwargs: object) -> core.RequestOutcome:
        assert source == "rusprofile"
        requested_urls.append(url)
        return outcomes.pop(0)

    monkeypatch.setattr(client, "request", fake_request)

    result = source.search(row)

    assert result.status == "success"
    assert result.entity_url == entity_url
    assert requested_urls == [search_url, search_url]
    retry_events = [
        event
        for event in client.progress_store.events_jsonl.read_text(encoding="utf-8").splitlines()
        if "rusprofile_transient_request_retry" in event
    ]
    assert len(retry_events) == 1
    assert "NameResolutionError" in retry_events[0]
    assert "getaddrinfo failed" in retry_events[0]


def test_rusprofile_entity_fetch_recovers_from_single_http_503_after_auth(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    client = _build_client(tmp_path)
    source = _build_source(client)
    row = core.RowInput(row_index=276, inn="7743238020", company_name="Test Company")
    search_url = f"{RUSPROFILE_ORIGIN}/search?query={row.inn}"
    entity_url = f"{RUSPROFILE_ORIGIN}/id/123456"
    listing_response = _build_response(search_url, text='<a href="/id/123456">Test Company</a>')
    entity_response = _build_response(entity_url, text=_authorized_entity_html(row.inn))
    outcomes = [
        core.RequestOutcome(ok=True, status="ok", response=listing_response, host="www.rusprofile.ru"),
        _http_503_outcome(entity_url),
        core.RequestOutcome(ok=True, status="ok", response=entity_response, host="www.rusprofile.ru"),
    ]
    requested_urls: list[str] = []

    def fake_request(url: str, *, source: str, **kwargs: object) -> core.RequestOutcome:
        assert source == "rusprofile"
        requested_urls.append(url)
        return outcomes.pop(0)

    monkeypatch.setattr(client, "request", fake_request)

    result = source.search(row)

    assert result.status == "success"
    assert result.entity_url == entity_url
    assert requested_urls == [search_url, entity_url, entity_url]


def test_rusprofile_entity_fetch_recovers_from_single_ssl_eof_after_auth(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    client = _build_client(tmp_path)
    source = _build_source(client)
    row = core.RowInput(row_index=276, inn="7743238020", company_name="Test Company")
    search_url = f"{RUSPROFILE_ORIGIN}/search?query={row.inn}"
    entity_url = f"{RUSPROFILE_ORIGIN}/id/123456"
    listing_response = _build_response(search_url, text='<a href="/id/123456">Test Company</a>')
    entity_response = _build_response(entity_url, text=_authorized_entity_html(row.inn))
    outcomes = [
        core.RequestOutcome(ok=True, status="ok", response=listing_response, host="www.rusprofile.ru"),
        _ssl_eof_outcome(),
        core.RequestOutcome(ok=True, status="ok", response=entity_response, host="www.rusprofile.ru"),
    ]
    requested_urls: list[str] = []

    def fake_request(url: str, *, source: str, **kwargs: object) -> core.RequestOutcome:
        assert source == "rusprofile"
        requested_urls.append(url)
        return outcomes.pop(0)

    monkeypatch.setattr(client, "request", fake_request)

    result = source.search(row)

    assert result.status == "success"
    assert result.entity_url == entity_url
    assert requested_urls == [search_url, entity_url, entity_url]


def test_rusprofile_search_does_not_retry_non_retryable_request_error(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    client = _build_client(tmp_path)
    source = _build_source(client)
    row = core.RowInput(row_index=58, inn="7804309200", company_name="Test Company")
    search_url = f"{RUSPROFILE_ORIGIN}/search?query={row.inn}"
    outcomes = [_non_retryable_request_error_outcome()]
    requested_urls: list[str] = []

    def fake_request(url: str, *, source: str, **kwargs: object) -> core.RequestOutcome:
        assert source == "rusprofile"
        requested_urls.append(url)
        return outcomes.pop(0)

    monkeypatch.setattr(client, "request", fake_request)

    result = source.search(row)

    assert result.status == "request_error"
    assert requested_urls == [search_url]
    assert result.errors == ["invalid URL/host: bad rusprofile search URL"]
    assert not client.progress_store.events_jsonl.exists()

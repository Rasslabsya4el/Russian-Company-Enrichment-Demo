from __future__ import annotations

import logging
from pathlib import Path

import pytest
import requests

import company_enrichment_core as core
from app.sources.zachestnyibiznes import ZachestnyBiznesSource, _PROFILE_PAGE_MARKERS


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
        logger=logging.getLogger("tests.zachestnyibiznes_resilience"),
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
        host="zachestnyibiznes.ru",
    )


def _read_timeout_outcome() -> core.RequestOutcome:
    return core.RequestOutcome(
        ok=False,
        status="request_error",
        error="HTTPSConnectionPool(host='zachestnyibiznes.ru', port=443): Read timed out. (read timeout=18)",
        host="zachestnyibiznes.ru",
        timeout=True,
    )


def _connect_timeout_blocked_outcome() -> core.RequestOutcome:
    return core.RequestOutcome(
        ok=False,
        status="blocked",
        error=(
            "HTTPSConnectionPool(host='zachestnyibiznes.ru', port=443): Max retries exceeded with url: "
            "/search?query=7703770101 (Caused by ConnectTimeoutError("
            "'Connection to zachestnyibiznes.ru timed out. (connect timeout=18)'))"
        ),
        host="zachestnyibiznes.ru",
        timeout=True,
        blocked=True,
    )


def _http_403_outcome(url: str) -> core.RequestOutcome:
    response = _build_response(url, status_code=403, text="Forbidden")
    return core.RequestOutcome(
        ok=False,
        status="http_403",
        response=response,
        error="HTTP 403",
        host="zachestnyibiznes.ru",
        blocked=True,
    )


def _search_html(entity_path: str) -> str:
    return f'<html><body><a href="{entity_path}">company</a></body></html>'


def _company_html(inn: str) -> str:
    markers = " ".join(_PROFILE_PAGE_MARKERS)
    return f"""
    <html>
      <head>
        <title>Test Company</title>
        <meta name="description" content="Company INN {inn}">
      </head>
      <body>
        <h1>Test Company</h1>
        <div>{inn}</div>
        <div>{markers}</div>
      </body>
    </html>
    """


def test_zachestnyibiznes_search_recovers_from_single_remote_disconnected(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    client = _build_client(tmp_path)
    source = ZachestnyBiznesSource(client)
    row = core.RowInput(row_index=64, inn="5056012769", company_name="Test Company")
    search_url = f"https://zachestnyibiznes.ru/search?query={row.inn}"
    entity_url = f"https://zachestnyibiznes.ru/company/ul/1125027000108_{row.inn}_OOO-Test"
    search_response = _build_response(search_url, text=_search_html("/company/ul/1125027000108_5056012769_OOO-Test"))
    entity_response = _build_response(entity_url, text=_company_html(row.inn))
    outcomes = [
        _remote_disconnected_outcome(),
        core.RequestOutcome(ok=True, status="ok", response=search_response, host="zachestnyibiznes.ru"),
        core.RequestOutcome(ok=True, status="ok", response=entity_response, host="zachestnyibiznes.ru"),
    ]
    requested_urls: list[str] = []

    def fake_request(url: str, *, source: str, **kwargs: object) -> core.RequestOutcome:
        assert source == "zachestnyibiznes"
        requested_urls.append(url)
        return outcomes.pop(0)

    monkeypatch.setattr(client, "request", fake_request)

    result = source.search(row)

    assert result.status == "success"
    assert result.entity_url == entity_url
    assert requested_urls == [search_url, search_url, entity_url]


def test_zachestnyibiznes_search_recovers_from_single_read_timeout(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    client = _build_client(tmp_path)
    source = ZachestnyBiznesSource(client)
    row = core.RowInput(row_index=64, inn="5056012769", company_name="Test Company")
    search_url = f"https://zachestnyibiznes.ru/search?query={row.inn}"
    entity_url = f"https://zachestnyibiznes.ru/company/ul/1125027000108_{row.inn}_OOO-Test"
    search_response = _build_response(search_url, text=_search_html("/company/ul/1125027000108_5056012769_OOO-Test"))
    entity_response = _build_response(entity_url, text=_company_html(row.inn))
    outcomes = [
        _read_timeout_outcome(),
        core.RequestOutcome(ok=True, status="ok", response=search_response, host="zachestnyibiznes.ru"),
        core.RequestOutcome(ok=True, status="ok", response=entity_response, host="zachestnyibiznes.ru"),
    ]
    requested_urls: list[str] = []

    def fake_request(url: str, *, source: str, **kwargs: object) -> core.RequestOutcome:
        assert source == "zachestnyibiznes"
        requested_urls.append(url)
        return outcomes.pop(0)

    monkeypatch.setattr(client, "request", fake_request)

    result = source.search(row)

    assert result.status == "success"
    assert result.entity_url == entity_url
    assert result.errors == []
    assert requested_urls == [search_url, search_url, entity_url]


def test_zachestnyibiznes_search_recovers_from_single_blocked_connect_timeout(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    client = _build_client(tmp_path)
    source = ZachestnyBiznesSource(client)
    row = core.RowInput(row_index=64, inn="7703770101", company_name="Test Company")
    search_url = f"https://zachestnyibiznes.ru/search?query={row.inn}"
    entity_url = f"https://zachestnyibiznes.ru/company/ul/1125027000108_{row.inn}_OOO-Test"
    search_response = _build_response(search_url, text=_search_html(f"/company/ul/1125027000108_{row.inn}_OOO-Test"))
    entity_response = _build_response(entity_url, text=_company_html(row.inn))
    outcomes = [
        _connect_timeout_blocked_outcome(),
        core.RequestOutcome(ok=True, status="ok", response=search_response, host="zachestnyibiznes.ru"),
        core.RequestOutcome(ok=True, status="ok", response=entity_response, host="zachestnyibiznes.ru"),
    ]
    requested_urls: list[str] = []

    def fake_request(url: str, *, source: str, **kwargs: object) -> core.RequestOutcome:
        assert source == "zachestnyibiznes"
        requested_urls.append(url)
        return outcomes.pop(0)

    monkeypatch.setattr(client, "request", fake_request)

    result = source.search(row)

    assert result.status == "success"
    assert result.entity_url == entity_url
    assert result.errors == []
    assert requested_urls == [search_url, search_url, entity_url]


def test_zachestnyibiznes_search_recovers_from_single_http_403(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    client = _build_client(tmp_path)
    source = ZachestnyBiznesSource(client)
    row = core.RowInput(row_index=64, inn="5056012769", company_name="Test Company")
    search_url = f"https://zachestnyibiznes.ru/search?query={row.inn}"
    entity_url = f"https://zachestnyibiznes.ru/company/ul/1125027000108_{row.inn}_OOO-Test"
    search_response = _build_response(search_url, text=_search_html(f"/company/ul/1125027000108_{row.inn}_OOO-Test"))
    entity_response = _build_response(entity_url, text=_company_html(row.inn))
    outcomes = [
        _http_403_outcome(search_url),
        core.RequestOutcome(ok=True, status="ok", response=search_response, host="zachestnyibiznes.ru"),
        core.RequestOutcome(ok=True, status="ok", response=entity_response, host="zachestnyibiznes.ru"),
    ]
    requested_urls: list[str] = []

    def fake_request(url: str, *, source: str, **kwargs: object) -> core.RequestOutcome:
        assert source == "zachestnyibiznes"
        requested_urls.append(url)
        return outcomes.pop(0)

    monkeypatch.setattr(client, "request", fake_request)

    result = source.search(row)

    assert result.status == "success"
    assert result.entity_url == entity_url
    assert result.errors == []
    assert requested_urls == [search_url, search_url, entity_url]


def test_zachestnyibiznes_entity_page_recovers_from_single_remote_disconnected(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    client = _build_client(tmp_path)
    source = ZachestnyBiznesSource(client)
    row = core.RowInput(row_index=64, inn="5056012769", company_name="Test Company")
    search_url = f"https://zachestnyibiznes.ru/search?query={row.inn}"
    entity_url = f"https://zachestnyibiznes.ru/company/ul/1125027000108_{row.inn}_OOO-Test"
    search_response = _build_response(search_url, text=_search_html("/company/ul/1125027000108_5056012769_OOO-Test"))
    entity_response = _build_response(entity_url, text=_company_html(row.inn))
    outcomes = [
        core.RequestOutcome(ok=True, status="ok", response=search_response, host="zachestnyibiznes.ru"),
        _remote_disconnected_outcome(),
        core.RequestOutcome(ok=True, status="ok", response=entity_response, host="zachestnyibiznes.ru"),
    ]
    requested_urls: list[str] = []

    def fake_request(url: str, *, source: str, **kwargs: object) -> core.RequestOutcome:
        assert source == "zachestnyibiznes"
        requested_urls.append(url)
        return outcomes.pop(0)

    monkeypatch.setattr(client, "request", fake_request)

    result = source.search(row)

    assert result.status == "success"
    assert result.entity_url == entity_url
    assert requested_urls == [search_url, entity_url, entity_url]


def test_zachestnyibiznes_entity_page_keeps_blocked_when_read_timeout_repeats(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    client = _build_client(tmp_path)
    source = ZachestnyBiznesSource(client)
    row = core.RowInput(row_index=64, inn="5056012769", company_name="Test Company")
    search_url = f"https://zachestnyibiznes.ru/search?query={row.inn}"
    entity_url = f"https://zachestnyibiznes.ru/company/ul/1125027000108_{row.inn}_OOO-Test"
    search_response = _build_response(search_url, text=_search_html("/company/ul/1125027000108_5056012769_OOO-Test"))
    outcomes = [
        core.RequestOutcome(ok=True, status="ok", response=search_response, host="zachestnyibiznes.ru"),
        _read_timeout_outcome(),
        _read_timeout_outcome(),
    ]
    requested_urls: list[str] = []

    def fake_request(url: str, *, source: str, **kwargs: object) -> core.RequestOutcome:
        assert source == "zachestnyibiznes"
        requested_urls.append(url)
        return outcomes.pop(0)

    monkeypatch.setattr(client, "request", fake_request)

    result = source.search(row)

    assert result.status == "blocked"
    assert result.entity_url == entity_url
    assert requested_urls == [search_url, entity_url, entity_url]
    assert any("Read timed out" in error for error in result.errors)


def test_zachestnyibiznes_entity_page_keeps_blocked_when_remote_disconnected_repeats(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    client = _build_client(tmp_path)
    source = ZachestnyBiznesSource(client)
    row = core.RowInput(row_index=64, inn="5056012769", company_name="Test Company")
    search_url = f"https://zachestnyibiznes.ru/search?query={row.inn}"
    entity_url = f"https://zachestnyibiznes.ru/company/ul/1125027000108_{row.inn}_OOO-Test"
    search_response = _build_response(search_url, text=_search_html("/company/ul/1125027000108_5056012769_OOO-Test"))
    outcomes = [
        core.RequestOutcome(ok=True, status="ok", response=search_response, host="zachestnyibiznes.ru"),
        _remote_disconnected_outcome(),
        _remote_disconnected_outcome(),
    ]
    requested_urls: list[str] = []

    def fake_request(url: str, *, source: str, **kwargs: object) -> core.RequestOutcome:
        assert source == "zachestnyibiznes"
        requested_urls.append(url)
        return outcomes.pop(0)

    monkeypatch.setattr(client, "request", fake_request)

    result = source.search(row)

    assert result.status == "blocked"
    assert result.entity_url == entity_url
    assert requested_urls == [search_url, entity_url, entity_url]
    assert any("Remote end closed connection without response" in error for error in result.errors)


def test_zachestnyibiznes_search_keeps_blocked_when_http_403_repeats(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    client = _build_client(tmp_path)
    source = ZachestnyBiznesSource(client)
    row = core.RowInput(row_index=64, inn="5056012769", company_name="Test Company")
    search_url = f"https://zachestnyibiznes.ru/search?query={row.inn}"
    outcomes = [
        _http_403_outcome(search_url),
        _http_403_outcome(search_url),
    ]
    requested_urls: list[str] = []

    def fake_request(url: str, *, source: str, **kwargs: object) -> core.RequestOutcome:
        assert source == "zachestnyibiznes"
        requested_urls.append(url)
        return outcomes.pop(0)

    monkeypatch.setattr(client, "request", fake_request)

    result = source.search(row)

    assert result.status == "blocked"
    assert requested_urls == [search_url, search_url]
    assert result.errors == ["HTTP 403"]


def test_zachestnyibiznes_search_keeps_blocked_when_connect_timeout_repeats(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    client = _build_client(tmp_path)
    source = ZachestnyBiznesSource(client)
    row = core.RowInput(row_index=64, inn="7703770101", company_name="Test Company")
    search_url = f"https://zachestnyibiznes.ru/search?query={row.inn}"
    outcomes = [
        _connect_timeout_blocked_outcome(),
        _connect_timeout_blocked_outcome(),
    ]
    requested_urls: list[str] = []

    def fake_request(url: str, *, source: str, **kwargs: object) -> core.RequestOutcome:
        assert source == "zachestnyibiznes"
        requested_urls.append(url)
        return outcomes.pop(0)

    monkeypatch.setattr(client, "request", fake_request)

    result = source.search(row)

    assert result.status == "blocked"
    assert requested_urls == [search_url, search_url]
    assert any("ConnectTimeoutError" in error for error in result.errors)

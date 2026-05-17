from __future__ import annotations

from types import SimpleNamespace

import company_enrichment_core as core
from app.sources.spark import SparkSource


class _SparkClient:
    def __init__(self, outcomes: list[core.RequestOutcome]) -> None:
        self._outcomes = list(outcomes)
        self.urls: list[str] = []

    def request(self, url: str, *, source: str) -> core.RequestOutcome:
        assert source == "spark"
        self.urls.append(url)
        return self._outcomes.pop(0)


def _response(url: str, text: str) -> SimpleNamespace:
    return SimpleNamespace(status_code=200, url=url, text=text)


def _tls_eof_error() -> core.RequestOutcome:
    return core.RequestOutcome(
        ok=False,
        status="request_error",
        error=(
            "HTTPSConnectionPool(host='spark-interfax.ru', port=443): Max retries exceeded "
            "with url: /search?Query=5001026970 (Caused by SSLError(SSLEOFError(8, "
            "'[SSL: UNEXPECTED_EOF_WHILE_READING] EOF occurred in violation of protocol')))"
        ),
        proxy_mode="direct",
    )


def _read_timeout_error() -> core.RequestOutcome:
    return core.RequestOutcome(
        ok=False,
        status="request_error",
        error="HTTPSConnectionPool(host='spark-interfax.ru', port=443): Read timed out. (read timeout=18)",
        proxy_mode="direct",
        timeout=True,
    )


def _connection_reset_error(
    error: str = (
        "HTTPSConnectionPool(host='spark-interfax.ru', port=443): Max retries exceeded "
        "with url: /search?Query=5001026970 (Caused by ProtocolError('Connection aborted.', "
        "ConnectionResetError(10054, 'An existing connection was forcibly closed by the remote host')))"
    ),
    *,
    proxy_mode: str = "direct",
) -> core.RequestOutcome:
    return core.RequestOutcome(
        ok=False,
        status="request_error",
        error=error,
        proxy_mode=proxy_mode,
    )


def test_spark_search_recovers_after_single_direct_tls_eof() -> None:
    listing_url = "https://spark-interfax.ru/search?Query=5001026970"
    entity_url = "https://spark-interfax.ru/company/example-inn-5001026970-ogrn"
    client = _SparkClient(
        [
            _tls_eof_error(),
            core.RequestOutcome(
                ok=True,
                status="ok",
                response=_response(listing_url, f'<a href="{entity_url}">ООО Тест</a>'),
            ),
            core.RequestOutcome(
                ok=True,
                status="ok",
                response=_response(
                    entity_url,
                    "<html><head><title>ООО Тест ИНН 5001026970</title></head>"
                    "<body>ИНН 5001026970</body></html>",
                ),
            ),
        ]
    )

    result = SparkSource(client).search(core.RowInput(row_index=1, inn="5001026970", company_name="ООО Тест"))

    assert result.status == "success"
    assert client.urls == [listing_url, listing_url, entity_url]
    assert result.errors == []


def test_spark_search_preserves_request_error_after_exhausted_direct_tls_eof() -> None:
    listing_url = "https://spark-interfax.ru/search?Query=5001026970"
    client = _SparkClient([_tls_eof_error(), _tls_eof_error()])

    result = SparkSource(client).search(core.RowInput(row_index=1, inn="5001026970", company_name="ООО Тест"))

    assert result.status == "request_error"
    assert client.urls == [listing_url, listing_url]
    assert result.errors
    assert result.availability["phones"]["status"] == "blocked"


def test_spark_search_recovers_after_single_direct_connection_reset() -> None:
    listing_url = "https://spark-interfax.ru/search?Query=5001026970"
    entity_url = "https://spark-interfax.ru/company/example-inn-5001026970-ogrn"
    client = _SparkClient(
        [
            _connection_reset_error(),
            core.RequestOutcome(
                ok=True,
                status="ok",
                response=_response(listing_url, f'<a href="{entity_url}">ООО Тест</a>'),
            ),
            core.RequestOutcome(
                ok=True,
                status="ok",
                response=_response(
                    entity_url,
                    "<html><head><title>ООО Тест ИНН 5001026970</title></head>"
                    "<body>ИНН 5001026970</body></html>",
                ),
            ),
        ]
    )

    result = SparkSource(client).search(core.RowInput(row_index=1, inn="5001026970", company_name="ООО Тест"))

    assert result.status == "success"
    assert client.urls == [listing_url, listing_url, entity_url]
    assert result.errors == []


def test_spark_search_preserves_request_error_after_exhausted_direct_remote_closed() -> None:
    listing_url = "https://spark-interfax.ru/search?Query=5001026970"
    remote_closed = (
        "HTTPSConnectionPool(host='spark-interfax.ru', port=443): Max retries exceeded "
        "with url: /search?Query=5001026970 (Caused by RemoteDisconnected("
        "'Remote end closed connection without response'))"
    )
    client = _SparkClient([_connection_reset_error(remote_closed), _connection_reset_error(remote_closed)])

    result = SparkSource(client).search(core.RowInput(row_index=1, inn="5001026970", company_name="ООО Тест"))

    assert result.status == "request_error"
    assert client.urls == [listing_url, listing_url]
    assert result.errors == [remote_closed]
    assert result.availability["phones"]["status"] == "blocked"


def test_spark_connection_reset_retry_stays_direct_only() -> None:
    listing_url = "https://spark-interfax.ru/search?Query=5001026970"
    client = _SparkClient([_connection_reset_error(proxy_mode="proxy")])

    result = SparkSource(client).search(core.RowInput(row_index=1, inn="5001026970", company_name="ООО Тест"))

    assert result.status == "request_error"
    assert client.urls == [listing_url]
    assert result.errors
    assert result.availability["phones"]["status"] == "blocked"


def test_spark_search_recovers_after_single_direct_read_timeout() -> None:
    listing_url = "https://spark-interfax.ru/search?Query=7718156134"
    entity_url = "https://spark-interfax.ru/company/example-inn-7718156134-ogrn"
    client = _SparkClient(
        [
            _read_timeout_error(),
            core.RequestOutcome(
                ok=True,
                status="ok",
                response=_response(listing_url, f'<a href="{entity_url}">ООО Таймаут</a>'),
            ),
            core.RequestOutcome(
                ok=True,
                status="ok",
                response=_response(
                    entity_url,
                    "<html><head><title>ООО Таймаут ИНН 7718156134</title></head>"
                    "<body>ИНН 7718156134</body></html>",
                ),
            ),
        ]
    )

    result = SparkSource(client).search(core.RowInput(row_index=1, inn="7718156134", company_name="ООО Таймаут"))

    assert result.status == "success"
    assert client.urls == [listing_url, listing_url, entity_url]
    assert result.errors == []


def test_spark_search_recovers_after_two_direct_read_timeouts() -> None:
    listing_url = "https://spark-interfax.ru/search?Query=5040133380"
    entity_url = "https://spark-interfax.ru/company/example-inn-5040133380-ogrn"
    client = _SparkClient(
        [
            _read_timeout_error(),
            _read_timeout_error(),
            core.RequestOutcome(
                ok=True,
                status="ok",
                response=_response(listing_url, f'<a href="{entity_url}">ООО Гидроэл</a>'),
            ),
            core.RequestOutcome(
                ok=True,
                status="ok",
                response=_response(
                    entity_url,
                    "<html><head><title>ООО Гидроэл ИНН 5040133380</title></head>"
                    "<body>ИНН 5040133380</body></html>",
                ),
            ),
        ]
    )

    result = SparkSource(client).search(core.RowInput(row_index=1, inn="5040133380", company_name="ООО Гидроэл"))

    assert result.status == "success"
    assert client.urls == [listing_url, listing_url, listing_url, entity_url]
    assert result.errors == []


def test_spark_search_preserves_request_error_after_exhausted_direct_read_timeout() -> None:
    listing_url = "https://spark-interfax.ru/search?Query=7718156134"
    client = _SparkClient([_read_timeout_error(), _read_timeout_error(), _read_timeout_error()])

    result = SparkSource(client).search(core.RowInput(row_index=1, inn="7718156134", company_name="ООО Таймаут"))

    assert result.status == "request_error"
    assert client.urls == [listing_url, listing_url, listing_url]
    assert result.errors == [
        "HTTPSConnectionPool(host='spark-interfax.ru', port=443): Read timed out. (read timeout=18)"
    ]
    assert result.availability["phones"]["status"] == "blocked"

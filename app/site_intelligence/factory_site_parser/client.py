from __future__ import annotations

import os
from typing import Any
from urllib.parse import urlparse

from app.runtime import ProxyPool, ProxySelection


class FactorySiteParserClient:
    def __init__(self, client: Any, *, proxy_pool: ProxyPool | None = None) -> None:
        self._client = client
        self.proxy_pool = self._resolve_proxy_pool(client, proxy_pool)
        self.progress_store = getattr(client, "progress_store", None)

    def request(
        self,
        url: str,
        *,
        source: str,
        allow_redirects: bool = True,
        timeout: int | None = None,
        proxy_selection: ProxySelection | None = None,
        **kwargs: Any,
    ) -> Any:
        host = urlparse(url).netloc.lower()
        if proxy_selection is not None:
            selection = proxy_selection
        else:
            selection = self._select_proxy(host)
        try:
            return self._client.request(
                url,
                source=source,
                allow_redirects=allow_redirects,
                timeout=timeout,
                proxy_selection=selection,
                **kwargs,
            )
        except TypeError:
            return self._client.request(
                url,
                source=source,
                allow_redirects=allow_redirects,
                timeout=timeout,
                **kwargs,
            )

    def _resolve_proxy_pool(self, client: Any, proxy_pool: ProxyPool | None) -> ProxyPool:
        if isinstance(proxy_pool, ProxyPool):
            return proxy_pool
        nested_pool = getattr(client, "proxy_pool", None)
        if isinstance(nested_pool, ProxyPool):
            return nested_pool
        return ProxyPool(os.getenv("PARSER_PROXIES"))

    def _select_proxy(self, host: str) -> ProxySelection:
        try:
            selection = self.proxy_pool.select(host, source_name="company_site")
        except TypeError:
            return self.proxy_pool.select(host)
        if selection.via_proxy or not getattr(self.proxy_pool, "entries", None):
            return selection
        attempt_guard = getattr(self.proxy_pool, "proxy_provider_attempt_guard", None)
        if callable(attempt_guard):
            try:
                if attempt_guard(source_name="company_site") is not None:
                    return selection
            except TypeError:
                pass
            except Exception:
                return selection
        try:
            return self.proxy_pool.select(host)
        except Exception:
            return selection

    def __getattr__(self, name: str) -> Any:
        return getattr(self._client, name)


__all__ = ["FactorySiteParserClient"]

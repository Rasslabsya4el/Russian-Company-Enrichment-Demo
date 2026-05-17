from __future__ import annotations

import hashlib
import os
import threading
import time
from dataclasses import dataclass, field
from typing import Any, Iterable
from urllib.parse import quote

import requests


DEFAULT_PROXY6_API_BASE = "https://px6.link/api"
DEFAULT_PROXY6_MAX_RPS = 3.0
DEFAULT_PROXY6_DIAGNOSTIC_CACHE_TTL_SECONDS = 300.0
PROXY6_PROVIDER_NAME = "proxy6"
PROXY_PROVIDER_STATUS_UNKNOWN = "proxy_provider_status_unknown"
PROXY_PROVIDER_INVENTORY_EMPTY_OR_EXPIRED = "proxy_provider_inventory_empty_or_expired"
PROXY_PROVIDER_INVENTORY_HEALTHY = "proxy_provider_inventory_healthy"
RUNTIME_PROXY_POOL_DEPLETED_PROVIDER_HEALTHY = "runtime_proxy_pool_depleted_provider_healthy"
PROXY6_OPERATOR_ACTION_CONFIGURE_OR_SYNC = (
    "set PROXY6_API_KEY or run scripts/proxy6/sync_proxy_pool.py before rerun"
)
PROXY6_OPERATOR_ACTION_RENEW = "top_up_or_renew_proxy_subscription"
PROXY6_OPERATOR_ACTION_REPAIR_RUNTIME_POOL = "repair_runtime_proxy_pool_sync_or_failover"
_PROXY6_DIAGNOSTIC_CACHE_LOCK = threading.Lock()
_PROXY6_DIAGNOSTIC_CACHE: tuple[tuple[str, ...], float, "Proxy6InventoryDiagnostic"] | None = None


class Proxy6ApiError(RuntimeError):
    def __init__(
        self,
        message: str,
        *,
        error_id: int | None = None,
        status_code: int | None = None,
        payload: dict[str, Any] | None = None,
    ) -> None:
        super().__init__(message)
        self.error_id = error_id
        self.status_code = status_code
        self.payload = payload or {}


@dataclass(frozen=True)
class Proxy6InventoryDiagnostic:
    provider_status: str
    operator_action: str
    reason: str
    active_usable_count: int = 0
    active_total_count: int = 0
    expired_count: int = 0
    inactive_count: int = 0
    total_count: int = 0
    parser_country: str = "ru"
    parser_descr: str = ""
    account_balance: str = ""
    account_currency: str = ""
    missing_config: str = ""
    error: str = ""
    error_id: int | None = None
    status_code: int | None = None

    @property
    def runtime_stop_class(self) -> str:
        if self.provider_status == PROXY_PROVIDER_INVENTORY_HEALTHY:
            return RUNTIME_PROXY_POOL_DEPLETED_PROVIDER_HEALTHY
        return self.provider_status or PROXY_PROVIDER_STATUS_UNKNOWN

    def as_event_fields(self) -> dict[str, object]:
        payload: dict[str, object] = {
            "proxy_provider": PROXY6_PROVIDER_NAME,
            "proxy_provider_status": self.provider_status,
            "proxy_provider_stop_class": self.runtime_stop_class,
            "operator_action": self.operator_action,
            "proxy_provider_reason": self.reason,
            "proxy_provider_active_usable_count": self.active_usable_count,
            "proxy_provider_active_total_count": self.active_total_count,
            "proxy_provider_expired_count": self.expired_count,
            "proxy_provider_inactive_count": self.inactive_count,
            "proxy_provider_total_count": self.total_count,
            "proxy_provider_parser_country": self.parser_country,
            "proxy_provider_parser_descr": self.parser_descr,
        }
        if self.account_balance:
            payload["proxy_provider_account_balance"] = self.account_balance
        if self.account_currency:
            payload["proxy_provider_account_currency"] = self.account_currency
        if self.missing_config:
            payload["proxy_provider_missing_config"] = self.missing_config
        if self.error:
            payload["proxy_provider_error"] = self.error
        if self.error_id is not None:
            payload["proxy_provider_error_id"] = self.error_id
        if self.status_code is not None:
            payload["proxy_provider_status_code"] = self.status_code
        return payload

    def operator_message(self) -> str:
        parts = [
            f"proxy_provider_status={self.provider_status}",
            f"proxy_provider_stop_class={self.runtime_stop_class}",
            f"operator_action={self.operator_action}",
            f"active_usable={self.active_usable_count}",
            f"active_total={self.active_total_count}",
            f"expired={self.expired_count}",
            f"inactive={self.inactive_count}",
            f"total={self.total_count}",
        ]
        if self.account_balance:
            parts.append(f"balance={self.account_balance}")
        if self.account_currency:
            parts.append(f"currency={self.account_currency}")
        if self.missing_config:
            parts.append(f"missing_config={self.missing_config}")
        if self.error:
            parts.append(f"provider_error={self.error}")
        if self.reason:
            parts.append(f"reason={self.reason}")
        return " ".join(parts)


@dataclass(frozen=True)
class Proxy6Proxy:
    id: str
    host: str
    port: str
    user: str
    password: str
    proxy_type: str
    country: str
    date: str
    date_end: str
    descr: str
    active: bool
    ip: str = ""
    raw: dict[str, Any] = field(default_factory=dict)

    def as_url(self, *, prefer_socks: bool = False) -> str:
        proxy_type = (self.proxy_type or "").strip().lower()
        if prefer_socks and proxy_type in {"socks", "auto"}:
            scheme = "socks5"
        else:
            scheme = "http"
        user = quote(self.user, safe="")
        password = quote(self.password, safe="")
        auth = f"{user}:{password}@" if self.user or self.password else ""
        return f"{scheme}://{auth}{self.host}:{self.port}"


def _decode_response_text(response: requests.Response) -> str:
    try:
        return response.text or ""
    except Exception:
        encoding = response.encoding or getattr(response, "apparent_encoding", None) or "utf-8"
        try:
            return response.content.decode(encoding, errors="replace")
        except Exception:
            return response.content.decode("utf-8", errors="replace")


def _parse_bool(value: Any) -> bool:
    normalized = str(value or "").strip().lower()
    return normalized in {"1", "true", "yes"}


def _normalize_type(value: Any) -> str:
    normalized = str(value or "").strip().lower()
    if normalized in {"http", "https"}:
        return "http"
    if normalized in {"socks", "socks5"}:
        return "socks"
    if normalized == "auto":
        return "auto"
    return normalized


def parse_proxy6_list(payload: dict[str, Any]) -> list[Proxy6Proxy]:
    raw_list = payload.get("list")
    if isinstance(raw_list, dict):
        items = raw_list.values()
    elif isinstance(raw_list, list):
        items = raw_list
    else:
        items = []
    parsed: list[Proxy6Proxy] = []
    for item in items:
        if not isinstance(item, dict):
            continue
        host = str(item.get("host", "")).strip()
        port = str(item.get("port", "")).strip()
        proxy_id = str(item.get("id", "")).strip()
        if not host or not port or not proxy_id:
            continue
        parsed.append(
            Proxy6Proxy(
                id=proxy_id,
                host=host,
                port=port,
                user=str(item.get("user", "")).strip(),
                password=str(item.get("pass", "")).strip(),
                proxy_type=_normalize_type(item.get("type", "")),
                country=str(item.get("country", "")).strip().lower(),
                date=str(item.get("date", "")).strip(),
                date_end=str(item.get("date_end", "")).strip(),
                descr=str(item.get("descr", "")).strip(),
                active=_parse_bool(item.get("active")),
                ip=str(item.get("ip", "")).strip(),
                raw=dict(item),
            )
        )
    return parsed


def build_parser_proxy_urls(
    proxies: Iterable[Proxy6Proxy],
    *,
    allow_socks: bool = False,
) -> list[str]:
    result: list[str] = []
    for proxy in proxies:
        proxy_type = (proxy.proxy_type or "").strip().lower()
        prefer_socks = allow_socks and proxy_type == "socks"
        result.append(proxy.as_url(prefer_socks=prefer_socks))
    return result


def _normalize_proxy6_parser_country(value: str | None) -> str:
    normalized = str(value or "").strip().lower()
    return normalized or "ru"


def _proxy_matches_parser_pool(proxy: Proxy6Proxy, *, country: str, descr: str) -> bool:
    normalized_country = _normalize_proxy6_parser_country(country)
    if normalized_country not in {"", "all"} and proxy.country != normalized_country:
        return False
    normalized_descr = str(descr or "").strip()
    if normalized_descr and proxy.descr != normalized_descr:
        return False
    return True


def _filter_parser_pool_proxies(
    proxies: Iterable[Proxy6Proxy],
    *,
    country: str,
    descr: str,
) -> list[Proxy6Proxy]:
    return [
        proxy
        for proxy in proxies
        if _proxy_matches_parser_pool(proxy, country=country, descr=descr)
    ]


def _unknown_proxy6_inventory_diagnostic(
    *,
    reason: str,
    parser_country: str,
    parser_descr: str,
    missing_config: str = "",
    error: str = "",
    error_id: int | None = None,
    status_code: int | None = None,
) -> Proxy6InventoryDiagnostic:
    return Proxy6InventoryDiagnostic(
        provider_status=PROXY_PROVIDER_STATUS_UNKNOWN,
        operator_action=PROXY6_OPERATOR_ACTION_CONFIGURE_OR_SYNC,
        reason=reason,
        parser_country=_normalize_proxy6_parser_country(parser_country),
        parser_descr=str(parser_descr or "").strip(),
        missing_config=missing_config,
        error=error,
        error_id=error_id,
        status_code=status_code,
    )


def diagnose_proxy6_inventory(
    client: object,
    *,
    parser_country: str = "ru",
    parser_descr: str = "",
    limit: int = 1000,
    max_pages: int = 20,
) -> Proxy6InventoryDiagnostic:
    normalized_country = _normalize_proxy6_parser_country(parser_country)
    normalized_descr = str(parser_descr or "").strip()
    try:
        account_payload = client.get_account()
        active_proxies = client.get_all_proxies(
            state="active",
            descr=normalized_descr,
            limit=limit,
            max_pages=max_pages,
        )
        expired_proxies = client.get_all_proxies(
            state="expired",
            descr=normalized_descr,
            limit=limit,
            max_pages=max_pages,
        )
        all_proxies = client.get_all_proxies(
            state="all",
            descr=normalized_descr,
            limit=limit,
            max_pages=max_pages,
        )
    except Proxy6ApiError as exc:
        return _unknown_proxy6_inventory_diagnostic(
            reason="Proxy6 API diagnostic failed; provider inventory state is unknown",
            parser_country=normalized_country,
            parser_descr=normalized_descr,
            error=str(exc),
            error_id=exc.error_id,
            status_code=exc.status_code,
        )
    except Exception as exc:
        return _unknown_proxy6_inventory_diagnostic(
            reason="Proxy6 API diagnostic failed before inventory classification",
            parser_country=normalized_country,
            parser_descr=normalized_descr,
            error=str(exc),
        )

    account_payload = account_payload if isinstance(account_payload, dict) else {}
    active_pool = _filter_parser_pool_proxies(
        active_proxies,
        country=normalized_country,
        descr=normalized_descr,
    )
    expired_pool = _filter_parser_pool_proxies(
        expired_proxies,
        country=normalized_country,
        descr=normalized_descr,
    )
    total_pool = _filter_parser_pool_proxies(
        all_proxies,
        country=normalized_country,
        descr=normalized_descr,
    )
    active_usable_count = sum(1 for proxy in active_pool if proxy.active)
    inactive_count = sum(1 for proxy in total_pool if not proxy.active)
    balance = str(account_payload.get("balance", "") or "").strip()
    currency = str(account_payload.get("currency", "") or "").strip()

    if active_usable_count > 0:
        return Proxy6InventoryDiagnostic(
            provider_status=PROXY_PROVIDER_INVENTORY_HEALTHY,
            operator_action=PROXY6_OPERATOR_ACTION_REPAIR_RUNTIME_POOL,
            reason="Proxy6 reports active parser-pool proxies; runtime pool depletion needs tracked proxy-pool sync/failover handling",
            active_usable_count=active_usable_count,
            active_total_count=len(active_pool),
            expired_count=len(expired_pool),
            inactive_count=inactive_count,
            total_count=len(total_pool),
            parser_country=normalized_country,
            parser_descr=normalized_descr,
            account_balance=balance,
            account_currency=currency,
        )

    return Proxy6InventoryDiagnostic(
        provider_status=PROXY_PROVIDER_INVENTORY_EMPTY_OR_EXPIRED,
        operator_action=PROXY6_OPERATOR_ACTION_RENEW,
        reason="Proxy6 reports no active usable parser-pool proxies",
        active_usable_count=0,
        active_total_count=len(active_pool),
        expired_count=len(expired_pool),
        inactive_count=inactive_count,
        total_count=len(total_pool),
        parser_country=normalized_country,
        parser_descr=normalized_descr,
        account_balance=balance,
        account_currency=currency,
    )


def clear_proxy6_inventory_diagnostic_cache() -> None:
    global _PROXY6_DIAGNOSTIC_CACHE
    with _PROXY6_DIAGNOSTIC_CACHE_LOCK:
        _PROXY6_DIAGNOSTIC_CACHE = None


def _proxy6_diagnostic_cache_ttl_seconds() -> float:
    raw_value = os.getenv("PROXY6_DIAGNOSTIC_CACHE_TTL_SECONDS", "").strip()
    try:
        return max(float(raw_value or DEFAULT_PROXY6_DIAGNOSTIC_CACHE_TTL_SECONDS), 0.0)
    except ValueError:
        return DEFAULT_PROXY6_DIAGNOSTIC_CACHE_TTL_SECONDS


def _proxy6_inventory_env_cache_key() -> tuple[str, ...]:
    api_key = os.getenv("PROXY6_API_KEY", "").strip()
    api_key_digest = hashlib.sha256(api_key.encode("utf-8")).hexdigest() if api_key else ""
    return (
        api_key_digest,
        os.getenv("PROXY6_PARSER_POOL_COUNTRY", "ru").strip(),
        os.getenv("PROXY6_PARSER_POOL_DESCR", "").strip(),
        os.getenv("PROXY6_API_TIMEOUT_SECONDS", "15").strip(),
        os.getenv("PROXY6_API_MAX_RPS", str(DEFAULT_PROXY6_MAX_RPS)).strip(),
        os.getenv("PROXY6_DIAGNOSTIC_LIMIT", "1000").strip(),
        os.getenv("PROXY6_DIAGNOSTIC_MAX_PAGES", "20").strip(),
    )


def _diagnose_proxy6_inventory_from_env_uncached() -> Proxy6InventoryDiagnostic:
    api_key = os.getenv("PROXY6_API_KEY", "").strip()
    parser_country = os.getenv("PROXY6_PARSER_POOL_COUNTRY", "ru").strip() or "ru"
    parser_descr = os.getenv("PROXY6_PARSER_POOL_DESCR", "").strip()
    if not api_key:
        return _unknown_proxy6_inventory_diagnostic(
            reason="PROXY6_API_KEY is not configured; provider inventory cannot be verified",
            parser_country=parser_country,
            parser_descr=parser_descr,
            missing_config="PROXY6_API_KEY",
        )
    try:
        timeout_seconds = int(os.getenv("PROXY6_API_TIMEOUT_SECONDS", "15") or "15")
    except ValueError:
        timeout_seconds = 15
    try:
        max_rps = float(os.getenv("PROXY6_API_MAX_RPS", str(DEFAULT_PROXY6_MAX_RPS)) or DEFAULT_PROXY6_MAX_RPS)
    except ValueError:
        max_rps = DEFAULT_PROXY6_MAX_RPS
    try:
        limit = int(os.getenv("PROXY6_DIAGNOSTIC_LIMIT", "1000") or "1000")
    except ValueError:
        limit = 1000
    try:
        max_pages = int(os.getenv("PROXY6_DIAGNOSTIC_MAX_PAGES", "20") or "20")
    except ValueError:
        max_pages = 20
    client = Proxy6Client(api_key, timeout_seconds=timeout_seconds, max_rps=max_rps)
    return diagnose_proxy6_inventory(
        client,
        parser_country=parser_country,
        parser_descr=parser_descr,
        limit=max(1, min(limit, 1000)),
        max_pages=max(1, max_pages),
    )


def diagnose_proxy6_inventory_from_env(
    *,
    use_cache: bool = True,
    force_refresh: bool = False,
) -> Proxy6InventoryDiagnostic:
    global _PROXY6_DIAGNOSTIC_CACHE
    if not use_cache:
        return _diagnose_proxy6_inventory_from_env_uncached()

    ttl_seconds = _proxy6_diagnostic_cache_ttl_seconds()
    if ttl_seconds <= 0:
        return _diagnose_proxy6_inventory_from_env_uncached()

    cache_key = _proxy6_inventory_env_cache_key()
    now = time.time()
    if not force_refresh:
        with _PROXY6_DIAGNOSTIC_CACHE_LOCK:
            cached = _PROXY6_DIAGNOSTIC_CACHE
            if cached is not None:
                cached_key, expires_at, diagnostic = cached
                if cached_key == cache_key and expires_at > now:
                    return diagnostic

    diagnostic = _diagnose_proxy6_inventory_from_env_uncached()
    with _PROXY6_DIAGNOSTIC_CACHE_LOCK:
        _PROXY6_DIAGNOSTIC_CACHE = (cache_key, now + ttl_seconds, diagnostic)
    return diagnostic


def load_active_proxy6_parser_proxies_from_env(
    *,
    force_refresh: bool = False,
) -> tuple[Proxy6InventoryDiagnostic, list[Proxy6Proxy]]:
    diagnostic = diagnose_proxy6_inventory_from_env(force_refresh=force_refresh)
    if diagnostic.provider_status != PROXY_PROVIDER_INVENTORY_HEALTHY:
        return diagnostic, []

    api_key = os.getenv("PROXY6_API_KEY", "").strip()
    if not api_key:
        return _unknown_proxy6_inventory_diagnostic(
            reason="PROXY6_API_KEY is not configured; provider proxy sync cannot run",
            parser_country=diagnostic.parser_country,
            parser_descr=diagnostic.parser_descr,
            missing_config="PROXY6_API_KEY",
        ), []

    try:
        timeout_seconds = int(os.getenv("PROXY6_API_TIMEOUT_SECONDS", "15") or "15")
    except ValueError:
        timeout_seconds = 15
    try:
        max_rps = float(os.getenv("PROXY6_API_MAX_RPS", str(DEFAULT_PROXY6_MAX_RPS)) or DEFAULT_PROXY6_MAX_RPS)
    except ValueError:
        max_rps = DEFAULT_PROXY6_MAX_RPS
    try:
        limit = int(os.getenv("PROXY6_DIAGNOSTIC_LIMIT", "1000") or "1000")
    except ValueError:
        limit = 1000
    try:
        max_pages = int(os.getenv("PROXY6_DIAGNOSTIC_MAX_PAGES", "20") or "20")
    except ValueError:
        max_pages = 20

    try:
        client = Proxy6Client(api_key, timeout_seconds=timeout_seconds, max_rps=max_rps)
        active_proxies = client.get_all_proxies(
            state="active",
            descr=diagnostic.parser_descr,
            limit=max(1, min(limit, 1000)),
            max_pages=max(1, max_pages),
            nokey=False,
        )
    except Proxy6ApiError as exc:
        return _unknown_proxy6_inventory_diagnostic(
            reason="Proxy6 API active-proxy sync failed",
            parser_country=diagnostic.parser_country,
            parser_descr=diagnostic.parser_descr,
            error=str(exc),
            error_id=exc.error_id,
            status_code=exc.status_code,
        ), []
    except Exception as exc:
        return _unknown_proxy6_inventory_diagnostic(
            reason="Proxy6 API active-proxy sync failed before proxy list materialization",
            parser_country=diagnostic.parser_country,
            parser_descr=diagnostic.parser_descr,
            error=str(exc),
        ), []

    active_pool = _filter_parser_pool_proxies(
        active_proxies,
        country=diagnostic.parser_country,
        descr=diagnostic.parser_descr,
    )
    return diagnostic, [proxy for proxy in active_pool if proxy.active]


class Proxy6Client:
    def __init__(
        self,
        api_key: str,
        *,
        base_url: str = DEFAULT_PROXY6_API_BASE,
        timeout_seconds: int = 30,
        max_rps: float = DEFAULT_PROXY6_MAX_RPS,
        session: requests.Session | None = None,
    ) -> None:
        self.api_key = (api_key or "").strip()
        if not self.api_key:
            raise ValueError("Proxy6 API key is required")
        self.base_url = base_url.rstrip("/")
        self.timeout_seconds = max(5, int(timeout_seconds))
        self.max_rps = max(0.1, float(max_rps))
        self.session = session or requests.Session()
        self._rate_lock = threading.Lock()
        self._next_request_at = 0.0

    def _throttle(self) -> None:
        with self._rate_lock:
            now = time.monotonic()
            wait_for = self._next_request_at - now
            if wait_for > 0:
                time.sleep(wait_for)
                now = time.monotonic()
            self._next_request_at = now + (1.0 / self.max_rps)

    def _endpoint(self, method: str = "") -> str:
        cleaned_method = method.strip("/")
        if cleaned_method:
            return f"{self.base_url}/{self.api_key}/{cleaned_method}"
        return f"{self.base_url}/{self.api_key}"

    def call(self, method: str = "", params: dict[str, Any] | None = None) -> dict[str, Any]:
        self._throttle()
        response = self.session.get(
            self._endpoint(method),
            params=params or {},
            timeout=self.timeout_seconds,
        )
        if response.status_code == 429:
            raise Proxy6ApiError(
                "Proxy6 API returned HTTP 429 (rate limited)",
                status_code=429,
                payload={"retry_after": response.headers.get("Retry-After", "")},
            )
        try:
            response.raise_for_status()
        except requests.HTTPError as exc:
            raise Proxy6ApiError(
                f"Proxy6 API HTTP error: {response.status_code}",
                status_code=response.status_code,
                payload={"body_preview": _decode_response_text(response)[:1000]},
            ) from exc
        try:
            payload = response.json()
        except ValueError as exc:
            raise Proxy6ApiError(
                "Proxy6 API returned non-JSON response",
                payload={"body_preview": _decode_response_text(response)[:1000]},
            ) from exc
        if not isinstance(payload, dict):
            raise Proxy6ApiError(
                "Proxy6 API returned unexpected payload type",
                payload={"payload_type": type(payload).__name__},
            )
        if str(payload.get("status", "")).lower() == "yes":
            return payload
        error_message = str(payload.get("error", "Unknown Proxy6 API error")).strip() or "Unknown Proxy6 API error"
        error_id_raw = payload.get("error_id")
        error_id = int(error_id_raw) if str(error_id_raw).isdigit() else None
        raise Proxy6ApiError(error_message, error_id=error_id, payload=payload)

    def get_account(self) -> dict[str, Any]:
        return self.call()

    def get_proxy_list(
        self,
        *,
        state: str = "active",
        descr: str = "",
        page: int = 1,
        limit: int = 1000,
        nokey: bool = True,
    ) -> list[Proxy6Proxy]:
        params: dict[str, Any] = {"state": state, "page": max(1, int(page)), "limit": max(1, min(int(limit), 1000))}
        if descr:
            params["descr"] = descr
        if nokey:
            params["nokey"] = ""
        payload = self.call("getproxy", params=params)
        return parse_proxy6_list(payload)

    def get_all_proxies(
        self,
        *,
        state: str = "active",
        descr: str = "",
        limit: int = 1000,
        max_pages: int = 50,
        nokey: bool = True,
    ) -> list[Proxy6Proxy]:
        all_items: list[Proxy6Proxy] = []
        current_page = 1
        while current_page <= max_pages:
            chunk = self.get_proxy_list(
                state=state,
                descr=descr,
                page=current_page,
                limit=limit,
                nokey=nokey,
            )
            if not chunk:
                break
            all_items.extend(chunk)
            if len(chunk) < limit:
                break
            current_page += 1
        return all_items

    def get_price(self, *, count: int, period: int, version: int = 4) -> dict[str, Any]:
        return self.call("getprice", params={"count": int(count), "period": int(period), "version": int(version)})

    def get_count(self, *, country: str = "ru", version: int = 4) -> dict[str, Any]:
        return self.call("getcount", params={"country": country.lower(), "version": int(version)})

    def get_countries(self, *, version: int = 4) -> dict[str, Any]:
        return self.call("getcountry", params={"version": int(version)})

    def check_proxy(self, *, proxy_id: str | None = None, proxy_string: str | None = None) -> dict[str, Any]:
        if proxy_id:
            return self.call("check", params={"ids": proxy_id})
        if proxy_string:
            return self.call("check", params={"proxy": proxy_string})
        raise ValueError("proxy_id or proxy_string must be provided")

    def buy(
        self,
        *,
        count: int,
        period: int,
        country: str,
        version: int = 4,
        proxy_type: str = "",
        descr: str = "",
        auto_prolong: bool = False,
        nokey: bool = False,
    ) -> dict[str, Any]:
        params: dict[str, Any] = {
            "count": int(count),
            "period": int(period),
            "country": country.lower(),
            "version": int(version),
        }
        normalized_type = _normalize_type(proxy_type)
        if normalized_type in {"http", "socks"}:
            params["type"] = normalized_type
        if descr:
            params["descr"] = descr[:50]
        if auto_prolong:
            params["auto_prolong"] = ""
        if nokey:
            params["nokey"] = ""
        return self.call("buy", params=params)

    def prolong(self, *, ids: Iterable[str], period: int, nokey: bool = False) -> dict[str, Any]:
        prepared_ids = [str(item).strip() for item in ids if str(item).strip()]
        if not prepared_ids:
            raise ValueError("ids for prolong are empty")
        params: dict[str, Any] = {"ids": ",".join(prepared_ids), "period": int(period)}
        if nokey:
            params["nokey"] = ""
        return self.call("prolong", params=params)

    def delete(self, *, ids: Iterable[str] | None = None, descr: str = "") -> dict[str, Any]:
        prepared_ids = [str(item).strip() for item in (ids or []) if str(item).strip()]
        params: dict[str, Any] = {}
        if prepared_ids:
            params["ids"] = ",".join(prepared_ids)
        if descr:
            params["descr"] = descr
        if not params:
            raise ValueError("ids or descr must be provided for delete")
        return self.call("delete", params=params)

    def set_description(self, *, new_descr: str, old_descr: str = "", ids: Iterable[str] | None = None) -> dict[str, Any]:
        params: dict[str, Any] = {"new": new_descr[:50]}
        prepared_ids = [str(item).strip() for item in (ids or []) if str(item).strip()]
        if prepared_ids:
            params["ids"] = ",".join(prepared_ids)
        elif old_descr:
            params["old"] = old_descr
        else:
            raise ValueError("ids or old_descr must be provided for set_description")
        return self.call("setdescr", params=params)

    def set_type(self, *, ids: Iterable[str], proxy_type: str) -> dict[str, Any]:
        prepared_ids = [str(item).strip() for item in ids if str(item).strip()]
        normalized_type = _normalize_type(proxy_type)
        if not prepared_ids:
            raise ValueError("ids for set_type are empty")
        if normalized_type not in {"http", "socks"}:
            raise ValueError("proxy_type for set_type must be http or socks")
        return self.call("settype", params={"ids": ",".join(prepared_ids), "type": normalized_type})

    def set_ip_auth(self, *, ips: Iterable[str]) -> dict[str, Any]:
        prepared_ips = [str(item).strip() for item in ips if str(item).strip()]
        if not prepared_ips:
            raise ValueError("ips for set_ip_auth are empty")
        return self.call("ipauth", params={"ip": ",".join(prepared_ips)})

    def clear_ip_auth(self) -> dict[str, Any]:
        return self.call("ipauth", params={"ip": "delete"})


__all__ = [
    "DEFAULT_PROXY6_API_BASE",
    "DEFAULT_PROXY6_DIAGNOSTIC_CACHE_TTL_SECONDS",
    "DEFAULT_PROXY6_MAX_RPS",
    "PROXY6_OPERATOR_ACTION_CONFIGURE_OR_SYNC",
    "PROXY6_OPERATOR_ACTION_RENEW",
    "PROXY6_OPERATOR_ACTION_REPAIR_RUNTIME_POOL",
    "PROXY6_PROVIDER_NAME",
    "PROXY_PROVIDER_INVENTORY_EMPTY_OR_EXPIRED",
    "PROXY_PROVIDER_INVENTORY_HEALTHY",
    "PROXY_PROVIDER_STATUS_UNKNOWN",
    "Proxy6ApiError",
    "Proxy6Client",
    "Proxy6InventoryDiagnostic",
    "Proxy6Proxy",
    "RUNTIME_PROXY_POOL_DEPLETED_PROVIDER_HEALTHY",
    "build_parser_proxy_urls",
    "clear_proxy6_inventory_diagnostic_cache",
    "diagnose_proxy6_inventory",
    "diagnose_proxy6_inventory_from_env",
    "load_active_proxy6_parser_proxies_from_env",
    "parse_proxy6_list",
]

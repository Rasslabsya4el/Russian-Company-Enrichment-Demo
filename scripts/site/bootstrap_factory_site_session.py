from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path
from urllib.parse import urlparse

if __package__ in {None, ""}:
    sys.path.append(str(Path(__file__).resolve().parents[2]))

from app.runtime import ProxyPool, ProxySelection
from app.site_intelligence.antibot import FactorySiteSessionStore


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Manual Chromium bootstrap for generic factory-site session state.")
    parser.add_argument("url", help="Target URL to open in Chromium for manual challenge completion.")
    parser.add_argument("--host", default="", help="Optional host override for session binding.")
    parser.add_argument("--session-root", default="", help="Optional FACTORY_SITE_SESSION_ROOT override.")
    parser.add_argument("--ttl-seconds", type=float, default=0.0, help="Optional session TTL override in seconds.")
    parser.add_argument("--timeout-ms", type=int, default=45000, help="Navigation timeout in milliseconds.")
    parser.add_argument("--proxy-mode", choices=("auto", "direct"), default="auto")
    parser.add_argument("--proxy-label", default="", help="Optional proxy label or proxy_id to pin for bootstrap.")
    parser.add_argument("--referer", default="", help="Optional referer value to store with the session.")
    parser.add_argument("--user-agent", default="", help="Optional user-agent override for the bootstrap context.")
    parser.add_argument("--manual", dest="manual", action="store_true", help="Wait for operator confirmation before saving storage state.")
    parser.add_argument("--no-manual", dest="manual", action="store_false", help="Save storage state immediately after page load.")
    parser.add_argument("--headed", dest="headless", action="store_false", help="Run Chromium in headed mode (default).")
    parser.add_argument("--headless", dest="headless", action="store_true", help="Run Chromium in headless mode.")
    parser.set_defaults(manual=True, headless=False)
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


def _resolve_host(url: str, host_override: str) -> str:
    if host_override.strip():
        return host_override.strip().lower()
    parsed = urlparse(url)
    host = (parsed.hostname or "").strip().lower()
    _assert(bool(host), f"Could not resolve host from URL: {url}")
    return host


def _selection_from_entry(entry: object) -> ProxySelection:
    return ProxySelection(
        url=str(getattr(entry, "url", "") or ""),
        source=str(getattr(entry, "source", "") or ""),
        proxy_id=str(getattr(entry, "proxy_id", "") or ""),
        label=str(getattr(entry, "label", "") or ""),
        host=str(getattr(entry, "host", "") or ""),
        port=str(getattr(entry, "port", "") or ""),
        country=str(getattr(entry, "country", "") or ""),
        via_proxy=bool(getattr(entry, "url", "")),
    )


def _select_proxy(args: argparse.Namespace, host: str) -> ProxySelection:
    if args.proxy_mode == "direct":
        return ProxySelection()
    pool = ProxyPool(os.getenv("PARSER_PROXIES"), proxy_file=os.getenv("PARSER_PROXIES_FILE", "").strip())
    if args.proxy_label.strip():
        needle = args.proxy_label.strip().lower()
        for entry in pool.entries:
            proxy_id = str(getattr(entry, "proxy_id", "") or "").strip().lower()
            label = str(getattr(entry, "label", "") or "").strip().lower()
            if needle in {proxy_id, label}:
                return _selection_from_entry(entry)
        raise RuntimeError(f"Proxy label not found: {args.proxy_label}")
    return pool.select(host)


def main() -> int:
    _configure_output_streams()
    args = parse_args()
    host = _resolve_host(args.url, args.host)
    store = FactorySiteSessionStore(
        root_dir=args.session_root.strip() or None,
        ttl_seconds=args.ttl_seconds if args.ttl_seconds > 0 else None,
    )
    paths = store.resolve(host)
    existing = store.load(host)
    proxy_selection = _select_proxy(args, host)

    try:
        from playwright.sync_api import sync_playwright
    except Exception as exc:
        raise RuntimeError("Playwright is required for bootstrap_factory_site_session.py") from exc

    print(f"url={args.url}")
    print(f"host={host}")
    print(f"domain={paths.domain}")
    print(f"session_file={paths.session_file}")
    print(f"storage_state_file={paths.storage_state_file}")
    print(f"existing_session={'yes' if existing is not None else 'no'}")
    print(f"proxy_label_or_id={proxy_selection.proxy_label_or_id or '-'}")
    print(f"proxy_mode={'proxy' if proxy_selection.via_proxy else 'direct'}")

    browser = None
    context = None
    page = None
    try:
        launch_kwargs: dict[str, object] = {"headless": bool(args.headless)}
        browser_proxy = proxy_selection.browser_proxy
        if browser_proxy:
            launch_kwargs["proxy"] = browser_proxy

        with sync_playwright() as playwright:
            browser = playwright.chromium.launch(**launch_kwargs)
            context_kwargs: dict[str, object] = {
                "ignore_https_errors": True,
                "locale": "ru-RU",
                "viewport": {"width": 1440, "height": 960},
            }
            if args.user_agent.strip():
                context_kwargs["user_agent"] = args.user_agent.strip()
            elif existing is not None and existing.user_agent:
                context_kwargs["user_agent"] = existing.user_agent
            if existing is not None and existing.storage_state_path and Path(existing.storage_state_path).exists():
                context_kwargs["storage_state"] = existing.storage_state_path

            context = browser.new_context(**context_kwargs)
            page = context.new_page()
            response = page.goto(args.url, wait_until="domcontentloaded", timeout=max(1, int(args.timeout_ms)))
            print(f"initial_http_status={response.status if response else 0}")
            print(f"loaded_url={page.url}")

            if args.manual:
                input("Complete the browser challenge/login if needed, then press Enter to save storage state...")

            storage_payload = context.storage_state()
            user_agent = args.user_agent.strip()
            if not user_agent:
                user_agent = str(page.evaluate("() => navigator.userAgent") or "")
            profile = store.save(
                host=host,
                storage_payload=storage_payload,
                final_url=page.url,
                user_agent=user_agent,
                referer=args.referer.strip() or args.url,
                proxy_label_or_id=proxy_selection.proxy_label_or_id,
                manual_bootstrap=True,
            )
    finally:
        try:
            if page is not None:
                page.close()
        except Exception:
            pass
        try:
            if context is not None:
                context.close()
        except Exception:
            pass
        try:
            if browser is not None:
                browser.close()
        except Exception:
            pass

    print(f"saved_final_url={profile.final_url}")
    print(f"saved_user_agent={profile.user_agent or '-'}")
    print(f"saved_proxy_label_or_id={profile.proxy_label_or_id or '-'}")
    print(f"manual_bootstrap={profile.manual_bootstrap}")
    print(f"expires_at={profile.expires_at}")
    print(f"session_file={paths.session_file}")
    print(f"storage_state_file={paths.storage_state_file}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

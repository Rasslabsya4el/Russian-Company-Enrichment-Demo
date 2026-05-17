from __future__ import annotations

import argparse
import os
import sys
from dataclasses import asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

if __package__ in {None, ""}:
    sys.path.append(str(Path(__file__).resolve().parents[2]))

from app.runtime.files import atomic_write_json, atomic_write_text, ensure_dir, load_env_file
from app.runtime.proxy6 import Proxy6ApiError, Proxy6Client, Proxy6Proxy, build_parser_proxy_urls


def utc_now_iso() -> str:
    return datetime.now(tz=timezone.utc).replace(microsecond=0).isoformat()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Sync Proxy6 active proxies into parser-ready pool files.")
    parser.add_argument("--env-file", default=".env", help="Optional .env file to load before reading env vars.")
    parser.add_argument("--api-key", default="", help="Proxy6 API key (fallback: PROXY6_API_KEY env).")
    parser.add_argument("--state", default="active", choices=("active", "expiring", "expired", "all"), help="Proxy6 getproxy state.")
    parser.add_argument("--descr", default="", help="Filter by technical description.")
    parser.add_argument("--country", default="ru", help="Filter resulting proxies by country (iso2). Use 'all' to skip.")
    parser.add_argument("--limit", type=int, default=1000, help="Proxy6 page size for getproxy (max 1000).")
    parser.add_argument("--max-pages", type=int, default=20, help="Safety bound for pagination.")
    parser.add_argument("--max-proxies", type=int, default=0, help="Trim resulting pool to N proxies. 0 means all.")
    parser.add_argument("--allow-socks", action="store_true", help="Write SOCKS URLs only for SOCKS-only proxies.")
    parser.add_argument("--check", action="store_true", help="Call Proxy6 check method for every proxy.")
    parser.add_argument("--require-check-ok", action="store_true", help="Keep only proxies where check returned proxy_status=true.")
    parser.add_argument("--strategy", default="sticky_by_host", choices=("round_robin", "sticky_by_host"), help="Runtime pool strategy.")
    parser.add_argument("--sticky-ttl-seconds", type=int, default=900, help="Sticky period for one host in seconds.")
    parser.add_argument("--ban-cooldown-seconds", type=int, default=300, help="Cooldown after proxy transport failures.")
    parser.add_argument(
        "--output-json",
        default="runtime_local/data/proxies/proxy6_pool.json",
        help="Where to write JSON pool for PARSER_PROXIES_FILE.",
    )
    parser.add_argument(
        "--output-list",
        default="runtime_local/data/proxies/proxy6_pool.txt",
        help="Where to write line-by-line proxy list.",
    )
    parser.add_argument(
        "--output-env",
        default="runtime_local/data/proxies/proxy6_pool.env",
        help="Where to write env snippet with parser proxy settings.",
    )
    parser.add_argument("--ensure-count", type=int, default=0, help="If current filtered pool is smaller, buy the missing amount.")
    parser.add_argument("--buy-period", type=int, default=30, help="Buy period in days for ensure-count.")
    parser.add_argument("--buy-country", default="ru", help="Country for ensure-count auto-buy.")
    parser.add_argument("--buy-version", type=int, default=4, help="Version for auto-buy: 4/3/6.")
    parser.add_argument("--buy-type", default="", choices=("", "http", "socks"), help="Optional protocol for newly bought proxies.")
    parser.add_argument("--buy-descr", default="parser_pool", help="Description for auto-buy.")
    parser.add_argument("--buy-auto-prolong", action="store_true", help="Enable auto prolong for newly bought proxies.")
    parser.add_argument("--dry-run", action="store_true", help="Do not write files, only print summary.")
    return parser.parse_args()


def _build_proxy_record(proxy: Proxy6Proxy, *, allow_socks: bool, check_payload: dict[str, Any] | None) -> dict[str, Any]:
    record = asdict(proxy)
    record["url"] = proxy.as_url(prefer_socks=allow_socks and proxy.proxy_type == "socks")
    if check_payload is not None:
        record["check"] = check_payload
        record["check_ok"] = bool(check_payload.get("proxy_status"))
    return record


def _print_summary(
    *,
    balance: Any,
    currency: Any,
    total_fetched: int,
    total_after_filters: int,
    total_after_checks: int,
    strategy: str,
    output_json: Path,
    output_env: Path,
    dry_run: bool,
) -> None:
    print(f"Balance: {balance} {currency}")
    print(f"Fetched proxies: {total_fetched}")
    print(f"After filters: {total_after_filters}")
    print(f"After check filters: {total_after_checks}")
    print(f"Runtime strategy: {strategy}")
    if dry_run:
        print("Dry-run: no files were written.")
        return
    print(f"Pool JSON: {output_json}")
    print(f"Env snippet: {output_env}")
    print("Next step:")
    print(f"  set PARSER_PROXIES_FILE={output_json}")
    print("  then run parser as usual.")


def _fetch_all_proxies(args: argparse.Namespace, client: Proxy6Client) -> list[Proxy6Proxy]:
    return client.get_all_proxies(
        state=args.state,
        descr=args.descr,
        limit=max(1, min(args.limit, 1000)),
        max_pages=max(1, args.max_pages),
    )


def _filter_proxies(args: argparse.Namespace, proxies: list[Proxy6Proxy]) -> list[Proxy6Proxy]:
    country_filter = args.country.strip().lower()
    filtered = [
        item
        for item in proxies
        if item.active and (country_filter in {"", "all"} or item.country == country_filter)
    ]
    if args.max_proxies > 0:
        filtered = filtered[: args.max_proxies]
    return filtered


def main() -> int:
    args = parse_args()
    env_file = Path(args.env_file).expanduser()
    if args.env_file and env_file.exists():
        load_env_file(env_file)
    api_key = (args.api_key or os.getenv("PROXY6_API_KEY", "")).strip()
    if not api_key:
        raise SystemExit("PROXY6_API_KEY is empty. Pass --api-key or set env variable.")

    client = Proxy6Client(api_key, timeout_seconds=30, max_rps=2.9)
    try:
        account_payload = client.get_account()
        all_proxies = _fetch_all_proxies(args, client)
        filtered = _filter_proxies(args, all_proxies)
        if args.ensure_count > 0 and len(filtered) < args.ensure_count:
            deficit = args.ensure_count - len(filtered)
            buy_payload = client.buy(
                count=deficit,
                period=args.buy_period,
                country=args.buy_country,
                version=args.buy_version,
                proxy_type=args.buy_type,
                descr=args.buy_descr,
                auto_prolong=args.buy_auto_prolong,
            )
            print(
                "Auto-buy completed: "
                f"order_id={buy_payload.get('order_id', 'unknown')} "
                f"count={buy_payload.get('count', deficit)} "
                f"price={buy_payload.get('price', 'unknown')}"
            )
            account_payload = client.get_account()
            all_proxies = _fetch_all_proxies(args, client)
            filtered = _filter_proxies(args, all_proxies)

        total_fetched = len(all_proxies)
        total_after_filters = len(filtered)

        check_map: dict[str, dict[str, Any]] = {}
        if args.check and filtered:
            for proxy in filtered:
                try:
                    check_map[proxy.id] = client.check_proxy(proxy_id=proxy.id)
                except Proxy6ApiError as exc:
                    check_map[proxy.id] = {
                        "proxy_status": False,
                        "error": str(exc),
                        "error_id": exc.error_id,
                    }
            if args.require_check_ok:
                filtered = [proxy for proxy in filtered if bool((check_map.get(proxy.id) or {}).get("proxy_status"))]
        total_after_checks = len(filtered)

        proxy_urls = build_parser_proxy_urls(filtered, allow_socks=args.allow_socks)
        output_json = Path(args.output_json).expanduser()
        output_list = Path(args.output_list).expanduser()
        output_env = Path(args.output_env).expanduser()
        pool_payload = {
            "provider": "proxy6",
            "generated_at": utc_now_iso(),
            "account": {
                "user_id": account_payload.get("user_id"),
                "balance": account_payload.get("balance"),
                "currency": account_payload.get("currency"),
            },
            "strategy": args.strategy,
            "sticky_ttl_seconds": int(args.sticky_ttl_seconds),
            "ban_cooldown_seconds": int(args.ban_cooldown_seconds),
            "total_fetched": total_fetched,
            "total_after_filters": total_after_filters,
            "total_after_checks": total_after_checks,
            "proxies": [
                _build_proxy_record(
                    proxy,
                    allow_socks=args.allow_socks,
                    check_payload=check_map.get(proxy.id) if args.check else None,
                )
                for proxy in filtered
            ],
        }
        env_lines = [
            f"PARSER_PROXIES_FILE={output_json.as_posix()}",
            f"PARSER_PROXY_STRATEGY={args.strategy}",
            f"PARSER_PROXY_STICKY_TTL_SECONDS={int(args.sticky_ttl_seconds)}",
            f"PARSER_PROXY_BAN_COOLDOWN_SECONDS={int(args.ban_cooldown_seconds)}",
        ]
        if proxy_urls and len(proxy_urls) <= 20:
            env_lines.append(f"PARSER_PROXIES={','.join(proxy_urls)}")
        env_text = "\n".join(env_lines) + "\n"

        if not args.dry_run:
            ensure_dir(output_json.parent)
            ensure_dir(output_list.parent)
            ensure_dir(output_env.parent)
            atomic_write_json(output_json, pool_payload)
            atomic_write_text(output_list, "\n".join(proxy_urls) + ("\n" if proxy_urls else ""))
            atomic_write_text(output_env, env_text)

        _print_summary(
            balance=account_payload.get("balance", "unknown"),
            currency=account_payload.get("currency", ""),
            total_fetched=total_fetched,
            total_after_filters=total_after_filters,
            total_after_checks=total_after_checks,
            strategy=args.strategy,
            output_json=output_json,
            output_env=output_env,
            dry_run=args.dry_run,
        )
        return 0
    except Proxy6ApiError as exc:
        print(f"Proxy6 API error: {exc} (error_id={exc.error_id}, status_code={exc.status_code})")
        if exc.payload:
            print(exc.payload)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())

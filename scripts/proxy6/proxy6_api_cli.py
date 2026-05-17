from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path
from typing import Any

if __package__ in {None, ""}:
    sys.path.append(str(Path(__file__).resolve().parents[2]))

from app.runtime.files import load_env_file
from app.runtime.proxy6 import Proxy6ApiError, Proxy6Client, parse_proxy6_list


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Proxy6 API helper CLI.")
    parser.add_argument("--env-file", default=".env", help="Optional .env file to preload.")
    parser.add_argument("--api-key", default="", help="Proxy6 API key (fallback: PROXY6_API_KEY env).")

    subparsers = parser.add_subparsers(dest="command", required=True)

    subparsers.add_parser("account", help="Show balance and account metadata.")

    countries = subparsers.add_parser("countries", help="List available countries.")
    countries.add_argument("--version", type=int, default=4)

    count = subparsers.add_parser("count", help="Get available proxy count for country.")
    count.add_argument("--country", default="ru")
    count.add_argument("--version", type=int, default=4)

    price = subparsers.add_parser("price", help="Estimate price for an order.")
    price.add_argument("--count", type=int, required=True)
    price.add_argument("--period", type=int, required=True)
    price.add_argument("--version", type=int, default=4)

    list_cmd = subparsers.add_parser("list", help="List owned proxies.")
    list_cmd.add_argument("--state", default="active", choices=("active", "expiring", "expired", "all"))
    list_cmd.add_argument("--descr", default="")
    list_cmd.add_argument("--limit", type=int, default=1000)
    list_cmd.add_argument("--page", type=int, default=1)
    list_cmd.add_argument("--with-keys", action="store_true", help="Keep associative keys in API payload.")

    check = subparsers.add_parser("check", help="Check proxy validity.")
    check.add_argument("--id", default="", help="Proxy internal id.")
    check.add_argument("--proxy", default="", help="Proxy string ip:port:user:pass.")

    buy = subparsers.add_parser("buy", help="Buy proxies.")
    buy.add_argument("--count", type=int, required=True)
    buy.add_argument("--period", type=int, required=True)
    buy.add_argument("--country", required=True)
    buy.add_argument("--version", type=int, default=4)
    buy.add_argument("--type", default="", choices=("", "http", "socks"))
    buy.add_argument("--descr", default="")
    buy.add_argument("--auto-prolong", action="store_true")
    buy.add_argument("--nokey", action="store_true")

    prolong = subparsers.add_parser("prolong", help="Prolong proxies.")
    prolong.add_argument("--ids", required=True, help="Comma-separated proxy ids.")
    prolong.add_argument("--period", type=int, required=True)
    prolong.add_argument("--nokey", action="store_true")

    delete = subparsers.add_parser("delete", help="Delete proxies.")
    delete.add_argument("--ids", default="", help="Comma-separated proxy ids.")
    delete.add_argument("--descr", default="")

    set_descr = subparsers.add_parser("set-descr", help="Update proxy descriptions.")
    set_descr.add_argument("--new-descr", required=True)
    set_descr.add_argument("--old-descr", default="")
    set_descr.add_argument("--ids", default="", help="Comma-separated proxy ids.")

    set_type = subparsers.add_parser("set-type", help="Change proxy protocol type.")
    set_type.add_argument("--ids", required=True, help="Comma-separated proxy ids.")
    set_type.add_argument("--type", required=True, choices=("http", "socks"))

    ipauth = subparsers.add_parser("ipauth", help="Bind or clear IP authorization for all proxies.")
    ipauth.add_argument("--ips", default="", help="Comma-separated IP list.")
    ipauth.add_argument("--clear", action="store_true", help="Remove IP authorization.")

    return parser


def print_json(payload: dict[str, Any]) -> None:
    print(json.dumps(payload, ensure_ascii=False, indent=2))


def _split_csv(value: str) -> list[str]:
    return [item.strip() for item in value.split(",") if item.strip()]


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    env_file = Path(args.env_file).expanduser()
    if args.env_file and env_file.exists():
        load_env_file(env_file)
    api_key = (args.api_key or os.getenv("PROXY6_API_KEY", "")).strip()
    if not api_key:
        raise SystemExit("PROXY6_API_KEY is empty. Pass --api-key or set env variable.")
    client = Proxy6Client(api_key, timeout_seconds=30, max_rps=2.9)
    try:
        if args.command == "account":
            print_json(client.get_account())
            return 0
        if args.command == "countries":
            print_json(client.get_countries(version=args.version))
            return 0
        if args.command == "count":
            print_json(client.get_count(country=args.country, version=args.version))
            return 0
        if args.command == "price":
            print_json(client.get_price(count=args.count, period=args.period, version=args.version))
            return 0
        if args.command == "list":
            payload = client.call(
                "getproxy",
                params={
                    "state": args.state,
                    "descr": args.descr,
                    "page": max(1, args.page),
                    "limit": max(1, min(args.limit, 1000)),
                    **({} if args.with_keys else {"nokey": ""}),
                },
            )
            proxies = parse_proxy6_list(payload)
            print_json(
                {
                    "status": payload.get("status", "yes"),
                    "balance": payload.get("balance"),
                    "currency": payload.get("currency"),
                    "list_count": len(proxies),
                    "proxies": [proxy.raw for proxy in proxies],
                }
            )
            return 0
        if args.command == "check":
            if args.id:
                print_json(client.check_proxy(proxy_id=args.id))
                return 0
            if args.proxy:
                print_json(client.check_proxy(proxy_string=args.proxy))
                return 0
            raise SystemExit("Pass --id or --proxy for check command.")
        if args.command == "buy":
            print_json(
                client.buy(
                    count=args.count,
                    period=args.period,
                    country=args.country,
                    version=args.version,
                    proxy_type=args.type,
                    descr=args.descr,
                    auto_prolong=args.auto_prolong,
                    nokey=args.nokey,
                )
            )
            return 0
        if args.command == "prolong":
            print_json(client.prolong(ids=_split_csv(args.ids), period=args.period, nokey=args.nokey))
            return 0
        if args.command == "delete":
            print_json(client.delete(ids=_split_csv(args.ids), descr=args.descr))
            return 0
        if args.command == "set-descr":
            print_json(
                client.set_description(
                    new_descr=args.new_descr,
                    old_descr=args.old_descr,
                    ids=_split_csv(args.ids),
                )
            )
            return 0
        if args.command == "set-type":
            print_json(client.set_type(ids=_split_csv(args.ids), proxy_type=args.type))
            return 0
        if args.command == "ipauth":
            if args.clear:
                print_json(client.clear_ip_auth())
                return 0
            ips = _split_csv(args.ips)
            if not ips:
                raise SystemExit("Pass --ips or use --clear for ipauth command.")
            print_json(client.set_ip_auth(ips=ips))
            return 0
    except Proxy6ApiError as exc:
        print_json(
            {
                "status": "no",
                "error": str(exc),
                "error_id": exc.error_id,
                "status_code": exc.status_code,
                "payload": exc.payload,
            }
        )
        return 2
    raise SystemExit(f"Unsupported command: {args.command}")


if __name__ == "__main__":
    raise SystemExit(main())

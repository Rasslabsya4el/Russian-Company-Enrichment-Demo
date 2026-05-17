from __future__ import annotations

import argparse
import csv
import json
import os
import random
import sys
import time
from collections import Counter
from dataclasses import asdict
from pathlib import Path
from typing import Any

from app.runtime import ProxyPool
from app.sources import RusprofileSource, SparkSource, ZachestnyBiznesSource

from company_enrichment_core import (
    ProgressStore,
    RateLimitedHttpClient,
    RowInput,
    SourceResult,
    configure_logger,
    ensure_dir,
    find_default_xlsx,
    is_valid_russian_inn,
    load_env_file,
    load_rows_from_xlsx,
    utc_now_iso,
)


SOURCE_LABELS = {
    "spark": "СПАРК",
    "rusprofile": "Rusprofile",
    "zachestnyibiznes": "ЗАЧЕСТНЫЙБИЗНЕС",
}

SOURCE_CLASSES = {
    "spark": SparkSource,
    "rusprofile": RusprofileSource,
    "zachestnyibiznes": ZachestnyBiznesSource,
}

SOURCE_DEFAULT_DELAYS = {
    "spark": [6.0, 5.0, 4.0, 3.5],
    "rusprofile": [8.0, 7.0, 6.0, 5.0],
    "zachestnyibiznes": [6.0, 5.0, 4.0, 3.5],
}

SOURCE_HOSTS = {
    "spark": {"spark-interfax.ru"},
    "rusprofile": {"www.rusprofile.ru", "rusprofile.ru"},
    "zachestnyibiznes": {"zachestnyibiznes.ru"},
}

BLOCK_STATUSES = {"rate_limited", "bot_gate"}
REQUEST_EVENT_TYPES = {"request_ok", "request_ok_insecure_tls", "rate_limited", "bot_gate", "http_error"}


def parse_delay_values(raw_value: str, source: str) -> list[float]:
    if not raw_value.strip():
        return list(SOURCE_DEFAULT_DELAYS[source])
    values: list[float] = []
    for chunk in raw_value.split(","):
        item = chunk.strip()
        if not item:
            continue
        values.append(float(item))
    if not values:
        raise ValueError("Не удалось распарсить список задержек")
    return values


def delay_token(value: float) -> str:
    return str(value).replace(".", "_")


def pick_rows(
    rows: list[RowInput],
    *,
    count: int,
    offset: int,
    random_sample: bool,
    seed: int,
) -> list[RowInput]:
    valid_rows = [row for row in rows if is_valid_russian_inn(row.inn)]
    if offset > 0:
        valid_rows = valid_rows[offset:]
    if count <= 0:
        raise ValueError("--count должен быть больше нуля")
    if random_sample:
        if count > len(valid_rows):
            raise ValueError(f"Запрошено {count} строк, но доступно только {len(valid_rows)} валидных ИНН")
        rng = random.Random(seed)
        selected = rng.sample(valid_rows, count)
        selected.sort(key=lambda row: row.row_index)
        return selected
    return valid_rows[:count]


def write_selected_rows_csv(path: Path, rows: list[RowInput]) -> None:
    ensure_dir(path.parent)
    with path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=["row_index", "inn", "company_name", "xlsx_site", "xlsx_phone", "comment"])
        writer.writeheader()
        for row in rows:
            writer.writerow(
                {
                    "row_index": row.row_index,
                    "inn": row.inn,
                    "company_name": row.company_name,
                    "xlsx_site": row.xlsx_site,
                    "xlsx_phone": row.xlsx_phone,
                    "comment": row.comment,
                }
            )


def source_contacts_summary(result: SourceResult) -> dict[str, Any]:
    return {
        "phones_open": sum(1 for item in result.phones if not item.masked),
        "phones_masked": sum(1 for item in result.phones if item.masked),
        "emails_open": sum(1 for item in result.emails if not item.masked),
        "emails_masked": sum(1 for item in result.emails if item.masked),
        "websites_open": sum(1 for item in result.websites if not item.masked),
        "websites_masked": sum(1 for item in result.websites if item.masked),
        "addresses_open": sum(1 for item in result.addresses if not item.masked),
        "addresses_masked": sum(1 for item in result.addresses if item.masked),
    }


def read_events(path: Path) -> list[dict[str, Any]]:
    events: list[dict[str, Any]] = []
    if not path.exists():
        return events
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line:
            continue
        try:
            payload = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(payload, dict):
            events.append(payload)
    return events


def merge_host_buckets(host_stats: dict[str, Any], hosts: set[str]) -> dict[str, Any]:
    merged = {
        "hosts": sorted(hosts),
        "total_events": 0,
        "event_types": {},
        "elapsed_seconds": {"count": 0, "sum": 0.0, "max": 0.0},
        "interval_seconds": {"count": 0, "sum": 0.0, "min": None, "max": 0.0},
        "cooldown_seconds": {"count": 0, "sum": 0.0, "max": 0.0},
    }
    for host, payload in host_stats.items():
        if host not in hosts or not isinstance(payload, dict):
            continue
        merged["total_events"] += int(payload.get("total_events", 0) or 0)
        for event_type, count in (payload.get("event_types") or {}).items():
            merged["event_types"][event_type] = merged["event_types"].get(event_type, 0) + int(count or 0)
        for metric_name in ("elapsed_seconds", "interval_seconds", "cooldown_seconds"):
            bucket = payload.get(metric_name) or {}
            target = merged[metric_name]
            count = int(bucket.get("count", 0) or 0)
            total_sum = float(bucket.get("sum", 0.0) or 0.0)
            maximum = float(bucket.get("max", 0.0) or 0.0)
            target["count"] += count
            target["sum"] += total_sum
            target["max"] = max(float(target.get("max", 0.0) or 0.0), maximum)
            if metric_name == "interval_seconds":
                current_min = bucket.get("min")
                if isinstance(current_min, (int, float)):
                    if target["min"] is None:
                        target["min"] = float(current_min)
                    else:
                        target["min"] = min(float(target["min"]), float(current_min))
    for metric_name in ("elapsed_seconds", "interval_seconds", "cooldown_seconds"):
        bucket = merged[metric_name]
        if bucket["count"]:
            bucket["avg"] = round(bucket["sum"] / bucket["count"], 4)
    return merged


def build_case_summary(
    *,
    source: str,
    delay_seconds: float,
    rows: list[RowInput],
    row_results: list[dict[str, Any]],
    progress: ProgressStore,
    started_at_epoch: float,
    ended_at_epoch: float,
) -> dict[str, Any]:
    statuses = Counter(item.get("status", "unknown") for item in row_results)
    events = read_events(progress.events_jsonl)
    target_hosts = SOURCE_HOSTS[source]
    target_events = [event for event in events if event.get("host") in target_hosts]
    target_request_events = [event for event in target_events if event.get("type") in REQUEST_EVENT_TYPES]
    first_block_event = next((event for event in target_events if event.get("type") in {"rate_limited", "bot_gate"}), None)
    first_block_row = next((item for item in row_results if item.get("status") in BLOCK_STATUSES), None)
    merged_host_stats = merge_host_buckets(progress.host_stats, target_hosts)

    elapsed_total = round(ended_at_epoch - started_at_epoch, 3)
    processed_rows = len(row_results)
    rows_per_minute = round(processed_rows / max(elapsed_total / 60, 1e-9), 2) if elapsed_total > 0 else 0.0
    successes = int(statuses.get("success", 0))
    masked_companies = sum(1 for item in row_results if item.get("has_any_masked_field"))
    companies_with_contacts = sum(1 for item in row_results if item.get("has_any_open_contact"))

    summary = {
        "source": source,
        "source_label": SOURCE_LABELS[source],
        "delay_seconds": delay_seconds,
        "started_at": time.strftime("%Y-%m-%dT%H:%M:%S%z", time.localtime(started_at_epoch)),
        "finished_at": time.strftime("%Y-%m-%dT%H:%M:%S%z", time.localtime(ended_at_epoch)),
        "elapsed_seconds": elapsed_total,
        "rows_planned": len(rows),
        "rows_processed": processed_rows,
        "rows_per_minute": rows_per_minute,
        "status_counts": dict(statuses),
        "success_rate": round(successes / processed_rows, 4) if processed_rows else 0.0,
        "companies_with_any_open_contact": companies_with_contacts,
        "companies_with_any_masked_field": masked_companies,
        "first_block": {
            "event_type": first_block_event.get("type") if first_block_event else "",
            "cooldown_seconds": first_block_event.get("cooldown_seconds") if first_block_event else 0,
            "elapsed_seconds": first_block_event.get("elapsed_seconds") if first_block_event else 0,
            "request_number": (
                next((idx for idx, event in enumerate(target_request_events, start=1) if event is first_block_event), 0)
                if first_block_event
                else 0
            ),
            "row_index": first_block_row.get("row_index") if first_block_row else 0,
            "inn": first_block_row.get("inn", "") if first_block_row else "",
            "company_name": first_block_row.get("company_name", "") if first_block_row else "",
            "row_status": first_block_row.get("status", "") if first_block_row else "",
            "row_elapsed_seconds": first_block_row.get("duration_seconds", 0.0) if first_block_row else 0.0,
        },
        "target_host_stats": merged_host_stats,
        "events_file": progress.events_jsonl.name,
        "host_stats_file": progress.host_stats_json.name,
        "row_results_file": "row_results.json",
    }
    summary["verdict"] = "blocked" if summary["first_block"]["event_type"] else "clean"
    return summary


def render_case_markdown(summary: dict[str, Any], row_results: list[dict[str, Any]]) -> str:
    first_block = summary.get("first_block") or {}
    host_stats = summary.get("target_host_stats") or {}
    event_types = host_stats.get("event_types") or {}
    lines = [
        f"# Calibration {summary.get('source_label', summary.get('source', ''))}",
        "",
        f"- Delay: `{summary.get('delay_seconds')}` seconds",
        f"- Rows planned: `{summary.get('rows_planned')}`",
        f"- Rows processed: `{summary.get('rows_processed')}`",
        f"- Elapsed: `{summary.get('elapsed_seconds')}` seconds",
        f"- Rows/min: `{summary.get('rows_per_minute')}`",
        f"- Verdict: `{summary.get('verdict')}`",
        "",
        "## Status Counts",
    ]
    for status, count in sorted((summary.get("status_counts") or {}).items()):
        lines.append(f"- `{status}`: `{count}`")
    lines.extend(
        [
            "",
            "## Host Request Stats",
            f"- request_ok: `{event_types.get('request_ok', 0) + event_types.get('request_ok_insecure_tls', 0)}`",
            f"- rate_limited: `{event_types.get('rate_limited', 0)}`",
            f"- bot_gate: `{event_types.get('bot_gate', 0)}`",
            f"- http_error: `{event_types.get('http_error', 0)}`",
            f"- cooldown_skip: `{event_types.get('cooldown_skip', 0)}`",
            f"- avg interval: `{(host_stats.get('interval_seconds') or {}).get('avg', 0)}`",
            f"- avg latency: `{(host_stats.get('elapsed_seconds') or {}).get('avg', 0)}`",
            f"- max cooldown: `{(host_stats.get('cooldown_seconds') or {}).get('max', 0)}`",
            "",
            "## First Block",
        ]
    )
    if first_block.get("event_type"):
        lines.extend(
            [
                f"- Event: `{first_block.get('event_type')}`",
                f"- Request number on target host: `{first_block.get('request_number')}`",
                f"- Company: `{first_block.get('inn')}` {first_block.get('company_name')}",
                f"- Row status: `{first_block.get('row_status')}`",
                f"- Cooldown: `{first_block.get('cooldown_seconds')}` seconds",
            ]
        )
    else:
        lines.append("- No block detected on this run")
    lines.extend(["", "## Rows"])
    for item in row_results:
        issues = []
        if item.get("errors"):
            issues.append("errors=" + "; ".join(item.get("errors") or []))
        if item.get("has_any_masked_field"):
            issues.append("masked=true")
        if item.get("has_any_open_contact"):
            issues.append("open_contact=true")
        issues_text = " | ".join(issues) if issues else "—"
        lines.append(
            f"- `{item.get('row_index')}` `{item.get('inn')}` {item.get('company_name')} | status=`{item.get('status')}` | duration=`{item.get('duration_seconds')}`s | {issues_text}"
        )
    lines.append("")
    return "\n".join(lines)


def render_master_markdown(
    *,
    source: str,
    selected_rows: list[RowInput],
    delays: list[float],
    case_summaries: list[dict[str, Any]],
    random_sample: bool,
    seed: int,
    input_path: Path,
) -> str:
    lines = [
        f"# Calibration Suite {SOURCE_LABELS[source]}",
        "",
        f"- Updated: `{utc_now_iso()}`",
        f"- Source: `{source}`",
        f"- Input: `{input_path}`",
        f"- Rows in sample: `{len(selected_rows)}`",
        f"- Random sample: `{str(random_sample).lower()}`",
        f"- Seed: `{seed}`",
        f"- Delay plan: `{', '.join(str(value) for value in delays)}`",
        "",
        "## Delay Results",
    ]
    for item in case_summaries:
        first_block = item.get("first_block") or {}
        host_stats = item.get("target_host_stats") or {}
        event_types = host_stats.get("event_types") or {}
        lines.append(
            f"- delay=`{item.get('delay_seconds')}` | verdict=`{item.get('verdict')}` | rows=`{item.get('rows_processed')}/{item.get('rows_planned')}` | rows_per_min=`{item.get('rows_per_minute')}` | request_ok=`{event_types.get('request_ok', 0) + event_types.get('request_ok_insecure_tls', 0)}` | 429=`{event_types.get('rate_limited', 0)}` | bot_gate=`{event_types.get('bot_gate', 0)}` | first_block_request=`{first_block.get('request_number', 0)}`"
        )
    clean_cases = [item for item in case_summaries if item.get("verdict") == "clean"]
    lines.extend(["", "## Recommendation"])
    if clean_cases:
        best = min(clean_cases, key=lambda item: float(item.get("delay_seconds", 0)))
        lines.append(
            f"- Current best safe delay on this sample: `{best.get('delay_seconds')}` seconds. Это самый быстрый delay без зафиксированного блока на выбранной выборке."
        )
    else:
        lines.append("- На этой серии все проверенные delay словили блок. Нужно повышать задержку или менять IP/сессию перед следующим тестом.")
    lines.extend(["", "## Sample Rows"])
    for row in selected_rows:
        lines.append(f"- `{row.row_index}` `{row.inn}` {row.company_name}")
    lines.append("")
    return "\n".join(lines)


def build_row_result(row: RowInput, result: SourceResult, duration_seconds: float) -> dict[str, Any]:
    contacts = source_contacts_summary(result)
    return {
        "row_index": row.row_index,
        "inn": row.inn,
        "company_name": row.company_name,
        "status": result.status,
        "duration_seconds": round(duration_seconds, 3),
        "entity_url": result.entity_url,
        "search_url": result.search_url,
        "listing_url": result.listing_url,
        "errors": list(result.errors),
        "notes": list(result.notes),
        "links": list(result.links),
        "http_status": result.http_status,
        "company_name_found": result.company_name_found,
        "contacts": contacts,
        "has_any_open_contact": any(contacts[key] > 0 for key in ("phones_open", "emails_open", "websites_open", "addresses_open")),
        "has_any_masked_field": any(contacts[key] > 0 for key in ("phones_masked", "emails_masked", "websites_masked", "addresses_masked")),
        "availability": result.availability,
        "raw_source_result": asdict(result),
    }


def create_http_client(
    *,
    logger: Any,
    progress: ProgressStore,
    source: str,
    delay_seconds: float,
    request_timeout: int,
    cooldown_429_seconds: int,
    cooldown_bot_seconds: int,
) -> RateLimitedHttpClient:
    min_delay_by_host = {}
    if source == "spark":
        min_delay_by_host = {"spark-interfax.ru": delay_seconds}
    elif source == "rusprofile":
        min_delay_by_host = {"www.rusprofile.ru": delay_seconds, "rusprofile.ru": delay_seconds}
    elif source == "zachestnyibiznes":
        min_delay_by_host = {"zachestnyibiznes.ru": delay_seconds}
    return RateLimitedHttpClient(
        logger=logger,
        progress_store=progress,
        min_delay_by_host=min_delay_by_host,
        request_timeout=request_timeout,
        cooldown_on_429=cooldown_429_seconds,
        cooldown_on_bot=cooldown_bot_seconds,
        proxy_pool=ProxyPool(os.getenv("PARSER_PROXIES")),
        list_org_session_file=None,
    )


def run_case(
    *,
    source: str,
    rows: list[RowInput],
    delay_seconds: float,
    output_dir: Path,
    request_timeout: int,
    cooldown_429_seconds: int,
    cooldown_bot_seconds: int,
    stop_on_first_block: bool,
) -> dict[str, Any]:
    ensure_dir(output_dir)
    logger = configure_logger(output_dir / "run.log")
    progress = ProgressStore(output_dir)
    client = create_http_client(
        logger=logger,
        progress=progress,
        source=source,
        delay_seconds=delay_seconds,
        request_timeout=request_timeout,
        cooldown_429_seconds=cooldown_429_seconds,
        cooldown_bot_seconds=cooldown_bot_seconds,
    )
    source_instance = SOURCE_CLASSES[source](client)
    row_results: list[dict[str, Any]] = []
    started_at_epoch = time.time()

    logger.info("Старт калибровки source=%s delay=%.2fs rows=%s", source, delay_seconds, len(rows))
    logger.info("Proxy pool enabled=%s", str(bool(os.getenv("PARSER_PROXIES", "").strip())).lower())

    for index, row in enumerate(rows, start=1):
        row_started_at = time.time()
        logger.info("[%s/%s] row=%s inn=%s company=%s", index, len(rows), row.row_index, row.inn, row.company_name)
        result = source_instance.search(row)
        row_payload = build_row_result(row, result, time.time() - row_started_at)
        row_results.append(row_payload)
        logger.info(
            "  status=%s duration=%.3fs open_contact=%s masked=%s",
            result.status,
            row_payload["duration_seconds"],
            row_payload["has_any_open_contact"],
            row_payload["has_any_masked_field"],
        )
        if result.status in BLOCK_STATUSES and stop_on_first_block:
            logger.warning("  остановка серии после первого блока: status=%s inn=%s", result.status, row.inn)
            break

    ended_at_epoch = time.time()
    summary = build_case_summary(
        source=source,
        delay_seconds=delay_seconds,
        rows=rows,
        row_results=row_results,
        progress=progress,
        started_at_epoch=started_at_epoch,
        ended_at_epoch=ended_at_epoch,
    )
    summary["proxy_pool_enabled"] = bool(os.getenv("PARSER_PROXIES", "").strip())

    row_results_path = output_dir / "row_results.json"
    summary_path = output_dir / "summary.json"
    summary_md_path = output_dir / "summary.md"
    row_results_path.write_text(json.dumps(row_results, ensure_ascii=False, indent=2), encoding="utf-8")
    summary_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    summary_md_path.write_text(render_case_markdown(summary, row_results), encoding="utf-8")
    return summary


def parse_args(argv: list[str] | None = None, *, fixed_source: str | None = None) -> argparse.Namespace:
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass
    parser = argparse.ArgumentParser(description="Калибровка задержек по одному агрегатору с логированием первых банов и throughput.")
    parser.add_argument("--source", choices=sorted(SOURCE_CLASSES.keys()), required=fixed_source is None, default=fixed_source or "", help="Какой агрегатор калибровать.")
    parser.add_argument("--env-file", default=".env", help="Локальный .env с настройками.")
    parser.add_argument("--input", default="", help="Путь до XLSX файла. По умолчанию первый .xlsx в текущей папке.")
    parser.add_argument("--output-dir", default="calibration_runs", help="Корневая папка для результатов калибровки.")
    parser.add_argument("--count", type=int, default=50, help="Сколько валидных ИНН брать в выборку.")
    parser.add_argument("--offset", type=int, default=0, help="Сдвиг по валидным строкам перед выборкой.")
    parser.add_argument("--random-sample", action="store_true", help="Брать случайную выборку, а не первые строки.")
    parser.add_argument("--seed", type=int, default=42, help="Seed для random sample.")
    parser.add_argument("--delays", default="", help="Список задержек через запятую, например 8,7,6.")
    parser.add_argument("--request-timeout", type=int, default=18, help="Таймаут одного HTTP-запроса.")
    parser.add_argument("--cooldown-429-seconds", type=int, default=3600, help="Cooldown при 429.")
    parser.add_argument("--cooldown-bot-seconds", type=int, default=5400, help="Cooldown при bot gate.")
    parser.add_argument("--continue-after-block", action="store_true", help="Не останавливать delay-case после первого блока.")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None, *, fixed_source: str | None = None) -> int:
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass
    args = parse_args(argv, fixed_source=fixed_source)
    env_file = Path(args.env_file)
    load_env_file(env_file)
    if args.request_timeout == 18 and os.getenv("REQUEST_TIMEOUT_SECONDS", "").strip():
        args.request_timeout = int(os.getenv("REQUEST_TIMEOUT_SECONDS", "18"))
    if args.cooldown_429_seconds == 3600 and os.getenv("COOLDOWN_429_SECONDS", "").strip():
        args.cooldown_429_seconds = int(os.getenv("COOLDOWN_429_SECONDS", "3600"))
    if args.cooldown_bot_seconds == 5400 and os.getenv("COOLDOWN_BOT_SECONDS", "").strip():
        args.cooldown_bot_seconds = int(os.getenv("COOLDOWN_BOT_SECONDS", "5400"))

    input_path = Path(args.input) if args.input else find_default_xlsx(Path.cwd())
    rows = load_rows_from_xlsx(input_path)
    selected_rows = pick_rows(
        rows,
        count=args.count,
        offset=args.offset,
        random_sample=args.random_sample,
        seed=args.seed,
    )
    delays = parse_delay_values(args.delays, args.source)

    suite_dir = Path(args.output_dir) / args.source / time.strftime("%Y%m%d_%H%M%S")
    ensure_dir(suite_dir)
    write_selected_rows_csv(suite_dir / "selected_rows.csv", selected_rows)
    (suite_dir / "selected_rows.json").write_text(
        json.dumps([row.__dict__ for row in selected_rows], ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    case_summaries: list[dict[str, Any]] = []
    for delay_seconds in delays:
        case_dir = suite_dir / f"delay_{delay_token(delay_seconds)}"
        summary = run_case(
            source=args.source,
            rows=selected_rows,
            delay_seconds=delay_seconds,
            output_dir=case_dir,
            request_timeout=args.request_timeout,
            cooldown_429_seconds=args.cooldown_429_seconds,
            cooldown_bot_seconds=args.cooldown_bot_seconds,
            stop_on_first_block=not args.continue_after_block,
        )
        case_summaries.append(summary)
        if summary.get("verdict") == "blocked":
            break

    master_summary = {
        "updated_at": utc_now_iso(),
        "source": args.source,
        "source_label": SOURCE_LABELS[args.source],
        "input": str(input_path),
        "suite_dir": str(suite_dir),
        "count": len(selected_rows),
        "offset": args.offset,
        "random_sample": args.random_sample,
        "seed": args.seed,
        "delays": delays,
        "cases": case_summaries,
    }
    (suite_dir / "summary.json").write_text(json.dumps(master_summary, ensure_ascii=False, indent=2), encoding="utf-8")
    (suite_dir / "summary.md").write_text(
        render_master_markdown(
            source=args.source,
            selected_rows=selected_rows,
            delays=delays,
            case_summaries=case_summaries,
            random_sample=args.random_sample,
            seed=args.seed,
            input_path=input_path,
        ),
        encoding="utf-8",
    )
    print(f"Calibration suite ready: {suite_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

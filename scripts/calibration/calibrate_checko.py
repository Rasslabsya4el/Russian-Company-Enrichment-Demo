from __future__ import annotations

import argparse
import csv
import json
import logging
import os
import statistics
import sys
import time
from collections import Counter
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Sequence

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from app.runtime import ProxyPool
from app.runtime.files import atomic_write_json, atomic_write_text, ensure_dir, load_env_file
from app.sources.checko import CheckoSource
from company_enrichment_core import (
    IMPORTANT_FIELDS,
    RateLimitedHttpClient,
    RequestOutcome,
    RowInput,
    SourceResult,
    is_valid_russian_inn,
    normalize_inn,
    normalize_whitespace,
)


CHECKO_HOSTS = ("checko.ru", "www.checko.ru")
ATTEMPT_FIELDNAMES = [
    "inn",
    "attempt_no",
    "delay_seconds",
    "source_status",
    "http_status",
    "elapsed_seconds",
    "final_url",
    "proxy_mode",
    "proxy_label",
    "error_or_reason",
]


@dataclass(frozen=True)
class AttemptRecord:
    inn: str
    attempt_no: int
    delay_seconds: float
    source_status: str
    http_status: int | None
    elapsed_seconds: float
    final_url: str
    proxy_mode: str
    proxy_label: str
    error_or_reason: str


@dataclass(frozen=True)
class DelayBucketSummary:
    delay_seconds: float
    total_attempts: int
    ok: int
    blocked: int
    status_429: int
    bot_gate: int
    median_elapsed_seconds: float | None
    p95_elapsed_seconds: float | None
    status_counts: dict[str, int]


@dataclass(frozen=True)
class Recommendation:
    delay_seconds: float
    env_line: str
    verdict: str
    rationale: str
    confidence: str


@dataclass(frozen=True)
class TracedRequest:
    requested_url: str
    request_status: str
    http_status: int | None
    final_url: str
    elapsed_seconds: float
    proxy_mode: str
    proxy_label: str
    error: str


class CalibrationProgressStore:
    def __init__(self) -> None:
        self.events: list[dict[str, Any]] = []

    def append_event(self, payload: dict[str, Any]) -> None:
        self.events.append(dict(payload))


class TracingRateLimitedHttpClient(RateLimitedHttpClient):
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self.traces: list[TracedRequest] = []

    def request(
        self,
        url: str,
        *,
        source: str,
        allow_redirects: bool = True,
        timeout: int | None = None,
        proxy_selection: Any = None,
    ) -> RequestOutcome:
        outcome = super().request(
            url,
            source=source,
            allow_redirects=allow_redirects,
            timeout=timeout,
            proxy_selection=proxy_selection,
        )
        final_url = outcome.response.url if outcome.response is not None else ""
        http_status = outcome.response.status_code if outcome.response is not None else None
        self.traces.append(
            TracedRequest(
                requested_url=url,
                request_status=outcome.status,
                http_status=http_status,
                final_url=final_url,
                elapsed_seconds=round(float(outcome.elapsed_seconds or 0.0), 3),
                proxy_mode=normalize_whitespace(outcome.proxy_mode) or "direct",
                proxy_label=normalize_whitespace(outcome.proxy_label),
                error=normalize_whitespace(outcome.error),
            )
        )
        return outcome


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Live calibration CLI for Checko delay buckets with operator-readable evidence."
    )
    parser.add_argument("--env-file", default=".env", help="Optional .env file to load before reading proxy/runtime settings.")
    parser.add_argument(
        "--inn",
        action="append",
        required=True,
        help="Target company INN. Repeat the flag for multiple companies.",
    )
    parser.add_argument(
        "--delays",
        default="12,10,8,6",
        help="Comma-separated delay grid in seconds, for example: 12,10,8,6",
    )
    parser.add_argument(
        "--attempts-per-delay",
        type=int,
        default=3,
        help="How many sequential passes to run for each delay bucket.",
    )
    parser.add_argument(
        "--output-dir",
        default="runtime_local/calibration_runs/checko",
        help="Base directory where the calibration suite directory will be created.",
    )
    parser.add_argument(
        "--request-timeout",
        type=int,
        default=20,
        help="Timeout for one HTTP request in seconds.",
    )
    parser.add_argument(
        "--cooldown-429-seconds",
        type=int,
        default=120,
        help="Per-client cooldown used after HTTP 429 during calibration.",
    )
    parser.add_argument(
        "--cooldown-bot-seconds",
        type=int,
        default=180,
        help="Per-client cooldown used after bot-gate during calibration.",
    )
    parser.add_argument(
        "--continue-after-block",
        action="store_true",
        help="Continue the delay bucket even after a blocked/rate-limited/bot-gate attempt.",
    )
    parser.add_argument(
        "--proxy-strategy",
        choices=("round_robin", "sticky_by_host"),
        default=None,
        help="Optional proxy strategy override for the calibration run.",
    )
    return parser.parse_args(argv)


def parse_delay_values(raw_value: str) -> list[float]:
    values: list[float] = []
    for chunk in raw_value.split(","):
        item = normalize_whitespace(chunk)
        if not item:
            continue
        value = float(item)
        if value <= 0:
            raise ValueError("Delay values must be > 0")
        values.append(value)
    if not values:
        raise ValueError("At least one delay value is required")
    return values


def normalize_inn_list(raw_values: list[str]) -> list[str]:
    normalized: list[str] = []
    seen: set[str] = set()
    for raw_value in raw_values:
        inn = normalize_inn(raw_value)
        if not inn:
            continue
        if not is_valid_russian_inn(inn):
            raise ValueError(f"Invalid Russian INN: {raw_value!r}")
        if inn in seen:
            continue
        seen.add(inn)
        normalized.append(inn)
    if not normalized:
        raise ValueError("At least one valid INN is required")
    return normalized


def format_delay(value: float) -> str:
    if float(value).is_integer():
        return str(int(value))
    return f"{value:.3f}".rstrip("0").rstrip(".")


def percentile(values: list[float], ratio: float) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    if len(ordered) == 1:
        return round(ordered[0], 3)
    if ratio <= 0:
        return round(ordered[0], 3)
    if ratio >= 1:
        return round(ordered[-1], 3)
    index = (len(ordered) - 1) * ratio
    lower_index = int(index)
    upper_index = min(lower_index + 1, len(ordered) - 1)
    lower_value = ordered[lower_index]
    upper_value = ordered[upper_index]
    interpolated = lower_value + (upper_value - lower_value) * (index - lower_index)
    return round(interpolated, 3)


def build_attempt_reason(result: SourceResult) -> str:
    if result.errors:
        return normalize_whitespace("; ".join(item for item in result.errors if normalize_whitespace(item)))

    for field_name in IMPORTANT_FIELDS:
        payload = result.availability.get(field_name) or {}
        if normalize_whitespace(str(payload.get("status", ""))) == "blocked":
            reason = normalize_whitespace(str(payload.get("reason", "")))
            if reason:
                return reason

    filtered_notes = [
        normalize_whitespace(note)
        for note in result.notes
        if normalize_whitespace(note) and not normalize_whitespace(note).startswith("checko_search_path=")
    ]
    if filtered_notes:
        return filtered_notes[-1]
    return ""


def blocked_status_for_summary(status: str) -> bool:
    normalized_status = normalize_whitespace(status)
    return normalized_status not in {"success", "rate_limited", "bot_gate"}


def summarize_delay_bucket(delay_seconds: float, attempts: list[AttemptRecord]) -> DelayBucketSummary:
    status_counts = Counter(record.source_status for record in attempts)
    elapsed_values = [record.elapsed_seconds for record in attempts]
    ok_count = status_counts.get("success", 0)
    status_429_count = status_counts.get("rate_limited", 0)
    bot_gate_count = status_counts.get("bot_gate", 0)
    blocked_count = sum(1 for record in attempts if blocked_status_for_summary(record.source_status))
    median_elapsed = round(statistics.median(elapsed_values), 3) if elapsed_values else None
    p95_elapsed = percentile(elapsed_values, 0.95)
    return DelayBucketSummary(
        delay_seconds=delay_seconds,
        total_attempts=len(attempts),
        ok=ok_count,
        blocked=blocked_count,
        status_429=status_429_count,
        bot_gate=bot_gate_count,
        median_elapsed_seconds=median_elapsed,
        p95_elapsed_seconds=p95_elapsed,
        status_counts=dict(sorted(status_counts.items())),
    )


def build_recommendation(summaries: list[DelayBucketSummary]) -> Recommendation:
    if not summaries:
        raise ValueError("At least one delay summary is required")

    ordered = sorted(summaries, key=lambda item: item.delay_seconds)
    strict_safe = [
        item
        for item in ordered
        if item.total_attempts > 0 and item.ok > 0 and item.status_429 == 0 and item.bot_gate == 0 and item.blocked == 0
    ]
    if strict_safe:
        chosen = strict_safe[0]
        return Recommendation(
            delay_seconds=chosen.delay_seconds,
            env_line=f"DELAY_CHECKO_SECONDS={format_delay(chosen.delay_seconds)}",
            verdict=f"safe cadence observed at {format_delay(chosen.delay_seconds)}s",
            rationale=(
                f"{format_delay(chosen.delay_seconds)}s was the smallest tested delay with "
                "zero 429, zero bot-gate, and zero other blocked statuses."
            ),
            confidence="strict_safe",
        )

    soft_safe = sorted(
        [
            item
            for item in summaries
            if item.total_attempts > 0 and item.ok > 0 and item.status_429 == 0 and item.bot_gate == 0
        ],
        key=lambda item: (item.blocked, -item.ok, -item.delay_seconds),
    )
    if soft_safe:
        chosen = soft_safe[0]
        return Recommendation(
            delay_seconds=chosen.delay_seconds,
            env_line=f"DELAY_CHECKO_SECONDS={format_delay(chosen.delay_seconds)}",
            verdict=f"cautious cadence candidate at {format_delay(chosen.delay_seconds)}s",
            rationale=(
                f"{format_delay(chosen.delay_seconds)}s avoided 429/bot-gate, "
                f"but still had {chosen.blocked} other blocked statuses."
            ),
            confidence="soft_safe",
        )

    chosen = max(summaries, key=lambda item: item.delay_seconds)
    return Recommendation(
        delay_seconds=chosen.delay_seconds,
        env_line=f"DELAY_CHECKO_SECONDS={format_delay(chosen.delay_seconds)}",
        verdict="no safe cadence observed in tested grid",
        rationale=(
            f"Even the most conservative tested delay {format_delay(chosen.delay_seconds)}s "
            "still produced rate-limit, bot-gate, or other blocked outcomes."
        ),
        confidence="unsafe_grid",
    )


def build_logger(log_path: Path) -> logging.Logger:
    logger_name = f"checko_calibration_{int(time.time() * 1000)}"
    logger = logging.getLogger(logger_name)
    logger.setLevel(logging.INFO)
    logger.handlers.clear()
    logger.propagate = False
    ensure_dir(log_path.parent)
    file_handler = logging.FileHandler(log_path, encoding="utf-8")
    file_handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
    logger.addHandler(file_handler)
    return logger


def create_client(
    *,
    logger: logging.Logger,
    progress_store: CalibrationProgressStore,
    delay_seconds: float,
    request_timeout: int,
    cooldown_429_seconds: int,
    cooldown_bot_seconds: int,
    proxy_pool: ProxyPool,
) -> TracingRateLimitedHttpClient:
    min_delay_by_host = {host: delay_seconds for host in CHECKO_HOSTS}
    return TracingRateLimitedHttpClient(
        logger=logger,
        progress_store=progress_store,
        min_delay_by_host=min_delay_by_host,
        request_timeout=request_timeout,
        cooldown_on_429=cooldown_429_seconds,
        cooldown_on_bot=cooldown_bot_seconds,
        proxy_pool=proxy_pool,
        list_org_session_file=None,
    )


def run_single_attempt(
    *,
    inn: str,
    attempt_no: int,
    delay_seconds: float,
    request_timeout: int,
    cooldown_429_seconds: int,
    cooldown_bot_seconds: int,
    proxy_pool: ProxyPool,
    logger: logging.Logger,
) -> AttemptRecord:
    progress_store = CalibrationProgressStore()
    client = create_client(
        logger=logger,
        progress_store=progress_store,
        delay_seconds=delay_seconds,
        request_timeout=request_timeout,
        cooldown_429_seconds=cooldown_429_seconds,
        cooldown_bot_seconds=cooldown_bot_seconds,
        proxy_pool=proxy_pool,
    )
    source = CheckoSource(client)
    row = RowInput(row_index=attempt_no, inn=inn, company_name="")

    started_at = time.time()
    result = source.search(row)
    elapsed_seconds = round(time.time() - started_at, 3)

    terminal_trace = client.traces[-1] if client.traces else None
    final_url = normalize_whitespace(result.entity_url or result.listing_url or result.search_url)
    if terminal_trace and terminal_trace.final_url:
        final_url = terminal_trace.final_url

    proxy_mode = "direct"
    proxy_label = ""
    if terminal_trace:
        proxy_mode = terminal_trace.proxy_mode or "direct"
        proxy_label = terminal_trace.proxy_label

    return AttemptRecord(
        inn=inn,
        attempt_no=attempt_no,
        delay_seconds=delay_seconds,
        source_status=normalize_whitespace(result.status),
        http_status=result.http_status or (terminal_trace.http_status if terminal_trace else None),
        elapsed_seconds=elapsed_seconds,
        final_url=final_url,
        proxy_mode=proxy_mode,
        proxy_label=proxy_label,
        error_or_reason=build_attempt_reason(result),
    )


def write_attempts_csv(path: Path, attempts: list[AttemptRecord]) -> None:
    ensure_dir(path.parent)
    with path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=ATTEMPT_FIELDNAMES)
        writer.writeheader()
        for attempt in attempts:
            writer.writerow(asdict(attempt))


def render_report(
    *,
    suite_dir: Path,
    attempts_csv_path: Path,
    summary_json_path: Path,
    proxy_pool: ProxyPool,
    attempts: list[AttemptRecord],
    summaries: list[DelayBucketSummary],
    recommendation: Recommendation,
    continue_after_block: bool,
) -> str:
    proxy_description = proxy_pool.describe()
    lines = [
        "Checko calibration report",
        "",
        f"suite_dir: {suite_dir}",
        f"attempts_csv: {attempts_csv_path}",
        f"summary_json: {summary_json_path}",
        f"attempts_total: {len(attempts)}",
        f"continue_after_block: {str(continue_after_block).lower()}",
        f"proxy_pool_enabled: {str(bool(proxy_description.get('enabled'))).lower()}",
        f"proxy_pool_count: {proxy_description.get('count', 0)}",
        f"proxy_strategy: {proxy_description.get('strategy', 'unknown')}",
        "",
        "delay_buckets:",
    ]
    for summary in sorted(summaries, key=lambda item: item.delay_seconds):
        lines.append(
            "- "
            f"delay={format_delay(summary.delay_seconds)}s | total={summary.total_attempts} | "
            f"ok={summary.ok} | blocked={summary.blocked} | 429={summary.status_429} | "
            f"bot_gate={summary.bot_gate} | median={summary.median_elapsed_seconds} | "
            f"p95={summary.p95_elapsed_seconds}"
        )
    lines.extend(
        [
            "",
            "recommendation:",
            f"- {recommendation.env_line}",
            f"- verdict: {recommendation.verdict}",
            f"- rationale: {recommendation.rationale}",
            f"- confidence: {recommendation.confidence}",
        ]
    )
    return "\n".join(lines) + "\n"


def build_summary_payload(
    *,
    suite_dir: Path,
    attempts_csv_path: Path,
    attempts: list[AttemptRecord],
    summaries: list[DelayBucketSummary],
    recommendation: Recommendation,
    proxy_pool: ProxyPool,
    args: argparse.Namespace,
) -> dict[str, Any]:
    return {
        "generated_at": datetime.now(tz=timezone.utc).replace(microsecond=0).isoformat(),
        "suite_dir": str(suite_dir),
        "attempts_csv": str(attempts_csv_path),
        "inns": list(dict.fromkeys(attempt.inn for attempt in attempts)),
        "delays": [summary.delay_seconds for summary in summaries],
        "attempts_per_delay": int(args.attempts_per_delay),
        "request_timeout": int(args.request_timeout),
        "cooldown_429_seconds": int(args.cooldown_429_seconds),
        "cooldown_bot_seconds": int(args.cooldown_bot_seconds),
        "continue_after_block": bool(args.continue_after_block),
        "proxy_pool": proxy_pool.describe(),
        "attempts": [asdict(attempt) for attempt in attempts],
        "delay_buckets": [
            {
                "delay_seconds": summary.delay_seconds,
                "total_attempts": summary.total_attempts,
                "ok": summary.ok,
                "blocked": summary.blocked,
                "429": summary.status_429,
                "bot_gate": summary.bot_gate,
                "median_elapsed_seconds": summary.median_elapsed_seconds,
                "p95_elapsed_seconds": summary.p95_elapsed_seconds,
                "status_counts": summary.status_counts,
            }
            for summary in summaries
        ],
        "recommendation": asdict(recommendation),
    }


def maybe_override_from_env(args: argparse.Namespace) -> None:
    if args.request_timeout == 20 and normalize_whitespace(os.getenv("REQUEST_TIMEOUT_SECONDS", "")):
        args.request_timeout = int(os.getenv("REQUEST_TIMEOUT_SECONDS", "20"))
    if args.cooldown_429_seconds == 120 and normalize_whitespace(os.getenv("COOLDOWN_429_SECONDS", "")):
        args.cooldown_429_seconds = int(os.getenv("COOLDOWN_429_SECONDS", "120"))
    if args.cooldown_bot_seconds == 180 and normalize_whitespace(os.getenv("COOLDOWN_BOT_SECONDS", "")):
        args.cooldown_bot_seconds = int(os.getenv("COOLDOWN_BOT_SECONDS", "180"))


def main(argv: Sequence[str] | None = None) -> int:
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    if hasattr(sys.stderr, "reconfigure"):
        sys.stderr.reconfigure(encoding="utf-8", errors="replace")

    args = parse_args(argv)
    env_file = Path(args.env_file).expanduser()
    if args.env_file and env_file.exists():
        load_env_file(env_file)
    maybe_override_from_env(args)

    inns = normalize_inn_list(list(args.inn))
    delays = parse_delay_values(args.delays)
    if args.attempts_per_delay < 1:
        raise SystemExit("--attempts-per-delay must be >= 1")

    proxy_pool = ProxyPool(
        os.getenv("PARSER_PROXIES", ""),
        proxy_file=os.getenv("PARSER_PROXIES_FILE", "").strip() or None,
        strategy=args.proxy_strategy or None,
    )

    suite_dir = Path(args.output_dir).expanduser().resolve() / time.strftime("%Y%m%d_%H%M%S")
    ensure_dir(suite_dir)
    logger = build_logger(suite_dir / "run.log")
    logger.info(
        "Start Checko calibration: inns=%s delays=%s attempts_per_delay=%s proxy_enabled=%s",
        ",".join(inns),
        ",".join(format_delay(item) for item in delays),
        args.attempts_per_delay,
        bool(proxy_pool.describe().get("enabled")),
    )

    attempts: list[AttemptRecord] = []
    summaries: list[DelayBucketSummary] = []

    for delay_seconds in delays:
        bucket_attempts: list[AttemptRecord] = []
        should_stop_bucket = False

        for attempt_no in range(1, args.attempts_per_delay + 1):
            for inn in inns:
                if bucket_attempts:
                    time.sleep(delay_seconds)
                logger.info("delay=%ss attempt=%s inn=%s", format_delay(delay_seconds), attempt_no, inn)
                record = run_single_attempt(
                    inn=inn,
                    attempt_no=attempt_no,
                    delay_seconds=delay_seconds,
                    request_timeout=args.request_timeout,
                    cooldown_429_seconds=args.cooldown_429_seconds,
                    cooldown_bot_seconds=args.cooldown_bot_seconds,
                    proxy_pool=proxy_pool,
                    logger=logger,
                )
                bucket_attempts.append(record)
                attempts.append(record)
                logger.info(
                    "  status=%s http=%s elapsed=%.3fs proxy=%s/%s reason=%s",
                    record.source_status,
                    record.http_status,
                    record.elapsed_seconds,
                    record.proxy_mode,
                    record.proxy_label or "-",
                    record.error_or_reason or "-",
                )
                if not args.continue_after_block and record.source_status in {"rate_limited", "bot_gate", "blocked"}:
                    should_stop_bucket = True
                    logger.warning(
                        "  stopping delay bucket after terminal status=%s inn=%s delay=%ss",
                        record.source_status,
                        inn,
                        format_delay(delay_seconds),
                    )
                    break
            if should_stop_bucket:
                break

        summaries.append(summarize_delay_bucket(delay_seconds, bucket_attempts))

    recommendation = build_recommendation(summaries)
    attempts_csv_path = suite_dir / "attempts.csv"
    summary_json_path = suite_dir / "summary.json"
    report_path = suite_dir / "report.txt"
    config_path = suite_dir / "config.json"

    write_attempts_csv(attempts_csv_path, attempts)
    summary_payload = build_summary_payload(
        suite_dir=suite_dir,
        attempts_csv_path=attempts_csv_path,
        attempts=attempts,
        summaries=summaries,
        recommendation=recommendation,
        proxy_pool=proxy_pool,
        args=args,
    )
    atomic_write_json(summary_json_path, summary_payload)
    atomic_write_json(
        config_path,
        {
            "env_file": str(env_file),
            "inns": inns,
            "delays": delays,
            "attempts_per_delay": args.attempts_per_delay,
            "request_timeout": args.request_timeout,
            "cooldown_429_seconds": args.cooldown_429_seconds,
            "cooldown_bot_seconds": args.cooldown_bot_seconds,
            "continue_after_block": args.continue_after_block,
            "proxy_strategy": args.proxy_strategy or proxy_pool.describe().get("strategy", ""),
        },
    )
    atomic_write_text(
        report_path,
        render_report(
            suite_dir=suite_dir,
            attempts_csv_path=attempts_csv_path,
            summary_json_path=summary_json_path,
            proxy_pool=proxy_pool,
            attempts=attempts,
            summaries=summaries,
            recommendation=recommendation,
            continue_after_block=args.continue_after_block,
        ),
    )

    print(f"ATTEMPTS_CSV {attempts_csv_path}")
    print(f"SUMMARY_JSON {summary_json_path}")
    print(f"REPORT_TXT {report_path}")
    for summary in sorted(summaries, key=lambda item: item.delay_seconds):
        print(
            "DELAY_BUCKET "
            f"delay={format_delay(summary.delay_seconds)} "
            f"ok={summary.ok} blocked={summary.blocked} "
            f"429={summary.status_429} bot_gate={summary.bot_gate} "
            f"median={summary.median_elapsed_seconds} p95={summary.p95_elapsed_seconds}"
        )
    print("RECOMMENDATION")
    print(recommendation.env_line)
    print(f"verdict: {recommendation.verdict}")
    print(f"rationale: {recommendation.rationale}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

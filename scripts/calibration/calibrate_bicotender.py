from __future__ import annotations

import argparse
import csv
import hashlib
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
from typing import Any, Iterable, Mapping, Sequence
from urllib.parse import urlparse

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from app.runtime import ProxyPool, ProxySelection
from app.runtime.files import atomic_write_json, atomic_write_text, ensure_dir, load_env_file
from app.sources.bicotender import (
    BICOTENDER_PUBLIC_ACCESS_BLOCKED,
    BICOTENDER_PUBLIC_ACCESS_REVIEW,
    BICOTENDER_PUBLIC_ACCESS_USABLE,
    BicotenderFetchResponse,
    BicotenderKeywordBatch,
    BicotenderListEvidence,
    BicotenderPublicAccessAssessment,
    BicotenderSearchQuery,
    assess_bicotender_public_access,
    build_bicotender_query_plan,
    classify_bicotender_signal,
    load_keyword_batches_from_json,
    parse_bicotender_result_list,
)
from company_enrichment_core import RateLimitedHttpClient, RequestOutcome, is_valid_russian_inn, normalize_inn, normalize_whitespace


BICOTENDER_HOSTS = ("bicotender.ru", "www.bicotender.ru")
DEFAULT_OUTPUT_DIR = REPO_ROOT / "runtime_local" / "calibration_runs" / "bicotender"
DEFAULT_FALLBACK_PROXY_FILE = REPO_ROOT / "runtime_local" / "data" / "proxies" / "proxy6_pool.json"
DEFAULT_KEYWORD_BATCH_FILE = REPO_ROOT / "config" / "bicotender_keyword_batches.example.json"
SOURCE_NAME = "bicotender"
REQUEST_STATUS_OK_STATIC_MARKER = "ok_static_marker"
REQUEST_STATUS_OK_ZERO_RESULTS_STATIC_MARKER = "ok_zero_results_static_marker"
REQUEST_STATUS_AMBIGUOUS_STATIC_MARKER = "ambiguous_no_usable_rows_static_marker"
AMBIGUOUS_NO_ROW_STATIC_MARKER_REASON = "ambiguous_no_usable_public_rows_with_static_access_marker"
ZERO_RESULT_STATIC_MARKER_REASONS = {
    "public_list_query_applied_zero_results",
    "static_access_marker_present_but_query_applied_zero_results",
}
ATTEMPT_FIELDNAMES = [
    "timestamp",
    "delay_seconds",
    "attempt_no",
    "request_no",
    "inn",
    "query_kind",
    "batch_index",
    "search_url",
    "final_url",
    "request_status",
    "http_status",
    "elapsed_seconds",
    "query_applied",
    "total_count",
    "visible_count",
    "parsed_item_count",
    "access_status",
    "access_reason",
    "classification_status",
    "classification_reason",
    "captcha_marker",
    "login_marker",
    "hard_challenge",
    "static_marker",
    "proxy_mode",
    "proxy_label_or_id",
    "error_or_reason",
]
HARD_REQUEST_STATUSES = {"rate_limited", "bot_gate", "http_403", "http_429"}


@dataclass(frozen=True)
class CalibrationProgressStore:
    events: list[dict[str, Any]]

    def append_event(self, payload: dict[str, Any]) -> None:
        self.events.append(dict(payload))


@dataclass(frozen=True)
class TraceRecord:
    requested_url: str
    request_status: str
    http_status: int | None
    final_url: str
    elapsed_seconds: float
    proxy_mode: str
    proxy_label_or_id: str
    error: str


@dataclass(frozen=True)
class FetchResult:
    response: BicotenderFetchResponse
    trace: TraceRecord
    proxy_selection: ProxySelection


@dataclass(frozen=True)
class AttemptRecord:
    timestamp: str
    delay_seconds: float
    attempt_no: int
    request_no: int
    inn: str
    query_kind: str
    batch_index: int | None
    search_url: str
    final_url: str
    request_status: str
    http_status: int | None
    elapsed_seconds: float
    query_applied: bool
    total_count: int | None
    visible_count: int
    parsed_item_count: int
    access_status: str
    access_reason: str
    classification_status: str
    classification_reason: str
    captcha_marker: bool
    login_marker: bool
    hard_challenge: bool
    static_marker: bool
    proxy_mode: str
    proxy_label_or_id: str
    error_or_reason: str


@dataclass(frozen=True)
class DelayBucketSummary:
    delay_seconds: float
    total_attempts: int
    clean: int
    zero_result: int
    blocked: int
    status_429: int
    status_403: int
    bot_gate: int
    ambiguous_no_row: int
    static_marker: int
    request_error: int
    median_elapsed_seconds: float | None
    p95_elapsed_seconds: float | None
    status_counts: dict[str, int]


@dataclass(frozen=True)
class Recommendation:
    delay_seconds: float
    env_lines: list[str]
    verdict: str
    rationale: str
    confidence: str
    exact_ban_threshold_known: bool


class TracingRateLimitedHttpClient(RateLimitedHttpClient):
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self.traces: list[TraceRecord] = []

    def request(
        self,
        url: str,
        *,
        source: str,
        allow_redirects: bool = True,
        timeout: int | None = None,
        proxy_selection: ProxySelection | None = None,
    ) -> RequestOutcome:
        outcome = super().request(
            url,
            source=source,
            allow_redirects=allow_redirects,
            timeout=timeout,
            proxy_selection=proxy_selection,
        )
        response = outcome.response
        self.traces.append(
            TraceRecord(
                requested_url=url,
                request_status=normalize_whitespace(outcome.status) or "unknown",
                http_status=response.status_code if response is not None else None,
                final_url=response.url if response is not None else "",
                elapsed_seconds=round(float(outcome.elapsed_seconds or 0.0), 3),
                proxy_mode=normalize_whitespace(outcome.proxy_mode) or "direct",
                proxy_label_or_id=redact_proxy_label(outcome.proxy_id or outcome.proxy_label),
                error=normalize_whitespace(outcome.error),
            )
        )
        return outcome


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Bounded strict-proxy Bicotender public-search calibration."
    )
    parser.add_argument("--env-file", default=".env", help="Optional .env file loaded before proxy settings.")
    parser.add_argument(
        "--inn",
        action="append",
        default=None,
        help="Target company INN. Repeat for more companies while staying under --max-requests.",
    )
    parser.add_argument("--delays", default="12,8,6,4", help="Comma-separated delay grid in seconds.")
    parser.add_argument("--attempts-per-delay", type=int, default=1, help="Sequential passes per delay bucket.")
    parser.add_argument("--max-requests", type=int, default=24, help="Hard cap for live public-search GETs.")
    parser.add_argument("--request-timeout", type=int, default=20, help="Timeout for one HTTP request in seconds.")
    parser.add_argument(
        "--output-dir",
        default=str(DEFAULT_OUTPUT_DIR),
        help="Base directory where the timestamped calibration suite directory is written.",
    )
    parser.add_argument(
        "--keyword-batches",
        default="",
        help="Accepted keyword batch JSON. Defaults to config/bicotender_keyword_batches.example.json.",
    )
    parser.add_argument(
        "--cooldown-429-seconds",
        type=int,
        default=None,
        help="Host cooldown used by the calibration client after HTTP 429.",
    )
    parser.add_argument(
        "--cooldown-bot-seconds",
        type=int,
        default=None,
        help="Host cooldown used by the calibration client after bot/captcha gate detection.",
    )
    parser.add_argument(
        "--proxy-strategy",
        choices=("round_robin", "sticky_by_host"),
        default=None,
        help="Optional proxy strategy override for this calibration run.",
    )
    parser.add_argument(
        "--continue-after-block",
        action="store_true",
        help="Continue after a hard block. Off by default to avoid brute-forcing the source.",
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


def normalize_inn_list(raw_values: Sequence[str] | None) -> list[str]:
    source_values = list(raw_values or ["5022055500"])
    normalized: list[str] = []
    seen: set[str] = set()
    for raw_value in source_values:
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
    index = (len(ordered) - 1) * ratio
    lower_index = int(index)
    upper_index = min(lower_index + 1, len(ordered) - 1)
    lower_value = ordered[lower_index]
    upper_value = ordered[upper_index]
    interpolated = lower_value + (upper_value - lower_value) * (index - lower_index)
    return round(interpolated, 3)


def int_from_env(name: str, default: int) -> int:
    raw_value = normalize_whitespace(os.getenv(name, ""))
    if not raw_value:
        return default
    try:
        return int(raw_value)
    except ValueError:
        return default


def resolve_keyword_batches_path(raw_path: str) -> Path:
    if normalize_whitespace(raw_path):
        path = Path(raw_path).expanduser()
        if not path.exists():
            raise FileNotFoundError(f"Keyword batches file not found: {path}")
        return path
    if DEFAULT_KEYWORD_BATCH_FILE.exists():
        return DEFAULT_KEYWORD_BATCH_FILE
    raise FileNotFoundError(
        f"Default Bicotender keyword batch file was not found: {DEFAULT_KEYWORD_BATCH_FILE}. "
        "Pass --keyword-batches explicitly."
    )


def proxy_file_from_env_or_fallback(fallback_file: Path = DEFAULT_FALLBACK_PROXY_FILE) -> str:
    configured_file = normalize_whitespace(os.getenv("PARSER_PROXIES_FILE", ""))
    if configured_file:
        return configured_file
    if normalize_whitespace(os.getenv("PARSER_PROXIES", "")):
        return ""
    if fallback_file.exists():
        return str(fallback_file)
    return ""


def create_proxy_pool(*, strategy: str | None = None, fallback_file: Path = DEFAULT_FALLBACK_PROXY_FILE) -> ProxyPool:
    return ProxyPool(
        os.getenv("PARSER_PROXIES", ""),
        proxy_file=proxy_file_from_env_or_fallback(fallback_file),
        strategy=strategy,
    )


def safe_proxy_summary(proxy_pool: ProxyPool) -> dict[str, Any]:
    description = dict(proxy_pool.describe())
    items = []
    for item in description.get("items", []) or []:
        if not isinstance(item, Mapping):
            continue
        label_source = normalize_whitespace(str(item.get("proxy_id") or item.get("label") or ""))
        items.append(
            {
                "proxy_label_or_id": redact_proxy_label(label_source),
                "country": normalize_whitespace(str(item.get("country", ""))),
                "failures": int(item.get("failures", 0) or 0),
                "cooldown_active": float(item.get("cooldown_until", 0.0) or 0.0) > time.time(),
                "source_kind": safe_source_kind(str(item.get("source", ""))),
            }
        )
    description["items"] = items
    return description


def safe_source_kind(source: str) -> str:
    normalized = normalize_whitespace(source)
    if normalized.startswith("env:"):
        return normalized
    if normalized.startswith("file:"):
        return "file"
    return normalized or "unknown"


def redact_proxy_label(value: str) -> str:
    normalized = normalize_whitespace(value)
    if not normalized:
        return ""
    digest = hashlib.sha256(normalized.encode("utf-8")).hexdigest()[:12]
    return f"proxy-{digest}"


def strict_proxy_preflight(proxy_pool: ProxyPool) -> dict[str, Any]:
    description = safe_proxy_summary(proxy_pool)
    usable_count = int(proxy_pool.usable_count())
    status = "ok" if bool(description.get("enabled")) and usable_count > 0 else "blocked"
    reason = "usable_proxy_available" if status == "ok" else "no_usable_proxy_pool_for_strict_proxy_calibration"
    return {
        "status": status,
        "reason": reason,
        "enabled": bool(description.get("enabled")),
        "count": int(description.get("count", 0) or 0),
        "usable_count": usable_count,
        "strategy": description.get("strategy", "unknown"),
        "proxy_summary": description,
    }


def build_logger(log_path: Path) -> logging.Logger:
    logger_name = f"bicotender_calibration_{int(time.time() * 1000)}"
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
    return TracingRateLimitedHttpClient(
        logger=logger,
        progress_store=progress_store,
        min_delay_by_host={host: delay_seconds for host in BICOTENDER_HOSTS},
        request_timeout=request_timeout,
        cooldown_on_429=cooldown_429_seconds,
        cooldown_on_bot=cooldown_bot_seconds,
        proxy_pool=proxy_pool,
        list_org_session_file=None,
    )


def fetch_public_search(
    *,
    client: TracingRateLimitedHttpClient,
    proxy_pool: ProxyPool,
    query: BicotenderSearchQuery,
    request_timeout: int,
) -> FetchResult:
    search_url = query.to_url()
    host = urlparse(search_url).netloc.lower()
    selection = proxy_pool.select(host)
    if not selection.via_proxy or not selection.url:
        trace = TraceRecord(
            requested_url=search_url,
            request_status="blocked_no_proxy",
            http_status=None,
            final_url="",
            elapsed_seconds=0.0,
            proxy_mode="blocked_no_proxy",
            proxy_label_or_id="",
            error="proxy selection unavailable before outbound request",
        )
        return FetchResult(
            response=BicotenderFetchResponse(html="", http_status=None, final_url=search_url, error=trace.error),
            trace=trace,
            proxy_selection=selection,
        )

    selected_label = redact_proxy_label(selection.proxy_label_or_id or selection.label)
    outcome = client.request(
        search_url,
        source=SOURCE_NAME,
        timeout=request_timeout,
        allow_redirects=True,
        proxy_selection=selection,
    )
    trace = client.traces[-1] if client.traces else TraceRecord(
        requested_url=search_url,
        request_status=normalize_whitespace(outcome.status) or "unknown",
        http_status=outcome.response.status_code if outcome.response is not None else None,
        final_url=outcome.response.url if outcome.response is not None else "",
        elapsed_seconds=round(float(outcome.elapsed_seconds or 0.0), 3),
        proxy_mode=normalize_whitespace(outcome.proxy_mode) or "proxy",
        proxy_label_or_id=selected_label,
        error=normalize_whitespace(outcome.error),
    )
    if not trace.proxy_label_or_id:
        trace = TraceRecord(
            requested_url=trace.requested_url,
            request_status=trace.request_status,
            http_status=trace.http_status,
            final_url=trace.final_url,
            elapsed_seconds=trace.elapsed_seconds,
            proxy_mode=trace.proxy_mode,
            proxy_label_or_id=selected_label,
            error=trace.error,
        )
    response = outcome.response
    return FetchResult(
        response=BicotenderFetchResponse(
            html=response.text if response is not None else "",
            http_status=response.status_code if response is not None else None,
            final_url=response.url if response is not None else search_url,
            error=normalize_whitespace(outcome.error),
        ),
        trace=trace,
        proxy_selection=selection,
    )


def planned_queries_for_inn(
    *,
    inn: str,
    keyword_batches: Sequence[BicotenderKeywordBatch],
) -> list[tuple[str, int | None, BicotenderSearchQuery, tuple[str, ...], int | None]]:
    plan = build_bicotender_query_plan(
        inn=inn,
        positive_keywords=tuple(keyword_batches),
        force_keyword_batches=True,
    )
    queries: list[tuple[str, int | None, BicotenderSearchQuery, tuple[str, ...], int | None]] = [
        (plan.preflight.kind, None, plan.preflight.query, (), None)
    ]
    for planned in plan.keyword_batches:
        queries.append(
            (
                planned.kind,
                planned.batch_index,
                planned.query,
                planned.terms,
                planned.batch_char_count,
            )
        )
    return queries


def attempt_from_response(
    *,
    timestamp: str,
    delay_seconds: float,
    attempt_no: int,
    request_no: int,
    inn: str,
    query_kind: str,
    batch_index: int | None,
    batch_char_count: int | None,
    query: BicotenderSearchQuery,
    positive_terms: Iterable[str],
    response: BicotenderFetchResponse,
    trace: TraceRecord,
) -> AttemptRecord:
    evidence = parse_bicotender_result_list(
        response.html,
        expected_query=query,
        http_status=response.http_status,
        final_url=response.final_url,
        query_kind=query_kind,
        batch_index=batch_index,
        batch_char_count=batch_char_count,
        positive_terms=positive_terms,
    )
    access = assess_bicotender_public_access(response.html, evidence, final_url=response.final_url)
    false_static_marker_bot_gate = is_false_static_marker_bot_gate(
        response=response,
        trace=trace,
        evidence=evidence,
        access=access,
    )
    zero_result_static_marker_bot_gate = is_zero_result_static_marker_bot_gate(
        response=response,
        trace=trace,
        evidence=evidence,
        access=access,
    )
    ambiguous_static_marker_no_rows = is_ambiguous_static_marker_no_rows(
        response=response,
        trace=trace,
        evidence=evidence,
        access=access,
    )
    softened_static_marker = (
        false_static_marker_bot_gate
        or zero_result_static_marker_bot_gate
        or ambiguous_static_marker_no_rows
    )
    if false_static_marker_bot_gate:
        request_status = REQUEST_STATUS_OK_STATIC_MARKER
    elif zero_result_static_marker_bot_gate:
        request_status = REQUEST_STATUS_OK_ZERO_RESULTS_STATIC_MARKER
    elif ambiguous_static_marker_no_rows:
        request_status = REQUEST_STATUS_AMBIGUOUS_STATIC_MARKER
    else:
        request_status = trace.request_status
    if response.error and not softened_static_marker:
        evidence = evidence_with_errors(evidence, (response.error,))
    classification = classify_bicotender_signal(evidence, positive_terms=positive_terms, batch_index=batch_index)
    return AttemptRecord(
        timestamp=timestamp,
        delay_seconds=delay_seconds,
        attempt_no=attempt_no,
        request_no=request_no,
        inn=inn,
        query_kind=query_kind,
        batch_index=batch_index,
        search_url=query.to_url(),
        final_url=response.final_url or trace.final_url,
        request_status=request_status,
        http_status=response.http_status,
        elapsed_seconds=trace.elapsed_seconds,
        query_applied=evidence.query_applied,
        total_count=evidence.total_count,
        visible_count=evidence.visible_count,
        parsed_item_count=evidence.parsed_item_count,
        access_status=access.status,
        access_reason=access.reason,
        classification_status=classification.status,
        classification_reason=classification.reason,
        captcha_marker=access.captcha_marker_present,
        login_marker=access.login_marker_present,
        hard_challenge=is_hard_challenge(access, trace),
        static_marker=is_static_marker(access),
        proxy_mode=trace.proxy_mode,
        proxy_label_or_id=trace.proxy_label_or_id,
        error_or_reason=attempt_reason(
            response=response,
            access=access,
            trace=trace,
            evidence=evidence,
            suppress_transport_error=softened_static_marker,
        ),
    )


def evidence_with_errors(evidence: BicotenderListEvidence, errors: Sequence[str]) -> BicotenderListEvidence:
    from dataclasses import replace

    return replace(evidence, errors=tuple((*evidence.errors, *tuple(item for item in errors if item))))


def is_static_marker(access: BicotenderPublicAccessAssessment) -> bool:
    if not (access.captcha_marker_present or access.login_marker_present):
        return False
    return access.status != BICOTENDER_PUBLIC_ACCESS_BLOCKED


def is_hard_challenge(access: BicotenderPublicAccessAssessment, trace: TraceRecord) -> bool:
    return access.status == BICOTENDER_PUBLIC_ACCESS_BLOCKED


def is_false_static_marker_bot_gate(
    *,
    response: BicotenderFetchResponse,
    trace: TraceRecord,
    evidence: BicotenderListEvidence,
    access: BicotenderPublicAccessAssessment,
) -> bool:
    return (
        trace.request_status == "bot_gate"
        and response.http_status == 200
        and evidence.query_applied
        and evidence.visible_count > 0
        and access.status == BICOTENDER_PUBLIC_ACCESS_USABLE
        and access.reason == "static_access_marker_present_but_public_rows_usable"
    )


def is_ambiguous_static_marker_no_rows(
    *,
    response: BicotenderFetchResponse,
    trace: TraceRecord,
    evidence: BicotenderListEvidence,
    access: BicotenderPublicAccessAssessment,
) -> bool:
    return (
        response.http_status == 200
        and evidence.query_applied
        and (evidence.visible_count <= 0 or evidence.parsed_item_count <= 0)
        and (access.captcha_marker_present or access.login_marker_present)
        and access.status == BICOTENDER_PUBLIC_ACCESS_REVIEW
        and access.reason == AMBIGUOUS_NO_ROW_STATIC_MARKER_REASON
        and trace.request_status not in {"rate_limited", "http_403", "http_429"}
    )


def is_zero_result_static_marker_bot_gate(
    *,
    response: BicotenderFetchResponse,
    trace: TraceRecord,
    evidence: BicotenderListEvidence,
    access: BicotenderPublicAccessAssessment,
) -> bool:
    return (
        trace.request_status == "bot_gate"
        and response.http_status == 200
        and evidence.query_applied
        and evidence.visible_count == 0
        and evidence.parsed_item_count == 0
        and access.status == BICOTENDER_PUBLIC_ACCESS_USABLE
        and access.reason in ZERO_RESULT_STATIC_MARKER_REASONS
    )


def attempt_reason(
    *,
    response: BicotenderFetchResponse,
    access: BicotenderPublicAccessAssessment,
    trace: TraceRecord,
    evidence: BicotenderListEvidence,
    suppress_transport_error: bool = False,
) -> str:
    parts = [
        "" if suppress_transport_error else normalize_whitespace(trace.error),
        "" if suppress_transport_error else normalize_whitespace(response.error),
        normalize_whitespace(access.reason),
        "; ".join(evidence.errors),
    ]
    for part in parts:
        if part:
            return part
    return ""


def record_is_hard_block(record: AttemptRecord) -> bool:
    if record.request_status in HARD_REQUEST_STATUSES:
        return True
    if record.http_status in (403, 429):
        return True
    if record.access_status == BICOTENDER_PUBLIC_ACCESS_BLOCKED:
        return True
    return False


def record_is_clean(record: AttemptRecord) -> bool:
    if record_is_hard_block(record):
        return False
    if record.request_status not in {
        "ok",
        REQUEST_STATUS_OK_STATIC_MARKER,
        REQUEST_STATUS_OK_ZERO_RESULTS_STATIC_MARKER,
    }:
        return False
    return record.access_status in {
        BICOTENDER_PUBLIC_ACCESS_USABLE,
        BICOTENDER_PUBLIC_ACCESS_REVIEW,
    }


def summarize_delay_bucket(delay_seconds: float, attempts: Sequence[AttemptRecord]) -> DelayBucketSummary:
    status_counts = Counter(record.request_status for record in attempts)
    elapsed_values = [record.elapsed_seconds for record in attempts if record.elapsed_seconds is not None]
    return DelayBucketSummary(
        delay_seconds=delay_seconds,
        total_attempts=len(attempts),
        clean=sum(1 for record in attempts if record_is_clean(record)),
        zero_result=sum(1 for record in attempts if record.query_applied and record.visible_count == 0),
        blocked=sum(1 for record in attempts if record_is_hard_block(record)),
        status_429=sum(1 for record in attempts if record.http_status == 429 or record.request_status == "rate_limited"),
        status_403=sum(1 for record in attempts if record.http_status == 403 or record.request_status == "http_403"),
        bot_gate=sum(1 for record in attempts if record.request_status == "bot_gate"),
        ambiguous_no_row=sum(
            1 for record in attempts if record.request_status == REQUEST_STATUS_AMBIGUOUS_STATIC_MARKER
        ),
        static_marker=sum(1 for record in attempts if record.static_marker),
        request_error=sum(1 for record in attempts if record.request_status in {"request_error", "timeout"}),
        median_elapsed_seconds=round(statistics.median(elapsed_values), 3) if elapsed_values else None,
        p95_elapsed_seconds=percentile(elapsed_values, 0.95),
        status_counts=dict(sorted(status_counts.items())),
    )


def build_recommendation(
    summaries: Sequence[DelayBucketSummary],
    *,
    cooldown_429_seconds: int,
    proxy_ban_cooldown_seconds: float,
) -> Recommendation:
    materialized = [summary for summary in summaries if summary.total_attempts > 0]
    if not materialized:
        return Recommendation(
            delay_seconds=12.0,
            env_lines=[
                "DELAY_BICOTENDER_SECONDS=12",
                f"PARSER_PROXY_BAN_COOLDOWN_SECONDS={int(proxy_ban_cooldown_seconds)}",
                f"COOLDOWN_429_SECONDS={cooldown_429_seconds}",
            ],
            verdict="blocked before live calibration",
            rationale="No Bicotender public-search request was sent, so delay remains a conservative placeholder.",
            confidence="blocked",
            exact_ban_threshold_known=False,
        )

    hard_blocked = [summary for summary in materialized if summary.blocked or summary.status_429 or summary.status_403 or summary.bot_gate]
    strict_safe = [
        summary
        for summary in sorted(materialized, key=lambda item: item.delay_seconds)
        if summary.total_attempts > 0
        and summary.clean == summary.total_attempts
        and summary.blocked == 0
        and summary.request_error == 0
    ]
    if hard_blocked:
        blocked_min_delay = min(item.delay_seconds for item in hard_blocked)
        safer_candidates = [item for item in strict_safe if item.delay_seconds > blocked_min_delay]
        chosen = (
            min(safer_candidates, key=lambda item: item.delay_seconds)
            if safer_candidates
            else max(materialized, key=lambda item: item.delay_seconds)
        )
        cooldown_429_recommendation = max(cooldown_429_seconds, 3600)
        proxy_cooldown_recommendation = max(int(proxy_ban_cooldown_seconds), 1800)
        if safer_candidates:
            verdict = f"hard block observed; use nearest clean slower bucket {format_delay(chosen.delay_seconds)}s"
            rationale = (
                "A 429/403/bot-gate/protected stop appeared inside the bounded grid; "
                "the recommendation uses the nearest tested slower clean bucket, but the exact threshold is not known."
            )
            confidence = "bounded_by_hard_block"
        else:
            verdict = f"hard block observed at {format_delay(chosen.delay_seconds)}s; no clean bucket observed"
            rationale = (
                "A 429/403/bot-gate/protected stop appeared before any clean Bicotender bucket was proven; "
                "keep the most conservative tested delay as a placeholder and pause broader integration until follow-up."
            )
            confidence = "unsafe_first_bucket"
        return Recommendation(
            delay_seconds=chosen.delay_seconds,
            env_lines=[
                f"DELAY_BICOTENDER_SECONDS={format_delay(chosen.delay_seconds)}",
                f"PARSER_PROXY_BAN_COOLDOWN_SECONDS={proxy_cooldown_recommendation}",
                f"COOLDOWN_429_SECONDS={cooldown_429_recommendation}",
            ],
            verdict=verdict,
            rationale=rationale,
            confidence=confidence,
            exact_ban_threshold_known=False,
        )

    if strict_safe:
        fastest_clean = strict_safe[0]
        slower_clean = [summary for summary in strict_safe if summary.delay_seconds > fastest_clean.delay_seconds]
        chosen = min(slower_clean, key=lambda item: item.delay_seconds) if slower_clean else fastest_clean
        return Recommendation(
            delay_seconds=chosen.delay_seconds,
            env_lines=[
                f"DELAY_BICOTENDER_SECONDS={format_delay(chosen.delay_seconds)}",
                f"PARSER_PROXY_BAN_COOLDOWN_SECONDS={int(proxy_ban_cooldown_seconds)}",
                f"COOLDOWN_429_SECONDS={cooldown_429_seconds}",
            ],
            verdict=f"no hard block inside cap; conservative delay {format_delay(chosen.delay_seconds)}s",
            rationale=(
                f"Fastest clean observed bucket was {format_delay(fastest_clean.delay_seconds)}s; "
                "recommendation keeps one tested slower bucket when available because the true threshold was not measured."
            ),
            confidence="conservative_clean_grid",
            exact_ban_threshold_known=False,
        )

    chosen = max(materialized, key=lambda item: item.delay_seconds)
    return Recommendation(
        delay_seconds=chosen.delay_seconds,
        env_lines=[
            f"DELAY_BICOTENDER_SECONDS={format_delay(chosen.delay_seconds)}",
            f"PARSER_PROXY_BAN_COOLDOWN_SECONDS={int(proxy_ban_cooldown_seconds)}",
            f"COOLDOWN_429_SECONDS={cooldown_429_seconds}",
        ],
        verdict=f"no strict clean bucket; keep most conservative tested delay {format_delay(chosen.delay_seconds)}s",
        rationale="The grid did not prove a clean bucket; do not broaden Bicotender usage from this run alone.",
        confidence="no_strict_clean_bucket",
        exact_ban_threshold_known=False,
    )


def run_calibration(
    *,
    args: argparse.Namespace,
    proxy_pool: ProxyPool,
    keyword_batches: Sequence[BicotenderKeywordBatch],
    suite_dir: Path,
    logger: logging.Logger,
) -> tuple[list[AttemptRecord], list[DelayBucketSummary], Recommendation, dict[str, Any]]:
    proxy_preflight = strict_proxy_preflight(proxy_pool)
    if proxy_preflight["status"] != "ok":
        recommendation = build_recommendation(
            [],
            cooldown_429_seconds=args.cooldown_429_seconds,
            proxy_ban_cooldown_seconds=float(proxy_pool.describe().get("ban_cooldown_seconds", 300.0)),
        )
        return [], [], recommendation, proxy_preflight

    delays = parse_delay_values(args.delays)
    inns = normalize_inn_list(args.inn)
    max_requests = max(int(args.max_requests), 1)
    attempts_per_delay = max(int(args.attempts_per_delay), 1)
    attempts: list[AttemptRecord] = []
    summaries: list[DelayBucketSummary] = []
    request_no = 0
    stop_all = False

    for delay_seconds in delays:
        bucket_attempts: list[AttemptRecord] = []
        progress_store = CalibrationProgressStore(events=[])
        client = create_client(
            logger=logger,
            progress_store=progress_store,
            delay_seconds=delay_seconds,
            request_timeout=args.request_timeout,
            cooldown_429_seconds=args.cooldown_429_seconds,
            cooldown_bot_seconds=args.cooldown_bot_seconds,
            proxy_pool=proxy_pool,
        )
        for attempt_no in range(1, attempts_per_delay + 1):
            for inn in inns:
                for query_kind, batch_index, query, positive_terms, batch_char_count in planned_queries_for_inn(
                    inn=inn,
                    keyword_batches=keyword_batches,
                ):
                    if request_no >= max_requests:
                        stop_all = True
                        break
                    request_no += 1
                    timestamp = datetime.now(tz=timezone.utc).replace(microsecond=0).isoformat()
                    fetch_result = fetch_public_search(
                        client=client,
                        proxy_pool=proxy_pool,
                        query=query,
                        request_timeout=args.request_timeout,
                    )
                    record = attempt_from_response(
                        timestamp=timestamp,
                        delay_seconds=delay_seconds,
                        attempt_no=attempt_no,
                        request_no=request_no,
                        inn=inn,
                        query_kind=query_kind,
                        batch_index=batch_index,
                        batch_char_count=batch_char_count,
                        query=query,
                        positive_terms=positive_terms,
                        response=fetch_result.response,
                        trace=fetch_result.trace,
                    )
                    if record.request_status == REQUEST_STATUS_OK_STATIC_MARKER:
                        clear_false_static_marker_transport_side_effects(
                            client=client,
                            proxy_pool=proxy_pool,
                            proxy_selection=fetch_result.proxy_selection,
                        )
                    if record.request_status == REQUEST_STATUS_OK_ZERO_RESULTS_STATIC_MARKER:
                        clear_false_static_marker_transport_side_effects(
                            client=client,
                            proxy_pool=proxy_pool,
                            proxy_selection=fetch_result.proxy_selection,
                        )
                    if record.request_status == REQUEST_STATUS_AMBIGUOUS_STATIC_MARKER:
                        clear_false_static_marker_transport_side_effects(
                            client=client,
                            proxy_pool=proxy_pool,
                            proxy_selection=fetch_result.proxy_selection,
                        )
                    attempts.append(record)
                    bucket_attempts.append(record)
                    logger.info(
                        "delay=%ss request=%s inn=%s kind=%s batch=%s status=%s http=%s proxy=%s reason=%s",
                        format_delay(delay_seconds),
                        request_no,
                        inn,
                        query_kind,
                        batch_index if batch_index is not None else "-",
                        record.request_status,
                        record.http_status if record.http_status is not None else "-",
                        record.proxy_label_or_id or "-",
                        record.error_or_reason or "-",
                    )
                    if record.request_status == "blocked_no_proxy":
                        stop_all = True
                        break
                    if record_is_hard_block(record) and not args.continue_after_block:
                        stop_all = True
                        break
                if stop_all:
                    break
            if stop_all:
                break
        summaries.append(summarize_delay_bucket(delay_seconds, bucket_attempts))
        if stop_all:
            break

    recommendation = build_recommendation(
        summaries,
        cooldown_429_seconds=args.cooldown_429_seconds,
        proxy_ban_cooldown_seconds=float(proxy_pool.describe().get("ban_cooldown_seconds", 300.0)),
    )
    return attempts, summaries, recommendation, proxy_preflight


def clear_false_static_marker_transport_side_effects(
    *,
    client: TracingRateLimitedHttpClient,
    proxy_pool: ProxyPool,
    proxy_selection: ProxySelection,
) -> None:
    client.clear_host_cooldown(*BICOTENDER_HOSTS)
    if proxy_selection.via_proxy and proxy_selection.url:
        proxy_pool.mark_ok(proxy_selection.url, source_name=SOURCE_NAME)


def write_attempts_csv(path: Path, attempts: Sequence[AttemptRecord]) -> None:
    ensure_dir(path.parent)
    with path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=ATTEMPT_FIELDNAMES)
        writer.writeheader()
        for attempt in attempts:
            writer.writerow(asdict(attempt))


def build_summary_payload(
    *,
    suite_dir: Path,
    attempts_csv_path: Path,
    report_path: Path,
    config_path: Path,
    attempts: Sequence[AttemptRecord],
    summaries: Sequence[DelayBucketSummary],
    recommendation: Recommendation,
    proxy_preflight: Mapping[str, Any],
    args: argparse.Namespace,
    keyword_batches_path: Path,
) -> dict[str, Any]:
    return {
        "generated_at": datetime.now(tz=timezone.utc).replace(microsecond=0).isoformat(),
        "status": run_status(attempts, proxy_preflight),
        "suite_dir": str(suite_dir),
        "attempts_csv": str(attempts_csv_path),
        "report_path": str(report_path),
        "config_path": str(config_path),
        "keyword_batches_path": str(keyword_batches_path),
        "request_budget": {
            "max_requests": int(args.max_requests),
            "used": len(attempts),
            "remaining": max(int(args.max_requests) - len(attempts), 0),
        },
        "proxy_preflight": proxy_preflight,
        "attempts": [asdict(attempt) for attempt in attempts],
        "delay_buckets": [asdict(summary) for summary in summaries],
        "recommendation": asdict(recommendation),
        "redaction_statement": "Proxy URLs, credentials, provider secrets, and .env contents are not written; proxy labels are hashed.",
    }


def run_status(attempts: Sequence[AttemptRecord], proxy_preflight: Mapping[str, Any]) -> str:
    if proxy_preflight.get("status") != "ok":
        return "blocked"
    if any(record.request_status == "blocked_no_proxy" for record in attempts):
        return "blocked"
    if attempts:
        return "completed"
    return "blocked"


def build_config_payload(
    *,
    args: argparse.Namespace,
    proxy_pool: ProxyPool,
    proxy_preflight: Mapping[str, Any],
    keyword_batches_path: Path,
) -> dict[str, Any]:
    return {
        "env_file_loaded_if_present": str(Path(args.env_file).expanduser()),
        "inns": normalize_inn_list(args.inn),
        "delays": parse_delay_values(args.delays),
        "attempts_per_delay": int(args.attempts_per_delay),
        "max_requests": int(args.max_requests),
        "request_timeout": int(args.request_timeout),
        "cooldown_429_seconds": int(args.cooldown_429_seconds),
        "cooldown_bot_seconds": int(args.cooldown_bot_seconds),
        "continue_after_block": bool(args.continue_after_block),
        "keyword_batches_path": str(keyword_batches_path),
        "proxy_strategy": proxy_preflight.get("strategy", proxy_pool.describe().get("strategy", "unknown")),
        "proxy_summary": proxy_preflight.get("proxy_summary", safe_proxy_summary(proxy_pool)),
        "redaction_note": "Proxy identity fields are hashed. Raw proxy URLs, credentials, provider secrets, and .env values are intentionally omitted.",
        "source_boundaries": [
            "public search/list pages only",
            "INN-only preflight and accepted keyword batch search URLs only",
            "no login, captcha solving, tender details, documents, pagination, hidden endpoints, or direct fallback",
        ],
    }


def render_report(
    *,
    suite_dir: Path,
    attempts_csv_path: Path,
    summary_json_path: Path,
    config_path: Path,
    attempts: Sequence[AttemptRecord],
    summaries: Sequence[DelayBucketSummary],
    recommendation: Recommendation,
    proxy_preflight: Mapping[str, Any],
) -> str:
    lines = [
        "Bicotender strict-proxy calibration report",
        "",
        f"suite_dir: {suite_dir}",
        f"attempts_csv: {attempts_csv_path}",
        f"summary_json: {summary_json_path}",
        f"config_json: {config_path}",
        f"status: {run_status(attempts, proxy_preflight)}",
        f"requests_used: {len(attempts)}",
        f"proxy_preflight: {proxy_preflight.get('status')} ({proxy_preflight.get('reason')})",
        f"proxy_count: {proxy_preflight.get('count', 0)}",
        f"proxy_usable_count: {proxy_preflight.get('usable_count', 0)}",
        "",
        "delay_bucket_summary:",
    ]
    if summaries:
        for summary in sorted(summaries, key=lambda item: item.delay_seconds, reverse=True):
            lines.append(
                "- "
                f"delay={format_delay(summary.delay_seconds)}s | total={summary.total_attempts} | "
                f"clean={summary.clean} | zero_result={summary.zero_result} | "
                f"blocked={summary.blocked} | 429={summary.status_429} | "
                f"403={summary.status_403} | bot_gate={summary.bot_gate} | "
                f"ambiguous_no_row={summary.ambiguous_no_row} | static_marker={summary.static_marker} | "
                f"request_error={summary.request_error} | "
                f"median={summary.median_elapsed_seconds} | p95={summary.p95_elapsed_seconds}"
            )
    else:
        lines.append("- no live request sent")
    lines.extend(
        [
            "",
            "recommendation:",
            f"- verdict: {recommendation.verdict}",
            f"- rationale: {recommendation.rationale}",
            f"- confidence: {recommendation.confidence}",
            f"- exact_ban_threshold_known: {str(recommendation.exact_ban_threshold_known).lower()}",
            "- env_lines:",
        ]
    )
    for line in recommendation.env_lines:
        lines.append(f"  {line}")
    lines.extend(
        [
            "",
            "redaction:",
            "- Proxy URLs, credentials, provider secrets, and .env contents are not written.",
            "- Proxy labels are hashed before CSV/JSON/report/log output.",
            "",
            "boundaries:",
            "- public search/list pages only",
            "- no login, captcha solving, detail pages, document fetches, pagination, hidden endpoints, or direct fallback",
        ]
    )
    return "\n".join(lines) + "\n"


def write_artifacts(
    *,
    suite_dir: Path,
    attempts: Sequence[AttemptRecord],
    summaries: Sequence[DelayBucketSummary],
    recommendation: Recommendation,
    proxy_preflight: Mapping[str, Any],
    args: argparse.Namespace,
    proxy_pool: ProxyPool,
    keyword_batches_path: Path,
) -> dict[str, Path]:
    attempts_csv_path = suite_dir / "attempts.csv"
    summary_json_path = suite_dir / "summary.json"
    report_path = suite_dir / "report.txt"
    config_path = suite_dir / "config.json"
    write_attempts_csv(attempts_csv_path, attempts)
    atomic_write_json(
        config_path,
        build_config_payload(
            args=args,
            proxy_pool=proxy_pool,
            proxy_preflight=proxy_preflight,
            keyword_batches_path=keyword_batches_path,
        ),
    )
    atomic_write_json(
        summary_json_path,
        build_summary_payload(
            suite_dir=suite_dir,
            attempts_csv_path=attempts_csv_path,
            report_path=report_path,
            config_path=config_path,
            attempts=attempts,
            summaries=summaries,
            recommendation=recommendation,
            proxy_preflight=proxy_preflight,
            args=args,
            keyword_batches_path=keyword_batches_path,
        ),
    )
    atomic_write_text(
        report_path,
        render_report(
            suite_dir=suite_dir,
            attempts_csv_path=attempts_csv_path,
            summary_json_path=summary_json_path,
            config_path=config_path,
            attempts=attempts,
            summaries=summaries,
            recommendation=recommendation,
            proxy_preflight=proxy_preflight,
        ),
    )
    return {
        "attempts_csv": attempts_csv_path,
        "summary_json": summary_json_path,
        "report_txt": report_path,
        "config_json": config_path,
    }


def main(argv: Sequence[str] | None = None) -> int:
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    if hasattr(sys.stderr, "reconfigure"):
        sys.stderr.reconfigure(encoding="utf-8", errors="replace")

    args = parse_args(argv)
    env_file = Path(args.env_file).expanduser()
    if args.env_file and env_file.exists():
        load_env_file(env_file)
    if args.cooldown_429_seconds is None:
        args.cooldown_429_seconds = int_from_env("COOLDOWN_429_SECONDS", 3600)
    if args.cooldown_bot_seconds is None:
        args.cooldown_bot_seconds = int_from_env("COOLDOWN_BOT_SECONDS", 5400)

    keyword_batches_path = resolve_keyword_batches_path(args.keyword_batches)
    keyword_batches = load_keyword_batches_from_json(str(keyword_batches_path))
    if not keyword_batches:
        raise SystemExit("Accepted Bicotender keyword batch artifact contains no batches")

    proxy_pool = create_proxy_pool(strategy=args.proxy_strategy)
    suite_dir = Path(args.output_dir).expanduser().resolve() / time.strftime("%Y%m%d_%H%M%S")
    ensure_dir(suite_dir)
    logger = build_logger(suite_dir / "run.log")
    logger.info("Start Bicotender calibration suite=%s", suite_dir)

    attempts, summaries, recommendation, proxy_preflight = run_calibration(
        args=args,
        proxy_pool=proxy_pool,
        keyword_batches=keyword_batches,
        suite_dir=suite_dir,
        logger=logger,
    )
    artifact_paths = write_artifacts(
        suite_dir=suite_dir,
        attempts=attempts,
        summaries=summaries,
        recommendation=recommendation,
        proxy_preflight=proxy_preflight,
        args=args,
        proxy_pool=proxy_pool,
        keyword_batches_path=keyword_batches_path,
    )

    print(f"SUITE_DIR {suite_dir}")
    for label, path in artifact_paths.items():
        print(f"{label.upper()} {path}")
    for summary in sorted(summaries, key=lambda item: item.delay_seconds, reverse=True):
        print(
            "DELAY_BUCKET "
            f"delay={format_delay(summary.delay_seconds)} "
            f"total={summary.total_attempts} clean={summary.clean} zero_result={summary.zero_result} "
            f"blocked={summary.blocked} "
            f"429={summary.status_429} 403={summary.status_403} bot_gate={summary.bot_gate} "
            f"ambiguous_no_row={summary.ambiguous_no_row} static_marker={summary.static_marker}"
        )
    print("RECOMMENDED_ENV_LINES")
    for line in recommendation.env_lines:
        print(line)
    print(f"EXACT_BAN_THRESHOLD_KNOWN {str(recommendation.exact_ban_threshold_known).lower()}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

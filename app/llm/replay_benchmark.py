from __future__ import annotations

import copy
import json
import os
import time
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import requests

from app.llm.benchmark_capture import SUPPORTED_LLM_BENCHMARK_STAGES
from app.llm.openai_responses import parse_openai_response
from app.llm.pricing import calculate_usage_cost_usd


_SUPPORTED_STAGE_SET = frozenset(SUPPORTED_LLM_BENCHMARK_STAGES)
_DEFAULT_OPENAI_BASE_URL = "https://api.openai.com/v1"
_DEFAULT_TIMEOUT_SECONDS = 30
ResponseSender = Callable[[dict[str, Any]], Mapping[str, Any]]


@dataclass(frozen=True, slots=True)
class ReplayFixture:
    stage: str
    ordinal: int
    row_index: int
    inn: str
    company_name: str
    url: str
    request_body_template: dict[str, Any]
    would_call_in_prod: bool
    prod_skip_reason: str
    trust_state: str
    decision_source_context: dict[str, Any]
    source_run_selection: dict[str, Any]
    fixture_hash: str

    @classmethod
    def from_mapping(cls, payload: Mapping[str, Any], *, line_no: int) -> ReplayFixture:
        stage = _normalize_text(payload.get("stage"))
        if stage not in _SUPPORTED_STAGE_SET:
            supported = ", ".join(SUPPORTED_LLM_BENCHMARK_STAGES)
            raise ValueError(
                f"Fixture line {line_no} has unsupported stage {stage!r}; expected one of: {supported}"
            )

        fixture_hash = _normalize_text(payload.get("fixture_hash"))
        if not fixture_hash:
            raise ValueError(f"Fixture line {line_no} is missing fixture_hash")

        return cls(
            stage=stage,
            ordinal=_require_int(payload, "ordinal", line_no=line_no),
            row_index=_require_int(payload, "row_index", line_no=line_no),
            inn=str(payload.get("inn", "") or ""),
            company_name=str(payload.get("company_name", "") or ""),
            url=str(payload.get("url", "") or ""),
            request_body_template=_require_object(payload, "request_body_template", line_no=line_no),
            would_call_in_prod=bool(payload.get("would_call_in_prod")),
            prod_skip_reason=str(payload.get("prod_skip_reason", "") or ""),
            trust_state=str(payload.get("trust_state", "") or ""),
            decision_source_context=_optional_object(payload, "decision_source_context"),
            source_run_selection=_optional_object(payload, "source_run_selection"),
            fixture_hash=fixture_hash,
        )


def load_replay_fixtures(fixtures_path: Path | str) -> list[ReplayFixture]:
    path = Path(fixtures_path).expanduser()
    fixtures: list[ReplayFixture] = []
    for line_no, raw_line in enumerate(path.read_text(encoding="utf-8").splitlines(), start=1):
        if not raw_line.strip():
            continue
        payload = json.loads(raw_line)
        if not isinstance(payload, Mapping):
            raise ValueError(f"Fixture line {line_no} must be a JSON object")
        fixtures.append(ReplayFixture.from_mapping(payload, line_no=line_no))
    if not fixtures:
        raise ValueError(f"Fixture file contains no records: {path}")
    return fixtures


def infer_replay_stage(fixtures: Sequence[ReplayFixture]) -> str:
    stages = {fixture.stage for fixture in fixtures}
    if not stages:
        raise ValueError("Replay benchmark received no fixtures")
    if len(stages) != 1:
        raise ValueError(
            "Replay benchmark expects fixtures for exactly one stage per run; "
            f"got: {', '.join(sorted(stages))}"
        )
    return next(iter(stages))


def summarize_benchmark_events(
    events: Sequence[Mapping[str, Any]],
    *,
    model: str,
    stage: str,
) -> dict[str, Any]:
    total_fixtures = len(events)
    success_count = 0
    error_count = 0
    parser_reason_counts: dict[str, int] = {}
    total_cost_usd: float | None = 0.0

    for event in events:
        status = _normalize_text(event.get("status"))
        if status == "success":
            success_count += 1
        else:
            error_count += 1

        parser_reason = _normalize_text(event.get("parser_reason"))
        if parser_reason:
            parser_reason_counts[parser_reason] = parser_reason_counts.get(parser_reason, 0) + 1

        total_cost_usd = _merge_cost_total(
            total_cost_usd,
            event.get("total_cost_usd"),
            mark_unknown=bool(event.get("cost_unknown")),
        )

    cost_per_success_usd = None
    if total_cost_usd is not None and success_count > 0:
        cost_per_success_usd = round(total_cost_usd / success_count, 8)

    success_rate = 0.0
    if total_fixtures > 0:
        success_rate = round(success_count / total_fixtures, 6)

    return {
        "total_fixtures": total_fixtures,
        "success_count": success_count,
        "error_count": error_count,
        "success_rate": success_rate,
        "parser_reason_counts": parser_reason_counts,
        "total_cost_usd": total_cost_usd,
        "cost_per_success_usd": cost_per_success_usd,
        "cost_unknown": total_cost_usd is None,
        "model": model,
        "stage": stage,
    }


def run_replay_benchmark(
    fixtures_path: Path | str,
    *,
    model: str,
    output_dir: Path | str,
    request_sender: ResponseSender | None = None,
) -> dict[str, Any]:
    normalized_model = _normalize_text(model)
    if not normalized_model:
        raise ValueError("Replay benchmark model must not be empty")

    fixtures_file = Path(fixtures_path).expanduser().resolve()
    target_dir = Path(output_dir).expanduser().resolve()
    fixtures = load_replay_fixtures(fixtures_file)
    stage = infer_replay_stage(fixtures)
    sender = request_sender or _build_request_sender()

    target_dir.mkdir(parents=True, exist_ok=True)
    events: list[dict[str, Any]] = []
    results: list[dict[str, Any]] = []

    for fixture_index, fixture in enumerate(fixtures, start=1):
        request_body = _build_request_body(fixture, model=normalized_model)
        started_at = time.perf_counter()
        payload: dict[str, Any] | None = None
        parsed_output: dict[str, Any] | None = None
        error: str = ""
        parser_reason: str | None = None
        parser_source: str | None = None

        try:
            raw_payload = sender(request_body)
            if not isinstance(raw_payload, Mapping):
                raise ValueError("Responses API returned a non-object JSON payload")
            payload = dict(raw_payload)
            parse_result = parse_openai_response(payload)
            parser_reason = parse_result.reason
            parser_source = parse_result.source
            if parse_result.data is not None:
                parsed_output = dict(parse_result.data)
            else:
                error = parse_result.message
        except Exception as exc:
            error = str(exc)

        elapsed_seconds = round(time.perf_counter() - started_at, 3)
        event = {
            "ts": _utc_now_iso(),
            "type": "llm_replay_benchmark_result",
            "status": "success" if parsed_output is not None else "error",
            "fixture_index": fixture_index,
            "fixture_hash": fixture.fixture_hash,
            "stage": fixture.stage,
            "model": normalized_model,
            "ordinal": fixture.ordinal,
            "row_index": fixture.row_index,
            "inn": fixture.inn,
            "company_name": fixture.company_name,
            "url": fixture.url,
            "would_call_in_prod": fixture.would_call_in_prod,
            "prod_skip_reason": fixture.prod_skip_reason,
            "trust_state": fixture.trust_state,
            "elapsed_seconds": elapsed_seconds,
            "parser_reason": parser_reason,
            "parser_source": parser_source,
            **build_openai_response_diagnostics(payload, parser_reason=parser_reason),
            **build_usage_cost_fields(normalized_model, payload),
        }
        if error:
            event["error"] = error
        if payload and isinstance(payload.get("id"), str):
            event["response_id"] = payload.get("id")
        events.append(event)

        result = {
            **event,
            "decision_source_context": fixture.decision_source_context,
            "source_run_selection": fixture.source_run_selection,
            "request_body": request_body,
            "parsed_output": parsed_output,
            "response_payload": payload,
        }
        results.append(result)

    summary = summarize_benchmark_events(events, model=normalized_model, stage=stage)
    summary.update(
        {
            "generated_at": _utc_now_iso(),
            "fixtures_path": str(fixtures_file),
            "output_dir": str(target_dir),
        }
    )

    events_path = target_dir / "benchmark_events.jsonl"
    summary_path = target_dir / "benchmark_summary.json"
    results_path = target_dir / "benchmark_results.json"

    _write_jsonl(events_path, events)
    _write_json(summary_path, summary)
    _write_json(
        results_path,
        {
            "generated_at": summary["generated_at"],
            "fixtures_path": str(fixtures_file),
            "output_dir": str(target_dir),
            "model": normalized_model,
            "stage": stage,
            "summary": summary,
            "results": results,
        },
    )

    return {
        "events_path": str(events_path),
        "summary_path": str(summary_path),
        "results_path": str(results_path),
        "summary": summary,
        "results": results,
    }


def build_openai_response_diagnostics(
    payload: Mapping[str, Any] | None,
    *,
    parser_reason: str | None = None,
) -> dict[str, Any]:
    output = payload.get("output") if isinstance(payload, Mapping) else None
    content_items: list[Mapping[str, Any]] = []
    if isinstance(output, list):
        for item in output:
            if not isinstance(item, Mapping):
                continue
            content = item.get("content")
            if not isinstance(content, list):
                continue
            for entry in content:
                if isinstance(entry, Mapping):
                    content_items.append(entry)

    response_status = payload.get("status") if isinstance(payload, Mapping) and isinstance(payload.get("status"), str) else None
    incomplete_details = payload.get("incomplete_details") if isinstance(payload, Mapping) else None
    incomplete_reason = (
        incomplete_details.get("reason")
        if isinstance(incomplete_details, Mapping) and isinstance(incomplete_details.get("reason"), str)
        else None
    )
    has_output_text = any(item.get("type") == "output_text" for item in content_items)
    has_parsed = any("parsed" in item for item in content_items)
    content_types = _dedupe_preserve_order(
        _normalize_text(item.get("type"))
        for item in content_items
        if _normalize_text(item.get("type"))
    )
    return {
        "parser_reason": parser_reason,
        "response_status": response_status,
        "has_output": isinstance(output, list) and bool(output),
        "has_output_text": has_output_text,
        "has_parsed": has_parsed,
        "content_types": content_types,
        "has_refusal": any(item.get("type") == "refusal" for item in content_items),
        "incomplete_reason": incomplete_reason,
    }


def build_usage_cost_fields(model: str, payload: Mapping[str, Any] | None) -> dict[str, Any]:
    usage = payload.get("usage") if isinstance(payload, Mapping) else None
    input_details = usage.get("input_tokens_details") if isinstance(usage, Mapping) else None
    cached_input_tokens = (
        input_details.get("cached_tokens")
        if isinstance(input_details, Mapping)
        else 0
    )
    cost = calculate_usage_cost_usd(
        model,
        input_tokens=usage.get("input_tokens") if isinstance(usage, Mapping) else None,
        output_tokens=usage.get("output_tokens") if isinstance(usage, Mapping) else None,
        cached_input_tokens=cached_input_tokens,
    )
    fields = {
        "input_tokens": cost.input_tokens,
        "output_tokens": cost.output_tokens,
        "input_cost_usd": cost.input_cost_usd,
        "output_cost_usd": cost.output_cost_usd,
        "total_cost_usd": cost.total_cost_usd,
    }
    if cost.cost_unknown:
        fields["cost_unknown"] = True
    return fields


def _build_request_body(fixture: ReplayFixture, *, model: str) -> dict[str, Any]:
    body = copy.deepcopy(fixture.request_body_template)
    body["model"] = model
    return body


def _build_request_sender() -> ResponseSender:
    api_key = os.getenv("OPENAI_API_KEY", "").strip()
    if not api_key:
        raise RuntimeError("OPENAI_API_KEY is required for replay benchmark")
    base_url = os.getenv("OPENAI_BASE_URL", _DEFAULT_OPENAI_BASE_URL).rstrip("/")
    timeout_seconds = int(os.getenv("OPENAI_TIMEOUT_SECONDS", str(_DEFAULT_TIMEOUT_SECONDS)) or _DEFAULT_TIMEOUT_SECONDS)

    def _send(body: dict[str, Any]) -> Mapping[str, Any]:
        response = requests.post(
            f"{base_url}/responses",
            headers={
                "Authorization": f"Bearer {api_key}",
                "Content-Type": "application/json",
            },
            json=body,
            timeout=timeout_seconds,
        )
        response.raise_for_status()
        payload = response.json()
        if not isinstance(payload, Mapping):
            raise ValueError("Responses API returned a non-object JSON payload")
        return payload

    return _send


def _write_jsonl(path: Path, rows: Sequence[Mapping[str, Any]]) -> None:
    with path.open("w", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row, ensure_ascii=False))
            handle.write("\n")


def _write_json(path: Path, payload: Mapping[str, Any]) -> None:
    path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


def _require_int(payload: Mapping[str, Any], field_name: str, *, line_no: int) -> int:
    raw_value = payload.get(field_name)
    try:
        return int(raw_value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"Fixture line {line_no} has invalid {field_name}: {raw_value!r}") from exc


def _require_object(payload: Mapping[str, Any], field_name: str, *, line_no: int) -> dict[str, Any]:
    value = payload.get(field_name)
    if not isinstance(value, Mapping):
        raise ValueError(f"Fixture line {line_no} is missing object field {field_name}")
    return dict(value)


def _optional_object(payload: Mapping[str, Any], field_name: str) -> dict[str, Any]:
    value = payload.get(field_name)
    if not isinstance(value, Mapping):
        return {}
    return dict(value)


def _dedupe_preserve_order(values: Sequence[str] | Any) -> list[str]:
    seen: set[str] = set()
    ordered: list[str] = []
    for value in values:
        if value in seen:
            continue
        seen.add(value)
        ordered.append(value)
    return ordered


def _merge_cost_total(current: float | None, delta: Any, *, mark_unknown: bool) -> float | None:
    if mark_unknown:
        return None
    if current is None:
        return None
    if isinstance(delta, (int, float)):
        return round(current + float(delta), 8)
    return current


def _normalize_text(value: Any) -> str:
    return " ".join(str(value or "").split())


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")

from __future__ import annotations

from dataclasses import asdict
from typing import Any
from urllib.parse import urlparse

from .models import ContentRecord, RouteStrategy, SiteProbe, normalize_worth_crawling
from .strategy import canonical_route_family, counts_toward_planner_coverage


def _canonical_accounting_key(
    *,
    route_family: str,
    host_cap: Any = "",
    route_pattern: Any = "",
    site_url: Any = "",
    legacy_accounting_key: Any = "",
) -> str:
    canonical_family = canonical_route_family(route_family) or route_family
    if not canonical_family:
        return ""
    host_key = _normalized_host_key(host_cap)
    if not host_key:
        host_key = _normalized_host_key(route_pattern)
    if not host_key:
        host_key = _normalized_host_key(site_url)
    if not host_key:
        host_key = _normalized_host_key_from_accounting(legacy_accounting_key)
    if not host_key:
        return canonical_family
    return f"{host_key}:{canonical_family}"


def _normalized_host_key(value: Any) -> str:
    raw = str(value or "").strip()
    if not raw:
        return ""
    parsed = urlparse(raw)
    if parsed.netloc:
        return parsed.netloc.strip().lower()
    if "://" not in raw and "/" not in raw:
        return raw.lower()
    return ""


def _normalized_host_key_from_accounting(value: Any) -> str:
    raw = str(value or "").strip()
    if not raw:
        return ""
    host_prefix, separator, _ = raw.rpartition(":")
    if separator:
        return host_prefix.strip().lower()
    return ""


def _route_family_from_accounting_key(value: Any) -> str:
    raw = str(value or "").strip()
    if not raw:
        return ""
    _, separator, suffix = raw.rpartition(":")
    candidate = suffix if separator else raw
    return canonical_route_family(candidate)


def _coerce_bool(value: Any, default: bool = False) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    if isinstance(value, str):
        normalized = value.strip().lower()
        if normalized in {"1", "true", "yes", "y", "on"}:
            return True
        if normalized in {"0", "false", "no", "n", "off", ""}:
            return False
    return default


def _coerce_int(value: Any, default: int = 0) -> int:
    if value is None or value == "":
        return default
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _coerce_optional_int(value: Any) -> int | None:
    if value is None or value == "":
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _coerce_float(value: Any, default: float = 0.0) -> float:
    if value is None or value == "":
        return default
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _coerce_str_list(value: Any) -> list[str]:
    if isinstance(value, list):
        return [str(item) for item in value]
    if isinstance(value, tuple | set):
        return [str(item) for item in value]
    return []


def _coerce_dict(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return dict(value)
    return {}


def site_probe_to_dict(probe: SiteProbe) -> dict[str, Any]:
    payload = asdict(probe)
    payload["worth_crawling"] = normalize_worth_crawling(probe.worth_crawling)
    return payload


def site_probe_from_dict(payload: dict[str, Any]) -> SiteProbe:
    return SiteProbe(
        url=payload.get("url", ""),
        final_url=payload.get("final_url", ""),
        status=payload.get("status", ""),
        http_status=payload.get("http_status"),
        content_type=payload.get("content_type", ""),
        encoding=payload.get("encoding", ""),
        site_class=payload.get("site_class", "F"),
        worth_crawling=normalize_worth_crawling(payload.get("worth_crawling", "false")),
        browser_required_default=_coerce_bool(payload.get("browser_required_default", False)),
        anti_bot_detected=_coerce_bool(payload.get("anti_bot_detected", False)),
        block_class=payload.get("block_class", ""),
        anti_bot_reason=payload.get("anti_bot_reason", ""),
        challenge_detected=_coerce_bool(payload.get("challenge_detected", False)),
        html_ok=_coerce_bool(payload.get("html_ok", False)),
        robots_found=_coerce_bool(payload.get("robots_found", False)),
        sitemap_found=_coerce_bool(payload.get("sitemap_found", False)),
        internal_links_count=_coerce_int(payload.get("internal_links_count", 0), 0),
        document_links_count=_coerce_int(payload.get("document_links_count", 0), 0),
        text_length=_coerce_int(payload.get("text_length", 0), 0),
        redirect_count=_coerce_int(payload.get("redirect_count", 0), 0),
        key_sections=_coerce_str_list(payload.get("key_sections", [])),
        sampled_urls=_coerce_str_list(payload.get("sampled_urls", [])),
        obvious_routes_attempted=_coerce_str_list(payload.get("obvious_routes_attempted", [])),
        cms_guess=payload.get("cms_guess", ""),
        failure_reason=payload.get("failure_reason", ""),
        timeout_reason=payload.get("timeout_reason", ""),
        notes=_coerce_str_list(payload.get("notes", [])),
        errors=_coerce_str_list(payload.get("errors", [])),
        transport_selected=payload.get("transport_selected", ""),
        transport_final=payload.get("transport_final", ""),
        blocked_by_policy=_coerce_bool(payload.get("blocked_by_policy", False)),
        escalation_reason=payload.get("escalation_reason", ""),
        normalized_symptoms=_coerce_str_list(payload.get("normalized_symptoms", [])),
        policy_hints=_coerce_dict(payload.get("policy_hints", {})),
    )


def route_strategy_to_dict(strategy: RouteStrategy) -> dict[str, Any]:
    payload = asdict(strategy)
    canonical_family = canonical_route_family(strategy.route_family or strategy.section_guess) or strategy.route_family
    payload["route_family"] = canonical_family
    payload["accounting_key"] = _canonical_accounting_key(
        route_family=canonical_family,
        host_cap=strategy.host_cap,
        route_pattern=strategy.route_pattern,
        site_url=strategy.site_url,
        legacy_accounting_key=strategy.accounting_key,
    )
    payload["counts_toward_coverage"] = counts_toward_planner_coverage(canonical_family)
    payload["queue_name"] = strategy.effective_queue_name()
    payload["skip_reason"] = strategy.effective_skip_reason()
    return payload


def route_strategy_from_dict(payload: dict[str, Any]) -> RouteStrategy:
    route_family = canonical_route_family(payload.get("route_family", "") or payload.get("section_guess", ""))
    if not route_family:
        route_family = _route_family_from_accounting_key(payload.get("accounting_key", ""))
    return RouteStrategy(
        site_url=payload.get("site_url", ""),
        route_pattern=payload.get("route_pattern", ""),
        section_guess=payload.get("section_guess", ""),
        mode=payload.get("mode", "skip"),
        confidence=_coerce_float(payload.get("confidence", 0.0), 0.0),
        route_family=route_family,
        priority=_coerce_int(payload.get("priority", 0), 0),
        crawl_budget=_coerce_int(payload.get("crawl_budget", 1), 1),
        queue_name=payload.get("queue_name", ""),
        accounting_key=_canonical_accounting_key(
            route_family=route_family,
            host_cap=payload.get("host_cap", ""),
            route_pattern=payload.get("route_pattern", ""),
            site_url=payload.get("site_url", ""),
            legacy_accounting_key=payload.get("accounting_key", ""),
        ),
        mandatory=_coerce_bool(payload.get("mandatory", False)),
        counts_toward_coverage=counts_toward_planner_coverage(route_family),
        skip_reason=payload.get("skip_reason", ""),
        max_depth=_coerce_optional_int(payload.get("max_depth")),
        host_cap=payload.get("host_cap", ""),
        path_pattern_cap=payload.get("path_pattern_cap", ""),
        reasons=_coerce_str_list(payload.get("reasons", [])),
        discovery_sources=_coerce_str_list(payload.get("discovery_sources", [])),
    )


def _normalize_tables(value: Any) -> list[list[list[str]]]:
    if not isinstance(value, list):
        return []
    tables: list[list[list[str]]] = []
    for table in value:
        if not isinstance(table, list):
            continue
        normalized_table: list[list[str]] = []
        for row in table:
            if not isinstance(row, list):
                continue
            normalized_table.append([str(cell or "") for cell in row])
        if normalized_table:
            tables.append(normalized_table)
    return tables


def _trace_from_payload(payload: dict[str, Any]) -> dict[str, Any]:
    trace = payload.get("trace")
    return dict(trace) if isinstance(trace, dict) else {}


def _metadata_from_payload(payload: dict[str, Any], trace: dict[str, Any]) -> dict[str, Any]:
    metadata = payload.get("metadata")
    result = dict(metadata) if isinstance(metadata, dict) else {}
    document = trace.get("document") if isinstance(trace, dict) else None
    if isinstance(document, dict):
        document_metadata = document.get("metadata")
        if isinstance(document_metadata, dict):
            for key, value in document_metadata.items():
                result.setdefault(str(key), value)
    attachment = trace.get("attachment") if isinstance(trace, dict) else None
    if isinstance(attachment, dict):
        result.setdefault("attachment", dict(attachment))
    if isinstance(document, dict):
        result.setdefault("document", dict(document))
    return result


def _evidence_ref_from_payload(
    payload: dict[str, Any],
    *,
    source_url_or_file: str,
    attachment: dict[str, Any] | None,
) -> Any:
    evidence_ref = payload.get("evidence_ref")
    if isinstance(evidence_ref, dict):
        return dict(evidence_ref)
    if evidence_ref:
        return {"value": str(evidence_ref)}
    derived: dict[str, Any] = {}
    if source_url_or_file:
        derived["source_url_or_file"] = source_url_or_file
    if isinstance(attachment, dict):
        if attachment.get("local_path"):
            derived["local_path"] = attachment["local_path"]
        if attachment.get("source_url"):
            derived["source_url"] = attachment["source_url"]
    return derived


def content_record_from_dict(payload: dict[str, Any]) -> ContentRecord:
    trace = _trace_from_payload(payload)
    metadata = _metadata_from_payload(payload, trace)
    source_url_or_file = str(
        payload.get("source_url_or_file")
        or payload.get("url")
        or payload.get("source_path")
        or ""
    )
    site_url = str(payload.get("site_url", "") or payload.get("site_id", "") or "")
    text = str(payload.get("text") or payload.get("cleaned_text") or payload.get("raw_text") or "")
    tables = _normalize_tables(payload.get("tables"))
    document = trace.get("document") if isinstance(trace, dict) else None
    attachment = trace.get("attachment") if isinstance(trace, dict) else None
    source_type = str(payload.get("source_type", "") or "")
    if not source_type and isinstance(document, dict):
        source_type = str(document.get("source_format") or "")
    evidence_ref = _evidence_ref_from_payload(
        payload,
        source_url_or_file=source_url_or_file,
        attachment=attachment if isinstance(attachment, dict) else None,
    )
    return ContentRecord(
        company_id=payload.get("company_id", ""),
        site_id=str(payload.get("site_id", "") or site_url),
        site_url=site_url,
        url=str(payload.get("url", "") or source_url_or_file),
        source_type=source_type,
        source_url_or_file=source_url_or_file,
        title=payload.get("title", ""),
        text=text,
        tables=tables,
        metadata=metadata,
        evidence_ref=evidence_ref,
        date=payload.get("date", ""),
        raw_text=str(payload.get("raw_text", "") or text),
        cleaned_text=str(payload.get("cleaned_text", "") or text),
        section_guess=payload.get("section_guess", ""),
        extraction_method=payload.get("extraction_method", ""),
        fetch_status=payload.get("fetch_status", ""),
        content_fingerprint=payload.get("content_fingerprint", ""),
        relevance_label=payload.get("relevance_label", "unknown"),
        relevance_score=float(payload.get("relevance_score", 0.0) or 0.0),
        relevance_reasons=list(payload.get("relevance_reasons", [])),
        llm_result=payload.get("llm_result"),
        notes=list(payload.get("notes", [])),
        trace=dict(trace) if isinstance(trace, dict) else {},
    )

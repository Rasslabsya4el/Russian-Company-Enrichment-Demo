from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Any
from urllib.parse import urlparse

from app.llm.benchmark_capture import describe_content_review_harvest_prod_skip_reason

from .factory_site_parser import FactorySiteParserCompany, FactorySiteParserResult
from .relevance import classify_content_record
from .site_authenticity import SiteDecision

_SITE_DECISION_CAPTURE_PATH = "preparse_trust_gate.site_decision.synthetic_candidate"
_CONTENT_REVIEW_CAPTURE_PATH = "preparse_trust_gate.content_review.forced_harvest"
_FORCED_HARVEST_NONE = "none"
_FORCED_HARVEST_SINGLE_SITE = "single_site_requests_only"
_FORCED_HARVEST_WIDENED = "widened_two_sites_requests_only"
_BENCHMARK_CONTENT_REVIEW_SITE_LIMIT = 2
_BENCHMARK_CONTENT_REVIEW_RECORD_LIMIT = 4
_GENERIC_EMAIL_DOMAINS = frozenset(
    {
        "gmail.com",
        "googlemail.com",
        "mail.ru",
        "bk.ru",
        "inbox.ru",
        "list.ru",
        "yandex.ru",
        "ya.ru",
        "rambler.ru",
        "icloud.com",
        "outlook.com",
        "hotmail.com",
        "live.com",
        "yahoo.com",
        "proton.me",
        "protonmail.com",
    }
)
_EXCLUDED_HINT_DOMAINS = frozenset(
    {
        "list-org.com",
        "rusprofile.ru",
        "spark-interfax.ru",
        "zachestnyibiznes.ru",
    }
)
_EMAIL_HINT_RE = re.compile(r"[A-Za-z0-9._%+-]+@(?:[A-Za-z0-9-]+\.)+[A-Za-z]{2,24}", flags=re.IGNORECASE)
_URL_HINT_RE = re.compile(r"(?:(?:https?://|www\.)[^\s\"'<>]+)", flags=re.IGNORECASE)
_BARE_DOMAIN_HINT_RE = re.compile(
    r"\b(?:[A-Za-z0-9](?:[A-Za-z0-9-]{0,61}[A-Za-z0-9])?\.)+[A-Za-z]{2,24}\b",
    flags=re.IGNORECASE,
)


@dataclass
class PreparseTrustGateResult:
    deep_parse_sites: list[str] = field(default_factory=list)
    surface_only_decisions: list[SiteDecision] = field(default_factory=list)
    trusted_surface_decisions_by_site: dict[str, SiteDecision] = field(default_factory=dict)
    notes: list[str] = field(default_factory=list)


@dataclass
class GatedFactorySiteParseResult:
    parsed_factory_sites: FactorySiteParserResult
    validated_sites: list[SiteDecision] = field(default_factory=list)
    notes: list[str] = field(default_factory=list)


def _normalized_candidate_url(analyzer: Any, url: str) -> str:
    normalized = analyzer.h.normalize_url(url)
    if normalized:
        return normalized
    return str(url or "").strip()


def _decision_metrics(decision: SiteDecision) -> str:
    return (
        f"auth={float(decision.authenticity_score or 0.0):.3f} "
        f"identity={float(decision.identity_score or 0.0):.3f} "
        f"viability={float(decision.viability_score or 0.0):.3f}"
    )


def _append_reason(decision: SiteDecision, reason: str) -> None:
    if reason not in decision.reasons:
        decision.reasons.append(reason)


def _read_payload_field(payload: Any, field_name: str) -> Any:
    if isinstance(payload, dict):
        return payload.get(field_name)
    return getattr(payload, field_name, None)


def _iter_payload_items(payload: Any, field_name: str) -> list[Any]:
    values = _read_payload_field(payload, field_name)
    if isinstance(values, list):
        return list(values)
    if isinstance(values, tuple):
        return list(values)
    if values:
        return [values]
    return []


def _compact_text(analyzer: Any, value: Any, limit: int) -> str:
    compact = getattr(getattr(analyzer, "h", None), "compact_text", None)
    text = str(value or "").strip()
    if callable(compact):
        return str(compact(text, limit) or "")
    if len(text) <= limit:
        return text
    return text[:limit].rstrip()


def _trim_hint_token(value: str) -> str:
    return str(value or "").strip().strip("\"'()[]{}<>.,;")


def _guess_registered_domain(analyzer: Any, host: str) -> str:
    helper = getattr(getattr(analyzer, "h", None), "guess_registered_domain", None)
    normalized_host = str(host or "").strip().lower()
    if callable(helper):
        return str(helper(normalized_host) or "").lower()
    if normalized_host.startswith("www."):
        normalized_host = normalized_host[4:]
    return normalized_host


def _registered_domain(analyzer: Any, url: str) -> str:
    normalized = _normalized_candidate_url(analyzer, url)
    if not normalized:
        return ""
    return _guess_registered_domain(analyzer, urlparse(normalized).netloc)


def _normalize_hint_site(analyzer: Any, raw_value: str) -> str:
    candidate = _trim_hint_token(raw_value)
    if not candidate:
        return ""
    if candidate.startswith("www."):
        candidate = f"https://{candidate}"
    elif "://" not in candidate:
        host_candidate = candidate.split("/", 1)[0]
        if _BARE_DOMAIN_HINT_RE.fullmatch(host_candidate):
            candidate = f"https://{candidate}"
    normalized = _normalized_candidate_url(analyzer, candidate)
    if not normalized:
        return ""
    parsed = urlparse(normalized)
    domain = _registered_domain(analyzer, normalized)
    if not domain or domain in _EXCLUDED_HINT_DOMAINS or domain in _GENERIC_EMAIL_DOMAINS:
        return ""
    if parsed.scheme and parsed.netloc and parsed.netloc.lower().startswith("www.") and parsed.path in {"", "/"}:
        return f"{parsed.scheme}://{domain}/"
    return normalized


def _normalize_email_domain_hint(analyzer: Any, email: str) -> str:
    text = _trim_hint_token(email).lower()
    if "@" not in text:
        return ""
    domain = _guess_registered_domain(analyzer, text.split("@", 1)[-1])
    if not domain or domain in _GENERIC_EMAIL_DOMAINS or domain in _EXCLUDED_HINT_DOMAINS:
        return ""
    return _normalize_hint_site(analyzer, f"https://{domain}")


def _extract_domain_like_hints(text: str) -> list[str]:
    if not text:
        return []
    values: list[str] = []
    values.extend(_trim_hint_token(item) for item in _URL_HINT_RE.findall(text))
    values.extend(_trim_hint_token(item) for item in _EMAIL_HINT_RE.findall(text))
    values.extend(_trim_hint_token(item) for item in _BARE_DOMAIN_HINT_RE.findall(text))
    deduped: list[str] = []
    seen: set[str] = set()
    for value in values:
        key = value.lower()
        if not key or key in seen:
            continue
        seen.add(key)
        deduped.append(value)
    return deduped


def _structured_contact_value(item: Any) -> str:
    if isinstance(item, dict):
        return str(item.get("value", "") or "")
    return str(getattr(item, "value", "") or "")


def _structured_contact_masked(item: Any) -> bool:
    if isinstance(item, dict):
        return bool(item.get("masked", False))
    return bool(getattr(item, "masked", False))


def _build_synthetic_site_hint_bundle(
    *,
    row: Any,
    known_contacts: dict[str, list[str]],
    source_results: dict[str, Any],
    analyzer: Any,
) -> dict[str, Any]:
    candidate_sites: list[str] = []
    reasons_by_site: dict[str, list[str]] = {}
    domain_to_site: dict[str, str] = {}
    best_site = ""
    primary_domain = ""

    def add_site(raw_value: str, reason: str, *, domain_hint: bool = False) -> str:
        normalized = (
            _normalize_email_domain_hint(analyzer, raw_value)
            if domain_hint or "@" in str(raw_value or "")
            else _normalize_hint_site(analyzer, raw_value)
        )
        if not normalized:
            return ""
        domain = _registered_domain(analyzer, normalized)
        if not domain:
            return ""
        existing = domain_to_site.get(domain)
        if existing:
            if reason not in reasons_by_site.setdefault(existing, []):
                reasons_by_site[existing].append(reason)
            return existing
        domain_to_site[domain] = normalized
        candidate_sites.append(normalized)
        reasons_by_site[normalized] = [reason]
        return normalized

    if getattr(row, "xlsx_site", ""):
        best_site = add_site(str(row.xlsx_site), "xlsx_input_site") or best_site

    for site in known_contacts.get("websites", []) or []:
        best_site = add_site(str(site), "known_contacts.website") or best_site
    for email in known_contacts.get("emails", []) or []:
        primary_domain = add_site(str(email), "known_contacts.email_domain", domain_hint=True) or primary_domain

    for source_name, payload in (source_results or {}).items():
        for website_item in _iter_payload_items(payload, "websites"):
            if _structured_contact_masked(website_item):
                continue
            best_site = add_site(_structured_contact_value(website_item), f"{source_name}.website") or best_site
        for email_item in _iter_payload_items(payload, "emails"):
            if _structured_contact_masked(email_item):
                continue
            primary_domain = (
                add_site(_structured_contact_value(email_item), f"{source_name}.email_domain", domain_hint=True)
                or primary_domain
            )
        for field_name in ("company_name_found", "snippets", "notes"):
            for raw_value in _iter_payload_items(payload, field_name):
                for hint in _extract_domain_like_hints(str(raw_value or "")):
                    reason = f"{source_name}.{field_name}"
                    if "@" in hint:
                        primary_domain = add_site(hint, f"{reason}.email_hint", domain_hint=True) or primary_domain
                        continue
                    normalized_site = add_site(hint, f"{reason}.site_hint")
                    if normalized_site and not best_site:
                        best_site = normalized_site

    synthetic_site = best_site or primary_domain or (candidate_sites[0] if candidate_sites else "")
    return {
        "site_url": synthetic_site,
        "best_site": best_site,
        "primary_domain": primary_domain,
        "candidate_sites": candidate_sites[:5],
        "hint_reasons": {key: list(value[:4]) for key, value in reasons_by_site.items()},
    }


def _build_minimal_synthetic_site_context(
    *,
    row: Any,
    site_url: str,
    known_contacts: dict[str, list[str]],
    source_results: dict[str, Any],
    analyzer: Any,
    hint_bundle: dict[str, Any],
    decision_status: str,
) -> dict[str, Any]:
    summarize_source_context = getattr(getattr(analyzer, "h", None), "summarize_source_context", None)
    aggregator_profile = summarize_source_context(source_results) if callable(summarize_source_context) else {}
    return {
        "xlsx_hint": {
            "input_site": str(getattr(row, "xlsx_site", "") or ""),
            "input_phone": str(getattr(row, "xlsx_phone", "") or ""),
            "comment": _compact_text(analyzer, getattr(row, "comment", ""), 220),
        },
        "aggregator_profile": aggregator_profile,
        "known_contacts": {
            "phones": list((known_contacts.get("phones") or [])[:5]),
            "emails": list((known_contacts.get("emails") or [])[:5]),
            "websites": list((known_contacts.get("websites") or [])[:5]),
            "addresses": [_compact_text(analyzer, item, 140) for item in (known_contacts.get("addresses") or [])[:3]],
        },
        "candidate_site": {
            "url": site_url,
            "final_url": site_url,
            "title": "",
            "description": "",
            "phones": [],
            "emails": [],
            "addresses": [],
            "fetched_pages": [],
            "text_excerpt": "",
        },
        "heuristics": {
            "decision_status": decision_status,
            "authenticity_score": 0.0,
            "identity_score": 0.0,
            "viability_score": 0.0,
            "industrial_score": 0.0,
            "conflict_penalty": 0.0,
            "hard_negative_hits": [],
            "matched_name_tokens": [],
            "positive_keywords": [],
            "negative_keywords": [],
            "flags": {},
            "identity_reasons": [],
            "industrial_reasons": [],
        },
        "business_goal": (
            "Need a trustworthy corporate site for this specific company. "
            f"Synthetic benchmark candidate assembled from source hints: {', '.join(hint_bundle.get('candidate_sites') or [])[:180]}"
        ),
    }


def _capture_site_decision_benchmark_fallback_if_forced(
    *,
    row: Any,
    known_contacts: dict[str, list[str]],
    source_results: dict[str, Any],
    analyzer: Any,
    blocker_reason: str,
) -> None:
    llm = getattr(analyzer, "llm", None)
    capture_blocker = getattr(llm, "capture_site_decision_blocker", None)
    capture_fixture = getattr(llm, "capture_forced_site_decision_fixture", None)
    should_force_stage = getattr(llm, "should_force_benchmark_stage", None)
    if not callable(should_force_stage) or not should_force_stage("site_decision"):
        return

    hint_bundle = _build_synthetic_site_hint_bundle(
        row=row,
        known_contacts=known_contacts,
        source_results=source_results,
        analyzer=analyzer,
    )
    synthetic_site = str(hint_bundle.get("site_url", "") or "")
    if synthetic_site and callable(capture_fixture):
        decision_status = "synthetic_candidate"
        llm_context = _build_minimal_synthetic_site_context(
            row=row,
            site_url=synthetic_site,
            known_contacts=known_contacts,
            source_results=source_results,
            analyzer=analyzer,
            hint_bundle=hint_bundle,
            decision_status=decision_status,
        )
        evaluate_site = getattr(analyzer, "_evaluate_site", None)
        derive_status = getattr(analyzer, "_derive_preparse_decision_status", None)
        build_llm_context = getattr(analyzer, "_build_llm_context", None)
        identity_flags: dict[str, Any] = {}
        if callable(evaluate_site):
            decision, combined_text, identity, industrial = evaluate_site(
                row,
                synthetic_site,
                known_contacts,
                source_results,
                allow_extra_pages=False,
            )
            if getattr(decision, "status", "") == "success":
                identity_flags = dict((identity or {}).get("flags") or {})
                if callable(derive_status):
                    decision_status = derive_status(
                        decision.authenticity_score,
                        decision.identity_score,
                        decision.viability_score,
                        identity_flags,
                        decision.hard_negative_hits,
                        decision.extracted_phones,
                        decision.extracted_emails,
                    )
                    decision.decision_status = decision_status
                if callable(build_llm_context):
                    llm_context = build_llm_context(
                        row=row,
                        decision=decision,
                        source_results=source_results,
                        known_contacts=known_contacts,
                        combined_text=combined_text,
                        identity=identity,
                        industrial=industrial,
                    )
        decision_source_context = {
            "capture_origin": "benchmark_synthetic_candidate",
            "decision_status": decision_status,
            "best_site": hint_bundle.get("best_site", ""),
            "primary_domain": hint_bundle.get("primary_domain", ""),
            "synthetic_hint_sites": list(hint_bundle.get("candidate_sites", [])[:5]),
            "hint_reasons": dict(hint_bundle.get("hint_reasons", {})),
            "identity_flags": identity_flags,
        }
        capture_fixture(
            row=row,
            site_url=synthetic_site,
            compressed_context=llm_context,
            trust_state=decision_status,
            prod_skip_reason=blocker_reason,
            decision_source_context=decision_source_context,
            benchmark_capture_path=_SITE_DECISION_CAPTURE_PATH,
            synthetic_candidate_used=True,
            forced_harvest_level=_FORCED_HARVEST_NONE,
            benchmark_synthetic_candidate=True,
        )
        return

    if callable(capture_blocker):
        capture_blocker(
            row=row,
            blocker_reason=blocker_reason,
            benchmark_capture_path=_SITE_DECISION_CAPTURE_PATH,
            synthetic_candidate_used=False,
            forced_harvest_level=_FORCED_HARVEST_NONE,
        )


def _harvest_benchmark_content_records(
    *,
    row: Any,
    candidate_sites: list[str],
    source_results: dict[str, Any],
    analyzer: Any,
    factory_site_parser: Any,
) -> tuple[list[Any], str, str]:
    normalized_sites: list[str] = []
    for raw_site in candidate_sites or []:
        normalized_site = _normalized_candidate_url(analyzer, raw_site)
        if normalized_site and normalized_site not in normalized_sites:
            normalized_sites.append(normalized_site)
    limited_sites = normalized_sites[:_BENCHMARK_CONTENT_REVIEW_SITE_LIMIT]
    attempted_sites: list[str] = []
    harvested_records: list[Any] = []
    if not callable(getattr(factory_site_parser, "parse", None)):
        return harvested_records, "", _FORCED_HARVEST_NONE

    for site_url in limited_sites:
        attempted_sites.append(site_url)
        benchmark_company = FactorySiteParserCompany.from_row(
            row,
            candidate_sites=[site_url],
            source_results=source_results,
        )
        harvest_result = factory_site_parser.parse(benchmark_company, dry_run=True)
        site_records = list(getattr(harvest_result, "content_records", []) or [])
        for record in site_records:
            classify_content_record(record)
        harvested_records.extend(site_records)
        if site_records:
            break

    forced_harvest_level = _FORCED_HARVEST_NONE
    if len(attempted_sites) == 1:
        forced_harvest_level = _FORCED_HARVEST_SINGLE_SITE
    elif len(attempted_sites) >= 2:
        forced_harvest_level = _FORCED_HARVEST_WIDENED
    primary_site = limited_sites[0] if limited_sites else ""
    return harvested_records[:_BENCHMARK_CONTENT_REVIEW_RECORD_LIMIT], primary_site, forced_harvest_level


def _capture_content_review_benchmark_fallback_if_forced(
    *,
    row: Any,
    candidate_sites: list[str],
    source_results: dict[str, Any],
    analyzer: Any,
    factory_site_parser: Any,
    parsed_factory_sites: FactorySiteParserResult,
) -> None:
    llm = getattr(analyzer, "llm", None)
    should_force_stage = getattr(llm, "should_force_benchmark_stage", None)
    capture_records = getattr(llm, "capture_content_review_benchmark_records", None)
    capture_blocker = getattr(llm, "capture_content_review_blocker", None)
    if not callable(should_force_stage) or not should_force_stage("content_review"):
        return
    if getattr(parsed_factory_sites, "content_records", None):
        return

    had_deep_parse_sites = bool(getattr(parsed_factory_sites, "plans", []))
    harvested_records, primary_site, forced_harvest_level = _harvest_benchmark_content_records(
        row=row,
        candidate_sites=candidate_sites,
        source_results=source_results,
        analyzer=analyzer,
        factory_site_parser=factory_site_parser,
    )

    captured = 0
    if harvested_records and callable(capture_records):
        captured = int(
            capture_records(
                row=row,
                records=harvested_records,
                primary_site=primary_site,
                default_prod_skip_reason=describe_content_review_harvest_prod_skip_reason(
                    had_deep_parse_sites=had_deep_parse_sites
                ),
                benchmark_forced_harvest=True,
                benchmark_capture_path=_CONTENT_REVIEW_CAPTURE_PATH,
                synthetic_candidate_used=False,
                forced_harvest_level=forced_harvest_level,
            )
            or 0
        )
    if captured == 0 and callable(capture_blocker):
        capture_blocker(
            row=row,
            blocker_reason="no_content_record",
            site_url=primary_site,
            benchmark_capture_path=_CONTENT_REVIEW_CAPTURE_PATH,
            synthetic_candidate_used=False,
            forced_harvest_level=forced_harvest_level,
        )


def gate_candidate_sites_before_deep_parse(
    *,
    row: Any,
    candidate_sites: list[str],
    known_contacts: dict[str, list[str]],
    source_results: dict[str, Any],
    analyzer: Any,
) -> PreparseTrustGateResult:
    result = PreparseTrustGateResult()
    if not any(str(site_url or "").strip() for site_url in candidate_sites or []):
        _capture_site_decision_benchmark_fallback_if_forced(
            row=row,
            known_contacts=known_contacts,
            source_results=source_results,
            analyzer=analyzer,
            blocker_reason="no_candidate_site",
        )
        return result

    seen_input_sites: set[str] = set()
    seen_parse_sites: set[str] = set()

    for site_url in candidate_sites or []:
        input_key = _normalized_candidate_url(analyzer, site_url)
        if not input_key or input_key in seen_input_sites:
            continue
        seen_input_sites.add(input_key)

        decision = analyzer.analyze_surface(row, site_url, known_contacts, source_results)
        candidate_key = _normalized_candidate_url(analyzer, decision.final_url or decision.url or site_url)
        if not candidate_key:
            candidate_key = input_key

        if decision.decision_status == "candidate":
            if candidate_key not in seen_parse_sites:
                result.deep_parse_sites.append(candidate_key)
                result.trusted_surface_decisions_by_site[candidate_key] = decision
                seen_parse_sites.add(candidate_key)
            result.notes.append(
                f"preparse trust gate allow_deep_parse site={candidate_key} {_decision_metrics(decision)}"
            )
            continue

        result.surface_only_decisions.append(decision)
        gate_label = "reject" if decision.decision_status == "rejected" else "ambiguous"
        result.notes.append(
            f"preparse trust gate skip_deep_parse site={candidate_key} gate={gate_label} {_decision_metrics(decision)}"
        )

    return result


def run_gated_factory_site_parse(
    *,
    row: Any,
    candidate_sites: list[str],
    known_contacts: dict[str, list[str]],
    source_results: dict[str, Any],
    analyzer: Any,
    factory_site_parser: Any,
) -> GatedFactorySiteParseResult:
    gate = gate_candidate_sites_before_deep_parse(
        row=row,
        candidate_sites=candidate_sites,
        known_contacts=known_contacts,
        source_results=source_results,
        analyzer=analyzer,
    )

    parser_company = FactorySiteParserCompany.from_row(
        row,
        candidate_sites=gate.deep_parse_sites,
        source_results=source_results,
    )
    parsed_factory_sites = (
        factory_site_parser.parse(parser_company)
        if gate.deep_parse_sites
        else FactorySiteParserResult(company=parser_company)
    )

    validated_sites = list(gate.surface_only_decisions)
    pending_surface = dict(gate.trusted_surface_decisions_by_site)
    for site_plan in parsed_factory_sites.plans:
        site_key = _normalized_candidate_url(analyzer, site_plan.site_url)
        surface_decision = pending_surface.pop(site_key, None)
        if not site_plan.allows_deep_check:
            if surface_decision is not None:
                _append_reason(surface_decision, "planner/probe blocked deep parse after cheap trust gate")
                validated_sites.append(surface_decision)
            continue
        validated_sites.append(analyzer.analyze(row, site_plan.site_url, known_contacts, source_results))

    for surface_decision in pending_surface.values():
        _append_reason(surface_decision, "planner returned no deep-check site plan after cheap trust gate")
        validated_sites.append(surface_decision)

    _capture_content_review_benchmark_fallback_if_forced(
        row=row,
        candidate_sites=candidate_sites,
        source_results=source_results,
        analyzer=analyzer,
        factory_site_parser=factory_site_parser,
        parsed_factory_sites=parsed_factory_sites,
    )

    return GatedFactorySiteParseResult(
        parsed_factory_sites=parsed_factory_sites,
        validated_sites=validated_sites,
        notes=list(gate.notes),
    )

from __future__ import annotations

import re
from collections.abc import Iterable
from typing import Any

from .common import (
    LEAD_NEGATIVE_KEYWORDS,
    dedupe_preserve_order,
    lead_family_keywords,
    route_family_for_section,
    route_supports_site_identity,
    site_identity_keywords,
)
from .models import ContentRecord
from .strategy import guess_section_from_url

STRONG_SIGNAL_KEYWORDS = frozenset(
    keyword
    for family_name in ("procurement", "surplus/realization")
    for keyword in lead_family_keywords.get(family_name, {})
)
DOCUMENT_SOURCE_HINTS = frozenset({"pdf", "doc", "docx", "xls", "xlsx", "csv", "txt", "json", "zip", "rar", "7z"})
NON_SAMPLE_ROUTE_FAMILIES = frozenset(
    {
        "procurement",
        "surplus/realization",
        "direct_sale",
        "docs/certificates",
        "production/products",
        "branches/warehouses",
        "files",
        "search",
    }
)
AUTHORITATIVE_PROVENANCE_KEYS = frozenset(
    {
        "route_origin",
        "from_sample",
        "source_kind",
        "route_family",
        "source_page",
        "discovery_source",
    }
)


def _fingerprint_for_record(record: ContentRecord) -> str:
    return record.content_fingerprint or f"{record.url}|{record.fetch_status}|{record.source_type}"


def _normalized_mapping(value: Any) -> dict[str, Any]:
    return dict(value) if isinstance(value, dict) else {}


def _normalized_string(value: Any) -> str:
    return str(value or "").strip()


def _normalized_bool(value: Any) -> bool | None:
    if isinstance(value, bool):
        return value
    normalized = _normalized_string(value).lower()
    if normalized in {"true", "1", "yes", "y", "on"}:
        return True
    if normalized in {"false", "0", "no", "n", "off"}:
        return False
    return None


def _provenance_payload(value: Any) -> dict[str, Any]:
    payload = _normalized_mapping(value)
    return {
        key: item
        for key, item in payload.items()
        if item is not None and item != "" and item != []
    }


def _source_kind(record: ContentRecord) -> str:
    crawl_trace = _factory_parser_crawl_trace(record)
    authoritative_source_kind = _normalized_string(crawl_trace.get("source_kind")).lower()
    if authoritative_source_kind in {"page", "document"}:
        return authoritative_source_kind
    source_type = _normalized_string(record.source_type).lower()
    evidence_kind = _normalized_string(record.evidence_ref.get("kind")).lower()
    if evidence_kind == "document_file":
        return "document"
    if source_type in DOCUMENT_SOURCE_HINTS:
        return "document"
    if _normalized_mapping(record.metadata.get("document_queue")):
        return "document"
    if _normalized_mapping(record.trace.get("document")):
        return "document"
    if _normalized_mapping(record.trace.get("document_queue")):
        return "document"
    if record.section_guess == "documents" and source_type != "html":
        return "document"
    return "page"


def _factory_parser_trace(record: ContentRecord) -> dict[str, Any]:
    return _normalized_mapping(record.trace.get("factory_site_parser"))


def _factory_parser_crawl_trace(record: ContentRecord) -> dict[str, Any]:
    return _normalized_mapping(_factory_parser_trace(record).get("crawl"))


def _authoritative_sample_hint(record: ContentRecord) -> bool | None:
    crawl_trace = _factory_parser_crawl_trace(record)
    from_sample = _normalized_bool(crawl_trace.get("from_sample"))
    if from_sample is not None:
        return from_sample
    route_origin = _normalized_string(crawl_trace.get("route_origin")).lower()
    if route_origin == "sample":
        return True
    if route_origin in {"planned", "homepage"}:
        return False
    return None


def _record_sample_hint(record: ContentRecord) -> bool | None:
    authoritative_hint = _authoritative_sample_hint(record)
    if authoritative_hint is not None:
        return authoritative_hint
    candidates = (
        _factory_parser_trace(record).get("is_sample"),
        _normalized_mapping(record.trace.get("crawl_execution")).get("is_sample"),
        record.trace.get("is_sample"),
        record.metadata.get("is_sample"),
        record.metadata.get("sample"),
        record.evidence_ref.get("is_sample"),
        record.evidence_ref.get("sample"),
    )
    for candidate in candidates:
        normalized = _normalized_bool(candidate)
        if normalized is not None:
            return normalized
    return None


def _record_provenance(record: ContentRecord) -> dict[str, Any]:
    provenance: dict[str, Any] = {}
    crawl_trace = _factory_parser_crawl_trace(record)
    authoritative_provenance = {
        key: value
        for key, value in _provenance_payload(crawl_trace).items()
        if key in AUTHORITATIVE_PROVENANCE_KEYS
    }
    for payload in (
        _factory_parser_trace(record),
        _normalized_mapping(record.trace.get("document_queue")),
        _normalized_mapping(record.metadata.get("document_queue")),
        _normalized_mapping(record.metadata.get("attachment_provenance")),
        _normalized_mapping(record.evidence_ref.get("attachment_provenance")),
    ):
        provenance.update(_provenance_payload(payload))
    provenance.update(authoritative_provenance)
    return provenance


def _lead_family_for_record(record: ContentRecord) -> str:
    taxonomy = _normalized_mapping(record.trace.get("page_signal_taxonomy"))
    lead_family = _normalized_string(taxonomy.get("lead_family")).lower()
    if lead_family and lead_family != "unknown":
        return lead_family
    inferred = _normalized_string(infer_lead_type_from_record(record)).lower()
    if inferred and inferred != "unknown":
        return inferred
    route_family = _normalized_string(taxonomy.get("route_family")).lower() or _resolve_route_family(record)
    if route_family in lead_family_keywords:
        return route_family
    return ""


def _is_relevant_record(record: ContentRecord) -> bool:
    return record.fetch_status == "success" and record.relevance_label in {"maybe_relevant", "likely_relevant"}


def _is_lead_candidate_record(record: ContentRecord) -> bool:
    if record.fetch_status != "success":
        return False
    if _is_relevant_record(record):
        return True
    lead_family = _lead_family_for_record(record)
    if lead_family in lead_family_keywords:
        return True
    route_family = _resolve_route_family(record)
    if route_family in lead_family_keywords:
        return True
    return _source_kind(record) == "document" and record.section_guess == "documents"


def _sample_flags(records: list[ContentRecord], *, sample_baseline: int) -> dict[str, bool]:
    remaining_sample_pages = max(sample_baseline, 0)
    flags: dict[str, bool] = {}
    for record in records:
        fingerprint = _fingerprint_for_record(record)
        explicit_hint = _record_sample_hint(record)
        if explicit_hint is not None:
            flags[fingerprint] = explicit_hint
            continue
        if _source_kind(record) != "page":
            flags[fingerprint] = False
            continue
        if remaining_sample_pages > 0:
            flags[fingerprint] = True
            remaining_sample_pages -= 1
            continue
        flags[fingerprint] = False
    return flags


def _is_non_sample_record(record: ContentRecord, *, sample_flags: dict[str, bool]) -> bool:
    authoritative_hint = _authoritative_sample_hint(record)
    if authoritative_hint is not None:
        return not authoritative_hint
    fingerprint = _fingerprint_for_record(record)
    if sample_flags.get(fingerprint, False):
        return False
    if _source_kind(record) == "document":
        return True
    provenance = _record_provenance(record)
    route_origin = _normalized_string(provenance.get("route_origin")).lower()
    if route_origin == "sample":
        return False
    if route_origin in {"planned", "homepage"}:
        return True
    if any(provenance.get(key) for key in ("source_page", "discovery_source", "status", "skip_reason")):
        return True
    route_family = _normalized_string(provenance.get("route_family")).lower() or _resolve_route_family(record)
    return route_family in NON_SAMPLE_ROUTE_FAMILIES


def _lead_evidence_item(
    record: ContentRecord,
    *,
    sample_flags: dict[str, bool],
) -> dict[str, Any]:
    taxonomy = _normalized_mapping(record.trace.get("page_signal_taxonomy"))
    route_family = _normalized_string(taxonomy.get("route_family")).lower() or _resolve_route_family(record)
    lead_family = _normalized_string(taxonomy.get("lead_family")).lower() or _lead_family_for_record(record)
    provenance = _record_provenance(record)
    source_kind = _source_kind(record)
    return {
        "fingerprint": _fingerprint_for_record(record),
        "url": record.url or record.source_url_or_file,
        "title": record.title,
        "source_kind": source_kind,
        "source_type": record.source_type,
        "route_family": route_family,
        "lead_family": lead_family,
        "section_guess": record.section_guess,
        "relevance_label": record.relevance_label,
        "relevance_score": round(float(record.relevance_score or 0.0), 3),
        "is_sample": sample_flags.get(_fingerprint_for_record(record), False),
        "is_non_sample": _is_non_sample_record(record, sample_flags=sample_flags),
        "evidence_kind": _normalized_string(record.evidence_ref.get("kind")),
        "provenance": provenance,
    }


def _record_haystack(record: ContentRecord, *, limit: int = 3000) -> str:
    return f"{record.url} {record.title} {record.section_guess} {record.cleaned_text[:limit]}".lower()


def _matched_keywords(haystack: str, keywords: dict[str, int]) -> list[str]:
    return [keyword for keyword in keywords if keyword in haystack]


def _resolve_route_family(record: ContentRecord) -> str:
    taxonomy = record.trace.get("page_signal_taxonomy")
    if isinstance(taxonomy, dict):
        route_family = str(taxonomy.get("route_family", "") or "").strip().lower()
        if route_family:
            return route_family
    return route_family_for_section(guess_section_from_url(record.url) or record.section_guess)


def _lead_family_matches(haystack: str) -> dict[str, list[str]]:
    return {
        family_name: _matched_keywords(haystack, keywords)
        for family_name, keywords in lead_family_keywords.items()
    }


def _infer_lead_family(record: ContentRecord, haystack: str, route_family: str) -> tuple[str, dict[str, list[str]]]:
    family_matches = _lead_family_matches(haystack)
    best_family = "unknown"
    best_score = 0.0
    for family_name, matches in family_matches.items():
        score = sum(float(lead_family_keywords[family_name][keyword]) for keyword in matches)
        if route_family == family_name and score > 0.0:
            score += 1.2
        if score > best_score:
            best_family = family_name
            best_score = score
    if best_family == "unknown" and route_family in lead_family_keywords:
        best_family = route_family
    if best_family == "unknown" and record.section_guess == "documents":
        best_family = "document"
    if best_family == "unknown" and record.section_guess == "news":
        best_family = "news"
    return best_family, family_matches


def _page_signal_taxonomy(record: ContentRecord, haystack: str) -> dict[str, object]:
    route_family = _resolve_route_family(record)
    lead_family, family_matches = _infer_lead_family(record, haystack, route_family)
    matched_identity_keywords = _matched_keywords(haystack, site_identity_keywords)
    corporate_route_hint_match = route_supports_site_identity(
        section_name=record.section_guess,
        route_family=route_family,
    )
    return {
        "route_family": route_family,
        "lead_family": lead_family,
        "site_identity_match": bool(matched_identity_keywords or corporate_route_hint_match),
        "corporate_route_hint_match": corporate_route_hint_match,
        "matched_identity_keywords": matched_identity_keywords[:8],
        "matched_lead_keywords": {
            family_name: matches[:8]
            for family_name, matches in family_matches.items()
            if matches
        },
    }


def _store_page_signal_taxonomy(record: ContentRecord, taxonomy: dict[str, object]) -> None:
    record.trace.setdefault("page_signal_taxonomy", {})
    record.trace["page_signal_taxonomy"].update(taxonomy)


def classify_content_record(record: ContentRecord) -> ContentRecord:
    haystack = _record_haystack(record)
    taxonomy = _page_signal_taxonomy(record, haystack)
    _store_page_signal_taxonomy(record, taxonomy)

    if record.fetch_status != "success" or not record.cleaned_text:
        record.relevance_label = "irrelevant"
        record.relevance_score = 0.0
        record.relevance_reasons = [f"fetch_status={record.fetch_status}"]
        return record

    score = 0.0
    reasons: list[str] = []
    strong_signal_count = 0

    matched_lead_keywords = taxonomy.get("matched_lead_keywords", {})
    if isinstance(matched_lead_keywords, dict):
        for family_name, family_hits in matched_lead_keywords.items():
            if not isinstance(family_hits, list):
                continue
            for keyword in family_hits:
                score += float(lead_family_keywords[family_name][keyword])
                reasons.append(f"family:{family_name}:{keyword}")
                if keyword in STRONG_SIGNAL_KEYWORDS:
                    strong_signal_count += 1

    matched_identity_keywords = taxonomy.get("matched_identity_keywords", [])
    if isinstance(matched_identity_keywords, list):
        for keyword in matched_identity_keywords:
            score += float(site_identity_keywords[keyword]) * 0.5
            reasons.append(f"identity:{keyword}")

    route_family = str(taxonomy.get("route_family", "") or "")
    if route_family in lead_family_keywords:
        score += 1.0
        reasons.append(f"route_family:{route_family}")

    for keyword, weight in LEAD_NEGATIVE_KEYWORDS.items():
        if keyword in haystack:
            score -= float(weight)
            reasons.append(f"negative:{keyword}")

    if record.section_guess in {"procurement", "tenders", "documents"}:
        score += 1.5
        reasons.append(f"section:{record.section_guess}")
    elif record.section_guess == "news":
        score += 0.5
        reasons.append("section:news")

    if re.search(r"\bлот\b|\bизвещени", haystack):
        score += 1.5
        reasons.append("lot_or_notice")
        strong_signal_count += 1
    if re.search(r"\b\d{2}[./-]\d{2}[./-]\d{4}\b", haystack):
        score += 0.8
        reasons.append("explicit_date")
    if re.search(r"№\s*\d+|номер процедуры|запрос предложений|аукцион", haystack):
        score += 1.2
        reasons.append("procedure_pattern")
        strong_signal_count += 1
    if any(token in haystack for token in ("о компании", "история", "структура", "контакты")):
        score -= 1.5
        reasons.append("generic_corporate_page")

    normalized_score = round(max(min(score / 10.0, 1.0), 0.0), 3)
    if record.section_guess in {"about", "homepage", "news", "contacts"} and strong_signal_count == 0:
        normalized_score = min(normalized_score, 0.25)
        reasons.append("section_penalty_noncommercial")

    if normalized_score >= 0.7:
        label = "likely_relevant"
    elif normalized_score >= 0.35:
        label = "maybe_relevant"
    else:
        label = "irrelevant"

    lead_family = str(taxonomy.get("lead_family", "") or "")
    if lead_family and lead_family != "unknown":
        reasons.append(f"lead_family:{lead_family}")
    if taxonomy.get("site_identity_match"):
        reasons.append("site_identity_match")

    record.relevance_label = label
    record.relevance_score = normalized_score
    record.relevance_reasons = dedupe_preserve_order(reasons)[:8]
    return record


def classify_content_records(records: Iterable[ContentRecord]) -> list[ContentRecord]:
    classified_records = list(records)
    for record in classified_records:
        classify_content_record(record)
    return classified_records


def summarize_record_set(records: Iterable[ContentRecord], *, sample_baseline: int = 2) -> dict[str, Any]:
    classified_records = classify_content_records(records)
    sample_flags = _sample_flags(classified_records, sample_baseline=sample_baseline)
    relevant_records = [record for record in classified_records if _is_relevant_record(record)]
    lead_candidate_records = [record for record in classified_records if _is_lead_candidate_record(record)]

    lead_evidence = [
        _lead_evidence_item(record, sample_flags=sample_flags)
        for record in lead_candidate_records
    ]
    lead_evidence.sort(
        key=lambda item: (
            0 if item["is_non_sample"] else 1,
            -float(item["relevance_score"]),
            item["fingerprint"],
        )
    )

    lead_families = dedupe_preserve_order(
        item["lead_family"]
        for item in lead_evidence
        if _normalized_string(item.get("lead_family"))
    )
    relevant_record_fingerprints = dedupe_preserve_order(
        _fingerprint_for_record(record)
        for record in (relevant_records or lead_candidate_records)
    )
    non_sample_record_fingerprints = dedupe_preserve_order(
        _fingerprint_for_record(record)
        for record in classified_records
        if _is_non_sample_record(record, sample_flags=sample_flags)
    )
    route_families = dedupe_preserve_order(
        _resolve_route_family(record)
        for record in classified_records
        if _resolve_route_family(record)
    )

    return {
        "record_count": len(classified_records),
        "page_records": sum(1 for record in classified_records if _source_kind(record) == "page"),
        "document_records": sum(1 for record in classified_records if _source_kind(record) == "document"),
        "sample_baseline": max(sample_baseline, 0),
        "relevant_record_fingerprints": relevant_record_fingerprints,
        "non_sample_record_fingerprints": non_sample_record_fingerprints,
        "non_sample_evidence_count": sum(1 for item in lead_evidence if item["is_non_sample"]),
        "lead_families": lead_families,
        "lead_evidence": lead_evidence,
        "route_families": route_families,
    }


def infer_lead_type_from_record(record: ContentRecord) -> str:
    haystack = _record_haystack(record, limit=1000)
    taxonomy = _page_signal_taxonomy(record, haystack)
    _store_page_signal_taxonomy(record, taxonomy)
    lead_family = str(taxonomy.get("lead_family", "unknown") or "unknown")
    if lead_family != "unknown":
        return lead_family
    if record.section_guess == "documents":
        return "document"
    if record.section_guess == "news":
        return "news"
    return "unknown"


def should_use_llm_record_review(record: ContentRecord) -> bool:
    return record.fetch_status == "success" and record.relevance_label == "maybe_relevant" and bool(record.cleaned_text)

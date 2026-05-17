from __future__ import annotations

import re
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

from .models import DossierDocumentRecord, EvidenceReference

_CANONICAL_PROCEDURE_KEYS = (
    "procedure_type",
    "status",
    "occurred_at",
    "title",
    "source_kind",
    "evidence_refs",
    "document_refs",
    "metadata",
)


@dataclass(slots=True)
class _ReferenceIndex:
    evidence_by_url: dict[str, list[EvidenceReference]]
    evidence_by_locator: dict[str, list[EvidenceReference]]
    evidence_by_checksum: dict[str, list[EvidenceReference]]
    evidence_by_dossier_file: dict[str, list[EvidenceReference]]
    document_by_url: dict[str, list[DossierDocumentRecord]]
    document_by_locator: dict[str, list[DossierDocumentRecord]]
    document_by_checksum: dict[str, list[DossierDocumentRecord]]
    document_by_dossier_file: dict[str, list[DossierDocumentRecord]]


def build_procedure_records(
    *,
    lead_cards: list[Any],
    lead_assembly: Mapping[str, Any] | dict[str, Any],
    evidence_references: list[EvidenceReference],
    document_records: list[DossierDocumentRecord],
) -> list[dict[str, Any]]:
    index = _build_reference_index(
        evidence_references=evidence_references,
        document_records=document_records,
    )
    merged_records: dict[tuple[Any, ...], dict[str, Any]] = {}

    for lead_card in _coerce_list(lead_cards):
        record = _procedure_from_lead_card(lead_card, index=index)
        if not _has_meaningful_value(record):
            continue
        _merge_procedure_record(merged_records, record)

    for raw_item in _coerce_list(_read_field(lead_assembly, "lead_evidence")):
        record = _procedure_from_lead_evidence(raw_item, index=index)
        if not _has_meaningful_value(record):
            continue
        _merge_procedure_record(merged_records, record)

    if not merged_records and _lead_assembly_has_signal(lead_assembly):
        summary_record = _procedure_from_lead_assembly_summary(lead_assembly, index=index)
        if _has_meaningful_value(summary_record):
            _merge_procedure_record(merged_records, summary_record)

    return list(merged_records.values())


def rebind_procedure_records(
    *,
    procedure_records: list[Any],
    evidence_references: list[EvidenceReference],
    document_records: list[DossierDocumentRecord],
) -> list[dict[str, Any]]:
    index = _build_reference_index(
        evidence_references=evidence_references,
        document_records=document_records,
    )
    merged_records: dict[tuple[Any, ...], dict[str, Any]] = {}

    for raw_record in _coerce_list(procedure_records):
        record = _rebind_procedure_record(raw_record, index=index)
        if not _has_meaningful_value(record):
            continue
        _merge_procedure_record(merged_records, record)

    return list(merged_records.values())


def _procedure_from_lead_card(
    lead_card: Any,
    *,
    index: _ReferenceIndex,
) -> dict[str, Any]:
    source_urls = _normalize_string_list(_read_field(lead_card, "source_urls"))
    evidence_refs = _resolve_evidence_refs(index=index, source_urls=source_urls)
    document_refs = _resolve_document_refs(index=index, source_urls=source_urls)
    procedure_type = _normalize_procedure_type(
        _first_non_empty(
            _text_field(lead_card, "procedure_type"),
            _text_field(lead_card, "lead_type"),
        )
    )
    title = _first_non_empty(
        _text_field(lead_card, "title"),
        _text_field(lead_card, "label"),
        source_urls[0] if source_urls else "",
    )
    source_kind = _first_non_empty(
        _infer_source_kind(
            explicit_source_kind=_text_field(lead_card, "source_kind"),
            evidence_refs=evidence_refs,
            document_refs=document_refs,
        ),
        "lead_card",
    )
    metadata = _normalized_metadata(
        source="lead_card",
        payload={
            "confidence": _normalize_number(_read_field(lead_card, "confidence")),
            "deadline": _text_field(lead_card, "deadline"),
            "why_relevant": _text_field(lead_card, "why_relevant"),
            "source_urls": source_urls,
        },
    )
    record = _canonical_procedure_record(
        procedure_type=procedure_type,
        status=_normalize_status(_text_field(lead_card, "status")),
        occurred_at=_first_non_empty(
            _text_field(lead_card, "occurred_at"),
            _text_field(lead_card, "date"),
        ),
        title=title,
        source_kind=source_kind,
        evidence_refs=evidence_refs,
        document_refs=document_refs,
        metadata=metadata,
    )
    return record if _procedure_has_dossier_refs(record) else {}


def _procedure_from_lead_evidence(
    raw_item: Any,
    *,
    index: _ReferenceIndex,
) -> dict[str, Any]:
    url = _first_non_empty(_text_field(raw_item, "url"), _text_field(raw_item, "source_url"))
    fingerprint = _first_non_empty(
        _text_field(raw_item, "fingerprint"),
        _text_field(raw_item, "record_locator"),
    )
    source_kind = _normalize_source_kind(_text_field(raw_item, "source_kind"))
    evidence_refs = _resolve_evidence_refs(
        index=index,
        source_urls=[url] if url else [],
        fingerprints=[fingerprint] if fingerprint else [],
        preferred_source_kind=source_kind,
    )
    document_refs = _resolve_document_refs(
        index=index,
        source_urls=[url] if url else [],
        fingerprints=[fingerprint] if fingerprint else [],
    )
    title = _first_non_empty(
        _text_field(raw_item, "title"),
        url,
        fingerprint,
    )
    procedure_type = _normalize_procedure_type(
        _first_non_empty(
            _text_field(raw_item, "procedure_type"),
            _text_field(raw_item, "lead_family"),
            _text_field(raw_item, "route_family"),
        )
    )
    metadata = _normalized_metadata(
        source="lead_assembly",
        payload={
            "fingerprint": fingerprint,
            "url": url,
            "source_type": _text_field(raw_item, "source_type"),
            "route_family": _text_field(raw_item, "route_family"),
            "lead_family": _text_field(raw_item, "lead_family"),
            "section_guess": _text_field(raw_item, "section_guess"),
            "relevance_label": _text_field(raw_item, "relevance_label"),
            "relevance_score": _normalize_number(_read_field(raw_item, "relevance_score")),
            "is_sample": bool(_read_field(raw_item, "is_sample")),
            "is_non_sample": bool(_read_field(raw_item, "is_non_sample")),
            "evidence_kind": _text_field(raw_item, "evidence_kind"),
            "provenance": _normalize_provenance(_read_field(raw_item, "provenance")),
        },
    )
    record = _canonical_procedure_record(
        procedure_type=procedure_type,
        status=_normalize_status(_text_field(raw_item, "status"), fallback=_status_from_relevance(_text_field(raw_item, "relevance_label"))),
        occurred_at=_first_non_empty(
            _text_field(raw_item, "occurred_at"),
            _text_field(raw_item, "date"),
            _text_field(_read_field(raw_item, "provenance"), "occurred_at"),
            _text_field(_read_field(raw_item, "provenance"), "date"),
        ),
        title=title,
        source_kind=_first_non_empty(
            _infer_source_kind(
                explicit_source_kind=source_kind,
                evidence_refs=evidence_refs,
                document_refs=document_refs,
            ),
            "unknown",
        ),
        evidence_refs=evidence_refs,
        document_refs=document_refs,
        metadata=metadata,
    )
    return record if _procedure_has_dossier_refs(record) else {}


def _procedure_from_lead_assembly_summary(
    lead_assembly: Mapping[str, Any] | dict[str, Any],
    *,
    index: _ReferenceIndex,
) -> dict[str, Any]:
    relevant_fingerprints = _normalize_string_list(_read_field(lead_assembly, "relevant_record_fingerprints"))
    source_urls = _normalize_string_list(_read_field(lead_assembly, "source_urls"))
    evidence_refs = _resolve_evidence_refs(
        index=index,
        source_urls=source_urls,
        fingerprints=relevant_fingerprints,
    )
    document_refs = _resolve_document_refs(
        index=index,
        source_urls=source_urls,
        fingerprints=relevant_fingerprints,
    )
    lead_families = _normalize_string_list(_read_field(lead_assembly, "lead_families"))
    route_families = _normalize_string_list(_read_field(lead_assembly, "route_families"))
    record = _canonical_procedure_record(
        procedure_type=_normalize_procedure_type(
            _first_non_empty(
                lead_families[0] if lead_families else "",
                route_families[0] if route_families else "",
            )
        ),
        status=_normalize_status(
            _text_field(lead_assembly, "status"),
            fallback="candidate" if evidence_refs or document_refs else "unknown",
        ),
        occurred_at=_first_non_empty(
            _text_field(lead_assembly, "occurred_at"),
            _text_field(lead_assembly, "date"),
        ),
        title=_text_field(lead_assembly, "title"),
        source_kind="aggregate",
        evidence_refs=evidence_refs,
        document_refs=document_refs,
        metadata=_normalized_metadata(
            source="lead_assembly_summary",
            payload={
                "mode": _text_field(lead_assembly, "mode"),
                "record_count": _normalize_number(_read_field(lead_assembly, "record_count")),
                "page_records": _normalize_number(_read_field(lead_assembly, "page_records")),
                "document_records": _normalize_number(_read_field(lead_assembly, "document_records")),
                "lead_evidence_count": _normalize_number(_read_field(lead_assembly, "lead_evidence_count")),
                "lead_families": lead_families,
                "route_families": route_families,
                "relevant_record_fingerprints": relevant_fingerprints,
                "non_sample_record_fingerprints": _normalize_string_list(
                    _read_field(lead_assembly, "non_sample_record_fingerprints")
                ),
            },
        ),
    )
    return record if _procedure_has_dossier_refs(record) else {}


def _canonical_procedure_record(
    *,
    procedure_type: str,
    status: str,
    occurred_at: str,
    title: str,
    source_kind: str,
    evidence_refs: list[dict[str, Any]],
    document_refs: list[dict[str, Any]],
    metadata: dict[str, Any],
) -> dict[str, Any]:
    return {
        "procedure_type": _first_non_empty(procedure_type, "unknown"),
        "status": _first_non_empty(status, "unknown"),
        "occurred_at": occurred_at,
        "title": title,
        "source_kind": _first_non_empty(source_kind, "unknown"),
        "evidence_refs": _dedupe_mappings(evidence_refs, key=_evidence_ref_key),
        "document_refs": _dedupe_mappings(document_refs, key=_document_ref_key),
        "metadata": metadata,
    }


def _build_reference_index(
    *,
    evidence_references: list[EvidenceReference],
    document_records: list[DossierDocumentRecord],
) -> _ReferenceIndex:
    evidence_by_url: dict[str, list[EvidenceReference]] = {}
    evidence_by_locator: dict[str, list[EvidenceReference]] = {}
    evidence_by_checksum: dict[str, list[EvidenceReference]] = {}
    evidence_by_dossier_file: dict[str, list[EvidenceReference]] = {}
    document_by_url: dict[str, list[DossierDocumentRecord]] = {}
    document_by_locator: dict[str, list[DossierDocumentRecord]] = {}
    document_by_checksum: dict[str, list[DossierDocumentRecord]] = {}
    document_by_dossier_file: dict[str, list[DossierDocumentRecord]] = {}

    for reference in evidence_references:
        if reference.source_url:
            evidence_by_url.setdefault(reference.source_url, []).append(reference)
        if reference.checksum:
            evidence_by_checksum.setdefault(reference.checksum, []).append(reference)
        if reference.dossier_file:
            evidence_by_dossier_file.setdefault(reference.dossier_file, []).append(reference)
        for locator in _evidence_reference_locators(reference):
            evidence_by_locator.setdefault(locator, []).append(reference)

    for document in document_records:
        if document.ledger.source_url:
            document_by_url.setdefault(document.ledger.source_url, []).append(document)
        if document.ledger.checksum:
            document_by_checksum.setdefault(document.ledger.checksum, []).append(document)
        dossier_file = _first_non_empty(document.dossier_file, document.ledger.dossier_file)
        if dossier_file:
            document_by_dossier_file.setdefault(dossier_file, []).append(document)
        for locator in _document_record_locators(document):
            document_by_locator.setdefault(locator, []).append(document)

    return _ReferenceIndex(
        evidence_by_url=evidence_by_url,
        evidence_by_locator=evidence_by_locator,
        evidence_by_checksum=evidence_by_checksum,
        evidence_by_dossier_file=evidence_by_dossier_file,
        document_by_url=document_by_url,
        document_by_locator=document_by_locator,
        document_by_checksum=document_by_checksum,
        document_by_dossier_file=document_by_dossier_file,
    )


def _resolve_evidence_refs(
    *,
    index: _ReferenceIndex,
    source_urls: list[str],
    fingerprints: list[str] | None = None,
    preferred_source_kind: str = "",
) -> list[dict[str, Any]]:
    matches: list[dict[str, Any]] = []
    seen: set[tuple[str, ...]] = set()

    for fingerprint in fingerprints or []:
        for reference in index.evidence_by_locator.get(fingerprint, []):
            if preferred_source_kind and preferred_source_kind not in {"unknown", _normalize_source_kind(reference.evidence_type)}:
                continue
            normalized = _evidence_reference_to_ref(reference)
            key = _evidence_ref_key(normalized)
            if key in seen:
                continue
            seen.add(key)
            matches.append(normalized)

    for source_url in source_urls:
        for reference in index.evidence_by_url.get(source_url, []):
            if preferred_source_kind and preferred_source_kind not in {"unknown", _normalize_source_kind(reference.evidence_type)}:
                continue
            normalized = _evidence_reference_to_ref(reference)
            key = _evidence_ref_key(normalized)
            if key in seen:
                continue
            seen.add(key)
            matches.append(normalized)

    return matches


def _resolve_document_refs(
    *,
    index: _ReferenceIndex,
    source_urls: list[str],
    fingerprints: list[str] | None = None,
) -> list[dict[str, Any]]:
    matches: list[dict[str, Any]] = []
    seen: set[tuple[str, ...]] = set()

    for fingerprint in fingerprints or []:
        for document in index.document_by_locator.get(fingerprint, []):
            normalized = _document_record_to_ref(document)
            key = _document_ref_key(normalized)
            if key in seen:
                continue
            seen.add(key)
            matches.append(normalized)

    for source_url in source_urls:
        for document in index.document_by_url.get(source_url, []):
            normalized = _document_record_to_ref(document)
            key = _document_ref_key(normalized)
            if key in seen:
                continue
            seen.add(key)
            matches.append(normalized)

    return matches


def _evidence_reference_to_ref(reference: EvidenceReference) -> dict[str, Any]:
    return {
        "evidence_type": reference.evidence_type,
        "source_url": reference.source_url,
        "source_path": reference.source_path,
        "dossier_file": reference.dossier_file,
        "record_locator": _evidence_reference_locator(reference),
        "checksum": reference.checksum,
    }


def _document_record_to_ref(document: DossierDocumentRecord) -> dict[str, Any]:
    return {
        "record_locator": _document_record_locator(document),
        "source_url": document.ledger.source_url,
        "source_path": _first_non_empty(document.source_path, document.ledger.local_path),
        "dossier_file": _first_non_empty(document.dossier_file, document.ledger.dossier_file),
        "filename": document.ledger.filename,
        "checksum": document.ledger.checksum,
    }


def _document_record_locator(document: DossierDocumentRecord) -> str:
    return _first_non_empty(
        document.ledger.checksum,
        _document_content_fingerprint(document),
        document.dossier_file,
        document.ledger.dossier_file,
        document.ledger.source_url,
        document.source_path,
        document.ledger.local_path,
        document.ledger.filename,
    )


def _evidence_reference_locator(reference: EvidenceReference) -> str:
    return _first_non_empty(
        reference.checksum,
        _text_field(reference.metadata, "content_fingerprint"),
        reference.dossier_file,
        reference.record_locator,
        reference.source_url,
    )


def _document_content_fingerprint(document: DossierDocumentRecord) -> str:
    content_record_trace = _copy_mapping(_read_field(document.trace, "content_record"))
    return _first_non_empty(
        _text_field(document.metadata, "content_fingerprint"),
        _text_field(content_record_trace, "content_fingerprint"),
    )


def _evidence_reference_locators(reference: EvidenceReference) -> list[str]:
    locators: list[str] = []
    seen: set[str] = set()
    for candidate in (
        _evidence_reference_locator(reference),
        reference.record_locator,
        _text_field(reference.metadata, "content_fingerprint"),
        reference.dossier_file,
    ):
        normalized = str(candidate or "").strip()
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        locators.append(normalized)
    return locators


def _document_record_locators(document: DossierDocumentRecord) -> list[str]:
    locators: list[str] = []
    seen: set[str] = set()
    for candidate in (
        _document_record_locator(document),
        _document_content_fingerprint(document),
        document.dossier_file,
        document.ledger.dossier_file,
    ):
        normalized = str(candidate or "").strip()
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        locators.append(normalized)
    return locators


def _normalized_metadata(*, source: str, payload: Mapping[str, Any] | dict[str, Any]) -> dict[str, Any]:
    metadata = {"source": source}
    for key, value in payload.items():
        normalized = _normalize_freeform(value)
        if _has_meaningful_value(normalized):
            metadata[str(key)] = normalized
    return metadata


def _normalize_provenance(value: Any) -> dict[str, Any]:
    provenance = _copy_mapping(value)
    normalized: dict[str, Any] = {}
    for key in (
        "route_family",
        "route_origin",
        "source_page",
        "discovery_source",
        "status",
        "skip_reason",
    ):
        normalized_value = _normalize_freeform(provenance.get(key))
        if _has_meaningful_value(normalized_value):
            normalized[key] = normalized_value
    return normalized


def _normalize_procedure_type(value: str) -> str:
    normalized = value.strip().casefold()
    if not normalized:
        return "unknown"
    normalized = re.sub(r"[\s/|.-]+", "_", normalized)
    normalized = re.sub(r"_+", "_", normalized).strip("_")
    return normalized or "unknown"


def _normalize_status(value: str, *, fallback: str = "unknown") -> str:
    normalized = value.strip().casefold()
    if not normalized:
        return fallback
    aliases = {
        "active": "open",
        "in_progress": "open",
        "ongoing": "open",
        "new": "new",
        "open": "open",
        "closed": "closed",
        "completed": "closed",
        "cancelled": "cancelled",
        "canceled": "cancelled",
        "archived": "archived",
        "likely_relevant": "candidate",
        "maybe_relevant": "candidate",
        "irrelevant": "rejected",
        "rejected": "rejected",
    }
    return aliases.get(normalized, normalized)


def _status_from_relevance(value: str) -> str:
    normalized = value.strip().casefold()
    if normalized in {"likely_relevant", "maybe_relevant"}:
        return "candidate"
    if normalized in {"irrelevant", "rejected"}:
        return "rejected"
    return "unknown"


def _normalize_source_kind(value: str) -> str:
    normalized = value.strip().casefold()
    if not normalized:
        return "unknown"
    aliases = {
        "html": "page",
        "htm": "page",
        "attachment": "document",
        "file": "document",
    }
    return aliases.get(normalized, normalized)


def _infer_source_kind(
    *,
    explicit_source_kind: str,
    evidence_refs: list[dict[str, Any]],
    document_refs: list[dict[str, Any]],
) -> str:
    normalized_explicit = _normalize_source_kind(explicit_source_kind)
    if normalized_explicit != "unknown":
        return normalized_explicit
    has_document_refs = bool(document_refs)
    evidence_types = {
        _normalize_source_kind(str(item.get("evidence_type", "") or ""))
        for item in evidence_refs
        if str(item.get("evidence_type", "") or "").strip()
    }
    if has_document_refs and evidence_types == {"document"}:
        return "document"
    if has_document_refs and evidence_types:
        return "mixed"
    if has_document_refs:
        return "document"
    if evidence_types == {"page"}:
        return "page"
    if evidence_types == {"document"}:
        return "document"
    if len(evidence_types) > 1:
        return "mixed"
    return "unknown"


def _lead_assembly_has_signal(lead_assembly: Mapping[str, Any] | dict[str, Any]) -> bool:
    if not isinstance(lead_assembly, Mapping):
        return False
    return any(
        _has_meaningful_value(_read_field(lead_assembly, field_name))
        for field_name in (
            "lead_evidence",
            "lead_evidence_count",
            "lead_families",
            "route_families",
            "relevant_record_fingerprints",
            "non_sample_record_fingerprints",
        )
    )


def _procedure_has_dossier_refs(record: Mapping[str, Any]) -> bool:
    if not isinstance(record, Mapping):
        return False
    return any(
        _has_meaningful_value(record.get(field_name))
        for field_name in ("evidence_refs", "document_refs")
    )


def _rebind_procedure_record(
    raw_record: Any,
    *,
    index: _ReferenceIndex,
) -> dict[str, Any]:
    record = _canonical_procedure_record(
        procedure_type=_normalize_procedure_type(_text_field(raw_record, "procedure_type")),
        status=_normalize_status(_text_field(raw_record, "status")),
        occurred_at=_text_field(raw_record, "occurred_at"),
        title=_text_field(raw_record, "title"),
        source_kind=_normalize_source_kind(_text_field(raw_record, "source_kind")),
        evidence_refs=_rebind_evidence_refs(index=index, refs=_read_field(raw_record, "evidence_refs")),
        document_refs=_rebind_document_refs(index=index, refs=_read_field(raw_record, "document_refs")),
        metadata=_normalize_existing_metadata(_read_field(raw_record, "metadata")),
    )
    return record if _procedure_has_dossier_refs(record) else {}


def _merge_procedure_record(
    merged_records: dict[tuple[Any, ...], dict[str, Any]],
    record: dict[str, Any],
) -> None:
    record_key = _procedure_record_key(record)
    existing = merged_records.get(record_key)
    if existing is None:
        merged_records[record_key] = record
        return

    for key in _CANONICAL_PROCEDURE_KEYS:
        if key == "evidence_refs":
            existing[key] = _dedupe_mappings(
                [*existing[key], *record[key]],
                key=_evidence_ref_key,
            )
            continue
        if key == "document_refs":
            existing[key] = _dedupe_mappings(
                [*existing[key], *record[key]],
                key=_document_ref_key,
            )
            continue
        if key == "metadata":
            existing[key] = _merge_metadata(existing[key], record[key])
            continue
        if _should_replace_procedure_field(key, existing.get(key), record.get(key)):
            existing[key] = record[key]


def _merge_metadata(base: dict[str, Any], extra: dict[str, Any]) -> dict[str, Any]:
    merged = dict(base)
    for key, value in extra.items():
        if key not in merged or not _has_meaningful_value(merged[key]):
            merged[key] = value
            continue
        if isinstance(merged[key], list) and isinstance(value, list):
            merged[key] = _dedupe_values([*merged[key], *value])
            continue
        if isinstance(merged[key], Mapping) and isinstance(value, Mapping):
            nested = dict(merged[key])
            for nested_key, nested_value in value.items():
                if nested_key not in nested or not _has_meaningful_value(nested[nested_key]):
                    nested[nested_key] = nested_value
            merged[key] = nested
    return merged


def _should_replace_procedure_field(field_name: str, existing_value: Any, new_value: Any) -> bool:
    if not _has_meaningful_value(new_value):
        return False
    if field_name in {"title", "occurred_at"}:
        return not _has_meaningful_value(existing_value)
    if field_name == "procedure_type":
        return str(existing_value or "") in {"", "unknown"} and str(new_value or "") not in {"", "unknown"}
    if field_name == "status":
        return str(existing_value or "") in {"", "unknown", "new"} and str(new_value or "") not in {"", "unknown", "new"}
    if field_name == "source_kind":
        return str(existing_value or "") in {"", "unknown", "lead_card"} and str(new_value or "") not in {
            "",
            "unknown",
            "lead_card",
        }
    return not _has_meaningful_value(existing_value)


def _procedure_record_key(record: dict[str, Any]) -> tuple[Any, ...]:
    evidence_keys = tuple(sorted(_evidence_ref_key(item) for item in record.get("evidence_refs", [])))
    document_keys = tuple(sorted(_document_ref_key(item) for item in record.get("document_refs", [])))
    if evidence_keys or document_keys:
        return (evidence_keys, document_keys)
    return (
        record.get("procedure_type", ""),
        record.get("source_kind", ""),
    )


def _evidence_ref_key(value: Mapping[str, Any]) -> tuple[str, ...]:
    return (
        str(value.get("evidence_type", "") or ""),
        str(value.get("dossier_file", "") or ""),
        str(value.get("record_locator", "") or ""),
        str(value.get("checksum", "") or ""),
        str(value.get("source_url", "") or ""),
    )


def _document_ref_key(value: Mapping[str, Any]) -> tuple[str, ...]:
    return (
        str(value.get("dossier_file", "") or ""),
        str(value.get("source_url", "") or ""),
        str(value.get("record_locator", "") or ""),
        str(value.get("checksum", "") or ""),
        str(value.get("filename", "") or ""),
    )


def _dedupe_mappings(
    values: list[dict[str, Any]],
    *,
    key: Any,
) -> list[dict[str, Any]]:
    deduped: list[dict[str, Any]] = []
    seen: set[Any] = set()
    for value in values:
        mapping_key = key(value)
        if mapping_key in seen:
            continue
        seen.add(mapping_key)
        deduped.append(value)
    return deduped


def _dedupe_values(values: list[Any]) -> list[Any]:
    deduped: list[Any] = []
    seen: set[str] = set()
    for value in values:
        marker = repr(value)
        if marker in seen:
            continue
        seen.add(marker)
        deduped.append(value)
    return deduped


def _rebind_evidence_refs(
    *,
    index: _ReferenceIndex,
    refs: Any,
) -> list[dict[str, Any]]:
    rebound: list[dict[str, Any]] = []
    seen: set[tuple[str, ...]] = set()
    for raw_ref in _coerce_list(refs):
        for reference in _match_evidence_references(index=index, raw_ref=raw_ref):
            normalized = _evidence_reference_to_ref(reference)
            key = _evidence_ref_key(normalized)
            if key in seen:
                continue
            seen.add(key)
            rebound.append(normalized)
    return rebound


def _rebind_document_refs(
    *,
    index: _ReferenceIndex,
    refs: Any,
) -> list[dict[str, Any]]:
    rebound: list[dict[str, Any]] = []
    seen: set[tuple[str, ...]] = set()
    for raw_ref in _coerce_list(refs):
        for document in _match_document_records(index=index, raw_ref=raw_ref):
            normalized = _document_record_to_ref(document)
            key = _document_ref_key(normalized)
            if key in seen:
                continue
            seen.add(key)
            rebound.append(normalized)
    return rebound


def _match_evidence_references(
    *,
    index: _ReferenceIndex,
    raw_ref: Any,
) -> list[EvidenceReference]:
    mapping = _copy_mapping(raw_ref)
    for matches in (
        _stable_evidence_matches(index.evidence_by_dossier_file.get(_text_field(mapping, "dossier_file"), [])),
        _stable_evidence_matches(
            reference
            for locator in _evidence_ref_lookup_locators(mapping)
            for reference in index.evidence_by_locator.get(locator, [])
        ),
        _stable_evidence_matches(index.evidence_by_checksum.get(_text_field(mapping, "checksum"), [])),
        _stable_evidence_matches(index.evidence_by_url.get(_text_field(mapping, "source_url"), [])),
    ):
        tier_resolved, resolved_matches = _resolve_unique_tier_match(matches)
        if tier_resolved:
            return resolved_matches
    return []


def _match_document_records(
    *,
    index: _ReferenceIndex,
    raw_ref: Any,
) -> list[DossierDocumentRecord]:
    mapping = _copy_mapping(raw_ref)
    for matches in (
        _stable_document_matches(index.document_by_dossier_file.get(_text_field(mapping, "dossier_file"), [])),
        _stable_document_matches(
            document
            for locator in _document_ref_lookup_locators(mapping)
            for document in index.document_by_locator.get(locator, [])
        ),
        _stable_document_matches(index.document_by_checksum.get(_text_field(mapping, "checksum"), [])),
        _stable_document_matches(index.document_by_url.get(_text_field(mapping, "source_url"), [])),
    ):
        tier_resolved, resolved_matches = _resolve_unique_tier_match(matches)
        if tier_resolved:
            return resolved_matches
    return []


def _resolve_unique_tier_match(matches: list[Any]) -> tuple[bool, list[Any]]:
    if not matches:
        return False, []
    if len(matches) == 1:
        return True, matches
    return True, []


def _stable_evidence_matches(references: Any) -> list[EvidenceReference]:
    matches: list[EvidenceReference] = []
    seen: set[tuple[str, ...]] = set()
    for reference in references:
        _append_evidence_match(matches, seen, reference)
    return sorted(matches, key=lambda item: _evidence_ref_key(_evidence_reference_to_ref(item)))


def _stable_document_matches(documents: Any) -> list[DossierDocumentRecord]:
    matches: list[DossierDocumentRecord] = []
    seen: set[tuple[str, ...]] = set()
    for document in documents:
        _append_document_match(matches, seen, document)
    return sorted(matches, key=lambda item: _document_ref_key(_document_record_to_ref(item)))


def _append_evidence_match(
    matches: list[EvidenceReference],
    seen: set[tuple[str, ...]],
    reference: EvidenceReference,
) -> None:
    normalized = _evidence_reference_to_ref(reference)
    key = _evidence_ref_key(normalized)
    if key in seen:
        return
    seen.add(key)
    matches.append(reference)


def _append_document_match(
    matches: list[DossierDocumentRecord],
    seen: set[tuple[str, ...]],
    document: DossierDocumentRecord,
) -> None:
    normalized = _document_record_to_ref(document)
    key = _document_ref_key(normalized)
    if key in seen:
        return
    seen.add(key)
    matches.append(document)


def _evidence_ref_lookup_locators(value: Mapping[str, Any]) -> list[str]:
    locators: list[str] = []
    seen: set[str] = set()
    weak_aliases = {
        _text_field(value, "checksum"),
        _text_field(value, "source_url"),
        _text_field(value, "dossier_file"),
    }
    for candidate in (
        _text_field(value, "record_locator"),
    ):
        normalized = candidate.strip()
        if not normalized or normalized in seen or normalized in weak_aliases:
            continue
        seen.add(normalized)
        locators.append(normalized)
    return locators


def _document_ref_lookup_locators(value: Mapping[str, Any]) -> list[str]:
    locators: list[str] = []
    seen: set[str] = set()
    weak_aliases = {
        _text_field(value, "checksum"),
        _text_field(value, "source_url"),
        _text_field(value, "dossier_file"),
    }
    for candidate in (
        _text_field(value, "record_locator"),
    ):
        normalized = candidate.strip()
        if not normalized or normalized in seen or normalized in weak_aliases:
            continue
        seen.add(normalized)
        locators.append(normalized)
    return locators


def _normalize_existing_metadata(value: Any) -> dict[str, Any]:
    normalized = _normalize_freeform(_copy_mapping(value))
    if not isinstance(normalized, Mapping):
        return {}
    return {str(key): normalized[key] for key in normalized}


def _normalize_freeform(value: Any) -> Any:
    if value is None or isinstance(value, (bool, int, float, str)):
        return value
    if isinstance(value, Mapping):
        normalized = {
            str(key): _normalize_freeform(item)
            for key, item in value.items()
        }
        return {
            key: item
            for key, item in normalized.items()
            if _has_meaningful_value(item)
        }
    if isinstance(value, (list, tuple, set, frozenset)):
        normalized_list = [_normalize_freeform(item) for item in value]
        return [item for item in normalized_list if _has_meaningful_value(item)]
    return str(value)


def _normalize_number(value: Any) -> int | float | None:
    if isinstance(value, bool) or value in (None, ""):
        return None
    if isinstance(value, (int, float)):
        return value
    text = str(value).strip()
    if not text:
        return None
    try:
        if "." in text:
            return float(text)
        return int(text)
    except ValueError:
        return None


def _normalize_string_list(value: Any) -> list[str]:
    normalized: list[str] = []
    seen: set[str] = set()
    for item in _coerce_list(value):
        text = str(item or "").strip()
        if not text or text in seen:
            continue
        seen.add(text)
        normalized.append(text)
    return normalized


def _coerce_list(value: Any) -> list[Any]:
    if value is None:
        return []
    if isinstance(value, list):
        return list(value)
    if isinstance(value, tuple):
        return list(value)
    return [value]


def _copy_mapping(value: Any) -> dict[str, Any]:
    if not isinstance(value, Mapping):
        return {}
    return {str(key): value[key] for key in value}


def _read_field(payload: Any, field_name: str, default: Any = None) -> Any:
    if isinstance(payload, Mapping):
        return payload.get(field_name, default)
    return getattr(payload, field_name, default)


def _text_field(payload: Any, field_name: str) -> str:
    return str(_read_field(payload, field_name, "") or "")


def _first_non_empty(*values: Any) -> str:
    for value in values:
        text = str(value or "")
        if text:
            return text
    return ""


def _has_meaningful_value(value: Any) -> bool:
    if value is None:
        return False
    if isinstance(value, str):
        return bool(value.strip())
    if isinstance(value, Mapping):
        return any(_has_meaningful_value(item) for item in value.values())
    if isinstance(value, list):
        return any(_has_meaningful_value(item) for item in value)
    if isinstance(value, tuple):
        return any(_has_meaningful_value(item) for item in value)
    return True

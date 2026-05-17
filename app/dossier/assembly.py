from __future__ import annotations

import re
from collections.abc import Mapping
from dataclasses import dataclass, fields, is_dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any, Callable

from .models import (
    CompanyDossier,
    DossierAttachmentLedgerEntry,
    DossierDocumentRecord,
    DossierPageRecord,
    EvidenceReference,
    ExtractedEntity,
)

if TYPE_CHECKING:
    from app.documents import AttachmentRecord, NormalizedContentRecord


_EMAIL_RE = re.compile(r"(?<![\w.+-])([A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,})(?![\w.-])", re.IGNORECASE)
_PHONE_RE = re.compile(r"(?<!\w)((?:\+7|8)[\d\s()\-]{9,}\d)")
_INN_RE = re.compile(r"\bинн\b\s*[:№]?\s*(\d{10}|\d{12})", re.IGNORECASE)
_KPP_RE = re.compile(r"\bкпп\b\s*[:№]?\s*(\d{9})", re.IGNORECASE)
_OGRN_RE = re.compile(r"\bогрн\b\s*[:№]?\s*(\d{13}|\d{15})", re.IGNORECASE)
_LEGAL_NAME_RE = re.compile(
    r"\b(?:ООО|АО|ПАО|ЗАО|ОАО|НАО|ИП|ФГУП|ГУП|МУП)\s+(?:[\"«][^\"»]{2,120}[\"»]|"
    r"[A-ZА-ЯЁ0-9][A-ZА-ЯЁа-яё0-9().\"«»-]*(?:\s+[A-ZА-ЯЁ0-9][A-ZА-ЯЁа-яё0-9().\"«»-]*){0,5})"
)
_ADDRESS_LABEL_RE = re.compile(
    r"(?:(?:юридический|почтовый|фактический)\s+адрес|адрес\s+производственной\s+площадки|"
    r"адрес\s+производства|адрес\s+площадки|адрес\s+склада|местонахождение|место\s+нахождения|"
    r"адрес|"
    r"производственная\s+площадка)\s*[:\-]\s*",
    re.IGNORECASE,
)
_ADDRESS_FIELD_STOP_RE = re.compile(r"\b(?:email|e-mail|телефон|контакты|инн|кпп|огрн)\b", re.IGNORECASE)
_ADDRESS_SENTENCE_BREAK_RE = re.compile(r"[.;](?=\s+[А-ЯЁA-Z])")
_ADDRESS_ABBREVIATIONS = frozenset(
    {
        "г", "гор", "обл", "ул", "пер", "пр", "пр-кт", "просп", "д", "стр", "соор",
        "корп", "кв", "оф", "тер", "пос", "пгт", "наб", "ш", "бул", "пл", "мкр", "р-н",
    }
)
_ADDRESS_CONTINUATION_WORDS = frozenset(
    {
        "г", "город", "ул", "улица", "д", "дом", "корп", "корпус", "стр", "строение", "оф",
        "офис", "тер", "территория", "пос", "поселок", "пр", "проспект", "пр-кт", "пер",
        "переулок", "наб", "набережная", "ш", "шоссе", "пл", "площадь", "мкр", "микрорайон",
        "р-н", "район",
    }
)
_ADDRESS_MAX_CHARS = 260
_JOB_TITLE_LINE_RE = re.compile(r"(?:должность|позиция)\s*[:\-]\s*([^\n\r,]{3,80})", re.IGNORECASE)
_JOB_TITLE_PATTERN = (
    r"генеральный директор|исполнительный директор|коммерческий директор|технический директор|"
    r"директор по закупкам|директор по снабжению|директор|руководитель отдела закупок|"
    r"руководитель отдела снабжения|начальник отдела закупок|начальник отдела снабжения|"
    r"менеджер по закупкам|менеджер по снабжению|менеджер по продажам|контактное лицо|руководитель"
)
_NAME_RE = r"[А-ЯЁ][а-яё]+(?:\s+[А-ЯЁ][а-яё]+){1,2}"
_NAME_WITH_TITLE_PATTERNS = (
    re.compile(rf"(?P<title>{_JOB_TITLE_PATTERN})\s*[:,-]?\s*(?P<name>{_NAME_RE})", re.IGNORECASE),
    re.compile(rf"(?P<name>{_NAME_RE})\s*[,;-]\s*(?P<title>{_JOB_TITLE_PATTERN})", re.IGNORECASE),
    re.compile(rf"(?:контактное лицо|contact person)\s*[:,-]?\s*(?P<name>{_NAME_RE})", re.IGNORECASE),
)
_SIGNAL_KEYWORDS: dict[str, tuple[str, ...]] = {
    "product_signal": ("продукц", "ассортимент", "каталог", "товар", "издел", "номенклатур"),
    "production_signal": ("производств", "завод", "цех", "мощност", "изготавлива", "выпускает"),
    "surplus_signal": (
        "неликвид", "излиш", "складск", "остатк", "сортовая продажа", "tmc", "mtr", "тмц",
        "мтр", "распродажа", "демонтаж", "лом",
    ),
    "procurement_signal": (
        "тендер", "закуп", "конкурс", "аукцион", "223-фз", "44-фз", "коммерческое предложение",
    ),
}

@dataclass(slots=True)
class _SourceMaterial:
    text: str
    evidence: EvidenceReference


def build_company_dossier(
    *,
    company_id: str = "",
    company_name: str = "",
    site_url: str = "",
    company: Any | None = None,
    company_result: Any | None = None,
    result: Any | None = None,
    page_records: list["NormalizedContentRecord"] | None = None,
    attachment_records: list["AttachmentRecord"] | None = None,
    content_records: list["NormalizedContentRecord"] | None = None,
    document_records: list["NormalizedContentRecord"] | None = None,
    records: list["NormalizedContentRecord"] | None = None,
    attachments: list["AttachmentRecord"] | None = None,
    documents: list["NormalizedContentRecord"] | None = None,
    lead_cards: list[Any] | None = None,
    site_refresh_plans: list[Any] | None = None,
    okved_profile: Any | None = None,
    okved_site_match: Any | None = None,
    okved_matches: list[Any] | None = None,
    company_metadata: dict[str, Any] | None = None,
) -> CompanyDossier:
    result_sources = _resolve_result_sources(result, company_result)
    company_source = _resolve_company_source(company, result_sources)
    profile = _resolve_profile(result_sources)
    profile_summary = _read_field(profile, "summary")
    profile_sites = _read_field(profile, "sites")
    resolved_lead_assembly = _first_mapping(*[_read_field(source, "lead_assembly") for source in result_sources])
    resolved_lead_cards: list[Any] = []
    for collection in (lead_cards, *[_read_field(source, "lead_cards") for source in result_sources]):
        resolved_lead_cards.extend(_coerce_list(collection))

    try:
        from .procedures import build_procedure_records
    except ImportError as exc:  # pragma: no cover - integration guard until the normalizer module lands
        raise RuntimeError(
            "app.dossier.procedures.build_procedure_records is required for dossier procedure assembly"
        ) from exc

    merged_content_records = _merge_unique_items(
        content_records,
        records,
        *[_read_field(source, "content_records") for source in result_sources],
        key=_content_record_key,
    )
    resolved_page_records = _merge_unique_items(
        page_records,
        [record for record in merged_content_records if _content_record_source_kind(record) == "page"],
        key=_content_record_key,
    )
    resolved_document_records = _merge_unique_items(
        document_records,
        documents,
        [record for record in merged_content_records if _content_record_source_kind(record) == "document"],
        key=_content_record_key,
    )
    resolved_attachment_records = _merge_unique_items(
        attachment_records,
        attachments,
        *[_read_field(source, "attachment_records") for source in result_sources],
        *[_read_field(source, "attachments") for source in result_sources],
        key=_attachment_record_key,
    )

    dossier_pages = [_page_record_from_input(record) for record in resolved_page_records]
    dossier_documents = _assemble_document_records(resolved_attachment_records, resolved_document_records)
    evidence_references = _build_evidence_references(dossier_pages, dossier_documents)

    return CompanyDossier(
        company_id=_first_non_empty(
            company_id,
            _text_field(company_source, "company_id"),
            _text_field(company_source, "inn"),
            _text_field(profile_summary, "inn"),
            *[_text_field(source, "company_id") for source in result_sources],
            *[_text_field(source, "inn") for source in result_sources],
        ),
        company_name=_first_non_empty(
            company_name,
            _text_field(company_source, "company_name"),
            _text_field(profile_summary, "company_name"),
            *[_text_field(source, "company_name") for source in result_sources],
        ),
        site_url=_first_non_empty(
            site_url,
            _text_field(company_source, "input_site"),
            _text_field(company_source, "site_url"),
            _text_field(profile_sites, "best_site"),
            _text_field(profile_sites, "primary_domain"),
            *[_text_field(source, "input_site") for source in result_sources],
            *[_text_field(source, "site_url") for source in result_sources],
        ),
        page_records=dossier_pages,
        document_records=dossier_documents,
        attachment_ledger=[document.ledger for document in dossier_documents],
        extracted_entities=_extract_entities(dossier_pages, dossier_documents),
        evidence_references=evidence_references,
        procedure_records=build_procedure_records(
            lead_cards=resolved_lead_cards,
            lead_assembly=resolved_lead_assembly,
            evidence_references=evidence_references,
            document_records=dossier_documents,
        ),
        history=_build_history_entries(
            result_sources=result_sources,
            lead_assembly=resolved_lead_assembly,
            relevance_summary=_first_mapping(*[_read_field(source, "relevance_summary") for source in result_sources]),
            crawl_execution=_first_mapping(*[_read_field(source, "crawl_execution") for source in result_sources]),
            visited_route_families=_merge_string_lists(*[_read_field(source, "visited_route_families") for source in result_sources]),
            notes=_merge_string_lists(*[_read_field(source, "notes") for source in result_sources]),
            status=_first_non_empty(*[_text_field(source, "status") for source in result_sources]),
            started_at=_first_non_empty(*[_text_field(source, "started_at") for source in result_sources]),
            finished_at=_first_non_empty(*[_text_field(source, "finished_at") for source in result_sources]),
        ),
        reprocess_inputs=_build_reprocess_inputs(
            site_refresh_plans=_merge_unique_items(
                site_refresh_plans,
                *[_read_field(source, "site_refresh_plans") for source in result_sources],
                key=_site_refresh_plan_key,
            ),
            plans=_merge_unique_items(*[_read_field(source, "plans") for source in result_sources], key=_planner_plan_key),
        ),
        okved_site_match=_first_not_none(
            okved_site_match,
            *[_read_field(source, "okved_site_match") for source in result_sources],
            _read_field(okved_profile, "site_match"),
        ),
        okved_matches=list(
            _merge_unique_items(
                okved_matches,
                *[_read_field(source, "okved_matches") for source in result_sources],
                key=_okved_match_key,
            )
        ),
        company_metadata=_build_company_metadata(
            explicit_metadata=company_metadata,
            company_source=company_source,
            result_sources=result_sources,
            profile=profile,
            okved_profile=okved_profile,
            lead_assembly=resolved_lead_assembly,
            relevance_summary=_first_mapping(*[_read_field(source, "relevance_summary") for source in result_sources]),
            crawl_execution=_first_mapping(*[_read_field(source, "crawl_execution") for source in result_sources]),
            visited_route_families=_merge_string_lists(*[_read_field(source, "visited_route_families") for source in result_sources]),
        ),
    )


def _page_record_from_input(record: Any) -> DossierPageRecord:
    source_url = _first_non_empty(
        _text_field(record, "url"),
        _text_field(record, "source_url_or_file"),
        _text_field(record, "site_url"),
    )
    return DossierPageRecord(
        source_url=source_url,
        site_url=_text_field(record, "site_url"),
        source_type=_text_field(record, "source_type"),
        title=_text_field(record, "title"),
        section_guess=_text_field(record, "section_guess"),
        date=_text_field(record, "date"),
        text=_text_field(record, "text"),
        raw_text=_text_field(record, "raw_text"),
        cleaned_text=_text_field(record, "cleaned_text"),
        tables=_copy_tables(_read_field(record, "tables")),
        content_fingerprint=_text_field(record, "content_fingerprint"),
        fetch_status=_text_field(record, "fetch_status"),
        relevance_label=_first_non_empty(_text_field(record, "relevance_label"), "unknown"),
        relevance_score=_float_or_default(_read_field(record, "relevance_score"), default=0.0),
        metadata=_copy_mapping(_read_field(record, "metadata")),
        evidence_ref=_copy_mapping(_read_field(record, "evidence_ref")),
        trace=_copy_mapping(_read_field(record, "trace")),
    )


def _document_record_from_input(record: Any) -> DossierDocumentRecord:
    ledger = _read_field(record, "ledger")
    extracted = _read_field(record, "extracted")
    trace_builder = getattr(record, "to_trace", None)
    trace = trace_builder() if callable(trace_builder) else _copy_mapping(_read_field(record, "trace"))

    dossier_ledger = DossierAttachmentLedgerEntry(
        source_url=_text_field(ledger, "source_url"),
        referrer_url=_text_field(ledger, "referrer_url"),
        filename=_text_field(ledger, "filename"),
        mime=_text_field(ledger, "mime"),
        size=_int_or_default(_read_field(ledger, "size"), default=0),
        checksum=_text_field(ledger, "checksum"),
        fetch_status=_text_field(ledger, "fetch_status"),
        entry_kind=_first_non_empty(_text_field(ledger, "entry_kind"), "attachment"),
        local_path=_text_field(ledger, "local_path"),
        dossier_file=_text_field(ledger, "dossier_file"),
        archive_depth=_int_or_default(_read_field(ledger, "archive_depth"), default=0),
        parent_archive_url=_text_field(ledger, "parent_archive_url"),
        warnings=_copy_list(_read_field(ledger, "warnings")),
    )
    return DossierDocumentRecord(
        ledger=dossier_ledger,
        source_path=_first_non_empty(_text_field(extracted, "source_path"), dossier_ledger.local_path),
        dossier_file=_text_field(extracted, "dossier_file"),
        source_format=_first_non_empty(_text_field(extracted, "source_format"), Path(dossier_ledger.filename).suffix.lstrip(".")),
        text=_text_field(extracted, "text"),
        tables=_copy_tables(_read_field(extracted, "tables")),
        sheet_names=_copy_list(_read_field(extracted, "sheet_names")),
        metadata=_copy_mapping(_read_field(extracted, "metadata")),
        warnings=_copy_list(_read_field(extracted, "warnings")),
        provider=_text_field(extracted, "provider"),
        confidence=_optional_float(_read_field(extracted, "confidence")),
        quality=_text_field(extracted, "quality"),
        trace=trace,
    )
def _document_record_from_content_record(record: Any) -> DossierDocumentRecord:
    metadata = _copy_mapping(_read_field(record, "metadata"))
    evidence_ref = _copy_mapping(_read_field(record, "evidence_ref"))
    trace = _copy_mapping(_read_field(record, "trace"))
    attachment_metadata = _copy_mapping(metadata.get("attachment"))
    attachment_trace = _copy_mapping(trace.get("attachment"))
    document_trace = _copy_mapping(trace.get("document"))
    content_fingerprint = _text_field(record, "content_fingerprint")

    source_url = _first_non_empty(
        _text_field(attachment_metadata, "source_url"),
        _text_field(attachment_trace, "source_url"),
        _text_field(evidence_ref, "source_url"),
        _text_field(record, "url"),
    )
    referrer_url = _first_non_empty(
        _text_field(attachment_metadata, "referrer_url"),
        _text_field(attachment_trace, "referrer_url"),
    )
    source_path = _first_non_empty(
        _text_field(attachment_metadata, "local_path"),
        _text_field(attachment_trace, "local_path"),
        _text_field(document_trace, "source_path"),
        _text_field(metadata, "source_path"),
        _text_field(evidence_ref, "source_path"),
        _text_field(record, "source_url_or_file"),
    )
    filename = _first_non_empty(
        _text_field(attachment_metadata, "filename"),
        _text_field(attachment_trace, "filename"),
        _text_field(evidence_ref, "filename"),
        Path(_first_non_empty(source_path, _text_field(record, "source_url_or_file"), _text_field(record, "url"))).name,
        _text_field(record, "title"),
    )
    ledger = DossierAttachmentLedgerEntry(
        source_url=source_url,
        referrer_url=referrer_url,
        filename=filename,
        mime=_first_non_empty(_text_field(attachment_metadata, "mime"), _text_field(attachment_trace, "mime"), _text_field(metadata, "mime")),
        size=_int_or_default(_first_not_none(_read_field(attachment_metadata, "size"), _read_field(attachment_trace, "size")), default=0),
        checksum=_first_non_empty(_text_field(attachment_metadata, "checksum"), _text_field(attachment_trace, "checksum"), _text_field(evidence_ref, "checksum")),
        fetch_status=_first_non_empty(_text_field(attachment_metadata, "fetch_status"), _text_field(attachment_trace, "fetch_status"), _text_field(record, "fetch_status")),
        entry_kind=_first_non_empty(_text_field(attachment_metadata, "entry_kind"), _text_field(attachment_trace, "entry_kind"), _text_field(evidence_ref, "entry_kind"), "attachment"),
        local_path=_first_non_empty(_text_field(attachment_metadata, "local_path"), _text_field(attachment_trace, "local_path"), source_path),
        dossier_file=_first_non_empty(_text_field(attachment_metadata, "dossier_file"), _text_field(attachment_trace, "dossier_file")),
        archive_depth=_int_or_default(_first_not_none(_read_field(attachment_metadata, "archive_depth"), _read_field(attachment_trace, "archive_depth")), default=0),
        parent_archive_url=_first_non_empty(_text_field(attachment_metadata, "parent_archive_url"), _text_field(attachment_trace, "parent_archive_url")),
        warnings=_merge_string_lists(_read_field(attachment_metadata, "warnings"), _read_field(attachment_trace, "warnings"), _read_field(metadata, "warnings"), _read_field(document_trace, "warnings"), _read_field(evidence_ref, "warnings")),
    )

    content_record_trace = _copy_mapping(trace.get("content_record"))
    if content_fingerprint:
        content_record_trace.setdefault("content_fingerprint", content_fingerprint)
    if evidence_ref:
        content_record_trace.setdefault("evidence_ref", evidence_ref)
    if content_record_trace:
        trace = _merge_freeform_mappings(trace, {"content_record": content_record_trace})

    merged_metadata = dict(metadata)
    if evidence_ref:
        merged_metadata.setdefault("evidence_ref", evidence_ref)
    if content_fingerprint:
        merged_metadata.setdefault("content_fingerprint", content_fingerprint)

    return DossierDocumentRecord(
        ledger=ledger,
        source_path=source_path,
        dossier_file=ledger.dossier_file,
        source_format=_first_non_empty(_text_field(metadata, "source_format"), _text_field(document_trace, "source_format"), _text_field(evidence_ref, "source_format"), Path(filename).suffix.lstrip("."), _text_field(record, "source_type")),
        text=_first_non_empty(_text_field(record, "text"), _text_field(record, "cleaned_text"), _text_field(record, "raw_text")),
        tables=_copy_tables(_read_field(record, "tables")),
        sheet_names=_merge_string_lists(_read_field(metadata, "sheet_names"), _read_field(document_trace, "sheet_names"), _read_field(evidence_ref, "sheet_names")),
        metadata=merged_metadata,
        warnings=_merge_string_lists(_read_field(metadata, "warnings"), _read_field(document_trace, "warnings"), _read_field(evidence_ref, "warnings"), ledger.warnings),
        provider=_first_non_empty(_text_field(metadata, "provider"), _text_field(document_trace, "provider"), _text_field(evidence_ref, "provider")),
        confidence=_first_float(_read_field(metadata, "confidence"), _read_field(document_trace, "confidence"), _read_field(evidence_ref, "confidence")),
        quality=_first_non_empty(_text_field(metadata, "quality"), _text_field(document_trace, "quality"), _text_field(evidence_ref, "quality")),
        trace=trace,
    )


def _assemble_document_records(attachment_records: list[Any], document_content_records: list[Any]) -> list[DossierDocumentRecord]:
    documents: list[DossierDocumentRecord] = []
    by_key: dict[tuple[str, ...], DossierDocumentRecord] = {}

    for record in attachment_records:
        document = _document_record_from_input(record)
        key = _document_record_key(document)
        if key in by_key:
            _merge_document_record(by_key[key], document)
            continue
        by_key[key] = document
        documents.append(document)

    for record in document_content_records:
        document = _document_record_from_content_record(record)
        key = _document_record_key(document)
        existing = by_key.get(key)
        if existing is None:
            by_key[key] = document
            documents.append(document)
            continue
        _merge_document_record(existing, document)

    return documents


def _merge_document_record(target: DossierDocumentRecord, extra: DossierDocumentRecord) -> None:
    target.ledger.source_url = _first_non_empty(target.ledger.source_url, extra.ledger.source_url)
    target.ledger.referrer_url = _first_non_empty(target.ledger.referrer_url, extra.ledger.referrer_url)
    target.ledger.filename = _first_non_empty(target.ledger.filename, extra.ledger.filename)
    target.ledger.mime = _first_non_empty(target.ledger.mime, extra.ledger.mime)
    target.ledger.size = target.ledger.size or extra.ledger.size
    target.ledger.checksum = _first_non_empty(target.ledger.checksum, extra.ledger.checksum)
    target.ledger.fetch_status = _first_non_empty(target.ledger.fetch_status, extra.ledger.fetch_status)
    target.ledger.entry_kind = _first_non_empty(target.ledger.entry_kind, extra.ledger.entry_kind, "attachment")
    target.ledger.local_path = _first_non_empty(target.ledger.local_path, extra.ledger.local_path)
    target.ledger.dossier_file = _first_non_empty(target.ledger.dossier_file, extra.ledger.dossier_file)
    target.ledger.archive_depth = target.ledger.archive_depth or extra.ledger.archive_depth
    target.ledger.parent_archive_url = _first_non_empty(target.ledger.parent_archive_url, extra.ledger.parent_archive_url)
    target.ledger.warnings = _merge_string_lists(target.ledger.warnings, extra.ledger.warnings)

    target.source_path = _first_non_empty(target.source_path, extra.source_path)
    target.dossier_file = _first_non_empty(target.dossier_file, extra.dossier_file, target.ledger.dossier_file)
    target.source_format = _first_non_empty(target.source_format, extra.source_format)
    target.text = _first_non_empty(target.text, extra.text)
    if not target.tables:
        target.tables = extra.tables
    target.sheet_names = _merge_string_lists(target.sheet_names, extra.sheet_names)
    target.metadata = _merge_freeform_mappings(target.metadata, extra.metadata)
    target.warnings = _merge_string_lists(target.warnings, extra.warnings)
    target.provider = _first_non_empty(target.provider, extra.provider)
    if target.confidence is None:
        target.confidence = extra.confidence
    target.quality = _first_non_empty(target.quality, extra.quality)
    target.trace = _merge_freeform_mappings(target.trace, extra.trace)


def _build_company_metadata(
    *,
    explicit_metadata: dict[str, Any] | None,
    company_source: Any | None,
    result_sources: list[Any],
    profile: Any,
    okved_profile: Any | None,
    lead_assembly: dict[str, Any],
    relevance_summary: dict[str, Any],
    crawl_execution: dict[str, Any],
    visited_route_families: list[str],
) -> dict[str, Any]:
    profile_summary = _read_field(profile, "summary")
    profile_sites = _read_field(profile, "sites")
    candidate_sites = _merge_string_lists(
        _read_field(company_source, "candidate_sites"),
        *[_read_field(source, "candidate_sites") for source in result_sources],
        _read_field(profile_sites, "candidate_sites"),
        _read_field(profile_sites, "confirmed_sites"),
    )

    company_snapshot: dict[str, Any] = {}
    _set_if_meaningful(
        company_snapshot,
        "company_id",
        _first_non_empty(_text_field(company_source, "company_id"), _text_field(company_source, "inn"), _text_field(profile_summary, "inn")),
    )
    _set_if_meaningful(
        company_snapshot,
        "company_name",
        _first_non_empty(_text_field(company_source, "company_name"), _text_field(profile_summary, "company_name")),
    )
    _set_if_meaningful(
        company_snapshot,
        "site_url",
        _first_non_empty(_text_field(company_source, "input_site"), _text_field(company_source, "site_url"), _text_field(profile_sites, "best_site")),
    )
    _set_if_meaningful(company_snapshot, "candidate_sites", candidate_sites)
    _set_if_meaningful(company_snapshot, "known_okved_codes", _copy_list(_read_field(company_source, "known_okved_codes")))
    _set_if_meaningful(company_snapshot, "activity_terms", _copy_list(_read_field(company_source, "activity_terms")))
    _set_if_meaningful(company_snapshot, "source_snippets", _copy_list(_read_field(company_source, "source_snippets")))
    _set_if_meaningful(company_snapshot, "source_notes", _copy_list(_read_field(company_source, "source_notes")))

    assembled: dict[str, Any] = {}
    _set_if_meaningful(assembled, "company", company_snapshot)
    if _has_meaningful_value(profile):
        assembled["profile"] = profile
    _set_if_meaningful(assembled, "source_results", _first_mapping(*[_read_field(source, "sources") for source in result_sources]))
    _set_if_meaningful(assembled, "trusted_contacts", _first_mapping(*[_read_field(source, "trusted_contacts") for source in result_sources]))
    _set_if_meaningful(assembled, "merged_contacts", _first_mapping(*[_read_field(source, "merged_contacts") for source in result_sources]))
    _set_if_meaningful(assembled, "domain_resolution", _first_not_none(*[_read_field(source, "domain_resolution") for source in result_sources]))
    _set_if_meaningful(assembled, "validated_sites", _first_non_empty_list(*[_read_field(source, "validated_sites") for source in result_sources]))
    _set_if_meaningful(assembled, "site_probes", _first_non_empty_list(*[_read_field(source, "site_probes") for source in result_sources]))
    _set_if_meaningful(assembled, "route_strategies", _first_non_empty_list(*[_read_field(source, "route_strategies") for source in result_sources]))
    _set_if_meaningful(assembled, "okved_profile", okved_profile)

    company_result_meta: dict[str, Any] = {}
    for field_name in (
        "row_index",
        "status",
        "started_at",
        "finished_at",
        "output_contract_version",
        "input_site",
        "input_phone",
        "input_comment",
    ):
        _set_if_meaningful(company_result_meta, field_name, _first_not_none(*[_read_field(source, field_name) for source in result_sources]))
    _set_if_meaningful(assembled, "company_result", company_result_meta)

    site_parser_meta: dict[str, Any] = {}
    _set_if_meaningful(site_parser_meta, "lead_assembly", lead_assembly)
    _set_if_meaningful(site_parser_meta, "relevance_summary", relevance_summary)
    _set_if_meaningful(site_parser_meta, "crawl_execution", crawl_execution)
    _set_if_meaningful(site_parser_meta, "visited_route_families", visited_route_families)
    _set_if_meaningful(site_parser_meta, "page_records", _first_not_none(*[_read_field(source, "page_records") for source in result_sources]))
    _set_if_meaningful(site_parser_meta, "document_records", _first_not_none(*[_read_field(source, "document_records") for source in result_sources]))
    _set_if_meaningful(assembled, "site_parser", site_parser_meta)

    return _merge_freeform_mappings(assembled, explicit_metadata or {})


def _build_history_entries(
    *,
    result_sources: list[Any],
    lead_assembly: dict[str, Any],
    relevance_summary: dict[str, Any],
    crawl_execution: dict[str, Any],
    visited_route_families: list[str],
    notes: list[str],
    status: str,
    started_at: str,
    finished_at: str,
) -> list[dict[str, Any]]:
    history: list[dict[str, Any]] = []

    source_statuses: dict[str, str] = {}
    for sources_payload in (_read_field(source, "sources") for source in result_sources):
        if not isinstance(sources_payload, Mapping):
            continue
        for source_name, source_payload in sources_payload.items():
            normalized_name = str(source_name or "").strip()
            normalized_status = _text_field(source_payload, "status")
            if normalized_name and normalized_status:
                source_statuses[normalized_name] = normalized_status

    result_entry = {
        "event_type": "result_layer_snapshot",
        "status": status,
        "started_at": started_at,
        "finished_at": finished_at,
        "notes": notes,
        "source_statuses": source_statuses,
    }
    if any(_has_meaningful_value(value) for key, value in result_entry.items() if key != "event_type"):
        history.append(result_entry)

    parser_entry = {
        "event_type": "site_parser_snapshot",
        "visited_route_families": visited_route_families,
        "lead_assembly": lead_assembly,
        "relevance_summary": relevance_summary,
        "crawl_execution": crawl_execution,
    }
    if any(_has_meaningful_value(value) for key, value in parser_entry.items() if key != "event_type"):
        history.append(parser_entry)

    return history


def _build_reprocess_inputs(
    *,
    site_refresh_plans: list[Any],
    plans: list[Any],
) -> list[dict[str, Any]]:
    reprocess_inputs: list[dict[str, Any]] = []

    for plan in site_refresh_plans:
        payload = {
            "input_type": "site_refresh_plan",
            "site_url": _text_field(plan, "site_url"),
            "cadence": _text_field(plan, "cadence"),
            "next_due_at": _text_field(plan, "next_due_at"),
            "reason": _text_field(plan, "reason"),
        }
        if _has_meaningful_value(payload):
            reprocess_inputs.append(payload)
    if reprocess_inputs:
        return reprocess_inputs

    for plan in plans:
        probe = _read_field(plan, "probe")
        routes = _coerce_list(_read_field(plan, "routes"))
        payload = {
            "input_type": "factory_site_plan",
            "site_url": _text_field(plan, "site_url"),
            "probe_status": _text_field(probe, "status"),
            "probe_site_class": _text_field(probe, "site_class"),
            "allows_deep_check": bool(getattr(plan, "allows_deep_check", False)),
            "route_count": len(routes),
            "routes": [
                {
                    "route_pattern": _text_field(route, "route_pattern"),
                    "section_guess": _text_field(route, "section_guess"),
                    "mode": _text_field(route, "mode"),
                    "route_family": _text_field(route, "route_family"),
                    "priority": _int_or_default(_read_field(route, "priority"), default=0),
                    "crawl_budget": _int_or_default(_read_field(route, "crawl_budget"), default=0),
                    "mandatory": bool(_read_field(route, "mandatory")),
                }
                for route in routes
            ],
            "notes": _merge_string_lists(_read_field(plan, "notes")),
        }
        if _has_meaningful_value(payload):
            reprocess_inputs.append(payload)

    return reprocess_inputs

def _build_evidence_references(
    page_records: list[DossierPageRecord],
    document_records: list[DossierDocumentRecord],
) -> list[EvidenceReference]:
    evidence_by_key: dict[tuple[str, ...], EvidenceReference] = {}
    for material in _source_materials(page_records, document_records):
        evidence_by_key.setdefault(_evidence_key(material.evidence), material.evidence)
    return list(evidence_by_key.values())


def _extract_entities(
    page_records: list[DossierPageRecord],
    document_records: list[DossierDocumentRecord],
) -> list[ExtractedEntity]:
    entities: dict[tuple[str, str], ExtractedEntity] = {}

    for material in _source_materials(page_records, document_records):
        text = material.text
        if not text:
            continue
        _extract_regex_entities(entities, text, material, "email", _EMAIL_RE, normalizer=_normalize_email)
        _extract_regex_entities(entities, text, material, "phone", _PHONE_RE, normalizer=_normalize_phone)
        _extract_regex_entities(entities, text, material, "inn", _INN_RE, group=1, normalizer=_digits_only)
        _extract_regex_entities(entities, text, material, "kpp", _KPP_RE, group=1, normalizer=_digits_only)
        _extract_regex_entities(entities, text, material, "ogrn", _OGRN_RE, group=1, normalizer=_digits_only)
        _extract_regex_entities(entities, text, material, "legal_name", _LEGAL_NAME_RE, normalizer=_normalize_legal_name)
        _extract_address_entities(entities, text, material)
        _extract_people_and_titles(entities, text, material)
        _extract_job_title_lines(entities, text, material)
        _extract_signals(entities, text, material)

    return sorted(entities.values(), key=lambda item: (item.entity_type, item.normalized_value or item.value))


def _source_materials(
    page_records: list[DossierPageRecord],
    document_records: list[DossierDocumentRecord],
) -> list[_SourceMaterial]:
    materials: list[_SourceMaterial] = []
    for record in page_records:
        text = _normalize_spaces("\n".join(part for part in (record.title, record.text, _tables_to_text(record.tables)) if part))
        materials.append(
            _SourceMaterial(
                text=text,
                evidence=EvidenceReference(
                    evidence_type="page",
                    source_url=record.source_url,
                    title=record.title,
                    record_locator=record.content_fingerprint or record.section_guess or record.source_url,
                    metadata=_copy_mapping(record.evidence_ref),
                    trace=_copy_mapping(record.trace),
                ),
            )
        )
    for record in document_records:
        text = _normalize_spaces(
            "\n".join(part for part in (record.ledger.filename, record.text, _tables_to_text(record.tables)) if part)
        )
        materials.append(
            _SourceMaterial(
                text=text,
                evidence=EvidenceReference(
                    evidence_type="document",
                    source_url=record.ledger.source_url,
                    source_path=record.source_path or record.ledger.local_path,
                    dossier_file=record.dossier_file or record.ledger.dossier_file,
                    title=record.ledger.filename,
                    record_locator=_document_record_locator(record),
                    checksum=record.ledger.checksum,
                    metadata={
                        "mime": record.ledger.mime,
                        "referrer_url": record.ledger.referrer_url,
                        "entry_kind": record.ledger.entry_kind,
                        "source_format": record.source_format,
                        "sheet_names": list(record.sheet_names),
                        "content_fingerprint": _document_content_fingerprint(record),
                    },
                    trace=_copy_mapping(record.trace),
                ),
            )
        )
    return materials


def _extract_regex_entities(
    entities: dict[tuple[str, str], ExtractedEntity],
    text: str,
    material: _SourceMaterial,
    entity_type: str,
    pattern: re.Pattern[str],
    *,
    group: int = 0,
    normalizer: Callable[[str], str],
) -> None:
    for match in pattern.finditer(text):
        value = _normalize_spaces(match.group(group))
        if not value:
            continue
        normalized_value = normalizer(value)
        if not normalized_value:
            continue
        _add_entity(
            entities,
            entity_type=entity_type,
            value=value,
            normalized_value=normalized_value,
            evidence=_with_snippet(material.evidence, _snippet(text, match.start(group), match.end(group))),
        )


def _extract_people_and_titles(
    entities: dict[tuple[str, str], ExtractedEntity],
    text: str,
    material: _SourceMaterial,
) -> None:
    for pattern in _NAME_WITH_TITLE_PATTERNS:
        for match in pattern.finditer(text):
            name = _normalize_spaces(match.groupdict().get("name", ""))
            title = _normalize_spaces(match.groupdict().get("title", ""))
            evidence = _with_snippet(material.evidence, _snippet(text, match.start(), match.end()))
            if name:
                _add_entity(
                    entities,
                    entity_type="person_name",
                    value=name,
                    normalized_value=_normalize_spaces(name.casefold()),
                    evidence=evidence,
                    attributes={"job_title": title} if title else None,
                )
            if title:
                _add_entity(
                    entities,
                    entity_type="job_title",
                    value=title,
                    normalized_value=_normalize_spaces(title.casefold()),
                    evidence=evidence,
                    attributes={"person_name": name} if name else None,
                )


def _extract_address_entities(
    entities: dict[tuple[str, str], ExtractedEntity],
    text: str,
    material: _SourceMaterial,
) -> None:
    for match in _ADDRESS_LABEL_RE.finditer(text):
        candidate = text[match.end() : match.end() + _ADDRESS_MAX_CHARS]
        value = _normalize_address(candidate)
        if len(value) < 10:
            continue
        _add_entity(
            entities,
            entity_type="address",
            value=value,
            normalized_value=value,
            evidence=_with_snippet(material.evidence, _snippet(text, match.start(), min(len(text), match.end() + len(value)))),
        )


def _extract_job_title_lines(
    entities: dict[tuple[str, str], ExtractedEntity],
    text: str,
    material: _SourceMaterial,
) -> None:
    for match in _JOB_TITLE_LINE_RE.finditer(text):
        title = _normalize_spaces(match.group(1))
        if not title:
            continue
        _add_entity(
            entities,
            entity_type="job_title",
            value=title,
            normalized_value=_normalize_spaces(title.casefold()),
            evidence=_with_snippet(material.evidence, _snippet(text, match.start(1), match.end(1))),
        )


def _extract_signals(
    entities: dict[tuple[str, str], ExtractedEntity],
    text: str,
    material: _SourceMaterial,
) -> None:
    lowered = text.casefold()
    for entity_type, keywords in _SIGNAL_KEYWORDS.items():
        for keyword in keywords:
            position = lowered.find(keyword.casefold())
            if position < 0:
                continue
            _add_entity(
                entities,
                entity_type=entity_type,
                value=keyword,
                normalized_value=keyword.casefold(),
                evidence=_with_snippet(material.evidence, _snippet(text, position, position + len(keyword))),
            )
            break


def _add_entity(
    entities: dict[tuple[str, str], ExtractedEntity],
    *,
    entity_type: str,
    value: str,
    normalized_value: str,
    evidence: EvidenceReference,
    attributes: dict[str, Any] | None = None,
) -> None:
    key = (entity_type, normalized_value or value)
    entity = entities.get(key)
    if entity is None:
        entities[key] = ExtractedEntity(
            entity_type=entity_type,
            value=value,
            normalized_value=normalized_value,
            evidence=[evidence],
            attributes=dict(attributes or {}),
        )
        return

    if attributes:
        for attr_key, attr_value in attributes.items():
            entity.attributes.setdefault(attr_key, attr_value)
    existing_keys = {_evidence_key(item) for item in entity.evidence}
    if _evidence_key(evidence) not in existing_keys:
        entity.evidence.append(evidence)


def _with_snippet(evidence: EvidenceReference, snippet: str) -> EvidenceReference:
    return EvidenceReference(
        evidence_type=evidence.evidence_type,
        source_url=evidence.source_url,
        source_path=evidence.source_path,
        dossier_file=evidence.dossier_file,
        title=evidence.title,
        snippet=snippet,
        record_locator=evidence.record_locator,
        checksum=evidence.checksum,
        metadata=_copy_mapping(evidence.metadata),
        trace=_copy_mapping(evidence.trace),
    )


def _evidence_key(evidence: EvidenceReference) -> tuple[str, ...]:
    return (
        evidence.evidence_type,
        evidence.source_url,
        evidence.source_path,
        evidence.dossier_file,
        evidence.record_locator,
        evidence.snippet,
    )


def _snippet(text: str, start: int, end: int, *, window: int = 120) -> str:
    left = max(0, start - window)
    right = min(len(text), end + window)
    return _normalize_spaces(text[left:right])


def _tables_to_text(tables: list[list[list[str]]]) -> str:
    lines: list[str] = []
    for table in tables:
        for row in table:
            line = " | ".join(_normalize_spaces(str(cell)) for cell in row if str(cell).strip())
            if line:
                lines.append(line)
    return "\n".join(lines)


def _normalize_email(value: str) -> str:
    return value.strip().casefold()


def _normalize_legal_name(value: str) -> str:
    cleaned = _normalize_spaces(value).strip(" ,.;:")
    cleaned = re.split(r"\b(?:ИНН|КПП|ОГРН|адрес|телефон|контакты)\b", cleaned, maxsplit=1, flags=re.IGNORECASE)[0]
    return _normalize_spaces(cleaned).strip(" ,.;:")


def _normalize_phone(value: str) -> str:
    digits = _digits_only(value)
    if len(digits) == 11 and digits.startswith("8"):
        return "7" + digits[1:]
    return digits


def _normalize_address(value: str) -> str:
    cleaned = _normalize_spaces(value[:_ADDRESS_MAX_CHARS])
    field_stop = _ADDRESS_FIELD_STOP_RE.search(cleaned)
    if field_stop:
        cleaned = cleaned[: field_stop.start()]
    sentence_stop = _find_address_sentence_stop(cleaned)
    if sentence_stop is not None:
        cleaned = cleaned[:sentence_stop]
    return _normalize_spaces(cleaned).strip(" ,.;:-")


def _digits_only(value: str) -> str:
    return re.sub(r"\D+", "", value)


def _normalize_spaces(value: str) -> str:
    return re.sub(r"\s+", " ", value or "").strip()


def _find_address_sentence_stop(value: str) -> int | None:
    for match in _ADDRESS_SENTENCE_BREAK_RE.finditer(value):
        previous_token_match = re.search(r"([A-Za-zА-Яа-яЁё0-9-]+)\s*$", value[: match.start()])
        next_token_match = re.match(r"\s*([A-Za-zА-Яа-яЁё0-9-]+)", value[match.end() :])
        previous_token = (previous_token_match.group(1) if previous_token_match else "").casefold()
        next_token = (next_token_match.group(1) if next_token_match else "").casefold()
        next_fragment = value[match.end() : match.end() + 40]
        if previous_token in _ADDRESS_ABBREVIATIONS:
            continue
        if _is_address_continuation_token(next_token):
            continue
        if "," in next_fragment:
            continue
        return match.start()
    return None


def _is_address_continuation_token(token: str) -> bool:
    return token in _ADDRESS_CONTINUATION_WORDS or token.endswith(("ская", "ский", "ское", "ские", "ий", "ый"))


def _read_field(payload: Any, field_name: str, default: Any = None) -> Any:
    if isinstance(payload, Mapping):
        return payload.get(field_name, default)
    return getattr(payload, field_name, default)


def _text_field(payload: Any, field_name: str) -> str:
    return str(_read_field(payload, field_name, "") or "")


def _copy_mapping(value: Any) -> dict[str, Any]:
    if not isinstance(value, Mapping):
        return {}
    return {str(key): value[key] for key in value}


def _copy_list(value: Any) -> list[str]:
    return [str(item) for item in _coerce_list(value)]


def _copy_tables(value: Any) -> list[list[list[str]]]:
    tables: list[list[list[str]]] = []
    for table in _coerce_list(value):
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


def _coerce_list(value: Any) -> list[Any]:
    if value is None:
        return []
    if isinstance(value, list):
        return list(value)
    if isinstance(value, tuple):
        return list(value)
    return [value]


def _merge_string_lists(*values: Any) -> list[str]:
    merged: list[str] = []
    seen: set[str] = set()
    for value in values:
        for item in _coerce_list(value):
            text = str(item or "").strip()
            if not text or text in seen:
                continue
            seen.add(text)
            merged.append(text)
    return merged


def _merge_freeform_mappings(base: Mapping[str, Any] | None, extra: Mapping[str, Any] | None) -> dict[str, Any]:
    result = _copy_mapping(base)
    for key, value in _copy_mapping(extra).items():
        existing = result.get(key)
        if isinstance(existing, Mapping) and isinstance(value, Mapping):
            result[key] = _merge_freeform_mappings(existing, value)
            continue
        result[key] = value
    return result


def _merge_unique_items(*collections: Any, key: Callable[[Any], tuple[str, ...]]) -> list[Any]:
    merged: list[Any] = []
    seen: set[tuple[str, ...]] = set()
    for collection in collections:
        for item in _coerce_list(collection):
            item_key = key(item)
            if item_key in seen:
                continue
            seen.add(item_key)
            merged.append(item)
    return merged


def _resolve_result_sources(*candidates: Any) -> list[Any]:
    result_sources: list[Any] = []
    seen_ids: set[int] = set()
    for candidate in candidates:
        if candidate is None:
            continue
        identity = id(candidate)
        if identity in seen_ids:
            continue
        seen_ids.add(identity)
        result_sources.append(candidate)
    return result_sources


def _resolve_company_source(company: Any | None, result_sources: list[Any]) -> Any | None:
    if company is not None:
        return company
    for source in result_sources:
        nested_company = _read_field(source, "company")
        if nested_company is not None:
            return nested_company
    return result_sources[0] if result_sources else None


def _resolve_profile(result_sources: list[Any]) -> Any:
    for source in result_sources:
        profile = _read_field(source, "profile")
        if profile is not None:
            return profile
    return {}


def _content_record_source_kind(record: Any) -> str:
    trace = _copy_mapping(_read_field(record, "trace"))
    parser_trace = _copy_mapping(trace.get("factory_site_parser"))
    crawl_trace = _copy_mapping(parser_trace.get("crawl"))
    source_kind = str(crawl_trace.get("source_kind", "") or "").strip().lower()
    if source_kind in {"page", "document"}:
        return source_kind
    source_type = _text_field(record, "source_type").strip().lower()
    if source_type in {"html", "htm"} or source_type.startswith("html"):
        return "page"
    return "document"


def _content_record_key(record: Any) -> tuple[str, ...]:
    return (
        _content_record_source_kind(record),
        _text_field(record, "content_fingerprint"),
        _text_field(record, "url"),
        _text_field(record, "source_url_or_file"),
        _text_field(record, "title"),
    )


def _attachment_record_key(record: Any) -> tuple[str, ...]:
    ledger = _read_field(record, "ledger")
    return (
        _text_field(ledger, "checksum"),
        _text_field(ledger, "source_url"),
        _text_field(ledger, "local_path"),
        _text_field(ledger, "filename"),
    )


def _site_refresh_plan_key(plan: Any) -> tuple[str, ...]:
    return (
        _text_field(plan, "site_url"),
        _text_field(plan, "cadence"),
        _text_field(plan, "next_due_at"),
        _text_field(plan, "reason"),
    )


def _planner_plan_key(plan: Any) -> tuple[str, ...]:
    routes = _coerce_list(_read_field(plan, "routes"))
    return (
        _text_field(plan, "site_url"),
        _text_field(_read_field(plan, "probe"), "url"),
        str(len(routes)),
        "|".join(_text_field(route, "route_pattern") for route in routes[:4]),
    )


def _okved_match_key(match: Any) -> tuple[str, ...]:
    summary = _text_field(match, "summary")
    return (
        _text_field(match, "record_fingerprint"),
        _text_field(match, "record_url"),
        _text_field(match, "verdict"),
        summary,
        "" if any((summary, _text_field(match, "record_fingerprint"), _text_field(match, "record_url"))) else repr(match),
    )


def _document_record_key(record: DossierDocumentRecord) -> tuple[str, ...]:
    return (
        _document_record_locator(record),
        record.ledger.source_url,
        record.source_path or record.ledger.local_path,
        record.ledger.filename,
    )


def _document_record_locator(record: DossierDocumentRecord) -> str:
    return _first_non_empty(
        record.ledger.checksum,
        _document_content_fingerprint(record),
        record.source_path,
        record.ledger.local_path,
        record.ledger.filename,
    )


def _document_content_fingerprint(record: DossierDocumentRecord) -> str:
    content_record_trace = _copy_mapping(record.trace.get("content_record"))
    return _first_non_empty(_text_field(record.metadata, "content_fingerprint"), _text_field(content_record_trace, "content_fingerprint"))


def _first_non_empty(*values: Any) -> str:
    for value in values:
        text = str(value or "")
        if text:
            return text
    return ""


def _first_not_none(*values: Any) -> Any | None:
    for value in values:
        if value is not None:
            return value
    return None


def _first_mapping(*values: Any) -> dict[str, Any]:
    for value in values:
        if isinstance(value, Mapping):
            return _copy_mapping(value)
    return {}


def _first_non_empty_list(*values: Any) -> list[Any]:
    for value in values:
        items = _coerce_list(value)
        if items:
            return items
    return []


def _int_or_default(value: Any, *, default: int) -> int:
    if value in (None, ""):
        return default
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _float_or_default(value: Any, *, default: float) -> float:
    if value in (None, ""):
        return default
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _optional_float(value: Any) -> float | None:
    if value in (None, ""):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _first_float(*values: Any) -> float | None:
    for value in values:
        parsed = _optional_float(value)
        if parsed is not None:
            return parsed
    return None


def _set_if_meaningful(payload: dict[str, Any], key: str, value: Any) -> None:
    if _has_meaningful_value(value):
        payload[key] = value


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
    if is_dataclass(value) and not isinstance(value, type):
        return any(_has_meaningful_value(getattr(value, field.name)) for field in fields(value))
    return True






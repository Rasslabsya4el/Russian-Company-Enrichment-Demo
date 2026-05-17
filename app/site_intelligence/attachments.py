from __future__ import annotations

import hashlib
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup

from app.documents.attachments import (
    ARCHIVE_EXTENSIONS,
    DIRECT_DOCUMENT_EXTENSIONS,
    AttachmentAcquirer,
    AttachmentLedgerEntry,
    AttachmentRecord,
    _document_text,
    is_supported_attachment_url,
)
from app.documents.content import normalize_extracted_document

from .common import compact_text, dedupe_preserve_order, normalize_url, route_family_for_section
from .models import ContentRecord
from .normalizer import extract_date_from_text

SUCCESS_ATTACHMENT_STATUSES = {
    "archive_empty",
    "archive_extracted",
    "archive_member_extracted",
    "downloaded",
    "extracted",
}

MIME_TYPE_FAMILIES = {
    "application/msword": "doc",
    "application/pdf": "pdf",
    "application/vnd.ms-excel": "xls",
    "application/vnd.ms-excel.sheet.binary.macroenabled.12": "xls",
    "application/vnd.ms-excel.sheet.macroenabled.12": "xlsm",
    "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet": "xlsx",
    "application/vnd.openxmlformats-officedocument.wordprocessingml.document": "docx",
    "application/x-7z-compressed": "7z",
    "application/x-rar": "rar",
    "application/x-rar-compressed": "rar",
    "application/zip": "zip",
    "text/csv": "csv",
}


@dataclass(frozen=True)
class DocumentQueueProvenance:
    route_family: str
    source_page: str
    discovery_source: str
    canonical_url: str


@dataclass(frozen=True)
class DocumentQueueCandidate:
    source_url: str
    referrer_url: str
    section_guess: str
    document_type: str
    provenance: DocumentQueueProvenance


def _copy_tables(value: Any) -> list[list[list[str]]]:
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


def _merge_metadata(primary: dict[str, Any], fallback: dict[str, Any]) -> dict[str, Any]:
    merged = dict(primary)
    for key, value in fallback.items():
        if key not in merged:
            merged[key] = value
            continue
        if isinstance(merged[key], dict) and isinstance(value, dict):
            nested = dict(merged[key])
            for nested_key, nested_value in value.items():
                nested.setdefault(nested_key, nested_value)
            merged[key] = nested
    return merged


def _filename_from_url(source_url: str) -> str:
    return Path(urlparse(source_url).path).name or "attachment"


def _document_type_key(source_url: str, *, filename: str = "", mime: str = "") -> str:
    candidate = filename or Path(urlparse(source_url).path).name
    suffix = Path(candidate).suffix.lower()
    if suffix in ARCHIVE_EXTENSIONS:
        return "archive"
    if suffix in DIRECT_DOCUMENT_EXTENSIONS:
        return suffix.lstrip(".")
    normalized_mime = (mime or "").split(";", 1)[0].strip().lower()
    mime_family = MIME_TYPE_FAMILIES.get(normalized_mime, "")
    if mime_family in {"zip", "rar", "7z"}:
        return "archive"
    return mime_family or "attachment"


def _queue_metadata(item: AttachmentRecord) -> dict[str, str]:
    payload: dict[str, str] = {}
    if item.ledger.queue_status:
        payload["status"] = item.ledger.queue_status
    if item.ledger.skip_reason:
        payload["skip_reason"] = item.ledger.skip_reason
    if item.ledger.canonical_url:
        payload["canonical_url"] = item.ledger.canonical_url
    return payload


def _document_metadata(item: AttachmentRecord) -> dict[str, Any]:
    attachment_metadata: dict[str, Any] = {
        "mime": item.ledger.mime,
        "size": item.ledger.size,
        "checksum": item.ledger.checksum,
        "entry_kind": item.ledger.entry_kind,
        "fetch_status": item.ledger.fetch_status,
        "referrer_url": item.ledger.referrer_url,
        "archive_depth": item.ledger.archive_depth,
        "parent_archive_url": item.ledger.parent_archive_url,
    }
    provenance = item.ledger.provenance_fields()
    queue_decision = _queue_metadata(item)
    if provenance:
        attachment_metadata["provenance"] = dict(provenance)
    if queue_decision:
        attachment_metadata["document_queue"] = dict(queue_decision)

    metadata: dict[str, Any] = {"attachment": attachment_metadata}
    if provenance:
        metadata["attachment_provenance"] = dict(provenance)
    if queue_decision:
        metadata["document_queue"] = dict(queue_decision)
    if item.extracted is None:
        return metadata

    document_metadata: dict[str, Any] = dict(item.extracted.metadata)
    document_metadata.setdefault("source_format", item.extracted.source_format)
    document_metadata.setdefault("source_path", item.extracted.source_path)
    if item.extracted.sheet_names:
        document_metadata.setdefault("sheet_names", list(item.extracted.sheet_names))
    if item.extracted.provider:
        document_metadata.setdefault("provider", item.extracted.provider)
    if item.extracted.confidence is not None:
        document_metadata.setdefault("confidence", item.extracted.confidence)
    if item.extracted.quality:
        document_metadata.setdefault("quality", item.extracted.quality)
    if item.extracted.warnings:
        document_metadata.setdefault("warnings", list(item.extracted.warnings))
    if item.extracted.trace:
        document_metadata.setdefault("trace", dict(item.extracted.trace))
    metadata["document"] = document_metadata
    return metadata


def _document_evidence_ref(item: AttachmentRecord) -> dict[str, Any]:
    evidence_ref: dict[str, Any] = {
        "kind": "document_attachment",
        "source_url_or_file": item.ledger.source_url or item.ledger.local_path,
        "source_url": item.ledger.source_url,
        "local_path": item.ledger.local_path,
        "filename": item.ledger.filename,
        "checksum": item.ledger.checksum,
        "entry_kind": item.ledger.entry_kind,
    }
    provenance = item.ledger.provenance_fields()
    queue_decision = _queue_metadata(item)
    if provenance:
        evidence_ref["attachment_provenance"] = dict(provenance)
    if queue_decision:
        evidence_ref["document_queue"] = dict(queue_decision)
    if item.extracted is not None:
        evidence_ref.setdefault("source_format", item.extracted.source_format)
        evidence_ref.setdefault("source_path", item.extracted.source_path)
        if item.extracted.sheet_names:
            evidence_ref.setdefault("sheet_names", list(item.extracted.sheet_names))
    return evidence_ref


def _fallback_document_to_content_record(
    *,
    company_id: str,
    site_id: str,
    item: AttachmentRecord,
    section_guess: str,
) -> ContentRecord:
    source_url_or_file = item.ledger.source_url or item.ledger.local_path
    text = _document_text(item.extracted)
    source_type = item.extracted.source_format if item.extracted is not None else item.ledger.entry_kind
    return ContentRecord(
        company_id=company_id,
        site_id=site_id,
        source_type=source_type,
        source_url_or_file=source_url_or_file,
        section_guess=section_guess or "documents",
        title=item.ledger.filename,
        text=text,
        tables=_copy_tables(item.extracted.tables if item.extracted is not None else []),
        metadata=_document_metadata(item),
        evidence_ref=_document_evidence_ref(item),
    )


def _base_document_record(
    *,
    company_id: str,
    site_id: str,
    item: AttachmentRecord,
    section_guess: str,
) -> ContentRecord:
    if item.extracted is None:
        return _fallback_document_to_content_record(
            company_id=company_id,
            site_id=site_id,
            item=item,
            section_guess=section_guess,
        )
    return normalize_extracted_document(
        item.extracted,
        company_id=company_id,
        site_id=site_id,
        source_url_or_file=item.ledger.source_url or item.ledger.local_path,
        source_type=item.extracted.source_format,
        section_guess=section_guess or "documents",
        title=item.ledger.filename,
        site_url=site_id,
        url=item.ledger.source_url,
        extraction_method="attachment_pipeline",
        fetch_status="success" if item.ledger.fetch_status in SUCCESS_ATTACHMENT_STATUSES else item.ledger.fetch_status,
        metadata=_document_metadata(item),
        evidence_ref=_document_evidence_ref(item),
        notes=list(item.ledger.warnings) + list(item.extracted.warnings),
        trace=item.to_trace(),
    )


def _skip_attachment_record(
    *,
    candidate: DocumentQueueCandidate,
    fetch_status: str,
    skip_reason: str,
    warning: str,
    filename: str = "",
    size: int = 0,
    checksum: str = "",
    mime: str = "",
    entry_kind: str = "attachment",
) -> AttachmentRecord:
    ledger = AttachmentLedgerEntry(
        source_url=candidate.source_url,
        referrer_url=candidate.referrer_url,
        filename=filename or _filename_from_url(candidate.source_url),
        mime=mime,
        size=max(0, int(size)),
        checksum=checksum,
        fetch_status=fetch_status,
        entry_kind=entry_kind,
        route_family=candidate.provenance.route_family,
        source_page=candidate.provenance.source_page,
        discovery_source=candidate.provenance.discovery_source,
        canonical_url=candidate.provenance.canonical_url,
        queue_status="skipped",
        skip_reason=skip_reason,
        warnings=[warning] if warning else [],
    )
    return AttachmentRecord(ledger=ledger)


class AttachmentCollector:
    def __init__(
        self,
        client: Any,
        storage_root: Path,
        *,
        enable_ocr: bool = True,
        ocr_provider_name: str | None = None,
        ocr_trace_dir: Path | None = None,
        ocr_execution_context: Any | None = None,
    ) -> None:
        self.max_chars = int(os.getenv("MAX_CONTENT_RECORD_CHARS", "4000"))
        self.max_links_per_page = max(1, int(os.getenv("SITE_ATTACHMENT_MAX_LINKS_PER_PAGE", "12")))
        self.max_queue_items = max(1, int(os.getenv("SITE_DOCUMENT_QUEUE_MAX_COUNT", str(self.max_links_per_page))))
        self.max_item_bytes = max(0, int(os.getenv("SITE_DOCUMENT_QUEUE_MAX_ITEM_BYTES", "12000000")))
        self.max_total_bytes = max(0, int(os.getenv("SITE_DOCUMENT_QUEUE_MAX_TOTAL_BYTES", "24000000")))
        self.max_per_type = max(0, int(os.getenv("SITE_DOCUMENT_QUEUE_MAX_PER_TYPE", "4")))
        self.seen_urls: set[str] = set()
        self.seen_checksums: set[str] = set()
        self.queue_items_used = 0
        self.queue_bytes_used = 0
        self.queue_type_counts: dict[str, int] = {}
        self.acquirer = AttachmentAcquirer(
            storage_root,
            client=client,
            enable_ocr=enable_ocr,
            ocr_provider_name=ocr_provider_name,
            ocr_trace_dir=ocr_trace_dir,
            ocr_execution_context=ocr_execution_context,
        )

    def response_looks_like_attachment(self, response: requests.Response, *, source_url: str | None = None) -> bool:
        return self.acquirer.looks_like_attachment(source_url or response.url, response.headers.get("Content-Type", ""))

    def collect_from_direct_response(
        self,
        *,
        company_id: str,
        site_url: str,
        response: requests.Response,
        source_url: str,
        referrer_url: str,
        section_guess: str,
        route_family: str = "",
        source_page: str = "",
        discovery_source: str = "",
    ) -> list[ContentRecord]:
        candidate = self._build_candidate(
            source_url=source_url,
            referrer_url=referrer_url,
            section_guess=section_guess,
            route_family=route_family,
            source_page=source_page or referrer_url or response.url or source_url,
            discovery_source=discovery_source or "planner_direct_document",
            mime=response.headers.get("Content-Type", ""),
        )
        records = self._admit_candidate(candidate)
        if records is None:
            records = self._acquire_response_candidate(candidate, response=response)
        return [self._to_content_record(company_id, site_url, section_guess, route_family, item) for item in records]

    def collect_from_html_response(
        self,
        *,
        company_id: str,
        site_url: str,
        response: requests.Response,
        section_guess: str,
        route_family: str = "",
        source_page: str = "",
        discovery_source: str = "",
    ) -> list[ContentRecord]:
        attachment_urls: list[str] = []
        soup = BeautifulSoup(response.text or "", "html.parser")
        for anchor in soup.select("a[href]"):
            href = (anchor.get("href") or "").strip()
            if not href or href.startswith(("mailto:", "tel:", "#")):
                continue
            full = urljoin(response.url, href)
            if not normalize_url(full) or not is_supported_attachment_url(full):
                continue
            attachment_urls.append(full)

        records: list[AttachmentRecord] = []
        deduped_urls = dedupe_preserve_order(attachment_urls)
        for overflow_url in deduped_urls[self.max_links_per_page :]:
            overflow_candidate = self._build_candidate(
                source_url=overflow_url,
                referrer_url=response.url,
                section_guess=section_guess,
                route_family=route_family,
                source_page=source_page or response.url,
                discovery_source=discovery_source or "page_attachment_link",
            )
            records.append(
                _skip_attachment_record(
                    candidate=overflow_candidate,
                    fetch_status="budget_skipped",
                    skip_reason="page_link_cap_exceeded",
                    warning=f"Page document discovery capped at {self.max_links_per_page} links.",
                )
            )

        for source_url in deduped_urls[: self.max_links_per_page]:
            candidate = self._build_candidate(
                source_url=source_url,
                referrer_url=response.url,
                section_guess=section_guess,
                route_family=route_family,
                source_page=source_page or response.url,
                discovery_source=discovery_source or "page_attachment_link",
            )
            skipped = self._admit_candidate(candidate)
            if skipped is not None:
                records.extend(skipped)
                continue
            records.extend(self._acquire_url_candidate(candidate))
        return [self._to_content_record(company_id, site_url, section_guess, route_family, item) for item in records]

    def _build_candidate(
        self,
        *,
        source_url: str,
        referrer_url: str,
        section_guess: str,
        route_family: str,
        source_page: str,
        discovery_source: str,
        mime: str = "",
    ) -> DocumentQueueCandidate:
        canonical_url = normalize_url(source_url) or source_url
        return DocumentQueueCandidate(
            source_url=canonical_url,
            referrer_url=referrer_url,
            section_guess=section_guess or "documents",
            document_type=_document_type_key(canonical_url, mime=mime),
            provenance=DocumentQueueProvenance(
                route_family=route_family_for_section(route_family or section_guess),
                source_page=normalize_url(source_page) or source_page or canonical_url,
                discovery_source=discovery_source or "document_link",
                canonical_url=canonical_url,
            ),
        )

    def _admit_candidate(self, candidate: DocumentQueueCandidate) -> list[AttachmentRecord] | None:
        if candidate.provenance.canonical_url in self.seen_urls:
            return [
                _skip_attachment_record(
                    candidate=candidate,
                    fetch_status="duplicate_skipped",
                    skip_reason="duplicate_canonical_url",
                    warning="Document queue skipped duplicate canonical URL.",
                )
            ]

        if self.max_queue_items > 0 and self.queue_items_used >= self.max_queue_items:
            return [
                _skip_attachment_record(
                    candidate=candidate,
                    fetch_status="budget_skipped",
                    skip_reason="count_cap_exceeded",
                    warning=f"Document queue count cap reached at {self.max_queue_items} items.",
                )
            ]

        used_for_type = self.queue_type_counts.get(candidate.document_type, 0)
        if self.max_per_type > 0 and used_for_type >= self.max_per_type:
            return [
                _skip_attachment_record(
                    candidate=candidate,
                    fetch_status="budget_skipped",
                    skip_reason=f"type_cap_exceeded:{candidate.document_type}",
                    warning=f"Document queue type cap reached for {candidate.document_type}.",
                )
            ]

        self.seen_urls.add(candidate.provenance.canonical_url)
        self.queue_items_used += 1
        self.queue_type_counts[candidate.document_type] = used_for_type + 1
        return None

    def _acquire_response_candidate(
        self,
        candidate: DocumentQueueCandidate,
        *,
        response: requests.Response,
    ) -> list[AttachmentRecord]:
        if self.max_item_bytes > 0 and len(response.content or b"") > self.max_item_bytes:
            return [
                _skip_attachment_record(
                    candidate=candidate,
                    fetch_status="heavy_skipped",
                    skip_reason="item_size_cap_exceeded",
                    warning=f"Document response size exceeded item cap of {self.max_item_bytes} bytes.",
                    size=len(response.content or b""),
                    mime=response.headers.get("Content-Type", ""),
                )
            ]
        records = self.acquirer.ingest_response(
            response,
            source_url=candidate.source_url,
            referrer_url=candidate.referrer_url,
            ledger_context=self._ledger_context(candidate, queue_status="queued"),
        )
        return self._finalize_candidate_records(candidate, records)

    def _acquire_url_candidate(self, candidate: DocumentQueueCandidate) -> list[AttachmentRecord]:
        records = self.acquirer.acquire_from_url(
            candidate.source_url,
            referrer_url=candidate.referrer_url,
            ledger_context=self._ledger_context(candidate, queue_status="queued"),
        )
        return self._finalize_candidate_records(candidate, records)

    def _finalize_candidate_records(
        self,
        candidate: DocumentQueueCandidate,
        records: list[AttachmentRecord],
    ) -> list[AttachmentRecord]:
        if not records:
            return []
        primary_record = records[0]
        primary_size = max(0, int(primary_record.ledger.size or 0))
        if self.max_item_bytes > 0 and primary_size > self.max_item_bytes:
            return [
                _skip_attachment_record(
                    candidate=candidate,
                    fetch_status="heavy_skipped",
                    skip_reason="item_size_cap_exceeded",
                    warning=f"Document queue skipped item larger than {self.max_item_bytes} bytes.",
                    filename=primary_record.ledger.filename,
                    size=primary_size,
                    checksum=primary_record.ledger.checksum,
                    mime=primary_record.ledger.mime,
                    entry_kind=primary_record.ledger.entry_kind,
                )
            ]

        if self.max_total_bytes > 0 and self.queue_bytes_used + primary_size > self.max_total_bytes:
            return [
                _skip_attachment_record(
                    candidate=candidate,
                    fetch_status="budget_skipped",
                    skip_reason="total_size_cap_exceeded",
                    warning=f"Document queue total size cap of {self.max_total_bytes} bytes would be exceeded.",
                    filename=primary_record.ledger.filename,
                    size=primary_size,
                    checksum=primary_record.ledger.checksum,
                    mime=primary_record.ledger.mime,
                    entry_kind=primary_record.ledger.entry_kind,
                )
            ]

        if primary_record.ledger.checksum and primary_record.ledger.checksum in self.seen_checksums:
            return [
                _skip_attachment_record(
                    candidate=candidate,
                    fetch_status="duplicate_skipped",
                    skip_reason="duplicate_checksum",
                    warning="Document queue skipped duplicate checksum.",
                    filename=primary_record.ledger.filename,
                    size=primary_size,
                    checksum=primary_record.ledger.checksum,
                    mime=primary_record.ledger.mime,
                    entry_kind=primary_record.ledger.entry_kind,
                )
            ]

        if primary_record.ledger.checksum:
            self.seen_checksums.add(primary_record.ledger.checksum)
        self.queue_bytes_used += primary_size

        for item in records:
            item.ledger.route_family = item.ledger.route_family or candidate.provenance.route_family
            item.ledger.source_page = item.ledger.source_page or candidate.provenance.source_page
            item.ledger.discovery_source = item.ledger.discovery_source or candidate.provenance.discovery_source
            item.ledger.canonical_url = item.ledger.canonical_url or candidate.provenance.canonical_url
            if item.ledger.fetch_status in SUCCESS_ATTACHMENT_STATUSES:
                item.ledger.queue_status = "acquired"
            elif item.ledger.queue_status != "skipped":
                item.ledger.queue_status = "processed"
        return records

    def _ledger_context(
        self,
        candidate: DocumentQueueCandidate,
        *,
        queue_status: str,
        skip_reason: str = "",
    ) -> dict[str, str]:
        return {
            "route_family": candidate.provenance.route_family,
            "source_page": candidate.provenance.source_page,
            "discovery_source": candidate.provenance.discovery_source,
            "canonical_url": candidate.provenance.canonical_url,
            "queue_status": queue_status,
            "skip_reason": skip_reason,
        }

    def _to_content_record(
        self,
        company_id: str,
        site_url: str,
        section_guess: str,
        route_family: str,
        item: AttachmentRecord,
    ) -> ContentRecord:
        base_record = _base_document_record(
            company_id=company_id,
            site_id=site_url,
            item=item,
            section_guess=section_guess,
        )
        base_text = str(getattr(base_record, "text", "") or "")
        text = base_text or _document_text(item.extracted)
        raw_text = compact_text(str(getattr(base_record, "raw_text", "") or "") or text, self.max_chars)
        cleaned_text = compact_text(str(getattr(base_record, "cleaned_text", "") or "") or text, self.max_chars)
        date_guess = extract_date_from_text(text[:2000]) if text else ""
        fingerprint_source = cleaned_text or item.ledger.checksum or item.ledger.canonical_url or item.ledger.source_url
        fetch_status = "success" if item.ledger.fetch_status in SUCCESS_ATTACHMENT_STATUSES else item.ledger.fetch_status
        notes = dedupe_preserve_order(
            list(item.ledger.warnings) + (list(item.extracted.warnings) if item.extracted else [])
        )[:6]
        trace = item.to_trace()
        trace.setdefault("page_signal_taxonomy", {})
        resolved_route_family = route_family_for_section(item.ledger.route_family or route_family or section_guess)
        trace["page_signal_taxonomy"].setdefault("route_family", resolved_route_family)
        trace["page_signal_taxonomy"].setdefault("section_guess", section_guess or "documents")
        if item.ledger.source_page:
            trace["page_signal_taxonomy"].setdefault("source_page", item.ledger.source_page)
        if item.ledger.discovery_source:
            trace["page_signal_taxonomy"].setdefault("discovery_source", item.ledger.discovery_source)
        if item.ledger.canonical_url:
            trace.setdefault("document_queue", {})
            trace["document_queue"].setdefault("canonical_url", item.ledger.canonical_url)
        if item.ledger.queue_status:
            trace.setdefault("document_queue", {})
            trace["document_queue"].setdefault("status", item.ledger.queue_status)
        if item.ledger.skip_reason:
            trace.setdefault("document_queue", {})
            trace["document_queue"].setdefault("skip_reason", item.ledger.skip_reason)
        metadata = _document_metadata(item)
        base_metadata = getattr(base_record, "metadata", {})
        if isinstance(base_metadata, dict):
            metadata = _merge_metadata(base_metadata, metadata)
        tables = _copy_tables(getattr(base_record, "tables", []))
        if not tables and item.extracted is not None:
            tables = _copy_tables(item.extracted.tables)
        return ContentRecord(
            company_id=str(getattr(base_record, "company_id", "") or company_id),
            site_id=str(getattr(base_record, "site_id", "") or site_url),
            site_url=str(getattr(base_record, "site_url", "") or site_url),
            url=str(getattr(base_record, "url", "") or item.ledger.source_url),
            source_type=str(
                getattr(base_record, "source_type", "")
                or (item.extracted.source_format if item.extracted is not None else item.ledger.entry_kind)
            ),
            source_url_or_file=str(
                getattr(base_record, "source_url_or_file", "") or item.ledger.source_url or item.ledger.local_path
            ),
            section_guess=str(getattr(base_record, "section_guess", "") or section_guess or "documents"),
            title=str(getattr(base_record, "title", "") or item.ledger.filename),
            text=base_text or cleaned_text,
            tables=tables,
            metadata=metadata,
            evidence_ref=getattr(base_record, "evidence_ref", None) or _document_evidence_ref(item),
            date=date_guess,
            raw_text=raw_text,
            cleaned_text=cleaned_text,
            extraction_method="attachment_pipeline",
            fetch_status=fetch_status,
            content_fingerprint=hashlib.sha1(fingerprint_source.encode("utf-8")).hexdigest(),
            relevance_label=str(getattr(base_record, "relevance_label", "unknown") or "unknown"),
            relevance_score=float(getattr(base_record, "relevance_score", 0.0) or 0.0),
            relevance_reasons=list(getattr(base_record, "relevance_reasons", []) or []),
            llm_result=getattr(base_record, "llm_result", None),
            notes=notes,
            trace=trace,
        )


__all__ = ["AttachmentCollector", "DocumentQueueCandidate", "DocumentQueueProvenance"]

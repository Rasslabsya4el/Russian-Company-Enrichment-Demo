from __future__ import annotations

import hashlib
import re
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

from app.documents.formats import ExtractedDocument, extract_document
from app.documents.ocr import PdfOcrProvider


def _looks_like_url(value: str) -> bool:
    parsed = urlparse(value)
    return bool(parsed.scheme and parsed.netloc)


def _normalize_text(value: str) -> str:
    return re.sub(r"\s+", " ", value or "").strip()


def _extract_date_guess(text: str) -> str:
    sample = text[:2000]
    match = re.search(r"\b(\d{2}[./-]\d{2}[./-]\d{4})\b", sample)
    if match:
        return match.group(1)
    match = re.search(r"\b(\d{4}-\d{2}-\d{2})\b", sample)
    if match:
        return match.group(1)
    return ""


def coerce_document_text(payload: ExtractedDocument) -> str:
    text = (payload.text or "").strip()
    if text:
        return text
    lines: list[str] = []
    for table in payload.tables:
        for row in table:
            if any(cell for cell in row):
                lines.append(" | ".join(row))
    return "\n".join(lines).strip()


def _merge_dicts(base: dict[str, Any], extra: dict[str, Any]) -> dict[str, Any]:
    result = dict(base)
    for key, value in extra.items():
        if key not in result:
            result[key] = value
            continue
        existing = result[key]
        if isinstance(existing, dict) and isinstance(value, dict):
            result[key] = _merge_dicts(existing, value)
        else:
            result[key] = value
    return result


def _build_content_fingerprint(text: str, source_url_or_file: str) -> str:
    fingerprint_source = text or source_url_or_file
    return hashlib.sha1(fingerprint_source.encode("utf-8")).hexdigest()


def _default_title(source_url_or_file: str, payload: ExtractedDocument) -> str:
    candidate = source_url_or_file or payload.source_path
    return Path(candidate).name if candidate else ""


def _default_metadata(payload: ExtractedDocument) -> dict[str, Any]:
    metadata: dict[str, Any] = {
        "source_format": payload.source_format,
        "source_path": payload.source_path,
    }
    if payload.sheet_names:
        metadata["sheet_names"] = list(payload.sheet_names)
    metadata.update(payload.metadata)
    if payload.provider:
        metadata["provider"] = payload.provider
    if payload.confidence is not None:
        metadata["confidence"] = payload.confidence
    if payload.quality:
        metadata["quality"] = payload.quality
    if payload.warnings:
        metadata["warnings"] = list(payload.warnings)
    return metadata


def _default_evidence_ref(payload: ExtractedDocument, source_url_or_file: str) -> dict[str, Any]:
    evidence_ref: dict[str, Any] = {
        "kind": "document_file",
        "source_format": payload.source_format,
        "source_path": payload.source_path,
        "source_url_or_file": source_url_or_file or payload.source_path,
        "sheet_names": list(payload.sheet_names),
        "warnings": list(payload.warnings),
        "trace": dict(payload.trace),
    }
    if payload.provider:
        evidence_ref["provider"] = payload.provider
    if payload.confidence is not None:
        evidence_ref["confidence"] = payload.confidence
    if payload.quality:
        evidence_ref["quality"] = payload.quality
    return evidence_ref


def build_evidence_ref(
    payload: ExtractedDocument,
    *,
    source_url_or_file: str | None = None,
    extra_evidence: dict[str, Any] | None = None,
) -> dict[str, Any]:
    evidence_ref = _default_evidence_ref(payload, source_url_or_file or payload.source_path)
    if extra_evidence:
        evidence_ref = _merge_dicts(evidence_ref, extra_evidence)
    return evidence_ref


def _default_trace(payload: ExtractedDocument) -> dict[str, Any]:
    return {
        "document": {
            "source_format": payload.source_format,
            "source_path": payload.source_path,
            "metadata": dict(payload.metadata),
            "warnings": list(payload.warnings),
            "provider": payload.provider,
            "confidence": payload.confidence,
            "quality": payload.quality,
            "sheet_names": list(payload.sheet_names),
            "trace": dict(payload.trace),
        }
    }


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


def _copy_mapping(value: Any) -> dict[str, Any]:
    return dict(value) if isinstance(value, dict) else {}


def _copy_string_list(value: Any) -> list[str]:
    if not isinstance(value, list):
        return []
    return [str(item) for item in value]


def _copy_optional_mapping(value: Any) -> dict[str, Any] | None:
    if value is None:
        return None
    if isinstance(value, dict):
        return dict(value)
    return {"value": str(value)}


LEGACY_POSITIONAL_FIELDS = (
    "company_id",
    "site_url",
    "url",
    "source_type",
    "title",
    "date",
    "raw_text",
    "cleaned_text",
    "section_guess",
    "extraction_method",
    "fetch_status",
    "content_fingerprint",
    "relevance_label",
    "relevance_score",
    "relevance_reasons",
    "llm_result",
    "notes",
    "trace",
)


@dataclass(init=False)
class NormalizedContentRecord:
    company_id: str
    site_id: str
    source_type: str
    source_url_or_file: str
    section_guess: str = ""
    title: str = ""
    text: str = ""
    tables: list[list[list[str]]] = field(default_factory=list)
    metadata: dict[str, Any] = field(default_factory=dict)
    evidence_ref: dict[str, Any] = field(default_factory=dict)
    site_url: str = ""
    url: str = ""
    date: str = ""
    raw_text: str = ""
    cleaned_text: str = ""
    extraction_method: str = ""
    fetch_status: str = ""
    content_fingerprint: str = ""
    relevance_label: str = "unknown"
    relevance_score: float = 0.0
    relevance_reasons: list[str] = field(default_factory=list)
    llm_result: dict[str, Any] | None = None
    notes: list[str] = field(default_factory=list)
    trace: dict[str, Any] = field(default_factory=dict)

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        if len(args) > len(LEGACY_POSITIONAL_FIELDS):
            raise TypeError(
                f"{type(self).__name__} expected at most {len(LEGACY_POSITIONAL_FIELDS)} positional arguments, got {len(args)}"
            )

        values = dict(kwargs)
        for field_name, value in zip(LEGACY_POSITIONAL_FIELDS, args):
            if field_name in values:
                raise TypeError(f"{type(self).__name__} got multiple values for argument {field_name!r}")
            values[field_name] = value

        supported_fields = set(type(self).__dataclass_fields__)
        unexpected = sorted(key for key in values if key not in supported_fields)
        if unexpected:
            unexpected_list = ", ".join(repr(key) for key in unexpected)
            raise TypeError(f"{type(self).__name__} got unexpected keyword argument(s): {unexpected_list}")

        self.company_id = str(values.get("company_id", "") or "")
        self.site_id = str(values.get("site_id", "") or "")
        self.source_type = str(values.get("source_type", "") or "")
        self.source_url_or_file = str(values.get("source_url_or_file", "") or "")
        self.section_guess = str(values.get("section_guess", "") or "")
        self.title = str(values.get("title", "") or "")
        self.text = str(values.get("text", "") or "")
        self.tables = _copy_tables(values.get("tables"))
        self.metadata = _copy_mapping(values.get("metadata"))
        evidence_ref = _copy_optional_mapping(values.get("evidence_ref"))
        self.evidence_ref = evidence_ref or {}
        self.site_url = str(values.get("site_url", "") or "")
        self.url = str(values.get("url", "") or "")
        self.date = str(values.get("date", "") or "")
        self.raw_text = str(values.get("raw_text", "") or "")
        self.cleaned_text = str(values.get("cleaned_text", "") or "")
        self.extraction_method = str(values.get("extraction_method", "") or "")
        self.fetch_status = str(values.get("fetch_status", "") or "")
        self.content_fingerprint = str(values.get("content_fingerprint", "") or "")
        self.relevance_label = str(values.get("relevance_label", "unknown") or "unknown")
        self.relevance_score = float(values.get("relevance_score", 0.0) or 0.0)
        self.relevance_reasons = _copy_string_list(values.get("relevance_reasons"))
        self.llm_result = _copy_optional_mapping(values.get("llm_result"))
        self.notes = _copy_string_list(values.get("notes"))
        self.trace = _copy_mapping(values.get("trace"))
        self.__post_init__()

    def __post_init__(self) -> None:
        self.site_id = str(self.site_id or self.site_url or "")
        self.site_url = str(self.site_url or self.site_id or "")
        self.url = str(self.url or "")
        self.source_url_or_file = str(self.source_url_or_file or self.url or "")
        self.section_guess = self.section_guess or "documents"
        self.text = str(self.text or self.cleaned_text or self.raw_text or "")
        if not self.raw_text:
            self.raw_text = self.text
        if not self.cleaned_text:
            self.cleaned_text = _normalize_text(self.text) or self.raw_text
        if not self.date:
            self.date = _extract_date_guess(self.text)
        if not self.title:
            self.title = Path(self.source_url_or_file).name if self.source_url_or_file else ""
        if not self.url and _looks_like_url(self.source_url_or_file):
            self.url = self.source_url_or_file
        if not self.site_url and self.url:
            parsed = urlparse(self.url)
            self.site_url = f"{parsed.scheme}://{parsed.netloc}" if parsed.scheme and parsed.netloc else ""
        if not self.site_id:
            self.site_id = self.site_url
        if not self.content_fingerprint:
            self.content_fingerprint = _build_content_fingerprint(self.cleaned_text or self.raw_text, self.source_url_or_file)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> NormalizedContentRecord:
        return cls(
            company_id=payload.get("company_id", ""),
            site_id=str(payload.get("site_id", "") or payload.get("site_url", "") or ""),
            source_type=payload.get("source_type", ""),
            source_url_or_file=str(payload.get("source_url_or_file", "") or payload.get("url", "") or ""),
            section_guess=payload.get("section_guess", ""),
            title=payload.get("title", ""),
            text=str(payload.get("text", "") or payload.get("cleaned_text", "") or payload.get("raw_text", "")),
            tables=[
                [[str(cell or "") for cell in row] for row in table]
                for table in payload.get("tables", [])
                if isinstance(table, list)
            ]
            if isinstance(payload.get("tables"), list)
            else [],
            metadata=dict(payload.get("metadata", {})) if isinstance(payload.get("metadata"), dict) else {},
            evidence_ref=dict(payload.get("evidence_ref", {})) if isinstance(payload.get("evidence_ref"), dict) else {},
            site_url=str(payload.get("site_url", "") or payload.get("site_id", "") or ""),
            url=str(payload.get("url", "") or payload.get("source_url_or_file", "") or ""),
            date=payload.get("date", ""),
            raw_text=payload.get("raw_text", ""),
            cleaned_text=payload.get("cleaned_text", ""),
            extraction_method=payload.get("extraction_method", ""),
            fetch_status=payload.get("fetch_status", ""),
            content_fingerprint=payload.get("content_fingerprint", ""),
            relevance_label=payload.get("relevance_label", "unknown"),
            relevance_score=float(payload.get("relevance_score", 0.0) or 0.0),
            relevance_reasons=list(payload.get("relevance_reasons", [])),
            llm_result=payload.get("llm_result"),
            notes=list(payload.get("notes", [])),
            trace=dict(payload.get("trace", {})),
        )


def normalize_extracted_document(
    payload: ExtractedDocument,
    *,
    company_id: str = "",
    site_id: str = "",
    source_url_or_file: str | None = None,
    source_type: str | None = None,
    section_guess: str = "documents",
    title: str = "",
    site_url: str = "",
    url: str = "",
    date: str = "",
    raw_text: str = "",
    cleaned_text: str = "",
    extraction_method: str = "document_extract",
    fetch_status: str = "success",
    content_fingerprint: str = "",
    relevance_label: str = "unknown",
    relevance_score: float = 0.0,
    relevance_reasons: list[str] | None = None,
    llm_result: dict[str, Any] | None = None,
    metadata: dict[str, Any] | None = None,
    evidence_ref: dict[str, Any] | None = None,
    notes: list[str] | None = None,
    trace: dict[str, Any] | None = None,
) -> NormalizedContentRecord:
    resolved_source = str(source_url_or_file or payload.source_path)
    text = coerce_document_text(payload)
    merged_metadata = _merge_dicts(_default_metadata(payload), metadata or {})
    merged_trace = _merge_dicts(_default_trace(payload), trace or {})
    record_notes = list(payload.warnings)
    if notes:
        record_notes.extend(notes)
    return NormalizedContentRecord(
        company_id=company_id,
        site_id=site_id,
        source_type=source_type or payload.source_format,
        source_url_or_file=resolved_source,
        section_guess=section_guess,
        title=title or _default_title(resolved_source, payload),
        text=text,
        tables=[[[cell for cell in row] for row in table] for table in payload.tables],
        metadata=merged_metadata,
        evidence_ref=build_evidence_ref(
            payload,
            source_url_or_file=resolved_source,
            extra_evidence=evidence_ref,
        ),
        site_url=site_url,
        url=url or (resolved_source if _looks_like_url(resolved_source) else ""),
        date=date,
        raw_text=raw_text or text,
        cleaned_text=cleaned_text or _normalize_text(text),
        extraction_method=extraction_method,
        fetch_status=fetch_status,
        content_fingerprint=content_fingerprint,
        relevance_label=relevance_label,
        relevance_score=relevance_score,
        relevance_reasons=list(relevance_reasons or []),
        llm_result=dict(llm_result) if isinstance(llm_result, dict) else llm_result,
        notes=record_notes,
        trace=merged_trace,
    )


def document_to_content_record(payload: ExtractedDocument, **kwargs: Any) -> NormalizedContentRecord:
    return normalize_extracted_document(payload, **kwargs)


def extract_content_record(
    path: str | Path,
    *,
    company_id: str = "",
    site_id: str = "",
    source_url_or_file: str | None = None,
    source_type: str | None = None,
    section_guess: str = "documents",
    title: str = "",
    site_url: str = "",
    url: str = "",
    date: str = "",
    raw_text: str = "",
    cleaned_text: str = "",
    extraction_method: str = "document_extract",
    fetch_status: str = "success",
    content_fingerprint: str = "",
    relevance_label: str = "unknown",
    relevance_score: float = 0.0,
    relevance_reasons: list[str] | None = None,
    llm_result: dict[str, Any] | None = None,
    metadata: dict[str, Any] | None = None,
    evidence_ref: dict[str, Any] | None = None,
    notes: list[str] | None = None,
    trace: dict[str, Any] | None = None,
    enable_ocr: bool = True,
    ocr_provider: PdfOcrProvider | None = None,
    ocr_provider_name: str | None = None,
    ocr_trace_dir: Path | None = None,
) -> NormalizedContentRecord:
    local_path = Path(path)
    payload = extract_document(
        local_path,
        enable_ocr=enable_ocr,
        ocr_provider=ocr_provider,
        ocr_provider_name=ocr_provider_name,
        ocr_trace_dir=ocr_trace_dir,
    )
    return normalize_extracted_document(
        payload,
        company_id=company_id,
        site_id=site_id,
        source_url_or_file=source_url_or_file or str(local_path),
        source_type=source_type,
        section_guess=section_guess,
        title=title,
        site_url=site_url,
        url=url,
        date=date,
        raw_text=raw_text,
        cleaned_text=cleaned_text,
        extraction_method=extraction_method,
        fetch_status=fetch_status,
        content_fingerprint=content_fingerprint,
        relevance_label=relevance_label,
        relevance_score=relevance_score,
        relevance_reasons=relevance_reasons,
        llm_result=llm_result,
        metadata=metadata,
        evidence_ref=evidence_ref,
        notes=notes,
        trace=trace,
    )


def extract_normalized_content(path: str | Path, **kwargs: Any) -> NormalizedContentRecord:
    return extract_content_record(path, **kwargs)


def content_record_to_dict(record: NormalizedContentRecord) -> dict[str, Any]:
    return record.to_dict()


__all__ = [
    "NormalizedContentRecord",
    "build_evidence_ref",
    "coerce_document_text",
    "content_record_to_dict",
    "document_to_content_record",
    "extract_content_record",
    "extract_normalized_content",
    "normalize_extracted_document",
]

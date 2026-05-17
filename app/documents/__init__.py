from __future__ import annotations

from typing import Any

from app.documents.attachments import AttachmentAcquirer, AttachmentLedgerEntry, AttachmentRecord
from app.documents.content import (
    NormalizedContentRecord,
    content_record_to_dict,
    extract_content_record,
    extract_normalized_content,
    normalize_extracted_document,
)
from app.documents.formats import ExtractedDocument, SupportedFormat, convert_document, detect_format, extract_document
from app.documents.ocr import (
    AzureDocumentIntelligencePdfProvider,
    OcrProviderResult,
    OcrSpacePdfProvider,
    PdfOcrProvider,
    PdfScanSummary,
    create_configured_ocr_provider,
    inspect_pdf_for_ocr,
    list_available_ocr_providers,
)


def __getattr__(name: str) -> Any:
    if name == "collect_content_records_for_site":
        from app.documents.pipeline import collect_content_records_for_site

        return collect_content_records_for_site
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")

__all__ = [
    "AttachmentAcquirer",
    "AttachmentLedgerEntry",
    "AttachmentRecord",
    "AzureDocumentIntelligencePdfProvider",
    "ExtractedDocument",
    "NormalizedContentRecord",
    "OcrProviderResult",
    "OcrSpacePdfProvider",
    "PdfOcrProvider",
    "PdfScanSummary",
    "SupportedFormat",
    "collect_content_records_for_site",
    "content_record_to_dict",
    "convert_document",
    "create_configured_ocr_provider",
    "detect_format",
    "extract_content_record",
    "extract_normalized_content",
    "extract_document",
    "inspect_pdf_for_ocr",
    "list_available_ocr_providers",
    "normalize_extracted_document",
]

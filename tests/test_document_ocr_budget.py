from __future__ import annotations

from pathlib import Path

from reportlab.lib.pagesizes import A4
from reportlab.pdfgen import canvas

from app.documents.formats import extract_document
from app.documents.ocr import OcrProviderResult


def _write_blank_pdf(path: Path, *, page_count: int) -> None:
    pdf = canvas.Canvas(str(path), pagesize=A4)
    for _ in range(page_count):
        pdf.showPage()
    pdf.save()


class _RecordingPdfProvider:
    name = "test_provider"

    def __init__(self, text: str) -> None:
        self.text = text
        self.calls = 0

    def is_configured(self) -> bool:
        return True

    def extract_pdf(self, path: Path, *, trace_dir=None) -> OcrProviderResult:
        self.calls += 1
        return OcrProviderResult(
            text=self.text,
            provider=self.name,
            trace={"path": str(path), "trace_dir": str(trace_dir or "")},
        )


def test_extract_document_skips_scan_only_pdf_ocr_outside_speed_budget(tmp_path, monkeypatch) -> None:
    pdf_path = tmp_path / "scan_only_tail.pdf"
    _write_blank_pdf(pdf_path, page_count=2)
    provider = _RecordingPdfProvider("OCR text should stay unreachable")
    monkeypatch.setenv("DOCUMENT_OCR_SCAN_ONLY_PAGE_CAP", "1")
    monkeypatch.setenv("DOCUMENT_OCR_SCAN_ONLY_BYTE_CAP", "0")

    payload = extract_document(
        pdf_path,
        enable_ocr=True,
        ocr_provider=provider,
        ocr_provider_name=provider.name,
    )

    assert provider.calls == 0
    assert payload.text == ""
    assert payload.metadata["ocr_attempted"] == "false"
    assert payload.metadata["ocr_applied"] == "false"
    assert payload.metadata["ocr_skip_reason"] == "scan_only_page_cap_exceeded"
    assert payload.trace["ocr"]["status"] == "budget_skipped"
    assert payload.trace["ocr"]["admission"]["page_count"] == 2
    assert payload.trace["ocr"]["admission"]["page_cap"] == 1
    assert any("OCR skipped for scan-only PDF outside speed budget" in warning for warning in payload.warnings)


def test_extract_document_keeps_ocr_for_small_scan_only_pdf_within_budget(tmp_path, monkeypatch) -> None:
    pdf_path = tmp_path / "scan_only_small.pdf"
    _write_blank_pdf(pdf_path, page_count=1)
    provider = _RecordingPdfProvider("Recovered OCR text")
    monkeypatch.setenv("DOCUMENT_OCR_SCAN_ONLY_PAGE_CAP", "4")
    monkeypatch.setenv("DOCUMENT_OCR_SCAN_ONLY_BYTE_CAP", "0")

    payload = extract_document(
        pdf_path,
        enable_ocr=True,
        ocr_provider=provider,
        ocr_provider_name=provider.name,
    )

    assert provider.calls == 1
    assert payload.text == "Recovered OCR text"
    assert payload.metadata["ocr_attempted"] == "true"
    assert payload.metadata["ocr_applied"] == "true"
    assert payload.metadata["ocr_provider"] == provider.name
    assert payload.trace["ocr"]["status"] == "succeeded"
    assert payload.trace["ocr_admission"]["reason"] == "within_budget"

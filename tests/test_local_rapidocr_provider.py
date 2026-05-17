from __future__ import annotations

from pathlib import Path

from PIL import Image, ImageDraw, ImageFont

from app.documents.formats import extract_document
from app.documents.ocr import RapidOcrLocalPdfProvider, list_available_ocr_providers


def _write_scan_only_pdf(path: Path, text: str) -> None:
    image = Image.new("RGB", (500, 120), "white")
    drawer = ImageDraw.Draw(image)
    font = ImageFont.load_default()
    drawer.text((20, 40), text, fill="black", font=font)
    image.save(path, "PDF")
    image.close()


def test_list_available_ocr_providers_includes_local_provider() -> None:
    providers = list_available_ocr_providers()
    assert "rapidocr_local" in providers


def test_rapidocr_local_provider_extracts_scan_only_pdf(tmp_path, monkeypatch) -> None:
    pdf_path = tmp_path / "scan_only_local.pdf"
    _write_scan_only_pdf(pdf_path, "HELLO 123")
    monkeypatch.setenv("DOCUMENT_OCR_SCAN_ONLY_PAGE_CAP", "4")
    monkeypatch.setenv("DOCUMENT_OCR_SCAN_ONLY_BYTE_CAP", "0")

    payload = extract_document(
        pdf_path,
        enable_ocr=True,
        ocr_provider=RapidOcrLocalPdfProvider(),
        ocr_provider_name="rapidocr_local",
    )

    normalized_text = payload.text.upper().replace(" ", "")
    assert payload.metadata["ocr_attempted"] == "true"
    assert payload.metadata["ocr_applied"] == "true"
    assert payload.metadata["ocr_provider"] == "rapidocr_local"
    assert payload.trace["ocr"]["status"] == "succeeded"
    assert payload.trace["ocr_admission"]["reason"] == "within_budget"
    assert "HELLO" in normalized_text
    assert "123" in normalized_text

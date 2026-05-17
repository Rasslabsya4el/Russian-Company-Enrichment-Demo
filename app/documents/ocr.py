from __future__ import annotations

import base64
import json
import os
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Protocol

import requests
from pypdf import PdfReader


@dataclass
class PdfPageScanSummary:
    page_number: int
    text_len: int
    image_count: int
    image_only: bool


@dataclass
class PdfScanSummary:
    page_count: int
    pages_with_text: int
    image_pages: int
    image_only_pages: int
    extracted_text_len: int
    should_run_ocr: bool
    trigger_reason: str
    per_page: list[PdfPageScanSummary] = field(default_factory=list)

    def to_trace(self) -> dict[str, Any]:
        return {
            "page_count": self.page_count,
            "pages_with_text": self.pages_with_text,
            "image_pages": self.image_pages,
            "image_only_pages": self.image_only_pages,
            "extracted_text_len": self.extracted_text_len,
            "should_run_ocr": self.should_run_ocr,
            "trigger_reason": self.trigger_reason,
            "per_page": [
                {
                    "page_number": item.page_number,
                    "text_len": item.text_len,
                    "image_count": item.image_count,
                    "image_only": item.image_only,
                }
                for item in self.per_page
            ],
        }


@dataclass
class OcrProviderResult:
    text: str
    provider: str
    confidence: float | None = None
    quality: str = ""
    warnings: list[str] = field(default_factory=list)
    trace: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class PdfOcrAdmission:
    should_attempt: bool
    reason: str
    page_count: int
    size_bytes: int
    page_cap: int
    size_cap_bytes: int

    def to_trace(self) -> dict[str, Any]:
        return {
            "should_attempt": self.should_attempt,
            "reason": self.reason,
            "page_count": self.page_count,
            "size_bytes": self.size_bytes,
            "page_cap": self.page_cap,
            "size_cap_bytes": self.size_cap_bytes,
        }


class PdfOcrProvider(Protocol):
    name: str

    def is_configured(self) -> bool:
        ...

    def extract_pdf(self, path: Path, *, trace_dir: Path | None = None) -> OcrProviderResult:
        ...


PROVIDER_NAME_ALIASES: dict[str, str] = {
    "ocr_space": "ocr_space",
    "ocrspace": "ocr_space",
    "ocr.space": "ocr_space",
    "azure_docintel": "azure_docintel",
    "azure": "azure_docintel",
    "azure_di": "azure_docintel",
    "azure_document_intelligence": "azure_docintel",
    "azure_ai_document_intelligence": "azure_docintel",
    "document_intelligence": "azure_docintel",
    "rapidocr_local": "rapidocr_local",
    "rapidocr": "rapidocr_local",
    "rapidocr_onnxruntime": "rapidocr_local",
    "local_rapidocr": "rapidocr_local",
    "local_ocr": "rapidocr_local",
}
LOW_PRIORITY_OCR_QUEUE_FAMILY = "low_priority_ocr"


def normalize_ocr_provider_name(provider_name: str | None) -> str:
    requested = (provider_name or "").strip().lower()
    if not requested:
        return ""
    return PROVIDER_NAME_ALIASES.get(requested, requested)


def inspect_pdf_for_ocr(path: Path) -> PdfScanSummary:
    reader = PdfReader(str(path))
    per_page: list[PdfPageScanSummary] = []
    pages_with_text = 0
    image_pages = 0
    image_only_pages = 0
    extracted_text_len = 0

    for index, page in enumerate(reader.pages, start=1):
        text = (page.extract_text() or "").strip()
        text_len = len(text)
        image_count = _page_image_count(page)
        if text_len > 0:
            pages_with_text += 1
            extracted_text_len += text_len
        if image_count > 0:
            image_pages += 1
        image_only = image_count > 0 and text_len == 0
        if image_only:
            image_only_pages += 1
        per_page.append(
            PdfPageScanSummary(
                page_number=index,
                text_len=text_len,
                image_count=image_count,
                image_only=image_only,
            )
        )

    page_count = len(reader.pages)
    is_image_only_pdf = page_count > 0 and image_only_pages == page_count
    should_run_ocr = extracted_text_len == 0 or is_image_only_pdf
    if is_image_only_pdf:
        trigger_reason = "image_only_pdf"
    elif extracted_text_len == 0:
        trigger_reason = "no_text_layer"
    else:
        trigger_reason = "not_required"
    return PdfScanSummary(
        page_count=page_count,
        pages_with_text=pages_with_text,
        image_pages=image_pages,
        image_only_pages=image_only_pages,
        extracted_text_len=extracted_text_len,
        should_run_ocr=should_run_ocr,
        trigger_reason=trigger_reason,
        per_page=per_page,
    )


def assess_pdf_ocr_admission(path: Path, *, scan_summary: PdfScanSummary) -> PdfOcrAdmission:
    size_bytes = 0
    try:
        size_bytes = max(int(path.stat().st_size or 0), 0)
    except Exception:
        size_bytes = 0
    page_cap = _read_ocr_budget_env("DOCUMENT_OCR_SCAN_ONLY_PAGE_CAP", default=24)
    size_cap_bytes = _read_ocr_budget_env("DOCUMENT_OCR_SCAN_ONLY_BYTE_CAP", default=4_000_000)
    if not scan_summary.should_run_ocr:
        return PdfOcrAdmission(
            should_attempt=False,
            reason="not_required",
            page_count=scan_summary.page_count,
            size_bytes=size_bytes,
            page_cap=page_cap,
            size_cap_bytes=size_cap_bytes,
        )
    if page_cap > 0 and scan_summary.page_count > page_cap:
        return PdfOcrAdmission(
            should_attempt=False,
            reason="scan_only_page_cap_exceeded",
            page_count=scan_summary.page_count,
            size_bytes=size_bytes,
            page_cap=page_cap,
            size_cap_bytes=size_cap_bytes,
        )
    if size_cap_bytes > 0 and size_bytes > size_cap_bytes:
        return PdfOcrAdmission(
            should_attempt=False,
            reason="scan_only_size_cap_exceeded",
            page_count=scan_summary.page_count,
            size_bytes=size_bytes,
            page_cap=page_cap,
            size_cap_bytes=size_cap_bytes,
        )
    return PdfOcrAdmission(
        should_attempt=True,
        reason="within_budget",
        page_count=scan_summary.page_count,
        size_bytes=size_bytes,
        page_cap=page_cap,
        size_cap_bytes=size_cap_bytes,
    )


def create_configured_ocr_provider(provider_name: str | None = None) -> PdfOcrProvider | None:
    requested = normalize_ocr_provider_name(provider_name or os.getenv("DOCUMENT_OCR_PROVIDER", ""))
    providers: dict[str, PdfOcrProvider] = {
        "ocr_space": OcrSpacePdfProvider(),
        "azure_docintel": AzureDocumentIntelligencePdfProvider(),
        "rapidocr_local": RapidOcrLocalPdfProvider(),
    }
    if requested:
        provider = providers.get(requested)
        if provider and provider.is_configured():
            return provider
        return None
    for candidate_name in ("ocr_space", "azure_docintel", "rapidocr_local"):
        provider = providers[candidate_name]
        if provider.is_configured():
            return provider
    return None


def list_available_ocr_providers() -> list[str]:
    result: list[str] = []
    for provider in (OcrSpacePdfProvider(), AzureDocumentIntelligencePdfProvider(), RapidOcrLocalPdfProvider()):
        if provider.is_configured():
            result.append(provider.name)
    return result


class OcrSpacePdfProvider:
    name = "ocr_space"

    def __init__(self) -> None:
        self.api_key = os.getenv("OCR_SPACE_API_KEY", "").strip()
        self.endpoint = os.getenv("OCR_SPACE_ENDPOINT", "https://api.ocr.space/parse/image").strip()
        self.language = os.getenv("OCR_SPACE_LANGUAGE", "rus").strip() or "rus"
        self.engine = os.getenv("OCR_SPACE_ENGINE", "1").strip() or "1"
        self.timeout = float(os.getenv("OCR_SPACE_TIMEOUT_SEC", "60"))

    def is_configured(self) -> bool:
        return bool(self.api_key)

    def extract_pdf(self, path: Path, *, trace_dir: Path | None = None) -> OcrProviderResult:
        files = {
            "file": (path.name, path.read_bytes(), "application/pdf"),
        }
        data = {
            "language": self.language,
            "filetype": "PDF",
            "detectOrientation": "true",
            "scale": "true",
            "isOverlayRequired": "false",
            "OCREngine": self.engine,
        }
        response = requests.post(
            self.endpoint,
            files=files,
            data=data,
            headers={"apikey": self.api_key},
            timeout=self.timeout,
        )
        response.raise_for_status()
        payload = response.json()

        text_blocks: list[str] = []
        warnings: list[str] = []
        for parsed in payload.get("ParsedResults") or []:
            parsed_text = str(parsed.get("ParsedText") or "").strip()
            if parsed_text:
                text_blocks.append(parsed_text)
            error_message = str(parsed.get("ErrorMessage") or "").strip()
            error_details = str(parsed.get("ErrorDetails") or "").strip()
            if error_message:
                warnings.append(error_message)
            if error_details:
                warnings.append(error_details)

        if payload.get("IsErroredOnProcessing"):
            warnings.append("OCR.space reported processing error.")
        exit_code = payload.get("OCRExitCode")
        if exit_code not in {None, 1, "1"}:
            warnings.append(f"OCR.space exit code: {exit_code}")

        text = "\n\n".join(block for block in text_blocks if block).strip()
        trace = {
            "provider": self.name,
            "endpoint": self.endpoint,
            "language": self.language,
            "engine": self.engine,
            "ocr_exit_code": exit_code,
            "is_errored_on_processing": bool(payload.get("IsErroredOnProcessing")),
            "processing_time_ms": str(payload.get("ProcessingTimeInMilliseconds") or ""),
            "parsed_result_count": len(payload.get("ParsedResults") or []),
        }
        trace.update(_persist_trace(trace_dir, path, self.name, payload, text))
        return OcrProviderResult(
            text=text,
            provider=self.name,
            warnings=_dedupe(warnings),
            trace=trace,
        )


class AzureDocumentIntelligencePdfProvider:
    name = "azure_docintel"

    def __init__(self) -> None:
        self.endpoint = os.getenv("AZURE_DOCUMENT_INTELLIGENCE_ENDPOINT", "").strip().rstrip("/")
        self.api_key = os.getenv("AZURE_DOCUMENT_INTELLIGENCE_KEY", "").strip()
        self.api_version = os.getenv("AZURE_DOCUMENT_INTELLIGENCE_API_VERSION", "2024-11-30").strip() or "2024-11-30"
        self.model_id = os.getenv("AZURE_DOCUMENT_INTELLIGENCE_MODEL", "prebuilt-read").strip() or "prebuilt-read"
        self.locale = os.getenv("AZURE_DOCUMENT_INTELLIGENCE_LOCALE", "").strip()
        self.timeout = float(os.getenv("AZURE_DOCUMENT_INTELLIGENCE_TIMEOUT_SEC", "90"))
        self.poll_interval = float(os.getenv("AZURE_DOCUMENT_INTELLIGENCE_POLL_SEC", "2"))
        self.max_polls = int(os.getenv("AZURE_DOCUMENT_INTELLIGENCE_MAX_POLLS", "30"))

    def is_configured(self) -> bool:
        return bool(self.endpoint and self.api_key)

    def extract_pdf(self, path: Path, *, trace_dir: Path | None = None) -> OcrProviderResult:
        analyze_url = (
            f"{self.endpoint}/documentintelligence/documentModels/{self.model_id}:analyze"
            f"?api-version={self.api_version}&outputContentFormat=text"
        )
        body: dict[str, Any] = {
            "base64Source": base64.b64encode(path.read_bytes()).decode("ascii"),
        }
        if self.locale:
            body["locale"] = self.locale

        response = requests.post(
            analyze_url,
            headers={
                "Ocp-Apim-Subscription-Key": self.api_key,
                "Content-Type": "application/json",
            },
            json=body,
            timeout=self.timeout,
        )
        response.raise_for_status()
        operation_location = response.headers.get("Operation-Location", "").strip()
        if not operation_location:
            raise RuntimeError("Azure Document Intelligence did not return Operation-Location.")

        last_payload: dict[str, Any] = {}
        last_status = "notStarted"
        for _ in range(self.max_polls):
            poll = requests.get(
                operation_location,
                headers={"Ocp-Apim-Subscription-Key": self.api_key},
                timeout=self.timeout,
            )
            poll.raise_for_status()
            last_payload = poll.json()
            last_status = str(last_payload.get("status") or "").lower()
            if last_status == "succeeded":
                break
            if last_status == "failed":
                raise RuntimeError(f"Azure Document Intelligence failed: {json.dumps(last_payload, ensure_ascii=False)[:500]}")
            retry_after = poll.headers.get("Retry-After", "").strip()
            time.sleep(_parse_retry_after(retry_after, self.poll_interval))
        else:
            raise RuntimeError(f"Azure Document Intelligence polling timed out with status={last_status}.")

        analyze_result = last_payload.get("analyzeResult") or {}
        text = str(analyze_result.get("content") or "").strip()
        confidences = _collect_azure_confidences(analyze_result)
        confidence = round(sum(confidences) / len(confidences), 4) if confidences else None
        warnings: list[str] = []
        if not text:
            warnings.append("Azure Document Intelligence returned empty content.")

        trace = {
            "provider": self.name,
            "endpoint": self.endpoint,
            "model_id": self.model_id,
            "api_version": self.api_version,
            "operation_location": operation_location,
            "status": last_status,
            "page_count": len(analyze_result.get("pages") or []),
            "locale": self.locale,
        }
        trace.update(_persist_trace(trace_dir, path, self.name, last_payload, text))
        return OcrProviderResult(
            text=text,
            provider=self.name,
            confidence=confidence,
            quality="word_confidence" if confidence is not None else "",
            warnings=warnings,
            trace=trace,
        )


class RapidOcrLocalPdfProvider:
    name = "rapidocr_local"

    def __init__(self) -> None:
        self.render_scale = _read_ocr_render_scale_env("DOCUMENT_OCR_LOCAL_RENDER_SCALE", default=2.0)
        self._pdfium: Any | None = None
        self._ocr_engine: Any | None = None

    def is_configured(self) -> bool:
        try:
            self._ensure_runtime()
            return True
        except Exception:
            return False

    def extract_pdf(self, path: Path, *, trace_dir: Path | None = None) -> OcrProviderResult:
        pdfium, ocr_engine = self._ensure_runtime()
        document = pdfium.PdfDocument(str(path))
        page_texts: list[str] = []
        confidences: list[float] = []
        page_traces: list[dict[str, Any]] = []
        warnings: list[str] = []

        try:
            page_count = len(document)
            for page_index in range(page_count):
                page = document[page_index]
                bitmap = None
                pil_image = None
                try:
                    bitmap = page.render(scale=self.render_scale)
                    pil_image = bitmap.to_pil()
                    result, _ = ocr_engine(pil_image)
                    page_text, page_confidences = _collect_rapidocr_text(result)
                    if page_text:
                        page_texts.append(page_text)
                    confidences.extend(page_confidences)
                    page_traces.append(
                        {
                            "page_number": page_index + 1,
                            "text_len": len(page_text),
                            "line_count": len(result or []),
                            "confidence_avg": round(sum(page_confidences) / len(page_confidences), 4)
                            if page_confidences
                            else None,
                        }
                    )
                finally:
                    if pil_image is not None:
                        try:
                            pil_image.close()
                        except Exception:
                            pass
                    if bitmap is not None:
                        try:
                            bitmap.close()
                        except Exception:
                            pass
                    try:
                        page.close()
                    except Exception:
                        pass
        finally:
            try:
                document.close()
            except Exception:
                pass

        text = "\n\n".join(chunk for chunk in page_texts if chunk).strip()
        if not text:
            warnings.append("RapidOCR local provider returned empty text.")
        confidence = round(sum(confidences) / len(confidences), 4) if confidences else None
        trace = {
            "provider": self.name,
            "render_scale": self.render_scale,
            "page_count": len(page_traces),
            "page_results": page_traces,
        }
        trace.update(
            _persist_trace(
                trace_dir,
                path,
                self.name,
                {
                    "provider": self.name,
                    "render_scale": self.render_scale,
                    "page_results": page_traces,
                },
                text,
            )
        )
        return OcrProviderResult(
            text=text,
            provider=self.name,
            confidence=confidence,
            quality="line_confidence" if confidence is not None else "",
            warnings=warnings,
            trace=trace,
        )

    def _ensure_runtime(self) -> tuple[Any, Any]:
        if self._pdfium is not None and self._ocr_engine is not None:
            return self._pdfium, self._ocr_engine
        pdfium, rapidocr_cls = _load_rapidocr_local_runtime()
        if pdfium is None or rapidocr_cls is None:
            raise RuntimeError("RapidOCR local runtime is not installed.")
        self._pdfium = pdfium
        self._ocr_engine = rapidocr_cls()
        return self._pdfium, self._ocr_engine


def _page_image_count(page: Any) -> int:
    images_attr = getattr(page, "images", None)
    if images_attr is not None:
        try:
            return len(images_attr)
        except Exception:
            return 0
    resources = page.get("/Resources")
    if not resources or "/XObject" not in resources:
        return 0
    count = 0
    for key in resources["/XObject"]:
        try:
            xobj = resources["/XObject"][key]
            if xobj.get("/Subtype") == "/Image":
                count += 1
        except Exception:
            continue
    return count


def _collect_azure_confidences(payload: dict[str, Any]) -> list[float]:
    values: list[float] = []
    for page in payload.get("pages") or []:
        for word in page.get("words") or []:
            confidence = word.get("confidence")
            if isinstance(confidence, (int, float)):
                values.append(float(confidence))
    return values


def _persist_trace(trace_dir: Path | None, source_path: Path, provider: str, raw_payload: Any, text: str) -> dict[str, str]:
    if trace_dir is None:
        return {}
    trace_dir.mkdir(parents=True, exist_ok=True)
    raw_path = trace_dir / f"{source_path.stem}_{provider}_raw.json"
    raw_path.write_text(json.dumps(raw_payload, ensure_ascii=False, indent=2), encoding="utf-8")
    result: dict[str, str] = {"raw_response_path": str(raw_path)}
    if text.strip():
        text_path = trace_dir / f"{source_path.stem}_{provider}.txt"
        text_path.write_text(text, encoding="utf-8")
        result["text_output_path"] = str(text_path)
    return result


def _parse_retry_after(value: str, default: float) -> float:
    try:
        return float(value)
    except Exception:
        return default


def _read_ocr_budget_env(name: str, *, default: int) -> int:
    raw_value = os.getenv(name, str(default))
    try:
        return max(int(raw_value or default), 0)
    except Exception:
        return max(default, 0)


def _read_ocr_render_scale_env(name: str, *, default: float) -> float:
    raw_value = os.getenv(name, str(default))
    try:
        return max(float(raw_value or default), 0.5)
    except Exception:
        return max(default, 0.5)


def _load_rapidocr_local_runtime() -> tuple[Any | None, Any | None]:
    try:
        import pypdfium2 as pdfium
        from rapidocr_onnxruntime import RapidOCR
    except Exception:
        return None, None
    return pdfium, RapidOCR


def _collect_rapidocr_text(result: Any) -> tuple[str, list[float]]:
    lines: list[str] = []
    confidences: list[float] = []
    for item in result or []:
        if not isinstance(item, (list, tuple)) or len(item) < 3:
            continue
        text = str(item[1] or "").strip()
        if text:
            lines.append(text)
        confidence = item[2]
        if isinstance(confidence, (int, float)):
            confidences.append(float(confidence))
    return "\n".join(lines).strip(), confidences


def _dedupe(values: list[str]) -> list[str]:
    seen: set[str] = set()
    result: list[str] = []
    for item in values:
        value = item.strip()
        if not value or value in seen:
            continue
        seen.add(value)
        result.append(value)
    return result


__all__ = [
    "PdfOcrAdmission",
    "LOW_PRIORITY_OCR_QUEUE_FAMILY",
    "AzureDocumentIntelligencePdfProvider",
    "OcrProviderResult",
    "OcrSpacePdfProvider",
    "RapidOcrLocalPdfProvider",
    "PdfOcrProvider",
    "PdfPageScanSummary",
    "PdfScanSummary",
    "assess_pdf_ocr_admission",
    "create_configured_ocr_provider",
    "inspect_pdf_for_ocr",
    "list_available_ocr_providers",
    "normalize_ocr_provider_name",
]

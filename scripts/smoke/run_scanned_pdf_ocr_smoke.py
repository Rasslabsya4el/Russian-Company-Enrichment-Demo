from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

if __package__ in {None, ""}:
    sys.path.append(str(Path(__file__).resolve().parents[2]))

from app.documents import extract_document, inspect_pdf_for_ocr
from app.documents.ocr import OcrProviderResult, create_configured_ocr_provider


DEFAULT_SAMPLE = Path("runtime_local/output/scanned_pdf_tests/originals/bytescout_scan.pdf")
DEFAULT_OFFLINE_TRACE_TEXT = Path("runtime_local/output/scanned_pdf_tests/ocr_space_demo/bytescout_scan_ocrspace.txt")


class EvidenceBackedFixtureProvider:
    name = "fixture_offline"

    def __init__(self, text: str) -> None:
        self.text = text.strip()

    def is_configured(self) -> bool:
        return True

    def extract_pdf(self, path: Path, *, trace_dir: Path | None = None) -> OcrProviderResult:
        return OcrProviderResult(
            text=self.text,
            provider=self.name,
            quality="evidence_replay",
            warnings=["Offline smoke used evidence-backed OCR text replay."],
            trace={
                "provider": self.name,
                "mode": "offline_replay",
                "evidence_text_len": len(self.text),
                "source_pdf": str(path),
                "trace_dir": str(trace_dir) if trace_dir else "",
            },
        )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Smoke-test OCR fallback on scan-only PDF artifacts.")
    parser.add_argument(
        "--input",
        default=str(DEFAULT_SAMPLE),
        help="Path to a scan-only PDF from runtime_local artifacts.",
    )
    parser.add_argument(
        "--offline-trace-text",
        default=str(DEFAULT_OFFLINE_TRACE_TEXT),
        help="Evidence-backed OCR text file used for offline replay smoke.",
    )
    parser.add_argument(
        "--live-provider",
        default=os.getenv("DOCUMENT_OCR_PROVIDER", ""),
        help="Optional live provider for configured OCR smoke: ocr_space or azure_docintel.",
    )
    parser.add_argument(
        "--trace-dir",
        default="runtime_local/output/scanned_pdf_tests/live_smoke_trace",
        help="Directory for live OCR raw/text traces when provider is configured.",
    )
    return parser.parse_args()


def _assert(condition: bool, message: str) -> None:
    if not condition:
        raise AssertionError(message)


def main() -> int:
    args = parse_args()
    input_path = Path(args.input).expanduser()
    offline_trace_text = Path(args.offline_trace_text).expanduser()
    trace_dir = Path(args.trace_dir).expanduser()

    if not input_path.exists():
        raise FileNotFoundError(f"Scan-only sample PDF not found: {input_path}")
    if not offline_trace_text.exists():
        raise FileNotFoundError(f"Offline OCR evidence text not found: {offline_trace_text}")

    base_payload = extract_document(input_path, enable_ocr=False)
    scan_summary = inspect_pdf_for_ocr(input_path)
    print(f"BASE text_len={len(base_payload.text)} warnings={len(base_payload.warnings)}")
    print(
        "SCAN "
        f"trigger={scan_summary.trigger_reason} "
        f"should_run_ocr={scan_summary.should_run_ocr} "
        f"image_only_pages={scan_summary.image_only_pages}/{scan_summary.page_count}"
    )
    _assert(len(base_payload.text) == 0, "Expected empty base extraction for scan-only PDF.")
    _assert(scan_summary.should_run_ocr, "Expected OCR trigger for scan-only PDF.")
    _assert(scan_summary.trigger_reason == "image_only_pdf", "Expected image_only_pdf trigger for bytescout fixture.")
    _assert(base_payload.trace.get("pdf_scan", {}).get("trigger_reason") == "image_only_pdf", "Expected OCR scan trace in base payload.")

    fixture_text = offline_trace_text.read_text(encoding="utf-8", errors="replace")
    offline_payload = extract_document(
        input_path,
        ocr_provider=EvidenceBackedFixtureProvider(fixture_text),
        ocr_provider_name="fixture_offline",
    )
    print(f"OFFLINE provider={offline_payload.provider} text_len={len(offline_payload.text)} quality={offline_payload.quality}")
    print(f"OFFLINE trace={json.dumps(offline_payload.trace.get('ocr', {}), ensure_ascii=False)[:300]}")
    _assert(offline_payload.provider == "fixture_offline", "Expected fixture_offline provider in offline smoke.")
    _assert(len(offline_payload.text.strip()) > 0, "Expected non-empty OCR text from offline replay provider.")
    _assert(offline_payload.metadata.get("ocr_applied") == "true", "Expected OCR-applied metadata in offline smoke.")
    _assert(
        offline_payload.trace.get("ocr", {}).get("status") == "succeeded",
        "Expected succeeded OCR status in offline smoke.",
    )

    live_provider_name = args.live_provider.strip().lower() or None
    live_provider = create_configured_ocr_provider(live_provider_name)
    if live_provider is None:
        requested = live_provider_name or "(auto)"
        print(f"LIVE_SKIPPED provider={requested} reason=no_configured_provider")
        return 0

    live_payload = extract_document(
        input_path,
        ocr_provider=live_provider,
        ocr_provider_name=live_provider.name,
        ocr_trace_dir=trace_dir,
    )
    print(
        "LIVE "
        f"provider={live_payload.provider} "
        f"text_len={len(live_payload.text)} "
        f"confidence={live_payload.confidence} "
        f"quality={live_payload.quality}"
    )
    print(f"LIVE trace={json.dumps(live_payload.trace.get('ocr', {}), ensure_ascii=False)[:300]}")
    _assert(len(live_payload.text.strip()) > 0, "Expected non-empty OCR text from live provider.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

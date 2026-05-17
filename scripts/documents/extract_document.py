from __future__ import annotations

import argparse
import sys
from pathlib import Path

if __package__ in {None, ""}:
    sys.path.append(str(Path(__file__).resolve().parents[2]))

from app.documents.formats import (
    extract_document,
    write_csv,
    write_doc,
    write_docx,
    write_json,
    write_pdf,
    write_txt,
    write_xls,
    write_xlsx,
)


WRITERS = {
    "txt": write_txt,
    "json": write_json,
    "csv": write_csv,
    "xls": write_xls,
    "xlsx": write_xlsx,
    "doc": write_doc,
    "docx": write_docx,
    "pdf": write_pdf,
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Extract PDF/DOC/DOCX/XLS/XLSX/CSV/TXT/JSON into normalized payload and optional output formats."
    )
    parser.add_argument("--input", required=True, help="Input file path.")
    parser.add_argument(
        "--output-dir",
        default="runtime_local/output/documents_extract",
        help="Directory for extracted artifacts.",
    )
    parser.add_argument(
        "--formats",
        default="json,txt",
        help="Comma-separated output formats from: json,txt,csv,xls,xlsx,doc,docx,pdf",
    )
    parser.add_argument(
        "--stem",
        default="",
        help="Optional output file stem. Default: input file stem.",
    )
    parser.add_argument(
        "--ocr-provider",
        default="",
        help="Optional OCR provider override for scan-only PDF: ocr_space or azure_docintel.",
    )
    parser.add_argument(
        "--ocr-trace-dir",
        default="",
        help="Optional directory for OCR raw response/text traces.",
    )
    parser.add_argument(
        "--disable-ocr",
        action="store_true",
        help="Disable OCR fallback even for scan-only PDF.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    input_path = Path(args.input).expanduser()
    if not input_path.exists():
        raise FileNotFoundError(f"Input file not found: {input_path}")

    output_dir = Path(args.output_dir).expanduser()
    output_dir.mkdir(parents=True, exist_ok=True)
    stem = args.stem.strip() or input_path.stem
    ocr_trace_dir = Path(args.ocr_trace_dir).expanduser() if args.ocr_trace_dir.strip() else None

    payload = extract_document(
        input_path,
        enable_ocr=not args.disable_ocr,
        ocr_provider_name=args.ocr_provider.strip() or None,
        ocr_trace_dir=ocr_trace_dir,
    )
    targets = [part.strip().lower() for part in args.formats.split(",") if part.strip()]
    if not targets:
        raise ValueError("No output formats requested.")

    for fmt in targets:
        writer = WRITERS.get(fmt)
        if writer is None:
            raise ValueError(f"Unsupported requested format: {fmt}")
        target_path = output_dir / f"{stem}.{fmt}"
        writer(payload, target_path)
        print(f"written: {target_path}")

    print(f"source_format={payload.source_format}")
    print(f"text_len={len(payload.text)}")
    print(f"tables={len(payload.tables)}")
    if payload.provider:
        print(f"provider={payload.provider}")
    if payload.confidence is not None:
        print(f"confidence={payload.confidence}")
    if payload.quality:
        print(f"quality={payload.quality}")
    if payload.warnings:
        print("warnings:")
        for item in payload.warnings:
            print(f"- {item}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

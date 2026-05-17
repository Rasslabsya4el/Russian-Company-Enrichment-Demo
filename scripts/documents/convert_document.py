from __future__ import annotations

import argparse
import sys
from pathlib import Path

if __package__ in {None, ""}:
    sys.path.append(str(Path(__file__).resolve().parents[2]))

from app.documents.formats import convert_document


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Convert document/spreadsheet files between PDF/DOC/DOCX/XLS/XLSX/CSV/TXT/JSON."
    )
    parser.add_argument("--input", required=True, help="Input file path.")
    parser.add_argument(
        "--output",
        default="",
        help="Single output file path. Use --output-dir + --to for multiple outputs.",
    )
    parser.add_argument(
        "--output-dir",
        default="",
        help="Directory for multiple outputs.",
    )
    parser.add_argument(
        "--to",
        default="",
        help="Comma-separated target extensions for multi-output mode (example: txt,json,xlsx).",
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
    ocr_trace_dir = Path(args.ocr_trace_dir).expanduser() if args.ocr_trace_dir.strip() else None

    if args.output.strip():
        output_path = Path(args.output).expanduser()
        payload = convert_document(
            input_path,
            output_path,
            enable_ocr=not args.disable_ocr,
            ocr_provider_name=args.ocr_provider.strip() or None,
            ocr_trace_dir=ocr_trace_dir,
        )
        print(f"converted: {input_path} -> {output_path}")
        if payload.warnings:
            print("warnings:")
            for item in payload.warnings:
                print(f"- {item}")
        return 0

    targets = [part.strip().lower() for part in args.to.split(",") if part.strip()]
    if not targets:
        raise ValueError("Either --output or --to must be provided.")
    output_dir = Path(args.output_dir).expanduser() if args.output_dir.strip() else Path("runtime_local/output/documents_convert")
    output_dir.mkdir(parents=True, exist_ok=True)
    for ext in targets:
        output_path = output_dir / f"{input_path.stem}_converted.{ext}"
        payload = convert_document(
            input_path,
            output_path,
            enable_ocr=not args.disable_ocr,
            ocr_provider_name=args.ocr_provider.strip() or None,
            ocr_trace_dir=ocr_trace_dir,
        )
        print(f"converted: {input_path} -> {output_path}")
        if payload.warnings:
            print("warnings:")
            for item in payload.warnings:
                print(f"- {item}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

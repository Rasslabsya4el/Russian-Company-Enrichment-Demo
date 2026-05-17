from __future__ import annotations

import argparse
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

if __package__ in {None, ""}:
    sys.path.append(str(Path(__file__).resolve().parents[2]))

from app.documents.formats import (
    ExtractedDocument,
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


SAMPLE_PAYLOAD = ExtractedDocument(
    source_path="sample",
    source_format="txt",
    text="Factory surplus lot 42\nContact procurement@example.com\nINN 7701234567",
    tables=[[["name", "qty", "unit"], ["copper cable", "120", "kg"]]],
    sheet_names=["Sheet1"],
    metadata={"purpose": "smoke"},
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate a local sample set and smoke-test document extractors.")
    parser.add_argument(
        "--keep-dir",
        default="",
        help="Optional directory to keep generated sample/input and output files.",
    )
    return parser.parse_args()


def _materialize_samples(sample_dir: Path) -> dict[str, Path]:
    sample_dir.mkdir(parents=True, exist_ok=True)
    files = {
        "pdf": sample_dir / "sample.pdf",
        "doc": sample_dir / "sample.doc",
        "docx": sample_dir / "sample.docx",
        "xls": sample_dir / "sample.xls",
        "xlsx": sample_dir / "sample.xlsx",
        "csv": sample_dir / "sample.csv",
        "txt": sample_dir / "sample.txt",
        "json": sample_dir / "sample.json",
    }
    write_pdf(SAMPLE_PAYLOAD, files["pdf"])
    write_doc(SAMPLE_PAYLOAD, files["doc"])
    write_docx(SAMPLE_PAYLOAD, files["docx"])
    write_xls(SAMPLE_PAYLOAD, files["xls"])
    write_xlsx(SAMPLE_PAYLOAD, files["xlsx"])
    write_csv(SAMPLE_PAYLOAD, files["csv"])
    write_txt(SAMPLE_PAYLOAD, files["txt"])
    write_json(SAMPLE_PAYLOAD, files["json"])
    return files


def _meaningful(payload: ExtractedDocument) -> bool:
    if (payload.text or "").strip():
        return True
    return any(any(any(cell.strip() for cell in row) for row in table) for table in payload.tables)


def _validate(fmt: str, payload: ExtractedDocument) -> tuple[bool, str]:
    if fmt == "doc":
        if _meaningful(payload):
            return True, "text extracted"
        if any("antiword" in warning.lower() for warning in payload.warnings):
            return True, "warning emitted"
        return False, "expected extracted text or explicit antiword warning"
    if _meaningful(payload):
        return True, "non-empty result"
    return False, "expected non-empty text or table result"


def _run_cli_probe(input_path: Path, output_dir: Path) -> None:
    cmd = [
        sys.executable,
        str(Path(__file__).resolve().parents[1] / "documents" / "extract_document.py"),
        "--input",
        str(input_path),
        "--output-dir",
        str(output_dir),
        "--formats",
        "json,txt",
    ]
    completed = subprocess.run(cmd, capture_output=True, text=True, check=False)
    if completed.returncode != 0:
        raise RuntimeError(f"CLI extractor failed with rc={completed.returncode}: {completed.stderr.strip()}")
    if "written:" not in completed.stdout:
        raise RuntimeError("CLI extractor did not report written outputs.")


def main() -> int:
    args = parse_args()
    temp_root = Path(args.keep_dir).expanduser() if args.keep_dir.strip() else Path(tempfile.mkdtemp(prefix="document-smoke-"))
    cleanup = not args.keep_dir.strip()
    sample_dir = temp_root / "samples"
    cli_output_dir = temp_root / "cli_outputs"

    try:
        files = _materialize_samples(sample_dir)
        _run_cli_probe(files["pdf"], cli_output_dir)

        failures: list[str] = []
        for fmt, path in files.items():
            payload = extract_document(path)
            ok, note = _validate(fmt, payload)
            status = "PASS" if ok else "FAIL"
            print(f"{status} {fmt}: {note}; text_len={len(payload.text)} warnings={len(payload.warnings)}")
            if payload.warnings:
                for warning in payload.warnings:
                    print(f"  warning: {warning}")
            if not ok:
                failures.append(f"{fmt}: {note}")

        antiword = shutil.which("antiword")
        print(f"antiword={'present' if antiword else 'missing'}")
        print(f"samples_dir={sample_dir}")
        print(f"cli_output_dir={cli_output_dir}")
        if failures:
            raise RuntimeError("Smoke failures: " + "; ".join(failures))
        return 0
    finally:
        if cleanup:
            shutil.rmtree(temp_root, ignore_errors=True)


if __name__ == "__main__":
    raise SystemExit(main())

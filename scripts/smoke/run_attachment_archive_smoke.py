from __future__ import annotations

import argparse
import shutil
import sys
import tempfile
import zipfile
from pathlib import Path

if __package__ in {None, ""}:
    sys.path.append(str(Path(__file__).resolve().parents[2]))

from app.documents.attachments import AttachmentAcquirer, AttachmentRecord, _document_text
from scripts.smoke.run_document_formats_smoke import _materialize_samples


FIXTURE_ROOT = Path(__file__).resolve().parent / "fixtures" / "archives"
RAR_FIXTURE = FIXTURE_ROOT / "rar" / "testfile.rar5.rar"
SEVEN_PUBLIC_FIXTURE = FIXTURE_ROOT / "7z" / "public_deflate.7z"
SEVEN_SAMPLE_FIXTURE = FIXTURE_ROOT / "7z" / "sample_bundle.7z"
SEVEN_UNSAFE_FIXTURE = FIXTURE_ROOT / "7z" / "unsafe_paths.7z"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Offline smoke for zip/rar/7z attachment acquisition.")
    parser.add_argument(
        "--keep-dir",
        default="",
        help="Optional directory to keep generated temporary archives and ingest artifacts.",
    )
    return parser.parse_args()


def _assert(condition: bool, message: str) -> None:
    if not condition:
        raise RuntimeError(message)


def _print_header(title: str) -> None:
    print(f"=== {title} ===")


def _print_ledger(record: AttachmentRecord) -> None:
    ledger = record.ledger
    print(
        "LEDGER "
        f"status={ledger.fetch_status} "
        f"kind={ledger.entry_kind} "
        f"filename={ledger.filename} "
        f"size={ledger.size} "
        f"checksum={ledger.checksum[:12]} "
        f"source_url={ledger.source_url} "
        f"referrer_url={ledger.referrer_url or '-'}"
    )
    text = _document_text(record.extracted)
    if text:
        preview = text.splitlines()[0][:120]
        source_format = record.extracted.source_format if record.extracted else ""
        print(f"TEXT filename={ledger.filename} format={source_format} text_len={len(text)} preview={preview}")
    for warning in ledger.warnings:
        print(f"WARNING filename={ledger.filename} message={warning}")


def _build_sample_zip(sample_dir: Path, archive_path: Path) -> None:
    files = _materialize_samples(sample_dir)
    archive_path.parent.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(archive_path, mode="w", compression=zipfile.ZIP_DEFLATED) as archive:
        for key in ("txt", "csv", "pdf"):
            archive.write(files[key], arcname=f"bundle/{files[key].name}")


def _build_unsafe_zip(sample_dir: Path, archive_path: Path) -> None:
    files = _materialize_samples(sample_dir)
    archive_path.parent.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(archive_path, mode="w", compression=zipfile.ZIP_DEFLATED) as archive:
        archive.writestr("../escape.txt", "escape payload")
        archive.write(files["txt"], arcname="safe/smoke.txt")


def _records_by_kind(records: list[AttachmentRecord]) -> tuple[list[AttachmentRecord], list[AttachmentRecord]]:
    archives = [record for record in records if record.ledger.entry_kind == "attachment"]
    members = [record for record in records if record.ledger.entry_kind == "archive_member"]
    return archives, members


def _member_map(records: list[AttachmentRecord]) -> dict[str, AttachmentRecord]:
    return {record.ledger.filename: record for record in records if record.ledger.entry_kind == "archive_member"}


def _validate_bundle_records(
    records: list[AttachmentRecord],
    *,
    expected_archive_name: str,
    expected_members: list[str],
    expected_text_checks: dict[str, str],
    expected_pdf_member: str | None = None,
) -> None:
    archives, members = _records_by_kind(records)
    _assert(len(archives) == 1, f"{expected_archive_name}: expected one outer archive ledger entry.")
    _assert(archives[0].ledger.fetch_status == "archive_extracted", f"{expected_archive_name}: expected archive_extracted outer status.")
    member_map = _member_map(records)
    for member_name in expected_members:
        _assert(member_name in member_map, f"{expected_archive_name}: missing member ledger entry for {member_name}.")
        _assert(member_map[member_name].ledger.fetch_status == "archive_member_extracted", f"{expected_archive_name}: member {member_name} not extracted.")
    for member_name, expected_snippet in expected_text_checks.items():
        text = _document_text(member_map[member_name].extracted)
        _assert(expected_snippet in text, f"{expected_archive_name}: member {member_name} text check failed.")
    if expected_pdf_member:
        payload = member_map[expected_pdf_member].extracted
        _assert(payload is not None and payload.source_format == "pdf", f"{expected_archive_name}: expected PDF member to go through extract_document().")


def _validate_unsafe_records(records: list[AttachmentRecord], *, expected_archive_name: str, safe_member_name: str) -> None:
    archives, members = _records_by_kind(records)
    _assert(len(archives) == 1, f"{expected_archive_name}: expected one outer archive ledger entry.")
    _assert(archives[0].ledger.fetch_status == "archive_extracted", f"{expected_archive_name}: expected archive_extracted outer status.")
    blocked = [record for record in members if record.ledger.fetch_status == "archive_member_blocked"]
    extracted = [record for record in members if record.ledger.fetch_status == "archive_member_extracted"]
    _assert(blocked, f"{expected_archive_name}: expected blocked unsafe member.")
    _assert(any(record.ledger.filename == "../escape.txt" for record in blocked), f"{expected_archive_name}: expected ../escape.txt blocked entry.")
    _assert(any(record.ledger.filename == safe_member_name for record in extracted), f"{expected_archive_name}: expected safe member extraction.")
    safe_record = next(record for record in extracted if record.ledger.filename == safe_member_name)
    _assert("Factory surplus lot 42" in _document_text(safe_record.extracted), f"{expected_archive_name}: safe member text check failed.")


def _rar_backend_status() -> tuple[str, str]:
    try:
        import rarfile
    except Exception as exc:
        return "package_missing", str(exc)
    try:
        rarfile.tool_setup(force=True)
    except Exception as exc:
        return "backend_missing", str(exc)
    return "available", ""


def _run_rar_smoke(acquirer: AttachmentAcquirer) -> None:
    _print_header("RAR")
    _assert(RAR_FIXTURE.exists(), f"Missing RAR fixture: {RAR_FIXTURE}")
    backend_status, detail = _rar_backend_status()
    print(f"RAR_BACKEND status={backend_status} detail={detail or '-'}")
    records = acquirer.ingest_local_file(
        RAR_FIXTURE,
        source_url="https://example.test/fixtures/testfile.rar5.rar",
        referrer_url="https://example.test/documents",
    )
    for record in records:
        _print_ledger(record)
    archives, members = _records_by_kind(records)
    _assert(len(archives) == 1, "RAR: expected one outer archive ledger entry.")
    _assert(len(members) == 1, "RAR: expected one inner member ledger entry.")
    _assert(members[0].ledger.filename == "testfile.txt", "RAR: expected inner member testfile.txt.")
    _assert(members[0].ledger.fetch_status == "archive_member_extracted", "RAR: expected extracted inner text file.")
    _assert("Testing 123" in _document_text(members[0].extracted), "RAR: expected extracted text from inner text file.")
    note = "full runtime available"
    if backend_status != "available":
        note = "fixture readable without external backend; full compressed-member coverage remains limited by environment"
    print(f"RAR_RESULT status=PASS outer=1 inner=1 extracted_text=yes note={note}")


def _run_zip_smoke(acquirer: AttachmentAcquirer, temp_root: Path) -> None:
    _print_header("ZIP")
    zip_dir = temp_root / "zip"
    sample_archive = zip_dir / "sample_bundle.zip"
    unsafe_archive = zip_dir / "unsafe_paths.zip"
    _build_sample_zip(zip_dir / "sample_docs", sample_archive)
    _build_unsafe_zip(zip_dir / "unsafe_docs", unsafe_archive)

    sample_records = acquirer.ingest_local_file(
        sample_archive,
        source_url="https://example.test/files/sample_bundle.zip",
        referrer_url="https://example.test/documents",
    )
    for record in sample_records:
        _print_ledger(record)
    _validate_bundle_records(
        sample_records,
        expected_archive_name="ZIP sample_bundle.zip",
        expected_members=["bundle/sample.txt", "bundle/sample.csv", "bundle/sample.pdf"],
        expected_text_checks={
            "bundle/sample.txt": "Factory surplus lot 42",
            "bundle/sample.csv": "copper cable",
            "bundle/sample.pdf": "Factory surplus lot 42",
        },
        expected_pdf_member="bundle/sample.pdf",
    )

    unsafe_records = acquirer.ingest_local_file(
        unsafe_archive,
        source_url="https://example.test/files/unsafe_paths.zip",
        referrer_url="https://example.test/documents",
    )
    for record in unsafe_records:
        _print_ledger(record)
    _validate_unsafe_records(unsafe_records, expected_archive_name="ZIP unsafe_paths.zip", safe_member_name="safe/smoke.txt")
    print("ZIP_RESULT status=PASS outer=1 inner_members=yes extracted_text=yes pdf_member=yes unsafe_block=yes")


def _run_7z_smoke(acquirer: AttachmentAcquirer) -> None:
    _print_header("7Z")
    for path in (SEVEN_PUBLIC_FIXTURE, SEVEN_SAMPLE_FIXTURE, SEVEN_UNSAFE_FIXTURE):
        _assert(path.exists(), f"Missing 7z fixture: {path}")

    public_records = acquirer.ingest_local_file(
        SEVEN_PUBLIC_FIXTURE,
        source_url="https://example.test/fixtures/public_deflate.7z",
        referrer_url="https://example.test/documents",
    )
    for record in public_records:
        _print_ledger(record)
    archives, members = _records_by_kind(public_records)
    _assert(len(archives) == 1, "7Z public_deflate.7z: expected one outer archive ledger entry.")
    _assert(any("test1.txt" == record.ledger.filename for record in members), "7Z public_deflate.7z: expected test1.txt member.")
    _assert(any(_document_text(record.extracted).strip() for record in members), "7Z public_deflate.7z: expected extracted text in public fixture.")

    sample_records = acquirer.ingest_local_file(
        SEVEN_SAMPLE_FIXTURE,
        source_url="https://example.test/fixtures/sample_bundle.7z",
        referrer_url="https://example.test/documents",
    )
    for record in sample_records:
        _print_ledger(record)
    _validate_bundle_records(
        sample_records,
        expected_archive_name="7Z sample_bundle.7z",
        expected_members=["bundle/sample.txt", "bundle/sample.csv", "bundle/sample.pdf"],
        expected_text_checks={
            "bundle/sample.txt": "Factory surplus lot 42",
            "bundle/sample.csv": "copper cable",
            "bundle/sample.pdf": "Factory surplus lot 42",
        },
        expected_pdf_member="bundle/sample.pdf",
    )

    unsafe_records = acquirer.ingest_local_file(
        SEVEN_UNSAFE_FIXTURE,
        source_url="https://example.test/fixtures/unsafe_paths.7z",
        referrer_url="https://example.test/documents",
    )
    for record in unsafe_records:
        _print_ledger(record)
    _validate_unsafe_records(unsafe_records, expected_archive_name="7Z unsafe_paths.7z", safe_member_name="safe/smoke.txt")
    print("7Z_RESULT status=PASS public_fixture=yes outer=1 inner_members=yes extracted_text=yes pdf_member=yes unsafe_block=yes")


def main() -> int:
    args = parse_args()
    temp_root = Path(args.keep_dir).expanduser() if args.keep_dir.strip() else Path(tempfile.mkdtemp(prefix="attachment-archive-smoke-"))
    cleanup = not args.keep_dir.strip()

    try:
        acquirer = AttachmentAcquirer(temp_root / "attachment_ingest")
        _run_zip_smoke(acquirer, temp_root)
        _run_rar_smoke(acquirer)
        _run_7z_smoke(acquirer)
        print(f"fixture_root={FIXTURE_ROOT}")
        print(f"artifact_root={temp_root / 'attachment_ingest'}")
        return 0
    finally:
        if cleanup:
            shutil.rmtree(temp_root, ignore_errors=True)


if __name__ == "__main__":
    raise SystemExit(main())

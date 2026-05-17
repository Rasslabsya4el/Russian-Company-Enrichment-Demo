from __future__ import annotations

import hashlib
import mimetypes
import os
import re
import shutil
import zipfile
from collections.abc import Mapping
from dataclasses import asdict, dataclass, field
from pathlib import Path, PurePosixPath
from typing import Any, get_args
from urllib.parse import unquote, urlparse

import requests

from app.documents.formats import ExtractedDocument, SupportedFormat, detect_format, extract_document


DIRECT_DOCUMENT_FORMATS = frozenset(str(item) for item in get_args(SupportedFormat))
DIRECT_DOCUMENT_EXTENSIONS = tuple(f".{item}" for item in sorted(DIRECT_DOCUMENT_FORMATS))
ARCHIVE_FORMATS = ("zip", "rar", "7z")
ARCHIVE_EXTENSIONS = tuple(f".{item}" for item in ARCHIVE_FORMATS)
SUPPORTED_ATTACHMENT_EXTENSIONS = DIRECT_DOCUMENT_EXTENSIONS + ARCHIVE_EXTENSIONS

MIME_EXTENSION_OVERRIDES = {
    "application/json": ".json",
    "application/msword": ".doc",
    "application/pdf": ".pdf",
    "application/vnd.ms-excel": ".xls",
    "application/vnd.ms-excel.sheet.binary.macroenabled.12": ".xls",
    "application/vnd.ms-excel.sheet.macroenabled.12": ".xlsm",
    "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet": ".xlsx",
    "application/vnd.openxmlformats-officedocument.wordprocessingml.document": ".docx",
    "application/x-7z-compressed": ".7z",
    "application/x-rar": ".rar",
    "application/x-rar-compressed": ".rar",
    "application/zip": ".zip",
    "text/csv": ".csv",
    "text/json": ".json",
    "text/plain": ".txt",
}

FAILURE_LEDGER_STATUSES = {
    "archive_depth_exceeded",
    "archive_extract_failed",
    "archive_member_blocked",
    "archive_member_depth_exceeded",
    "archive_member_extract_failed",
    "archive_member_unsupported",
    "extract_failed",
    "invalid_url",
    "request_error",
    "unsupported_format",
}

LEDGER_CONTEXT_FIELDS = (
    "route_family",
    "source_page",
    "discovery_source",
    "canonical_url",
    "queue_status",
    "skip_reason",
)


@dataclass
class AttachmentLedgerEntry:
    source_url: str
    referrer_url: str
    filename: str
    mime: str
    size: int
    checksum: str
    fetch_status: str
    entry_kind: str = "attachment"
    local_path: str = ""
    archive_depth: int = 0
    parent_archive_url: str = ""
    route_family: str = ""
    source_page: str = ""
    discovery_source: str = ""
    canonical_url: str = ""
    queue_status: str = ""
    skip_reason: str = ""
    warnings: list[str] = field(default_factory=list)

    def provenance_fields(self) -> dict[str, str]:
        payload = {
            "route_family": self.route_family,
            "source_page": self.source_page,
            "discovery_source": self.discovery_source,
            "canonical_url": self.canonical_url,
        }
        return {key: value for key, value in payload.items() if value}

    def queue_decision_fields(self) -> dict[str, str]:
        payload = {
            "queue_status": self.queue_status,
            "skip_reason": self.skip_reason,
        }
        return {key: value for key, value in payload.items() if value}


@dataclass
class AttachmentRecord:
    ledger: AttachmentLedgerEntry
    extracted: ExtractedDocument | None = None

    def to_trace(self) -> dict[str, Any]:
        payload: dict[str, Any] = {"attachment": asdict(self.ledger)}
        provenance = self.ledger.provenance_fields()
        queue_decision = self.ledger.queue_decision_fields()
        if provenance:
            payload["attachment_provenance"] = provenance
        if queue_decision:
            payload["attachment_queue"] = queue_decision
        if self.extracted is None:
            return payload
        metadata = dict(self.extracted.metadata)
        trace = dict(self.extracted.trace)
        if provenance:
            metadata.setdefault("attachment_provenance", provenance)
            trace.setdefault("attachment_provenance", provenance)
        if queue_decision:
            metadata.setdefault("attachment_queue", queue_decision)
            trace.setdefault("attachment_queue", queue_decision)
        payload["document"] = {
            "source_format": self.extracted.source_format,
            "source_path": self.extracted.source_path,
            "metadata": metadata,
            "warnings": list(self.extracted.warnings),
            "provider": self.extracted.provider,
            "confidence": self.extracted.confidence,
            "quality": self.extracted.quality,
            "sheet_names": list(self.extracted.sheet_names),
            "trace": trace,
        }
        return payload


@dataclass
class ArchiveMemberSpec:
    name: str
    size: int
    is_dir: bool = False


def _normalize_mime(value: str | None) -> str:
    return (value or "").split(";", 1)[0].strip().lower()


def _sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _sha256_path(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as fp:
        for chunk in iter(lambda: fp.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _sanitize_filename(value: str) -> str:
    cleaned = value.replace("\\", "/").strip().split("/")[-1]
    cleaned = re.sub(r"[^\w.\-]+", "_", cleaned, flags=re.UNICODE).strip("._")
    return cleaned or "attachment"


def _guess_extension(filename: str, mime: str) -> str:
    suffix = Path(filename).suffix.lower()
    if suffix:
        return suffix
    override = MIME_EXTENSION_OVERRIDES.get(mime)
    if override:
        return override
    guessed, _encoding = mimetypes.guess_type(filename)
    if guessed and guessed in MIME_EXTENSION_OVERRIDES:
        return MIME_EXTENSION_OVERRIDES[guessed]
    return ""


def is_supported_attachment_url(url: str) -> bool:
    path = urlparse(url).path.lower()
    return any(path.endswith(ext) for ext in SUPPORTED_ATTACHMENT_EXTENSIONS)


def _looks_like_supported_attachment(url: str, mime: str) -> bool:
    if is_supported_attachment_url(url):
        return True
    return _guess_extension(urlparse(url).path, mime) in SUPPORTED_ATTACHMENT_EXTENSIONS


def _filename_from_content_disposition(value: str) -> str:
    if not value:
        return ""
    encoded = re.search(r"filename\*\s*=\s*([^']*)''([^;]+)", value, flags=re.IGNORECASE)
    if encoded:
        encoding = encoded.group(1) or "utf-8"
        return unquote(encoded.group(2), encoding=encoding, errors="replace").strip().strip('"')
    plain = re.search(r'filename\s*=\s*"([^"]+)"', value, flags=re.IGNORECASE)
    if plain:
        return plain.group(1).strip()
    fallback = re.search(r"filename\s*=\s*([^;]+)", value, flags=re.IGNORECASE)
    if fallback:
        return fallback.group(1).strip().strip('"')
    return ""


def _resolve_filename(source_url: str, *, content_disposition: str, mime: str, checksum: str) -> str:
    filename = _filename_from_content_disposition(content_disposition)
    if not filename:
        parsed = urlparse(source_url)
        filename = Path(unquote(parsed.path)).name
    filename = _sanitize_filename(filename or f"attachment_{checksum[:12]}")
    suffix = _guess_extension(filename, mime)
    if suffix and Path(filename).suffix.lower() != suffix:
        filename = f"{filename}{suffix}"
    return filename


def _safe_relative_archive_path(name: str) -> PurePosixPath | None:
    normalized = (name or "").replace("\\", "/").strip().strip("/")
    if not normalized:
        return None
    pure = PurePosixPath(normalized)
    if pure.is_absolute():
        return None
    if pure.parts and ":" in pure.parts[0]:
        return None
    if any(part in {"", ".", ".."} for part in pure.parts):
        return None
    return pure


def _ensure_within_root(path: Path, root: Path) -> bool:
    try:
        path.resolve().relative_to(root.resolve())
        return True
    except ValueError:
        return False


def _document_text(payload: ExtractedDocument | None) -> str:
    if payload is None:
        return ""
    text = (payload.text or "").strip()
    if text:
        return text
    if not payload.tables:
        return ""
    lines: list[str] = []
    for table in payload.tables:
        for row in table:
            if any(cell for cell in row):
                lines.append(" | ".join(row))
    return "\n".join(lines).strip()


def _apply_ledger_context(ledger: AttachmentLedgerEntry, ledger_context: Mapping[str, Any] | None) -> AttachmentLedgerEntry:
    if not ledger_context:
        return ledger
    for field_name in LEDGER_CONTEXT_FIELDS:
        if field_name not in ledger_context:
            continue
        value = ledger_context[field_name]
        if value is None:
            continue
        setattr(ledger, field_name, str(value).strip())
    return ledger


def _ledger_context_kwargs(ledger: AttachmentLedgerEntry) -> dict[str, str]:
    return {field_name: getattr(ledger, field_name) for field_name in LEDGER_CONTEXT_FIELDS}


class AttachmentAcquirer:
    def __init__(
        self,
        storage_root: Path,
        *,
        client: Any | None = None,
        max_archive_depth: int | None = None,
        enable_ocr: bool = True,
        ocr_provider_name: str | None = None,
        ocr_trace_dir: Path | None = None,
        ocr_execution_context: Any | None = None,
    ) -> None:
        self.client = client
        self.storage_root = storage_root
        self.download_root = storage_root / "downloads"
        self.archive_root = storage_root / "archive_members"
        self.max_archive_depth = max_archive_depth if max_archive_depth is not None else max(1, int(os.getenv("ATTACHMENT_ARCHIVE_MAX_DEPTH", "2")))
        self.enable_ocr = enable_ocr
        self.ocr_provider_name = ocr_provider_name
        self.ocr_trace_dir = ocr_trace_dir
        self.ocr_execution_context = ocr_execution_context
        self.download_root.mkdir(parents=True, exist_ok=True)
        self.archive_root.mkdir(parents=True, exist_ok=True)

    def looks_like_attachment(self, source_url: str, mime: str = "") -> bool:
        return _looks_like_supported_attachment(source_url, _normalize_mime(mime))

    def acquire_from_url(
        self,
        source_url: str,
        *,
        referrer_url: str,
        ledger_context: Mapping[str, Any] | None = None,
    ) -> list[AttachmentRecord]:
        if self.client is None:
            raise ValueError("AttachmentAcquirer requires client for remote downloads.")
        outcome = self.client.request(source_url, source="site_attachment", timeout=25)
        if not outcome.ok or not outcome.response:
            filename = _sanitize_filename(Path(urlparse(source_url).path).name or "attachment")
            return [
                AttachmentRecord(
                    ledger=_apply_ledger_context(
                        AttachmentLedgerEntry(
                            source_url=source_url,
                            referrer_url=referrer_url,
                            filename=filename,
                            mime="",
                            size=0,
                            checksum="",
                            fetch_status=outcome.status or "request_error",
                            entry_kind="attachment",
                            warnings=[outcome.error or outcome.status],
                        ),
                        ledger_context,
                    )
                )
            ]
        return self.ingest_response(
            outcome.response,
            source_url=source_url,
            referrer_url=referrer_url,
            ledger_context=ledger_context,
        )

    def ingest_response(
        self,
        response: requests.Response,
        *,
        source_url: str,
        referrer_url: str,
        ledger_context: Mapping[str, Any] | None = None,
    ) -> list[AttachmentRecord]:
        mime = _normalize_mime(response.headers.get("Content-Type", ""))
        data = response.content or b""
        checksum = _sha256_bytes(data)
        filename = _resolve_filename(
            source_url,
            content_disposition=response.headers.get("Content-Disposition", ""),
            mime=mime,
            checksum=checksum,
        )
        local_path = self._write_downloaded_bytes(data, filename=filename, checksum=checksum)
        return self._process_local_path(
            local_path,
            source_url=source_url,
            referrer_url=referrer_url,
            filename=filename,
            mime=mime,
            archive_depth=0,
            parent_archive_url="",
            entry_kind="attachment",
            ledger_context=ledger_context,
        )

    def ingest_local_file(
        self,
        path: Path,
        *,
        source_url: str | None = None,
        referrer_url: str = "",
        ledger_context: Mapping[str, Any] | None = None,
    ) -> list[AttachmentRecord]:
        data = path.read_bytes()
        checksum = _sha256_bytes(data)
        filename = _sanitize_filename(path.name)
        local_path = self._write_downloaded_bytes(data, filename=filename, checksum=checksum)
        mime = _normalize_mime(mimetypes.guess_type(path.name)[0] or "")
        return self._process_local_path(
            local_path,
            source_url=source_url or path.resolve().as_uri(),
            referrer_url=referrer_url,
            filename=filename,
            mime=mime,
            archive_depth=0,
            parent_archive_url="",
            entry_kind="attachment",
            ledger_context=ledger_context,
        )

    def _write_downloaded_bytes(self, data: bytes, *, filename: str, checksum: str) -> Path:
        safe_name = _sanitize_filename(filename)
        target = self.download_root / f"{checksum[:12]}_{safe_name}"
        if not target.exists():
            target.write_bytes(data)
        return target

    def _process_local_path(
        self,
        local_path: Path,
        *,
        source_url: str,
        referrer_url: str,
        filename: str,
        mime: str,
        archive_depth: int,
        parent_archive_url: str,
        entry_kind: str,
        ledger_context: Mapping[str, Any] | None = None,
    ) -> list[AttachmentRecord]:
        checksum = _sha256_path(local_path)
        size = local_path.stat().st_size
        suffix = Path(filename).suffix.lower() or Path(local_path.name).suffix.lower()
        fmt = suffix.lstrip(".")
        ledger = _apply_ledger_context(
            AttachmentLedgerEntry(
                source_url=source_url,
                referrer_url=referrer_url,
                filename=filename,
                mime=mime or _normalize_mime(mimetypes.guess_type(filename)[0] or ""),
                size=size,
                checksum=checksum,
                fetch_status="downloaded",
                entry_kind=entry_kind,
                local_path=str(local_path),
                archive_depth=archive_depth,
                parent_archive_url=parent_archive_url,
            ),
            ledger_context,
        )

        if fmt in DIRECT_DOCUMENT_FORMATS:
            return [self._extract_file_record(local_path, ledger)]
        if fmt in ARCHIVE_FORMATS:
            records = self._expand_archive(local_path, ledger)
            if ledger.fetch_status == "downloaded":
                has_successful_members = any(
                    record.ledger.entry_kind == "archive_member" and record.ledger.fetch_status not in FAILURE_LEDGER_STATUSES
                    for record in records[1:]
                )
                ledger.fetch_status = "archive_extracted" if has_successful_members else "archive_empty"
            return records

        ledger.fetch_status = "archive_member_unsupported" if entry_kind == "archive_member" else "unsupported_format"
        ledger.warnings.append(f"Unsupported attachment format: {suffix or 'unknown'}")
        return [AttachmentRecord(ledger=ledger)]

    def _extract_file_record(self, local_path: Path, ledger: AttachmentLedgerEntry) -> AttachmentRecord:
        try:
            payload = extract_document(
                local_path,
                enable_ocr=self.enable_ocr,
                ocr_provider_name=self.ocr_provider_name,
                ocr_trace_dir=self.ocr_trace_dir,
                ocr_execution_context=self.ocr_execution_context,
            )
        except Exception as exc:
            ledger.fetch_status = "archive_member_extract_failed" if ledger.entry_kind == "archive_member" else "extract_failed"
            ledger.warnings.append(str(exc))
            return AttachmentRecord(ledger=ledger)
        ledger.fetch_status = "archive_member_extracted" if ledger.entry_kind == "archive_member" else "extracted"
        return AttachmentRecord(ledger=ledger, extracted=payload)

    def _expand_archive(self, archive_path: Path, parent_ledger: AttachmentLedgerEntry) -> list[AttachmentRecord]:
        if parent_ledger.archive_depth >= self.max_archive_depth:
            parent_ledger.fetch_status = "archive_member_depth_exceeded" if parent_ledger.entry_kind == "archive_member" else "archive_depth_exceeded"
            parent_ledger.warnings.append(f"Archive depth {parent_ledger.archive_depth} exceeds max depth {self.max_archive_depth}.")
            return [AttachmentRecord(ledger=parent_ledger)]

        try:
            members = self._list_archive_members(archive_path)
        except Exception as exc:
            parent_ledger.fetch_status = "archive_member_extract_failed" if parent_ledger.entry_kind == "archive_member" else "archive_extract_failed"
            parent_ledger.warnings.append(str(exc))
            return [AttachmentRecord(ledger=parent_ledger)]

        extraction_root = self.archive_root / f"{parent_ledger.checksum[:12]}_d{parent_ledger.archive_depth + 1}"
        extraction_root.mkdir(parents=True, exist_ok=True)

        records: list[AttachmentRecord] = [AttachmentRecord(ledger=parent_ledger)]
        valid_members: list[tuple[ArchiveMemberSpec, PurePosixPath]] = []
        blocked_records: list[AttachmentRecord] = []
        for member in members:
            if member.is_dir:
                continue
            relative_path = _safe_relative_archive_path(member.name)
            if relative_path is None:
                blocked_records.append(
                    AttachmentRecord(
                        ledger=AttachmentLedgerEntry(
                            source_url=f"{parent_ledger.source_url}!/{member.name}",
                            referrer_url=parent_ledger.source_url,
                            filename=member.name,
                            mime=_normalize_mime(mimetypes.guess_type(member.name)[0] or ""),
                            size=member.size,
                            checksum="",
                            fetch_status="archive_member_blocked",
                            entry_kind="archive_member",
                            archive_depth=parent_ledger.archive_depth + 1,
                            parent_archive_url=parent_ledger.source_url,
                            warnings=["Blocked unsafe archive member path."],
                            **_ledger_context_kwargs(parent_ledger),
                        )
                    )
                )
                continue
            valid_members.append((member, relative_path))

        try:
            self._extract_valid_members(archive_path, extraction_root, valid_members)
        except Exception as exc:
            parent_ledger.fetch_status = "archive_member_extract_failed" if parent_ledger.entry_kind == "archive_member" else "archive_extract_failed"
            parent_ledger.warnings.append(str(exc))
            return [AttachmentRecord(ledger=parent_ledger)] + blocked_records

        records.extend(blocked_records)
        for member, relative_path in valid_members:
            extracted_path = extraction_root / Path(*relative_path.parts)
            if not extracted_path.exists() or not _ensure_within_root(extracted_path, extraction_root):
                records.append(
                    AttachmentRecord(
                        ledger=AttachmentLedgerEntry(
                            source_url=f"{parent_ledger.source_url}!/{relative_path.as_posix()}",
                            referrer_url=parent_ledger.source_url,
                            filename=relative_path.as_posix(),
                            mime=_normalize_mime(mimetypes.guess_type(relative_path.name)[0] or ""),
                            size=member.size,
                            checksum="",
                            fetch_status="archive_member_extract_failed",
                            entry_kind="archive_member",
                            archive_depth=parent_ledger.archive_depth + 1,
                            parent_archive_url=parent_ledger.source_url,
                            warnings=["Archive member missing after extraction."],
                            **_ledger_context_kwargs(parent_ledger),
                        )
                    )
                )
                continue
            if extracted_path.is_symlink():
                records.append(
                    AttachmentRecord(
                        ledger=AttachmentLedgerEntry(
                            source_url=f"{parent_ledger.source_url}!/{relative_path.as_posix()}",
                            referrer_url=parent_ledger.source_url,
                            filename=relative_path.as_posix(),
                            mime=_normalize_mime(mimetypes.guess_type(relative_path.name)[0] or ""),
                            size=member.size,
                            checksum="",
                            fetch_status="archive_member_blocked",
                            entry_kind="archive_member",
                            archive_depth=parent_ledger.archive_depth + 1,
                            parent_archive_url=parent_ledger.source_url,
                            warnings=["Blocked symlink archive member."],
                            **_ledger_context_kwargs(parent_ledger),
                        )
                    )
                )
                continue
            member_source_url = f"{parent_ledger.source_url}!/{relative_path.as_posix()}"
            records.extend(
                self._process_local_path(
                    extracted_path,
                    source_url=member_source_url,
                    referrer_url=parent_ledger.source_url,
                    filename=relative_path.as_posix(),
                    mime=_normalize_mime(mimetypes.guess_type(relative_path.name)[0] or ""),
                    archive_depth=parent_ledger.archive_depth + 1,
                    parent_archive_url=parent_ledger.source_url,
                    entry_kind="archive_member",
                    ledger_context=_ledger_context_kwargs(parent_ledger),
                )
            )
        return records

    def _list_archive_members(self, archive_path: Path) -> list[ArchiveMemberSpec]:
        fmt = detect_format(archive_path)
        if fmt == "zip":
            with zipfile.ZipFile(archive_path) as archive:
                return [ArchiveMemberSpec(name=info.filename, size=info.file_size, is_dir=info.is_dir()) for info in archive.infolist()]
        if fmt == "rar":
            import rarfile

            with rarfile.RarFile(archive_path) as archive:
                return [
                    ArchiveMemberSpec(
                        name=info.filename,
                        size=int(getattr(info, "file_size", 0) or 0),
                        is_dir=bool(getattr(info, "isdir", lambda: False)()),
                    )
                    for info in archive.infolist()
                ]
        if fmt == "7z":
            import py7zr

            with py7zr.SevenZipFile(archive_path, mode="r") as archive:
                infos = list(archive.list())
            return [
                ArchiveMemberSpec(
                    name=str(getattr(info, "filename", "")),
                    size=int(getattr(info, "uncompressed", 0) or 0),
                    is_dir=bool(getattr(info, "is_directory", False)),
                )
                for info in infos
            ]
        raise ValueError(f"Unsupported archive format: {fmt}")

    def _extract_valid_members(
        self,
        archive_path: Path,
        extraction_root: Path,
        members: list[tuple[ArchiveMemberSpec, PurePosixPath]],
    ) -> None:
        if not members:
            return
        fmt = detect_format(archive_path)
        if fmt == "zip":
            with zipfile.ZipFile(archive_path) as archive:
                for member, relative_path in members:
                    target = extraction_root / Path(*relative_path.parts)
                    target.parent.mkdir(parents=True, exist_ok=True)
                    with archive.open(member.name) as src, target.open("wb") as dst:
                        shutil.copyfileobj(src, dst)
            return
        if fmt == "rar":
            import rarfile

            with rarfile.RarFile(archive_path) as archive:
                for member, relative_path in members:
                    target = extraction_root / Path(*relative_path.parts)
                    target.parent.mkdir(parents=True, exist_ok=True)
                    with archive.open(member.name) as src, target.open("wb") as dst:
                        shutil.copyfileobj(src, dst)
            return
        if fmt == "7z":
            import py7zr

            with py7zr.SevenZipFile(archive_path, mode="r") as archive:
                available = set(str(name) for name in archive.getnames())
                targets: set[str] = set()
                for member, relative_path in members:
                    member_name = str(member.name)
                    if member_name in available:
                        targets.add(member_name)
                    parts: list[str] = []
                    for part in relative_path.parts[:-1]:
                        parts.append(part)
                        candidate = "/".join(parts)
                        if candidate in available:
                            targets.add(candidate)
                if members and not targets:
                    raise ValueError("Unable to resolve safe 7z targets for extraction.")
                archive.extract(path=extraction_root, targets=sorted(targets))
            return
        raise ValueError(f"Unsupported archive format: {fmt}")


__all__ = [
    "ARCHIVE_EXTENSIONS",
    "AttachmentAcquirer",
    "AttachmentLedgerEntry",
    "AttachmentRecord",
    "DIRECT_DOCUMENT_EXTENSIONS",
    "DIRECT_DOCUMENT_FORMATS",
    "FAILURE_LEDGER_STATUSES",
    "SUPPORTED_ATTACHMENT_EXTENSIONS",
    "_document_text",
    "is_supported_attachment_url",
]

from __future__ import annotations

from copy import deepcopy
from datetime import datetime, timezone
import json
import re
import shutil
from pathlib import Path, PurePosixPath, PureWindowsPath

from .archive import (
    ARCHIVE_MANIFEST_FILENAME,
    dossier_store_archive_manifest_from_dict,
    dossier_store_archive_manifest_to_dict,
)
from .models import CompanyDossier, DossierDocumentRecord, EvidenceReference
from .serialization import (
    ARCHIVE_MANIFEST_REVISION_LAYOUT,
    dossier_from_dict,
    dossier_store_company_manifest_from_dict,
    dossier_store_company_manifest_to_dict,
    dossier_store_revision_manifest_from_dict,
    dossier_store_revision_manifest_to_dict,
    dossier_to_dict,
)


COMPANY_MANIFEST_FILENAME = "company_store.json"
LATEST_MANIFEST_FILENAME = "latest.json"
REVISIONS_DIRNAME = "revisions"
REVISION_MANIFEST_FILENAME = "revision.json"
DOSSIER_FILENAME = "company_dossier.json"
ATTACHMENTS_DIRNAME = "attachments"
LEGACY_VERSION_ID = "legacy"


class DossierStoreNotFoundError(FileNotFoundError):
    pass


class DossierStoreCompanyNameMismatchError(LookupError):
    pass


class CompanyDossierStore:
    def __init__(self, output_root: str | Path) -> None:
        self.output_root = Path(output_root)

    def write(self, dossier: CompanyDossier) -> Path:
        self.output_root.mkdir(parents=True, exist_ok=True)
        dossier_dir = self._company_directory_for_write(dossier)
        dossier_dir.mkdir(parents=True, exist_ok=True)
        persisted_source_lookup = self._build_persisted_source_lookup(dossier_dir)
        revision_id = self._next_revision_id(dossier_dir)
        revision_dir = dossier_dir / REVISIONS_DIRNAME / revision_id
        revision_dir.mkdir(parents=True, exist_ok=False)
        persisted_dossier = deepcopy(dossier)
        archive_files: dict[str, dict[str, object]] = {}
        self._copy_local_attachments(
            dossier,
            persisted_dossier,
            revision_dir,
            persisted_source_lookup,
            archive_files,
        )
        self._refresh_procedure_records(dossier)
        self._refresh_procedure_records(persisted_dossier)
        json_path = revision_dir / DOSSIER_FILENAME
        self._write_json(json_path, dossier_to_dict(persisted_dossier))
        archive_manifest = dossier_store_archive_manifest_to_dict(
            attachments_dir=ATTACHMENTS_DIRNAME,
            files=self._archive_manifest_entries(archive_files),
        )
        self._write_json(revision_dir / ARCHIVE_MANIFEST_FILENAME, archive_manifest)
        revision_manifest = dossier_store_revision_manifest_to_dict(
            revision_id=revision_id,
            company_id=dossier.company_id,
            company_name=dossier.company_name,
            stored_at=self._now_iso(),
            dossier_filename=DOSSIER_FILENAME,
            attachments_dir=ATTACHMENTS_DIRNAME,
            revision_layout=ARCHIVE_MANIFEST_REVISION_LAYOUT,
            archive_manifest_filename=ARCHIVE_MANIFEST_FILENAME,
        )
        self._write_json(revision_dir / REVISION_MANIFEST_FILENAME, revision_manifest)
        company_manifest = dossier_store_company_manifest_to_dict(
            company_id=dossier.company_id,
            company_name=dossier.company_name,
            latest_revision_id=revision_id,
        )
        self._write_json(dossier_dir / COMPANY_MANIFEST_FILENAME, company_manifest)
        self._write_json(dossier_dir / LATEST_MANIFEST_FILENAME, revision_manifest)
        return json_path

    def load(self, company_id: str, version: str, *, company_name: str | None = None) -> CompanyDossier:
        dossier_dir = self._company_directory_for_lookup(company_id, company_name=company_name)
        if version == LEGACY_VERSION_ID:
            return self._load_legacy_dossier(dossier_dir)
        revision_dir = dossier_dir / REVISIONS_DIRNAME / version
        manifest = self._load_revision_manifest(revision_dir)
        if manifest["company_id"] != company_id:
            raise ValueError(
                f"Revision {version!r} belongs to company_id={manifest['company_id']!r}, expected {company_id!r}"
            )
        archive_manifest = self._load_archive_manifest(revision_dir, manifest)
        json_path = self._resolve_revision_snapshot_path(
            revision_dir,
            manifest["dossier_filename"],
            field_name="revision.json:dossier_filename",
        )
        return self._load_dossier_snapshot(json_path, revision_dir, archive_manifest=archive_manifest)

    def load_latest(self, company_id: str, *, company_name: str | None = None) -> CompanyDossier:
        dossier_dir = self._company_directory_for_lookup(company_id, company_name=company_name)
        latest_manifest_path = dossier_dir / LATEST_MANIFEST_FILENAME
        if latest_manifest_path.is_file():
            try:
                manifest = dossier_store_revision_manifest_from_dict(self._read_json(latest_manifest_path))
            except (json.JSONDecodeError, KeyError, TypeError, ValueError):
                manifest = None
            if manifest is not None:
                try:
                    return self.load(company_id, manifest["revision_id"], company_name=company_name)
                except (FileNotFoundError, ValueError):
                    if self._is_archive_aware_revision_manifest(manifest):
                        raise
                    pass
        versions = self.list_versions(company_id, company_name=company_name)
        if not versions:
            raise FileNotFoundError(f"No dossier revisions found for company_id={company_id!r}")
        return self.load(company_id, versions[-1], company_name=company_name)

    def list_versions(self, company_id: str, *, company_name: str | None = None) -> list[str]:
        try:
            dossier_dir = self._company_directory_for_lookup(company_id, company_name=company_name)
        except DossierStoreNotFoundError:
            return []
        ordered_versions = [
            manifest["revision_id"]
            for _, manifest in self._valid_revision_manifests(dossier_dir)
            if manifest["company_id"] == company_id
        ]
        if self._legacy_dossier_path(dossier_dir).is_file():
            return [LEGACY_VERSION_ID, *ordered_versions]
        return ordered_versions

    def _reset_dossier_file_references(self, dossier: CompanyDossier) -> None:
        for entry in dossier.attachment_ledger:
            entry.dossier_file = ""
        for document in dossier.document_records:
            document.dossier_file = ""
            document.ledger.dossier_file = ""
        for entity in dossier.extracted_entities:
            for evidence in entity.evidence:
                evidence.dossier_file = ""
        for evidence in dossier.evidence_references:
            evidence.dossier_file = ""

    def _copy_local_attachments(
        self,
        dossier: CompanyDossier,
        persisted_dossier: CompanyDossier,
        dossier_dir: Path,
        persisted_source_lookup: dict[str, Path],
        archive_files: dict[str, dict[str, object]],
    ) -> None:
        attachments_dir = dossier_dir / ATTACHMENTS_DIRNAME
        copied_files: dict[str, tuple[str, str]] = {}
        runtime_ledger_locations: dict[int, tuple[str, str]] = {}
        runtime_ledger_indices: dict[int, int] = {}

        for ledger_index, (entry, persisted_entry) in enumerate(
            zip(dossier.attachment_ledger, persisted_dossier.attachment_ledger),
        ):
            runtime_ledger_indices[id(entry)] = ledger_index
            location = self._materialize_attachment(
                source=self._ledger_source_path(entry, persisted_source_lookup),
                raw_paths=[entry.local_path, entry.dossier_file],
                attachments_dir=attachments_dir,
                dossier_dir=dossier_dir,
                filename=entry.filename,
                checksum=entry.checksum,
                index=ledger_index + 1,
                copied_files=copied_files,
            )
            if location is None:
                entry.local_path = ""
                entry.dossier_file = ""
                persisted_entry.local_path = ""
                persisted_entry.dossier_file = ""
                continue
            relative_path, target_path = location
            entry.dossier_file = relative_path
            entry.local_path = target_path
            persisted_entry.dossier_file = relative_path
            persisted_entry.local_path = ""
            runtime_ledger_locations[id(entry)] = location
            self._record_archive_file(
                archive_files,
                relative_path=relative_path,
                filename=entry.filename,
                checksum=entry.checksum,
                size=entry.size,
                mime=entry.mime,
                entry_kind=entry.entry_kind,
                attachment_index=ledger_index,
            )

        for document_index, (document, persisted_document) in enumerate(
            zip(dossier.document_records, persisted_dossier.document_records),
        ):
            location = runtime_ledger_locations.get(id(document.ledger))
            if location is None:
                location = self._materialize_attachment(
                    source=self._attachment_source_path(document, persisted_source_lookup),
                    raw_paths=[
                        document.source_path,
                        document.ledger.local_path,
                        document.dossier_file,
                        document.ledger.dossier_file,
                    ],
                    attachments_dir=attachments_dir,
                    dossier_dir=dossier_dir,
                    filename=document.ledger.filename,
                    checksum=document.ledger.checksum,
                    index=document_index + 1,
                    copied_files=copied_files,
                )
                if location is not None:
                    runtime_ledger_locations[id(document.ledger)] = location
            if location is None:
                document.source_path = ""
                document.ledger.local_path = ""
                document.dossier_file = ""
                document.ledger.dossier_file = ""
                persisted_document.source_path = ""
                persisted_document.dossier_file = ""
                persisted_document.ledger.local_path = ""
                persisted_document.ledger.dossier_file = ""
                continue
            relative_path, target_path = location
            document.dossier_file = relative_path
            document.source_path = target_path
            document.ledger.dossier_file = relative_path
            document.ledger.local_path = target_path
            persisted_document.dossier_file = relative_path
            persisted_document.source_path = ""
            persisted_document.ledger.dossier_file = relative_path
            persisted_document.ledger.local_path = ""
            self._record_archive_file(
                archive_files,
                relative_path=relative_path,
                filename=document.ledger.filename,
                checksum=document.ledger.checksum,
                size=document.ledger.size,
                mime=document.ledger.mime,
                entry_kind=document.ledger.entry_kind,
                attachment_index=runtime_ledger_indices.get(id(document.ledger)),
                document_index=document_index,
            )

        for entity, persisted_entity in zip(dossier.extracted_entities, persisted_dossier.extracted_entities):
            for evidence, persisted_evidence in zip(entity.evidence, persisted_entity.evidence):
                self._normalize_evidence_reference(
                    evidence,
                    persisted_evidence,
                    copied_files,
                    persisted_source_lookup,
                    attachments_dir,
                    dossier_dir,
                    archive_files,
                )
        for evidence, persisted_evidence in zip(dossier.evidence_references, persisted_dossier.evidence_references):
            self._normalize_evidence_reference(
                evidence,
                persisted_evidence,
                copied_files,
                persisted_source_lookup,
                attachments_dir,
                dossier_dir,
                archive_files,
            )

    def _attachment_source_path(
        self,
        document: DossierDocumentRecord,
        persisted_source_lookup: dict[str, Path],
    ) -> Path | None:
        runtime_source = self._existing_source_path(document.source_path, document.ledger.local_path)
        if runtime_source is not None:
            return runtime_source
        return self._persisted_source_path(
            persisted_source_lookup,
            document.dossier_file,
            document.ledger.dossier_file,
        )

    def _ledger_source_path(self, entry, persisted_source_lookup: dict[str, Path]) -> Path | None:
        runtime_source = self._existing_source_path(entry.local_path)
        if runtime_source is not None:
            return runtime_source
        return self._persisted_source_path(persisted_source_lookup, entry.dossier_file)

    def _target_filename(self, document: DossierDocumentRecord, index: int, attachments_dir: Path) -> str:
        filename = document.ledger.filename or Path(document.source_path or document.ledger.local_path).name or "attachment"
        return self._target_filename_from_parts(
            filename=filename,
            checksum=document.ledger.checksum,
            index=index,
            attachments_dir=attachments_dir,
        )

    def _target_filename_from_parts(
        self,
        *,
        filename: str,
        checksum: str,
        index: int,
        attachments_dir: Path,
    ) -> str:
        normalized = self._sanitize_filename(filename or "attachment")
        stem = Path(normalized).stem or f"attachment_{index}"
        suffix = Path(normalized).suffix
        checksum_part = checksum[:12] if checksum else str(index)
        candidates = [f"{stem}{suffix}", f"{stem}_{checksum_part}{suffix}"]
        for candidate in candidates:
            if not (attachments_dir / candidate).exists():
                return candidate
        counter = 2
        while True:
            candidate = f"{stem}_{checksum_part}_{counter}{suffix}"
            if not (attachments_dir / candidate).exists():
                return candidate
            counter += 1

    def _normalize_evidence_reference(
        self,
        evidence: EvidenceReference,
        persisted_evidence: EvidenceReference,
        copied_files: dict[str, tuple[str, str]],
        persisted_source_lookup: dict[str, Path],
        attachments_dir: Path,
        dossier_dir: Path,
        archive_files: dict[str, dict[str, object]],
    ) -> None:
        persisted_evidence.source_path = ""
        source = self._existing_source_path(evidence.source_path)
        if source is None:
            source = self._persisted_source_path(persisted_source_lookup, evidence.dossier_file)
        if source is not None:
            location: tuple[str, str] | None = None
            for key in self._source_keys(source, [evidence.source_path, evidence.dossier_file]):
                if key in copied_files:
                    location = copied_files[key]
                    break
            if location is None:
                location = self._materialize_attachment(
                    source=source,
                    raw_paths=[evidence.source_path, evidence.dossier_file],
                    attachments_dir=attachments_dir,
                    dossier_dir=dossier_dir,
                    filename=self._evidence_filename(evidence, source),
                    checksum=evidence.checksum,
                    index=len(copied_files) + 1,
                    copied_files=copied_files,
                )
            if location is not None:
                relative_path, target_path = location
                evidence.dossier_file = relative_path
                evidence.source_path = target_path
                persisted_evidence.dossier_file = relative_path
                self._record_archive_file(
                    archive_files,
                    relative_path=relative_path,
                    filename=self._evidence_filename(evidence, source),
                    checksum=evidence.checksum,
                    size=source.stat().st_size,
                    mime="",
                    entry_kind="evidence",
                )
                return
        evidence.source_path = ""
        evidence.dossier_file = ""
        persisted_evidence.dossier_file = ""

    def _evidence_filename(self, evidence: EvidenceReference, source: Path) -> str:
        if evidence.title:
            candidate = Path(evidence.title).name
            if candidate and candidate != ".":
                return candidate
        if evidence.dossier_file:
            candidate = Path(evidence.dossier_file).name
            if candidate and candidate != ".":
                return candidate
        return source.name or "evidence"

    def _existing_source_path(self, *raw_paths: str) -> Path | None:
        for raw_path in raw_paths:
            if not raw_path:
                continue
            candidate = Path(raw_path)
            if candidate.is_file():
                return candidate
        return None

    def _persisted_source_path(
        self,
        persisted_source_lookup: dict[str, Path],
        *relative_paths: str,
    ) -> Path | None:
        for relative_path in relative_paths:
            for key in self._persisted_source_keys(relative_path):
                candidate = persisted_source_lookup.get(key)
                if candidate is not None and candidate.is_file():
                    return candidate
        return None

    def _canonical_persisted_relative_path(self, relative_path: str) -> str:
        normalized = str(relative_path or "").strip()
        if not normalized:
            return ""
        return PurePosixPath(normalized.replace("\\", "/")).as_posix()

    def _persisted_source_keys(self, relative_path: str) -> list[str]:
        raw = str(relative_path or "").strip()
        if not raw:
            return []
        canonical = self._canonical_persisted_relative_path(raw)
        keys: list[str] = []
        for key in (
            raw,
            canonical,
            raw.replace("/", "\\"),
            raw.replace("\\", "/"),
            canonical.replace("/", "\\"),
        ):
            if key and key not in keys:
                keys.append(key)
        return keys

    def _build_persisted_source_lookup(self, dossier_dir: Path) -> dict[str, Path]:
        lookup: dict[str, Path] = {}
        for _, manifest in reversed(self._valid_revision_manifests(dossier_dir)):
            revision_dir = dossier_dir / REVISIONS_DIRNAME / manifest["revision_id"]
            self._register_attachment_tree_sources(
                lookup,
                revision_dir,
                revision_dir / manifest["attachments_dir"],
            )
        self._register_attachment_tree_sources(
            lookup,
            dossier_dir,
            dossier_dir / ATTACHMENTS_DIRNAME,
        )
        return lookup

    def _register_attachment_tree_sources(
        self,
        lookup: dict[str, Path],
        dossier_root: Path,
        attachments_dir: Path,
    ) -> None:
        if not attachments_dir.is_dir():
            return
        for candidate in attachments_dir.rglob("*"):
            if not candidate.is_file():
                continue
            relative_path = str(candidate.relative_to(dossier_root))
            for key in self._persisted_source_keys(relative_path):
                lookup.setdefault(key, candidate)

    def _company_directory_for_write(self, dossier: CompanyDossier) -> Path:
        existing_dirs = self._find_company_directories(dossier.company_id)
        if len(existing_dirs) > 1:
            self._raise_ambiguous_company_write(dossier.company_id)
        if existing_dirs:
            return existing_dirs[0]
        return self.output_root / self._directory_name(dossier)

    def _company_directory_for_lookup(self, company_id: str, *, company_name: str | None = None) -> Path:
        matches = self._find_company_directories(company_id)
        if company_name is not None:
            for candidate in matches:
                if self._stored_company_name(candidate, company_id=company_id) == company_name:
                    return candidate
            raise DossierStoreCompanyNameMismatchError(
                f"No dossier store found for company_id={company_id!r} with company_name={company_name!r}"
            )
        if not matches:
            raise DossierStoreNotFoundError(f"No dossier store found for company_id={company_id!r}")
        if len(matches) > 1:
            self._raise_ambiguous_company_lookup(company_id)
        return matches[0]

    def _raise_ambiguous_company_lookup(self, company_id: str) -> None:
        raise ValueError(
            f"Multiple dossier stores found for company_id={company_id!r}; pass company_name to disambiguate"
        )

    def _raise_ambiguous_company_write(self, company_id: str) -> None:
        raise ValueError(
            f"Multiple dossier stores found for company_id={company_id!r}; cannot determine write target"
        )

    def _find_company_directories(self, company_id: str) -> list[Path]:
        if not self.output_root.exists():
            return []
        matches: list[Path] = []
        for candidate in self.output_root.iterdir():
            if not candidate.is_dir():
                continue
            manifest = self._load_company_manifest(candidate)
            if manifest is not None:
                if manifest["company_id"] == company_id:
                    matches.append(candidate)
                continue
            recovered_manifest = self._recover_company_manifest_from_revisions(candidate, company_id=company_id)
            if recovered_manifest is not None and recovered_manifest["company_id"] == company_id:
                matches.append(candidate)
                continue
            legacy_path = self._legacy_dossier_path(candidate)
            if not legacy_path.is_file():
                continue
            try:
                legacy_dossier = dossier_from_dict(self._read_json(legacy_path))
            except (OSError, json.JSONDecodeError, KeyError, TypeError, ValueError):
                continue
            if legacy_dossier.company_id == company_id:
                matches.append(candidate)
        matches.sort(key=lambda path: path.name)
        return matches

    def _load_revision_manifest(self, revision_dir: Path) -> dict[str, str]:
        manifest_path = revision_dir / REVISION_MANIFEST_FILENAME
        if not manifest_path.is_file():
            raise FileNotFoundError(f"Revision manifest not found: {manifest_path}")
        manifest = dossier_store_revision_manifest_from_dict(self._read_json(manifest_path))
        self._validate_revision_local_filename(manifest["dossier_filename"])
        revision_layout = manifest.get("revision_layout")
        if revision_layout is not None and revision_layout != ARCHIVE_MANIFEST_REVISION_LAYOUT:
            raise ValueError(
                f"Unsupported revision.json:revision_layout {revision_layout!r}; "
                f"expected {ARCHIVE_MANIFEST_REVISION_LAYOUT!r}"
            )
        archive_manifest_filename = manifest.get("archive_manifest_filename")
        if archive_manifest_filename:
            self._validate_revision_local_filename(archive_manifest_filename)
        if revision_layout == ARCHIVE_MANIFEST_REVISION_LAYOUT and not archive_manifest_filename:
            raise ValueError(
                "Archive-aware revision manifest must define revision.json:archive_manifest_filename"
            )
        return manifest

    def _load_company_manifest(self, dossier_dir: Path) -> dict[str, str] | None:
        manifest_path = dossier_dir / COMPANY_MANIFEST_FILENAME
        if not manifest_path.is_file():
            return None
        try:
            return dossier_store_company_manifest_from_dict(self._read_json(manifest_path))
        except (OSError, json.JSONDecodeError, KeyError, TypeError, ValueError):
            return None

    def _valid_revision_manifests(self, dossier_dir: Path) -> list[tuple[datetime, dict[str, str]]]:
        revisions_dir = dossier_dir / REVISIONS_DIRNAME
        manifests: list[tuple[datetime, dict[str, str]]] = []
        if not revisions_dir.is_dir():
            return manifests
        for candidate in revisions_dir.iterdir():
            if not candidate.is_dir():
                continue
            try:
                manifest = self._load_revision_manifest(candidate)
                stored_at = self._parse_stored_at(manifest["stored_at"])
            except (FileNotFoundError, json.JSONDecodeError, KeyError, TypeError, ValueError):
                continue
            manifests.append((stored_at, manifest))
        manifests.sort(key=lambda item: (item[0], item[1]["revision_id"]))
        return manifests

    def _recover_company_manifest_from_revisions(
        self,
        dossier_dir: Path,
        *,
        company_id: str | None = None,
    ) -> dict[str, str] | None:
        manifests = self._valid_revision_manifests(dossier_dir)
        if not manifests:
            return None
        if company_id is not None:
            manifests = [item for item in manifests if item[1]["company_id"] == company_id]
            if not manifests:
                return None
        else:
            company_ids = {manifest["company_id"] for _, manifest in manifests}
            if len(company_ids) != 1:
                return None
        latest_manifest = manifests[-1][1]
        return {
            "company_id": latest_manifest["company_id"],
            "company_name": latest_manifest["company_name"],
            "latest_revision_id": latest_manifest["revision_id"],
        }

    def _stored_company_name(self, dossier_dir: Path, *, company_id: str | None = None) -> str | None:
        manifest = self._load_company_manifest(dossier_dir)
        if manifest is not None:
            return manifest["company_name"]
        recovered_manifest = self._recover_company_manifest_from_revisions(dossier_dir, company_id=company_id)
        if recovered_manifest is not None:
            return recovered_manifest["company_name"]
        legacy_path = self._legacy_dossier_path(dossier_dir)
        if not legacy_path.is_file():
            return None
        try:
            legacy_dossier = dossier_from_dict(self._read_json(legacy_path))
        except (OSError, json.JSONDecodeError, KeyError, TypeError, ValueError):
            return None
        return legacy_dossier.company_name

    def _directory_name(self, dossier: CompanyDossier) -> str:
        return self._directory_name_from_parts(dossier.company_id, dossier.company_name)

    def _directory_name_from_parts(self, company_id: str, company_name: str) -> str:
        base = "_".join(part for part in (company_id, company_name) if part).strip("_")
        return self._sanitize_filename(base or "company_dossier")

    def _sanitize_filename(self, value: str) -> str:
        cleaned = re.sub(r"[^\w.\-]+", "_", value, flags=re.UNICODE).strip("._")
        return cleaned or "artifact"

    def _next_revision_id(self, dossier_dir: Path) -> str:
        revisions_dir = dossier_dir / REVISIONS_DIRNAME
        revisions_dir.mkdir(parents=True, exist_ok=True)
        base_revision_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
        revision_id = base_revision_id
        counter = 2
        while (revisions_dir / revision_id).exists():
            revision_id = f"{base_revision_id}_{counter}"
            counter += 1
        return revision_id

    def _read_json(self, path: Path) -> dict[str, object]:
        return json.loads(path.read_text(encoding="utf-8"))

    def _write_json(self, path: Path, payload: dict[str, object]) -> None:
        path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

    def _now_iso(self) -> str:
        return datetime.now(timezone.utc).isoformat()

    def _legacy_dossier_path(self, dossier_dir: Path) -> Path:
        return dossier_dir / DOSSIER_FILENAME

    def _load_legacy_dossier(self, dossier_dir: Path) -> CompanyDossier:
        legacy_path = self._legacy_dossier_path(dossier_dir)
        if not legacy_path.is_file():
            raise FileNotFoundError(f"Legacy dossier snapshot not found: {legacy_path}")
        json_path = self._resolve_revision_snapshot_path(
            dossier_dir,
            legacy_path.name,
            field_name="legacy:dossier_filename",
        )
        return self._load_dossier_snapshot(json_path, dossier_dir)

    def _load_dossier_snapshot(
        self,
        json_path: Path,
        dossier_root: Path,
        *,
        archive_manifest: dict[str, object] | None = None,
    ) -> CompanyDossier:
        dossier = dossier_from_dict(self._read_json(json_path))
        if archive_manifest is not None:
            self._validate_archive_bindings(dossier, archive_manifest)
        self._hydrate_persisted_paths(dossier, dossier_root, archive_manifest=archive_manifest)
        self._refresh_procedure_records(dossier)
        return dossier

    def _hydrate_persisted_paths(
        self,
        dossier: CompanyDossier,
        dossier_root: Path,
        *,
        archive_manifest: dict[str, object] | None = None,
    ) -> None:
        archive_lookup = self._archive_manifest_lookup(archive_manifest, dossier_root) if archive_manifest else None
        if archive_lookup is not None:
            self._clear_runtime_paths(dossier)
        for document_index, document in enumerate(dossier.document_records):
            persisted_path = self._resolve_hydrated_path(
                dossier_root,
                self._effective_document_dossier_file(document, document_index=document_index),
                field_name="document_records[].dossier_file",
                archive_lookup=archive_lookup,
            )
            if persisted_path is None:
                continue
            persisted_str = str(persisted_path)
            document.source_path = persisted_str
            document.ledger.local_path = persisted_str
        for entry in dossier.attachment_ledger:
            persisted_path = self._resolve_hydrated_path(
                dossier_root,
                entry.dossier_file,
                field_name="attachment_ledger[].dossier_file",
                archive_lookup=archive_lookup,
            )
            if persisted_path is None:
                continue
            entry.local_path = str(persisted_path)
        for entity in dossier.extracted_entities:
            for evidence in entity.evidence:
                self._hydrate_evidence_source_path(evidence, dossier_root, archive_lookup=archive_lookup)
        for evidence in dossier.evidence_references:
            self._hydrate_evidence_source_path(evidence, dossier_root, archive_lookup=archive_lookup)

    def _effective_document_dossier_file(
        self,
        document: DossierDocumentRecord,
        *,
        document_index: int | None = None,
    ) -> str:
        document_path = self._canonical_persisted_relative_path(document.dossier_file)
        ledger_path = self._canonical_persisted_relative_path(document.ledger.dossier_file)
        if document_path and ledger_path and document_path != ledger_path:
            field_name = (
                f"document_records[{document_index}]"
                if document_index is not None
                else "document_records[]"
            )
            raise ValueError(
                f"{field_name}.dossier_file must match {field_name}.ledger.dossier_file when both are set"
            )
        return document_path or ledger_path

    def _hydrate_evidence_source_path(
        self,
        evidence: EvidenceReference,
        dossier_root: Path,
        *,
        archive_lookup: dict[str, Path] | None,
    ) -> None:
        persisted_path = self._resolve_hydrated_path(
            dossier_root,
            evidence.dossier_file,
            field_name="evidence[].dossier_file",
            archive_lookup=archive_lookup,
        )
        if persisted_path is None:
            return
        evidence.source_path = str(persisted_path)

    def _resolve_hydrated_path(
        self,
        dossier_root: Path,
        relative_path: str,
        *,
        field_name: str,
        archive_lookup: dict[str, Path] | None,
    ) -> Path | None:
        if archive_lookup is not None:
            if not relative_path:
                return None
            persisted_path = archive_lookup.get(self._canonical_persisted_relative_path(relative_path))
            if persisted_path is None:
                raise ValueError(f"{field_name} path {relative_path!r} is missing from archive manifest")
            return persisted_path
        return self._resolve_persisted_path(dossier_root, relative_path, field_name=field_name)

    def _refresh_procedure_records(self, dossier: CompanyDossier) -> None:
        if not dossier.procedure_records:
            return
        from .procedures import rebind_procedure_records

        dossier.procedure_records = rebind_procedure_records(
            procedure_records=dossier.procedure_records,
            evidence_references=dossier.evidence_references,
            document_records=dossier.document_records,
        )

    def _load_archive_manifest(
        self,
        revision_dir: Path,
        revision_manifest: dict[str, str],
    ) -> dict[str, object] | None:
        archive_manifest_filename = revision_manifest.get("archive_manifest_filename")
        if not self._is_archive_aware_revision_manifest(revision_manifest):
            return None
        if not archive_manifest_filename:
            raise ValueError("Archive-aware revision manifest is missing revision.json:archive_manifest_filename")
        manifest_path = self._resolve_revision_snapshot_path(
            revision_dir,
            archive_manifest_filename,
            field_name="revision.json:archive_manifest_filename",
        )
        archive_manifest = dossier_store_archive_manifest_from_dict(self._read_json(manifest_path))
        attachments_dir = archive_manifest.get("attachments_dir")
        if attachments_dir != revision_manifest["attachments_dir"]:
            raise ValueError("archive manifest attachments_dir must match revision.json:attachments_dir")
        return archive_manifest

    def _is_archive_aware_revision_manifest(self, manifest: dict[str, str]) -> bool:
        revision_layout = manifest.get("revision_layout")
        if revision_layout == ARCHIVE_MANIFEST_REVISION_LAYOUT:
            return True
        return bool(manifest.get("archive_manifest_filename"))

    def _archive_manifest_lookup(
        self,
        archive_manifest: dict[str, object],
        dossier_root: Path,
    ) -> dict[str, Path]:
        files = archive_manifest.get("files")
        if not isinstance(files, list):
            raise ValueError("archive manifest files must be a list")
        lookup: dict[str, Path] = {}
        for index, item in enumerate(files):
            if not isinstance(item, dict):
                raise ValueError(f"archive manifest files[{index}] must be a mapping")
            relative_path = item.get("dossier_file")
            if not isinstance(relative_path, str) or not relative_path:
                raise ValueError(f"archive manifest files[{index}].dossier_file must be a non-empty string")
            normalized_relative_path = self._canonical_persisted_relative_path(relative_path)
            persisted_path = self._resolve_persisted_path(
                dossier_root,
                normalized_relative_path,
                field_name=f"archive_manifest.files[{index}].dossier_file",
            )
            if persisted_path is None:
                raise FileNotFoundError(
                    f"Archive file not found for archive_manifest.files[{index}].dossier_file={relative_path!r}"
                )
            if normalized_relative_path in lookup:
                raise ValueError(f"Duplicate archive manifest dossier_file entry: {relative_path!r}")
            lookup[normalized_relative_path] = persisted_path
        return lookup

    def _validate_archive_bindings(
        self,
        dossier: CompanyDossier,
        archive_manifest: dict[str, object],
    ) -> None:
        files = archive_manifest.get("files")
        if not isinstance(files, list):
            raise ValueError("archive manifest files must be a list")
        referenced_attachment_indices: set[int] = set()
        referenced_document_indices: set[int] = set()
        for index, item in enumerate(files):
            if not isinstance(item, dict):
                raise ValueError(f"archive manifest files[{index}] must be a mapping")
            relative_path = item.get("dossier_file")
            if not isinstance(relative_path, str) or not relative_path:
                raise ValueError(f"archive manifest files[{index}].dossier_file must be a non-empty string")
            normalized_relative_path = self._canonical_persisted_relative_path(relative_path)
            attachment_indices = item.get("attachment_indices")
            document_indices = item.get("document_indices")
            if not isinstance(attachment_indices, list):
                raise ValueError(f"archive manifest files[{index}].attachment_indices must be a list")
            if not isinstance(document_indices, list):
                raise ValueError(f"archive manifest files[{index}].document_indices must be a list")
            for attachment_index in attachment_indices:
                if isinstance(attachment_index, bool) or not isinstance(attachment_index, int):
                    raise TypeError(
                        f"archive manifest files[{index}].attachment_indices[] must contain ints only"
                    )
                if attachment_index < 0 or attachment_index >= len(dossier.attachment_ledger):
                    raise ValueError(
                        f"archive manifest files[{index}].attachment_indices contains out-of-range index "
                        f"{attachment_index}"
                    )
                if (
                    self._canonical_persisted_relative_path(dossier.attachment_ledger[attachment_index].dossier_file)
                    != normalized_relative_path
                ):
                    raise ValueError(
                        f"archive manifest files[{index}] mismatches attachment_ledger[{attachment_index}].dossier_file"
                    )
                referenced_attachment_indices.add(attachment_index)
            for document_index in document_indices:
                if isinstance(document_index, bool) or not isinstance(document_index, int):
                    raise TypeError(
                        f"archive manifest files[{index}].document_indices[] must contain ints only"
                    )
                if document_index < 0 or document_index >= len(dossier.document_records):
                    raise ValueError(
                        f"archive manifest files[{index}].document_indices contains out-of-range index "
                        f"{document_index}"
                    )
                document = dossier.document_records[document_index]
                document_path = self._effective_document_dossier_file(document, document_index=document_index)
                if document_path != normalized_relative_path:
                    raise ValueError(
                        f"archive manifest files[{index}] mismatches document_records[{document_index}].dossier_file"
                    )
                referenced_document_indices.add(document_index)
        for attachment_index, entry in enumerate(dossier.attachment_ledger):
            if (
                self._canonical_persisted_relative_path(entry.dossier_file)
                and attachment_index not in referenced_attachment_indices
            ):
                raise ValueError(f"attachment_ledger[{attachment_index}] is missing from archive manifest")
        for document_index, document in enumerate(dossier.document_records):
            if (
                self._effective_document_dossier_file(document, document_index=document_index)
                and document_index not in referenced_document_indices
            ):
                raise ValueError(f"document_records[{document_index}] is missing from archive manifest")

    def _record_archive_file(
        self,
        archive_files: dict[str, dict[str, object]],
        *,
        relative_path: str,
        filename: str,
        checksum: str,
        size: int,
        mime: str,
        entry_kind: str,
        attachment_index: int | None = None,
        document_index: int | None = None,
    ) -> None:
        relative_path = self._canonical_persisted_relative_path(relative_path)
        file_entry = archive_files.setdefault(
            relative_path,
            {
                "dossier_file": relative_path,
                "filename": filename,
                "checksum": checksum,
                "size": size,
                "mime": mime,
                "entry_kind": entry_kind,
                "attachment_indices": [],
                "document_indices": [],
            },
        )
        if attachment_index is not None and attachment_index not in file_entry["attachment_indices"]:
            file_entry["attachment_indices"].append(attachment_index)
        if document_index is not None and document_index not in file_entry["document_indices"]:
            file_entry["document_indices"].append(document_index)

    def _archive_manifest_entries(self, archive_files: dict[str, dict[str, object]]) -> list[dict[str, object]]:
        entries = list(archive_files.values())
        entries.sort(key=lambda item: str(item["dossier_file"]))
        for entry in entries:
            entry["attachment_indices"] = sorted(set(entry["attachment_indices"]))
            entry["document_indices"] = sorted(set(entry["document_indices"]))
        return entries

    def _clear_runtime_paths(self, dossier: CompanyDossier) -> None:
        for entry in dossier.attachment_ledger:
            entry.local_path = ""
        for document in dossier.document_records:
            document.source_path = ""
            document.ledger.local_path = ""
        for entity in dossier.extracted_entities:
            for evidence in entity.evidence:
                evidence.source_path = ""
        for evidence in dossier.evidence_references:
            evidence.source_path = ""

    def _resolve_persisted_path(self, dossier_root: Path, relative_path: str, *, field_name: str) -> Path | None:
        if not relative_path:
            return None
        normalized_relative_path = self._canonical_persisted_relative_path(relative_path)
        candidate_path = PurePosixPath(normalized_relative_path)
        if candidate_path.is_absolute() or PureWindowsPath(relative_path).is_absolute():
            raise ValueError(f"Invalid {field_name} path {relative_path!r}: absolute paths are not allowed")
        if ".." in candidate_path.parts:
            raise ValueError(f"Invalid {field_name} path {relative_path!r}: parent traversal is not allowed")
        root_resolved = dossier_root.resolve(strict=False)
        try:
            relative_candidate = Path(*candidate_path.parts) if candidate_path.parts else Path(".")
            candidate = (dossier_root / relative_candidate).resolve(strict=False)
        except OSError as exc:
            raise ValueError(f"Invalid {field_name} path {relative_path!r}: {exc}") from exc
        if not self._is_within_root(candidate, root_resolved):
            raise ValueError(f"Invalid {field_name} path {relative_path!r}: path escapes revision root")
        if candidate.is_file():
            return candidate
        return None

    def _resolve_revision_snapshot_path(self, dossier_root: Path, filename: str, *, field_name: str) -> Path:
        self._validate_revision_local_filename(filename)
        root_resolved = dossier_root.resolve(strict=False)
        try:
            candidate = (dossier_root / filename).resolve(strict=False)
        except OSError as exc:
            raise ValueError(f"Invalid {field_name} path {filename!r}: {exc}") from exc
        if not self._is_within_root(candidate, root_resolved):
            raise ValueError(f"Invalid {field_name} path {filename!r}: path escapes revision root")
        if not candidate.is_file():
            raise FileNotFoundError(f"Revision snapshot not found: {candidate}")
        return candidate

    def _validate_revision_local_filename(self, filename: str) -> None:
        candidate = Path(filename)
        if candidate.is_absolute():
            raise ValueError(f"Invalid revision dossier_filename {filename!r}: absolute paths are not allowed")
        if filename in {".", ".."}:
            raise ValueError(
                f"Invalid revision dossier_filename {filename!r}: expected a local filename inside the revision root"
            )
        if candidate.name != filename or candidate.parent != Path("."):
            raise ValueError(
                f"Invalid revision dossier_filename {filename!r}: expected a local filename inside the revision root"
            )

    def _is_within_root(self, candidate: Path, root: Path) -> bool:
        try:
            candidate.relative_to(root)
        except ValueError:
            return False
        return True

    def _parse_stored_at(self, stored_at: str) -> datetime:
        normalized = stored_at.replace("Z", "+00:00")
        parsed = datetime.fromisoformat(normalized)
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return parsed.astimezone(timezone.utc)

    def _materialize_attachment(
        self,
        *,
        source: Path | None,
        raw_paths: list[str],
        attachments_dir: Path,
        dossier_dir: Path,
        filename: str,
        checksum: str,
        index: int,
        copied_files: dict[str, tuple[str, str]],
    ) -> tuple[str, str] | None:
        if source is None:
            return None
        for key in self._source_keys(source, raw_paths):
            existing = copied_files.get(key)
            if existing is not None:
                return existing
        attachments_dir.mkdir(parents=True, exist_ok=True)
        relative_path = PurePosixPath(
            ATTACHMENTS_DIRNAME,
            self._target_filename_from_parts(
                filename=filename or source.name,
                checksum=checksum,
                index=index,
                attachments_dir=attachments_dir,
            ),
        )
        relative_path_str = relative_path.as_posix()
        target_path = dossier_dir / Path(*relative_path.parts)
        shutil.copy2(source, target_path)
        target_path_str = str(target_path)
        location = (relative_path_str, target_path_str)
        for key in self._source_keys(source, raw_paths):
            copied_files[key] = location
        return location

    def _source_keys(self, source: Path, raw_paths: list[str]) -> list[str]:
        keys = [raw_path for raw_path in raw_paths if raw_path]
        try:
            resolved = str(source.resolve())
        except OSError:
            resolved = str(source)
        keys.append(str(source))
        keys.append(resolved)
        unique_keys: list[str] = []
        for key in keys:
            if key not in unique_keys:
                unique_keys.append(key)
        return unique_keys


def write_company_dossier(dossier: CompanyDossier, *, output_dir: str | Path) -> Path:
    return CompanyDossierStore(output_dir).write(dossier)

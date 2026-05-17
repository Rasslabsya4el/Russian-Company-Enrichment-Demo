from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Mapping


DOSSIER_CONTRACT_VERSION = "L5.1"


@dataclass(slots=True)
class EvidenceReference:
    evidence_type: str
    source_url: str = ""
    source_path: str = ""
    dossier_file: str = ""
    title: str = ""
    snippet: str = ""
    record_locator: str = ""
    checksum: str = ""
    metadata: dict[str, Any] = field(default_factory=dict)
    trace: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class DossierPageRecord:
    source_url: str
    site_url: str
    source_type: str
    title: str = ""
    section_guess: str = ""
    date: str = ""
    text: str = ""
    raw_text: str = ""
    cleaned_text: str = ""
    tables: list[list[list[str]]] = field(default_factory=list)
    content_fingerprint: str = ""
    fetch_status: str = ""
    relevance_label: str = "unknown"
    relevance_score: float = 0.0
    metadata: dict[str, Any] = field(default_factory=dict)
    evidence_ref: dict[str, Any] = field(default_factory=dict)
    trace: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class DossierAttachmentLedgerEntry:
    source_url: str
    referrer_url: str
    filename: str
    mime: str
    size: int
    checksum: str
    fetch_status: str
    entry_kind: str = "attachment"
    local_path: str = ""
    dossier_file: str = ""
    archive_depth: int = 0
    parent_archive_url: str = ""
    warnings: list[str] = field(default_factory=list)


@dataclass(slots=True)
class DossierDocumentRecord:
    ledger: DossierAttachmentLedgerEntry
    source_path: str = ""
    dossier_file: str = ""
    source_format: str = ""
    text: str = ""
    tables: list[list[list[str]]] = field(default_factory=list)
    sheet_names: list[str] = field(default_factory=list)
    metadata: dict[str, Any] = field(default_factory=dict)
    warnings: list[str] = field(default_factory=list)
    provider: str = ""
    confidence: float | None = None
    quality: str = ""
    trace: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class ExtractedEntity:
    entity_type: str
    value: str
    normalized_value: str = ""
    confidence: float | None = None
    evidence: list[EvidenceReference] = field(default_factory=list)
    attributes: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class CompanyDossier:
    company_id: str
    company_name: str
    site_url: str
    dossier_contract_version: str = DOSSIER_CONTRACT_VERSION
    page_records: list[DossierPageRecord] = field(default_factory=list)
    document_records: list[DossierDocumentRecord] = field(default_factory=list)
    attachment_ledger: list[DossierAttachmentLedgerEntry] = field(default_factory=list)
    extracted_entities: list[ExtractedEntity] = field(default_factory=list)
    evidence_references: list[EvidenceReference] = field(default_factory=list)
    procedure_records: list[dict[str, Any]] = field(default_factory=list)
    history: list[dict[str, Any]] = field(default_factory=list)
    reprocess_inputs: list[dict[str, Any]] = field(default_factory=list)
    okved_site_match: Any | None = None
    okved_matches: list[Any] = field(default_factory=list)
    company_metadata: dict[str, Any] = field(default_factory=dict)
    created_at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())

    def to_dict(self) -> dict[str, Any]:
        from .serialization import dossier_to_dict

        return dossier_to_dict(self)

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> CompanyDossier:
        from .serialization import dossier_from_dict

        return dossier_from_dict(data)

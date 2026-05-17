from __future__ import annotations

from collections.abc import Hashable, Mapping
from dataclasses import fields, is_dataclass
from datetime import datetime
from pathlib import PurePosixPath, PureWindowsPath
from typing import Any

from .models import (
    DOSSIER_CONTRACT_VERSION,
    CompanyDossier,
    DossierAttachmentLedgerEntry,
    DossierDocumentRecord,
    DossierPageRecord,
    EvidenceReference,
    ExtractedEntity,
)

DOSSIER_STORE_LAYOUT_VERSION = "L5.2"
ARCHIVE_MANIFEST_REVISION_LAYOUT = "archive_manifest"


def dossier_to_dict(dossier: CompanyDossier) -> dict[str, Any]:
    version = _validate_string(dossier.dossier_contract_version, field_name="dossier_contract_version", allow_empty=False)
    if version != DOSSIER_CONTRACT_VERSION:
        raise ValueError(f"Unsupported dossier_contract_version: {version}")
    page_records = _validate_list(dossier.page_records, field_name="page_records")
    document_records = _validate_list(dossier.document_records, field_name="document_records")
    attachment_ledger = _validate_list(dossier.attachment_ledger, field_name="attachment_ledger")
    extracted_entities = _validate_list(dossier.extracted_entities, field_name="extracted_entities")
    evidence_references = _validate_list(dossier.evidence_references, field_name="evidence_references")
    procedure_records = _validate_list(dossier.procedure_records, field_name="procedure_records")
    history = _validate_list(dossier.history, field_name="history")
    reprocess_inputs = _validate_list(dossier.reprocess_inputs, field_name="reprocess_inputs")
    okved_matches = _validate_list(dossier.okved_matches, field_name="okved_matches")
    attachment_ledger_ids = {id(entry) for entry in attachment_ledger}
    for record in document_records:
        if id(record.ledger) not in attachment_ledger_ids:
            raise ValueError("document_records[].ledger must reference an entry from attachment_ledger")

    return {
        "dossier_contract_version": version,
        "company_id": _validate_string(dossier.company_id, field_name="company_id"),
        "company_name": _validate_string(dossier.company_name, field_name="company_name"),
        "site_url": _validate_string(dossier.site_url, field_name="site_url"),
        "created_at": _validate_timestamp_string(dossier.created_at, field_name="created_at"),
        "company_metadata": _serialize_mapping(dossier.company_metadata),
        "okved_site_match": _serialize_freeform(dossier.okved_site_match),
        "okved_matches": [_serialize_freeform(item) for item in okved_matches],
        "page_records": [_serialize_page_record(record) for record in page_records],
        "document_records": [_serialize_document_record(record) for record in document_records],
        "attachment_ledger": [_serialize_attachment_ledger_entry(entry) for entry in attachment_ledger],
        "extracted_entities": [_serialize_extracted_entity(entity) for entity in extracted_entities],
        "evidence_references": [_serialize_evidence_reference(reference) for reference in evidence_references],
        "procedure_records": [_serialize_mapping(record) for record in procedure_records],
        "history": [_serialize_mapping(entry) for entry in history],
        "reprocess_inputs": [_serialize_mapping(entry) for entry in reprocess_inputs],
    }


def dossier_from_dict(data: Mapping[str, Any]) -> CompanyDossier:
    data = _require_mapping(data, field_name="dossier")
    version = _require_string(data, "dossier_contract_version", allow_empty=False)
    if version != DOSSIER_CONTRACT_VERSION:
        raise ValueError(f"Unsupported dossier_contract_version: {version}")

    raw_attachment_ledger = _require_list(data, "attachment_ledger")
    attachment_ledger, ledger_lookup = _deserialize_attachment_ledger(raw_attachment_ledger)

    document_records = [
        _deserialize_document_record(item, ledger_lookup) for item in _require_list(data, "document_records")
    ]

    return CompanyDossier(
        company_id=_require_string(data, "company_id"),
        company_name=_require_string(data, "company_name"),
        site_url=_require_string(data, "site_url"),
        dossier_contract_version=version,
        page_records=[_deserialize_page_record(item) for item in _require_list(data, "page_records")],
        document_records=document_records,
        attachment_ledger=attachment_ledger,
        extracted_entities=[
            _deserialize_extracted_entity(item) for item in _require_list(data, "extracted_entities")
        ],
        evidence_references=[
            _deserialize_evidence_reference(item) for item in _require_list(data, "evidence_references")
        ],
        procedure_records=_deserialize_mapping_list(_require_list(data, "procedure_records"), field_name="procedure_records"),
        history=_deserialize_mapping_list(_require_list(data, "history"), field_name="history"),
        reprocess_inputs=_deserialize_mapping_list(_require_list(data, "reprocess_inputs"), field_name="reprocess_inputs"),
        okved_site_match=_deserialize_freeform(_require_value(data, "okved_site_match", allow_none=True)),
        okved_matches=[_deserialize_freeform(item) for item in _require_list(data, "okved_matches")],
        company_metadata=_deserialize_mapping(
            _require_mapping(_require_value(data, "company_metadata"), field_name="company_metadata"),
            field_name="company_metadata",
        ),
        created_at=_require_timestamp_string(data, "created_at"),
    )


def dossier_store_company_manifest_to_dict(
    *,
    company_id: str,
    company_name: str,
    latest_revision_id: str,
) -> dict[str, Any]:
    return {
        "store_layout_version": DOSSIER_STORE_LAYOUT_VERSION,
        "company_id": _validate_string(company_id, field_name="company_id"),
        "company_name": _validate_string(company_name, field_name="company_name"),
        "latest_revision_id": _validate_string(
            latest_revision_id,
            field_name="latest_revision_id",
            allow_empty=False,
        ),
    }


def dossier_store_company_manifest_from_dict(data: Mapping[str, Any]) -> dict[str, str]:
    normalized = _require_mapping(data, field_name="company_store_manifest")
    version = _require_string(normalized, "store_layout_version", allow_empty=False)
    if version != DOSSIER_STORE_LAYOUT_VERSION:
        raise ValueError(f"Unsupported store_layout_version: {version}")
    return {
        "store_layout_version": version,
        "company_id": _require_string(normalized, "company_id"),
        "company_name": _require_string(normalized, "company_name"),
        "latest_revision_id": _require_string(normalized, "latest_revision_id", allow_empty=False),
    }


def dossier_store_revision_manifest_to_dict(
    *,
    revision_id: str,
    company_id: str,
    company_name: str,
    stored_at: str,
    dossier_filename: str,
    attachments_dir: str,
    revision_layout: str | None = None,
    archive_manifest_filename: str | None = None,
) -> dict[str, Any]:
    normalized_revision_layout = revision_layout
    if archive_manifest_filename is not None and normalized_revision_layout is None:
        normalized_revision_layout = ARCHIVE_MANIFEST_REVISION_LAYOUT
    manifest = {
        "store_layout_version": DOSSIER_STORE_LAYOUT_VERSION,
        "revision_id": _validate_string(revision_id, field_name="revision_id", allow_empty=False),
        "company_id": _validate_string(company_id, field_name="company_id"),
        "company_name": _validate_string(company_name, field_name="company_name"),
        "stored_at": _validate_timestamp_string(stored_at, field_name="stored_at"),
        "dossier_filename": _validate_revision_local_filename(dossier_filename, field_name="dossier_filename"),
        "attachments_dir": _validate_non_empty_revision_relative_path(
            attachments_dir,
            field_name="attachments_dir",
        ),
    }
    if normalized_revision_layout is not None:
        manifest["revision_layout"] = _validate_revision_layout(
            normalized_revision_layout,
            field_name="revision_layout",
        )
    if archive_manifest_filename is not None:
        manifest["archive_manifest_filename"] = _validate_revision_local_filename(
            archive_manifest_filename,
            field_name="archive_manifest_filename",
        )
    return manifest


def dossier_store_revision_manifest_from_dict(data: Mapping[str, Any]) -> dict[str, str]:
    normalized = _require_mapping(data, field_name="revision_store_manifest")
    version = _require_string(normalized, "store_layout_version", allow_empty=False)
    if version != DOSSIER_STORE_LAYOUT_VERSION:
        raise ValueError(f"Unsupported store_layout_version: {version}")
    manifest = {
        "store_layout_version": version,
        "revision_id": _require_string(normalized, "revision_id", allow_empty=False),
        "company_id": _require_string(normalized, "company_id"),
        "company_name": _require_string(normalized, "company_name"),
        "stored_at": _require_timestamp_string(normalized, "stored_at"),
        "dossier_filename": _validate_revision_local_filename(
            _require_string(normalized, "dossier_filename", allow_empty=False),
            field_name="dossier_filename",
        ),
        "attachments_dir": _validate_non_empty_revision_relative_path(
            _require_string(normalized, "attachments_dir", allow_empty=False),
            field_name="attachments_dir",
        ),
    }
    if "revision_layout" in normalized:
        manifest["revision_layout"] = _validate_revision_layout(
            _require_string(normalized, "revision_layout", allow_empty=False),
            field_name="revision_layout",
        )
    if "archive_manifest_filename" in normalized:
        manifest["archive_manifest_filename"] = _validate_revision_local_filename(
            _require_string(normalized, "archive_manifest_filename", allow_empty=False),
            field_name="archive_manifest_filename",
        )
    return manifest


def _serialize_page_record(record: DossierPageRecord) -> dict[str, Any]:
    return {
        "source_url": record.source_url,
        "site_url": record.site_url,
        "source_type": record.source_type,
        "title": record.title,
        "section_guess": record.section_guess,
        "date": record.date,
        "text": record.text,
        "raw_text": record.raw_text,
        "cleaned_text": record.cleaned_text,
        "tables": _serialize_tables(record.tables, field_name="page_records[].tables"),
        "content_fingerprint": record.content_fingerprint,
        "fetch_status": record.fetch_status,
        "relevance_label": record.relevance_label,
        "relevance_score": record.relevance_score,
        "metadata": _serialize_mapping(record.metadata),
        "evidence_ref": _serialize_mapping(record.evidence_ref),
        "trace": _serialize_mapping(record.trace),
    }


def _serialize_attachment_ledger_entry(entry: DossierAttachmentLedgerEntry) -> dict[str, Any]:
    return {
        "source_url": entry.source_url,
        "referrer_url": entry.referrer_url,
        "filename": entry.filename,
        "mime": entry.mime,
        "size": entry.size,
        "checksum": entry.checksum,
        "fetch_status": entry.fetch_status,
        "entry_kind": entry.entry_kind,
        "local_path": entry.local_path,
        "dossier_file": _validate_revision_relative_path(entry.dossier_file, field_name="attachment_ledger[].dossier_file"),
        "archive_depth": entry.archive_depth,
        "parent_archive_url": entry.parent_archive_url,
        "warnings": _serialize_string_list(entry.warnings, field_name="attachment_ledger[].warnings"),
    }


def _serialize_document_record(record: DossierDocumentRecord) -> dict[str, Any]:
    return {
        "ledger": _serialize_attachment_ledger_entry(record.ledger),
        "source_path": record.source_path,
        "dossier_file": _validate_revision_relative_path(record.dossier_file, field_name="document_records[].dossier_file"),
        "source_format": record.source_format,
        "text": record.text,
        "tables": _serialize_tables(record.tables, field_name="document_records[].tables"),
        "sheet_names": _serialize_string_list(record.sheet_names, field_name="document_records[].sheet_names"),
        "metadata": _serialize_mapping(record.metadata),
        "warnings": _serialize_string_list(record.warnings, field_name="document_records[].warnings"),
        "provider": record.provider,
        "confidence": record.confidence,
        "quality": record.quality,
        "trace": _serialize_mapping(record.trace),
    }


def _serialize_evidence_reference(reference: EvidenceReference) -> dict[str, Any]:
    return {
        "evidence_type": reference.evidence_type,
        "source_url": reference.source_url,
        "source_path": reference.source_path,
        "dossier_file": _validate_revision_relative_path(
            reference.dossier_file,
            field_name="evidence_references[].dossier_file",
        ),
        "title": reference.title,
        "snippet": reference.snippet,
        "record_locator": reference.record_locator,
        "checksum": reference.checksum,
        "metadata": _serialize_mapping(reference.metadata),
        "trace": _serialize_mapping(reference.trace),
    }


def _serialize_extracted_entity(entity: ExtractedEntity) -> dict[str, Any]:
    return {
        "entity_type": entity.entity_type,
        "value": entity.value,
        "normalized_value": entity.normalized_value,
        "confidence": entity.confidence,
        "evidence": [_serialize_evidence_reference(reference) for reference in entity.evidence],
        "attributes": _serialize_mapping(entity.attributes),
    }


def _serialize_mapping(value: Mapping[str, Any] | dict[str, Any]) -> dict[str, Any]:
    return {
        str(key): _serialize_freeform(value[key])
        for key in sorted(value, key=lambda item: str(item))
    }


def _serialize_freeform(value: Any) -> Any:
    if value is None or isinstance(value, (bool, int, float, str)):
        return value
    if is_dataclass(value) and not isinstance(value, type):
        return {
            field.name: _serialize_freeform(getattr(value, field.name))
            for field in fields(value)
        }
    if isinstance(value, Mapping):
        return {
            str(key): _serialize_freeform(value[key])
            for key in sorted(value, key=lambda item: str(item))
        }
    if isinstance(value, list):
        return [_serialize_freeform(item) for item in value]
    if isinstance(value, tuple):
        raise TypeError("Tuples are not supported in dossier contract payloads; use lists instead")
    raise TypeError(f"Unsupported value type for dossier serialization: {type(value).__name__}")


def _serialize_tables(value: Any, *, field_name: str) -> list[list[list[str]]]:
    if value is None:
        raise TypeError(f"{field_name} must be a list, got NoneType")
    tables: list[list[list[str]]] = []
    for table_index, table in enumerate(_optional_list(value)):
        if not isinstance(table, list):
            raise TypeError(f"{field_name}[{table_index}] must be a list, got {type(table).__name__}")
        normalized_table: list[list[str]] = []
        for row_index, row in enumerate(table):
            if not isinstance(row, list):
                raise TypeError(
                    f"{field_name}[{table_index}][{row_index}] must be a list, got {type(row).__name__}"
                )
            normalized_row: list[str] = []
            for cell_index, cell in enumerate(row):
                if not isinstance(cell, str):
                    raise TypeError(
                        f"{field_name}[{table_index}][{row_index}][{cell_index}] "
                        f"must be a string, got {type(cell).__name__}"
                    )
                normalized_row.append(cell)
            normalized_table.append(normalized_row)
        tables.append(normalized_table)
    return tables


def _serialize_string_list(value: Any, *, field_name: str) -> list[str]:
    values = _optional_list(value)
    result: list[str] = []
    for index, item in enumerate(values):
        result.append(_validate_string(item, field_name=f"{field_name}[{index}]"))
    return result


def _deserialize_page_record(value: Any) -> DossierPageRecord:
    data = _require_mapping(value, field_name="page_records[]")
    return DossierPageRecord(
        source_url=_require_string(data, "source_url"),
        site_url=_require_string(data, "site_url"),
        source_type=_require_string(data, "source_type"),
        title=_string_or_default(data.get("title")),
        section_guess=_string_or_default(data.get("section_guess")),
        date=_string_or_default(data.get("date")),
        text=_string_or_default(data.get("text")),
        raw_text=_string_or_default(data.get("raw_text")),
        cleaned_text=_string_or_default(data.get("cleaned_text")),
        tables=_deserialize_tables(data.get("tables"), field_name="page_records[].tables"),
        content_fingerprint=_string_or_default(data.get("content_fingerprint")),
        fetch_status=_string_or_default(data.get("fetch_status")),
        relevance_label=_string_or_default(data.get("relevance_label"), default="unknown"),
        relevance_score=_float_or_default(data.get("relevance_score"), default=0.0),
        metadata=_deserialize_mapping(data.get("metadata"), field_name="metadata"),
        evidence_ref=_deserialize_mapping(data.get("evidence_ref"), field_name="evidence_ref"),
        trace=_deserialize_mapping(data.get("trace"), field_name="trace"),
    )


def _deserialize_attachment_ledger_entry(value: Any) -> DossierAttachmentLedgerEntry:
    data = _require_mapping(value, field_name="attachment_ledger[]")
    return DossierAttachmentLedgerEntry(
        source_url=_require_string(data, "source_url"),
        referrer_url=_require_string(data, "referrer_url"),
        filename=_require_string(data, "filename"),
        mime=_require_string(data, "mime"),
        size=_int_or_default(data.get("size"), default=0),
        checksum=_require_string(data, "checksum"),
        fetch_status=_require_string(data, "fetch_status"),
        entry_kind=_string_or_default(data.get("entry_kind"), default="attachment"),
        local_path=_string_or_default(data.get("local_path")),
        dossier_file=_validate_revision_relative_path(
            _string_or_default(data.get("dossier_file")),
            field_name="attachment_ledger[].dossier_file",
        ),
        archive_depth=_int_or_default(data.get("archive_depth"), default=0),
        parent_archive_url=_string_or_default(data.get("parent_archive_url")),
        warnings=_deserialize_string_list(data.get("warnings"), field_name="attachment_ledger[].warnings"),
    )


def _deserialize_document_record(
    value: Any,
    ledger_lookup: dict[Hashable, DossierAttachmentLedgerEntry],
) -> DossierDocumentRecord:
    data = _require_mapping(value, field_name="document_records[]")
    ledger_data = _require_mapping(data.get("ledger"), field_name="document_records[].ledger")
    ledger_key = _attachment_ledger_key_from_mapping(ledger_data, field_name="document_records[].ledger")
    ledger = ledger_lookup.get(ledger_key)
    if ledger is None:
        raise ValueError("document_records[].ledger must reference an entry from attachment_ledger")

    dossier_file = _validate_revision_relative_path(
        _string_or_default(data.get("dossier_file")),
        field_name="document_records[].dossier_file",
    )
    if dossier_file and ledger.dossier_file and dossier_file != ledger.dossier_file:
        raise ValueError(
            "document_records[].dossier_file must match document_records[].ledger.dossier_file when both are set"
        )

    return DossierDocumentRecord(
        ledger=ledger,
        source_path=_string_or_default(data.get("source_path")),
        dossier_file=dossier_file,
        source_format=_string_or_default(data.get("source_format")),
        text=_string_or_default(data.get("text")),
        tables=_deserialize_tables(data.get("tables"), field_name="document_records[].tables"),
        sheet_names=_deserialize_string_list(data.get("sheet_names"), field_name="document_records[].sheet_names"),
        metadata=_deserialize_mapping(data.get("metadata"), field_name="metadata"),
        warnings=_deserialize_string_list(data.get("warnings"), field_name="document_records[].warnings"),
        provider=_string_or_default(data.get("provider")),
        confidence=_optional_float(data.get("confidence")),
        quality=_string_or_default(data.get("quality")),
        trace=_deserialize_mapping(data.get("trace"), field_name="trace"),
    )


def _deserialize_evidence_reference(value: Any) -> EvidenceReference:
    data = _require_mapping(value, field_name="evidence_references[]")
    return EvidenceReference(
        evidence_type=_require_string(data, "evidence_type"),
        source_url=_string_or_default(data.get("source_url")),
        source_path=_string_or_default(data.get("source_path")),
        dossier_file=_validate_revision_relative_path(
            _string_or_default(data.get("dossier_file")),
            field_name="evidence_references[].dossier_file",
        ),
        title=_string_or_default(data.get("title")),
        snippet=_string_or_default(data.get("snippet")),
        record_locator=_string_or_default(data.get("record_locator")),
        checksum=_string_or_default(data.get("checksum")),
        metadata=_deserialize_mapping(data.get("metadata"), field_name="metadata"),
        trace=_deserialize_mapping(data.get("trace"), field_name="trace"),
    )


def _deserialize_extracted_entity(value: Any) -> ExtractedEntity:
    data = _require_mapping(value, field_name="extracted_entities[]")
    return ExtractedEntity(
        entity_type=_require_string(data, "entity_type"),
        value=_require_string(data, "value"),
        normalized_value=_string_or_default(data.get("normalized_value")),
        confidence=_optional_float(data.get("confidence")),
        evidence=[_deserialize_evidence_reference(item) for item in _optional_list(data.get("evidence"))],
        attributes=_deserialize_mapping(data.get("attributes"), field_name="attributes"),
    )


def _deserialize_mapping_list(value: list[Any], *, field_name: str) -> list[dict[str, Any]]:
    result: list[dict[str, Any]] = []
    for index, item in enumerate(value):
        item_field = f"{field_name}[{index}]"
        result.append(_deserialize_mapping(_require_mapping(item, field_name=item_field), field_name=item_field))
    return result


def _deserialize_mapping(value: Any, *, field_name: str) -> dict[str, Any]:
    if value is None:
        return {}
    data = _require_mapping(value, field_name=field_name)
    return {
        str(key): _deserialize_freeform(data[key])
        for key in sorted(data, key=lambda item: str(item))
    }


def _deserialize_list(value: Any) -> list[Any]:
    return [_deserialize_freeform(item) for item in _optional_list(value)]


def _deserialize_tables(value: Any, *, field_name: str) -> list[list[list[str]]]:
    if value is None:
        raise TypeError(f"{field_name} must be a list, got NoneType")
    tables: list[list[list[str]]] = []
    for table_index, table in enumerate(_optional_list(value)):
        if not isinstance(table, list):
            raise TypeError(f"{field_name}[{table_index}] must be a list, got {type(table).__name__}")
        normalized_table: list[list[str]] = []
        for row_index, row in enumerate(table):
            if not isinstance(row, list):
                raise TypeError(
                    f"{field_name}[{table_index}][{row_index}] must be a list, got {type(row).__name__}"
                )
            normalized_row: list[str] = []
            for cell_index, cell in enumerate(row):
                if not isinstance(cell, str):
                    raise TypeError(
                        f"{field_name}[{table_index}][{row_index}][{cell_index}] "
                        f"must be a string, got {type(cell).__name__}"
                    )
                normalized_row.append(cell)
            normalized_table.append(normalized_row)
        tables.append(normalized_table)
    return tables


def _deserialize_string_list(value: Any, *, field_name: str) -> list[str]:
    values = _optional_list(value)
    result: list[str] = []
    for index, item in enumerate(values):
        result.append(_validate_string(item, field_name=f"{field_name}[{index}]"))
    return result


def _deserialize_freeform(value: Any) -> Any:
    if value is None or isinstance(value, (bool, int, float, str)):
        return value
    if isinstance(value, Mapping):
        return {
            str(key): _deserialize_freeform(value[key])
            for key in sorted(value, key=lambda item: str(item))
        }
    if isinstance(value, list):
        return [_deserialize_freeform(item) for item in value]
    if isinstance(value, tuple):
        raise TypeError("Tuples are not supported in dossier contract payloads; use lists instead")
    raise TypeError(f"Unsupported value type for dossier deserialization: {type(value).__name__}")


def _deserialize_attachment_ledger(
    raw_attachment_ledger: list[Any],
) -> tuple[list[DossierAttachmentLedgerEntry], dict[Hashable, DossierAttachmentLedgerEntry]]:
    attachment_ledger: list[DossierAttachmentLedgerEntry] = []
    ledger_lookup: dict[Hashable, DossierAttachmentLedgerEntry] = {}
    for raw_item in raw_attachment_ledger:
        normalized = _require_mapping(raw_item, field_name="attachment_ledger[]")
        key = _attachment_ledger_key_from_mapping(normalized, field_name="attachment_ledger[]")
        entry = ledger_lookup.get(key)
        if entry is None:
            entry = _deserialize_attachment_ledger_entry(normalized)
            ledger_lookup[key] = entry
        attachment_ledger.append(entry)
    return attachment_ledger, ledger_lookup


def _attachment_ledger_key_from_mapping(value: Any, *, field_name: str) -> Hashable:
    return _attachment_ledger_key_from_entry(_deserialize_attachment_ledger_entry(_require_mapping(value, field_name=field_name)))


def _attachment_ledger_key_from_entry(entry: DossierAttachmentLedgerEntry) -> Hashable:
    return _freeze_value(_serialize_attachment_ledger_entry(entry))


def _freeze_value(value: Any) -> Hashable:
    if isinstance(value, Mapping):
        return tuple((str(key), _freeze_value(value[key])) for key in sorted(value, key=lambda item: str(item)))
    if isinstance(value, list):
        return tuple(_freeze_value(item) for item in value)
    return value


def _optional_list(value: Any) -> list[Any]:
    if value is None:
        return []
    if not isinstance(value, list):
        raise TypeError(f"Expected list, got {type(value).__name__}")
    return value


def _require_mapping(value: Any, *, field_name: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise TypeError(f"{field_name} must be a mapping, got {type(value).__name__}")
    return value


def _require_string(data: Mapping[str, Any], field_name: str, *, allow_empty: bool = True) -> str:
    if field_name not in data or data[field_name] is None:
        raise KeyError(f"Missing required field: {field_name}")
    return _validate_string(data[field_name], field_name=field_name, allow_empty=allow_empty)


def _require_timestamp_string(data: Mapping[str, Any], field_name: str) -> str:
    if field_name not in data or data[field_name] is None:
        raise KeyError(f"Missing required field: {field_name}")
    return _validate_timestamp_string(data[field_name], field_name=field_name)


def _string_or_default(value: Any, *, default: str = "") -> str:
    if value is None:
        return default
    return str(value)


def _validate_string(value: Any, *, field_name: str, allow_empty: bool = True) -> str:
    if not isinstance(value, str):
        raise TypeError(f"{field_name} must be a string, got {type(value).__name__}")
    if not value and not allow_empty:
        raise ValueError(f"{field_name} must be a non-empty string")
    return value


def _validate_revision_local_filename(value: Any, *, field_name: str) -> str:
    normalized = _validate_string(value, field_name=field_name, allow_empty=False)
    if _is_absolute_store_path(normalized):
        raise ValueError(f"{field_name} must be a local filename inside the revision root")
    if "/" in normalized or "\\" in normalized:
        raise ValueError(f"{field_name} must be a local filename inside the revision root")
    if normalized in {".", ".."}:
        raise ValueError(f"{field_name} must be a local filename inside the revision root")
    return normalized


def _validate_revision_relative_path(value: Any, *, field_name: str) -> str:
    normalized = _validate_string(value, field_name=field_name)
    if not normalized:
        return normalized
    if _is_absolute_store_path(normalized):
        raise ValueError(f"{field_name} must be a relative path inside the revision root")
    parts = [part for part in normalized.replace("\\", "/").split("/") if part and part != "."]
    if any(part == ".." for part in parts):
        raise ValueError(f"{field_name} must not escape the revision root")
    return "/".join(parts)


def _validate_non_empty_revision_relative_path(value: Any, *, field_name: str) -> str:
    normalized = _validate_revision_relative_path(value, field_name=field_name)
    parts = [part for part in normalized.replace("\\", "/").split("/") if part and part != "."]
    if not parts:
        raise ValueError(f"{field_name} must be a relative path inside the revision root")
    return normalized


def _validate_revision_layout(value: Any, *, field_name: str) -> str:
    normalized = _validate_string(value, field_name=field_name, allow_empty=False)
    if normalized != ARCHIVE_MANIFEST_REVISION_LAYOUT:
        raise ValueError(
            f"{field_name} must be {ARCHIVE_MANIFEST_REVISION_LAYOUT!r} when provided"
        )
    return normalized


def _is_absolute_store_path(value: str) -> bool:
    if value.startswith(("/", "\\")):
        return True
    windows_path = PureWindowsPath(value)
    return PurePosixPath(value).is_absolute() or windows_path.is_absolute() or bool(windows_path.drive)


def _validate_timestamp_string(value: Any, *, field_name: str) -> str:
    normalized = _validate_string(value, field_name=field_name, allow_empty=False)
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError as exc:
        raise ValueError(f"{field_name} must be an ISO 8601 timestamp with timezone") from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError(f"{field_name} must be an ISO 8601 timestamp with timezone")
    if normalized != parsed.isoformat():
        raise ValueError(f"{field_name} must be a canonical ISO 8601 timestamp with timezone")
    return normalized


def _require_list(data: Mapping[str, Any], field_name: str) -> list[Any]:
    if field_name not in data:
        raise KeyError(f"Missing required field: {field_name}")
    return _validate_list(data[field_name], field_name=field_name)


def _require_value(data: Mapping[str, Any], field_name: str, *, allow_none: bool = False) -> Any:
    if field_name not in data:
        raise KeyError(f"Missing required field: {field_name}")
    value = data[field_name]
    if value is None and not allow_none:
        raise TypeError(f"{field_name} must not be null")
    return value


def _validate_list(value: Any, *, field_name: str) -> list[Any]:
    if not isinstance(value, list):
        raise TypeError(f"{field_name} must be a list, got {type(value).__name__}")
    return value


def _int_or_default(value: Any, *, default: int) -> int:
    if value is None:
        return default
    return int(value)


def _float_or_default(value: Any, *, default: float) -> float:
    if value is None:
        return default
    return float(value)


def _optional_float(value: Any) -> float | None:
    if value is None:
        return None
    return float(value)


__all__ = [
    "ARCHIVE_MANIFEST_REVISION_LAYOUT",
    "DOSSIER_STORE_LAYOUT_VERSION",
    "dossier_from_dict",
    "dossier_store_company_manifest_from_dict",
    "dossier_store_company_manifest_to_dict",
    "dossier_store_revision_manifest_from_dict",
    "dossier_store_revision_manifest_to_dict",
    "dossier_to_dict",
]

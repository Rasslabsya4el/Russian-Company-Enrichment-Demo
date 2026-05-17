from __future__ import annotations

import argparse
import importlib
import inspect
import json
import shutil
import sys
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable

if __package__ in {None, ""}:
    sys.path.append(str(Path(__file__).resolve().parents[2]))

from app.dossier import CompanyDossierStore
from app.documents.attachments import AttachmentAcquirer, AttachmentRecord
from app.documents.content import normalize_extracted_document
from app.site_intelligence.factory_site_parser.models import FactorySiteParserCompany
from app.site_intelligence.factory_site_parser.okved_match import FactorySiteOkvedMatcher
from app.site_intelligence.models import ContentRecord


COMPANY_ID = "7701234567"
COMPANY_NAME = "\u041e\u041e\u041e \u041f\u0440\u043e\u043c\u0422\u0435\u0445 \u0421\u0438\u043d\u0442\u0435\u0442\u0438\u043a\u0430"
ADDRESS_REGION = "\u041c\u043e\u0441\u043a\u043e\u0432\u0441\u043a\u0430\u044f \u043e\u0431\u043b\u0430\u0441\u0442\u044c"
SITE_URL = "https://promtech-smoke.example"
PAGE_BASE = f"{SITE_URL}/company-dossier-smoke"
DOC_SOURCE_URL = f"{SITE_URL}/files/company-dossier-smoke-offer.txt"


@dataclass(frozen=True)
class ResolvedCallable:
    name: str
    target: Callable[..., Any]


@dataclass(frozen=True)
class ResolvedDossierApi:
    build: ResolvedCallable
    write: ResolvedCallable | None
    public_names: list[str]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Offline vertical smoke for per-company dossier assembly using synthetic page and document inputs."
    )
    parser.add_argument(
        "--keep-dir",
        default="",
        help="Optional directory to keep generated synthetic inputs and dossier artifacts.",
    )
    return parser.parse_args()


def _assert(condition: bool, message: str) -> None:
    if not condition:
        raise RuntimeError(message)


def _configure_output_streams() -> None:
    for stream_name in ("stdout", "stderr"):
        stream = getattr(sys, stream_name, None)
        if stream is None or not hasattr(stream, "reconfigure"):
            continue
        try:
            stream.reconfigure(encoding="utf-8", errors="backslashreplace")
        except OSError:
            continue


def _make_content_record(
    *,
    url: str,
    section_guess: str,
    title: str,
    text: str,
    metadata: dict[str, Any] | None = None,
    evidence_ref: dict[str, Any] | None = None,
) -> ContentRecord:
    return ContentRecord(
        company_id=COMPANY_ID,
        site_id=SITE_URL,
        site_url=SITE_URL,
        url=url,
        source_type="html",
        source_url_or_file=url,
        section_guess=section_guess,
        title=title,
        text=text,
        metadata=dict(metadata or {}),
        evidence_ref=dict(evidence_ref or {}),
        extraction_method="synthetic_smoke",
        fetch_status="success",
        relevance_label="synthetic",
        relevance_score=1.0,
        relevance_reasons=["synthetic_vertical_smoke"],
        notes=["synthetic_page_record"],
        trace={"synthetic": {"kind": "page_record", "source_url": url}},
    )


def _build_page_records() -> list[ContentRecord]:
    production_text = (
        "ООО ПромТех Синтетика ведет собственное производство металлоконструкций и сварных рам. "
        "Производственный цех выпускает опорные блоки и сварные узлы по чертежам заказчика. "
        "Основной профиль: ОКВЭД 25.11. Контакты отдела снабжения: procurement@promtech-smoke.example."
    )
    surplus_text = (
        "Раздел реализации ТМЦ: неликвиды, складские остатки и сортовая продажа листового металла. "
        "Доступны остатки балки, швеллера и кабеля после модернизации производственной площадки. "
        "Контакт: warehouse@promtech-smoke.example, +7 (495) 123-45-67."
    )
    return [
        _make_content_record(
            url=f"{PAGE_BASE}/production",
            section_guess="production",
            title="Собственное производство металлоконструкций",
            text=production_text,
            metadata={"synthetic": {"record_kind": "page", "page_role": "production"}},
            evidence_ref={"kind": "synthetic_page", "source_url_or_file": f"{PAGE_BASE}/production"},
        ),
        _make_content_record(
            url=f"{PAGE_BASE}/surplus",
            section_guess="products",
            title="Неликвиды и складские остатки",
            text=surplus_text,
            metadata={"synthetic": {"record_kind": "page", "page_role": "surplus"}},
            evidence_ref={"kind": "synthetic_page", "source_url_or_file": f"{PAGE_BASE}/surplus"},
        ),
    ]


def _write_document_fixture(temp_root: Path) -> Path:
    fixture_dir = temp_root / "input_documents"
    fixture_dir.mkdir(parents=True, exist_ok=True)
    fixture_path = fixture_dir / "company-dossier-smoke-offer.txt"
    fixture_path.write_text(
        "\n".join(
            [
                "Коммерческое предложение по складским остаткам",
                f"\u041a\u043e\u043c\u043f\u0430\u043d\u0438\u044f: {COMPANY_NAME}",
                f"\u0418\u041d\u041d: {COMPANY_ID}",
                "Дата: 2026-04-10",
                "Номенклатура: медный кабель, лист стальной 09Г2С, сварные рамные узлы.",
                "Контакты: procurement@promtech-smoke.example, +7 (495) 123-45-67.",
                (
                    "\u0410\u0434\u0440\u0435\u0441 \u043f\u043b\u043e\u0449\u0430\u0434\u043a\u0438: "
                    f"{ADDRESS_REGION}, \u0438\u043d\u0434\u0443\u0441\u0442\u0440\u0438\u0430\u043b\u044c\u043d\u044b\u0439 "
                    "\u043f\u0430\u0440\u043a \u0421\u0435\u0432\u0435\u0440."
                ),
                "Источник: локальный synthetic smoke artifact.",
            ]
        ),
        encoding="utf-8",
    )
    return fixture_path


def _document_record_from_attachment(item: AttachmentRecord) -> ContentRecord:
    _assert(item.extracted is not None, "Synthetic document fixture did not produce an extracted document.")
    metadata = {
        "attachment": {
            "source_url": item.ledger.source_url,
            "referrer_url": item.ledger.referrer_url,
            "filename": item.ledger.filename,
            "mime": item.ledger.mime,
            "size": item.ledger.size,
            "checksum": item.ledger.checksum,
            "fetch_status": item.ledger.fetch_status,
            "entry_kind": item.ledger.entry_kind,
            "local_path": item.ledger.local_path,
            "archive_depth": item.ledger.archive_depth,
            "parent_archive_url": item.ledger.parent_archive_url,
            "warnings": list(item.ledger.warnings),
        }
    }
    evidence_ref = {
        "kind": "document_attachment",
        "source_url_or_file": item.ledger.source_url or item.ledger.local_path,
        "source_url": item.ledger.source_url,
        "local_path": item.ledger.local_path,
        "filename": item.ledger.filename,
        "checksum": item.ledger.checksum,
        "entry_kind": item.ledger.entry_kind,
    }
    return normalize_extracted_document(
        item.extracted,
        company_id=COMPANY_ID,
        site_id=SITE_URL,
        source_url_or_file=item.ledger.source_url or item.ledger.local_path,
        source_type=item.extracted.source_format,
        section_guess="documents",
        title=item.ledger.filename,
        site_url=SITE_URL,
        url=item.ledger.source_url,
        extraction_method="attachment_pipeline",
        fetch_status=item.ledger.fetch_status,
        metadata=metadata,
        evidence_ref=evidence_ref,
        notes=list(item.ledger.warnings),
        trace=item.to_trace(),
    )


def _build_document_inputs(temp_root: Path) -> tuple[list[AttachmentRecord], list[ContentRecord]]:
    fixture_path = _write_document_fixture(temp_root)
    acquirer = AttachmentAcquirer(temp_root / "attachment_ingest", enable_ocr=False)
    attachment_records = acquirer.ingest_local_file(
        fixture_path,
        source_url=DOC_SOURCE_URL,
        referrer_url=f"{PAGE_BASE}/documents",
    )
    document_records = [record for record in attachment_records if record.extracted is not None]
    _assert(document_records, "Synthetic document fixture did not yield any extracted attachment records.")
    return attachment_records, [_document_record_from_attachment(document_records[0])]


def _build_company() -> FactorySiteParserCompany:
    return FactorySiteParserCompany(
        company_id=COMPANY_ID,
        company_name=COMPANY_NAME,
        input_site=SITE_URL,
        candidate_sites=[SITE_URL],
        known_okved_codes=["25.11"],
        activity_terms=["металлоконструкции", "сварные рамы", "производственный цех"],
        source_snippets=["Собственное производство металлоконструкций и сварных узлов."],
        source_notes=["Есть раздел реализации неликвидов и складских остатков."],
    )


def _load_dossier_payload(dossier_path: Path) -> dict[str, Any]:
    _assert(dossier_path.is_file(), f"Dossier JSON was not written: {dossier_path}")
    with dossier_path.open("r", encoding="utf-8") as handle:
        payload = json.load(handle)
    _assert(isinstance(payload, dict), "Written dossier JSON must deserialize into an object.")
    return payload


def _require_list(payload: dict[str, Any], key: str) -> list[Any]:
    value = payload.get(key)
    _assert(isinstance(value, list), f"Dossier field '{key}' must be a list.")
    return value


def _require_nested_dict(payload: dict[str, Any], key: str, *, parent: str) -> dict[str, Any]:
    value = payload.get(key)
    _assert(isinstance(value, dict), f"{parent}.{key} must be an object.")
    return value


def _require_non_empty_string(value: Any, *, label: str) -> str:
    text = str(value or "").strip()
    _assert(text, f"{label} must be populated.")
    return text


def _assert_relative_attachment_path(value: str, *, label: str) -> str:
    path = Path(value)
    _assert(not path.is_absolute(), f"{label} must stay relative inside the dossier bundle.")
    normalized = path.as_posix()
    _assert(normalized.startswith("attachments/"), f"{label} must point into dossier attachments/, got '{normalized}'.")
    return normalized


def _assert_fixture_path(value: Any, *, label: str) -> str:
    path_text = _require_non_empty_string(value, label=label)
    _assert(
        Path(path_text).name.endswith(Path(DOC_SOURCE_URL).name),
        f"{label} must point to the synthetic fixture file.",
    )
    return path_text


def _assert_persisted_runtime_path_cleared(value: Any, *, label: str) -> None:
    _assert(not str(value or "").strip(), f"{label} must stay empty in the persisted dossier snapshot.")


def _assert_persisted_acceptance_smoke(dossier_payload: dict[str, Any]) -> None:
    page_records = _require_list(dossier_payload, "page_records")
    document_records = _require_list(dossier_payload, "document_records")
    attachment_ledger = _require_list(dossier_payload, "attachment_ledger")
    extracted_entities = _require_list(dossier_payload, "extracted_entities")
    evidence_references = _require_list(dossier_payload, "evidence_references")
    company_metadata = dossier_payload.get("company_metadata")
    _assert(dossier_payload.get("company_id") == COMPANY_ID, "Acceptance smoke requires exact company_id in dossier payload.")
    _assert(
        dossier_payload.get("company_name") == COMPANY_NAME,
        "Acceptance smoke requires exact company_name in dossier payload.",
    )
    _assert(dossier_payload.get("site_url") == SITE_URL, "Acceptance smoke requires exact site_url in dossier payload.")
    _require_non_empty_string(dossier_payload.get("created_at"), label="created_at")
    _assert(isinstance(company_metadata, dict), "company_metadata must be an object.")

    _assert(len(page_records) >= 2, f"Acceptance smoke requires page_records >= 2, got {len(page_records)}")
    _assert(len(document_records) >= 1, f"Acceptance smoke requires document_records >= 1, got {len(document_records)}")
    _assert(
        len(attachment_ledger) >= 1,
        f"Acceptance smoke requires attachment_ledger >= 1, got {len(attachment_ledger)}",
    )
    okved_site_match = _require_nested_dict(dossier_payload, "okved_site_match", parent="dossier")
    _require_non_empty_string(okved_site_match.get("verdict"), label="okved_site_match.verdict")

    entities_by_type: dict[str, list[dict[str, Any]]] = {}
    for entity in extracted_entities:
        if not isinstance(entity, dict):
            continue
        entity_type = str(entity.get("entity_type") or "").strip()
        if entity_type:
            entities_by_type.setdefault(entity_type, []).append(entity)

    for entity_type in ("email", "phone", "inn", "legal_name", "address"):
        _assert(entities_by_type.get(entity_type), f"Acceptance smoke requires extracted entity '{entity_type}'.")

    expected_dossier_files: set[str] = set()
    for index, document_record in enumerate(document_records, start=1):
        _assert(isinstance(document_record, dict), f"document_records[{index}] must be an object.")
        dossier_file = _assert_relative_attachment_path(
            _require_non_empty_string(document_record.get("dossier_file"), label=f"document_records[{index}].dossier_file"),
            label=f"document_records[{index}].dossier_file",
        )
        _assert_persisted_runtime_path_cleared(
            document_record.get("source_path"),
            label=f"document_records[{index}].source_path",
        )
        _require_non_empty_string(document_record.get("source_format"), label=f"document_records[{index}].source_format")
        ledger = _require_nested_dict(document_record, "ledger", parent=f"document_records[{index}]")
        ledger_dossier_file = _assert_relative_attachment_path(
            _require_non_empty_string(ledger.get("dossier_file"), label=f"document_records[{index}].ledger.dossier_file"),
            label=f"document_records[{index}].ledger.dossier_file",
        )
        _assert(
            ledger_dossier_file == dossier_file,
            f"document_records[{index}] dossier_file must match ledger.dossier_file.",
        )
        _require_non_empty_string(ledger.get("filename"), label=f"document_records[{index}].ledger.filename")
        _require_non_empty_string(ledger.get("checksum"), label=f"document_records[{index}].ledger.checksum")
        _require_non_empty_string(ledger.get("fetch_status"), label=f"document_records[{index}].ledger.fetch_status")
        _assert_persisted_runtime_path_cleared(
            ledger.get("local_path"),
            label=f"document_records[{index}].ledger.local_path",
        )
        expected_dossier_files.add(dossier_file)

    for index, ledger_entry in enumerate(attachment_ledger, start=1):
        _assert(isinstance(ledger_entry, dict), f"attachment_ledger[{index}] must be an object.")
        _require_non_empty_string(ledger_entry.get("source_url"), label=f"attachment_ledger[{index}].source_url")
        _require_non_empty_string(ledger_entry.get("referrer_url"), label=f"attachment_ledger[{index}].referrer_url")
        _require_non_empty_string(ledger_entry.get("filename"), label=f"attachment_ledger[{index}].filename")
        _require_non_empty_string(ledger_entry.get("checksum"), label=f"attachment_ledger[{index}].checksum")
        _require_non_empty_string(ledger_entry.get("fetch_status"), label=f"attachment_ledger[{index}].fetch_status")
        _assert_persisted_runtime_path_cleared(
            ledger_entry.get("local_path"),
            label=f"attachment_ledger[{index}].local_path",
        )
        ledger_dossier_file = _assert_relative_attachment_path(
            _require_non_empty_string(ledger_entry.get("dossier_file"), label=f"attachment_ledger[{index}].dossier_file"),
            label=f"attachment_ledger[{index}].dossier_file",
        )
        _assert(
            ledger_dossier_file in expected_dossier_files,
            f"attachment_ledger[{index}].dossier_file must match a document_records dossier_file.",
        )

    document_evidence_references = []
    for index, reference in enumerate(evidence_references, start=1):
        _assert(isinstance(reference, dict), f"evidence_references[{index}] must be an object.")
        evidence_type = _require_non_empty_string(
            reference.get("evidence_type"),
            label=f"evidence_references[{index}].evidence_type",
        )
        if evidence_type != "document":
            continue
        _assert_persisted_runtime_path_cleared(
            reference.get("source_path"),
            label=f"evidence_references[{index}].source_path",
        )
        dossier_file = _assert_relative_attachment_path(
            _require_non_empty_string(reference.get("dossier_file"), label=f"evidence_references[{index}].dossier_file"),
            label=f"evidence_references[{index}].dossier_file",
        )
        _require_non_empty_string(reference.get("title"), label=f"evidence_references[{index}].title")
        _require_non_empty_string(reference.get("checksum"), label=f"evidence_references[{index}].checksum")
        _require_non_empty_string(reference.get("record_locator"), label=f"evidence_references[{index}].record_locator")
        _assert(
            dossier_file in expected_dossier_files,
            f"evidence_references[{index}].dossier_file must match a copied attachment.",
        )
        document_evidence_references.append(reference)
    _assert(
        document_evidence_references,
        "Acceptance smoke requires at least one document evidence_reference with rebound dossier_file.",
    )

    address_entity = next(
        (
            entity
            for entity in entities_by_type["address"]
            if ADDRESS_REGION in str(entity.get("value") or "")
        ),
        None,
    )
    _assert(address_entity is not None, f"Acceptance smoke requires an address entity containing '{ADDRESS_REGION}'.")

    address_evidence = address_entity.get("evidence")
    _assert(isinstance(address_evidence, list) and address_evidence, "Address entity must include evidence.")
    document_evidence = [
        evidence
        for evidence in address_evidence
        if isinstance(evidence, dict) and str(evidence.get("evidence_type") or "").strip() == "document"
    ]
    _assert(document_evidence, "Address entity must be backed by document evidence.")
    for evidence in document_evidence:
        _assert_persisted_runtime_path_cleared(
            evidence.get("source_path"),
            label="address evidence.source_path",
        )

    dossier_backed_address_evidence = [
        evidence for evidence in document_evidence if str(evidence.get("dossier_file") or "").strip()
    ]
    _assert(dossier_backed_address_evidence, "Address entity must include dossier-backed document evidence.")
    for evidence in dossier_backed_address_evidence:
        dossier_file = _assert_relative_attachment_path(
            _require_non_empty_string(evidence.get("dossier_file"), label="address evidence.dossier_file"),
            label="address evidence.dossier_file",
        )
        _assert(
            dossier_file in expected_dossier_files,
            "Dossier-backed address evidence must include dossier_file inside the written dossier.",
        )
    _assert(
        any(
            ref.get("dossier_file") == dossier_backed_address_evidence[0].get("dossier_file")
            for ref in document_evidence_references
        ),
        "Address document evidence must also be represented in top-level evidence_references.",
    )


def _load_hydrated_dossier(*, output_dir: Path) -> Any:
    store = CompanyDossierStore(output_dir)
    return store.load_latest(COMPANY_ID, company_name=COMPANY_NAME)


def _assert_hydrated_acceptance_smoke(dossier: Any) -> None:
    document_records = list(getattr(dossier, "document_records", []))
    attachment_ledger = list(getattr(dossier, "attachment_ledger", []))
    evidence_references = list(getattr(dossier, "evidence_references", []))
    extracted_entities = list(getattr(dossier, "extracted_entities", []))

    _assert(len(document_records) >= 1, f"Acceptance smoke requires document_records >= 1, got {len(document_records)}")
    _assert(
        len(attachment_ledger) >= 1,
        f"Acceptance smoke requires attachment_ledger >= 1, got {len(attachment_ledger)}",
    )

    expected_dossier_files: set[str] = set()
    for index, document_record in enumerate(document_records, start=1):
        dossier_file = _assert_relative_attachment_path(
            _require_non_empty_string(getattr(document_record, "dossier_file", ""), label=f"document_records[{index}].dossier_file"),
            label=f"document_records[{index}].dossier_file",
        )
        _assert_fixture_path(getattr(document_record, "source_path", ""), label=f"document_records[{index}].source_path")
        ledger = getattr(document_record, "ledger", None)
        _assert(ledger is not None, f"document_records[{index}].ledger must be populated after hydration.")
        ledger_dossier_file = _assert_relative_attachment_path(
            _require_non_empty_string(getattr(ledger, "dossier_file", ""), label=f"document_records[{index}].ledger.dossier_file"),
            label=f"document_records[{index}].ledger.dossier_file",
        )
        _assert(
            ledger_dossier_file == dossier_file,
            f"document_records[{index}] dossier_file must match ledger.dossier_file after hydration.",
        )
        _assert_fixture_path(
            getattr(ledger, "local_path", ""),
            label=f"document_records[{index}].ledger.local_path",
        )
        expected_dossier_files.add(dossier_file)

    for index, ledger_entry in enumerate(attachment_ledger, start=1):
        _assert_fixture_path(
            getattr(ledger_entry, "local_path", ""),
            label=f"attachment_ledger[{index}].local_path",
        )
        ledger_dossier_file = _assert_relative_attachment_path(
            _require_non_empty_string(getattr(ledger_entry, "dossier_file", ""), label=f"attachment_ledger[{index}].dossier_file"),
            label=f"attachment_ledger[{index}].dossier_file",
        )
        _assert(
            ledger_dossier_file in expected_dossier_files,
            f"attachment_ledger[{index}].dossier_file must match a hydrated document_records dossier_file.",
        )

    document_evidence_references = []
    for index, reference in enumerate(evidence_references, start=1):
        if str(getattr(reference, "evidence_type", "") or "").strip() != "document":
            continue
        _assert_fixture_path(
            getattr(reference, "source_path", ""),
            label=f"evidence_references[{index}].source_path",
        )
        dossier_file = _assert_relative_attachment_path(
            _require_non_empty_string(getattr(reference, "dossier_file", ""), label=f"evidence_references[{index}].dossier_file"),
            label=f"evidence_references[{index}].dossier_file",
        )
        _assert(
            dossier_file in expected_dossier_files,
            f"evidence_references[{index}].dossier_file must match a hydrated document_records dossier_file.",
        )
        document_evidence_references.append(reference)
    _assert(
        document_evidence_references,
        "Acceptance smoke requires at least one hydrated document evidence_reference with source_path and dossier_file.",
    )

    address_entity = next(
        (
            entity
            for entity in extracted_entities
            if str(getattr(entity, "entity_type", "") or "").strip() == "address"
            and ADDRESS_REGION in str(getattr(entity, "value", "") or "")
        ),
        None,
    )
    _assert(address_entity is not None, f"Acceptance smoke requires an address entity containing '{ADDRESS_REGION}'.")

    address_evidence = list(getattr(address_entity, "evidence", []) or [])
    _assert(address_evidence, "Address entity must include hydrated evidence.")
    document_evidence = [
        evidence
        for evidence in address_evidence
        if str(getattr(evidence, "evidence_type", "") or "").strip() == "document"
    ]
    _assert(document_evidence, "Address entity must be backed by hydrated document evidence.")

    dossier_backed_address_evidence = [
        evidence for evidence in document_evidence if str(getattr(evidence, "dossier_file", "") or "").strip()
    ]
    _assert(dossier_backed_address_evidence, "Address entity must include dossier-backed hydrated document evidence.")

    for evidence in dossier_backed_address_evidence:
        dossier_file = _assert_relative_attachment_path(
            _require_non_empty_string(getattr(evidence, "dossier_file", ""), label="address evidence.dossier_file"),
            label="address evidence.dossier_file",
        )
        _assert_fixture_path(getattr(evidence, "source_path", ""), label="address evidence.source_path")
        _assert(
            dossier_file in expected_dossier_files,
            "Hydrated address evidence must include dossier_file inside the written dossier.",
        )
    _assert(
        any(
            getattr(ref, "dossier_file", "") == getattr(dossier_backed_address_evidence[0], "dossier_file", "")
            for ref in document_evidence_references
        ),
        "Address document evidence must also be represented in hydrated top-level evidence_references.",
    )


def _resolve_public_callable(module: Any, preferred_names: list[str], *, keywords: tuple[str, ...]) -> ResolvedCallable | None:
    for name in preferred_names:
        candidate = getattr(module, name, None)
        if callable(candidate):
            return ResolvedCallable(name=name, target=candidate)

    public_names = [name for name in dir(module) if not name.startswith("_")]
    for name in sorted(public_names):
        lowered = name.lower()
        if all(keyword in lowered for keyword in keywords):
            candidate = getattr(module, name)
            if callable(candidate):
                return ResolvedCallable(name=name, target=candidate)
    return None


def _resolve_dossier_api() -> ResolvedDossierApi:
    module = importlib.import_module("app.dossier")
    public_names = sorted(name for name in dir(module) if not name.startswith("_"))
    build_callable = _resolve_public_callable(
        module,
        preferred_names=["build_company_dossier", "assemble_company_dossier", "build_dossier", "assemble_dossier"],
        keywords=("dossier", "build"),
    ) or _resolve_public_callable(
        module,
        preferred_names=[],
        keywords=("dossier", "assemble"),
    )
    if build_callable is None:
        raise RuntimeError(
            "app.dossier does not expose a public dossier builder. "
            f"Available public names: {public_names or ['<none>']}"
        )

    write_callable = _resolve_public_callable(
        module,
        preferred_names=["write_company_dossier", "write_dossier", "save_company_dossier", "save_dossier"],
        keywords=("dossier", "write"),
    ) or _resolve_public_callable(
        module,
        preferred_names=[],
        keywords=("dossier", "save"),
    )
    return ResolvedDossierApi(build=build_callable, write=write_callable, public_names=public_names)


def _call_with_supported_kwargs(target: Callable[..., Any], **candidate_kwargs: Any) -> Any:
    signature = inspect.signature(target)
    parameters = signature.parameters
    accepts_kwargs = any(parameter.kind == inspect.Parameter.VAR_KEYWORD for parameter in parameters.values())

    if accepts_kwargs:
        return target(**candidate_kwargs)

    supported_kwargs = {
        name: value
        for name, value in candidate_kwargs.items()
        if name in parameters and parameters[name].kind in (inspect.Parameter.POSITIONAL_OR_KEYWORD, inspect.Parameter.KEYWORD_ONLY)
    }
    missing = [
        name
        for name, parameter in parameters.items()
        if parameter.default is inspect.Signature.empty
        and parameter.kind in (inspect.Parameter.POSITIONAL_OR_KEYWORD, inspect.Parameter.KEYWORD_ONLY)
        and name not in supported_kwargs
    ]
    if missing:
        raise RuntimeError(f"Cannot call {target.__qualname__}; missing required parameters: {', '.join(missing)}")
    return target(**supported_kwargs)


def _invoke_resolved(callable_ref: ResolvedCallable, **candidate_kwargs: Any) -> Any:
    target = callable_ref.target
    if inspect.isclass(target):
        instance = target()
        for method_name in ("build", "assemble", "write", "save", "__call__"):
            method = getattr(instance, method_name, None)
            if callable(method):
                return _call_with_supported_kwargs(method, **candidate_kwargs)
        raise RuntimeError(f"Resolved class {callable_ref.name} has no supported build/write method.")
    return _call_with_supported_kwargs(target, **candidate_kwargs)


def _normalize_output_path(result: Any, *, output_dir: Path) -> Path | None:
    if isinstance(result, Path):
        return result
    if isinstance(result, str) and result.strip():
        return Path(result)
    if isinstance(result, dict):
        for key in ("path", "output_path", "dossier_path", "written_path"):
            value = result.get(key)
            if isinstance(value, str) and value.strip():
                return Path(value)
    if result is None:
        candidates = sorted(path for path in output_dir.rglob("*") if path.is_file())
        if len(candidates) == 1:
            return candidates[0]
    return None


def _build_and_write_dossier(
    *,
    output_dir: Path,
    company: FactorySiteParserCompany,
    page_records: list[ContentRecord],
    document_records: list[ContentRecord],
    attachment_records: list[AttachmentRecord],
    okved_profile: Any,
    okved_matches: Any,
    okved_site_match: Any,
) -> Path:
    api = _resolve_dossier_api()
    content_records = [*page_records, *document_records]
    common_kwargs = {
        "company": company,
        "company_id": company.company_id,
        "company_name": company.company_name,
        "site_url": company.input_site,
        "page_records": page_records,
        "document_records": document_records,
        "content_records": content_records,
        "records": content_records,
        "attachment_records": attachment_records,
        "attachments": attachment_records,
        "documents": document_records,
        "okved_profile": okved_profile,
        "okved_matches": okved_matches,
        "okved_site_match": okved_site_match,
        "output_dir": output_dir,
    }
    build_result = _invoke_resolved(api.build, **common_kwargs)
    build_path = _normalize_output_path(build_result, output_dir=output_dir)
    if build_path is not None:
        return build_path

    writer = api.write
    if writer is None and build_result is not None:
        for method_name in ("write", "save"):
            method = getattr(build_result, method_name, None)
            if callable(method):
                write_result = _call_with_supported_kwargs(
                    method,
                    output_dir=output_dir,
                    company_id=company.company_id,
                    company_name=company.company_name,
                    site_url=company.input_site,
                )
                written_path = _normalize_output_path(write_result, output_dir=output_dir)
                if written_path is not None:
                    return written_path

    if writer is None:
        raise RuntimeError(
            "app.dossier builder returned an in-memory result, but no public dossier writer was found. "
            f"Available public names: {api.public_names or ['<none>']}"
        )

    write_result = _invoke_resolved(
        writer,
        dossier=build_result,
        output_dir=output_dir,
        company_id=company.company_id,
        company_name=company.company_name,
        site_url=company.input_site,
    )
    written_path = _normalize_output_path(write_result, output_dir=output_dir)
    if written_path is None:
        raise RuntimeError(
            f"Dossier writer {writer.name} completed, but no output path could be determined in {output_dir}."
        )
    return written_path


def main() -> int:
    _configure_output_streams()
    args = parse_args()
    temp_root = Path(args.keep_dir).expanduser() if args.keep_dir.strip() else Path(tempfile.mkdtemp(prefix="company-dossier-smoke-"))
    cleanup = not args.keep_dir.strip()
    output_dir = temp_root / "dossier_output"

    try:
        output_dir.mkdir(parents=True, exist_ok=True)
        company = _build_company()
        page_records = _build_page_records()
        attachment_records, document_records = _build_document_inputs(temp_root)
        matcher = FactorySiteOkvedMatcher()
        okved_profile, okved_matches = matcher.match_records(company, page_records)
        okved_site_match = okved_profile.site_match
        dossier_path = _build_and_write_dossier(
            output_dir=output_dir,
            company=company,
            page_records=page_records,
            document_records=document_records,
            attachment_records=attachment_records,
            okved_profile=okved_profile,
            okved_matches=okved_matches,
            okved_site_match=okved_site_match,
        )
        dossier_payload = _load_dossier_payload(dossier_path)
        _assert_persisted_acceptance_smoke(dossier_payload)
        hydrated_dossier = _load_hydrated_dossier(output_dir=output_dir)
        _assert_hydrated_acceptance_smoke(hydrated_dossier)
        print(f"dossier_path={dossier_path}")
        print(
            "PASS "
            f"company_id={company.company_id} "
            f"pages={len(dossier_payload['page_records'])} "
            f"documents={len(dossier_payload['document_records'])} "
            f"attachments={len(dossier_payload['attachment_ledger'])} "
            f"okved_verdict={okved_site_match.verdict if okved_site_match else 'skipped'}"
        )
        return 0
    finally:
        if cleanup:
            shutil.rmtree(temp_root, ignore_errors=True)


if __name__ == "__main__":
    raise SystemExit(main())

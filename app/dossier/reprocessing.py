from __future__ import annotations

from copy import deepcopy
from datetime import datetime, timezone
from pathlib import Path

from .assembly import _build_evidence_references, _extract_entities
from .models import CompanyDossier
from .procedures import rebind_procedure_records
from .store import CompanyDossierStore

_DERIVED_SECTIONS = [
    "evidence_references",
    "extracted_entities",
    "procedure_records",
]


def reprocess_company_dossier(dossier: CompanyDossier) -> CompanyDossier:
    reprocessed = deepcopy(dossier)
    reprocessed.evidence_references = _build_evidence_references(
        reprocessed.page_records,
        reprocessed.document_records,
    )
    reprocessed.extracted_entities = _extract_entities(
        reprocessed.page_records,
        reprocessed.document_records,
    )
    if reprocessed.procedure_records:
        reprocessed.procedure_records = rebind_procedure_records(
            procedure_records=reprocessed.procedure_records,
            evidence_references=reprocessed.evidence_references,
            document_records=reprocessed.document_records,
        )
    else:
        reprocessed.procedure_records = []
    reprocessed.history.append(
        {
            "event_type": "stored_revision_reprocess",
            "reprocess_mode": "without_refetch",
            "performed_at": datetime.now(timezone.utc).isoformat(),
            "network_used": False,
            "derived_sections": list(_DERIVED_SECTIONS),
        }
    )
    return reprocessed


def load_and_reprocess_dossier(
    *,
    output_dir: str | Path,
    company_id: str,
    version: str | None = None,
    company_name: str | None = None,
) -> CompanyDossier:
    store = CompanyDossierStore(output_dir)
    dossier = (
        store.load(company_id, version, company_name=company_name)
        if version is not None
        else store.load_latest(company_id, company_name=company_name)
    )
    return reprocess_company_dossier(dossier)

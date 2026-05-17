from __future__ import annotations

from pathlib import Path
from typing import Any

import company_enrichment_core as core

from .assembly import build_company_dossier
from .store import CompanyDossierStore


def _refresh_result_for_dossier(result: Any) -> Any:
    if isinstance(result, dict):
        refreshed = dict(result)
        refreshed["profile"] = core.company_profile_payload_from_result(refreshed)
        return refreshed

    if hasattr(result, "profile") and hasattr(result, "output_contract_version"):
        core.refresh_company_result_profile(result)
        return core.serialize_company_result(result)

    return result


def build_and_store_company_dossier(*, result: Any, output_dir: str | Path) -> dict[str, Any]:
    output_root = Path(output_dir)
    dossier = build_company_dossier(result=_refresh_result_for_dossier(result))
    store = CompanyDossierStore(output_root / "company_dossiers")
    revision_file = store.write(dossier)
    return {
        "store_root": "company_dossiers",
        "company_id": dossier.company_id,
        "company_name": dossier.company_name,
        "revision_id": revision_file.parent.name,
        "revision_file": revision_file.relative_to(output_root).as_posix(),
        "dossier_contract_version": dossier.dossier_contract_version,
    }

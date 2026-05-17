from __future__ import annotations

import csv
from pathlib import Path

from openpyxl import load_workbook

from company_enrichment_core import (
    choose_best_company_contacts,
    flatten_company_result_for_export,
    write_flat_csv,
    write_flat_xlsx,
)


def _base_result(validated_sites: list[dict[str, object]]) -> dict[str, object]:
    trusted_site = "http://trusted.example/"
    return {
        "row_index": 55,
        "inn": "7725385488",
        "company_name": 'ООО "ПК "ТЕХМЕКС"',
        "status": "completed",
        "trusted_contacts": {
            "phones": ["+7 499 638-28-90"],
            "emails": [],
            "websites": [trusted_site],
            "addresses": [],
        },
        "merged_contacts": {
            "phones": ["+7 499 638-28-90"],
            "emails": ["info@example.com"],
            "websites": [trusted_site, "https://fallback.example/"],
            "addresses": [],
        },
        "validated_sites": validated_sites,
        "domain_resolution": {
            "status": "verified",
            "selected_primary_domain": trusted_site,
            "selected_primary_status": "verified",
            "candidates": [
                {
                    "url": trusted_site,
                    "source": "xlsx_input",
                }
            ],
            "notes": [],
        },
        "sources": {
            "zachestnyibiznes": {
                "status": "success",
                "websites": [{"value": trusted_site}],
            },
            "rusprofile": {
                "status": "success",
                "websites": [{"value": "https://fallback.example/"}],
            },
        },
        "profile": {
            "sites": {
                "best_site": trusted_site,
                "best_site_status": "trusted",
                "best_site_sources": ["stale_profile"],
            }
        },
    }


def _assert_export_surfaces(
    tmp_path: Path,
    result: dict[str, object],
    expected_best_site: str,
    expected_status: str,
) -> None:
    best = choose_best_company_contacts(result)
    assert best["best_site"] == expected_best_site
    assert best["best_site_status"] == expected_status

    flat_row = flatten_company_result_for_export(result)
    assert flat_row["best_site"] == expected_best_site
    assert flat_row["best_site_status"] == expected_status

    csv_path = tmp_path / "final_results.csv"
    xlsx_path = tmp_path / "final_results.xlsx"
    write_flat_csv(csv_path, [flat_row])
    write_flat_xlsx(xlsx_path, [flat_row])

    with csv_path.open("r", encoding="utf-8-sig", newline="") as handle:
        csv_row = next(csv.DictReader(handle))
    assert csv_row["best_site"] == expected_best_site
    assert csv_row["best_site_status"] == expected_status

    workbook = load_workbook(xlsx_path)
    sheet = workbook.active
    header = [cell.value for cell in sheet[1]]
    values = [cell.value for cell in sheet[2]]
    xlsx_row = dict(zip(header, values))
    assert xlsx_row["best_site"] == expected_best_site
    assert xlsx_row["best_site_status"] == expected_status


def test_flat_export_preserves_surface_only_validated_status(tmp_path: Path) -> None:
    result = _base_result(
        validated_sites=[
            {
                "url": "https://samip.ru/",
                "final_url": "https://samip.ru/",
                "decision_status": "suspicious",
                "decision_source": "cheap_preparse_gate",
                "belongs_to_company": False,
                "authenticity_score": 0.327,
                "identity_score": 0.520,
            },
            {
                "url": "https://podshipnik.info/",
                "final_url": "https://podshipnik.info/",
                "decision_status": "rejected",
                "decision_source": "cheap_preparse_gate",
                "belongs_to_company": False,
                "authenticity_score": 0.190,
                "identity_score": 0.220,
            },
        ]
    )

    _assert_export_surfaces(
        tmp_path=tmp_path,
        result=result,
        expected_best_site="https://samip.ru/",
        expected_status="suspicious",
    )


def test_flat_export_keeps_verified_validated_site_status(tmp_path: Path) -> None:
    result = _base_result(
        validated_sites=[
            {
                "url": "https://factory.example/",
                "final_url": "https://factory.example/catalog",
                "decision_status": "verified",
                "decision_source": "heuristics",
                "belongs_to_company": True,
                "authenticity_score": 0.980,
                "identity_score": 1.000,
            }
        ]
    )

    _assert_export_surfaces(
        tmp_path=tmp_path,
        result=result,
        expected_best_site="https://factory.example/catalog",
        expected_status="verified",
    )

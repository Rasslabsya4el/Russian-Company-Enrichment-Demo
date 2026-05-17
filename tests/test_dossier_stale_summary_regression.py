from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from app.dossier.integration import build_and_store_company_dossier
from company_enrichment_core import summarize_company_decision


def make_page_record(*, url: str) -> dict[str, object]:
    return {
        "url": url,
        "site_url": "https://factory.example/",
        "source_type": "html",
        "title": "Open tender page",
        "text": "Tender details and procurement conditions.",
        "content_fingerprint": "page:tender-open-lot",
        "fetch_status": "success",
        "section_guess": "tenders",
    }


def make_document_record(*, url: str) -> dict[str, object]:
    return {
        "source_url_or_file": url,
        "source_type": "pdf",
        "title": "Procurement specification",
        "text": "Specification for the open tender.",
        "content_fingerprint": "doc:specification-pdf",
        "fetch_status": "success",
    }


class DossierStaleSummaryRegressionTests(unittest.TestCase):
    def test_build_and_store_company_dossier_prefers_fresh_profile_summary_and_sites(self) -> None:
        page_url = "https://factory.example/tenders/open-lot"
        document_url = "https://factory.example/files/specification.pdf"
        result = {
            "inn": "7701234567",
            "company_name": "Factory Alpha",
            "site_url": "https://factory.example/",
            "status": "completed",
            "candidate_sites": [
                "https://factory.example/",
                "https://factory.example/catalog/",
            ],
            "validated_sites": [
                {
                    "url": "https://factory.example/",
                    "final_url": "https://factory.example/",
                    "decision_status": "verified",
                    "belongs_to_company": True,
                    "reasons": ["Official factory site"],
                },
                {
                    "url": "https://factory.example/catalog/",
                    "final_url": "https://factory.example/catalog/",
                    "decision_status": "candidate",
                    "belongs_to_company": False,
                    "reasons": ["Catalog subsite"],
                },
            ],
            "site_probes": [
                {
                    "url": "https://factory.example/",
                    "site_class": "A",
                    "worth_crawling": "true",
                },
                {
                    "url": "https://factory.example/catalog/",
                    "site_class": "C",
                    "worth_crawling": "limited",
                },
            ],
            "domain_resolution": {
                "status": "verified",
                "selected_primary_domain": "factory.example",
                "candidates": [
                    {
                        "url": "https://factory.example/",
                        "source": "manual",
                    }
                ],
                "notes": ["verified via footer contact block"],
            },
            "profile": {
                "summary": {
                    "inn": "0000000000",
                    "company_name": "Stale Name",
                    "processing_status": "running",
                    "domain_resolution_status": "",
                    "lead_count": 0,
                    "decision_summary": "stale summary",
                },
                "sites": {
                    "primary_domain": "stale.example",
                    "best_site": "https://stale.example/",
                    "best_site_status": "trusted",
                    "best_site_sources": ["stale_catalog"],
                    "candidate_sites": ["https://stale.example/catalog/"],
                    "confirmed_sites": ["https://stale.example/"],
                    "site_classes": ["F"],
                    "worth_crawling": ["false"],
                },
            },
            "lead_cards": [
                {
                    "title": "Open tender",
                    "lead_type": "tender",
                    "status": "open",
                    "source_urls": [page_url],
                    "date": "2026-04-10",
                }
            ],
            "content_records": [
                make_page_record(url=page_url),
                make_document_record(url=document_url),
            ],
            "sources": {
                "spark": {
                    "status": "completed",
                    "websites": [{"value": "https://factory.example/"}],
                }
            },
        }
        expected_summary = {
            "inn": result["inn"],
            "company_name": result["company_name"],
            "processing_status": result["status"],
            "domain_resolution_status": result["domain_resolution"]["status"],
            "lead_count": len(result["lead_cards"]),
            "decision_summary": summarize_company_decision(result),
        }
        expected_sites = {
            "primary_domain": "factory.example",
            "best_site": "https://factory.example/",
            "best_site_status": "verified",
            "best_site_sources": ["spark", "domain_resolution:manual"],
            "candidate_sites": [
                "https://factory.example/",
                "https://factory.example/catalog/",
            ],
            "confirmed_sites": ["https://factory.example/"],
            "site_classes": ["A", "C"],
            "worth_crawling": ["true", "limited"],
        }

        with tempfile.TemporaryDirectory() as tmpdir:
            stored = build_and_store_company_dossier(result=result, output_dir=tmpdir)
            dossier_path = Path(tmpdir) / stored["revision_file"]

            with dossier_path.open("r", encoding="utf-8") as handle:
                dossier_payload = json.load(handle)

        actual_summary = dossier_payload["company_metadata"]["profile"]["summary"]
        for field_name, expected_value in expected_summary.items():
            self.assertEqual(actual_summary[field_name], expected_value)
        actual_sites = dossier_payload["company_metadata"]["profile"]["sites"]
        for field_name, expected_value in expected_sites.items():
            self.assertEqual(actual_sites[field_name], expected_value)
        self.assertEqual(stored["company_id"], result["inn"])
        self.assertEqual(stored["company_name"], result["company_name"])
        self.assertEqual(dossier_payload["company_id"], result["inn"])
        self.assertEqual(dossier_payload["company_name"], result["company_name"])
        self.assertEqual(len(dossier_payload["page_records"]), 1)
        self.assertEqual(len(dossier_payload["document_records"]), 1)
        self.assertEqual(len(dossier_payload["procedure_records"]), 1)


if __name__ == "__main__":
    unittest.main()

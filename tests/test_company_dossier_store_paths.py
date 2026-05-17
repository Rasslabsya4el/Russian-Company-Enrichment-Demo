from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from app.dossier.models import (
    CompanyDossier,
    DossierAttachmentLedgerEntry,
    DossierDocumentRecord,
    EvidenceReference,
)
from app.dossier.store import ARCHIVE_MANIFEST_FILENAME, CompanyDossierStore


class CompanyDossierStorePathTests(unittest.TestCase):
    def test_write_persist_load_canonicalizes_attachment_paths_and_accepts_legacy_snapshot(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp_path = Path(tmpdir)
            source_path = tmp_path / "offer.pdf"
            source_path.write_bytes(b"%PDF-1.4\ncanonical path regression\n")
            expected_dossier_file = "attachments/offer.pdf"

            ledger = DossierAttachmentLedgerEntry(
                source_url="https://example.com/files/offer.pdf",
                referrer_url="https://example.com/tenders",
                filename="offer.pdf",
                mime="application/pdf",
                size=source_path.stat().st_size,
                checksum="a" * 64,
                fetch_status="downloaded",
                local_path=str(source_path),
                dossier_file=r"attachments\offer.pdf",
            )
            document = DossierDocumentRecord(
                ledger=ledger,
                source_path=str(source_path),
                dossier_file=r"attachments\offer.pdf",
                source_format="pdf",
                text="Offer attachment",
            )
            evidence = EvidenceReference(
                evidence_type="attachment",
                source_path=str(source_path),
                dossier_file=r"attachments\offer.pdf",
                checksum=ledger.checksum,
                title="Offer attachment",
            )
            dossier = CompanyDossier(
                company_id="7700000000",
                company_name='OOO "Test Zavod"',
                site_url="https://example.com",
                attachment_ledger=[ledger],
                document_records=[document],
                evidence_references=[evidence],
            )
            store = CompanyDossierStore(tmp_path / "store")

            snapshot_path = store.write(dossier)
            archive_manifest_path = snapshot_path.parent / ARCHIVE_MANIFEST_FILENAME

            persisted_snapshot_text = snapshot_path.read_text(encoding="utf-8")
            self.assertNotIn("attachments\\", persisted_snapshot_text)
            persisted_snapshot = json.loads(persisted_snapshot_text)
            self.assertEqual(persisted_snapshot["attachment_ledger"][0]["dossier_file"], expected_dossier_file)
            self.assertEqual(persisted_snapshot["document_records"][0]["dossier_file"], expected_dossier_file)
            self.assertEqual(
                persisted_snapshot["document_records"][0]["ledger"]["dossier_file"],
                expected_dossier_file,
            )
            self.assertEqual(persisted_snapshot["evidence_references"][0]["dossier_file"], expected_dossier_file)

            persisted_archive_manifest_text = archive_manifest_path.read_text(encoding="utf-8")
            self.assertNotIn("attachments\\", persisted_archive_manifest_text)
            persisted_archive_manifest = json.loads(persisted_archive_manifest_text)
            self.assertEqual(persisted_archive_manifest["files"][0]["dossier_file"], expected_dossier_file)

            loaded = store.load_latest("7700000000")
            self.assertEqual(loaded.attachment_ledger[0].dossier_file, expected_dossier_file)
            self.assertEqual(loaded.document_records[0].dossier_file, expected_dossier_file)
            self.assertEqual(loaded.document_records[0].ledger.dossier_file, expected_dossier_file)
            self.assertEqual(loaded.evidence_references[0].dossier_file, expected_dossier_file)
            self.assertEqual(loaded.document_records[0].source_path, loaded.attachment_ledger[0].local_path)
            self.assertEqual(loaded.document_records[0].source_path, loaded.evidence_references[0].source_path)
            self.assertTrue(Path(loaded.document_records[0].source_path).is_file())

            persisted_snapshot["attachment_ledger"][0]["dossier_file"] = r"attachments\offer.pdf"
            persisted_snapshot["document_records"][0]["dossier_file"] = r"attachments\offer.pdf"
            persisted_snapshot["document_records"][0]["ledger"]["dossier_file"] = r"attachments\offer.pdf"
            persisted_snapshot["evidence_references"][0]["dossier_file"] = r"attachments\offer.pdf"
            snapshot_path.write_text(json.dumps(persisted_snapshot, ensure_ascii=False, indent=2), encoding="utf-8")

            persisted_archive_manifest["files"][0]["dossier_file"] = r"attachments\offer.pdf"
            archive_manifest_path.write_text(
                json.dumps(persisted_archive_manifest, ensure_ascii=False, indent=2),
                encoding="utf-8",
            )

            legacy_loaded = store.load_latest("7700000000")
            self.assertEqual(legacy_loaded.attachment_ledger[0].dossier_file, expected_dossier_file)
            self.assertEqual(legacy_loaded.document_records[0].dossier_file, expected_dossier_file)
            self.assertEqual(legacy_loaded.document_records[0].ledger.dossier_file, expected_dossier_file)
            self.assertEqual(legacy_loaded.evidence_references[0].dossier_file, expected_dossier_file)
            self.assertEqual(legacy_loaded.document_records[0].source_path, legacy_loaded.attachment_ledger[0].local_path)
            self.assertEqual(legacy_loaded.document_records[0].source_path, legacy_loaded.evidence_references[0].source_path)
            self.assertTrue(Path(legacy_loaded.document_records[0].source_path).is_file())

    def test_load_and_load_latest_reject_snapshot_when_document_and_ledger_dossier_paths_diverge(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp_path = Path(tmpdir)
            source_path = tmp_path / "offer.pdf"
            source_path.write_bytes(b"%PDF-1.4\nstore path mismatch regression\n")

            ledger = DossierAttachmentLedgerEntry(
                source_url="https://example.com/files/offer.pdf",
                referrer_url="https://example.com/tenders",
                filename="offer.pdf",
                mime="application/pdf",
                size=source_path.stat().st_size,
                checksum="b" * 64,
                fetch_status="downloaded",
                local_path=str(source_path),
                dossier_file="attachments/offer.pdf",
            )
            document = DossierDocumentRecord(
                ledger=ledger,
                source_path=str(source_path),
                dossier_file="attachments/offer.pdf",
                source_format="pdf",
                text="Offer attachment",
            )
            dossier = CompanyDossier(
                company_id="7700000000",
                company_name='OOO "Test Zavod"',
                site_url="https://example.com",
                attachment_ledger=[ledger],
                document_records=[document],
            )
            store = CompanyDossierStore(tmp_path / "store")

            snapshot_path = store.write(dossier)
            revision_id = snapshot_path.parent.name
            persisted_snapshot = json.loads(snapshot_path.read_text(encoding="utf-8"))
            persisted_snapshot["attachment_ledger"][0]["dossier_file"] = "attachments/other-offer.pdf"
            persisted_snapshot["document_records"][0]["dossier_file"] = "attachments/offer.pdf"
            persisted_snapshot["document_records"][0]["ledger"]["dossier_file"] = "attachments/other-offer.pdf"
            snapshot_path.write_text(json.dumps(persisted_snapshot, ensure_ascii=False, indent=2), encoding="utf-8")

            with self.assertRaisesRegex(ValueError, "must match"):
                store.load("7700000000", revision_id)

            with self.assertRaisesRegex(ValueError, "must match"):
                store.load_latest("7700000000")


if __name__ == "__main__":
    unittest.main()

from __future__ import annotations

import argparse
import shutil
import sys
import tempfile
from pathlib import Path

import requests

if __package__ in {None, ""}:
    sys.path.append(str(Path(__file__).resolve().parents[2]))

from app.documents.attachments import AttachmentLedgerEntry, AttachmentRecord
from app.documents.formats import ExtractedDocument
from app.site_intelligence.factory_site_parser.documents import FactorySiteDocumentsStage


SITE_URL = "https://queue-smoke.example"
DOC_URL = f"{SITE_URL}/files/planner-direct.pdf"
HTML_URL = f"{SITE_URL}/docs"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Synthetic smoke for planner-driven document queue provenance and dedup.")
    parser.add_argument(
        "--keep-dir",
        default="",
        help="Optional directory to keep temporary attachment roots.",
    )
    return parser.parse_args()


def _assert(condition: bool, message: str) -> None:
    if not condition:
        raise RuntimeError(message)


def _make_response(url: str, *, content_type: str, body: str) -> requests.Response:
    response = requests.Response()
    response.status_code = 200
    response.url = url
    response.reason = "OK"
    response.encoding = "utf-8"
    response.headers["Content-Type"] = content_type
    response._content = body.encode("utf-8")
    return response


def _apply_context(ledger: AttachmentLedgerEntry, ledger_context: dict[str, str] | None) -> AttachmentLedgerEntry:
    for key, value in (ledger_context or {}).items():
        if hasattr(ledger, key) and value is not None:
            setattr(ledger, key, str(value))
    return ledger


def _document_record(
    *,
    source_url: str,
    referrer_url: str,
    filename: str,
    checksum: str,
    size: int,
    text: str,
    mime: str,
    ledger_context: dict[str, str] | None,
) -> AttachmentRecord:
    ledger = _apply_context(
        AttachmentLedgerEntry(
            source_url=source_url,
            referrer_url=referrer_url,
            filename=filename,
            mime=mime,
            size=size,
            checksum=checksum,
            fetch_status="extracted",
            entry_kind="attachment",
            local_path=str(Path("synthetic") / filename),
        ),
        ledger_context,
    )
    payload = ExtractedDocument(
        source_path=filename,
        source_format=Path(filename).suffix.lstrip(".") or "pdf",
        text=text,
        tables=[],
        metadata={"synthetic": "document_queue_smoke"},
        trace={"synthetic": {"filename": filename}},
    )
    return AttachmentRecord(ledger=ledger, extracted=payload)


def _archive_record(
    *,
    source_url: str,
    referrer_url: str,
    filename: str,
    checksum: str,
    size: int,
    mime: str,
    ledger_context: dict[str, str] | None,
) -> AttachmentRecord:
    ledger = _apply_context(
        AttachmentLedgerEntry(
            source_url=source_url,
            referrer_url=referrer_url,
            filename=filename,
            mime=mime,
            size=size,
            checksum=checksum,
            fetch_status="archive_extracted",
            entry_kind="attachment",
            local_path=str(Path("synthetic") / filename),
        ),
        ledger_context,
    )
    return AttachmentRecord(ledger=ledger)


class FakeAcquirer:
    def looks_like_attachment(self, source_url: str, mime: str = "") -> bool:
        return source_url.endswith((".pdf", ".zip")) or "pdf" in mime.lower() or "zip" in mime.lower()

    def ingest_response(
        self,
        response: requests.Response,
        *,
        source_url: str,
        referrer_url: str,
        ledger_context: dict[str, str] | None = None,
    ) -> list[AttachmentRecord]:
        return [
            _document_record(
                source_url=source_url,
                referrer_url=referrer_url,
                filename="planner-direct.pdf",
                checksum="checksum-direct",
                size=len(response.content or b""),
                text="Planner direct PDF with procurement contacts.",
                mime="application/pdf",
                ledger_context=ledger_context,
            )
        ]

    def acquire_from_url(
        self,
        source_url: str,
        *,
        referrer_url: str,
        ledger_context: dict[str, str] | None = None,
    ) -> list[AttachmentRecord]:
        if source_url.endswith("/offer.pdf"):
            return [
                _document_record(
                    source_url=source_url,
                    referrer_url=referrer_url,
                    filename="offer.pdf",
                    checksum="checksum-offer",
                    size=48,
                    text="Offer PDF discovered from HTML route.",
                    mime="application/pdf",
                    ledger_context=ledger_context,
                )
            ]
        if source_url.endswith("/duplicate.pdf"):
            return [
                _document_record(
                    source_url=source_url,
                    referrer_url=referrer_url,
                    filename="duplicate.pdf",
                    checksum="checksum-direct",
                    size=40,
                    text="Duplicate checksum PDF.",
                    mime="application/pdf",
                    ledger_context=ledger_context,
                )
            ]
        if source_url.endswith("/heavy.zip"):
            return [
                _archive_record(
                    source_url=source_url,
                    referrer_url=referrer_url,
                    filename="heavy.zip",
                    checksum="checksum-heavy",
                    size=256,
                    mime="application/zip",
                    ledger_context=ledger_context,
                )
            ]
        raise RuntimeError(f"Unexpected source_url for fake acquirer: {source_url}")


def _assert_provenance(record: Any, *, route_family: str, source_page: str, discovery_source: str) -> None:
    provenance = dict(record.metadata.get("attachment_provenance", {}))
    _assert(provenance.get("route_family") == route_family, f"{record.title}: missing route_family provenance.")
    _assert(provenance.get("source_page") == source_page, f"{record.title}: missing source_page provenance.")
    _assert(provenance.get("discovery_source") == discovery_source, f"{record.title}: missing discovery_source provenance.")
    evidence_provenance = dict(record.evidence_ref.get("attachment_provenance", {}))
    _assert(evidence_provenance.get("route_family") == route_family, f"{record.title}: evidence_ref missing route_family.")
    _assert(record.trace.get("page_signal_taxonomy", {}).get("route_family") == route_family, f"{record.title}: trace missing route_family.")


def main() -> int:
    args = parse_args()
    temp_root = Path(args.keep_dir).expanduser() if args.keep_dir.strip() else Path(tempfile.mkdtemp(prefix="document-queue-smoke-"))
    cleanup = not args.keep_dir.strip()

    try:
        stage = FactorySiteDocumentsStage(object(), storage_root=temp_root, enable_ocr=False)
        collector = stage.build_collector("7701234567")
        collector.acquirer = FakeAcquirer()
        collector.max_item_bytes = 64
        collector.max_total_bytes = 1024
        collector.max_queue_items = 8
        collector.max_per_type = 4

        direct_response = _make_response(
            DOC_URL,
            content_type="application/pdf",
            body="planner direct pdf body",
        )
        direct_records = stage.collect_direct_response(
            collector=collector,
            company_id="7701234567",
            site_url=SITE_URL,
            response=direct_response,
            source_url=DOC_URL,
            referrer_url=SITE_URL,
            section_guess="documents",
            route_family="docs/certificates",
        )
        _assert(len(direct_records) == 1, "Expected one direct-response document record.")
        direct_record = direct_records[0]
        _assert(direct_record.fetch_status == "success", "Direct planner-discovered document should be acquired successfully.")
        _assert(direct_record.metadata.get("document_queue", {}).get("status") == "acquired", "Direct document must be marked as acquired.")
        _assert_provenance(
            direct_record,
            route_family="docs/certificates",
            source_page=SITE_URL,
            discovery_source="planner_direct_document",
        )

        html_response = _make_response(
            HTML_URL,
            content_type="text/html",
            body=(
                '<a href="/files/offer.pdf">offer</a>'
                '<a href="/files/offer.pdf#copy">offer-duplicate-url</a>'
                '<a href="/files/duplicate.pdf">duplicate-hash</a>'
                '<a href="/files/heavy.zip">heavy-archive</a>'
            ),
        )
        html_records = stage.collect_html_attachments(
            collector=collector,
            company_id="7701234567",
            site_url=SITE_URL,
            response=html_response,
            fetch_status="success",
            section_guess="documents",
            route_family="docs/certificates",
        )
        _assert(len(html_records) == 4, f"Expected four HTML-queue outcomes, got {len(html_records)}.")

        acquired_offer = next(
            record for record in html_records if record.title == "offer.pdf" and record.fetch_status == "success"
        )
        duplicate_url = next(
            record
            for record in html_records
            if record.fetch_status == "duplicate_skipped"
            and record.metadata.get("document_queue", {}).get("skip_reason") == "duplicate_canonical_url"
        )
        duplicate_hash = next(
            record
            for record in html_records
            if record.fetch_status == "duplicate_skipped"
            and record.metadata.get("document_queue", {}).get("skip_reason") == "duplicate_checksum"
        )
        heavy_zip = next(
            record
            for record in html_records
            if record.fetch_status == "heavy_skipped"
            and record.metadata.get("document_queue", {}).get("skip_reason") == "item_size_cap_exceeded"
        )

        _assert(acquired_offer.metadata.get("document_queue", {}).get("status") == "acquired", "HTML offer must be acquired.")
        _assert(duplicate_url.metadata.get("document_queue", {}).get("status") == "skipped", "Duplicate canonical URL must be skipped.")
        _assert(duplicate_hash.metadata.get("document_queue", {}).get("status") == "skipped", "Duplicate checksum must be skipped.")
        _assert(heavy_zip.metadata.get("document_queue", {}).get("status") == "skipped", "Heavy archive must be skipped.")

        for record in html_records:
            _assert_provenance(
                record,
                route_family="docs/certificates",
                source_page=HTML_URL,
                discovery_source="page_attachment_link",
            )

        print(
            "PASS "
            f"direct={len(direct_records)} "
            f"html={len(html_records)} "
            "dedup=canonical+checksum "
            "heavy_skip=yes "
            "provenance=yes"
        )
        print(f"artifact_root={temp_root}")
        return 0
    finally:
        if cleanup:
            shutil.rmtree(temp_root, ignore_errors=True)


if __name__ == "__main__":
    raise SystemExit(main())

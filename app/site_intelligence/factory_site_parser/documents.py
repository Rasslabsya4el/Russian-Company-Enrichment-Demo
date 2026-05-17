from __future__ import annotations

import tempfile
from pathlib import Path
from typing import Any

import requests

from app.site_intelligence.attachments import AttachmentCollector
from app.site_intelligence.models import ContentRecord


def _normalize_source_page(*values: str) -> str:
    for value in values:
        cleaned = str(value or "").strip()
        if cleaned:
            return cleaned
    return ""


class FactorySiteDocumentsStage:
    def __init__(
        self,
        client: Any,
        *,
        storage_root: Path | None = None,
        enable_ocr: bool = True,
        ocr_provider_name: str | None = None,
        ocr_trace_dir: Path | None = None,
        ocr_execution_context: Any | None = None,
    ) -> None:
        self.client = client
        self.storage_root = storage_root or (Path(tempfile.gettempdir()) / "factory_site_parser_attachments")
        self.enable_ocr = enable_ocr
        self.ocr_provider_name = ocr_provider_name
        self.ocr_trace_dir = ocr_trace_dir
        self.ocr_execution_context = ocr_execution_context

    def build_collector(self, company_id: str) -> AttachmentCollector:
        return AttachmentCollector(
            self.client,
            self.storage_root / company_id,
            enable_ocr=self.enable_ocr,
            ocr_provider_name=self.ocr_provider_name,
            ocr_trace_dir=self.ocr_trace_dir,
            ocr_execution_context=self.ocr_execution_context,
        )

    def collect_direct_response(
        self,
        *,
        collector: AttachmentCollector,
        company_id: str,
        site_url: str,
        response: requests.Response | None,
        source_url: str,
        referrer_url: str,
        section_guess: str,
        route_family: str = "",
    ) -> list[ContentRecord]:
        if not response or not collector.response_looks_like_attachment(response, source_url=source_url):
            return []
        return collector.collect_from_direct_response(
            company_id=company_id,
            site_url=site_url,
            response=response,
            source_url=source_url,
            referrer_url=referrer_url,
            section_guess=section_guess,
            route_family=route_family,
            source_page=_normalize_source_page(referrer_url, site_url, response.url, source_url),
            discovery_source="planner_direct_document",
        )

    def collect_html_attachments(
        self,
        *,
        collector: AttachmentCollector,
        company_id: str,
        site_url: str,
        response: requests.Response | None,
        fetch_status: str,
        section_guess: str,
        route_family: str = "",
    ) -> list[ContentRecord]:
        if not response or fetch_status != "success":
            return []
        return collector.collect_from_html_response(
            company_id=company_id,
            site_url=site_url,
            response=response,
            section_guess=section_guess,
            route_family=route_family,
            source_page=_normalize_source_page(response.url, site_url),
            discovery_source="page_attachment_link",
        )


__all__ = ["FactorySiteDocumentsStage"]

from __future__ import annotations

import hashlib
import os
import re
from typing import Any

import requests
from bs4 import BeautifulSoup

from .common import compact_text, normalize_whitespace, parse_title_and_meta, route_family_for_section
from .models import ContentRecord, RouteStrategy


def extract_date_from_text(text: str) -> str:
    normalized = normalize_whitespace(text)
    match = re.search(r"\b(\d{2}[./-]\d{2}[./-]\d{4})\b", normalized)
    if match:
        return match.group(1)
    match = re.search(r"\b(\d{4}-\d{2}-\d{2})\b", normalized)
    if match:
        return match.group(1)
    return ""


def _route_taxonomy_trace(route: RouteStrategy) -> dict[str, dict[str, str]]:
    route_family = route_family_for_section(route.route_family or route.section_guess)
    return {
        "page_signal_taxonomy": {
            "route_family": route_family,
            "section_guess": route.section_guess,
        }
    }


def _extract_html_tables(soup: BeautifulSoup) -> list[list[list[str]]]:
    tables: list[list[list[str]]] = []
    for table in soup.select("table"):
        rows: list[list[str]] = []
        for row in table.select("tr"):
            cells = row.find_all(["th", "td"])
            if not cells:
                continue
            normalized_row = [normalize_whitespace(cell.get_text(" ", strip=True)) for cell in cells]
            if any(normalized_row):
                rows.append(normalized_row)
        if rows:
            tables.append(rows)
    return tables


def _html_metadata(
    *,
    route: RouteStrategy,
    response: requests.Response | None,
    description: str,
    notes: list[str],
    table_count: int,
) -> dict[str, Any]:
    metadata: dict[str, Any] = {
        "route_pattern": route.route_pattern,
        "route_mode": route.mode,
        "route_family": route_family_for_section(route.route_family or route.section_guess),
        "section_guess": route.section_guess,
    }
    if description:
        metadata["description"] = description
    if notes:
        metadata["notes"] = notes[:5]
    if table_count:
        metadata["table_count"] = table_count
    if response is not None:
        metadata["status_code"] = response.status_code
        if response.headers.get("Content-Type"):
            metadata["content_type"] = response.headers.get("Content-Type", "")
        if response.encoding:
            metadata["encoding"] = response.encoding
    return metadata


class Normalizer:
    def __init__(self) -> None:
        self.max_chars = int(os.getenv("MAX_CONTENT_RECORD_CHARS", "4000"))

    def normalize_html_record(
        self,
        *,
        company_id: str,
        site_url: str,
        route: RouteStrategy,
        response: requests.Response | None,
        fetch_status: str,
        notes: list[str],
    ) -> ContentRecord:
        if not response:
            fingerprint_source = f"{route.route_pattern}|{fetch_status}|{'|'.join(notes)}"
            metadata = _html_metadata(route=route, response=None, description="", notes=notes, table_count=0)
            return ContentRecord(
                company_id=company_id,
                site_id=site_url,
                site_url=site_url,
                url=route.route_pattern,
                source_type="html",
                source_url_or_file=route.route_pattern,
                section_guess=route.section_guess,
                text="",
                tables=[],
                metadata=metadata,
                evidence_ref={
                    "kind": "html_page",
                    "source_url_or_file": route.route_pattern,
                    "route_pattern": route.route_pattern,
                },
                extraction_method=route.mode,
                fetch_status=fetch_status,
                content_fingerprint=hashlib.sha1(fingerprint_source.encode("utf-8")).hexdigest(),
                notes=notes[:5],
                trace=_route_taxonomy_trace(route),
            )

        html = response.text or ""
        soup = BeautifulSoup(html, "html.parser")
        meta = parse_title_and_meta(soup)
        raw_text = normalize_whitespace(soup.get_text(" ", strip=True))
        raw_text_compact = compact_text(raw_text, self.max_chars)
        cleaned_text = compact_text(raw_text, self.max_chars)
        tables = _extract_html_tables(soup)
        date_guess = extract_date_from_text(raw_text[:2000])
        fingerprint_source = cleaned_text or response.url or route.route_pattern
        metadata = _html_metadata(
            route=route,
            response=response,
            description=meta.get("description", ""),
            notes=notes,
            table_count=len(tables),
        )
        trace = _route_taxonomy_trace(route)
        trace["html"] = {
            "description": meta.get("description", ""),
            "table_count": len(tables),
        }
        return ContentRecord(
            company_id=company_id,
            site_id=site_url,
            site_url=site_url,
            url=response.url,
            source_type="html",
            source_url_or_file=response.url,
            title=meta["title"],
            text=cleaned_text,
            tables=tables,
            metadata=metadata,
            evidence_ref={
                "kind": "html_page",
                "source_url_or_file": response.url,
                "route_pattern": route.route_pattern,
                "status_code": response.status_code,
            },
            date=date_guess,
            raw_text=raw_text_compact,
            cleaned_text=cleaned_text,
            section_guess=route.section_guess,
            extraction_method=route.mode,
            fetch_status=fetch_status,
            content_fingerprint=hashlib.sha1(fingerprint_source.encode("utf-8")).hexdigest(),
            notes=notes[:5],
            trace=trace,
        )

from __future__ import annotations

from bs4 import BeautifulSoup

import company_enrichment_core as core


class BaseSource:
    source_name = "base"

    def __init__(self, client: core.RateLimitedHttpClient) -> None:
        self.client = client

    def search(self, row: core.RowInput) -> core.SourceResult:
        raise NotImplementedError

    @staticmethod
    def _extract_generic_contacts(page_url: str, soup: BeautifulSoup, page_text: str) -> dict[str, list[core.ContactItem]]:
        anchors = soup.select("a[href]")
        contacts: dict[str, list[core.ContactItem]] = {field_name: [] for field_name in core.SHARED_CONTACT_FIELDS}
        phones = contacts["phones"]
        emails = contacts["emails"]
        websites = contacts["websites"]
        addresses = contacts["addresses"]

        for anchor in anchors:
            href = core.normalize_whitespace(anchor.get("href", ""))
            if href.startswith("tel:"):
                normalized_phone = core.normalize_phone_candidate(href.replace("tel:", "", 1))
                if not normalized_phone:
                    normalized_phone = core.normalize_phone_candidate(anchor.get_text(" ", strip=True))
                if normalized_phone:
                    phones.append(core.ContactItem(value=normalized_phone, source_url=page_url, kind="phone"))
            elif href.startswith("mailto:"):
                emails.append(core.ContactItem(value=href.replace("mailto:", "").strip(), source_url=page_url, kind="email"))
            elif href.startswith("http"):
                cleaned = core.sanitize_website_url(href)
                if cleaned:
                    websites.append(core.ContactItem(value=cleaned, source_url=page_url, kind="website"))

        for value in core.extract_emails(page_text):
            emails.append(core.ContactItem(value=value, source_url=page_url, kind="email"))
        for value in core.extract_probable_addresses(page_text):
            addresses.append(core.ContactItem(value=value, source_url=page_url, kind="address"))

        return {field_name: core.dedupe_contact_items(items) for field_name, items in contacts.items()}


def entity_page_matches_row_inn(row: core.RowInput, meta: dict[str, str], page_text: str) -> bool:
    if not row.inn:
        return True
    haystack = " ".join(
        part
        for part in (
            meta.get("title", ""),
            meta.get("description", ""),
            page_text,
        )
        if part
    )
    return row.inn in haystack


def _mark_terminal_source_result(
    result: core.SourceResult,
    *,
    status: str,
    note: str = "",
    entity_url: str = "",
) -> None:
    result.status = status
    if entity_url:
        result.entity_url = entity_url
    normalized_note = core.normalize_whitespace(note)
    if normalized_note:
        result.notes.append(normalized_note)
    core.clear_source_contact_fields(result)


def mark_source_blocked(result: core.SourceResult, *, reason: str, status: str = "blocked") -> None:
    _mark_terminal_source_result(result, status=status, note=reason)
    core.mark_source_blocked(result, reason=reason)


def mark_source_not_found(result: core.SourceResult, *, reason: str) -> None:
    _mark_terminal_source_result(result, status="not_found", note=reason)


def mark_source_entity_mismatch(result: core.SourceResult, row: core.RowInput, company_url: str) -> None:
    _mark_terminal_source_result(
        result,
        status="mismatch",
        entity_url=company_url,
        note=f"Карточка источника не содержит ожидаемый ИНН {row.inn}; данные по ней отброшены",
    )

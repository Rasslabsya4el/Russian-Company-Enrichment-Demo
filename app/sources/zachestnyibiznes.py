from __future__ import annotations

import re
from urllib.parse import quote_plus, urljoin

from bs4 import BeautifulSoup

import company_enrichment_core as core
from .base import (
    BaseSource,
    entity_page_matches_row_inn,
    mark_source_blocked,
    mark_source_entity_mismatch,
    mark_source_not_found,
)

_BARE_DOMAIN_PATTERN = re.compile(
    r"\b(?:https?://)?(?:www\.)?[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?(?:\.[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?)+\b",
    flags=re.IGNORECASE,
)
_SEARCH_NOT_FOUND_MARKERS = (
    "ничего не найдено",
    "не найдено ни одной компании",
    "компания не найдена",
    "по вашему запросу ничего не найдено",
    "результаты поиска: 0",
)
_PROFILE_PAGE_MARKERS = (
    "огрн",
    "руководитель",
    "официальный сайт",
    "юридический адрес",
    "основной вид деятельности",
)
RETRYABLE_ZACHESTNYIBIZNES_REQUEST_ERROR_MARKERS = (
    "connect timeout",
    "connecttimeouterror",
    "remotedisconnected",
    "remote end closed connection without response",
    "connection aborted",
    "connection reset by peer",
    "connection reset",
    "read timed out",
    "read timeout",
    "timed out",
)


def _append_error(result: core.SourceResult, error: str | None) -> None:
    normalized_error = core.normalize_whitespace(error)
    if normalized_error:
        result.errors.append(normalized_error)


def _contact_items_from_urls(text: str, page_url: str) -> list[core.ContactItem]:
    items: list[core.ContactItem] = []
    for raw_url in core.extract_urls(text):
        cleaned = core.sanitize_website_url(raw_url)
        if cleaned:
            items.append(core.ContactItem(value=cleaned, source_url=page_url, kind="website"))
    return items


def _recover_official_site(page_text: str, page_url: str) -> list[core.ContactItem]:
    site_value = core.label_value(page_text, "Официальный сайт")
    if not site_value:
        site_value = core.extract_text_snippet(page_text, "Официальный сайт", span=120)
    if not site_value or "не указан" in site_value.lower():
        return []

    recovered = _contact_items_from_urls(site_value, page_url)
    if recovered:
        return recovered

    for candidate in _BARE_DOMAIN_PATTERN.findall(site_value):
        cleaned = core.sanitize_website_url(candidate)
        if cleaned:
            return [core.ContactItem(value=cleaned, source_url=page_url, kind="website")]
    return []


def _looks_like_search_not_found(page_text: str) -> bool:
    lowered = page_text.lower()
    return any(marker in lowered for marker in _SEARCH_NOT_FOUND_MARKERS)


def _looks_like_company_profile(meta: dict[str, str], page_text: str) -> bool:
    title = core.normalize_whitespace(meta.get("title", ""))
    description = core.normalize_whitespace(meta.get("description", ""))
    lowered = page_text.lower()
    marker_hits = sum(1 for marker in _PROFILE_PAGE_MARKERS if marker in lowered)
    return bool(title) and (marker_hits >= 2 or (marker_hits >= 1 and bool(description)))


class ZachestnyBiznesSource(BaseSource):
    source_name = "zachestnyibiznes"

    def _is_retryable_request_outcome(self, outcome: core.RequestOutcome) -> bool:
        if outcome.ok:
            return False
        normalized_status = core.normalize_whitespace(outcome.status)
        if outcome.blocked and normalized_status == "http_403":
            return True
        detail = core.normalize_whitespace(outcome.error).lower()
        if normalized_status == "blocked":
            return outcome.timeout and any(
                marker in detail
                for marker in ("connect timeout", "connecttimeouterror")
            )
        if normalized_status != "request_error":
            return False
        if outcome.timeout:
            return True
        if not detail:
            return False
        return any(marker in detail for marker in RETRYABLE_ZACHESTNYIBIZNES_REQUEST_ERROR_MARKERS)

    def _request_outcome(self, url: str, *, allow_transient_retry: bool = False) -> core.RequestOutcome:
        outcome = self.client.request(url, source=self.source_name)
        if allow_transient_retry and self._is_retryable_request_outcome(outcome):
            return self.client.request(url, source=self.source_name)
        return outcome

    @staticmethod
    def _recover_shared_contacts(
        page_url: str,
        meta: dict[str, str],
        page_text: str,
    ) -> dict[str, list[core.ContactItem]]:
        description = core.normalize_whitespace(meta.get("description", ""))
        recovered = {
            "websites": _contact_items_from_urls(description, page_url),
            "addresses": [
                core.ContactItem(value=address, source_url=page_url, kind="address")
                for address in core.extract_probable_addresses(description)
            ],
        }
        recovered["websites"].extend(_recover_official_site(page_text, page_url))
        return {
            field_name: core.dedupe_contact_items(items)
            for field_name, items in recovered.items()
        }

    def search(self, row: core.RowInput) -> core.SourceResult:
        search_url = f"https://zachestnyibiznes.ru/search?query={quote_plus(row.inn)}"
        result = core.SourceResult(source=self.source_name, status="pending", search_url=search_url)
        outcome = self._request_outcome(search_url, allow_transient_retry=True)
        if not outcome.ok or not outcome.response:
            _append_error(result, outcome.error)
            mark_source_blocked(result, reason=outcome.error or outcome.status or "blocked")
            return result

        response = outcome.response
        result.http_status = response.status_code
        result.listing_url = response.url
        if core.looks_like_bot_gate(response, response.text):
            mark_source_blocked(result, reason="Search page is blocked by bot gate")
            return result
        soup = BeautifulSoup(response.text, "html.parser")
        search_page_text = core.normalize_whitespace(soup.get_text(" ", strip=True))
        link = soup.select_one('a[href*="/company/ul/"]')
        if not link:
            if _looks_like_search_not_found(search_page_text):
                mark_source_not_found(result, reason="Компания не найдена в поиске ЗАЧЕСТНЫЙБИЗНЕС")
                core.finalize_source_availability(result)
            else:
                mark_source_blocked(result, reason="Search page did not expose a company card")
            return result

        entity_url = urljoin(response.url, link["href"])
        page = self._request_outcome(entity_url, allow_transient_retry=True)
        if not page.ok or not page.response:
            result.entity_url = entity_url
            _append_error(result, page.error)
            mark_source_blocked(result, reason=page.error or page.status or "blocked")
            return result

        company_response = page.response
        result.entity_url = company_response.url
        if core.looks_like_bot_gate(company_response, company_response.text):
            mark_source_blocked(result, reason="Company page is blocked by bot gate")
            return result
        result.links = [link_url for link_url in (result.listing_url, result.entity_url) if link_url]
        company_soup = BeautifulSoup(company_response.text, "html.parser")
        page_text = core.normalize_whitespace(company_soup.get_text(" ", strip=True))
        meta = core.parse_title_and_meta(company_soup)
        result.company_name_found = meta["title"]
        if meta["description"]:
            result.snippets.append(meta["description"])
        if not _looks_like_company_profile(meta, page_text):
            mark_source_blocked(result, reason="Company page did not expose a company card")
            return result
        if not entity_page_matches_row_inn(row, meta, page_text):
            mark_source_entity_mismatch(result, row, company_response.url)
            core.finalize_source_availability(result)
            return result

        generic = self._extract_generic_contacts(company_response.url, company_soup, page_text)
        recovered = self._recover_shared_contacts(company_response.url, meta, page_text)
        result.phones.extend(generic["phones"])
        result.emails.extend(generic["emails"])
        result.websites.extend(generic["websites"])
        result.websites.extend(recovered["websites"])
        result.addresses.extend(generic["addresses"])
        result.addresses.extend(recovered["addresses"])

        result.status = "success"
        result.phones = core.dedupe_contact_items(result.phones)
        result.emails = core.dedupe_contact_items(result.emails)
        result.websites = core.dedupe_contact_items(result.websites)
        result.addresses = core.dedupe_contact_items(result.addresses)
        core.finalize_source_availability(result)
        return result

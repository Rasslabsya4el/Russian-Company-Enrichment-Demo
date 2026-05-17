from __future__ import annotations

from urllib.parse import quote_plus, urljoin

from bs4 import BeautifulSoup

import company_enrichment_core as core
from .base import BaseSource, entity_page_matches_row_inn, mark_source_entity_mismatch


SPARK_DEMO_MASK_STATUS = "guest"
SPARK_DEMO_MASK_REASON = "СПАРК показывает часть данных только в demo-режиме"
SPARK_FIELD_ABSENT_REASON = "СПАРК пишет: Сведения отсутствуют"


SPARK_DIRECT_TRANSPORT_RETRY_ATTEMPTS = 2
SPARK_DIRECT_READ_TIMEOUT_RETRY_ATTEMPTS = 3
SPARK_DIRECT_READ_TIMEOUT_RETRY_ERROR_MARKERS = (
    "read timed out",
    "read timeout",
    "readtimeout",
)
SPARK_DIRECT_TLS_EOF_RETRY_ERROR_MARKERS = (
    "ssleoferror",
    "unexpected_eof_while_reading",
    "eof occurred in violation of protocol",
)
SPARK_DIRECT_CONNECTION_RESET_RETRY_ERROR_MARKERS = (
    "connectionreseterror",
    "connection reset by peer",
    "forcibly closed by the remote host",
    "remote end closed connection without response",
    "remote host closed",
    "winerror 10054",
    "10054",
)


def _spark_value_is_masked(value_node: BeautifulSoup) -> bool:
    own_classes = set(value_node.get("class", []))
    return (
        value_node.select_one(".plug-value.js-popup-open") is not None
        or ("js-popup-open" in own_classes and value_node.get("data-popup") == "demo")
        or value_node.select_one(".js-popup-open[data-popup='demo']") is not None
    )


def _spark_value_is_absent(value_text: str) -> bool:
    return "сведения отсутствуют" in value_text.lower()


def _spark_field_status(result: core.SourceResult, field_name: str) -> str:
    payload = result.availability.get(field_name) or {}
    return core.normalize_source_availability_status(str(payload.get("status", "")), default="")


def _remember_masked_field(result: core.SourceResult, field_name: str, value_text: str, reason: str) -> None:
    current_payload = result.availability.get(field_name) or {}
    current_examples = list(current_payload.get("masked_examples") or [])
    masked_example = core.normalize_whitespace(value_text)
    if masked_example:
        result.masked_rows.append(f"{field_name}:{masked_example}")
        current_examples.append(masked_example)
    core.set_field_availability(
        result,
        field_name,
        "masked",
        reason=reason or str(current_payload.get("reason", "")),
        masked_examples=core.dedupe_preserve_order(current_examples),
        open_count=0,
    )


def _remember_absent_field(result: core.SourceResult, field_name: str, reason: str) -> None:
    if _spark_field_status(result, field_name) == "masked":
        return
    core.set_field_availability(result, field_name, "absent", reason=reason, open_count=0)


class SparkSource(BaseSource):
    source_name = "spark"

    def _direct_transport_retry_attempts(self, outcome: core.RequestOutcome) -> int:
        if outcome.ok or outcome.response is not None or outcome.status != "request_error":
            return 1
        if outcome.proxy_mode not in {"", "direct"}:
            return 1
        lowered_error = core.normalize_whitespace(outcome.error).lower()
        if any(marker in lowered_error for marker in SPARK_DIRECT_READ_TIMEOUT_RETRY_ERROR_MARKERS):
            return SPARK_DIRECT_READ_TIMEOUT_RETRY_ATTEMPTS
        if any(marker in lowered_error for marker in SPARK_DIRECT_TLS_EOF_RETRY_ERROR_MARKERS):
            return SPARK_DIRECT_TRANSPORT_RETRY_ATTEMPTS
        if any(marker in lowered_error for marker in SPARK_DIRECT_CONNECTION_RESET_RETRY_ERROR_MARKERS):
            return SPARK_DIRECT_TRANSPORT_RETRY_ATTEMPTS
        return 1

    def _request_with_direct_transport_retry(self, url: str) -> core.RequestOutcome:
        last_outcome: core.RequestOutcome | None = None
        attempts = 0
        allowed_attempts = 1
        while attempts < allowed_attempts:
            outcome = self.client.request(url, source=self.source_name)
            attempts += 1
            last_outcome = outcome
            retry_attempts = self._direct_transport_retry_attempts(outcome)
            if retry_attempts <= 1:
                return outcome
            allowed_attempts = max(allowed_attempts, retry_attempts)
        return last_outcome if last_outcome is not None else self.client.request(url, source=self.source_name)

    def search(self, row: core.RowInput) -> core.SourceResult:
        search_url = f"https://spark-interfax.ru/search?Query={quote_plus(row.inn)}"
        result = core.SourceResult(source=self.source_name, status="pending", search_url=search_url)
        outcome = self._request_with_direct_transport_retry(search_url)
        if not outcome.ok or not outcome.response:
            result.status = outcome.status
            result.errors.append(outcome.error)
            core.mark_source_blocked(result, reason=outcome.error or outcome.status)
            return result

        response = outcome.response
        result.http_status = response.status_code
        result.listing_url = response.url
        soup = BeautifulSoup(response.text, "html.parser")
        link = soup.select_one(f'a[href*="-inn-{row.inn}-"]')
        if not link:
            result.status = "not_found"
            result.notes.append("Компания не найдена в поиске СПАРК")
            core.finalize_source_availability(result)
            return result

        entity_url = urljoin(response.url, link["href"])
        page = self._request_with_direct_transport_retry(entity_url)
        if not page.ok or not page.response:
            result.status = page.status
            result.entity_url = entity_url
            result.errors.append(page.error)
            core.mark_source_blocked(result, reason=page.error or page.status)
            return result

        company_response = page.response
        result.entity_url = company_response.url
        result.links = [search_url, entity_url]
        company_soup = BeautifulSoup(company_response.text, "html.parser")
        page_text = core.normalize_whitespace(company_soup.get_text(" ", strip=True))
        meta = core.parse_title_and_meta(company_soup)
        result.company_name_found = meta["title"]
        if meta["description"]:
            result.snippets.append(meta["description"])
        if not entity_page_matches_row_inn(row, meta, page_text):
            mark_source_entity_mismatch(result, row, company_response.url)
            core.finalize_source_availability(result)
            return result

        generic = self._extract_generic_contacts(company_response.url, company_soup, page_text)
        generic_addresses = list(generic["addresses"])
        saw_demo_mask = False

        contacts_header = company_soup.select_one('h2#_contacts')
        contacts_section = contacts_header.find_parent("div", class_="company-params-section") if contacts_header else None
        if contacts_section:
            for row_block in contacts_section.select(".company-characteristics__row"):
                name_node = row_block.select_one(".company-characteristics__name")
                value_node = row_block.select_one(".company-characteristics__value")
                if not name_node or not value_node:
                    continue
                label = core.normalize_whitespace(name_node.get_text(" ", strip=True)).lower()
                value_text = core.normalize_whitespace(value_node.get_text(" ", strip=True))
                is_masked = _spark_value_is_masked(value_node)
                is_absent = _spark_value_is_absent(value_text)
                if "телефон" in label:
                    if is_masked:
                        _remember_masked_field(
                            result,
                            "phones",
                            value_text,
                            reason="СПАРК показывает телефон под demo-маской",
                        )
                        saw_demo_mask = True
                    elif is_absent:
                        _remember_absent_field(result, "phones", SPARK_FIELD_ABSENT_REASON)
                    else:
                        for phone in core.extract_phones(value_text):
                            result.phones.append(core.ContactItem(value=phone, source_url=company_response.url, kind="phone"))
                elif "почта" in label or "email" in label:
                    if is_masked:
                        _remember_masked_field(
                            result,
                            "emails",
                            value_text,
                            reason="СПАРК показывает email под demo-маской",
                        )
                        saw_demo_mask = True
                    elif is_absent:
                        _remember_absent_field(result, "emails", SPARK_FIELD_ABSENT_REASON)
                    else:
                        for email in core.extract_emails(value_text):
                            result.emails.append(core.ContactItem(value=email, source_url=company_response.url, kind="email"))
                elif label == "сайт":
                    if is_masked:
                        _remember_masked_field(
                            result,
                            "websites",
                            value_text,
                            reason="СПАРК показывает сайт под demo-маской",
                        )
                        saw_demo_mask = True
                    elif is_absent:
                        _remember_absent_field(result, "websites", SPARK_FIELD_ABSENT_REASON)
                    else:
                        for anchor in value_node.select('a[href]'):
                            href = core.sanitize_website_url(anchor.get("href", ""))
                            if href:
                                result.websites.append(core.ContactItem(value=href, source_url=company_response.url, kind="website"))
                        for url in core.extract_urls(value_text):
                            cleaned = core.sanitize_website_url(url)
                            if cleaned:
                                result.websites.append(core.ContactItem(value=cleaned, source_url=company_response.url, kind="website"))

        for row_block in company_soup.select(".company-characteristics__row"):
            name_node = row_block.select_one(".company-characteristics__name")
            value_node = row_block.select_one(".company-characteristics__value")
            if not name_node or not value_node:
                continue
            label = core.normalize_whitespace(name_node.get_text(" ", strip=True)).lower()
            value_text = core.normalize_whitespace(value_node.get_text(" ", strip=True))
            is_masked = _spark_value_is_masked(value_node)
            is_absent = _spark_value_is_absent(value_text)
            if "адрес" in label:
                if is_masked:
                    _remember_masked_field(
                        result,
                        "addresses",
                        value_text,
                        reason="СПАРК показывает адрес под demo-маской",
                    )
                    saw_demo_mask = True
                elif is_absent:
                    _remember_absent_field(result, "addresses", SPARK_FIELD_ABSENT_REASON)
                elif value_text:
                    result.addresses.append(core.ContactItem(value=value_text, source_url=company_response.url, kind="address"))
            elif label == "руководитель" and is_masked:
                result.masked_rows.append(f"management:{value_text}")
                core.set_field_availability(
                    result,
                    "management",
                    "masked",
                    reason="СПАРК помечает блок руководителя как demo-only",
                    masked_examples=[value_text],
                    open_count=0,
                )
                saw_demo_mask = True
            elif label == "учредители" and is_masked:
                result.masked_rows.append(f"founders:{value_text}")
                core.set_field_availability(
                    result,
                    "founders",
                    "masked",
                    reason="СПАРК помечает блок учредителей как demo-only",
                    masked_examples=[value_text],
                    open_count=0,
                )
                saw_demo_mask = True

        if _spark_field_status(result, "addresses") not in {"masked", "absent"}:
            result.addresses.extend(generic_addresses)
        result.phones = core.dedupe_contact_items(result.phones)
        result.emails = core.dedupe_contact_items(result.emails)
        result.websites = core.dedupe_contact_items(result.websites)
        result.addresses = core.dedupe_contact_items(result.addresses)
        result.masked_rows = core.dedupe_preserve_order(result.masked_rows)
        if saw_demo_mask:
            result.status = SPARK_DEMO_MASK_STATUS
            result.notes.append(SPARK_DEMO_MASK_REASON)
        else:
            result.status = "success"
        core.finalize_source_availability(result)
        return result

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any
from urllib.parse import quote_plus, urljoin, urlparse

import requests
from bs4 import BeautifulSoup, Tag

import company_enrichment_core as core
from .base import (
    BaseSource,
    entity_page_matches_row_inn,
    mark_source_blocked,
    mark_source_entity_mismatch,
    mark_source_not_found,
)


CHECKO_ORIGIN = "https://checko.ru"
CHECKO_RF_PROXY_REQUIRED_REASON = (
    "Checko source is access-gated in current environment: "
    "rf_proxy_required / region-access boundary / access-gated source"
)
CHECKO_FIELD_ABSENT_REASON = "Field not found in Checko company card"
CHECKO_FIELD_UNKNOWN_REASON = "Checko company card did not confirm this block"
CHECKO_CARD_MARKERS = (
    "огрн",
    "инн",
    "кпп",
    "юридический адрес",
    "контакты",
    "виды деятельности",
)
CHECKO_SEARCH_NOT_FOUND_MARKERS = (
    "ничего не найдено",
    "по вашему запросу ничего не найдено",
    "организации не найдены",
    "результаты поиска: 0",
)
CHECKO_ACCESS_GATED_TEXT_MARKERS = (
    "429 too many requests",
    "access denied",
    "доступ ограничен",
    "доступ запрещен",
    "доступ запрещён",
    "только для пользователей из россии",
    "доступен только из россии",
    "российский ip",
    "рф прокси",
    "rf proxy",
    "region access",
)
CHECKO_PROXY_RESET_RETRY_ATTEMPTS = 2
CHECKO_PROXY_RESET_ERROR_MARKERS = (
    "connectionreseterror",
    "connection reset",
    "connection aborted",
    "forcibly closed",
    "remote host closed",
    "10054",
)
CHECKO_PROXY_READ_TIMEOUT_ERROR_MARKERS = (
    "read timed out",
    "read timeout",
    "timed out",
    "timeout",
)
CHECKO_STOP_LABELS = (
    "дата регистрации",
    "вид деятельности",
    "юридический адрес",
    "адрес",
    "организационно-правовая форма",
    "уставный капитал",
    "финансовая отчетность",
    "выручка",
    "чистая прибыль",
    "специальный налоговый режим",
    "генеральный директор",
    "директор",
    "руководитель",
    "учредитель",
    "учредители",
    "контакты неверны",
    "виды деятельности оквэд",
    "виды деятельности",
    "налоги и сборы",
    "реквизиты",
    "сведения о регистрации",
    "коды статистики",
)
CHECKO_SECTION_STOPS = (
    "оценка надежности",
    "реквизиты",
    "сведения о регистрации",
    "коды статистики",
    "контакты неверны",
    "виды деятельности оквэд",
    "виды деятельности",
    "финансовая отчетность",
    "налоги и сборы",
    "руководитель",
    "учредитель",
)
CHECKO_MANAGEMENT_LABELS = (
    "генеральный директор",
    "директор",
    "руководитель",
)
CHECKO_FOUNDER_LABELS = (
    "учредитель",
    "учредители",
)
CHECKO_COMPANY_NAME_MARKERS = (
    "ооо",
    "ао",
    "пао",
    "зао",
    "ип",
    "общество",
    "акционерное",
    "индивидуальный предприниматель",
)
CHECKO_REGION_MARKERS = (
    "область",
    "край",
    "республика",
    "автономный округ",
    "автономная область",
    "г.",
    "город",
    "район",
)
CHECKO_OKVED_RE = re.compile(r"(?<!\d)(\d{2}(?:\.\d{1,2}){0,3})(?!\d)")
CHECKO_INN_RE = re.compile(r"(?<!\d)(\d{10}|\d{12})(?!\d)")
CHECKO_BARE_DOMAIN_RE = re.compile(
    r"\b(?:https?://)?(?:www\.)?[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?"
    r"(?:\.[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?)+\b",
    flags=re.IGNORECASE,
)


@dataclass(frozen=True)
class CheckoListingCandidate:
    entity_url: str
    company_name: str = ""
    inn: str = ""
    context: str = ""


@dataclass(frozen=True)
class CheckoListingResolution:
    status: str
    entity_url: str = ""
    note: str = ""


def decode_response_text(response: requests.Response) -> str:
    try:
        return response.text or ""
    except Exception:
        encoding = response.encoding or getattr(response, "apparent_encoding", None) or "utf-8"
        try:
            return response.content.decode(encoding, errors="replace")
        except Exception:
            return response.content.decode("utf-8", errors="replace")


def detect_checko_access_boundary(
    *,
    request_status: str = "",
    response_status: int | None = None,
    html: str = "",
) -> tuple[str, str] | None:
    normalized_status = core.normalize_whitespace(request_status)
    lowered_text = core.normalize_whitespace(html).lower()

    if normalized_status == core.REQUEST_STATUS_BLOCKED_NO_PROXY:
        reason_detail = core.normalize_whitespace(html)
        if not reason_detail:
            reason_detail = "proxy-bound runtime blocked outbound request before dispatch"
        return (
            core.REQUEST_STATUS_BLOCKED_NO_PROXY,
            f"{CHECKO_RF_PROXY_REQUIRED_REASON} ({reason_detail}; direct request disabled)",
        )

    if normalized_status == "rate_limited" or response_status == 429 or "429 too many requests" in lowered_text:
        return "rate_limited", f"{CHECKO_RF_PROXY_REQUIRED_REASON} (HTTP 429 Too Many Requests)"

    if normalized_status == "bot_gate":
        return "blocked", f"{CHECKO_RF_PROXY_REQUIRED_REASON} (bot/captcha gate)"

    if normalized_status == "http_403" or response_status == 403:
        return "blocked", f"{CHECKO_RF_PROXY_REQUIRED_REASON} (HTTP 403 Forbidden)"

    if any(marker in lowered_text for marker in CHECKO_ACCESS_GATED_TEXT_MARKERS):
        return "blocked", CHECKO_RF_PROXY_REQUIRED_REASON

    return None


def resolve_checko_listing_entity(
    row: core.RowInput,
    *,
    listing_url: str,
    html: str,
) -> CheckoListingResolution:
    soup = BeautifulSoup(html, "html.parser")
    page_text = core.normalize_whitespace(soup.get_text(" ", strip=True))
    meta = core.parse_title_and_meta(soup)

    if _looks_like_company_card(meta, page_text) and entity_page_matches_row_inn(row, meta, page_text):
        return CheckoListingResolution(
            status="resolved",
            entity_url=listing_url,
            note=f"Checko search resolved directly to company card for ИНН {row.inn}",
        )

    candidates = extract_checko_listing_candidates(listing_url, html)
    exact_matches = [candidate for candidate in candidates if candidate.inn == row.inn]
    if exact_matches:
        return CheckoListingResolution(
            status="resolved",
            entity_url=exact_matches[0].entity_url,
            note=f"Checko listing matched exact ИНН {row.inn}",
        )

    if len(candidates) == 1:
        return CheckoListingResolution(
            status="resolved",
            entity_url=candidates[0].entity_url,
            note=f"Checko listing returned a single company candidate for ИНН query {row.inn}",
        )

    if _looks_like_search_not_found(page_text):
        return CheckoListingResolution(
            status="not_found",
            note=f"Компания с ИНН {row.inn} не найдена в поиске Checko",
        )

    if candidates:
        return CheckoListingResolution(
            status="unresolved",
            note=(
                f"Checko search returned {len(candidates)} company candidates for ИНН {row.inn}, "
                "but none exposed an unambiguous exact match"
            ),
        )

    return CheckoListingResolution(
        status="unresolved",
        note=f"Checko search page did not expose a company candidate for ИНН {row.inn}",
    )


def parse_checko_company_html(company_url: str, html: str) -> dict[str, Any]:
    soup = BeautifulSoup(html, "html.parser")
    meta = core.parse_title_and_meta(soup)
    page_text = core.normalize_whitespace(soup.get_text(" ", strip=True))
    lines = _normalized_text_lines(soup)
    contact_block = _extract_section_text(
        soup,
        start_markers=("контакты",),
        stop_markers=("контакты неверны", "виды деятельности", "финансовая отчетность"),
    )
    okved_block = _extract_section_text(
        soup,
        start_markers=("виды деятельности оквэд", "виды деятельности"),
        stop_markers=("финансовая отчетность", "налоги и сборы", "руководитель", "учредитель"),
    )

    phone_values = core.normalize_phone_values(core.extract_phones(contact_block))
    email_values = core.extract_emails(contact_block)
    website_values = _extract_website_candidates(contact_block)

    address_candidates: list[str] = []
    address_candidates.extend(core.extract_probable_addresses(contact_block))
    address_candidates.extend(core.extract_probable_addresses(page_text))
    legal_address = _extract_following_value(lines, labels=("юридический адрес", "адрес"))
    if legal_address:
        address_candidates.append(legal_address)

    primary_okved, additional_okveds = _parse_okved_entries(okved_block)
    company_name = _extract_company_name(meta, lines, soup)

    notes: list[str] = []
    description = core.normalize_whitespace(meta.get("description", ""))
    snippets: list[str] = []
    if description:
        snippets.append(description)

    management_count = _count_heading_occurrences(lines, CHECKO_MANAGEMENT_LABELS)
    founders_count = _count_heading_occurrences(lines, CHECKO_FOUNDER_LABELS)

    availability = {
        "management": core.build_field_availability_payload(
            "open" if management_count else "unknown",
            reason="Checko company card exposes management data" if management_count else CHECKO_FIELD_UNKNOWN_REASON,
            open_count=management_count,
        ),
        "founders": core.build_field_availability_payload(
            "open" if founders_count else "unknown",
            reason="Checko company card exposes founder data" if founders_count else CHECKO_FIELD_UNKNOWN_REASON,
            open_count=founders_count,
        ),
    }

    return {
        "company_name": company_name,
        "title": meta.get("title", ""),
        "description": description,
        "page_text": page_text,
        "phones": [
            core.ContactItem(value=value, source_url=company_url, kind="phone")
            for value in phone_values
        ],
        "emails": [
            core.ContactItem(value=value, source_url=company_url, kind="email")
            for value in email_values
        ],
        "websites": [
            core.ContactItem(value=value, source_url=company_url, kind="website")
            for value in website_values
        ],
        "addresses": [
            core.ContactItem(value=value, source_url=company_url, kind="address")
            for value in _dedupe_addresses(address_candidates)
        ],
        "primary_okved": primary_okved,
        "additional_okveds": additional_okveds,
        "notes": notes,
        "snippets": snippets,
        "availability": availability,
    }


def extract_checko_listing_candidates(listing_url: str, html: str) -> list[CheckoListingCandidate]:
    soup = BeautifulSoup(html, "html.parser")
    candidates: list[CheckoListingCandidate] = []
    seen_urls: set[str] = set()

    for anchor in soup.select('a[href*="/company/"]'):
        href = core.normalize_whitespace(anchor.get("href", ""))
        if not href:
            continue
        entity_url = urljoin(listing_url, href)
        parsed = urlparse(entity_url)
        if parsed.netloc != "checko.ru" or "/company/" not in parsed.path:
            continue
        if entity_url in seen_urls:
            continue
        seen_urls.add(entity_url)

        context = _candidate_context(anchor)
        inn = ""
        inn_match = CHECKO_INN_RE.search(context)
        if inn_match:
            inn = core.normalize_inn(inn_match.group(1))

        candidates.append(
            CheckoListingCandidate(
                entity_url=entity_url,
                company_name=core.normalize_whitespace(anchor.get_text(" ", strip=True)),
                inn=inn,
                context=context,
            )
        )

    return candidates


def _looks_like_search_not_found(page_text: str) -> bool:
    lowered = page_text.lower()
    return any(marker in lowered for marker in CHECKO_SEARCH_NOT_FOUND_MARKERS)


def _looks_like_company_card(meta: dict[str, str], page_text: str) -> bool:
    title = core.normalize_whitespace(meta.get("title", ""))
    if not title:
        return False
    lowered = page_text.lower()
    marker_hits = sum(1 for marker in CHECKO_CARD_MARKERS if marker in lowered)
    return marker_hits >= 3 and "инн" in lowered


def _normalized_text_lines(soup: BeautifulSoup) -> list[str]:
    return [
        core.normalize_whitespace(line)
        for line in soup.get_text("\n", strip=True).splitlines()
        if core.normalize_whitespace(line)
    ]


def _candidate_context(anchor: Tag) -> str:
    best = core.normalize_whitespace(anchor.get_text(" ", strip=True))
    for parent in anchor.parents:
        if not isinstance(parent, Tag):
            continue
        parent_text = core.normalize_whitespace(parent.get_text(" ", strip=True))
        if len(parent_text) < max(len(best), 20):
            continue
        if len(parent_text) <= 500:
            best = parent_text
        if "инн" in parent_text.lower():
            return parent_text
    return best


def _extract_section_text(
    soup: BeautifulSoup,
    *,
    start_markers: tuple[str, ...],
    stop_markers: tuple[str, ...],
) -> str:
    heading_candidates = [
        tag
        for tag in soup.find_all(re.compile(r"^h[1-6]$", flags=re.IGNORECASE))
        if any(marker in core.normalize_whitespace(tag.get_text(" ", strip=True)).lower() for marker in start_markers)
    ]
    if heading_candidates:
        heading = heading_candidates[-1]
        chunks = [core.normalize_whitespace(heading.get_text(" ", strip=True))]
        for sibling in heading.next_siblings:
            if isinstance(sibling, Tag) and sibling.name and re.fullmatch(r"h[1-6]", sibling.name, flags=re.IGNORECASE):
                break
            text = (
                core.normalize_whitespace(sibling.get_text("\n", strip=True))
                if isinstance(sibling, Tag)
                else core.normalize_whitespace(str(sibling))
            )
            if text:
                chunks.append(text)
        prepared = "\n".join(chunk for chunk in chunks if chunk)
        if prepared:
            return _truncate_section_text(prepared, stop_markers=stop_markers)

    lines = _normalized_text_lines(soup)
    matched_indexes = [
        index
        for index, line in enumerate(lines)
        if any(line.lower().startswith(marker) for marker in start_markers)
    ]
    if not matched_indexes:
        return ""

    start_index = matched_indexes[-1]
    collected: list[str] = []
    for line in lines[start_index:]:
        lowered = line.lower()
        if collected and any(lowered.startswith(marker) for marker in stop_markers):
            break
        collected.append(line)
    return _truncate_section_text("\n".join(collected), stop_markers=stop_markers)


def _extract_following_value(lines: list[str], *, labels: tuple[str, ...]) -> str:
    normalized_labels = tuple(label.lower() for label in labels)
    for index, line in enumerate(lines):
        lowered = line.lower()
        inline_value = _extract_inline_labeled_value(line, normalized_labels)
        if inline_value:
            return inline_value
        if lowered not in normalized_labels:
            continue
        values: list[str] = []
        for candidate in lines[index + 1 : index + 5]:
            candidate_lower = candidate.lower()
            if any(candidate_lower.startswith(stop_label) for stop_label in CHECKO_STOP_LABELS):
                break
            values.append(candidate)
            if any(ch.isdigit() for ch in candidate):
                break
        joined = core.normalize_whitespace(" ".join(values))
        if joined:
            return joined
    return ""


def _extract_inline_labeled_value(line: str, labels: tuple[str, ...]) -> str:
    lowered = line.lower()
    for label in labels:
        if lowered == label:
            return ""
        if lowered.startswith(label + " "):
            return core.normalize_whitespace(line[len(label) :])
        if lowered.startswith(label + ":"):
            return core.normalize_whitespace(line[len(label) + 1 :])
    return ""


def _extract_company_name(meta: dict[str, str], lines: list[str], soup: BeautifulSoup) -> str:
    heading = soup.select_one("h1")
    short_name = core.normalize_whitespace(heading.get_text(" ", strip=True) if heading else "")
    full_name = ""
    if short_name:
        try:
            line_index = lines.index(short_name)
        except ValueError:
            line_index = -1
        if line_index >= 0:
            for candidate in lines[line_index + 1 : line_index + 5]:
                if len(candidate) <= len(short_name):
                    continue
                if _looks_like_company_name(candidate):
                    full_name = candidate
                    break

    meta_name = _clean_checko_title(meta.get("title", ""))
    for candidate in (full_name, meta_name, short_name):
        if candidate:
            return candidate
    return ""


def _looks_like_company_name(value: str) -> bool:
    lowered = core.normalize_whitespace(value).lower()
    return any(marker in lowered for marker in CHECKO_COMPANY_NAME_MARKERS)


def _clean_checko_title(title: str) -> str:
    normalized = core.normalize_whitespace(title)
    if not normalized:
        return ""

    cleaned = re.split(r"\s+[—-]\s+ИНН\s+\d{10,12}\b", normalized, maxsplit=1, flags=re.IGNORECASE)[0]
    cleaned = re.split(r"\s+[—-]\s+ОГРН\s+\d{13}\b", cleaned, maxsplit=1, flags=re.IGNORECASE)[0]
    parts = [part.strip() for part in re.split(r"\s+[—-]\s+", cleaned) if part.strip()]
    if len(parts) >= 2 and _looks_like_region_segment(parts[-1]):
        cleaned = " - ".join(parts[:-1])
    return core.normalize_whitespace(cleaned)


def _looks_like_region_segment(value: str) -> bool:
    lowered = core.normalize_whitespace(value).lower()
    return any(marker in lowered for marker in CHECKO_REGION_MARKERS)


def _extract_website_candidates(text: str) -> list[str]:
    candidates: list[str] = []
    for raw_url in core.extract_urls(text):
        cleaned = core.sanitize_website_url(raw_url)
        if cleaned and urlparse(cleaned).netloc.lower() != "checko.ru":
            candidates.append(cleaned)
    for match in CHECKO_BARE_DOMAIN_RE.finditer(text):
        if match.start() > 0 and text[match.start() - 1] == "@":
            continue
        cleaned = core.sanitize_website_url(match.group(0))
        if cleaned and urlparse(cleaned).netloc.lower() != "checko.ru":
            candidates.append(cleaned)
    return core.dedupe_preserve_order(candidates)


def _truncate_section_text(text: str, *, stop_markers: tuple[str, ...]) -> str:
    lines = [core.normalize_whitespace(line) for line in text.splitlines() if core.normalize_whitespace(line)]
    if not lines:
        return ""

    collected: list[str] = []
    for index, line in enumerate(lines):
        lowered = line.lower()
        if index > 0 and any(lowered.startswith(marker) for marker in stop_markers):
            break
        collected.append(line)
    return "\n".join(collected)


def _dedupe_addresses(values: list[str]) -> list[str]:
    normalized: list[str] = []
    normalized_keys: list[str] = []
    for value in values:
        cleaned = core.sanitize_address_candidate(value)
        if not cleaned:
            continue
        dedupe_key = re.sub(r"[^0-9a-zа-я]+", "", cleaned.lower().replace("ё", "е"))
        resolved = False
        for index, existing_key in enumerate(normalized_keys):
            if dedupe_key == existing_key or dedupe_key in existing_key:
                resolved = True
                break
            if existing_key in dedupe_key:
                normalized[index] = cleaned
                normalized_keys[index] = dedupe_key
                resolved = True
                break
        if resolved:
            continue
        normalized.append(cleaned)
        normalized_keys.append(dedupe_key)
    return normalized


def _parse_okved_entries(okved_block: str) -> tuple[core.OkvedEntry | None, list[core.OkvedEntry]]:
    normalized_lines = [
        core.normalize_whitespace(line)
        for line in okved_block.splitlines()
        if core.normalize_whitespace(line)
    ]
    entries: list[core.OkvedEntry] = []
    seen: set[tuple[str, str]] = set()
    for line in normalized_lines:
        match = CHECKO_OKVED_RE.search(line)
        if not match:
            continue
        code = match.group(1)
        before = core.normalize_whitespace(line[: match.start()]).strip(" -:;,.")
        after = core.normalize_whitespace(line[match.end() :]).strip(" -:;,.")
        label = after or before
        if not label or "виды деятельности" in label.lower():
            continue
        key = (code, label)
        if key in seen:
            continue
        seen.add(key)
        entries.append(core.OkvedEntry(code=code, label=label))

    if not entries:
        return None, []
    return entries[0], entries[1:]


def _count_heading_occurrences(lines: list[str], labels: tuple[str, ...]) -> int:
    normalized_labels = tuple(label.lower() for label in labels)
    count = 0
    for line in lines:
        lowered = line.lower()
        if lowered in normalized_labels:
            count += 1
            continue
        if any(lowered.startswith(label + " ") or lowered.startswith(label + ":") for label in normalized_labels):
            count += 1
    return count


class CheckoSource(BaseSource):
    source_name = "checko"

    def _is_retryable_proxy_reset(self, outcome: core.RequestOutcome) -> bool:
        if outcome.ok or outcome.response is not None or outcome.status != "request_error":
            return False
        if outcome.proxy_mode not in {"proxy", "proxy_bound", "proxy-bound"}:
            return False
        lowered_error = core.normalize_whitespace(outcome.error).lower()
        return (
            outcome.timeout
            or any(marker in lowered_error for marker in CHECKO_PROXY_RESET_ERROR_MARKERS)
            or any(marker in lowered_error for marker in CHECKO_PROXY_READ_TIMEOUT_ERROR_MARKERS)
        )

    def _request_with_proxy_reset_retry(self, url: str) -> core.RequestOutcome:
        last_outcome: core.RequestOutcome | None = None
        for _ in range(CHECKO_PROXY_RESET_RETRY_ATTEMPTS):
            outcome = self.client.request(url, source=self.source_name)
            last_outcome = outcome
            if not self._is_retryable_proxy_reset(outcome):
                return outcome
        return last_outcome if last_outcome is not None else self.client.request(url, source=self.source_name)

    def search(self, row: core.RowInput) -> core.SourceResult:
        search_url = f"{CHECKO_ORIGIN}/search?query={quote_plus(row.inn)}"
        result = core.SourceResult(source=self.source_name, status="pending", search_url=search_url)
        result.notes.append("checko_search_path=inn")

        outcome = self._request_with_proxy_reset_retry(search_url)
        response = outcome.response
        if response is not None:
            result.http_status = response.status_code
            result.listing_url = response.url

        boundary = detect_checko_access_boundary(
            request_status=outcome.status,
            response_status=response.status_code if response is not None else None,
            html=decode_response_text(response) if response is not None else outcome.error,
        )
        if boundary:
            status, reason = boundary
            if outcome.error and outcome.error != "429 Too Many Requests":
                result.errors.append(outcome.error)
            mark_source_blocked(result, reason=reason, status=status)
            return result

        if not outcome.ok or response is None:
            if outcome.error:
                result.errors.append(outcome.error)
            mark_source_blocked(
                result,
                reason=outcome.error or outcome.status or "blocked",
                status=outcome.status or "blocked",
            )
            return result

        listing_html = decode_response_text(response)
        resolution = resolve_checko_listing_entity(row, listing_url=response.url, html=listing_html)
        if resolution.note:
            result.notes.append(resolution.note)

        if resolution.status == "not_found":
            mark_source_not_found(result, reason=resolution.note or f"Компания с ИНН {row.inn} не найдена в Checko")
            core.finalize_source_availability(result)
            return result

        if resolution.status != "resolved" or not resolution.entity_url:
            mark_source_blocked(result, reason=resolution.note or "Checko listing resolution failed")
            return result

        result.entity_url = resolution.entity_url
        result.links = core.dedupe_preserve_order([search_url, result.listing_url, result.entity_url])

        company_response = response
        if core.normalize_whitespace(result.entity_url) != core.normalize_whitespace(response.url):
            page = self._request_with_proxy_reset_retry(result.entity_url)
            company_response = page.response
            if company_response is not None:
                result.http_status = company_response.status_code
            boundary = detect_checko_access_boundary(
                request_status=page.status,
                response_status=company_response.status_code if company_response is not None else None,
                html=decode_response_text(company_response) if company_response is not None else page.error,
            )
            if boundary:
                status, reason = boundary
                if page.error and page.error != "429 Too Many Requests":
                    result.errors.append(page.error)
                mark_source_blocked(result, reason=reason, status=status)
                return result
            if not page.ok or company_response is None:
                if page.error:
                    result.errors.append(page.error)
                mark_source_blocked(
                    result,
                    reason=page.error or page.status or "blocked",
                    status=page.status or "blocked",
                )
                return result

        result.entity_url = company_response.url
        result.http_status = company_response.status_code
        result.links = core.dedupe_preserve_order([search_url, result.listing_url, result.entity_url])

        company_html = decode_response_text(company_response)
        boundary = detect_checko_access_boundary(
            response_status=company_response.status_code,
            html=company_html,
        )
        if boundary:
            status, reason = boundary
            mark_source_blocked(result, reason=reason, status=status)
            return result

        company_soup = BeautifulSoup(company_html, "html.parser")
        page_text = core.normalize_whitespace(company_soup.get_text(" ", strip=True))
        meta = core.parse_title_and_meta(company_soup)
        if not _looks_like_company_card(meta, page_text):
            mark_source_blocked(result, reason="Checko company page did not expose a parsable company card")
            return result

        payload = parse_checko_company_html(result.entity_url, company_html)
        result.company_name_found = core.normalize_whitespace(str(payload.get("company_name", ""))) or _clean_checko_title(meta.get("title", ""))
        if not entity_page_matches_row_inn(row, meta, str(payload.get("page_text", ""))):
            mark_source_entity_mismatch(result, row, result.entity_url)
            core.finalize_source_availability(result)
            return result

        result.phones = core.dedupe_contact_items(list(payload.get("phones", [])))
        result.emails = core.dedupe_contact_items(list(payload.get("emails", [])))
        result.websites = core.dedupe_contact_items(list(payload.get("websites", [])))
        result.addresses = core.dedupe_contact_items(list(payload.get("addresses", [])))
        result.primary_okved = payload.get("primary_okved")
        result.additional_okveds = list(payload.get("additional_okveds", []))
        result.availability = {
            field_name: dict(field_payload)
            for field_name, field_payload in (payload.get("availability") or {}).items()
            if field_name in core.IMPORTANT_FIELDS and isinstance(field_payload, dict)
        }

        for note in payload.get("notes", []) or []:
            normalized_note = core.normalize_whitespace(str(note))
            if normalized_note:
                result.notes.append(normalized_note)
        for snippet in payload.get("snippets", []) or []:
            normalized_snippet = core.normalize_whitespace(str(snippet))
            if normalized_snippet:
                result.snippets.append(normalized_snippet)

        result.status = "success"
        core.finalize_source_availability(result)
        return result

from __future__ import annotations

import json
import os
import re
from pathlib import Path
from threading import Lock
from typing import Any
from urllib.parse import quote_plus, urljoin

import requests
from bs4 import BeautifulSoup, Tag

import company_enrichment_core as core
from app.runtime.captcha import (
    CaptchaChallenge,
    CaptchaKind,
    detect_captcha_kind,
    extract_sitekey_from_html,
    solve_captcha,
)
from app.runtime.config import resolve_rusprofile_session_profile_file
from .base import BaseSource, entity_page_matches_row_inn, mark_source_entity_mismatch


RUSPROFILE_ORIGIN = "https://www.rusprofile.ru"
LOGIN_ENDPOINT = f"{RUSPROFILE_ORIGIN}/auth.php?action=login"
CAPTCHA_VALIDATE_ENDPOINT = f"{RUSPROFILE_ORIGIN}/captcha-validate"
CSRF_COOKIE_NAME = "__Host-csrf-token"
CSRF_HEADER_NAME = "X-Csrf-Token"
DEFAULT_AUTH_PROBE_URL = f"{RUSPROFILE_ORIGIN}/id/792592"
DEFAULT_SESSION_PROFILE_PATH = Path("runtime_local") / "browser_sessions" / "rusprofile_session_profile.json"
CARD_CLEANUP_SELECTORS = (
    ".copy_button",
    ".company-info__quetip_growth",
    ".company-info__quetip_fell",
    ".ico",
    "svg",
    "use",
    "sup",
)
ADDRESS_LABEL_HINTS = (
    "юридический адрес",
    "фактический адрес",
    "почтовый адрес",
    "адрес",
)
FOOTER_CLASS_MARKERS = (
    "footer",
    "breadcrumbs",
    "breadcrumb",
    "copy",
    "copyright",
)
OKVED_CODE_RE = re.compile(r"\b\d{2}(?:\.\d{1,2}){0,3}\b")
OKVED_LABEL_MARKERS = (
    "\u043e\u043a\u0432\u044d\u0434",
    "\u0432\u0438\u0434 \u0434\u0435\u044f\u0442\u0435\u043b\u044c\u043d",
)
OKVED_PRIMARY_LABEL_MARKERS = (
    "\u043e\u0441\u043d\u043e\u0432\u043d",
)
OKVED_ADDITIONAL_LABEL_MARKERS = (
    "\u0434\u043e\u043f\u043e\u043b\u043d\u0438\u0442",
)
OKVED_TRIM_CHARS = " ,;:-\u2013\u2014"
AUTHORIZED_ONLY_BOUNDARY_REASON = "Rusprofile company card requires an authorized session for full data"
RUSPROFILE_FIELD_ABSENT_REASON = "Field not found in Rusprofile company card"
RETRYABLE_RUSPROFILE_REQUEST_ERROR_MARKERS = (
    "remotedisconnected",
    "remote end closed connection without response",
    "connection aborted",
    "connection reset by peer",
    "connection reset",
    "connect timeout",
    "connecttimeouterror",
    "failed to resolve",
    "getaddrinfo failed",
    "nameresolutionerror",
    "name or service not known",
    "ssleoferror",
    "temporary failure in name resolution",
    "unexpected_eof_while_reading",
    "eof occurred in violation of protocol",
    "read timed out",
    "read timeout",
    "readtimeout",
)
RETRYABLE_RUSPROFILE_HTTP_STATUSES = frozenset({"http_503"})


def decode_response_text(response: requests.Response) -> str:
    try:
        return response.text or ""
    except Exception:
        encoding = response.encoding or getattr(response, "apparent_encoding", None) or "utf-8"
        try:
            return response.content.decode(encoding, errors="replace")
        except Exception:
            return response.content.decode("utf-8", errors="replace")


def element_to_clean_text(node: Tag) -> str:
    clone_soup = BeautifulSoup(str(node), "html.parser")
    clone_node = clone_soup.find(node.name)
    if not clone_node:
        return core.normalize_whitespace(node.get_text(" ", strip=True))
    for selector in CARD_CLEANUP_SELECTORS:
        for hit in clone_node.select(selector):
            hit.decompose()
    return core.normalize_whitespace(clone_node.get_text(" ", strip=True))


def _normalize_address_text(value: str) -> str:
    text = core.normalize_whitespace(value)
    if not text:
        return ""
    text = re.sub(r"\s+,", ",", text)
    text = re.sub(r",\s*,", ", ", text)
    text = re.sub(r"\s{2,}", " ", text)
    text = re.sub(r"(д\.\s*\d+)\s+(\d+\b)", r"\1\2", text, flags=re.IGNORECASE)
    return text.strip(" ,;")


def _address_compare_key(value: str) -> str:
    lowered = value.lower().replace("ё", "е")
    return re.sub(r"[^0-9a-zа-я]+", "", lowered)


def merge_addresses_prefer_complete(candidates: list[str]) -> list[str]:
    merged: list[str] = []
    merged_keys: list[str] = []
    for raw_value in candidates:
        normalized = _normalize_address_text(raw_value)
        if len(normalized) < 12:
            continue
        key = _address_compare_key(normalized)
        if len(key) < 10:
            continue

        resolved = False
        for index, existing_key in enumerate(merged_keys):
            if key == existing_key:
                if len(normalized) > len(merged[index]):
                    merged[index] = normalized
                    merged_keys[index] = key
                resolved = True
                break
            if key in existing_key:
                resolved = True
                break
            if existing_key in key:
                merged[index] = normalized
                merged_keys[index] = key
                resolved = True
                break
        if not resolved:
            merged.append(normalized)
            merged_keys.append(key)
    return merged


def _looks_like_okved_label(value: str) -> bool:
    normalized = core.normalize_whitespace(value).lower()
    if not normalized:
        return False
    if any(marker in normalized for marker in OKVED_LABEL_MARKERS):
        return True
    return "\u0432\u0438\u0434" in normalized and "\u0434\u0435\u044f\u0442" in normalized


def _build_okved_payload(code: str, label: str) -> dict[str, str] | None:
    code_clean = core.normalize_whitespace(code).strip(OKVED_TRIM_CHARS)
    label_clean = core.normalize_whitespace(label).strip(OKVED_TRIM_CHARS)
    if not code_clean or not label_clean:
        return None
    return {
        "code": code_clean,
        "label": label_clean,
        "display": core.build_okved_display(code_clean, label_clean),
    }


def _parse_okved_segment(text: str) -> dict[str, str] | None:
    normalized = core.normalize_whitespace(text)
    if not normalized:
        return None
    match = OKVED_CODE_RE.search(normalized)
    if not match:
        return None

    code = match.group(0)
    suffix = core.normalize_whitespace(normalized[match.end() :]).strip(OKVED_TRIM_CHARS)
    if suffix:
        return _build_okved_payload(code, suffix)

    prefix = core.normalize_whitespace(normalized[: match.start()]).strip(OKVED_TRIM_CHARS)
    if prefix and not _looks_like_okved_label(prefix):
        return _build_okved_payload(code, prefix)
    return None


def _dedupe_okved_payloads(entries: list[dict[str, str]]) -> list[dict[str, str]]:
    seen: set[tuple[str, str]] = set()
    deduped: list[dict[str, str]] = []
    for entry in entries:
        code = core.normalize_whitespace(str(entry.get("code", "")))
        label = core.normalize_whitespace(str(entry.get("label", "")))
        if not code or not label:
            continue
        key = (code, label)
        if key in seen:
            continue
        seen.add(key)
        deduped.append(
            {
                "code": code,
                "label": label,
                "display": core.build_okved_display(code, label),
            }
        )
    return deduped


def _extract_okved_entries_from_text(text: str) -> list[dict[str, str]]:
    normalized = core.normalize_whitespace(text)
    if not normalized:
        return []

    matches = list(OKVED_CODE_RE.finditer(normalized))
    if not matches:
        return []

    entries: list[dict[str, str]] = []
    for index, match in enumerate(matches):
        next_start = matches[index + 1].start() if index + 1 < len(matches) else len(normalized)
        entry = _parse_okved_segment(normalized[match.start() : next_start])
        if entry:
            entries.append(entry)
    return _dedupe_okved_payloads(entries)


def _extract_okved_entries_from_node(node: Tag) -> list[dict[str, str]]:
    candidate_texts: list[str] = []
    for selector in ("li", "p", "div", "a"):
        for candidate in node.select(selector):
            text = element_to_clean_text(candidate)
            if text:
                candidate_texts.append(text)

    structured_entries: list[dict[str, str]] = []
    for text in core.dedupe_preserve_order(candidate_texts):
        structured_entries.extend(_extract_okved_entries_from_text(text))
    if structured_entries:
        return _dedupe_okved_payloads(structured_entries)

    return _extract_okved_entries_from_text(element_to_clean_text(node))


def extract_okveds_from_cards(cards: list[dict[str, Any]]) -> tuple[dict[str, str] | None, list[dict[str, str]]]:
    primary: dict[str, str] | None = None
    additional_candidates: list[dict[str, str]] = []

    for card in cards:
        label = core.normalize_whitespace(str(card.get("label", ""))).lower()
        if not _looks_like_okved_label(label):
            continue

        entries_payload = card.get("okved_entries")
        entries = _dedupe_okved_payloads(entries_payload) if isinstance(entries_payload, list) else []
        if not entries:
            entries = _extract_okved_entries_from_text(str(card.get("value", "")))
        if not entries:
            continue

        is_primary = any(marker in label for marker in OKVED_PRIMARY_LABEL_MARKERS)
        is_additional = any(marker in label for marker in OKVED_ADDITIONAL_LABEL_MARKERS)

        if is_primary and primary is None:
            primary = entries[0]
            additional_candidates.extend(entries[1:])
            continue

        if primary is None and not is_additional:
            primary = entries[0]
            additional_candidates.extend(entries[1:])
            continue

        additional_candidates.extend(entries)

    additional: list[dict[str, str]] = []
    primary_key = None
    if primary:
        primary_key = (primary["code"], primary["label"])
    for entry in _dedupe_okved_payloads(additional_candidates):
        if primary_key and (entry["code"], entry["label"]) == primary_key:
            continue
        additional.append(entry)

    return primary, additional


def collect_structured_addresses(soup: BeautifulSoup, cards: list[dict[str, Any]]) -> list[str]:
    candidates: list[str] = []

    for card in cards:
        label = core.normalize_whitespace(card.get("label", "")).lower()
        value = core.normalize_whitespace(card.get("value", ""))
        if label and any(hint in label for hint in ADDRESS_LABEL_HINTS) and value:
            candidates.append(value)

    for node in soup.select("[itemprop='address']"):
        text_value = element_to_clean_text(node)
        if text_value:
            candidates.append(text_value)

        postal_node = node.select_one("[itemprop='postalCode']")
        region_node = node.select_one("[itemprop='addressRegion']")
        street_node = node.select_one("[itemprop='streetAddress']")
        postal = core.normalize_whitespace(postal_node.get_text(" ", strip=True) if postal_node else "")
        region = core.normalize_whitespace(region_node.get_text(" ", strip=True) if region_node else "")
        street = core.normalize_whitespace(street_node.get_text(" ", strip=True) if street_node else "")
        if street and (postal or region):
            candidates.append(", ".join(part for part in (postal, region, street) if part))

    return merge_addresses_prefer_complete(candidates)


def _is_footer_link(anchor: Tag) -> bool:
    data_track_click = core.normalize_whitespace(anchor.get("data-track-click", "")).lower()
    if "contacts,site" in data_track_click:
        return False

    for parent in anchor.parents:
        if not isinstance(parent, Tag):
            continue
        if parent.name == "footer":
            return True
        class_text = " ".join(parent.get("class", [])).lower()
        id_text = core.normalize_whitespace(parent.get("id", "")).lower()
        if any(marker in class_text for marker in FOOTER_CLASS_MARKERS):
            return True
        if any(marker in id_text for marker in FOOTER_CLASS_MARKERS):
            return True
    return False


def extract_rusprofile_websites(soup: BeautifulSoup) -> list[str]:
    candidates: list[str] = []
    primary_selectors = (
        "#contacts-row a[itemprop='url'][href]",
        "#contacts-row a[href^='http']",
        ".company-info__contact.site a[href^='http']",
        "a[data-track-click*='contacts,site'][href^='http']",
    )
    for selector in primary_selectors:
        for anchor in soup.select(selector):
            href = core.normalize_whitespace(anchor.get("href", ""))
            cleaned = core.sanitize_website_url(href, keep_path=True)
            if cleaned:
                candidates.append(cleaned)
    if candidates:
        return core.dedupe_preserve_order(candidates)

    fallback_candidates: list[str] = []
    for anchor in soup.select("a[href^='http']"):
        if _is_footer_link(anchor):
            continue
        href = core.normalize_whitespace(anchor.get("href", ""))
        cleaned = core.sanitize_website_url(href, keep_path=True)
        if cleaned:
            fallback_candidates.append(cleaned)
    return core.dedupe_preserve_order(fallback_candidates)


def _extract_contact_note(anchor: Tag) -> str:
    container = anchor.find_parent("div", class_="company-info__contact")
    if container is None:
        return ""
    note_node = container.select_one(".company-info__contact-notice")
    return core.normalize_whitespace(note_node.get_text(" ", strip=True) if note_node else "")


def _collect_visible_contacts(soup: BeautifulSoup) -> dict[str, list[dict[str, str]]]:
    contacts = {"phones": [], "emails": [], "websites": []}

    for anchor in soup.select("#contacts-row a[href^='tel:'], a[data-track-click*='contacts,phone'][href^='tel:']"):
        raw_value = core.normalize_whitespace(anchor.get_text(" ", strip=True)) or core.normalize_whitespace(
            anchor.get("href", "").replace("tel:", "", 1)
        )
        normalized = core.normalize_phone_candidate(raw_value)
        if normalized:
            contacts["phones"].append({"value": normalized, "note": _extract_contact_note(anchor)})

    for anchor in soup.select("#contacts-row a[href^='mailto:'], a[data-track-click*='contacts,email'][href^='mailto:']"):
        email = core.normalize_whitespace(anchor.get("href", "").replace("mailto:", "", 1)) or core.normalize_whitespace(
            anchor.get_text(" ", strip=True)
        )
        if email:
            contacts["emails"].append({"value": email.lower(), "note": _extract_contact_note(anchor)})

    for website in extract_rusprofile_websites(soup):
        contacts["websites"].append({"value": website, "note": ""})

    return contacts


def detect_page_access_state(html: str) -> tuple[str, str]:
    soup = BeautifulSoup(html, "html.parser")
    main_info = soup.select_one("#main_info")
    data_user = core.normalize_whitespace(main_info.get("data-user", "") if main_info else "")
    if data_user.lower() in {"true", "1"}:
        return "authorized", data_user
    if data_user.lower() in {"false", "0"}:
        return "guest", data_user

    user_match = re.search(r"RPF\.store\.user\s*=\s*(\{.*?\})\s*;", html, flags=re.DOTALL)
    if user_match:
        try:
            payload = json.loads(user_match.group(1))
            return ("authorized" if payload.get("id") else "guest"), data_user
        except Exception:
            return "unknown", data_user
    return "unknown", data_user


def detect_logged_in(html: str) -> tuple[bool, str]:
    page_state, data_user = detect_page_access_state(html)
    return page_state == "authorized", data_user


def get_csrf_token(session: requests.Session) -> str:
    return core.normalize_whitespace(session.cookies.get(CSRF_COOKIE_NAME, ""))


def build_ajax_headers(company_url: str, csrf_token: str) -> dict[str, str]:
    return {
        "Origin": RUSPROFILE_ORIGIN,
        "Referer": company_url,
        "X-Requested-With": "XMLHttpRequest",
        CSRF_HEADER_NAME: csrf_token,
    }


def parse_sitekeys(html: str) -> tuple[str, str]:
    soup = BeautifulSoup(html, "html.parser")
    main_info = soup.select_one("#main_info")
    sitekey = core.normalize_whitespace(main_info.get("data-sitekey", "") if main_info else "")
    invisible_sitekey = core.normalize_whitespace(main_info.get("data-invisible-sitekey", "") if main_info else "")
    return sitekey, invisible_sitekey


def safe_json(response: requests.Response) -> dict[str, object]:
    try:
        payload = response.json()
        if isinstance(payload, dict):
            return payload
    except Exception:
        pass
    return {"raw": decode_response_text(response)[:2000], "status_code": response.status_code}


def needs_captcha(login_json: dict[str, object]) -> bool:
    code = int(login_json.get("code", 0) or 0)
    message = core.normalize_whitespace(str(login_json.get("message", ""))).lower()
    if code == 255:
        return True
    return any(marker in message for marker in ("captcha", "капч", "робот", "автоматичес"))


def describe_login_failure(login_json: dict[str, object]) -> str:
    status_code = core.normalize_whitespace(str(login_json.get("status_code", "")))
    code = core.normalize_whitespace(str(login_json.get("code", "")))
    message = core.normalize_whitespace(str(login_json.get("message", "")))
    error = core.normalize_whitespace(str(login_json.get("error", "")))
    details: list[str] = []
    if status_code:
        details.append(f"http={status_code}")
    if code:
        details.append(f"code={code}")
    if message:
        details.append(f"message={message}")
    if error and error != message:
        details.append(f"error={error}")
    if details:
        return f"Rusprofile login failed: {', '.join(details)}"

    raw_excerpt = core.normalize_whitespace(str(login_json.get("raw", "")))
    if raw_excerpt:
        prefix = f"http={status_code}, " if status_code else ""
        return f"Rusprofile login failed: {prefix}non_json={raw_excerpt[:240]}"

    if status_code:
        return f"Rusprofile login failed: http={status_code}"
    return "Rusprofile login failed"


def build_company_availability(
    *,
    contacts_open: dict[str, list[dict[str, str]]],
    status: str,
    status_reason: str,
    masked_values: list[str],
) -> dict[str, dict[str, Any]]:
    availability: dict[str, dict[str, Any]] = {}
    normalized_reason = core.normalize_whitespace(status_reason) or AUTHORIZED_ONLY_BOUNDARY_REASON

    for field_name in core.SHARED_CONTACT_FIELDS:
        open_count = len(contacts_open.get(field_name, []))
        if open_count > 0:
            availability[field_name] = core.build_field_availability_payload("open", open_count=open_count)
        elif status == "guest":
            availability[field_name] = core.build_field_availability_payload(
                "masked",
                reason=normalized_reason,
                masked_examples=masked_values,
            )
        else:
            availability[field_name] = core.build_field_availability_payload(
                "absent",
                reason=RUSPROFILE_FIELD_ABSENT_REASON,
            )

    for field_name in core.NON_CONTACT_AVAILABILITY_FIELDS:
        if status == "guest":
            availability[field_name] = core.build_field_availability_payload(
                "masked",
                reason=normalized_reason,
                masked_examples=masked_values,
            )
        else:
            availability[field_name] = core.build_field_availability_payload("unknown")

    return availability


def parse_company_payload(company_url: str, response: requests.Response) -> dict[str, Any]:
    html = decode_response_text(response)
    soup = BeautifulSoup(html, "html.parser")
    meta = core.parse_title_and_meta(soup)
    page_auth_state, data_user_attr = detect_page_access_state(html)
    logged_in = page_auth_state == "authorized"

    cards: list[dict[str, Any]] = []
    for dl in soup.select("dl"):
        dts = dl.select("dt")
        dds = dl.select("dd")
        if not dts or not dds:
            continue
        for dt, dd in zip(dts, dds):
            key = core.normalize_whitespace(dt.get_text(" ", strip=True))
            value_raw = core.normalize_whitespace(dd.get_text(" ", strip=True))
            value_clean = element_to_clean_text(dd)
            if (not value_clean) and dd.select(".under_mask"):
                masked_parts = [
                    core.normalize_whitespace(item.get_text(" ", strip=True))
                    for item in dd.select(".under_mask")
                    if core.normalize_whitespace(item.get_text(" ", strip=True))
                ]
                if masked_parts:
                    value_clean = core.normalize_whitespace(" ".join(masked_parts))
            if key or value_raw or value_clean:
                card_payload: dict[str, Any] = {"label": key, "value": value_clean or value_raw}
                if _looks_like_okved_label(key):
                    card_payload["okved_entries"] = _extract_okved_entries_from_node(dd)
                cards.append(card_payload)

    page_text = core.normalize_whitespace(soup.get_text(" ", strip=True))
    company_name = core.normalize_whitespace(soup.select_one("h1").get_text(" ", strip=True) if soup.select_one("h1") else "")
    if not company_name:
        company_name = meta["title"]

    inn = ""
    ogrn = ""
    for item in cards:
        label = item["label"].lower()
        value = item["value"]
        if not inn and "инн" in label:
            inn_match = re.search(r"\b\d{10,12}\b", value)
            if inn_match:
                inn = inn_match.group(0)
        if not ogrn and "огрн" in label:
            ogrn_match = re.search(r"\b\d{13,15}\b", value)
            if ogrn_match:
                ogrn = ogrn_match.group(0)

    visible_contacts = _collect_visible_contacts(soup)
    structured_addresses = collect_structured_addresses(soup, cards)
    primary_okved, additional_okveds = extract_okveds_from_cards(cards)
    probable_addresses = core.extract_probable_addresses(page_text)
    masked_values = [
        core.normalize_whitespace(node.get_text(" ", strip=True))
        for node in soup.select(".under_mask")
        if core.normalize_whitespace(node.get_text(" ", strip=True))
    ]

    main_info = soup.select_one("#main_info")
    notes: list[str] = []
    if main_info is not None:
        notes.append(f"main_info.data-user={main_info.get('data-user', '')}")
        notes.append(f"main_info.data-sitekey={main_info.get('data-sitekey', '')}")
        notes.append(f"main_info.data-invisible-sitekey={main_info.get('data-invisible-sitekey', '')}")

    payload_status = "success"
    status_reason = ""
    access_boundary = "authorized"
    if page_auth_state != "authorized":
        payload_status = "guest"
        access_boundary = "authorized_only"
        status_reason = (
            f"Rusprofile entity page is not authorized "
            f"(page_state={page_auth_state}, data-user={data_user_attr or 'unknown'})"
        )
    elif masked_values:
        payload_status = "guest"
        access_boundary = "authorized_only"
        status_reason = "Rusprofile entity page still exposes masked values behind an authorized-only boundary"
    if payload_status == "guest":
        primary_okved = None
        additional_okveds = []

    contacts_open = {
        "phones": visible_contacts["phones"],
        "emails": visible_contacts["emails"],
        "websites": visible_contacts["websites"],
        "addresses": [{"value": item, "note": ""} for item in merge_addresses_prefer_complete(structured_addresses + probable_addresses)],
    }
    notes.append(f"page_auth_state={page_auth_state}")
    notes.append(f"payload_status={payload_status}")

    return {
        "status": payload_status,
        "status_reason": status_reason,
        "page_auth_state": page_auth_state,
        "access_boundary": access_boundary,
        "authorization": {
            "page_state": page_auth_state,
            "logged_in": logged_in,
            "data_user_attr": data_user_attr,
            "boundary": access_boundary,
        },
        "final_url": response.url or company_url,
        "title": meta["title"],
        "description": meta["description"],
        "logged_in": logged_in,
        "data_user_attr": data_user_attr,
        "inn": inn,
        "ogrn": ogrn,
        "company_name": company_name,
        "primary_okved": primary_okved,
        "additional_okveds": additional_okveds,
        "contacts_open": contacts_open,
        "availability": build_company_availability(
            contacts_open=contacts_open,
            status=payload_status,
            status_reason=status_reason,
            masked_values=core.dedupe_preserve_order(masked_values),
        ),
        "masked_values_raw": core.dedupe_preserve_order(masked_values),
        "notes": notes,
    }


def apply_open_contacts(result: core.SourceResult, *, source_url: str, contacts_open: dict[str, list[dict[str, str]]]) -> None:
    for item in contacts_open.get("phones", []):
        value = core.normalize_whitespace(str(item.get("value", "")))
        if value:
            result.phones.append(
                core.ContactItem(
                    value=value,
                    source_url=source_url,
                    kind="phone",
                    note=core.normalize_whitespace(str(item.get("note", ""))),
                )
            )
    for item in contacts_open.get("emails", []):
        value = core.normalize_whitespace(str(item.get("value", ""))).lower()
        if value:
            result.emails.append(
                core.ContactItem(
                    value=value,
                    source_url=source_url,
                    kind="email",
                    note=core.normalize_whitespace(str(item.get("note", ""))),
                )
            )
    for item in contacts_open.get("websites", []):
        value = core.sanitize_website_url(str(item.get("value", "")), keep_path=True)
        if value:
            result.websites.append(
                core.ContactItem(
                    value=value,
                    source_url=source_url,
                    kind="website",
                    note=core.normalize_whitespace(str(item.get("note", ""))),
                )
            )
    for item in contacts_open.get("addresses", []):
        value = core.normalize_whitespace(str(item.get("value", "")))
        if value:
            result.addresses.append(
                core.ContactItem(
                    value=value,
                    source_url=source_url,
                    kind="address",
                    note=core.normalize_whitespace(str(item.get("note", ""))),
                )
            )

    result.phones = core.dedupe_contact_items(result.phones)
    result.emails = core.dedupe_contact_items(result.emails)
    result.websites = core.dedupe_contact_items(result.websites)
    result.addresses = core.dedupe_contact_items(result.addresses)


class RusprofileSource(BaseSource):
    source_name = "rusprofile"

    def __init__(self, client: core.RateLimitedHttpClient) -> None:
        super().__init__(client)
        self._auth_lock = Lock()
        self._auth_checked = False
        self._auth_ok = False
        self._auth_error = ""
        self._auth_method = ""
        self._profile_loaded = False
        self._profile_path: Path | None = None

    def _auth_probe_url(self) -> str:
        probe_url = core.normalize_whitespace(os.getenv("RUSPROFILE_AUTH_PROBE_URL", DEFAULT_AUTH_PROBE_URL))
        return probe_url or DEFAULT_AUTH_PROBE_URL

    def _session_profile_path(self) -> Path:
        explicit = core.normalize_whitespace(os.getenv("RUSPROFILE_SESSION_PROFILE_FILE", ""))
        if explicit:
            return Path(explicit).expanduser()
        return Path.cwd() / DEFAULT_SESSION_PROFILE_PATH

    def _auth_event(self, event_type: str, **payload: object) -> None:
        try:
            self.client.progress_store.append_event(
                {
                    "ts": core.utc_now_iso(),
                    "type": event_type,
                    "source": "rusprofile_auth",
                    "host": "www.rusprofile.ru",
                    **payload,
                }
            )
        except Exception:
            pass

    def _source_event(self, event_type: str, **payload: object) -> None:
        try:
            self.client.progress_store.append_event(
                {
                    "ts": core.utc_now_iso(),
                    "type": event_type,
                    "source": self.source_name,
                    "host": "www.rusprofile.ru",
                    **payload,
                }
            )
        except Exception:
            pass

    def _serialize_session_cookies(self) -> list[dict[str, Any]]:
        cookies: list[dict[str, Any]] = []
        for cookie in self.client.session.cookies:
            if not cookie.name or "rusprofile.ru" not in (cookie.domain or ""):
                continue
            cookies.append(
                {
                    "name": cookie.name,
                    "value": cookie.value,
                    "domain": cookie.domain,
                    "path": cookie.path or "/",
                    "secure": bool(cookie.secure),
                }
            )
        return cookies

    def _save_session_profile(self) -> None:
        profile_path = self._profile_path or self._session_profile_path()
        cookies = self._serialize_session_cookies()
        if not cookies:
            return
        profile = {
            "source": "rusprofile",
            "created_at": core.utc_now_iso(),
            "user_agent": core.normalize_whitespace(self.client.session.headers.get("User-Agent", "")),
            "referer": RUSPROFILE_ORIGIN,
            "cookies": cookies,
            "cookie_header": core.cookie_header_from_items(cookies),
        }
        core.save_session_profile(profile_path, profile)
        self._profile_path = profile_path
        self._auth_event("rusprofile_auth_profile_saved", profile_file=str(profile_path))

    def _load_profile_fallback(self) -> bool:
        if self._profile_loaded:
            return False

        raw_profile_file = core.normalize_whitespace(os.getenv("RUSPROFILE_SESSION_PROFILE_FILE", ""))
        profile_path = resolve_rusprofile_session_profile_file(raw_profile_file=raw_profile_file, cwd=Path.cwd())
        if not profile_path:
            return False

        profile = core.load_session_profile(profile_path)
        if not profile:
            return False

        cookies = profile.get("cookies") if isinstance(profile.get("cookies"), list) else []
        for cookie in cookies:
            if not isinstance(cookie, dict):
                continue
            name = core.normalize_whitespace(str(cookie.get("name", "")))
            value = str(cookie.get("value", ""))
            domain = core.normalize_whitespace(str(cookie.get("domain", ""))) or None
            path = core.normalize_whitespace(str(cookie.get("path", "/"))) or "/"
            if not name:
                continue
            self.client.session.cookies.set(name, value, domain=domain, path=path)

        profile_user_agent = core.normalize_whitespace(str(profile.get("user_agent", "")))
        if profile_user_agent:
            self.client.session.headers["User-Agent"] = profile_user_agent

        self._profile_loaded = True
        self._profile_path = profile_path
        self._auth_event("rusprofile_auth_profile_loaded", profile_file=str(profile_path))
        return True

    def _is_retryable_request_outcome(self, outcome: core.RequestOutcome) -> bool:
        if outcome.ok:
            return False
        status = core.normalize_whitespace(outcome.status)
        if status in RETRYABLE_RUSPROFILE_HTTP_STATUSES:
            return True
        if status != "request_error":
            return False
        detail = core.normalize_whitespace(outcome.error).lower()
        if not detail:
            return False
        return any(marker in detail for marker in RETRYABLE_RUSPROFILE_REQUEST_ERROR_MARKERS)

    def _request_outcome(self, url: str, *, allow_transient_retry: bool = False) -> core.RequestOutcome:
        outcome = self.client.request(url, source=self.source_name)
        if allow_transient_retry and self._is_retryable_request_outcome(outcome):
            self._source_event(
                "rusprofile_transient_request_retry",
                url=url,
                request_status=outcome.status,
                error=outcome.error,
                timeout=outcome.timeout,
                attempt=1,
                next_attempt=2,
            )
            return self.client.request(url, source=self.source_name)
        return outcome

    def _request_page(self, url: str, *, allow_transient_retry: bool = False) -> requests.Response | None:
        outcome = self._request_outcome(url, allow_transient_retry=allow_transient_retry)
        if not outcome.ok or not outcome.response:
            return None
        return outcome.response

    def _solve_and_validate_captcha(
        self,
        *,
        company_url: str,
        company_html: str,
        provider_name: str,
        api_key: str,
    ) -> None:
        main_sitekey, invisible_sitekey = parse_sitekeys(company_html)
        detected_kind = detect_captcha_kind(url=company_url, html=company_html, body_text="")
        if detected_kind == CaptchaKind.UNKNOWN:
            detected_kind = CaptchaKind.RECAPTCHA_V2

        sitekey = invisible_sitekey or main_sitekey or extract_sitekey_from_html(company_html, captcha_kind=detected_kind)
        if not sitekey:
            raise RuntimeError("Rusprofile captcha sitekey not found")

        challenge = CaptchaChallenge(kind=detected_kind, website_url=company_url, website_key=sitekey)
        solution = solve_captcha(challenge, provider_name=provider_name, api_key=api_key)
        self._auth_event(
            "rusprofile_auth_captcha_solved",
            captcha_kind=detected_kind.value,
            provider=solution.provider,
            task_type=solution.task_type,
            task_id=solution.task_id,
        )

        csrf_token = get_csrf_token(self.client.session)
        headers = build_ajax_headers(company_url, csrf_token)
        validate_payload = {
            "recaptcha-v2-invisible-token": solution.token,
            "__csrf": csrf_token,
        }
        validate_response = self.client.session.post(
            CAPTCHA_VALIDATE_ENDPOINT,
            headers=headers,
            data=validate_payload,
            timeout=45,
        )
        if validate_response.status_code >= 400:
            raise RuntimeError(f"Rusprofile captcha validate failed: HTTP {validate_response.status_code}")

    def _attempt_login(self, *, company_url: str, page_html: str) -> tuple[bool, str]:
        email = core.normalize_whitespace(os.getenv("RUSPROFILE_EMAIL", ""))
        password = core.normalize_whitespace(os.getenv("RUSPROFILE_PASSWORD", ""))
        provider_name = core.normalize_whitespace(os.getenv("CAPTCHA_PROVIDER", "capmonster"))
        api_key = core.normalize_whitespace(os.getenv("CAPMONSTER_API_KEY", ""))
        if not email or not password:
            return False, "RUSPROFILE_EMAIL/RUSPROFILE_PASSWORD are not configured"

        csrf_token = get_csrf_token(self.client.session)
        if not csrf_token:
            page = self._request_page(company_url)
            if page is not None:
                page_html = decode_response_text(page)
            csrf_token = get_csrf_token(self.client.session)
        if not csrf_token:
            return False, "Rusprofile CSRF token is missing"

        headers = build_ajax_headers(company_url, csrf_token)
        payload = {"login": email, "password": password, "__csrf": csrf_token}
        login_response = self.client.session.post(LOGIN_ENDPOINT, headers=headers, data=payload, timeout=45)
        login_json = safe_json(login_response)
        login_json["status_code"] = login_response.status_code

        if not bool(login_json.get("success")) and needs_captcha(login_json):
            if not api_key:
                return False, "Rusprofile requested captcha, but CAPMONSTER_API_KEY is empty"
            self._solve_and_validate_captcha(
                company_url=company_url,
                company_html=page_html,
                provider_name=provider_name,
                api_key=api_key,
            )
            csrf_token = get_csrf_token(self.client.session)
            headers = build_ajax_headers(company_url, csrf_token)
            payload = {"login": email, "password": password, "__csrf": csrf_token}
            login_response = self.client.session.post(LOGIN_ENDPOINT, headers=headers, data=payload, timeout=45)
            login_json = safe_json(login_response)
            login_json["status_code"] = login_response.status_code

        if not bool(login_json.get("success")):
            return False, describe_login_failure(login_json)
        return True, "ok"

    def _ensure_authenticated(self) -> tuple[bool, str]:
        with self._auth_lock:
            if self._auth_checked:
                return self._auth_ok, self._auth_error

            probe_url = self._auth_probe_url()
            self._load_profile_fallback()
            page = self._request_page(probe_url)
            page_html = decode_response_text(page) if page else ""
            if page:
                page_state, data_user = detect_page_access_state(page_html)
                if page_state == "authorized":
                    self._auth_checked = True
                    self._auth_ok = True
                    self._auth_method = "existing_or_profile_session"
                    self._auth_event("rusprofile_auth_ok", method=self._auth_method, data_user=data_user)
                    self._save_session_profile()
                    return True, ""

            ok, reason = self._attempt_login(company_url=probe_url, page_html=page_html)
            if ok:
                verify_page = self._request_page(probe_url)
                verify_html = decode_response_text(verify_page) if verify_page else ""
                verify_state, verify_data_user = detect_page_access_state(verify_html)
                if verify_state == "authorized":
                    self._auth_checked = True
                    self._auth_ok = True
                    self._auth_method = "password_login"
                    self._auth_event("rusprofile_auth_ok", method=self._auth_method, data_user=verify_data_user)
                    self._save_session_profile()
                    return True, ""

                if self._load_profile_fallback():
                    profile_verify_page = self._request_page(probe_url)
                    profile_verify_html = decode_response_text(profile_verify_page) if profile_verify_page else ""
                    profile_state, profile_data_user = detect_page_access_state(profile_verify_html)
                    if profile_state == "authorized":
                        self._auth_checked = True
                        self._auth_ok = True
                        self._auth_method = "session_profile_fallback"
                        self._auth_event("rusprofile_auth_ok", method=self._auth_method, data_user=profile_data_user)
                        self._save_session_profile()
                        return True, ""
                reason = f"Rusprofile login returned OK but session state is {verify_state or 'unknown'}"

            self._auth_checked = True
            self._auth_ok = False
            self._auth_error = reason
            self._auth_event("rusprofile_auth_failed", reason=reason)
            return False, reason

    def _reset_auth_state(self) -> None:
        self._auth_checked = False
        self._auth_ok = False
        self._auth_error = ""
        self._auth_method = ""
        self._profile_loaded = False

    def _ensure_logged_entity_response(self, response: requests.Response, *, entity_url: str) -> tuple[str, requests.Response | None, str]:
        response_text = decode_response_text(response)
        page_state, data_user = detect_page_access_state(response_text)
        if page_state == "authorized":
            return "success", response, ""

        self._auth_event(
            "rusprofile_auth_guest_entity",
            entity_url=entity_url,
            final_url=response.url,
            data_user=data_user,
            page_state=page_state,
        )
        self._reset_auth_state()
        auth_ok, auth_error = self._ensure_authenticated()
        if not auth_ok:
            return "auth_failed", None, f"Rusprofile entity page returned non-authorized content, auth refresh failed: {auth_error}"

        retry_outcome = self._request_outcome(entity_url, allow_transient_retry=True)
        if not retry_outcome.ok or not retry_outcome.response:
            return retry_outcome.status, None, retry_outcome.error or retry_outcome.status

        retry_response = retry_outcome.response
        retry_state, retry_data_user = detect_page_access_state(decode_response_text(retry_response))
        if retry_state != "authorized":
            return (
                "guest",
                retry_response,
                f"Rusprofile still returns non-authorized entity page after auth refresh "
                f"(page_state={retry_state}, data-user={retry_data_user or 'unknown'})",
            )
        return "success", retry_response, ""

    def search(self, row: core.RowInput) -> core.SourceResult:
        search_url = f"{RUSPROFILE_ORIGIN}/search?query={quote_plus(row.inn)}"
        result = core.SourceResult(source=self.source_name, status="pending", search_url=search_url)

        auth_ok, auth_error = self._ensure_authenticated()
        if not auth_ok:
            result.status = "auth_failed"
            result.errors.append(auth_error)
            core.mark_source_blocked(result, reason=f"Rusprofile auth failed: {auth_error}")
            return result
        if self._auth_method:
            result.notes.append(f"rusprofile_auth={self._auth_method}")

        outcome = self._request_outcome(search_url, allow_transient_retry=True)
        if not outcome.ok or not outcome.response:
            result.status = outcome.status
            result.errors.append(outcome.error)
            core.mark_source_blocked(result, reason=outcome.error or outcome.status)
            return result

        response = outcome.response
        result.http_status = response.status_code
        result.listing_url = response.url

        if "/id/" not in response.url:
            soup = BeautifulSoup(decode_response_text(response), "html.parser")
            company_link = soup.select_one('a[href^="/id/"]')
            if not company_link:
                result.status = "not_found"
                result.notes.append("Не найден переход в карточку компании")
                core.finalize_source_availability(result)
                return result
            entity_url = urljoin(response.url, company_link["href"])
            page = self._request_outcome(entity_url, allow_transient_retry=True)
            if not page.ok or not page.response:
                result.status = page.status
                result.errors.append(page.error)
                result.entity_url = entity_url
                core.mark_source_blocked(result, reason=page.error or page.status)
                return result
            response = page.response

        result.entity_url = response.url
        entity_status, entity_response, entity_error = self._ensure_logged_entity_response(response, entity_url=result.entity_url)
        if entity_response is None:
            result.status = entity_status
            result.errors.append(entity_error or entity_status)
            core.mark_source_blocked(result, reason=entity_error or result.status)
            return result

        response = entity_response
        result.entity_url = response.url
        result.http_status = response.status_code
        result.links = [search_url, response.url]
        payload = parse_company_payload(result.entity_url, response)
        result.company_name_found = core.normalize_whitespace(str(payload.get("company_name", ""))) or core.normalize_whitespace(
            str(payload.get("title", ""))
        )
        result.primary_okved = core.okved_entry_from_dict(payload.get("primary_okved"))
        result.additional_okveds = core.okved_entries_from_payload(payload.get("additional_okveds"))
        description = core.normalize_whitespace(str(payload.get("description", "")))
        if description:
            result.snippets.append(description)

        response_text = decode_response_text(response)
        haystack = " ".join(part for part in (result.company_name_found, description, response_text) if part)
        meta_for_match = {"title": result.company_name_found, "description": description}
        if not entity_page_matches_row_inn(row, meta_for_match, haystack):
            mark_source_entity_mismatch(result, row, response.url)
            core.finalize_source_availability(result)
            return result

        apply_open_contacts(result, source_url=response.url, contacts_open=payload.get("contacts_open", {}))
        result.availability = {
            field_name: dict(field_payload)
            for field_name, field_payload in (payload.get("availability", {}) or {}).items()
            if field_name in core.IMPORTANT_FIELDS and isinstance(field_payload, dict)
        }
        result.masked_rows = [
            str(value)
            for value in payload.get("masked_values_raw", [])
            if core.normalize_whitespace(str(value))
        ]
        for note in payload.get("notes", []) or []:
            cleaned_note = core.normalize_whitespace(str(note))
            if cleaned_note:
                result.notes.append(cleaned_note)
        if entity_error and entity_status == "guest":
            result.notes.append(entity_error)

        payload_status = core.normalize_whitespace(str(payload.get("status", "")))
        if payload_status == "guest":
            result.status = "guest"
            guest_reason = core.normalize_whitespace(str(payload.get("status_reason", ""))) or entity_error or AUTHORIZED_ONLY_BOUNDARY_REASON
            result.notes.append(guest_reason)
            core.finalize_source_availability(result)
            return result

        result.status = "success"
        core.finalize_source_availability(result)
        return result

from __future__ import annotations

import re
from collections import Counter
from dataclasses import dataclass, field
from typing import Any, Callable
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup

from .common import surplus_only_keywords

ACTIVITY_PROFILE_TOKEN_RE = re.compile(r"[a-zа-яё]{3,}", flags=re.IGNORECASE)
SURPLUS_ACTIVITY_TERMS = frozenset(keyword for keyword in surplus_only_keywords if " " not in keyword)
SURPLUS_ACTIVITY_PHRASES = tuple(
    keyword.lower().replace("ё", "е")
    for keyword in sorted(surplus_only_keywords)
    if " " in keyword
)
SURPLUS_ACTIVITY_STEMS = frozenset(
    {
        *(keyword.lower().replace("ё", "е") for keyword in SURPLUS_ACTIVITY_TERMS),
        "реализ",
        "складск",
        "остатк",
        "невостреб",
    }
)
CORPORATE_IDENTITY_TERM_STEMS = frozenset(
    {
        "завод",
        "производ",
        "цех",
        "металлоконструк",
        "оборудован",
        "комбинат",
        "фабрик",
        "предприят",
    }
)


def _normalize_activity_fragment(value: str | None) -> str:
    return re.sub(r"\s+", " ", (value or "").strip().lower().replace("ё", "е")).strip()


def _matched_surplus_phrase_stems(text: str) -> set[str]:
    matched_stems: set[str] = set()
    normalized_text = _normalize_activity_fragment(text)
    for phrase in SURPLUS_ACTIVITY_PHRASES:
        if phrase and phrase in normalized_text:
            for token in ACTIVITY_PROFILE_TOKEN_RE.findall(phrase):
                normalized_token = _normalize_activity_fragment(token)
                if normalized_token:
                    matched_stems.add(normalized_token)
                    matched_stems.add(normalized_token[:7])
    return matched_stems


def _is_identity_activity_token(token: str, company_name_tokens: set[str]) -> bool:
    normalized = _normalize_activity_fragment(token)
    if not normalized:
        return False
    if normalized in company_name_tokens:
        return True
    return any(identity_stem in normalized for identity_stem in CORPORATE_IDENTITY_TERM_STEMS)


def _is_surplus_activity_token(token: str, *, phrase_stems: set[str], company_name_tokens: set[str]) -> bool:
    normalized = _normalize_activity_fragment(token)
    if not normalized or _is_identity_activity_token(normalized, company_name_tokens):
        return False
    if any(phrase_stem and phrase_stem in normalized for phrase_stem in phrase_stems):
        return True
    return any(stem and stem in normalized for stem in SURPLUS_ACTIVITY_STEMS)


SOFT_404_PATTERNS = {
    "404",
    "page not found",
    "the requested page does not exist",
    "страница не найдена",
    "запрашиваемая страница не найдена",
    "запрошенная страница не существует",
}

DOMAIN_PARKING_PATTERNS = {
    "domain is for sale",
    "this domain is for sale",
    "buy this domain",
    "domain parking",
    "sedoparking",
    "afternic",
    "parkingcrew",
}

DIRECTORY_PORTAL_PATTERNS = {
    "каталог компаний",
    "справочник организаций",
    "база компаний",
    "directory of companies",
    "business directory",
}

SITE_AUTH_STATUS_RANK = {"verified": 0, "candidate": 1, "suspicious": 2, "rejected": 3}
LOW_PRIORITY_EXTRA_CHECK_QUEUE_FAMILY = "low_priority_extra_check"
LOW_PRIORITY_LLM_QUEUE_FAMILY = "low_priority_llm"


@dataclass(frozen=True)
class SiteAuthHelpers:
    normalize_url: Callable[[str], str]
    normalize_whitespace: Callable[[str | None], str]
    parse_title_and_meta: Callable[[BeautifulSoup], dict[str, str]]
    dedupe_preserve_order: Callable[[Any], list[str]]
    extract_emails: Callable[[str], list[str]]
    extract_phones: Callable[[str], list[str]]
    extract_probable_addresses: Callable[[str], list[str]]
    normalize_phone_values: Callable[[Any], list[str]]
    normalize_address_values: Callable[[Any], list[str]]
    normalize_phone_candidate: Callable[[str], str]
    company_tokens: Callable[[str | None], set[str]]
    normalized_phone_digits: Callable[[str], str]
    guess_registered_domain: Callable[[str], str]
    address_identity_tokens: Callable[[str], dict[str, set[str]]]
    is_valid_russian_inn: Callable[[str], bool]
    keyword_found_in_text: Callable[[str, str], bool]
    compact_text: Callable[..., str]
    summarize_source_context: Callable[[dict[str, Any]], dict[str, Any]]
    looks_like_bot_gate: Callable[[requests.Response, str], bool]
    contact_path_hints: list[str] | tuple[str, ...]
    contact_link_text_hints: list[str] | tuple[str, ...]
    industrial_positive_keywords: dict[str, float | int]
    industrial_negative_keywords: dict[str, float | int]
    generic_email_domains: set[str]
    company_token_stopwords: set[str]
    activity_token_stopwords: set[str]
    non_corporate_domains: set[str]


@dataclass
class SiteDecision:
    url: str
    final_url: str = ""
    status: str = ""
    identity_score: float = 0.0
    viability_score: float = 0.0
    industrial_score: float = 0.0
    authenticity_score: float = 0.0
    conflict_penalty: float = 0.0
    belongs_to_company: bool = False
    decision_status: str = "rejected"
    industrial_relevance: str = "unknown"
    decision_source: str = "heuristics"
    reasons: list[str] = field(default_factory=list)
    evidence: list[str] = field(default_factory=list)
    hard_negative_hits: list[str] = field(default_factory=list)
    fetched_pages: list[str] = field(default_factory=list)
    title: str = ""
    description: str = ""
    extracted_phones: list[str] = field(default_factory=list)
    extracted_emails: list[str] = field(default_factory=list)
    extracted_addresses: list[str] = field(default_factory=list)
    matched_name_tokens: list[str] = field(default_factory=list)
    matched_keywords: list[str] = field(default_factory=list)
    negative_keywords: list[str] = field(default_factory=list)
    llm_result: dict[str, Any] | None = None
    errors: list[str] = field(default_factory=list)


def should_use_llm_review(
    decision_status: str,
    authenticity_score: float,
    hard_negative_hits: list[str],
    identity_flags: dict[str, Any],
) -> bool:
    if decision_status not in {"candidate", "suspicious"}:
        return False
    if authenticity_score >= 0.82:
        return False
    if hard_negative_hits and not identity_flags.get("inn_match") and not identity_flags.get("domain_matches_email"):
        return False
    if identity_flags.get("inn_match") and authenticity_score >= 0.62:
        return False
    return True


class SiteAuthenticityAnalyzer:
    def __init__(self, client: Any, llm: Any, helpers: SiteAuthHelpers) -> None:
        self.client = client
        self.llm = llm
        self.h = helpers

    def analyze_surface(
        self,
        row: Any,
        site_url: str,
        known_contacts: dict[str, list[str]],
        source_results: dict[str, Any],
    ) -> SiteDecision:
        decision, _, identity, _ = self._evaluate_site(
            row,
            site_url,
            known_contacts,
            source_results,
            allow_extra_pages=False,
        )
        decision.decision_source = "cheap_preparse_gate"
        decision.belongs_to_company = False

        if decision.status != "success":
            decision.decision_status = "rejected" if decision.status == "invalid_url" else "suspicious"
            gate_reason = (
                "cheap trust gate rejected candidate before deep parse"
                if decision.decision_status == "rejected"
                else "cheap trust gate kept candidate surface-only before deep parse"
            )
            if gate_reason not in decision.reasons:
                decision.reasons.append(gate_reason)
            return decision

        identity_flags = identity.get("flags") or {}
        decision.decision_status = self._derive_preparse_decision_status(
            decision.authenticity_score,
            decision.identity_score,
            decision.viability_score,
            identity_flags,
            decision.hard_negative_hits,
            decision.extracted_phones,
            decision.extracted_emails,
        )
        gate_reason = {
            "candidate": "cheap trust gate allowed deep parse",
            "suspicious": "cheap trust gate kept candidate surface-only before deep parse",
            "rejected": "cheap trust gate rejected candidate before deep parse",
        }.get(decision.decision_status, "cheap trust gate applied")
        if gate_reason not in decision.reasons:
            decision.reasons.append(gate_reason)
        return decision

    def analyze(
        self,
        row: Any,
        site_url: str,
        known_contacts: dict[str, list[str]],
        source_results: dict[str, Any],
    ) -> SiteDecision:
        decision, combined_text, identity, industrial = self._evaluate_site(
            row,
            site_url,
            known_contacts,
            source_results,
            allow_extra_pages=True,
        )
        if decision.status != "success":
            return decision

        identity_flags = identity.get("flags") or {}
        decision.decision_status = self._derive_site_decision_status(
            decision.authenticity_score,
            decision.identity_score,
            identity_flags,
            decision.hard_negative_hits,
        )
        decision.belongs_to_company = decision.decision_status == "verified"

        if should_use_llm_review(
            decision.decision_status,
            decision.authenticity_score,
            decision.hard_negative_hits,
            identity_flags,
        ):
            llm_context = self._build_llm_context(
                row=row,
                decision=decision,
                source_results=source_results,
                known_contacts=known_contacts,
                combined_text=combined_text,
                identity=identity,
                industrial=industrial,
            )
            llm_result = self.llm.decide(row, decision.final_url or self.h.normalize_url(site_url), llm_context)
            if llm_result:
                decision.llm_result = llm_result
                decision.decision_source = "llm_assisted"
                llm_confidence = float(llm_result.get("confidence", 0.0) or 0.0)
                if llm_confidence >= 0.55:
                    llm_belongs = bool(llm_result.get("belongs_to_company", decision.belongs_to_company))
                    if llm_belongs:
                        if decision.decision_status == "suspicious":
                            decision.decision_status = "candidate"
                        if decision.authenticity_score >= 0.62:
                            decision.decision_status = "verified"
                    else:
                        decision.decision_status = "suspicious" if decision.authenticity_score >= 0.5 else "rejected"
                    decision.belongs_to_company = decision.decision_status == "verified"
                    decision.industrial_relevance = llm_result.get("industrial_relevance", decision.industrial_relevance)
                reason = self.h.normalize_whitespace(str(llm_result.get("reason", "")))
                if reason:
                    decision.reasons.append(f"LLM: {reason}")

        return decision

    def _evaluate_site(
        self,
        row: Any,
        site_url: str,
        known_contacts: dict[str, list[str]],
        source_results: dict[str, Any],
        *,
        allow_extra_pages: bool,
    ) -> tuple[SiteDecision, str, dict[str, Any], dict[str, Any]]:
        decision = SiteDecision(url=site_url, status="pending")
        normalized_url = self.h.normalize_url(site_url)
        if not normalized_url:
            decision.status = "invalid_url"
            decision.errors.append("invalid site url")
            return decision, "", {}, {}

        first = self.client.request(normalized_url, source="company_site", timeout=20)
        if not first.ok or not first.response:
            decision.status = first.status
            decision.errors.append(first.error)
            return decision, "", {}, {}

        page = first.response
        page, fallback_note = self._fallback_to_homepage_if_soft_404(page)
        if fallback_note:
            decision.reasons.append(fallback_note)

        decision.final_url = page.url
        decision.fetched_pages.append(page.url)
        soup = BeautifulSoup(page.text, "html.parser")
        meta = self.h.parse_title_and_meta(soup)
        decision.title = meta["title"]
        decision.description = meta["description"]

        combined_text = self.h.normalize_whitespace(soup.get_text(" ", strip=True))
        decision.hard_negative_hits = self._detect_hard_negative_signals(page, combined_text)
        contacts = self._extract_site_contacts(page.url, soup, combined_text)
        decision.extracted_phones = contacts["phones"]
        decision.extracted_emails = contacts["emails"]
        decision.extracted_addresses = contacts["addresses"]

        if allow_extra_pages:
            extra_links = self._discover_extra_pages(page.url, soup)
            for extra_link in extra_links[:2]:
                extra = self.client.request(extra_link, source="company_site", timeout=20)
                if not extra.ok or not extra.response:
                    continue
                extra_response = extra.response
                decision.fetched_pages.append(extra_response.url)
                extra_soup = BeautifulSoup(extra_response.text, "html.parser")
                extra_text = self.h.normalize_whitespace(extra_soup.get_text(" ", strip=True))
                combined_text = self.h.normalize_whitespace(combined_text + " " + extra_text)
                extracted = self._extract_site_contacts(extra_response.url, extra_soup, extra_text)
                decision.extracted_phones = self.h.dedupe_preserve_order(decision.extracted_phones + extracted["phones"])
                decision.extracted_emails = self.h.dedupe_preserve_order(decision.extracted_emails + extracted["emails"])
                decision.extracted_addresses = self.h.dedupe_preserve_order(decision.extracted_addresses + extracted["addresses"])

        identity = self._identity_score(row, decision, combined_text, known_contacts)
        activity_profile = self._build_activity_profile(row, source_results)
        industrial = self._industrial_score(combined_text, activity_profile)
        viability = self._viability_score(page, soup, combined_text, decision)
        conflicts = self._conflict_penalty(row, decision, combined_text, known_contacts, identity.get("flags") or {})

        decision.identity_score = round(identity["score"], 3)
        decision.industrial_score = round(industrial["score"], 3)
        decision.viability_score = round(viability["score"], 3)
        decision.conflict_penalty = round(conflicts["penalty"], 3)
        decision.matched_name_tokens = sorted(identity["matched_tokens"])
        decision.matched_keywords = industrial["positive_hits"]
        decision.negative_keywords = industrial["negative_hits"]
        decision.reasons.extend(identity["reasons"])
        decision.reasons.extend(viability["reasons"])
        decision.reasons.extend(industrial["reasons"])
        decision.reasons.extend(conflicts["reasons"])

        raw_authenticity = (
            decision.identity_score * 0.58
            + decision.viability_score * 0.22
            + decision.industrial_score * 0.2
            - decision.conflict_penalty
        )
        decision.authenticity_score = round(max(0.0, min(raw_authenticity, 1.0)), 3)
        decision.industrial_relevance = (
            "high"
            if decision.industrial_score >= 0.75
            else "medium"
            if decision.industrial_score >= 0.45
            else "low"
            if decision.industrial_score >= 0.2
            else "none"
        )
        decision.evidence = self.h.dedupe_preserve_order(
            [*identity.get("evidence", []), *viability.get("evidence", []), *industrial.get("evidence", [])]
        )[:8]
        if decision.hard_negative_hits:
            decision.reasons.append("hard negatives: " + ", ".join(decision.hard_negative_hits[:4]))

        decision.status = "success"
        return decision, combined_text, identity, industrial

    def _build_llm_context(
        self,
        *,
        row: Any,
        decision: SiteDecision,
        source_results: dict[str, Any],
        known_contacts: dict[str, list[str]],
        combined_text: str,
        identity: dict[str, Any],
        industrial: dict[str, Any],
    ) -> dict[str, Any]:
        identity_flags = identity.get("flags") or {}
        source_context = self.h.summarize_source_context(source_results)
        return {
            "xlsx_hint": {
                "input_site": row.xlsx_site,
                "input_phone": row.xlsx_phone,
                "comment": self.h.compact_text(row.comment, 220),
            },
            "aggregator_profile": source_context,
            "known_contacts": {
                "phones": known_contacts.get("phones", [])[:5],
                "emails": known_contacts.get("emails", [])[:5],
                "websites": known_contacts.get("websites", [])[:5],
                "addresses": [self.h.compact_text(item, 140) for item in known_contacts.get("addresses", [])[:3]],
            },
            "candidate_site": {
                "url": decision.url,
                "final_url": decision.final_url,
                "title": decision.title,
                "description": decision.description,
                "phones": decision.extracted_phones[:5],
                "emails": decision.extracted_emails[:5],
                "addresses": [self.h.compact_text(item, 140) for item in decision.extracted_addresses[:3]],
                "fetched_pages": decision.fetched_pages[:4],
                "text_excerpt": self.h.compact_text(combined_text, 2600),
            },
            "heuristics": {
                "decision_status": decision.decision_status,
                "authenticity_score": round(decision.authenticity_score, 3),
                "identity_score": round(decision.identity_score, 3),
                "viability_score": round(decision.viability_score, 3),
                "industrial_score": round(decision.industrial_score, 3),
                "conflict_penalty": round(decision.conflict_penalty, 3),
                "hard_negative_hits": decision.hard_negative_hits[:5],
                "matched_name_tokens": decision.matched_name_tokens[:8],
                "positive_keywords": decision.matched_keywords[:12],
                "negative_keywords": decision.negative_keywords[:8],
                "flags": identity_flags,
                "identity_reasons": [self.h.compact_text(item, 140) for item in identity.get("reasons", [])[:6]],
                "industrial_reasons": [self.h.compact_text(item, 140) for item in industrial.get("reasons", [])[:6]],
            },
            "business_goal": (
                "Need a trustworthy corporate site for this specific company. "
                "If this is just a similar industry site, catalog, reseller, marketplace, or unrelated brand, reject it."
            ),
        }

    def _discover_extra_pages(self, base_url: str, soup: BeautifulSoup) -> list[str]:
        base_host = self.h.guess_registered_domain(urlparse(base_url).netloc)
        candidates: list[str] = []
        for anchor in soup.select("a[href]"):
            href = self.h.normalize_whitespace(anchor.get("href", ""))
            text = self.h.normalize_whitespace(anchor.get_text(" ", strip=True)).lower()
            if not href or href.startswith("mailto:") or href.startswith("tel:"):
                continue
            full = self.h.normalize_url(urljoin(base_url, href))
            if not full:
                continue
            if self.h.guess_registered_domain(urlparse(full).netloc) != base_host:
                continue
            low = full.lower()
            if any(hint in low for hint in self.h.contact_path_hints) or any(
                hint in text for hint in self.h.contact_link_text_hints
            ):
                candidates.append(full)
        return self.h.dedupe_preserve_order(candidates)

    def _extract_site_contacts(self, page_url: str, soup: BeautifulSoup, text: str) -> dict[str, list[str]]:
        phones = self.h.extract_phones(text)
        emails = self.h.extract_emails(text)
        addresses: list[str] = []
        for anchor in soup.select("a[href]"):
            href = self.h.normalize_whitespace(anchor.get("href", ""))
            if href.startswith("tel:"):
                normalized_phone = self.h.normalize_phone_candidate(href.replace("tel:", "", 1))
                if not normalized_phone:
                    normalized_phone = self.h.normalize_phone_candidate(anchor.get_text(" ", strip=True))
                if normalized_phone:
                    phones.append(normalized_phone)
            elif href.startswith("mailto:"):
                emails.append(href.replace("mailto:", "").strip())
        footer = soup.find("footer")
        if footer:
            footer_text = self.h.normalize_whitespace(footer.get_text(" ", strip=True))
            addresses.extend(self.h.extract_probable_addresses(footer_text))
        addresses.extend(self.h.extract_probable_addresses(text)[:3])
        return {
            "phones": self.h.normalize_phone_values(phones),
            "emails": self.h.dedupe_preserve_order(emails),
            "addresses": self.h.normalize_address_values(addresses),
        }

    def _fallback_to_homepage_if_soft_404(self, page: requests.Response) -> tuple[requests.Response, str]:
        path = urlparse(page.url).path or "/"
        is_soft_404 = self._looks_soft_404(page.status_code, page.text or "")
        if not is_soft_404 or path in {"", "/"}:
            return page, ""
        parsed = urlparse(page.url)
        homepage = f"{parsed.scheme}://{parsed.netloc}/"
        fallback = self.client.request(homepage, source="company_site", timeout=20)
        if not fallback.ok or not fallback.response:
            return page, ""
        if self._looks_soft_404(fallback.response.status_code, fallback.response.text or ""):
            return page, ""
        note = f"candidate path looked like 404, switched to homepage: {homepage}"
        return fallback.response, note

    def _looks_soft_404(self, status_code: int, text: str) -> bool:
        combined = self.h.normalize_whitespace(text).lower()[:8000]
        if status_code >= 400:
            return True
        return any(pattern in combined for pattern in SOFT_404_PATTERNS)

    def _detect_hard_negative_signals(self, page: requests.Response, text: str) -> list[str]:
        combined = f"{page.url}\n{self.h.normalize_whitespace(text)}".lower()[:12000]
        hits: list[str] = []
        if page.status_code >= 400:
            hits.append(f"http_{page.status_code}")
        if any(pattern in combined for pattern in SOFT_404_PATTERNS):
            hits.append("soft_404")
        if any(pattern in combined for pattern in DOMAIN_PARKING_PATTERNS):
            hits.append("domain_parking")
        if any(pattern in combined for pattern in DIRECTORY_PORTAL_PATTERNS):
            hits.append("directory_portal")
        if self.h.looks_like_bot_gate(page, text):
            hits.append("anti_bot_gate")
        domain = self.h.guess_registered_domain(urlparse(page.url).netloc)
        if domain and domain in self.h.non_corporate_domains:
            hits.append("non_corporate_domain")
        return self.h.dedupe_preserve_order(hits)

    def _build_activity_profile(self, row: Any, source_results: dict[str, Any]) -> dict[str, Any]:
        raw_parts = [row.company_name]
        for source in source_results.values():
            raw_parts.extend(source.snippets[:4])
            raw_parts.extend(source.notes[:3])
        raw = self.h.normalize_whitespace(" ".join(raw_parts))
        okved_codes = sorted(set(re.findall(r"\b\d{2}\.\d{2}\b", raw)))
        tokens = re.findall(r"[a-zа-яё]{4,}", raw.lower(), flags=re.IGNORECASE)
        filtered = [
            token
            for token in tokens
            if token not in self.h.company_token_stopwords
            and token not in self.h.activity_token_stopwords
            and token not in SURPLUS_ACTIVITY_TERMS
            and not token.isdigit()
        ]
        normalized_raw = _normalize_activity_fragment(raw)
        company_name_tokens = {token.lower().replace("ё", "е") for token in self.h.company_tokens(row.company_name)}
        matched_phrase_stems = _matched_surplus_phrase_stems(normalized_raw)
        filtered = [
            token
            for token in filtered
            if not _is_surplus_activity_token(
                token,
                phrase_stems=matched_phrase_stems,
                company_name_tokens=company_name_tokens,
            )
        ]
        normalized_tokens = ACTIVITY_PROFILE_TOKEN_RE.findall(normalized_raw)
        filtered = [
            token
            for token in normalized_tokens
            if token not in self.h.company_token_stopwords
            and token not in self.h.activity_token_stopwords
            and not token.isdigit()
            and not _is_surplus_activity_token(
                token,
                phrase_stems=matched_phrase_stems,
                company_name_tokens=company_name_tokens,
            )
        ]
        token_counts = Counter(filtered)
        terms = [token for token, _ in token_counts.most_common(18)]
        return {"okved_codes": okved_codes, "terms": terms, "raw": self.h.compact_text(raw, 800)}

    def _identity_score(
        self,
        row: Any,
        decision: SiteDecision,
        text: str,
        known_contacts: dict[str, list[str]],
    ) -> dict[str, Any]:
        score = 0.0
        reasons: list[str] = []
        evidence: list[str] = []
        matched_tokens: set[str] = set()
        flags = {
            "address_overlap": False,
            "domain_matches_email": False,
            "domain_matches_known_website": False,
            "domain_matches_input_site": False,
            "email_overlap": False,
            "inn_match": False,
            "name_tokens_found": False,
            "phone_overlap": False,
            "title_match": False,
        }
        row_tokens = self.h.company_tokens(row.company_name)
        lower_text = text.lower()
        for token in row_tokens:
            if token in lower_text:
                matched_tokens.add(token)
        if row_tokens:
            token_ratio = len(matched_tokens) / max(len(row_tokens), 1)
            token_score = min(token_ratio * 0.38, 0.38)
            score += token_score
            if matched_tokens:
                flags["name_tokens_found"] = True
                reasons.append(f"name token overlap: {', '.join(sorted(matched_tokens)[:8])}")
                evidence.append("name tokens")

        if row.inn and row.inn in lower_text:
            flags["inn_match"] = True
            score += 0.46
            reasons.append("company INN found on site")
            evidence.append("inn_match")

        known_phone_digits = {self.h.normalized_phone_digits(item) for item in known_contacts.get("phones", [])}
        site_phone_digits = {self.h.normalized_phone_digits(item) for item in decision.extracted_phones}
        phone_overlap = {item for item in site_phone_digits if item and item in known_phone_digits}
        if phone_overlap:
            flags["phone_overlap"] = True
            score += 0.22
            reasons.append("phone overlap with aggregator contacts")
            evidence.append("phone_overlap")

        known_emails = {
            self.h.normalize_whitespace(item).lower()
            for item in known_contacts.get("emails", [])
            if self.h.normalize_whitespace(item)
        }
        site_emails = {
            self.h.normalize_whitespace(item).lower()
            for item in decision.extracted_emails
            if self.h.normalize_whitespace(item)
        }
        email_overlap = known_emails.intersection(site_emails)
        if email_overlap:
            flags["email_overlap"] = True
            score += 0.2
            reasons.append("email overlap with aggregator contacts")
            evidence.append("email_overlap")

        known_domains = {
            self.h.guess_registered_domain(urlparse(self.h.normalize_url(item)).netloc)
            for item in known_contacts.get("websites", [])
            if self.h.normalize_url(item)
        }
        site_domain = self.h.guess_registered_domain(urlparse(self.h.normalize_url(decision.final_url or decision.url)).netloc)
        known_email_domains = {
            self.h.guess_registered_domain(item.split("@", 1)[-1].lower())
            for item in known_contacts.get("emails", [])
            if "@" in item and item.split("@", 1)[-1].lower() not in self.h.generic_email_domains
        }
        input_site_domain = self.h.guess_registered_domain(urlparse(self.h.normalize_url(row.xlsx_site)).netloc) if row.xlsx_site else ""
        if site_domain and site_domain in known_email_domains:
            flags["domain_matches_email"] = True
            score += 0.32
            reasons.append("site domain matches corporate email domain")
            evidence.append("domain_email_match")
        if site_domain and site_domain in known_domains:
            flags["domain_matches_known_website"] = True
            score += 0.22
            reasons.append("site domain matches aggregator website domain")
            evidence.append("domain_known_match")
        if site_domain and input_site_domain and site_domain == input_site_domain:
            flags["domain_matches_input_site"] = True
            score += 0.3
            reasons.append("site domain matches input spreadsheet site")
            evidence.append("domain_input_match")
        if site_domain and flags["domain_matches_email"] and flags["domain_matches_known_website"]:
            score += 0.08
            reasons.append("domain is confirmed by both website and corporate email")

        known_address_postals: set[str] = set()
        known_address_tokens: set[str] = set()
        for address in known_contacts.get("addresses", []):
            parsed = self.h.address_identity_tokens(address)
            known_address_postals.update(parsed["postals"])
            known_address_tokens.update(parsed["tokens"])
        if known_address_postals or known_address_tokens:
            for address in decision.extracted_addresses:
                parsed = self.h.address_identity_tokens(address)
                postal_overlap = known_address_postals.intersection(parsed["postals"])
                token_overlap = known_address_tokens.intersection(parsed["tokens"])
                if (postal_overlap and token_overlap) or len(token_overlap) >= 2:
                    flags["address_overlap"] = True
                    score += 0.14
                    reasons.append("address overlap with aggregator profile")
                    evidence.append("address_overlap")
                    break

        title = f"{decision.title} {decision.description}".lower()
        if any(token in title for token in row_tokens):
            flags["title_match"] = True
            score += 0.12
            reasons.append("company name appears in title/description")
            evidence.append("title_match")

        return {
            "score": min(score, 1.0),
            "reasons": reasons,
            "evidence": evidence,
            "matched_tokens": matched_tokens,
            "flags": flags,
        }

    def _industrial_score(self, text: str, activity_profile: dict[str, Any] | None = None) -> dict[str, Any]:
        lower = text.lower()
        positive_hits: list[str] = []
        negative_hits: list[str] = []
        surplus_hits = [keyword for keyword in surplus_only_keywords if self.h.keyword_found_in_text(lower, keyword)]
        industrial_score = 0.0
        for keyword, weight in self.h.industrial_positive_keywords.items():
            if self.h.keyword_found_in_text(lower, keyword):
                positive_hits.append(keyword)
                industrial_score += 0.06 * weight
        for keyword, weight in self.h.industrial_negative_keywords.items():
            if self.h.keyword_found_in_text(lower, keyword):
                negative_hits.append(keyword)
                industrial_score -= 0.08 * weight
        industrial_score = max(0.0, min(industrial_score, 1.0))

        profile_score = 0.0
        profile_reasons: list[str] = []
        evidence: list[str] = []
        if activity_profile:
            terms = activity_profile.get("terms") or []
            okved_codes = activity_profile.get("okved_codes") or []
            matched_terms = [term for term in terms if self.h.keyword_found_in_text(lower, term)]
            if terms:
                ratio = len(matched_terms) / max(min(len(terms), 12), 1)
                profile_score += min(0.6, ratio * 0.85)
                if matched_terms:
                    evidence.append("activity terms: " + ", ".join(matched_terms[:6]))
            matched_okved = [code for code in okved_codes if code in lower]
            if matched_okved:
                profile_score += 0.35
                evidence.append("okved codes: " + ", ".join(matched_okved[:3]))
            if matched_terms:
                profile_reasons.append("activity profile overlaps with site content")
            elif terms:
                profile_reasons.append("weak activity-profile overlap")
            profile_score = max(0.0, min(profile_score, 1.0))

        score = industrial_score if not activity_profile else (industrial_score * 0.45 + profile_score * 0.55)
        score = max(0.0, min(score, 1.0))
        reasons: list[str] = []
        if positive_hits:
            reasons.append(f"industrial markers: {', '.join(positive_hits[:8])}")
            evidence.append("industrial keywords: " + ", ".join(positive_hits[:6]))
        if negative_hits:
            reasons.append(f"negative markers: {', '.join(negative_hits[:6])}")
        if surplus_hits:
            reasons.append(f"surplus markers kept separate: {', '.join(surplus_hits[:6])}")
            evidence.append("surplus keywords: " + ", ".join(surplus_hits[:6]))
        reasons.extend(profile_reasons)
        return {
            "score": score,
            "reasons": reasons,
            "positive_hits": positive_hits,
            "negative_hits": negative_hits,
            "evidence": self.h.dedupe_preserve_order(evidence)[:5],
        }

    def _viability_score(
        self,
        page: requests.Response,
        soup: BeautifulSoup,
        text: str,
        decision: SiteDecision,
    ) -> dict[str, Any]:
        score = 0.0
        reasons: list[str] = []
        evidence: list[str] = []
        if 200 <= page.status_code < 400:
            score += 0.2
            evidence.append(f"http_status={page.status_code}")
        text_len = len(text)
        if text_len >= 1400:
            score += 0.25
            evidence.append(f"text_length={text_len}")
        elif text_len >= 500:
            score += 0.16
        elif text_len >= 220:
            score += 0.1
        link_count = len(soup.select("a[href]"))
        if link_count >= 10:
            score += 0.2
            evidence.append(f"link_count={link_count}")
        elif link_count >= 4:
            score += 0.12
        if decision.extracted_phones or decision.extracted_emails or decision.extracted_addresses:
            score += 0.2
            evidence.append("contacts_detected")
        if decision.title or decision.description:
            score += 0.1
        if any(hit in {"soft_404", "domain_parking"} for hit in decision.hard_negative_hits):
            score -= 0.45
            reasons.append("soft404 or parking pattern detected")
        if "anti_bot_gate" in decision.hard_negative_hits:
            score -= 0.2
            reasons.append("anti-bot gate detected on candidate page")
        return {"score": max(0.0, min(score, 1.0)), "reasons": reasons, "evidence": evidence[:5]}

    def _conflict_penalty(
        self,
        row: Any,
        decision: SiteDecision,
        text: str,
        known_contacts: dict[str, list[str]],
        identity_flags: dict[str, Any],
    ) -> dict[str, Any]:
        penalty = 0.0
        reasons: list[str] = []
        if decision.hard_negative_hits:
            penalty += min(0.32, 0.11 * len(decision.hard_negative_hits))
            reasons.append("hard-negative penalty applied")

        inn_candidates = {item for item in re.findall(r"\b\d{10}\b", text) if self.h.is_valid_russian_inn(item)}
        if row.inn and inn_candidates and row.inn not in inn_candidates:
            penalty += 0.22
            reasons.append("site contains different legal INN markers")

        has_known_phones = bool(known_contacts.get("phones"))
        has_known_emails = bool(known_contacts.get("emails"))
        if has_known_phones and not identity_flags.get("phone_overlap") and decision.extracted_phones:
            penalty += 0.08
            reasons.append("site phones do not overlap known company phones")
        if has_known_emails and not identity_flags.get("email_overlap") and decision.extracted_emails:
            penalty += 0.08
            reasons.append("site emails do not overlap known company emails")
        if not identity_flags.get("name_tokens_found") and not identity_flags.get("inn_match"):
            penalty += 0.08
            reasons.append("no legal identity markers detected on site")

        return {"penalty": max(0.0, min(penalty, 0.65)), "reasons": reasons}

    def _derive_site_decision_status(
        self,
        authenticity_score: float,
        identity_score: float,
        identity_flags: dict[str, Any],
        hard_negative_hits: list[str],
    ) -> str:
        if hard_negative_hits:
            has_strong_identity = bool(identity_flags.get("inn_match") or identity_flags.get("domain_matches_email"))
            if has_strong_identity and authenticity_score >= 0.45:
                return "suspicious"
            return "rejected"
        if authenticity_score >= 0.75 and identity_score >= 0.55:
            return "verified"
        if authenticity_score >= 0.52:
            return "candidate"
        if authenticity_score >= 0.3:
            return "suspicious"
        return "rejected"

    def _derive_preparse_decision_status(
        self,
        authenticity_score: float,
        identity_score: float,
        viability_score: float,
        identity_flags: dict[str, Any],
        hard_negative_hits: list[str],
        extracted_phones: list[str],
        extracted_emails: list[str],
    ) -> str:
        strong_identity = bool(
            identity_flags.get("inn_match")
            or identity_flags.get("domain_matches_email")
            or identity_flags.get("domain_matches_known_website")
            or identity_flags.get("domain_matches_input_site")
        )
        medium_identity = bool(
            identity_flags.get("phone_overlap")
            or identity_flags.get("email_overlap")
            or identity_flags.get("address_overlap")
            or identity_flags.get("title_match")
        )
        has_contacts = bool(extracted_phones or extracted_emails)

        if hard_negative_hits:
            if strong_identity and authenticity_score >= 0.42:
                return "suspicious"
            return "rejected"
        if strong_identity and authenticity_score >= 0.42 and viability_score >= 0.16:
            return "candidate"
        if strong_identity and medium_identity and authenticity_score >= 0.34 and viability_score >= 0.12:
            return "candidate"
        if identity_score >= 0.52 and authenticity_score >= 0.5 and has_contacts:
            return "candidate"
        if authenticity_score <= 0.18 and identity_score < 0.12 and not has_contacts:
            return "rejected"
        return "suspicious"

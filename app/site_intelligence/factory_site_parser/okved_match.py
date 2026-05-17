from __future__ import annotations

import re
from collections import Counter
from dataclasses import dataclass
from urllib.parse import urlparse

from app.site_intelligence.common import compact_text, dedupe_preserve_order, normalize_whitespace
from app.site_intelligence.models import ContentRecord

from .models import (
    FactorySiteOkvedEvidence,
    FactorySiteOkvedMatch,
    FactorySiteOkvedPageSignal,
    FactorySiteOkvedProfile,
    FactorySiteOkvedSiteMatch,
    FactorySiteParserCompany,
)

TOKEN_PATTERN = re.compile(r"[a-zа-яё0-9-]{4,}", flags=re.IGNORECASE)
OKVED_PATTERN = re.compile(r"\b\d{2}\.\d{2}\b")

POSITIVE_SIGNAL_GROUPS = (
    "industrial_identity_positive",
    "product_or_process_positive",
    "okved_code_positive",
    "generic_corporate_weak",
)
NEGATIVE_SIGNAL_GROUPS = (
    "dealer_negative",
    "portal_catalog_negative",
    "marketplace_aggregator_negative",
    "reseller_negative",
    "unrelated_service_negative",
)
ALL_SIGNAL_GROUPS = POSITIVE_SIGNAL_GROUPS + NEGATIVE_SIGNAL_GROUPS

GROUP_CAPS: dict[str, float] = {
    "industrial_identity_positive": 0.35,
    "product_or_process_positive": 0.45,
    "okved_code_positive": 0.35,
    "generic_corporate_weak": 0.15,
    "dealer_negative": 0.55,
    "portal_catalog_negative": 0.65,
    "marketplace_aggregator_negative": 0.7,
    "reseller_negative": 0.55,
    "unrelated_service_negative": 0.8,
}

GROUP_LABELS = {
    "industrial_identity_positive": "industrial identity",
    "product_or_process_positive": "product/process context",
    "okved_code_positive": "OKVED code",
    "generic_corporate_weak": "generic corporate page",
    "dealer_negative": "dealer signal",
    "portal_catalog_negative": "portal/catalog signal",
    "marketplace_aggregator_negative": "marketplace/aggregator signal",
    "reseller_negative": "reseller signal",
    "unrelated_service_negative": "unrelated service signal",
}

STOPWORDS = {
    "company",
    "group",
    "holding",
    "industrial",
    "engineering",
    "manufacturing",
    "официальный",
    "завод",
    "компания",
    "комбинат",
    "предприятие",
    "производство",
    "производственный",
    "производственная",
    "промышленный",
    "общество",
    "ограниченной",
    "ответственностью",
    "акционерное",
    "публичное",
    "товарищество",
    "заводские",
}

GENERIC_PROFILE_TERMS = {
    "factory",
    "plant",
    "company",
    "industrial",
    "завод",
    "предприятие",
    "производство",
    "продукция",
    "оборудование",
    "изделия",
    "услуги",
    "услуга",
    "промышленность",
    "металл",
    "сталь",
    "компания",
    "партнер",
    "решения",
}

PROFILE_NOISE_TERMS = GENERIC_PROFILE_TERMS | {
    "официальный",
    "дилер",
    "каталог",
    "портал",
    "маркетплейс",
    "агрегатор",
    "база",
    "поставщик",
    "сервис",
    "консалтинг",
    "юридический",
    "бухгалтерский",
}

INDUSTRIAL_POSITIVE_PATTERNS = (
    ("собственное производство", 0.22, "page claims in-house manufacturing"),
    ("собственный завод", 0.2, "page claims its own plant"),
    ("производственный комплекс", 0.18, "page describes a production complex"),
    ("производственная площадка", 0.18, "page describes a production site"),
    ("производственный цех", 0.16, "page mentions a production workshop"),
    ("завод", 0.14, "page explicitly uses plant/factory language"),
    ("фабрик", 0.12, "page explicitly uses factory language"),
    ("комбинат", 0.12, "page explicitly uses plant language"),
    ("производител", 0.12, "page positions the company as a manufacturer"),
    ("изготовител", 0.12, "page positions the company as a fabricator"),
    ("цех", 0.1, "page mentions workshops or production areas"),
    ("промышленн", 0.08, "page has an industrial context"),
)

PRODUCT_PROCESS_PATTERNS = (
    ("продукц", 0.08, "page describes products"),
    ("изготавлива", 0.14, "page describes manufacturing work"),
    ("выпуска", 0.12, "page describes output or product release"),
    ("серийн", 0.08, "page mentions serial production"),
    ("сварк", 0.1, "page contains a process keyword"),
    ("резк", 0.08, "page contains a process keyword"),
    ("обработк", 0.08, "page contains a process keyword"),
    ("лить", 0.08, "page contains a process keyword"),
    ("гост", 0.1, "page references technical standards"),
    ("техническ", 0.08, "page references technical specifications"),
    ("мощност", 0.1, "page describes production capacity"),
    ("технологическ", 0.08, "page describes a production process"),
    ("чертеж", 0.08, "page mentions production by drawing/specification"),
    ("издели", 0.08, "page describes output or product types"),
)

GENERIC_CORPORATE_PATTERNS = (
    ("о компании", 0.06, "page looks like a corporate/about section"),
    ("история компании", 0.05, "page looks like a corporate/about section"),
    ("миссия", 0.04, "page looks like a generic corporate page"),
    ("контакты", 0.04, "page looks like a generic corporate page"),
    ("about us", 0.04, "page looks like a corporate/about section"),
    ("company profile", 0.05, "page looks like a generic corporate page"),
)

DEALER_NEGATIVE_PATTERNS = (
    ("официальный дилер", 0.28, "page explicitly says the business is a dealer"),
    ("дилер", 0.22, "page has a dealer signal"),
    ("дистрибьют", 0.22, "page has a distributor signal"),
    ("официальный партнер", 0.18, "page has a dealer/partner signal"),
    ("каталог брендов", 0.18, "page focuses on brands instead of own production"),
    ("наши бренды", 0.14, "page focuses on brands instead of own production"),
    ("продукцию ведущих производителей", 0.24, "page resells products from external manufacturers"),
)

PORTAL_CATALOG_NEGATIVE_PATTERNS = (
    ("каталог предприятий", 0.34, "page looks like a company catalog"),
    ("каталог компаний", 0.3, "page looks like a company catalog"),
    ("база поставщиков", 0.32, "page looks like a supplier portal"),
    ("карточка компании", 0.28, "page looks like a company card in a portal"),
    ("список компаний", 0.26, "page looks like a directory"),
    ("реестр предприятий", 0.3, "page looks like a directory"),
    ("поиск поставщиков", 0.24, "page looks like a supplier portal"),
    ("поставщики россии", 0.22, "page aggregates suppliers"),
)

MARKETPLACE_AGGREGATOR_NEGATIVE_PATTERNS = (
    ("маркетплейс", 0.38, "page looks like a marketplace"),
    ("агрегатор", 0.34, "page looks like an aggregator"),
    ("предложения поставщиков", 0.28, "page aggregates offers from suppliers"),
    ("товары от разных продавцов", 0.3, "page aggregates multiple sellers"),
    ("несколько продавцов", 0.26, "page aggregates multiple sellers"),
    ("объявления", 0.24, "page looks like an announcement board"),
)

RESELLER_NEGATIVE_PATTERNS = (
    ("торговый дом", 0.24, "page looks like a trading/reseller business"),
    ("комплексные поставки", 0.24, "page focuses on supplies/resale"),
    ("поставка оборудования", 0.2, "page focuses on supplies/resale"),
    ("официальный поставщик", 0.18, "page focuses on supplies/resale"),
    ("продажа оборудования", 0.16, "page focuses on resale"),
    ("складская программа", 0.18, "page focuses on stocked resale"),
    ("импорт", 0.14, "page focuses on external sourcing"),
)

UNRELATED_SERVICE_NEGATIVE_PATTERNS = (
    ("юридическ", 0.34, "page describes legal services"),
    ("бухгалтер", 0.32, "page describes accounting services"),
    ("консалт", 0.3, "page describes consulting services"),
    ("аудит", 0.28, "page describes audit services"),
    ("налогов", 0.26, "page describes tax services"),
    ("регистрация ооо", 0.32, "page describes business registration services"),
    ("кадровый аутсорсинг", 0.3, "page describes unrelated service work"),
    ("абонентское обслуживание", 0.18, "page describes service retainers"),
)

SECTION_HINTS: dict[str, tuple[str, float, str]] = {
    "about": ("generic_corporate_weak", 0.06, "section guess points to a corporate/about page"),
    "company": ("generic_corporate_weak", 0.05, "section guess points to a corporate/about page"),
    "contacts": ("generic_corporate_weak", 0.05, "section guess points to a generic contact page"),
    "production": ("industrial_identity_positive", 0.12, "section guess points to production"),
    "products": ("product_or_process_positive", 0.1, "section guess points to products"),
    "dealers": ("dealer_negative", 0.22, "section guess points to a dealer page"),
    "partners": ("dealer_negative", 0.16, "section guess points to a partner page"),
    "marketplace": ("marketplace_aggregator_negative", 0.24, "section guess points to a marketplace"),
    "services": ("unrelated_service_negative", 0.16, "section guess points to services"),
}

PATH_HINTS: dict[str, tuple[str, float, str]] = {
    "about": ("generic_corporate_weak", 0.05, "URL path looks like an about/company page"),
    "contacts": ("generic_corporate_weak", 0.04, "URL path looks like a contact page"),
    "production": ("industrial_identity_positive", 0.12, "URL path points to production"),
    "products": ("product_or_process_positive", 0.1, "URL path points to products"),
    "dealers": ("dealer_negative", 0.2, "URL path points to a dealer page"),
    "brands": ("dealer_negative", 0.16, "URL path points to a brands page"),
    "suppliers": ("portal_catalog_negative", 0.18, "URL path points to supplier listings"),
    "companies": ("portal_catalog_negative", 0.18, "URL path points to company listings"),
    "marketplace": ("marketplace_aggregator_negative", 0.24, "URL path points to a marketplace"),
}


def _normalize_text(value: str | None) -> str:
    return normalize_whitespace(value).lower().replace("ё", "е") if value else ""


def _tokenize(value: str | None) -> list[str]:
    return [token.strip("-") for token in TOKEN_PATTERN.findall(_normalize_text(value))]


def _term_key(token: str) -> str:
    normalized = _normalize_text(token).strip("-")
    if len(normalized) >= 12:
        return normalized[:10]
    if len(normalized) >= 9:
        return normalized[:8]
    if len(normalized) >= 6:
        return normalized[:6]
    return normalized


@dataclass(frozen=True)
class _ProfileTerm:
    term: str
    normalized_term: str
    keys: tuple[str, ...]
    generic: bool


@dataclass(frozen=True)
class _RecordContext:
    title: str
    content: str
    section: str
    url_path: str
    combined_text: str
    token_keys: frozenset[str]

    def find_source(self, fragment: str) -> str:
        needle = _normalize_text(fragment)
        if needle and needle in self.title:
            return "title"
        if needle and needle in self.section:
            return "section_guess"
        if needle and needle in self.url_path:
            return "url_path"
        if needle and needle in self.content:
            return "content"
        return "combined_text"


class FactorySiteOkvedMatcher:
    def __init__(self, *, max_terms: int = 12) -> None:
        self.max_terms = max(1, max_terms)

    def build_profile(self, company: FactorySiteParserCompany) -> FactorySiteOkvedProfile:
        raw_parts = [company.company_name]
        raw_parts.extend(company.source_snippets)
        raw_parts.extend(company.source_notes)
        raw_parts.extend(company.activity_terms)
        raw = normalize_whitespace(" ".join(raw_parts))
        okved_codes = dedupe_preserve_order(list(company.known_okved_codes) + OKVED_PATTERN.findall(raw))

        counted_tokens = Counter(
            token.lower()
            for token in _tokenize(raw)
            if token and token.lower() not in STOPWORDS and not token.isdigit()
        )

        extracted_terms: list[str] = []
        for term, count in counted_tokens.most_common(self.max_terms * 4):
            if term in PROFILE_NOISE_TERMS:
                continue
            if count < 2 and len(term) < 10:
                continue
            extracted_terms.append(term)

        explicit_terms = [normalize_whitespace(term) for term in company.activity_terms if normalize_whitespace(term)]
        terms = dedupe_preserve_order(explicit_terms + extracted_terms)
        return FactorySiteOkvedProfile(
            okved_codes=okved_codes[:8],
            terms=terms[: self.max_terms],
            raw=compact_text(raw, 800),
        )

    def match_records(
        self,
        company: FactorySiteParserCompany,
        records: list[ContentRecord],
    ) -> tuple[FactorySiteOkvedProfile, list[FactorySiteOkvedMatch]]:
        profile = self.build_profile(company)
        term_index = self._build_term_index(profile.terms)
        matches = [self._match_record(profile, term_index, record) for record in records]
        profile.site_match = self._aggregate_site_match(matches)
        return profile, matches

    def _build_term_index(self, terms: list[str]) -> list[_ProfileTerm]:
        result: list[_ProfileTerm] = []
        seen: set[str] = set()
        generic_keys = {_term_key(term) for term in GENERIC_PROFILE_TERMS}
        stopword_keys = {_term_key(stopword) for stopword in STOPWORDS}
        for term in terms:
            normalized_term = _normalize_text(term)
            if not normalized_term or normalized_term in seen:
                continue
            seen.add(normalized_term)
            keys = tuple(
                dedupe_preserve_order(
                    key
                    for key in (_term_key(token) for token in _tokenize(normalized_term))
                    if key and key not in stopword_keys
                )
            )
            if not keys:
                continue
            generic = normalized_term in GENERIC_PROFILE_TERMS or all(key in generic_keys for key in keys)
            result.append(_ProfileTerm(term=term, normalized_term=normalized_term, keys=keys, generic=generic))
        return result

    def _match_record(
        self,
        profile: FactorySiteOkvedProfile,
        term_index: list[_ProfileTerm],
        record: ContentRecord,
    ) -> FactorySiteOkvedMatch:
        context = self._build_record_context(record)
        evidence = self._collect_pattern_evidence(context, "industrial_identity_positive", INDUSTRIAL_POSITIVE_PATTERNS)
        evidence.extend(self._collect_pattern_evidence(context, "product_or_process_positive", PRODUCT_PROCESS_PATTERNS))
        evidence.extend(self._collect_pattern_evidence(context, "generic_corporate_weak", GENERIC_CORPORATE_PATTERNS))
        evidence.extend(self._collect_pattern_evidence(context, "dealer_negative", DEALER_NEGATIVE_PATTERNS))
        evidence.extend(self._collect_pattern_evidence(context, "portal_catalog_negative", PORTAL_CATALOG_NEGATIVE_PATTERNS))
        evidence.extend(
            self._collect_pattern_evidence(
                context,
                "marketplace_aggregator_negative",
                MARKETPLACE_AGGREGATOR_NEGATIVE_PATTERNS,
            )
        )
        evidence.extend(self._collect_pattern_evidence(context, "reseller_negative", RESELLER_NEGATIVE_PATTERNS))
        evidence.extend(
            self._collect_pattern_evidence(context, "unrelated_service_negative", UNRELATED_SERVICE_NEGATIVE_PATTERNS)
        )
        evidence.extend(self._collect_hint_evidence(context))

        matched_codes = [code for code in profile.okved_codes if code and code in context.combined_text]
        evidence.extend(self._build_okved_evidence(context, matched_codes))

        has_manufacturing_context = any(
            item.signal_group in {"industrial_identity_positive", "product_or_process_positive", "okved_code_positive"}
            for item in evidence
        )
        matched_term_entries = self._match_profile_terms(context, term_index)
        evidence.extend(self._build_term_evidence(matched_term_entries, manufacturing_context=has_manufacturing_context))

        evidence = self._dedupe_evidence(evidence)
        signal_breakdown = self._build_signal_breakdown(evidence)
        positive_evidence = self._sort_evidence(
            [item for item in evidence if item.signal_group in POSITIVE_SIGNAL_GROUPS]
        )
        negative_evidence = self._sort_evidence(
            [item for item in evidence if item.signal_group in NEGATIVE_SIGNAL_GROUPS]
        )
        positive_score = round(sum(signal_breakdown[group] for group in POSITIVE_SIGNAL_GROUPS), 3)
        negative_score = round(sum(signal_breakdown[group] for group in NEGATIVE_SIGNAL_GROUPS), 3)
        score = round(self._compute_score(signal_breakdown), 3)
        matched_terms = dedupe_preserve_order(term.term for term, _source in matched_term_entries)[:8]
        verdict = self._determine_verdict(
            score=score,
            signal_breakdown=signal_breakdown,
            matched_terms=matched_terms,
            matched_codes=matched_codes,
        )
        summary = self._build_record_summary(
            verdict=verdict,
            matched_terms=matched_terms,
            matched_codes=matched_codes,
            signal_breakdown=signal_breakdown,
        )
        return FactorySiteOkvedMatch(
            record_fingerprint=record.content_fingerprint,
            record_url=record.url,
            score=score,
            verdict=verdict,
            positive_score=positive_score,
            negative_score=negative_score,
            positive_evidence=positive_evidence,
            negative_evidence=negative_evidence,
            matched_okved_codes=matched_codes[:4],
            matched_terms=matched_terms[:6],
            summary=summary,
            signal_breakdown=signal_breakdown,
        )

    def _build_record_context(self, record: ContentRecord) -> _RecordContext:
        parsed = urlparse(record.url or "")
        raw_content = " ".join(part for part in [record.cleaned_text, record.raw_text] if part)
        url_path = re.sub(r"[/_.?=&-]+", " ", " ".join(part for part in [parsed.netloc, parsed.path, parsed.query] if part))
        combined = " ".join(
            part
            for part in [
                _normalize_text(record.title),
                _normalize_text(raw_content),
                _normalize_text(record.section_guess),
                _normalize_text(url_path),
            ]
            if part
        )
        stopword_keys = {_term_key(stopword) for stopword in STOPWORDS}
        return _RecordContext(
            title=_normalize_text(record.title),
            content=_normalize_text(raw_content),
            section=_normalize_text(record.section_guess),
            url_path=_normalize_text(url_path),
            combined_text=combined,
            token_keys=frozenset(
                key
                for key in (_term_key(token) for token in _tokenize(combined))
                if key and key not in stopword_keys
            ),
        )

    def _collect_pattern_evidence(
        self,
        context: _RecordContext,
        signal_group: str,
        patterns: tuple[tuple[str, float, str], ...],
    ) -> list[FactorySiteOkvedEvidence]:
        evidence: list[FactorySiteOkvedEvidence] = []
        for fragment, weight, reason in patterns:
            normalized_fragment = _normalize_text(fragment)
            if normalized_fragment and normalized_fragment in context.combined_text:
                evidence.append(
                    FactorySiteOkvedEvidence(
                        signal_group=signal_group,
                        source=context.find_source(fragment),
                        matched_text=fragment,
                        weight=weight,
                        reason=reason,
                    )
                )
        return evidence

    def _collect_hint_evidence(self, context: _RecordContext) -> list[FactorySiteOkvedEvidence]:
        evidence: list[FactorySiteOkvedEvidence] = []
        for hint, (signal_group, weight, reason) in SECTION_HINTS.items():
            if hint in context.section:
                evidence.append(
                    FactorySiteOkvedEvidence(
                        signal_group=signal_group,
                        source="section_guess",
                        matched_text=context.section,
                        weight=weight,
                        reason=reason,
                    )
                )
        for hint, (signal_group, weight, reason) in PATH_HINTS.items():
            if hint in context.url_path:
                evidence.append(
                    FactorySiteOkvedEvidence(
                        signal_group=signal_group,
                        source="url_path",
                        matched_text=hint,
                        weight=weight,
                        reason=reason,
                    )
                )
        return evidence

    def _build_okved_evidence(
        self,
        context: _RecordContext,
        matched_codes: list[str],
    ) -> list[FactorySiteOkvedEvidence]:
        evidence: list[FactorySiteOkvedEvidence] = []
        for index, code in enumerate(matched_codes):
            evidence.append(
                FactorySiteOkvedEvidence(
                    signal_group="okved_code_positive",
                    source=context.find_source(code),
                    matched_text=code,
                    weight=0.28 if index == 0 else 0.07,
                    reason="page explicitly contains an OKVED code from the company profile",
                )
            )
        return evidence

    def _match_profile_terms(
        self,
        context: _RecordContext,
        term_index: list[_ProfileTerm],
    ) -> list[tuple[_ProfileTerm, str]]:
        matches: list[tuple[_ProfileTerm, str]] = []
        for term in term_index:
            if term.normalized_term in context.combined_text:
                matches.append((term, context.find_source(term.normalized_term)))
                continue
            if len(term.keys) == 1:
                if term.keys[0] and term.keys[0] in context.token_keys:
                    matches.append((term, "content"))
                continue
            if all(key in context.token_keys for key in term.keys):
                matches.append((term, "content"))
        return matches

    def _build_term_evidence(
        self,
        matched_terms: list[tuple[_ProfileTerm, str]],
        *,
        manufacturing_context: bool,
    ) -> list[FactorySiteOkvedEvidence]:
        evidence: list[FactorySiteOkvedEvidence] = []
        for profile_term, source in matched_terms:
            if profile_term.generic:
                weight = 0.03 if manufacturing_context else 0.02
                reason = "page reuses a generic company-profile term"
            else:
                weight = 0.14 if manufacturing_context else 0.07
                reason = (
                    "page reuses a company-profile term in manufacturing context"
                    if manufacturing_context
                    else "page reuses a company-profile term without strong manufacturing context"
                )
            evidence.append(
                FactorySiteOkvedEvidence(
                    signal_group="product_or_process_positive",
                    source=source,
                    matched_text=profile_term.term,
                    weight=weight,
                    reason=reason,
                )
            )
        return evidence

    def _dedupe_evidence(self, evidence: list[FactorySiteOkvedEvidence]) -> list[FactorySiteOkvedEvidence]:
        result: list[FactorySiteOkvedEvidence] = []
        seen: set[tuple[str, str, str]] = set()
        for item in evidence:
            key = (item.signal_group, item.source, _normalize_text(item.matched_text))
            if key in seen:
                continue
            seen.add(key)
            result.append(item)
        return result

    def _sort_evidence(self, evidence: list[FactorySiteOkvedEvidence]) -> list[FactorySiteOkvedEvidence]:
        return sorted(
            evidence,
            key=lambda item: (-item.weight, item.signal_group, item.source, _normalize_text(item.matched_text)),
        )

    def _build_signal_breakdown(self, evidence: list[FactorySiteOkvedEvidence]) -> dict[str, float]:
        breakdown = {group: 0.0 for group in ALL_SIGNAL_GROUPS}
        for group in ALL_SIGNAL_GROUPS:
            total = sum(item.weight for item in evidence if item.signal_group == group)
            breakdown[group] = round(min(GROUP_CAPS[group], total), 3)
        return breakdown

    def _compute_score(self, signal_breakdown: dict[str, float]) -> float:
        positive_score = sum(signal_breakdown[group] for group in POSITIVE_SIGNAL_GROUPS)
        negative_score = sum(signal_breakdown[group] for group in NEGATIVE_SIGNAL_GROUPS)
        return max(0.0, min(1.0, 0.15 + positive_score - negative_score))

    def _determine_verdict(
        self,
        *,
        score: float,
        signal_breakdown: dict[str, float],
        matched_terms: list[str],
        matched_codes: list[str],
    ) -> str:
        industrial = signal_breakdown["industrial_identity_positive"]
        product = signal_breakdown["product_or_process_positive"]
        okved = signal_breakdown["okved_code_positive"]
        generic = signal_breakdown["generic_corporate_weak"]
        negative = sum(signal_breakdown[group] for group in NEGATIVE_SIGNAL_GROUPS)
        anchor_positive = industrial >= 0.14 or okved >= 0.28
        substantive_positive = product >= 0.22 or okved >= 0.28
        weak_overlap_only = bool(matched_terms or matched_codes) and not substantive_positive
        strong_business_model_negative = any(
            signal_breakdown[group] >= 0.3
            for group in (
                "dealer_negative",
                "portal_catalog_negative",
                "marketplace_aggregator_negative",
                "reseller_negative",
                "unrelated_service_negative",
            )
        )

        if negative >= 0.55 and score < 0.6:
            return "mismatch"
        if strong_business_model_negative and negative >= max(0.35, industrial + product + okved - 0.1):
            return "mismatch"
        if score >= 0.76 and anchor_positive and substantive_positive and negative < 0.28:
            return "strong_match"
        if score >= 0.56 and (anchor_positive or product >= 0.18 or okved >= 0.2) and negative < 0.4:
            return "weak_match"
        if score >= 0.32 or generic >= 0.06 or weak_overlap_only:
            return "uncertain"
        return "mismatch"

    def _build_record_summary(
        self,
        *,
        verdict: str,
        matched_terms: list[str],
        matched_codes: list[str],
        signal_breakdown: dict[str, float],
    ) -> str:
        positive_groups = self._top_group_labels(signal_breakdown, POSITIVE_SIGNAL_GROUPS)
        negative_groups = self._top_group_labels(signal_breakdown, NEGATIVE_SIGNAL_GROUPS)
        details: list[str] = []
        if matched_codes:
            details.append("okved=" + ", ".join(matched_codes[:2]))
        if matched_terms:
            details.append("terms=" + ", ".join(matched_terms[:3]))
        if positive_groups:
            details.append("positive=" + ", ".join(positive_groups[:2]))
        if negative_groups:
            details.append("negative=" + ", ".join(negative_groups[:2]))
        if not details:
            return "no semantic OKVED/site signal found"
        return f"{verdict}: " + "; ".join(details)

    def _aggregate_site_match(self, matches: list[FactorySiteOkvedMatch]) -> FactorySiteOkvedSiteMatch:
        if not matches:
            return FactorySiteOkvedSiteMatch(
                score=0.0,
                verdict="uncertain",
                summary="no content records available for semantic validation",
                signal_breakdown={group: 0.0 for group in ALL_SIGNAL_GROUPS},
            )

        site_breakdown = {group: 0.0 for group in ALL_SIGNAL_GROUPS}
        for group in ALL_SIGNAL_GROUPS:
            group_scores = sorted(
                (match.signal_breakdown.get(group, 0.0) for match in matches if match.signal_breakdown.get(group, 0.0) > 0),
                reverse=True,
            )
            if not group_scores:
                continue
            total = group_scores[0]
            for extra in group_scores[1:3]:
                total += extra * 0.4
            site_breakdown[group] = round(min(GROUP_CAPS[group], total), 3)

        score = round(self._compute_score(site_breakdown), 3)
        matched_terms = dedupe_preserve_order(term for match in matches for term in match.matched_terms)
        matched_codes = dedupe_preserve_order(code for match in matches for code in match.matched_okved_codes)
        verdict = self._determine_verdict(
            score=score,
            signal_breakdown=site_breakdown,
            matched_terms=matched_terms,
            matched_codes=matched_codes,
        )
        positive_pages = self._select_page_signals(matches, positive=True)
        negative_pages = self._select_page_signals(matches, positive=False)
        summary = self._build_site_summary(
            verdict=verdict,
            signal_breakdown=site_breakdown,
            matched_terms=matched_terms,
            positive_pages=positive_pages,
            negative_pages=negative_pages,
        )
        return FactorySiteOkvedSiteMatch(
            score=score,
            verdict=verdict,
            positive_pages=positive_pages,
            negative_pages=negative_pages,
            summary=summary,
            signal_breakdown=site_breakdown,
        )

    def _select_page_signals(
        self,
        matches: list[FactorySiteOkvedMatch],
        *,
        positive: bool,
    ) -> list[FactorySiteOkvedPageSignal]:
        if positive:
            filtered = [match for match in matches if match.positive_score > 0]
            filtered.sort(key=lambda match: (match.positive_score - match.negative_score, match.score), reverse=True)
        else:
            filtered = [match for match in matches if match.negative_score > 0]
            filtered.sort(
                key=lambda match: (match.negative_score, match.negative_score - match.positive_score, -match.score),
                reverse=True,
            )
        return [
            FactorySiteOkvedPageSignal(
                record_fingerprint=match.record_fingerprint,
                record_url=match.record_url,
                score=match.score,
                verdict=match.verdict,
                summary=match.summary,
            )
            for match in filtered[:3]
        ]

    def _build_site_summary(
        self,
        *,
        verdict: str,
        signal_breakdown: dict[str, float],
        matched_terms: list[str],
        positive_pages: list[FactorySiteOkvedPageSignal],
        negative_pages: list[FactorySiteOkvedPageSignal],
    ) -> str:
        positive_groups = self._top_group_labels(signal_breakdown, POSITIVE_SIGNAL_GROUPS)
        negative_groups = self._top_group_labels(signal_breakdown, NEGATIVE_SIGNAL_GROUPS)
        positive_page_labels = ", ".join(self._short_url(page.record_url) for page in positive_pages[:2])
        negative_page_labels = ", ".join(self._short_url(page.record_url) for page in negative_pages[:2])
        term_label = ", ".join(matched_terms[:3])

        if verdict == "strong_match":
            bits: list[str] = []
            if positive_page_labels:
                bits.append(f"positive pages: {positive_page_labels}")
            if term_label:
                bits.append(f"profile terms: {term_label}")
            if positive_groups:
                bits.append("signals: " + ", ".join(positive_groups[:2]))
            return "strong semantic match; " + "; ".join(bits)
        if verdict == "weak_match":
            bits = []
            if positive_page_labels:
                bits.append(f"positive pages: {positive_page_labels}")
            if term_label:
                bits.append(f"profile terms: {term_label}")
            if negative_groups:
                bits.append("negative pressure: " + ", ".join(negative_groups[:1]))
            return "weak semantic match; " + "; ".join(bits)
        if verdict == "mismatch":
            bits = []
            if negative_page_labels:
                bits.append(f"negative pages: {negative_page_labels}")
            if negative_groups:
                bits.append("dominant negatives: " + ", ".join(negative_groups[:2]))
            if positive_groups:
                bits.append("weak positives: " + ", ".join(positive_groups[:1]))
            return "semantic mismatch; " + "; ".join(bits)

        bits = []
        if positive_page_labels:
            bits.append(f"partial positives: {positive_page_labels}")
        if negative_page_labels:
            bits.append(f"negative pages: {negative_page_labels}")
        if not bits and positive_groups:
            bits.append("signals: " + ", ".join(positive_groups[:2]))
        return "semantic match is uncertain; " + "; ".join(bits) if bits else "semantic match is uncertain"

    def _top_group_labels(
        self,
        signal_breakdown: dict[str, float],
        groups: tuple[str, ...],
    ) -> list[str]:
        ranked = sorted(
            ((GROUP_LABELS[group], signal_breakdown[group]) for group in groups if signal_breakdown.get(group, 0.0) > 0),
            key=lambda item: item[1],
            reverse=True,
        )
        return [label for label, _score in ranked[:3]]

    def _short_url(self, url: str) -> str:
        parsed = urlparse(url)
        host = parsed.netloc or url
        path = parsed.path.rstrip("/") or "/"
        return f"{host}{path}"


__all__ = ["FactorySiteOkvedMatcher"]

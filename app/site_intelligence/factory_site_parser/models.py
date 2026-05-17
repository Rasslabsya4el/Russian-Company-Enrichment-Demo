from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Any

from app.site_intelligence.common import DOCUMENT_EXTENSIONS, dedupe_preserve_order, normalize_whitespace
from app.site_intelligence.fetcher import FetchTelemetry
from app.site_intelligence.models import ContentRecord, RouteStrategy, SiteProbe, normalize_worth_crawling

OKVED_PATTERN = re.compile(r"\b\d{2}\.\d{2}\b")

FACTORY_SITE_TRUST_STATE_UNKNOWN = "unknown"
FACTORY_SITE_TRUST_STATE_TRUSTED = "trusted"
FACTORY_SITE_TRUST_STATE_AMBIGUOUS = "ambiguous"
FACTORY_SITE_TRUST_STATE_REJECTED = "rejected"

TRUSTED_FACTORY_SITE_MATCH_VERDICTS = frozenset({"strong_match", "weak_match"})
AMBIGUOUS_FACTORY_SITE_MATCH_VERDICTS = frozenset({"uncertain"})
REJECTED_FACTORY_SITE_MATCH_VERDICTS = frozenset({"mismatch"})

HEAVY_FETCH_ROUTE_FAMILIES = frozenset({"docs/certificates", "files"})
HEAVY_FETCH_SECTIONS = frozenset({"documents", "files"})
BROWSER_EMBARGO_ROUTE_MODES = frozenset({"hybrid", "playwright"})


def _coerce_list(values: Any) -> list[str]:
    if not values:
        return []
    result: list[str] = []
    for value in values:
        cleaned = normalize_whitespace(str(value))
        if cleaned:
            result.append(cleaned)
    return result


def _read_field(payload: Any, field_name: str) -> Any:
    if isinstance(payload, dict):
        return payload.get(field_name)
    return getattr(payload, field_name, None)


def _normalized_route_value(value: str | None) -> str:
    return normalize_whitespace(value).lower()


def factory_site_trust_state_from_verdict(verdict: str | None) -> str:
    normalized_verdict = _normalized_route_value(verdict)
    if normalized_verdict in TRUSTED_FACTORY_SITE_MATCH_VERDICTS:
        return FACTORY_SITE_TRUST_STATE_TRUSTED
    if normalized_verdict in REJECTED_FACTORY_SITE_MATCH_VERDICTS:
        return FACTORY_SITE_TRUST_STATE_REJECTED
    if normalized_verdict in AMBIGUOUS_FACTORY_SITE_MATCH_VERDICTS:
        return FACTORY_SITE_TRUST_STATE_AMBIGUOUS
    return FACTORY_SITE_TRUST_STATE_UNKNOWN


def route_requires_trusted_fetch(route: RouteStrategy) -> bool:
    route_family = _normalized_route_value(getattr(route, "route_family", ""))
    section_guess = _normalized_route_value(getattr(route, "section_guess", ""))
    route_pattern = str(getattr(route, "route_pattern", "") or "").strip().lower().split("?", 1)[0]
    if route_family in HEAVY_FETCH_ROUTE_FAMILIES or section_guess in HEAVY_FETCH_SECTIONS:
        return True
    return any(route_pattern.endswith(ext) for ext in DOCUMENT_EXTENSIONS)


def route_mode_with_browser_embargo(route: RouteStrategy, *, browser_embargo: bool) -> str:
    normalized_mode = _normalized_route_value(getattr(route, "mode", "")) or "requests"
    if normalized_mode == "skip":
        return "skip"
    if browser_embargo and normalized_mode in BROWSER_EMBARGO_ROUTE_MODES:
        return "requests"
    return normalized_mode


@dataclass
class FactorySiteFetchPolicy:
    trust_state: str = FACTORY_SITE_TRUST_STATE_UNKNOWN
    trust_verdict: str = ""
    trust_summary: str = ""
    heavy_fetch_embargo: bool = True

    @property
    def allows_heavy_fetch(self) -> bool:
        return not self.heavy_fetch_embargo and self.trust_state == FACTORY_SITE_TRUST_STATE_TRUSTED


@dataclass
class FactorySiteParserCompany:
    company_id: str
    company_name: str
    input_site: str = ""
    candidate_sites: list[str] = field(default_factory=list)
    known_okved_codes: list[str] = field(default_factory=list)
    activity_terms: list[str] = field(default_factory=list)
    source_snippets: list[str] = field(default_factory=list)
    source_notes: list[str] = field(default_factory=list)

    @classmethod
    def from_row(
        cls,
        row: Any,
        *,
        candidate_sites: list[str] | None = None,
        source_results: dict[str, Any] | None = None,
    ) -> "FactorySiteParserCompany":
        snippets: list[str] = []
        notes: list[str] = []
        raw_parts = [normalize_whitespace(str(getattr(row, "company_name", "")))]

        for source_payload in (source_results or {}).values():
            source_snippets = _coerce_list(_read_field(source_payload, "snippets"))
            source_notes = _coerce_list(_read_field(source_payload, "notes"))
            snippets.extend(source_snippets[:4])
            notes.extend(source_notes[:3])
            raw_parts.extend(source_snippets[:3])
            raw_parts.extend(source_notes[:2])

        okved_codes = dedupe_preserve_order(OKVED_PATTERN.findall(" ".join(raw_parts)))
        return cls(
            company_id=normalize_whitespace(str(getattr(row, "inn", ""))),
            company_name=normalize_whitespace(str(getattr(row, "company_name", ""))),
            input_site=normalize_whitespace(str(getattr(row, "xlsx_site", ""))),
            candidate_sites=dedupe_preserve_order(candidate_sites or []),
            known_okved_codes=okved_codes,
            source_snippets=dedupe_preserve_order(snippets),
            source_notes=dedupe_preserve_order(notes),
        )

    def iter_candidate_sites(self) -> list[str]:
        return dedupe_preserve_order([self.input_site] + list(self.candidate_sites or []))


@dataclass
class FactorySiteMapSection:
    route_family: str
    section_guess: str
    crawl_budget: int = 0
    discovered_urls: list[str] = field(default_factory=list)
    planned_urls: list[str] = field(default_factory=list)
    planned_count: int = 0
    skipped_count: int = 0
    discovery_sources: list[str] = field(default_factory=list)
    reasons: list[str] = field(default_factory=list)
    skip_reasons: list[str] = field(default_factory=list)
    required_floor: bool = False
    floor_met: bool = False
    coverage_status: str = "not_discovered"


@dataclass
class FactorySiteCoverageStatus:
    coverage_key: str
    route_families: list[str] = field(default_factory=list)
    discovered: bool = False
    required: bool = False
    covered: bool = False
    selected_route_family: str = ""
    selected_url: str = ""
    skip_reason: str = ""


@dataclass
class FactorySiteSkippedRoute:
    route_family: str
    route_pattern: str
    skip_reason: str
    details: list[str] = field(default_factory=list)


@dataclass
class FactorySiteCapUsage:
    cap_type: str
    cap_key: str
    limit: int
    used: int = 0


@dataclass
class FactorySiteFamilyBudget:
    route_family: str
    section_guess: str
    discovered_count: int = 0
    budget_limit: int = 0
    planned_count: int = 0
    remaining_budget: int = 0
    selected_urls: list[str] = field(default_factory=list)
    skipped_count: int = 0
    skip_reasons: list[str] = field(default_factory=list)
    required_floor: bool = False
    floor_met: bool = False


@dataclass
class FactorySiteBudgetAccounting:
    global_budget: int = 0
    planned_routes: int = 0
    remaining_budget: int = 0
    family_budgets: list[FactorySiteFamilyBudget] = field(default_factory=list)
    cap_usage: list[FactorySiteCapUsage] = field(default_factory=list)
    skipped_routes: list[FactorySiteSkippedRoute] = field(default_factory=list)


@dataclass
class FactorySiteCrawlMap:
    site_url: str
    sections: list[FactorySiteMapSection] = field(default_factory=list)
    discovery_sources: list[str] = field(default_factory=list)
    coverage: list[FactorySiteCoverageStatus] = field(default_factory=list)
    budget_accounting: FactorySiteBudgetAccounting | None = None
    notes: list[str] = field(default_factory=list)


@dataclass
class FactorySitePlan:
    site_url: str
    probe: SiteProbe
    routes: list[RouteStrategy] = field(default_factory=list)
    crawl_map: FactorySiteCrawlMap | None = None
    budget_accounting: FactorySiteBudgetAccounting | None = None
    coverage: list[FactorySiteCoverageStatus] = field(default_factory=list)
    notes: list[str] = field(default_factory=list)
    fetch_telemetry: list[FetchTelemetry] = field(default_factory=list)
    fetch_policy: FactorySiteFetchPolicy = field(default_factory=FactorySiteFetchPolicy)
    access_state: str = ""
    block_class: str = ""
    anti_bot_reason: str = ""
    breaker_mode: str = "normal"
    manual_handoff_required: bool = False
    challenge_detected: bool = False
    session_reused: bool = False

    @property
    def crawl_queue(self) -> list[RouteStrategy]:
        return list(self.routes)

    @property
    def allows_deep_check(self) -> bool:
        if self.probe.status != "success":
            return False
        if self.probe.site_class in {"D", "E", "F"}:
            return False
        return normalize_worth_crawling(getattr(self.probe, "worth_crawling", "false")) != "false"

    @property
    def trust_state(self) -> str:
        return self.fetch_policy.trust_state


@dataclass
class FactorySiteOkvedProfile:
    okved_codes: list[str] = field(default_factory=list)
    terms: list[str] = field(default_factory=list)
    raw: str = ""
    site_match: FactorySiteOkvedSiteMatch | None = None


@dataclass
class FactorySiteOkvedEvidence:
    signal_group: str
    source: str
    matched_text: str
    weight: float
    reason: str = ""


@dataclass
class FactorySiteOkvedPageSignal:
    record_fingerprint: str
    record_url: str
    score: float
    verdict: str
    summary: str = ""


@dataclass
class FactorySiteOkvedSiteMatch:
    score: float = 0.0
    verdict: str = "uncertain"
    positive_pages: list[FactorySiteOkvedPageSignal] = field(default_factory=list)
    negative_pages: list[FactorySiteOkvedPageSignal] = field(default_factory=list)
    summary: str = ""
    signal_breakdown: dict[str, float] = field(default_factory=dict)


@dataclass
class FactorySiteOkvedMatch:
    record_fingerprint: str
    record_url: str
    score: float = 0.0
    verdict: str = "uncertain"
    positive_score: float = 0.0
    negative_score: float = 0.0
    positive_evidence: list[FactorySiteOkvedEvidence] = field(default_factory=list)
    negative_evidence: list[FactorySiteOkvedEvidence] = field(default_factory=list)
    matched_okved_codes: list[str] = field(default_factory=list)
    matched_terms: list[str] = field(default_factory=list)
    summary: str = ""
    signal_breakdown: dict[str, float] = field(default_factory=dict)


@dataclass
class FactorySiteParserResult:
    company: FactorySiteParserCompany
    plans: list[FactorySitePlan] = field(default_factory=list)
    site_probes: list[SiteProbe] = field(default_factory=list)
    route_strategies: list[RouteStrategy] = field(default_factory=list)
    crawl_maps: list[FactorySiteCrawlMap] = field(default_factory=list)
    content_records: list[ContentRecord] = field(default_factory=list)
    fetch_telemetry: list[FetchTelemetry] = field(default_factory=list)
    okved_profile: FactorySiteOkvedProfile | None = None
    okved_matches: list[FactorySiteOkvedMatch] = field(default_factory=list)
    okved_site_match: FactorySiteOkvedSiteMatch | None = None
    notes: list[str] = field(default_factory=list)

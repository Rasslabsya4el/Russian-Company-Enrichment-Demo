from __future__ import annotations

from urllib.parse import unquote, urlparse

from .common import (
    DOCUMENT_EXTENSIONS,
    corporate_route_hints,
    dedupe_preserve_order,
    normalize_whitespace,
    route_family_for_section,
    surplus_route_hints,
)
from .models import RouteStrategy, SiteProbe

PLANNER_ROUTE_FAMILY_BY_SECTION = {
    "homepage": "company/about",
    "about": "company/about",
    "contacts": "contacts",
    "products": "production/products",
    "services": "services",
    "news": "news",
    "documents": "docs/certificates",
    "procurement": "procurement",
    "sales": "surplus/realization",
    "vacancies": "vacancies",
    "branches": "branches/warehouses",
    "files": "files",
    "search": "search",
}
COVERAGE_ROUTE_FAMILIES = {
    "company/about",
    "contacts",
    "production/products",
    "docs/certificates",
    "files",
    "procurement",
    "surplus/realization",
}


def counts_toward_planner_coverage(route_family: str) -> bool:
    canonical_family = canonical_route_family(route_family) or route_family
    return canonical_family in COVERAGE_ROUTE_FAMILIES


def _repair_mojibake(value: str | None) -> str:
    return _repair_mojibake_safe(value)
    cleaned = normalize_whitespace(value)
    if not cleaned or ("Ð" not in cleaned and "Ñ" not in cleaned):
        return cleaned
    try:
        repaired = cleaned.encode("latin1").decode("utf-8")
    except UnicodeError:
        return cleaned
    return normalize_whitespace(repaired)


def _repair_mojibake_safe(value: str | None) -> str:
    cleaned = normalize_whitespace(value)
    if not cleaned or ("\u00d0" not in cleaned and "\u00d1" not in cleaned):
        return cleaned
    try:
        repaired = cleaned.encode("latin1").decode("utf-8")
    except UnicodeError:
        return cleaned
    return normalize_whitespace(repaired)


def _normalized_section_hints(route_hints: dict[str, tuple[str, ...]]) -> list[tuple[str, tuple[str, ...]]]:
    normalized_hints: list[tuple[str, tuple[str, ...]]] = []
    for section_name, hints in route_hints.items():
        normalized_hints.append(
            (
                section_name,
                tuple(_repair_mojibake_safe(hint).lower() for hint in hints if _repair_mojibake_safe(hint)),
            )
        )
    return normalized_hints


SURPLUS_SECTION_HINTS = _normalized_section_hints(surplus_route_hints)
CORPORATE_SECTION_HINTS = _normalized_section_hints(corporate_route_hints)
SECTION_HINTS: list[tuple[str, tuple[str, ...]]] = [*SURPLUS_SECTION_HINTS, *CORPORATE_SECTION_HINTS]
DOCUMENT_HEAVY_SECTIONS = {"documents", "files", "procurement", "search"}


def guess_section_from_url(url: str) -> str:
    normalized = normalize_whitespace(unquote(url)).lower()
    parsed = urlparse(normalized)
    path_query = f"{parsed.netloc}{parsed.path}?{parsed.query}".rstrip("?")
    if any(path_query.endswith(ext) for ext in DOCUMENT_EXTENSIONS):
        return "files"
    for section_name, hints in SECTION_HINTS:
        if any(hint in path_query for hint in hints):
            return section_name
    return "homepage"


def canonical_route_family(section_name: str) -> str:
    normalized = normalize_whitespace(section_name).lower()
    if not normalized:
        return ""
    return PLANNER_ROUTE_FAMILY_BY_SECTION.get(normalized, route_family_for_section(normalized))


def queue_name_for_route(*, mode: str, section: str) -> str:
    if mode == "skip":
        return "skip"
    if mode == "playwright":
        return "browser"
    if mode == "hybrid":
        return "hybrid"
    if section in DOCUMENT_HEAVY_SECTIONS:
        return "documents"
    return "http"


def route_caps(*, site_url: str, route_url: str, section: str) -> tuple[int | None, str, str]:
    site_host = normalize_whitespace(urlparse(site_url).netloc).lower()
    parsed = urlparse(route_url or site_url)
    host_cap = normalize_whitespace(parsed.netloc).lower() or site_host
    path_segments = [segment for segment in parsed.path.split("/") if segment]
    if any((parsed.path or "").lower().endswith(ext) for ext in DOCUMENT_EXTENSIONS):
        path_pattern_cap = parsed.path or "/"
    elif path_segments:
        path_pattern_cap = f"/{path_segments[0]}/"
    else:
        path_pattern_cap = "/"

    if section in DOCUMENT_HEAVY_SECTIONS:
        max_depth = 4
    elif section in {"products", "services", "sales", "news", "branches"}:
        max_depth = 3
    elif section == "homepage":
        max_depth = 1
    else:
        max_depth = 2
    return max_depth, host_cap, path_pattern_cap


class StrategySelector:
    def select(self, site_url: str, probe: SiteProbe) -> list[RouteStrategy]:
        urls = dedupe_preserve_order([site_url] + list(probe.sampled_urls or []))
        if not urls:
            return []

        strategies: list[RouteStrategy] = []
        for priority, url in enumerate(urls[:5], start=1):
            section = guess_section_from_url(url)
            route_family = canonical_route_family(section)
            mode, confidence, reasons = self.decide_mode(probe, section)
            strategies.append(self._build_route_strategy(site_url=site_url, url=url, section=section, route_family=route_family, mode=mode, confidence=confidence, priority=priority, reasons=reasons))
        return strategies

    def decide_mode(self, probe: SiteProbe, section: str) -> tuple[str, float, list[str]]:
        if probe.site_class == "F":
            return "skip", 0.95, [f"site_class={probe.site_class}", "probe says the site is not worth cheap crawl"]
        if probe.site_class == "E":
            return "skip", 0.93, [f"site_class={probe.site_class}", "anti-bot or gated probe result keeps planner queue disabled"]
        if probe.site_class == "D":
            return "playwright", 0.85, ["heavy JS or SPA shell detected", "browser mode is the safe default"]
        if probe.site_class == "C":
            if section in {"procurement", "documents", "files", "search"}:
                return "hybrid", 0.78, ["mixed site", "route may need browser only for targeted pages"]
            return "requests", 0.7, ["mixed site but the route looks fetchable via HTML"]
        if probe.site_class in {"A", "B"}:
            if section in {"contacts", "about", "news", "homepage", "products", "services", "sales", "branches"}:
                return "requests", 0.9, [f"site_class={probe.site_class}", "section should be HTTP-friendly"]
            return "requests", 0.82, [f"site_class={probe.site_class}", "plain HTML crawl is likely enough"]
        return "skip", 0.5, ["unable to choose a reliable fetch strategy"]

    def _build_route_strategy(
        self,
        *,
        site_url: str,
        url: str,
        section: str,
        route_family: str,
        mode: str,
        confidence: float,
        priority: int,
        reasons: list[str],
    ) -> RouteStrategy:
        max_depth, host_cap, path_pattern_cap = route_caps(site_url=site_url, route_url=url, section=section)
        queue_name = queue_name_for_route(mode=mode, section=section)
        normalized_family = route_family or canonical_route_family(section) or section or "company/about"
        return RouteStrategy(
            site_url=site_url,
            route_pattern=url,
            section_guess=section,
            mode=mode,
            confidence=confidence,
            route_family=normalized_family,
            priority=priority,
            crawl_budget=1,
            queue_name=queue_name,
            accounting_key=f"{host_cap}:{normalized_family}" if host_cap else normalized_family,
            mandatory=False,
            counts_toward_coverage=counts_toward_planner_coverage(normalized_family),
            skip_reason=reasons[0] if mode == "skip" and reasons else "",
            max_depth=max_depth,
            host_cap=host_cap,
            path_pattern_cap=path_pattern_cap,
            reasons=reasons,
            discovery_sources=["site_probe"],
        )


__all__ = ["StrategySelector", "guess_section_from_url"]

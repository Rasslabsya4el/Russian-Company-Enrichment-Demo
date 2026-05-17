from __future__ import annotations

import codecs
import gzip
import json
import os
import re
from collections import defaultdict
from dataclasses import dataclass, field
from html import unescape
from typing import Any
from urllib.parse import unquote, urljoin, urlparse

from bs4 import BeautifulSoup

from app.site_intelligence import SiteProber, StrategySelector
from app.site_intelligence.common import (
    DOCUMENT_EXTENSIONS,
    dedupe_preserve_order,
    guess_registered_domain,
    normalize_url,
    normalize_whitespace,
    route_family_for_section,
    surplus_route_hints,
)
from app.site_intelligence.models import normalize_worth_crawling
from app.site_intelligence.strategy import queue_name_for_route, route_caps

from .models import (
    FactorySiteBudgetAccounting,
    FactorySiteCapUsage,
    FactorySiteCoverageStatus,
    FactorySiteCrawlMap,
    FactorySiteFamilyBudget,
    FactorySiteMapSection,
    FactorySiteParserCompany,
    FactorySitePlan,
    FactorySiteSkippedRoute,
)

SURPLUS_ROUTE_FAMILY = route_family_for_section("sales")
SURPLUS_ROUTE_KEYWORDS = tuple(
    dedupe_preserve_order((*surplus_route_hints.get("sales", ()), "stock", "surplus", "sale-off", "sklad", "warehouse"))
)

MANDATORY_ROUTE_FAMILIES: list[dict[str, Any]] = [
    {
        "family": "company/about",
        "section": "about",
        "weight": 96,
        "budget": 2,
        "keywords": ("about", "company", "enterprise", "o-kompanii", "o_nas", "о компании", "о нас", "предприят", "завод"),
        "subdomains": ("company", "corp"),
    },
    {
        "family": "contacts",
        "section": "contacts",
        "weight": 100,
        "budget": 2,
        "keywords": ("contact", "contacts", "kontakt", "kontakty", "feedback", "rekvizit", "контакт", "обратная связь", "реквизит"),
        "subdomains": ("contacts", "office"),
    },
    {
        "family": "production/products",
        "section": "products",
        "weight": 93,
        "budget": 3,
        "keywords": ("product", "products", "catalog", "catalogue", "produk", "каталог", "продук", "ассортимент", "nomenclature", "товар"),
        "subdomains": ("catalog", "products", "shop"),
    },
    {
        "family": "services",
        "section": "services",
        "weight": 78,
        "budget": 2,
        "keywords": ("service", "services", "uslug", "сервис", "услуг", "engineering", "инжиниринг", "монтаж", "обслуживание"),
        "subdomains": ("service", "services"),
    },
    {
        "family": "news",
        "section": "news",
        "weight": 62,
        "budget": 2,
        "keywords": ("news", "press", "blog", "media", "новост", "пресс", "событи"),
        "subdomains": ("news", "media", "press"),
    },
    {
        "family": "docs/certificates",
        "section": "documents",
        "weight": 88,
        "budget": 3,
        "keywords": ("docs", "documents", "document", "certificate", "cert", "sert", "download", "документ", "сертифик", "лиценз", "паспорт", "декларац"),
        "subdomains": ("docs", "doc", "cert", "quality"),
    },
    {
        "family": "procurement",
        "section": "procurement",
        "weight": 91,
        "budget": 3,
        "keywords": ("procurement", "purchase", "purchases", "supplier", "suppliers", "tender", "tenders", "torg", "zakup", "закуп", "тендер", "торги", "поставщик"),
        "subdomains": ("zakupki", "tender", "tenders", "supplier", "trade"),
    },
    {
        "family": SURPLUS_ROUTE_FAMILY,
        "section": "sales",
        "weight": 90,
        "budget": 3,
        "keywords": SURPLUS_ROUTE_KEYWORDS,
        "subdomains": ("sale", "sales", "realization", "sklad", "warehouse"),
    },
    {
        "family": "vacancies",
        "section": "vacancies",
        "weight": 54,
        "budget": 1,
        "keywords": ("career", "vacancy", "vacancies", "job", "jobs", "hr", "rabota", "ваканс", "карьер", "работа"),
        "subdomains": ("career", "job", "hr"),
    },
    {
        "family": "branches/warehouses",
        "section": "branches",
        "weight": 84,
        "budget": 2,
        "keywords": ("branch", "branches", "warehouse", "warehouses", "office", "offices", "filial", "dealer", "склад", "филиал", "представител", "офис", "дилер"),
        "subdomains": ("warehouse", "branch", "branches", "office"),
    },
    {
        "family": "files",
        "section": "files",
        "weight": 80,
        "budget": 4,
        "keywords": ("download", "file", "files", "pdf", "doc", "xls", "xlsx", "zip", "rar", "скачать", "файл", "архив"),
        "subdomains": ("files", "download"),
    },
]

OPTIONAL_ROUTE_FAMILIES: list[dict[str, Any]] = [
    {
        "family": "search",
        "section": "search",
        "weight": 58,
        "budget": 1,
        "keywords": ("search", "query", "find", "lookup", "поиск", "искать"),
        "subdomains": ("search",),
    }
]

FAMILY_RULES = {item["family"]: item for item in MANDATORY_ROUTE_FAMILIES + OPTIONAL_ROUTE_FAMILIES}
FAMILY_ORDER = [item["family"] for item in MANDATORY_ROUTE_FAMILIES] + [item["family"] for item in OPTIONAL_ROUTE_FAMILIES]
ROOT_FAMILY = "company/about"
SITEMAP_LOC_RE = re.compile(r"<loc>\s*(.*?)\s*</loc>", flags=re.IGNORECASE | re.DOTALL)
SITEMAP_XML_ENCODING_RE = re.compile(
    br"<\?xml[^>]*encoding\s*=\s*['\"]\s*([^\"']+?)\s*['\"]",
    flags=re.IGNORECASE,
)

HTML_REGION_SELECTORS: list[tuple[str, tuple[str, ...]]] = [
    ("menu", ("header a[href]", "nav a[href]", "[role='navigation'] a[href]", "[class*='menu'] a[href]", "[class*='nav'] a[href]")),
    ("breadcrumbs", ("[class*='breadcrumb'] a[href]", "[aria-label*='breadcrumb'] a[href]")),
    ("footer", ("footer a[href]", "[class*='footer'] a[href]")),
    ("internal_links", ("main a[href]", "article a[href]", "body a[href]")),
]

SOURCE_BONUS = {
    "homepage": 4.2,
    "menu": 4.6,
    "breadcrumbs": 4.0,
    "footer": 3.5,
    "internal_links": 2.7,
    "robots.txt": 2.6,
    "robots_allow": 2.0,
    "sitemap.xml": 4.3,
    "site_probe": 3.2,
    "documents": 3.8,
    "common_subdomain": 3.4,
    "search_form": 3.3,
}

EXACT_PATH_HINTS = {
    "company/about": ("/about", "/about/", "/company", "/company/", "/o-kompanii", "/o-kompanii/"),
    "contacts": ("/contacts", "/contacts/", "/contact", "/contact/", "/kontakt", "/kontakty", "/rekvizity", "/feedback"),
    "production/products": ("/catalog", "/catalog/", "/products", "/products/", "/product", "/product/"),
    "services": ("/services", "/services/", "/service", "/service/"),
    "news": ("/news", "/news/", "/press", "/press/"),
    "docs/certificates": ("/documents", "/documents/", "/docs", "/docs/", "/certificates", "/certificate"),
    "procurement": ("/procurement", "/procurement/", "/zakupki", "/zakupki/", "/tenders", "/tenders/"),
    SURPLUS_ROUTE_FAMILY: ("/sales", "/sales/", "/sale", "/sale/", "/realization", "/realization/"),
    "vacancies": ("/vacancies", "/vacancies/", "/career", "/career/", "/jobs", "/jobs/"),
    "branches/warehouses": ("/branches", "/branches/", "/branch", "/branch/", "/warehouses", "/warehouse/"),
    "files": ("/files", "/files/", "/download", "/download/"),
    "search": ("/search", "/search/"),
}


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


def _normalized_keyword_token(value: str | None) -> str:
    return _repair_mojibake_safe(value).lower()


def _normalize_sitemap_encoding_token(value: Any) -> str:
    token = normalize_whitespace("" if value is None else str(value))
    if not token:
        return ""
    if "=" in token:
        left, right = token.rsplit("=", 1)
        if "charset" in left.lower() or "encoding" in left.lower():
            token = right
    token = token.strip().strip("\"'")
    for separator in (";", ",", " "):
        if separator in token:
            token = token.split(separator, 1)[0].strip()
    return token.strip("\"'")


def _validated_sitemap_encoding(value: Any) -> str:
    token = _normalize_sitemap_encoding_token(value)
    if not token:
        return ""
    try:
        return codecs.lookup(token).name
    except LookupError:
        return ""


def _declared_sitemap_encoding(raw: bytes) -> str:
    match = SITEMAP_XML_ENCODING_RE.search(raw[:256])
    if not match:
        return ""
    return _normalize_sitemap_encoding_token(match.group(1).decode("ascii", errors="ignore"))

DEFAULT_GLOBAL_CRAWL_BUDGET = 10
DEFAULT_HOST_CAP = DEFAULT_GLOBAL_CRAWL_BUDGET
DEFAULT_SIMPLE_PATH_PATTERN_CAP = 1
DEFAULT_MULTI_PATH_PATTERN_CAP = 2
MULTI_PATH_PATTERN_SECTIONS = frozenset(
    {
        "branches",
        "documents",
        "files",
        "news",
        "procurement",
        "products",
        "sales",
        "services",
    }
)
COVERAGE_REQUIREMENTS: list[dict[str, Any]] = [
    {"coverage_key": "company/about", "route_families": ("company/about",), "required": True},
    {"coverage_key": "contacts", "route_families": ("contacts",), "required": True},
    {"coverage_key": "production/products", "route_families": ("production/products",), "required": True},
    {"coverage_key": "docs/files", "route_families": ("docs/certificates", "files"), "required_if_discovered": True},
    {"coverage_key": SURPLUS_ROUTE_FAMILY, "route_families": (SURPLUS_ROUTE_FAMILY,), "required_if_discovered": True},
    {"coverage_key": "procurement", "route_families": ("procurement",), "required_if_discovered": True},
]


@dataclass
class _CandidateState:
    url: str
    route_family: str
    section_guess: str
    family_match_score: float = 0.0
    evidence_score: float = 0.0
    is_document: bool = False
    guessed_only: bool = True
    matched_tokens: list[str] = field(default_factory=list)
    reasons: list[str] = field(default_factory=list)
    discovery_sources: list[str] = field(default_factory=list)
    source_pages: list[str] = field(default_factory=list)

    def merged_reasons(self) -> list[str]:
        return dedupe_preserve_order(self.reasons)

    def merged_sources(self) -> list[str]:
        return dedupe_preserve_order(self.discovery_sources)

    def merged_source_pages(self) -> list[str]:
        return dedupe_preserve_order(self.source_pages)

    def merged_tokens(self) -> list[str]:
        return dedupe_preserve_order(self.matched_tokens)


class FactorySitePlanner:
    def __init__(
        self,
        client: Any,
        *,
        prober: SiteProber | None = None,
        strategy_selector: StrategySelector | None = None,
    ) -> None:
        self.client = client
        self.prober = prober or SiteProber(client)
        self.strategy_selector = strategy_selector or StrategySelector()
        self.max_discovery_pages = max(2, int(os.getenv("FACTORY_SITE_PLANNER_MAX_DISCOVERY_PAGES", "6")))
        self.max_sitemap_urls = max(40, int(os.getenv("FACTORY_SITE_PLANNER_MAX_SITEMAP_URLS", "180")))
        self.max_candidates_per_family = max(1, int(os.getenv("FACTORY_SITE_PLANNER_MAX_CANDIDATES_PER_FAMILY", "3")))
        self.max_links_per_region = max(10, int(os.getenv("FACTORY_SITE_PLANNER_MAX_LINKS_PER_REGION", "40")))
        self.max_nested_sitemaps = max(1, int(os.getenv("FACTORY_SITE_PLANNER_MAX_NESTED_SITEMAPS", "5")))
        self.max_subdomain_checks = max(3, int(os.getenv("FACTORY_SITE_PLANNER_MAX_SUBDOMAIN_CHECKS", "8")))
        self.global_crawl_budget = self._read_optional_int_env("FACTORY_SITE_PLANNER_GLOBAL_CRAWL_BUDGET")
        self.family_budget_overrides = self._read_int_map_env("FACTORY_SITE_PLANNER_FAMILY_BUDGET_OVERRIDES")
        self.depth_caps = self._read_int_map_env("FACTORY_SITE_PLANNER_DEPTH_CAPS")
        self.host_caps = self._read_int_map_env("FACTORY_SITE_PLANNER_HOST_CAPS")
        self.path_pattern_caps = self._read_path_pattern_caps("FACTORY_SITE_PLANNER_PATH_PATTERN_CAPS")
        self.max_ranked_candidates_per_family = max(
            self.max_candidates_per_family,
            max((int(rule["budget"]) for rule in FAMILY_RULES.values()), default=1),
            max(self.family_budget_overrides.values(), default=0),
        )

    def _read_optional_int_env(self, name: str) -> int | None:
        raw = normalize_whitespace(os.getenv(name, ""))
        if not raw:
            return None
        try:
            return max(0, int(raw))
        except ValueError:
            return None

    def _read_int_map_env(self, name: str) -> dict[str, int]:
        raw = normalize_whitespace(os.getenv(name, ""))
        if not raw:
            return {}
        try:
            payload = json.loads(raw)
        except json.JSONDecodeError:
            return {}
        if not isinstance(payload, dict):
            return {}
        result: dict[str, int] = {}
        for key, value in payload.items():
            cleaned_key = normalize_whitespace(str(key))
            if not cleaned_key:
                continue
            try:
                result[cleaned_key] = max(0, int(value))
            except (TypeError, ValueError):
                continue
        return result

    def _read_path_pattern_caps(self, name: str) -> list[tuple[re.Pattern[str], str, int]]:
        raw_caps = self._read_int_map_env(name)
        compiled: list[tuple[re.Pattern[str], str, int]] = []
        for pattern, limit in raw_caps.items():
            try:
                compiled.append((re.compile(pattern, flags=re.IGNORECASE), pattern, limit))
            except re.error:
                continue
        compiled.sort(key=lambda item: item[1])
        return compiled

    def plan(self, company: FactorySiteParserCompany, *, max_sites: int | None = None) -> list[FactorySitePlan]:
        candidate_sites = company.iter_candidate_sites()
        if max_sites is not None:
            candidate_sites = candidate_sites[:max_sites]

        plans: list[FactorySitePlan] = []
        for candidate_site in dedupe_preserve_order(candidate_sites):
            probe = self.prober.probe(candidate_site)
            crawl_map, routes, budget_accounting, coverage = self._build_structured_plan(candidate_site, probe)
            notes = self._build_notes(candidate_site, probe, crawl_map, routes)
            plans.append(
                FactorySitePlan(
                    site_url=candidate_site,
                    probe=probe,
                    routes=routes,
                    crawl_map=crawl_map,
                    budget_accounting=budget_accounting,
                    coverage=coverage,
                    notes=notes,
                )
            )
        return plans

    def _build_structured_plan(
        self,
        site_url: str,
        probe: Any,
    ) -> tuple[FactorySiteCrawlMap, list[Any], FactorySiteBudgetAccounting, list[FactorySiteCoverageStatus]]:
        normalized_site = normalize_url(getattr(probe, "final_url", "") or site_url)
        if not normalized_site:
            budget_accounting = FactorySiteBudgetAccounting()
            crawl_map = FactorySiteCrawlMap(
                site_url=site_url,
                sections=self._empty_sections(),
                budget_accounting=budget_accounting,
                notes=["planner could not normalize the site url"],
            )
            return crawl_map, [], budget_accounting, []

        parsed_site = urlparse(normalized_site)
        origin = f"{parsed_site.scheme}://{parsed_site.netloc}"
        root_domain = guess_registered_domain(parsed_site.netloc)
        candidates: dict[str, _CandidateState] = {}
        request_cache: dict[str, Any] = {}

        self._register_candidate(
            candidates,
            url=normalized_site,
            origin=origin,
            root_domain=root_domain,
            discovery_source="homepage",
            source_page=normalized_site,
            anchor_text="home",
            context_text="site root fallback",
            guessed=False,
        )

        for sampled_url in dedupe_preserve_order(list(getattr(probe, "sampled_urls", []) or [])):
            self._register_candidate(
                candidates,
                url=sampled_url,
                origin=origin,
                root_domain=root_domain,
                discovery_source="site_probe",
                source_page=normalized_site,
                anchor_text="sampled route",
                context_text="probe sampled the route",
                guessed=False,
            )

        if getattr(probe, "status", "") == "success":
            self._discover_from_html(
                normalized_site,
                origin=origin,
                root_domain=root_domain,
                candidates=candidates,
                request_cache=request_cache,
            )
            for page_url in self._select_follow_up_pages(candidates, normalized_site):
                self._discover_from_html(
                    page_url,
                    origin=origin,
                    root_domain=root_domain,
                    candidates=candidates,
                    request_cache=request_cache,
                )

        self._discover_from_robots_and_sitemaps(
            origin=origin,
            root_domain=root_domain,
            candidates=candidates,
            request_cache=request_cache,
            homepage_url=normalized_site,
        )
        self._probe_common_subdomains(
            normalized_site=normalized_site,
            origin=origin,
            root_domain=root_domain,
            candidates=candidates,
            request_cache=request_cache,
        )
        self._ensure_fallback_candidate(candidates, normalized_site, origin, root_domain)

        routes, budget_accounting, coverage = self._rank_routes(origin, probe, candidates)
        crawl_map = self._build_crawl_map(origin, candidates, routes, budget_accounting, coverage)
        return crawl_map, routes, budget_accounting, coverage

    def _discover_from_html(
        self,
        page_url: str,
        *,
        origin: str,
        root_domain: str,
        candidates: dict[str, _CandidateState],
        request_cache: dict[str, Any],
    ) -> None:
        response = self._fetch_response(page_url, source="crawl_planner_html", request_cache=request_cache)
        if response is None or not self._looks_like_html(response):
            return

        soup = BeautifulSoup(response.text or "", "html.parser")
        seen_per_region: dict[str, set[str]] = defaultdict(set)
        for region_name, selectors in HTML_REGION_SELECTORS:
            anchors: list[Any] = []
            for selector in selectors:
                anchors.extend(soup.select(selector))
            for anchor in anchors:
                href = normalize_whitespace(anchor.get("href", ""))
                if not href or href.startswith(("mailto:", "tel:", "#", "javascript:")):
                    continue
                full_url = normalize_url(urljoin(response.url, href))
                if not full_url or full_url in seen_per_region[region_name]:
                    continue
                seen_per_region[region_name].add(full_url)
                anchor_text = normalize_whitespace(anchor.get_text(" ", strip=True))
                title_text = normalize_whitespace(anchor.get("title", "") or anchor.get("aria-label", ""))
                parent_text = normalize_whitespace(anchor.parent.get_text(" ", strip=True) if anchor.parent else "")
                context_text = title_text or (parent_text[:120] if not anchor_text else "")
                self._register_candidate(
                    candidates,
                    url=full_url,
                    origin=origin,
                    root_domain=root_domain,
                    discovery_source="documents" if self._is_document_url(full_url) else region_name,
                    source_page=response.url,
                    anchor_text=anchor_text,
                    context_text=context_text,
                    guessed=False,
                )
                if len(seen_per_region[region_name]) >= self.max_links_per_region:
                    break

        self._register_search_forms(
            soup,
            current_url=response.url,
            origin=origin,
            root_domain=root_domain,
            candidates=candidates,
        )

    def _register_search_forms(
        self,
        soup: BeautifulSoup,
        *,
        current_url: str,
        origin: str,
        root_domain: str,
        candidates: dict[str, _CandidateState],
    ) -> None:
        for form in soup.select("form"):
            action = normalize_whitespace(form.get("action", ""))
            inputs = form.select("input")
            names = " ".join(normalize_whitespace(item.get("name", "")).lower() for item in inputs)
            search_like = bool(form.select("input[type='search']")) or any(token in names for token in ("search", "query", "q", "s", "поиск"))
            if not search_like:
                continue
            form_url = normalize_url(urljoin(current_url, action or current_url))
            if not form_url:
                continue
            self._register_candidate(
                candidates,
                url=form_url,
                origin=origin,
                root_domain=root_domain,
                discovery_source="search_form",
                source_page=current_url,
                anchor_text="search form",
                context_text="on-site search form detected",
                guessed=False,
            )

    def _discover_from_robots_and_sitemaps(
        self,
        *,
        origin: str,
        root_domain: str,
        candidates: dict[str, _CandidateState],
        request_cache: dict[str, Any],
        homepage_url: str,
    ) -> None:
        sitemap_urls = [urljoin(origin, "/sitemap.xml")]
        robots = self._fetch_response(urljoin(origin, "/robots.txt"), source="crawl_planner_robots", request_cache=request_cache)
        if robots is not None and robots.status_code < 400:
            robots_text = robots.text or ""
            for raw_line in robots_text.splitlines():
                line = normalize_whitespace(raw_line)
                lower = line.lower()
                if lower.startswith("sitemap:"):
                    try:
                        sitemap_candidate = urljoin(origin, line.split(":", 1)[1].strip())
                    except ValueError:
                        continue
                    sitemap_target = normalize_url(sitemap_candidate)
                    if sitemap_target:
                        sitemap_urls.append(sitemap_target)
                elif lower.startswith("allow:"):
                    allow_target = normalize_whitespace(line.split(":", 1)[1])
                    if allow_target and allow_target.startswith("/"):
                        self._register_candidate(
                            candidates,
                            url=urljoin(origin, allow_target),
                            origin=origin,
                            root_domain=root_domain,
                            discovery_source="robots_allow",
                            source_page=homepage_url,
                            anchor_text="robots allow",
                            context_text=allow_target,
                            guessed=False,
                        )

        pending = dedupe_preserve_order(sitemap_urls)
        visited: set[str] = set()
        seen_urls = 0
        while pending and len(visited) < self.max_nested_sitemaps and seen_urls < self.max_sitemap_urls:
            sitemap_url = pending.pop(0)
            if sitemap_url in visited:
                continue
            visited.add(sitemap_url)
            sitemap_response = self._fetch_response(sitemap_url, source="crawl_planner_sitemap", request_cache=request_cache)
            if sitemap_response is None or sitemap_response.status_code >= 400:
                continue
            for loc_url in self._extract_sitemap_locs(sitemap_response):
                if loc_url in visited and loc_url.endswith((".xml", ".gz")):
                    continue
                lower_loc = loc_url.lower()
                if (lower_loc.endswith(".xml") or lower_loc.endswith(".gz")) and "sitemap" in lower_loc:
                    if len(visited) + len(pending) < self.max_nested_sitemaps + 1:
                        pending.append(loc_url)
                    continue
                if guess_registered_domain(urlparse(loc_url).netloc) != root_domain:
                    continue
                seen_urls += 1
                self._register_candidate(
                    candidates,
                    url=loc_url,
                    origin=origin,
                    root_domain=root_domain,
                    discovery_source="sitemap.xml",
                    source_page=sitemap_url,
                    anchor_text="sitemap loc",
                    context_text="discovered in sitemap.xml",
                    guessed=False,
                )
                if seen_urls >= self.max_sitemap_urls:
                    break

    def _probe_common_subdomains(
        self,
        *,
        normalized_site: str,
        origin: str,
        root_domain: str,
        candidates: dict[str, _CandidateState],
        request_cache: dict[str, Any],
    ) -> None:
        parsed = urlparse(normalized_site)
        scheme = parsed.scheme or "https"
        missing_families = {
            family
            for family in FAMILY_ORDER
            if family != "search" and not any(candidate.route_family == family for candidate in candidates.values())
        }
        guess_map: dict[str, str] = {}
        for family in FAMILY_ORDER:
            if family not in missing_families:
                continue
            for subdomain in FAMILY_RULES[family]["subdomains"]:
                guess_map.setdefault(f"{scheme}://{subdomain}.{root_domain}/", family)

        checked = 0
        guessed_success_seen = False
        post_success_hard_misses = 0
        post_success_hard_miss_limit = 2
        for guessed_url, family_name in guess_map.items():
            if checked >= self.max_subdomain_checks:
                break
            if normalize_url(guessed_url) == origin + "/":
                continue
            response = self._fetch_response(guessed_url, source="crawl_planner_subdomain", request_cache=request_cache)
            checked += 1
            final_url = normalize_url(getattr(response, "url", "")) if response is not None else ""
            same_registered_domain = bool(final_url) and guess_registered_domain(urlparse(final_url).netloc) == root_domain
            is_success_path = response is not None and response.status_code < 400 and same_registered_domain
            if not is_success_path:
                if guessed_success_seen:
                    post_success_hard_misses += 1
                    if post_success_hard_misses >= post_success_hard_miss_limit:
                        break
                continue

            normalized_final = normalize_url(final_url)
            known_before_register = bool(normalized_final) and normalized_final in candidates
            accepted_guessed_success = known_before_register
            if not accepted_guessed_success:
                self._register_candidate(
                    candidates,
                    url=final_url,
                    origin=origin,
                    root_domain=root_domain,
                    discovery_source="common_subdomain",
                    source_page=origin,
                    anchor_text=family_name,
                    context_text=f"common subdomain resolved for {family_name}",
                    guessed=True,
                )
                accepted_guessed_success = bool(normalized_final) and normalized_final in candidates
            if accepted_guessed_success:
                guessed_success_seen = True
                post_success_hard_misses = 0

    def _ensure_fallback_candidate(
        self,
        candidates: dict[str, _CandidateState],
        normalized_site: str,
        origin: str,
        root_domain: str,
    ) -> None:
        if candidates:
            return
        self._register_candidate(
            candidates,
            url=normalized_site,
            origin=origin,
            root_domain=root_domain,
            discovery_source="homepage",
            source_page=normalized_site,
            anchor_text="home",
            context_text="fallback route candidate",
            guessed=False,
        )

    def _select_follow_up_pages(self, candidates: dict[str, _CandidateState], homepage_url: str) -> list[str]:
        buckets = self._family_buckets(candidates)
        follow_up: list[str] = []
        for family in FAMILY_ORDER:
            bucket = buckets.get(family, [])
            for candidate in bucket:
                if candidate.url == homepage_url or candidate.is_document:
                    continue
                follow_up.append(candidate.url)
                break
            if len(follow_up) >= self.max_discovery_pages:
                break
        return dedupe_preserve_order(follow_up)

    def _rank_routes(
        self,
        origin: str,
        probe: Any,
        candidates: dict[str, _CandidateState],
    ) -> tuple[list[Any], FactorySiteBudgetAccounting, list[FactorySiteCoverageStatus]]:
        buckets = self._family_buckets(candidates)
        family_limits = {
            family: (self._family_budget_limit(family) if buckets.get(family) else 0)
            for family in FAMILY_ORDER
        }
        global_budget = self._resolve_global_budget(buckets, family_limits)
        positions = {family: 0 for family in FAMILY_ORDER}
        family_counts = {family: 0 for family in FAMILY_ORDER}
        selected_urls: set[str] = set()
        routes: list[Any] = []
        coverage: list[FactorySiteCoverageStatus] = []
        skip_registry: dict[str, FactorySiteSkippedRoute] = {}
        decision_cache: dict[str, dict[str, Any]] = {}
        host_usage: dict[str, int] = defaultdict(int)
        host_limits: dict[str, int] = {}
        path_usage: dict[str, int] = defaultdict(int)
        path_limits: dict[str, int] = {}
        selected_depths: dict[str, list[int]] = defaultdict(list)
        selected_depth_limits: dict[str, int] = {}
        remaining_budget = global_budget

        for requirement in self._coverage_requirements(buckets):
            status = FactorySiteCoverageStatus(
                coverage_key=requirement["coverage_key"],
                route_families=list(requirement["route_families"]),
                discovered=requirement["discovered"],
                required=requirement["required"],
            )
            if not requirement["required"]:
                status.skip_reason = "not_discovered"
                coverage.append(status)
                continue
            if remaining_budget <= 0:
                status.skip_reason = "global_budget_exhausted"
                coverage.append(status)
                continue

            selected = False
            for family in requirement["route_families"]:
                if family_counts.get(family, 0) >= family_limits.get(family, 0):
                    continue
                candidate, payload = self._consume_candidate(
                    family=family,
                    origin=origin,
                    probe=probe,
                    buckets=buckets,
                    positions=positions,
                    selected_urls=selected_urls,
                    host_usage=host_usage,
                    path_usage=path_usage,
                    skip_registry=skip_registry,
                    decision_cache=decision_cache,
                )
                if candidate is None or payload is None:
                    continue

                payload["mandatory"] = True
                payload["counts_toward_coverage"] = True
                payload["coverage_key"] = requirement["coverage_key"]
                payload["reasons"] = dedupe_preserve_order(["coverage floor selected", *payload["reasons"]])[:8]
                routes.append(self._to_route_strategy({**payload, "priority": len(routes) + 1}))
                selected_urls.add(candidate.url)
                family_counts[family] += 1
                remaining_budget -= 1
                host_usage[payload["host_key"]] += 1
                host_limits[payload["host_key"]] = payload["host_limit"]
                path_usage[payload["path_key"]] += 1
                path_limits[payload["path_key"]] = payload["path_limit"]
                selected_depths[family].append(payload["depth"])
                selected_depth_limits[family] = payload["depth_limit"]
                status.covered = True
                status.selected_route_family = family
                status.selected_url = candidate.url
                selected = True
                break

            if not selected:
                status.skip_reason = "coverage_floor_unmet" if status.discovered else "not_discovered"
            coverage.append(status)

        while remaining_budget > 0:
            added = False
            for family in FAMILY_ORDER:
                if family_counts.get(family, 0) >= family_limits.get(family, 0):
                    continue
                candidate, payload = self._consume_candidate(
                    family=family,
                    origin=origin,
                    probe=probe,
                    buckets=buckets,
                    positions=positions,
                    selected_urls=selected_urls,
                    host_usage=host_usage,
                    path_usage=path_usage,
                    skip_registry=skip_registry,
                    decision_cache=decision_cache,
                )
                if candidate is None or payload is None:
                    continue

                routes.append(self._to_route_strategy({**payload, "priority": len(routes) + 1}))
                selected_urls.add(candidate.url)
                family_counts[family] += 1
                remaining_budget -= 1
                host_usage[payload["host_key"]] += 1
                host_limits[payload["host_key"]] = payload["host_limit"]
                path_usage[payload["path_key"]] += 1
                path_limits[payload["path_key"]] = payload["path_limit"]
                selected_depths[family].append(payload["depth"])
                selected_depth_limits[family] = payload["depth_limit"]
                added = True
                if remaining_budget <= 0:
                    break
            if not added:
                break

        self._append_budget_skip_reasons(
            buckets=buckets,
            positions=positions,
            selected_urls=selected_urls,
            family_counts=family_counts,
            family_limits=family_limits,
            remaining_budget=remaining_budget,
            skip_registry=skip_registry,
        )
        budget_accounting = self._build_budget_accounting(
            buckets=buckets,
            coverage=coverage,
            family_limits=family_limits,
            family_counts=family_counts,
            global_budget=global_budget,
            routes=routes,
            host_usage=host_usage,
            host_limits=host_limits,
            path_usage=path_usage,
            path_limits=path_limits,
            selected_depths=selected_depths,
            selected_depth_limits=selected_depth_limits,
            skip_registry=skip_registry,
        )
        return routes, budget_accounting, coverage

    def _build_crawl_map(
        self,
        site_url: str,
        candidates: dict[str, _CandidateState],
        routes: list[Any],
        budget_accounting: FactorySiteBudgetAccounting,
        coverage: list[FactorySiteCoverageStatus],
    ) -> FactorySiteCrawlMap:
        buckets = self._family_buckets(candidates)
        family_budget_map = {item.route_family: item for item in budget_accounting.family_budgets}
        coverage_by_key = self._coverage_status_map(coverage)
        sections: list[FactorySiteMapSection] = []
        all_sources: list[str] = []
        discovered_count = 0
        for family in FAMILY_ORDER:
            rule = FAMILY_RULES[family]
            bucket = buckets.get(family, [])
            family_budget = family_budget_map.get(family)
            if family == "search" and not bucket and family_budget is None:
                continue
            if bucket and family != "search":
                discovered_count += 1
            section_sources = dedupe_preserve_order(source for candidate in bucket for source in candidate.merged_sources())
            section_reasons = dedupe_preserve_order(reason for candidate in bucket for reason in candidate.merged_reasons())[:6]
            aggregate_coverage, required_floor, floor_met = self._family_coverage_state(
                family=family,
                coverage_by_key=coverage_by_key,
            )
            coverage_status = self._section_coverage_status(
                family=family,
                discovered=bool(bucket),
                planned_count=family_budget.planned_count if family_budget else 0,
                aggregate_coverage=aggregate_coverage,
                required_floor=required_floor,
            )
            sections.append(
                FactorySiteMapSection(
                    route_family=family,
                    section_guess=rule["section"],
                    crawl_budget=family_budget.budget_limit if family_budget else 0,
                    discovered_urls=[candidate.url for candidate in bucket[: self.max_ranked_candidates_per_family]],
                    planned_urls=list(family_budget.selected_urls) if family_budget else [],
                    planned_count=family_budget.planned_count if family_budget else 0,
                    skipped_count=family_budget.skipped_count if family_budget else 0,
                    discovery_sources=section_sources,
                    reasons=section_reasons if bucket else ["not discovered"],
                    skip_reasons=list(family_budget.skip_reasons) if family_budget else [],
                    required_floor=required_floor,
                    floor_met=floor_met,
                    coverage_status=coverage_status,
                )
            )
            all_sources.extend(section_sources)

        required_total = sum(1 for status in coverage if status.required)
        covered_total = sum(1 for status in coverage if status.required and status.covered)
        notes = [
            f"mapped {discovered_count}/{len(MANDATORY_ROUTE_FAMILIES)} mandatory route families",
            f"planned {budget_accounting.planned_routes}/{budget_accounting.global_budget} crawl routes",
            f"coverage floor met {covered_total}/{required_total} groups",
        ]
        if budget_accounting.skipped_routes:
            notes.append(f"skipped {len(budget_accounting.skipped_routes)} candidates due to caps or budget")
        if routes:
            top_routes = ", ".join(f"{route.route_family} -> {route.route_pattern}" for route in routes[:4])
            notes.append(f"top crawl order: {top_routes}")
        return FactorySiteCrawlMap(
            site_url=site_url,
            sections=sections,
            discovery_sources=dedupe_preserve_order(all_sources),
            coverage=coverage,
            budget_accounting=budget_accounting,
            notes=notes,
        )

    def _build_notes(self, candidate_site: str, probe: Any, crawl_map: FactorySiteCrawlMap, routes: list[Any]) -> list[str]:
        notes: list[str] = []
        if probe.status != "success":
            notes.append(f"probe failed for {candidate_site}: {probe.status}")
        elif probe.site_class in {"E", "F"}:
            notes.append(f"probe classified {candidate_site} as {probe.site_class}; crawl stays shallow")
        elif probe.site_class == "D":
            notes.append(f"probe classified {candidate_site} as JS-heavy; planner will mark browser routes")
        elif normalize_worth_crawling(getattr(probe, "worth_crawling", "false")) == "false":
            notes.append(f"probe says {candidate_site} is not worth crawling")

        notes.extend(crawl_map.notes[:4])
        if not routes:
            notes.append("planner did not produce executable crawl queue")
        elif crawl_map.budget_accounting is not None:
            notes.append(
                f"planned routes {crawl_map.budget_accounting.planned_routes}/{crawl_map.budget_accounting.global_budget}"
            )

        missing = [section.route_family for section in crawl_map.sections if not section.discovered_urls and section.route_family != "search"]
        if missing:
            notes.append("missing families: " + ", ".join(missing[:5]))
        uncovered_required = [status.coverage_key for status in crawl_map.coverage if status.required and not status.covered]
        if uncovered_required:
            notes.append("uncovered required groups: " + ", ".join(uncovered_required))
        return dedupe_preserve_order(notes)

    def _family_buckets(self, candidates: dict[str, _CandidateState]) -> dict[str, list[_CandidateState]]:
        buckets: dict[str, list[_CandidateState]] = defaultdict(list)
        for candidate in candidates.values():
            buckets[candidate.route_family].append(candidate)
        for family, bucket in buckets.items():
            bucket.sort(key=lambda item: (-self._candidate_rank_score(item), self._url_depth(item.url), item.url))
            buckets[family] = bucket[: self.max_ranked_candidates_per_family]
        return buckets

    def _family_budget_limit(self, family: str) -> int:
        if family in self.family_budget_overrides:
            return max(0, int(self.family_budget_overrides[family]))
        rule = FAMILY_RULES.get(family, {})
        return max(0, int(rule.get("budget", 0) or 0))

    def _resolve_global_budget(
        self,
        buckets: dict[str, list[_CandidateState]],
        family_limits: dict[str, int],
    ) -> int:
        if self.global_crawl_budget is not None:
            return self.global_crawl_budget
        discovered_budget = sum(family_limits[family] for family in FAMILY_ORDER if buckets.get(family))
        return max(1, min(DEFAULT_GLOBAL_CRAWL_BUDGET, discovered_budget or 1))

    def _coverage_requirements(self, buckets: dict[str, list[_CandidateState]]) -> list[dict[str, Any]]:
        requirements: list[dict[str, Any]] = []
        for item in COVERAGE_REQUIREMENTS:
            discovered = any(buckets.get(family) for family in item["route_families"])
            required = bool(item.get("required")) or bool(item.get("required_if_discovered") and discovered)
            requirements.append({**item, "discovered": discovered, "required": required})
        return requirements

    def _coverage_key_for_family(self, family: str) -> str:
        for item in COVERAGE_REQUIREMENTS:
            if family in item["route_families"]:
                return str(item["coverage_key"])
        return ""

    def _coverage_status_map(self, coverage: list[FactorySiteCoverageStatus]) -> dict[str, FactorySiteCoverageStatus]:
        return {
            status.coverage_key: status
            for status in coverage
            if status.coverage_key
        }

    def _family_coverage_state(
        self,
        *,
        family: str,
        coverage_by_key: dict[str, FactorySiteCoverageStatus],
    ) -> tuple[FactorySiteCoverageStatus | None, bool, bool]:
        coverage_key = self._coverage_key_for_family(family)
        aggregate_coverage = coverage_by_key.get(coverage_key) if coverage_key else None
        required_floor = bool(aggregate_coverage and aggregate_coverage.required)
        floor_met = bool(aggregate_coverage and aggregate_coverage.covered)
        return aggregate_coverage, required_floor, floor_met

    def _section_coverage_status(
        self,
        *,
        family: str,
        discovered: bool,
        planned_count: int,
        aggregate_coverage: FactorySiteCoverageStatus | None,
        required_floor: bool,
    ) -> str:
        if aggregate_coverage and aggregate_coverage.covered:
            if aggregate_coverage.selected_route_family == family:
                return "covered"
            if planned_count > 0:
                return "planned"
            return "group_covered"
        if not discovered:
            return "not_discovered"
        if planned_count > 0:
            return "planned"
        if required_floor:
            return "required_uncovered"
        return "discovered"

    def _host_limit_for_candidate(self, *, host_key: str, family: str) -> int:
        if host_key in self.host_caps:
            return max(1, int(self.host_caps[host_key]))
        if family in self.host_caps:
            return max(1, int(self.host_caps[family]))
        return DEFAULT_HOST_CAP

    def _default_path_pattern_limit(self, section: str) -> int:
        if section in MULTI_PATH_PATTERN_SECTIONS:
            return DEFAULT_MULTI_PATH_PATTERN_CAP
        return DEFAULT_SIMPLE_PATH_PATTERN_CAP

    def _path_pattern_limit_for_candidate(self, *, url: str, section: str) -> int:
        for pattern, _, limit in self.path_pattern_caps:
            if pattern.search(url):
                return max(1, int(limit))
        return self._default_path_pattern_limit(section)

    def _depth_limit_for_candidate(self, candidate: _CandidateState, suggested_limit: int | None) -> int:
        if candidate.route_family in self.depth_caps:
            return max(0, int(self.depth_caps[candidate.route_family]))
        if candidate.section_guess in self.depth_caps:
            return max(0, int(self.depth_caps[candidate.section_guess]))
        if suggested_limit is None:
            return 0
        return max(0, int(suggested_limit))

    def _candidate_plan_payload(self, origin: str, probe: Any, candidate: _CandidateState) -> dict[str, Any]:
        mode, mode_confidence, mode_reasons = self.strategy_selector.decide_mode(probe, candidate.section_guess)
        confidence = min(0.99, round((candidate.evidence_score / 100.0) + (mode_confidence * 0.35), 3))
        suggested_depth_limit, host_key, path_key = route_caps(
            site_url=origin,
            route_url=candidate.url,
            section=candidate.section_guess,
        )
        depth_limit = self._depth_limit_for_candidate(candidate, suggested_depth_limit)
        host_limit = self._host_limit_for_candidate(host_key=host_key, family=candidate.route_family)
        path_limit = self._path_pattern_limit_for_candidate(url=candidate.url, section=candidate.section_guess)
        coverage_key = self._coverage_key_for_family(candidate.route_family)
        reasons = dedupe_preserve_order(candidate.merged_reasons() + mode_reasons)[:8]
        return {
            "site_url": origin,
            "route_pattern": candidate.url,
            "section_guess": candidate.section_guess,
            "mode": mode,
            "confidence": confidence,
            "route_family": candidate.route_family,
            "crawl_budget": 1,
            "queue_name": queue_name_for_route(mode=mode, section=candidate.section_guess),
            "accounting_key": f"{host_key}:{candidate.route_family}" if host_key else candidate.route_family,
            "mandatory": False,
            "counts_toward_coverage": bool(coverage_key),
            "coverage_key": coverage_key,
            "skip_reason": "mode_skip" if mode == "skip" else "",
            "max_depth": depth_limit,
            "host_cap": host_key,
            "path_pattern_cap": path_key,
            "depth": self._url_depth(candidate.url),
            "depth_limit": depth_limit,
            "host_key": host_key,
            "host_limit": host_limit,
            "path_key": path_key,
            "path_limit": path_limit,
            "reasons": reasons,
            "discovery_sources": candidate.merged_sources(),
        }

    def _candidate_skip_details(self, payload: dict[str, Any]) -> tuple[str, list[str]] | None:
        if payload["mode"] == "skip":
            details = [payload["reasons"][0]] if payload["reasons"] else []
            return "mode_skip", details
        if payload["depth"] > payload["depth_limit"]:
            return "depth_cap_reached", [f"depth={payload['depth']}", f"depth_cap={payload['depth_limit']}"]
        return None

    def _consume_candidate(
        self,
        *,
        family: str,
        origin: str,
        probe: Any,
        buckets: dict[str, list[_CandidateState]],
        positions: dict[str, int],
        selected_urls: set[str],
        host_usage: dict[str, int],
        path_usage: dict[str, int],
        skip_registry: dict[str, FactorySiteSkippedRoute],
        decision_cache: dict[str, dict[str, Any]],
    ) -> tuple[_CandidateState | None, dict[str, Any] | None]:
        bucket = buckets.get(family, [])
        while positions.get(family, 0) < len(bucket):
            candidate = bucket[positions[family]]
            positions[family] += 1
            if candidate.url in selected_urls:
                continue
            payload = decision_cache.get(candidate.url)
            if payload is None:
                payload = self._candidate_plan_payload(origin, probe, candidate)
                decision_cache[candidate.url] = payload

            skip = self._candidate_skip_details(payload)
            if skip is not None:
                skip_reason, details = skip
                self._record_skipped_candidate(skip_registry, candidate, skip_reason, details)
                continue
            if host_usage[payload["host_key"]] >= payload["host_limit"]:
                self._record_skipped_candidate(
                    skip_registry,
                    candidate,
                    "host_cap_reached",
                    [f"host={payload['host_key']}", f"host_cap={payload['host_limit']}"],
                )
                continue
            if path_usage[payload["path_key"]] >= payload["path_limit"]:
                self._record_skipped_candidate(
                    skip_registry,
                    candidate,
                    "path_pattern_cap_reached",
                    [f"path_pattern={payload['path_key']}", f"path_pattern_cap={payload['path_limit']}"],
                )
                continue
            return candidate, payload
        return None, None

    def _record_skipped_candidate(
        self,
        skip_registry: dict[str, FactorySiteSkippedRoute],
        candidate: _CandidateState,
        skip_reason: str,
        details: list[str],
    ) -> None:
        if candidate.url in skip_registry:
            return
        skip_registry[candidate.url] = FactorySiteSkippedRoute(
            route_family=candidate.route_family,
            route_pattern=candidate.url,
            skip_reason=skip_reason,
            details=dedupe_preserve_order(details),
        )

    def _append_budget_skip_reasons(
        self,
        *,
        buckets: dict[str, list[_CandidateState]],
        positions: dict[str, int],
        selected_urls: set[str],
        family_counts: dict[str, int],
        family_limits: dict[str, int],
        remaining_budget: int,
        skip_registry: dict[str, FactorySiteSkippedRoute],
    ) -> None:
        for family in FAMILY_ORDER:
            bucket = buckets.get(family, [])
            budget_reason = ""
            if remaining_budget <= 0:
                budget_reason = "global_budget_exhausted"
            elif family_counts.get(family, 0) >= family_limits.get(family, 0):
                budget_reason = "family_budget_exhausted"
            if not budget_reason:
                continue
            for candidate in bucket[positions.get(family, 0) :]:
                if candidate.url in selected_urls:
                    continue
                details: list[str] = []
                if budget_reason == "family_budget_exhausted":
                    details.append(f"family_budget_limit={family_limits.get(family, 0)}")
                self._record_skipped_candidate(skip_registry, candidate, budget_reason, details)

    def _build_budget_accounting(
        self,
        *,
        buckets: dict[str, list[_CandidateState]],
        coverage: list[FactorySiteCoverageStatus],
        family_limits: dict[str, int],
        family_counts: dict[str, int],
        global_budget: int,
        routes: list[Any],
        host_usage: dict[str, int],
        host_limits: dict[str, int],
        path_usage: dict[str, int],
        path_limits: dict[str, int],
        selected_depths: dict[str, list[int]],
        selected_depth_limits: dict[str, int],
        skip_registry: dict[str, FactorySiteSkippedRoute],
    ) -> FactorySiteBudgetAccounting:
        family_rank = {family: index for index, family in enumerate(FAMILY_ORDER)}
        coverage_by_key = self._coverage_status_map(coverage)
        skipped_routes = sorted(
            skip_registry.values(),
            key=lambda item: (family_rank.get(item.route_family, 999), item.route_pattern),
        )
        family_budgets: list[FactorySiteFamilyBudget] = []
        for family in FAMILY_ORDER:
            bucket = buckets.get(family, [])
            selected_urls = [route.route_pattern for route in routes if route.route_family == family]
            family_skips = [item for item in skipped_routes if item.route_family == family]
            if family == "search" and not bucket and not selected_urls and not family_skips:
                continue
            budget_limit = family_limits.get(family, 0) if bucket or selected_urls else 0
            _, required_floor, floor_met = self._family_coverage_state(
                family=family,
                coverage_by_key=coverage_by_key,
            )
            family_budgets.append(
                FactorySiteFamilyBudget(
                    route_family=family,
                    section_guess=FAMILY_RULES[family]["section"],
                    discovered_count=len(bucket),
                    budget_limit=budget_limit,
                    planned_count=family_counts.get(family, 0),
                    remaining_budget=max(budget_limit - family_counts.get(family, 0), 0),
                    selected_urls=selected_urls,
                    skipped_count=len(family_skips),
                    skip_reasons=dedupe_preserve_order(item.skip_reason for item in family_skips)[:6],
                    required_floor=required_floor,
                    floor_met=floor_met,
                )
            )

        cap_usage: list[FactorySiteCapUsage] = []
        for family in sorted(selected_depths.keys(), key=lambda item: family_rank.get(item, 999)):
            cap_usage.append(
                FactorySiteCapUsage(
                    cap_type="depth",
                    cap_key=family,
                    limit=selected_depth_limits.get(family, 0),
                    used=max(selected_depths.get(family, [0])),
                )
            )
        for host_key in sorted(host_usage.keys()):
            cap_usage.append(
                FactorySiteCapUsage(
                    cap_type="host",
                    cap_key=host_key,
                    limit=host_limits.get(host_key, DEFAULT_HOST_CAP),
                    used=host_usage[host_key],
                )
            )
        for path_key in sorted(path_usage.keys()):
            cap_usage.append(
                FactorySiteCapUsage(
                    cap_type="path_pattern",
                    cap_key=path_key,
                    limit=path_limits.get(path_key, DEFAULT_SIMPLE_PATH_PATTERN_CAP),
                    used=path_usage[path_key],
                )
            )

        return FactorySiteBudgetAccounting(
            global_budget=global_budget,
            planned_routes=len(routes),
            remaining_budget=max(global_budget - len(routes), 0),
            family_budgets=family_budgets,
            cap_usage=cap_usage,
            skipped_routes=skipped_routes,
        )

    def _candidate_rank_score(self, candidate: _CandidateState) -> float:
        family_weight = float(FAMILY_RULES[candidate.route_family]["weight"])
        source_count_bonus = min(9.0, 1.35 * len(candidate.merged_sources()))
        token_bonus = min(8.0, 1.15 * len(candidate.merged_tokens()))
        depth_penalty = min(4.5, self._url_depth(candidate.url) * 0.55)
        guessed_penalty = 2.2 if candidate.guessed_only else 0.0
        return family_weight + candidate.family_match_score + candidate.evidence_score + source_count_bonus + token_bonus - depth_penalty - guessed_penalty

    def _register_candidate(
        self,
        candidates: dict[str, _CandidateState],
        *,
        url: str,
        origin: str,
        root_domain: str,
        discovery_source: str,
        source_page: str,
        anchor_text: str,
        context_text: str,
        guessed: bool,
    ) -> None:
        normalized = normalize_url(url)
        if not normalized:
            return
        parsed = urlparse(normalized)
        if guess_registered_domain(parsed.netloc) != root_domain:
            return
        if not parsed.scheme.startswith("http"):
            return

        family, section, family_score, matched_tokens, reasons = self._classify_candidate(
            normalized,
            anchor_text=anchor_text,
            context_text=context_text,
            discovery_source=discovery_source,
            source_page=source_page,
            origin=origin,
        )
        if not family:
            return

        source_bonus = SOURCE_BONUS.get(discovery_source, 1.0)
        if discovery_source in {"menu", "breadcrumbs"}:
            source_bonus += 0.6
        if source_page and source_page != normalized:
            source_bonus += 0.35

        state = candidates.get(normalized)
        if state is None:
            state = _CandidateState(
                url=normalized,
                route_family=family,
                section_guess=section,
                family_match_score=family_score,
                evidence_score=source_bonus,
                is_document=self._is_document_url(normalized),
                guessed_only=guessed,
            )
            candidates[normalized] = state
        else:
            current_weight = FAMILY_RULES[state.route_family]["weight"]
            new_weight = FAMILY_RULES[family]["weight"]
            if family_score > state.family_match_score or (family_score == state.family_match_score and new_weight > current_weight):
                state.route_family = family
                state.section_guess = section
            state.family_match_score = max(state.family_match_score, family_score)
            state.evidence_score += source_bonus
            state.is_document = state.is_document or self._is_document_url(normalized)
            state.guessed_only = state.guessed_only and guessed

        state.discovery_sources.append(discovery_source)
        if source_page:
            state.source_pages.append(source_page)
        state.reasons.extend(reasons)
        if anchor_text:
            state.reasons.append(f"anchor:{normalize_whitespace(anchor_text)[:80]}")
        state.matched_tokens.extend(matched_tokens)

    def _classify_candidate(
        self,
        url: str,
        *,
        anchor_text: str,
        context_text: str,
        discovery_source: str,
        source_page: str,
        origin: str,
    ) -> tuple[str, str, float, list[str], list[str]]:
        parsed = urlparse(url)
        path = normalize_whitespace(unquote(parsed.path)).lower()
        query = normalize_whitespace(unquote(parsed.query)).lower()
        host = normalize_whitespace(parsed.netloc).lower()
        anchor = normalize_whitespace(anchor_text).lower()
        context = normalize_whitespace(context_text).lower()
        source_hint = normalize_whitespace(discovery_source).lower()
        full_text = " ".join(part for part in (path, query, host, anchor, context, source_hint) if part)
        is_document = self._is_document_url(url)

        best_family = ""
        best_section = ""
        best_score = 0.0
        best_tokens: list[str] = []
        best_reasons: list[str] = []
        for family, rule in FAMILY_RULES.items():
            score = 0.0
            matched_tokens: list[str] = []
            reasons: list[str] = []
            for token in rule["keywords"]:
                token_lower = _normalized_keyword_token(token)
                if token_lower in path or token_lower in host:
                    score += 3.2
                    matched_tokens.append(token_lower)
                    reasons.append(f"path:{token_lower}")
                elif token_lower in anchor:
                    score += 3.8
                    matched_tokens.append(token_lower)
                    reasons.append(f"anchor:{token_lower}")
                elif token_lower in context or token_lower in query:
                    score += 2.4
                    matched_tokens.append(token_lower)
                    reasons.append(f"context:{token_lower}")

            if path in EXACT_PATH_HINTS.get(family, ()):
                score += 5.0
                reasons.append("canonical path match")

            if is_document and family == "files":
                score += 4.0
                reasons.append("direct document url")
            if is_document and family == "docs/certificates" and any(
                _normalized_keyword_token(token) in full_text
                for token in ("cert", "сертифик", "паспорт", "declar", "декларац")
            ):
                score += 4.5
                reasons.append("document looks like certificate or quality doc")
            if discovery_source == "search_form" and family == "search":
                score += 5.0
                reasons.append("on-site search form")
            if source_page == origin and family == ROOT_FAMILY and parsed.path in {"", "/"}:
                score += 1.8
                reasons.append("homepage fallback")

            if score > best_score:
                best_family = family
                best_section = rule["section"]
                best_score = score
                best_tokens = matched_tokens
                best_reasons = reasons

        if best_family:
            return best_family, best_section, best_score, best_tokens, best_reasons
        if is_document:
            return "files", "files", 4.0, ["document"], ["document extension"]
        if parsed.path in {"", "/"}:
            return ROOT_FAMILY, FAMILY_RULES[ROOT_FAMILY]["section"], 1.8, ["homepage"], ["homepage fallback"]
        return "", "", 0.0, [], []

    def _extract_sitemap_locs(self, response: Any) -> list[str]:
        raw = response.content or b""
        if raw[:2] == b"\x1f\x8b" or response.url.lower().endswith(".gz"):
            try:
                raw = gzip.decompress(raw)
            except OSError:
                pass
        text = self._decode_sitemap_text(raw, response)
        urls = [normalize_url(unescape(match)) for match in SITEMAP_LOC_RE.findall(text)]
        return [url for url in dedupe_preserve_order(urls) if url]

    def _decode_sitemap_text(self, raw: bytes, response: Any) -> str:
        fallback_text = ""
        for encoding in dedupe_preserve_order(
            [
                _validated_sitemap_encoding(getattr(response, "encoding", "")),
                _validated_sitemap_encoding(_declared_sitemap_encoding(raw)),
                "utf-8",
                "utf-8-sig",
                "utf-16",
                "windows-1251",
                "latin-1",
            ]
        ):
            if not encoding:
                continue
            try:
                text = raw.decode(encoding, errors="ignore")
            except (LookupError, UnicodeError):
                continue
            if not fallback_text:
                fallback_text = text
            lowered = text[:512].lower()
            if "<loc" in lowered or "<urlset" in lowered or "<sitemapindex" in lowered:
                return text
        return fallback_text

    def _fetch_response(self, url: str, *, source: str, request_cache: dict[str, Any]) -> Any | None:
        normalized = normalize_url(url)
        if not normalized:
            return None
        if normalized in request_cache:
            return request_cache[normalized]
        outcome = self.client.request(normalized, source=source, timeout=12)
        response = outcome.response if getattr(outcome, "ok", False) else None
        request_cache[normalized] = response
        return response

    def _looks_like_html(self, response: Any) -> bool:
        content_type = normalize_whitespace(response.headers.get("Content-Type", "").lower())
        if "html" in content_type:
            return True
        text = response.text or ""
        return "<html" in text[:800].lower()

    def _is_document_url(self, url: str) -> bool:
        lowered = url.lower()
        return any(lowered.endswith(ext) for ext in DOCUMENT_EXTENSIONS)

    def _url_depth(self, url: str) -> int:
        path = urlparse(url).path.strip("/")
        if not path:
            return 0
        return len([part for part in path.split("/") if part])

    def _empty_sections(self) -> list[FactorySiteMapSection]:
        return [
            FactorySiteMapSection(
                route_family=rule["family"],
                section_guess=rule["section"],
                crawl_budget=0,
                reasons=["not discovered"],
            )
            for rule in MANDATORY_ROUTE_FAMILIES
        ]

    def _to_route_strategy(self, payload: dict[str, Any]) -> Any:
        from app.site_intelligence.models import RouteStrategy

        return RouteStrategy(
            site_url=payload["site_url"],
            route_pattern=payload["route_pattern"],
            section_guess=payload["section_guess"],
            mode=payload["mode"],
            confidence=payload["confidence"],
            route_family=payload["route_family"],
            priority=payload["priority"],
            crawl_budget=payload["crawl_budget"],
            queue_name=payload.get("queue_name", ""),
            accounting_key=payload.get("accounting_key", ""),
            mandatory=bool(payload.get("mandatory", False)),
            counts_toward_coverage=bool(payload.get("counts_toward_coverage", False)),
            skip_reason=payload.get("skip_reason", ""),
            max_depth=payload.get("max_depth"),
            host_cap=payload.get("host_cap", ""),
            path_pattern_cap=payload.get("path_pattern_cap", ""),
            reasons=list(payload["reasons"]),
            discovery_sources=list(payload["discovery_sources"]),
        )


__all__ = ["FactorySitePlanner"]

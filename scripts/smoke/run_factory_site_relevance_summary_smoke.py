from __future__ import annotations

import copy
import sys
from pathlib import Path
from typing import Any

from requests import Response

if __package__ in {None, ""}:
    sys.path.append(str(Path(__file__).resolve().parents[2]))

from app.site_intelligence.common import dedupe_preserve_order
from app.site_intelligence.factory_site_parser.entrypoint import FactorySiteParser
from app.site_intelligence.factory_site_parser.fetch import FactorySiteFetchStage
from app.site_intelligence.factory_site_parser.models import (
    FactorySiteBudgetAccounting,
    FactorySiteCrawlMap,
    FactorySiteFamilyBudget,
    FactorySiteMapSection,
    FactorySiteOkvedProfile,
    FactorySiteParserCompany,
    FactorySitePlan,
    FactorySiteSkippedRoute,
)
from app.site_intelligence.fetcher import FetchResult, FetchTelemetry
from app.site_intelligence.models import ContentRecord, RouteStrategy, SiteProbe


SITE_URL = "https://deep-crawl-smoke.example"
SAMPLE_BASELINE = 2
HOST = "deep-crawl-smoke.example"
ABOUT_URL = f"{SITE_URL}/about"
CONTACTS_URL = f"{SITE_URL}/contacts"
PROCUREMENT_URL = f"{SITE_URL}/procurement/tenders"
DOCUMENT_URL = f"{SITE_URL}/files/tender-spec.pdf"
SAMPLE_DOCUMENT_URL = f"{SITE_URL}/files/sample-spec.pdf"
CONFLICT_DOCUMENT_URL = f"{SITE_URL}/files/conflict-spec.pdf"
DUPLICATE_ROUTE_DOCUMENT_URL_A = f"{SITE_URL}/files/duplicate-route-a.pdf"
DUPLICATE_ROUTE_DOCUMENT_URL_B = f"{SITE_URL}/files/duplicate-route-b.pdf"


def _assert(condition: bool, message: str) -> None:
    if not condition:
        raise RuntimeError(message)


def _record(
    *,
    source_type: str,
    url: str,
    section_guess: str,
    title: str,
    text: str,
    route_family: str,
    is_sample: bool,
    metadata: dict[str, Any] | None = None,
    evidence_ref: dict[str, Any] | None = None,
    trace: dict[str, Any] | None = None,
    content_fingerprint: str | None = None,
) -> ContentRecord:
    parser_trace = {
        "route_family": route_family,
        "is_sample": is_sample,
    }
    if isinstance(trace, dict):
        parser_trace.update(dict(trace.get("factory_site_parser", {})))

    merged_trace = dict(trace or {})
    merged_trace["factory_site_parser"] = parser_trace
    merged_trace.setdefault(
        "page_signal_taxonomy",
        {
            "route_family": route_family,
        },
    )
    return ContentRecord(
        company_id="7701234567",
        site_id=SITE_URL,
        source_type=source_type,
        source_url_or_file=url,
        section_guess=section_guess,
        title=title,
        text=text,
        metadata=dict(metadata or {}),
        evidence_ref=dict(evidence_ref or {}),
        site_url=SITE_URL,
        url=url,
        fetch_status="success",
        extraction_method="smoke_fixture",
        trace=merged_trace,
        content_fingerprint=str(content_fingerprint or ""),
    )


def _build_response(*, url: str, body: str | bytes, content_type: str) -> Response:
    response = Response()
    response.status_code = 200
    response.url = url
    payload = body.encode("utf-8") if isinstance(body, str) else body
    response._content = payload
    response.headers["Content-Type"] = content_type
    if content_type.startswith("text/") or "html" in content_type:
        response.encoding = "utf-8"
    return response


class FakeClient:
    progress_store = None

    def request(self, url: str, **kwargs: Any) -> Any:
        raise RuntimeError(f"Network must not be used in offline smoke: {url}")


class FakePlanner:
    def __init__(self) -> None:
        self.plan_payload = self._build_plan()

    def plan(self, company: FactorySiteParserCompany, *, max_sites: int | None = None) -> list[FactorySitePlan]:
        return [self.plan_payload]

    def _build_plan(self) -> FactorySitePlan:
        routes = [
            RouteStrategy(
                site_url=SITE_URL,
                route_pattern=ABOUT_URL,
                section_guess="about",
                mode="requests",
                confidence=0.9,
                route_family="company/about",
                priority=1,
            ),
            RouteStrategy(
                site_url=SITE_URL,
                route_pattern=CONTACTS_URL,
                section_guess="contacts",
                mode="requests",
                confidence=0.9,
                route_family="contacts",
                priority=2,
            ),
            RouteStrategy(
                site_url=SITE_URL,
                route_pattern=PROCUREMENT_URL,
                section_guess="procurement",
                mode="requests",
                confidence=0.95,
                route_family="procurement",
                priority=3,
            ),
            RouteStrategy(
                site_url=SITE_URL,
                route_pattern=DOCUMENT_URL,
                section_guess="documents",
                mode="requests",
                confidence=0.92,
                route_family="docs/certificates",
                priority=4,
            ),
        ]
        budget = FactorySiteBudgetAccounting(
            global_budget=4,
            planned_routes=4,
            remaining_budget=0,
            family_budgets=[
                FactorySiteFamilyBudget(
                    route_family="company/about",
                    section_guess="about",
                    discovered_count=1,
                    budget_limit=1,
                    planned_count=1,
                    remaining_budget=0,
                    selected_urls=[ABOUT_URL],
                    floor_met=True,
                ),
                FactorySiteFamilyBudget(
                    route_family="contacts",
                    section_guess="contacts",
                    discovered_count=1,
                    budget_limit=1,
                    planned_count=1,
                    remaining_budget=0,
                    selected_urls=[CONTACTS_URL],
                    floor_met=True,
                ),
                FactorySiteFamilyBudget(
                    route_family="procurement",
                    section_guess="procurement",
                    discovered_count=2,
                    budget_limit=1,
                    planned_count=1,
                    remaining_budget=0,
                    selected_urls=[PROCUREMENT_URL],
                    skipped_count=1,
                    skip_reasons=["global_budget_exhausted"],
                    required_floor=True,
                    floor_met=True,
                ),
                FactorySiteFamilyBudget(
                    route_family="docs/certificates",
                    section_guess="documents",
                    discovered_count=1,
                    budget_limit=1,
                    planned_count=1,
                    remaining_budget=0,
                    selected_urls=[DOCUMENT_URL],
                    required_floor=True,
                    floor_met=True,
                ),
            ],
            skipped_routes=[
                FactorySiteSkippedRoute(
                    route_family="search",
                    route_pattern=f"{SITE_URL}/search?q=tender",
                    skip_reason="global_budget_exhausted",
                    details=["budget_cap=4"],
                ),
                FactorySiteSkippedRoute(
                    route_family="surplus/realization",
                    route_pattern=f"{SITE_URL}/sale/archive",
                    skip_reason="policy_blocked_browser_only",
                    details=["manual_handoff_required=true"],
                ),
            ],
        )
        crawl_map = FactorySiteCrawlMap(
            site_url=SITE_URL,
            sections=[
                FactorySiteMapSection(
                    route_family="company/about",
                    section_guess="about",
                    crawl_budget=1,
                    discovered_urls=[ABOUT_URL],
                    planned_urls=[ABOUT_URL],
                    planned_count=1,
                    required_floor=True,
                    floor_met=True,
                    coverage_status="covered",
                ),
                FactorySiteMapSection(
                    route_family="contacts",
                    section_guess="contacts",
                    crawl_budget=1,
                    discovered_urls=[CONTACTS_URL],
                    planned_urls=[CONTACTS_URL],
                    planned_count=1,
                    required_floor=True,
                    floor_met=True,
                    coverage_status="covered",
                ),
                FactorySiteMapSection(
                    route_family="procurement",
                    section_guess="procurement",
                    crawl_budget=1,
                    discovered_urls=[PROCUREMENT_URL, f"{SITE_URL}/procurement/archive"],
                    planned_urls=[PROCUREMENT_URL],
                    planned_count=1,
                    skipped_count=1,
                    skip_reasons=["global_budget_exhausted"],
                    required_floor=True,
                    floor_met=True,
                    coverage_status="covered",
                ),
                FactorySiteMapSection(
                    route_family="docs/certificates",
                    section_guess="documents",
                    crawl_budget=1,
                    discovered_urls=[DOCUMENT_URL],
                    planned_urls=[DOCUMENT_URL],
                    planned_count=1,
                    required_floor=True,
                    floor_met=True,
                    coverage_status="covered",
                ),
            ],
            budget_accounting=budget,
            notes=["offline smoke crawl map"],
        )
        plan = FactorySitePlan(
            site_url=SITE_URL,
            probe=SiteProbe(
                url=SITE_URL,
                final_url=SITE_URL,
                status="success",
                site_class="B",
                worth_crawling="true",
                sampled_urls=[ABOUT_URL, CONTACTS_URL],
                html_ok=True,
            ),
            routes=routes,
            crawl_map=crawl_map,
            budget_accounting=budget,
            notes=["offline smoke plan"],
        )
        plan.access_state = ""
        plan.block_class = ""
        plan.anti_bot_reason = ""
        plan.breaker_mode = "normal"
        plan.manual_handoff_required = False
        plan.challenge_detected = False
        plan.session_reused = False
        return plan


class SingleRoutePlanner:
    def __init__(
        self,
        *,
        route_pattern: str,
        sampled_urls: list[str] | None = None,
        route_family: str = "docs/certificates",
        section_guess: str = "documents",
    ) -> None:
        self.route_pattern = route_pattern
        self.sampled_urls = list(sampled_urls or [])
        self.route_family = route_family
        self.section_guess = section_guess

    def plan(self, company: FactorySiteParserCompany, *, max_sites: int | None = None) -> list[FactorySitePlan]:
        route = RouteStrategy(
            site_url=SITE_URL,
            route_pattern=self.route_pattern,
            section_guess=self.section_guess,
            mode="requests",
            confidence=0.95,
            route_family=self.route_family,
            priority=1,
        )
        budget = FactorySiteBudgetAccounting(
            global_budget=1,
            planned_routes=1,
            remaining_budget=0,
            family_budgets=[
                FactorySiteFamilyBudget(
                    route_family=self.route_family,
                    section_guess=self.section_guess,
                    discovered_count=1,
                    budget_limit=1,
                    planned_count=1,
                    remaining_budget=0,
                    selected_urls=[self.route_pattern],
                    required_floor=True,
                    floor_met=True,
                )
            ],
        )
        crawl_map = FactorySiteCrawlMap(
            site_url=SITE_URL,
            sections=[
                FactorySiteMapSection(
                    route_family=self.route_family,
                    section_guess=self.section_guess,
                    crawl_budget=1,
                    discovered_urls=[self.route_pattern],
                    planned_urls=[self.route_pattern],
                    planned_count=1,
                    required_floor=True,
                    floor_met=True,
                    coverage_status="covered",
                )
            ],
            budget_accounting=budget,
            notes=["single-route smoke crawl map"],
        )
        plan = FactorySitePlan(
            site_url=SITE_URL,
            probe=SiteProbe(
                url=SITE_URL,
                final_url=SITE_URL,
                status="success",
                site_class="B",
                worth_crawling="true",
                sampled_urls=list(self.sampled_urls),
                html_ok=True,
            ),
            routes=[route],
            crawl_map=crawl_map,
            budget_accounting=budget,
            notes=["single-route smoke plan"],
        )
        plan.access_state = ""
        plan.block_class = ""
        plan.anti_bot_reason = ""
        plan.breaker_mode = "normal"
        plan.manual_handoff_required = False
        plan.challenge_detected = False
        plan.session_reused = False
        return [plan]


def _build_plan_for_routes(
    route_specs: list[dict[str, str]],
    *,
    sampled_urls: list[str] | None = None,
    notes_prefix: str = "custom",
) -> FactorySitePlan:
    routes: list[RouteStrategy] = []
    family_counts: dict[tuple[str, str], list[str]] = {}
    for index, spec in enumerate(route_specs, start=1):
        route_pattern = spec["route_pattern"]
        route_family = spec.get("route_family", "docs/certificates")
        section_guess = spec.get("section_guess", "documents")
        routes.append(
            RouteStrategy(
                site_url=SITE_URL,
                route_pattern=route_pattern,
                section_guess=section_guess,
                mode="requests",
                confidence=0.95,
                route_family=route_family,
                priority=index,
            )
        )
        family_counts.setdefault((route_family, section_guess), []).append(route_pattern)

    family_budgets = [
        FactorySiteFamilyBudget(
            route_family=route_family,
            section_guess=section_guess,
            discovered_count=len(urls),
            budget_limit=len(urls),
            planned_count=len(urls),
            remaining_budget=0,
            selected_urls=list(urls),
            required_floor=True,
            floor_met=True,
        )
        for (route_family, section_guess), urls in family_counts.items()
    ]
    sections = [
        FactorySiteMapSection(
            route_family=route_family,
            section_guess=section_guess,
            crawl_budget=len(urls),
            discovered_urls=list(urls),
            planned_urls=list(urls),
            planned_count=len(urls),
            required_floor=True,
            floor_met=True,
            coverage_status="covered",
        )
        for (route_family, section_guess), urls in family_counts.items()
    ]
    budget = FactorySiteBudgetAccounting(
        global_budget=len(routes),
        planned_routes=len(routes),
        remaining_budget=0,
        family_budgets=family_budgets,
    )
    crawl_map = FactorySiteCrawlMap(
        site_url=SITE_URL,
        sections=sections,
        budget_accounting=budget,
        notes=[f"{notes_prefix} smoke crawl map"],
    )
    plan = FactorySitePlan(
        site_url=SITE_URL,
        probe=SiteProbe(
            url=SITE_URL,
            final_url=SITE_URL,
            status="success",
            site_class="B",
            worth_crawling="true",
            sampled_urls=list(sampled_urls or []),
            html_ok=True,
        ),
        routes=routes,
        crawl_map=crawl_map,
        budget_accounting=budget,
        notes=[f"{notes_prefix} smoke plan"],
    )
    plan.access_state = ""
    plan.block_class = ""
    plan.anti_bot_reason = ""
    plan.breaker_mode = "normal"
    plan.manual_handoff_required = False
    plan.challenge_detected = False
    plan.session_reused = False
    return plan


class StaticPlanPlanner:
    def __init__(self, plan: FactorySitePlan) -> None:
        self.plan_payload = plan

    def plan(self, company: FactorySiteParserCompany, *, max_sites: int | None = None) -> list[FactorySitePlan]:
        return [copy.deepcopy(self.plan_payload)]


class LargePlanPlanner(StaticPlanPlanner):
    def __init__(self, *, route_count: int) -> None:
        _assert(route_count >= 2, "LargePlanPlanner requires at least two routes.")
        route_specs = [
            {
                "route_pattern": ABOUT_URL,
                "route_family": "company/about",
                "section_guess": "about",
            }
        ]
        route_specs.extend(
            {
                "route_pattern": f"{SITE_URL}/oversized/route-{index}",
                "route_family": f"oversized/family-{index}",
                "section_guess": "oversized",
            }
            for index in range(1, route_count)
        )
        super().__init__(_build_plan_for_routes(route_specs, notes_prefix="large-plan"))


class FakeFetcher:
    def __init__(self) -> None:
        self.last_fetch_result: FetchResult | None = None
        self.last_fetch_telemetry: FetchTelemetry | None = None
        self._responses = {
            ABOUT_URL: _build_response(
                url=ABOUT_URL,
                content_type="text/html; charset=utf-8",
                body=(
                    "<html><head><title>About factory</title></head><body>"
                    "<h1>About factory</h1>"
                    "<p>Factory history and production overview for industrial machinery plant.</p>"
                    "</body></html>"
                ),
            ),
            CONTACTS_URL: _build_response(
                url=CONTACTS_URL,
                content_type="text/html; charset=utf-8",
                body=(
                    "<html><head><title>Contacts</title></head><body>"
                    "<h1>Contacts</h1>"
                    "<p>Main office contacts, requisites, shipping address and warehouse phone.</p>"
                    "</body></html>"
                ),
            ),
            PROCUREMENT_URL: _build_response(
                url=PROCUREMENT_URL,
                content_type="text/html; charset=utf-8",
                body=(
                    "<html><head><title>Procurement tenders</title></head><body>"
                    "<h1>Procurement tenders</h1>"
                    "<p>Tender lot notice procurement auction steel fabrication 15.03.2026 "
                    "supplier documentation and qualification sheet.</p>"
                    f"<a href=\"{DOCUMENT_URL}\">Tender specification PDF</a>"
                    "</body></html>"
                ),
            ),
            DOCUMENT_URL: _build_response(
                url=DOCUMENT_URL,
                content_type="application/pdf",
                body=b"%PDF-1.4 offline smoke tender specification bytes\n",
            ),
            SAMPLE_DOCUMENT_URL: _build_response(
                url=SAMPLE_DOCUMENT_URL,
                content_type="application/pdf",
                body=b"%PDF-1.4 offline smoke sample specification bytes\n",
            ),
            CONFLICT_DOCUMENT_URL: _build_response(
                url=CONFLICT_DOCUMENT_URL,
                content_type="application/pdf",
                body=b"%PDF-1.4 offline smoke conflicting specification bytes\n",
            ),
        }

    def fetch(
        self,
        url: str,
        mode: str,
        *,
        route_family: str = "",
        section_name: str = "",
    ) -> tuple[Response | None, str, list[str]]:
        response = self._responses.get(url)
        if response is None:
            self.last_fetch_telemetry = FetchTelemetry(
                host=HOST,
                url=url,
                fetch_mode=mode,
                route_family=route_family,
                section_name=section_name,
                status="missing_fixture",
                access_state="blocked",
                transport_selected=mode,
                transport_final=mode,
                escalation_reason="missing_fixture",
            )
            self.last_fetch_result = FetchResult(
                response=None,
                status="missing_fixture",
                notes=["offline fixture missing"],
                access_state="blocked",
                route_family=route_family,
                section_name=section_name,
                transport_selected=mode,
                transport_final=mode,
                escalation_reason="missing_fixture",
                attempts=[self.last_fetch_telemetry],
            )
            return None, "missing_fixture", ["offline fixture missing"]

        self.last_fetch_telemetry = FetchTelemetry(
            host=HOST,
            url=url,
            fetch_mode=mode,
            route_family=route_family,
            section_name=section_name,
            status="success",
            http_status=response.status_code,
            access_state="completed_with_content",
            transport_selected=mode,
            transport_final=mode,
        )
        self.last_fetch_result = FetchResult(
            response=response,
            status="success",
            notes=["offline fixture hit"],
            access_state="completed_with_content",
            completed_with_content=True,
            route_family=route_family,
            section_name=section_name,
            transport_selected=mode,
            transport_final=mode,
            attempts=[self.last_fetch_telemetry],
        )
        return response, "success", ["offline fixture hit"]


class FakeNormalizer:
    def normalize_html_record(
        self,
        *,
        company_id: str,
        site_url: str,
        route: RouteStrategy,
        response: Response | None,
        fetch_status: str,
        notes: list[str],
    ) -> ContentRecord:
        is_sample = route.route_pattern in {ABOUT_URL, CONTACTS_URL}
        title = ""
        text = ""
        if response is not None:
            title = response.url.rstrip("/").rsplit("/", 1)[-1].replace("-", " ").title()
            text = response.text
        return _record(
            source_type="html",
            url=response.url if response is not None else route.route_pattern,
            section_guess=route.section_guess,
            title=title or route.section_guess.title(),
            text=text or "offline smoke placeholder",
            route_family=route.route_family,
            is_sample=is_sample,
            metadata={"normalizer_notes": list(notes)},
            evidence_ref={"kind": "html_page"},
            trace={
                "factory_site_parser": {
                    "source_page": route.route_pattern,
                    "discovery_source": "sampled_route" if is_sample else "planned_route",
                }
            },
        )


class FakeDocumentsStage:
    def build_collector(self, company_id: str) -> dict[str, str]:
        return {"company_id": company_id}

    def collect_direct_response(
        self,
        *,
        collector: Any,
        company_id: str,
        site_url: str,
        response: Response | None,
        source_url: str,
        referrer_url: str,
        section_guess: str,
        route_family: str = "",
    ) -> list[ContentRecord]:
        if response is None:
            return []
        content_type = str(response.headers.get("Content-Type", "") or "").lower()
        if "pdf" not in content_type:
            return []
        if response.url == SAMPLE_DOCUMENT_URL:
            return [
                _record(
                    source_type="pdf",
                    url=response.url,
                    section_guess=section_guess,
                    title="sample-spec.pdf",
                    text=(
                        "Sample tender document with procurement contacts, sample route marker, "
                        "supplier qualification sheet and delivery terms."
                    ),
                    route_family=route_family or "docs/certificates",
                    is_sample=False,
                    metadata={
                        "attachment_provenance": {
                            "route_origin": "planned",
                            "from_sample": False,
                            "source_kind": "document",
                            "route_family": route_family or "docs/certificates",
                            "source_page": PROCUREMENT_URL,
                            "discovery_source": "planner_direct_document",
                        }
                    },
                    evidence_ref={
                        "kind": "document_file",
                        "attachment_provenance": {
                            "route_origin": "planned",
                            "from_sample": False,
                            "source_kind": "document",
                            "route_family": route_family or "docs/certificates",
                            "source_page": PROCUREMENT_URL,
                            "discovery_source": "planner_direct_document",
                        },
                    },
                    trace={
                        "factory_site_parser": {
                            "source_page": PROCUREMENT_URL,
                            "discovery_source": "planner_direct_document",
                        },
                        "document_queue": {
                            "status": "acquired",
                            "source_page": PROCUREMENT_URL,
                            "discovery_source": "planner_direct_document",
                        },
                    },
                )
            ]
        if response.url == CONFLICT_DOCUMENT_URL:
            return [
                _record(
                    source_type="pdf",
                    url=response.url,
                    section_guess=section_guess,
                    title="conflict-spec.pdf",
                    text=(
                        "Conflicting provenance document with procurement contacts, lot number 52, "
                        "supplier requirements and delivery terms."
                    ),
                    route_family=route_family or "docs/certificates",
                    is_sample=True,
                    metadata={
                        "attachment_provenance": {
                            "route_origin": "sample",
                            "from_sample": True,
                            "source_kind": "document",
                            "route_family": route_family or "docs/certificates",
                            "source_page": ABOUT_URL,
                            "discovery_source": "sampled_route",
                        }
                    },
                    evidence_ref={
                        "kind": "document_file",
                        "attachment_provenance": {
                            "route_origin": "sample",
                            "from_sample": True,
                            "source_kind": "document",
                            "route_family": route_family or "docs/certificates",
                            "source_page": ABOUT_URL,
                            "discovery_source": "sampled_route",
                        },
                    },
                    trace={
                        "factory_site_parser": {
                            "source_page": ABOUT_URL,
                            "discovery_source": "sampled_route",
                        },
                        "document_queue": {
                            "status": "acquired",
                            "source_page": ABOUT_URL,
                            "discovery_source": "sampled_route",
                        },
                    },
                )
            ]
        return [
            _record(
                source_type="pdf",
                url=response.url,
                section_guess=section_guess,
                title="tender-spec.pdf",
                text=(
                    "Tender documentation with procurement contacts, lot number 42, "
                    "supplier qualification sheet and procurement deadline 15.03.2026."
                ),
                route_family=route_family or "docs/certificates",
                is_sample=False,
                metadata={
                    "document_queue": {
                        "status": "acquired",
                        "source_page": PROCUREMENT_URL,
                        "discovery_source": "planner_direct_document",
                    }
                },
                evidence_ref={
                    "kind": "document_file",
                    "attachment_provenance": {
                        "route_family": route_family or "docs/certificates",
                        "source_page": PROCUREMENT_URL,
                        "discovery_source": "planner_direct_document",
                    },
                },
                trace={
                    "factory_site_parser": {
                        "source_page": PROCUREMENT_URL,
                        "discovery_source": "planner_direct_document",
                    },
                    "document_queue": {
                        "status": "acquired",
                        "source_page": PROCUREMENT_URL,
                        "discovery_source": "planner_direct_document",
                    },
                },
            )
        ]

    def collect_html_attachments(
        self,
        *,
        collector: Any,
        company_id: str,
        site_url: str,
        response: Response | None,
        fetch_status: str,
        section_guess: str,
        route_family: str = "",
    ) -> list[ContentRecord]:
        return []


class FakeOkvedMatcher:
    def match_records(
        self,
        company: FactorySiteParserCompany,
        content_records: list[ContentRecord],
    ) -> tuple[FactorySiteOkvedProfile, list[Any]]:
        return FactorySiteOkvedProfile(), []


class LegacyListFetchStage:
    def fetch(
        self,
        company: FactorySiteParserCompany,
        plans: list[FactorySitePlan],
        *,
        dry_run: bool = False,
    ) -> list[ContentRecord]:
        return [
            _record(
                source_type="html",
                url=ABOUT_URL,
                section_guess="about",
                title="About factory",
                text="Factory overview and machine-building profile.",
                route_family="company/about",
                is_sample=False,
                trace={
                    "factory_site_parser": {
                        "crawl": {
                            "site_url": SITE_URL,
                            "route_pattern": ABOUT_URL,
                            "route_family": "company/about",
                            "route_origin": "planned",
                            "from_sample": False,
                            "source_kind": "page",
                        }
                    }
                },
            ),
            _record(
                source_type="pdf",
                url=DOCUMENT_URL,
                section_guess="documents",
                title="legacy-spec.pdf",
                text="Legacy fetch-stage document with procurement evidence.",
                route_family="docs/certificates",
                is_sample=False,
                trace={
                    "factory_site_parser": {
                        "crawl": {
                            "site_url": SITE_URL,
                            "route_pattern": DOCUMENT_URL,
                            "route_family": "docs/certificates",
                            "route_origin": "planned",
                            "from_sample": False,
                            "source_kind": "document",
                        }
                    }
                },
            ),
        ]


class CrossRouteDuplicateFetchStage:
    def fetch(
        self,
        company: FactorySiteParserCompany,
        plans: list[FactorySitePlan],
        *,
        dry_run: bool = False,
    ) -> tuple[list[ContentRecord], dict[str, Any]]:
        shared_fingerprint = "cross-route-duplicate-fingerprint"
        records = [
            _record(
                source_type="pdf",
                url=DUPLICATE_ROUTE_DOCUMENT_URL_A,
                section_guess="documents",
                title="duplicate-a.pdf",
                text="Cross-route duplicate document body.",
                route_family="docs/certificates",
                is_sample=False,
                content_fingerprint=shared_fingerprint,
                trace={
                    "factory_site_parser": {
                        "crawl": {
                            "site_url": SITE_URL,
                            "route_pattern": DUPLICATE_ROUTE_DOCUMENT_URL_A,
                            "route_family": "docs/certificates",
                            "route_origin": "planned",
                            "from_sample": False,
                            "source_kind": "document",
                        }
                    }
                },
            ),
            _record(
                source_type="pdf",
                url=DUPLICATE_ROUTE_DOCUMENT_URL_B,
                section_guess="documents",
                title="duplicate-b.pdf",
                text="Cross-route duplicate document body.",
                route_family="docs/certificates",
                is_sample=False,
                content_fingerprint=shared_fingerprint,
                trace={
                    "factory_site_parser": {
                        "crawl": {
                            "site_url": SITE_URL,
                            "route_pattern": DUPLICATE_ROUTE_DOCUMENT_URL_B,
                            "route_family": "docs/certificates",
                            "route_origin": "planned",
                            "from_sample": False,
                            "source_kind": "document",
                        }
                    }
                },
            ),
        ]
        executed_routes = []
        for route_pattern in (DUPLICATE_ROUTE_DOCUMENT_URL_A, DUPLICATE_ROUTE_DOCUMENT_URL_B):
            executed_routes.append(
                {
                    "site_url": SITE_URL,
                    "route_pattern": route_pattern,
                    "route_family": "docs/certificates",
                    "section_guess": "documents",
                    "route_origin": "planned",
                    "from_sample": False,
                    "status": "executed",
                    "page_records": 0,
                    "document_records": 1,
                    "record_count": 1,
                    "content_fingerprint": shared_fingerprint,
                    "content_fingerprints": [shared_fingerprint],
                    "raw_page_records": 0,
                    "raw_document_records": 1,
                    "raw_record_count": 1,
                    "raw_content_fingerprints": [shared_fingerprint],
                }
            )
        crawl_execution = {
            "visited_route_families": ["docs/certificates"],
            "page_records": 0,
            "document_records": 2,
            "record_count": 2,
            "executed_route_count": 2,
            "skipped_route_count": 0,
            "raw_page_records": 0,
            "raw_document_records": 2,
            "raw_record_count": 2,
            "non_sample_record_fingerprints": [shared_fingerprint],
            "raw_non_sample_record_fingerprints": [shared_fingerprint],
            "executed_routes": executed_routes,
            "skipped_routes": [],
            "document_queue": [
                {
                    "site_url": SITE_URL,
                    "route_pattern": DUPLICATE_ROUTE_DOCUMENT_URL_A,
                    "route_family": "docs/certificates",
                    "route_origin": "planned",
                    "from_sample": False,
                    "status": "crawled",
                    "content_fingerprint": shared_fingerprint,
                }
            ],
            "policy_skips": [],
            "budget": {
                "sites": [
                    {
                        "site_url": SITE_URL,
                        "budget": {
                            "planned_routes": 2,
                        },
                    }
                ],
                "executed_routes": 2,
                "skipped_routes": 0,
            },
            "sites": [
                {
                    "site_url": SITE_URL,
                    "page_records": 0,
                    "document_records": 2,
                    "record_count": 2,
                    "raw_page_records": 0,
                    "raw_document_records": 2,
                    "raw_record_count": 2,
                    "visited_route_families": ["docs/certificates"],
                    "executed_routes": executed_routes,
                    "skipped_routes": [],
                    "executed_route_count": 2,
                    "skipped_route_count": 0,
                }
            ],
            "runtime_summary": {
                "page_records": 0,
                "document_records": 2,
                "record_count": 2,
                "executed_route_count": 2,
                "skipped_route_count": 0,
                "document_queue_count": 1,
                "visited_route_families": ["docs/certificates"],
            },
            "dry_run": dry_run,
        }
        return records, crawl_execution


class StalePrefilledCountsFetchStage:
    def fetch(
        self,
        company: FactorySiteParserCompany,
        plans: list[FactorySitePlan],
        *,
        dry_run: bool = False,
    ) -> tuple[list[ContentRecord], dict[str, Any]]:
        record = _record(
            source_type="html",
            url=ABOUT_URL,
            section_guess="about",
            title="About factory",
            text="Single canonical page for stale-count repro.",
            route_family="company/about",
            is_sample=False,
            trace={
                "factory_site_parser": {
                    "crawl": {
                        "site_url": SITE_URL,
                        "route_pattern": ABOUT_URL,
                        "route_family": "company/about",
                        "route_origin": "planned",
                        "from_sample": False,
                        "source_kind": "page",
                    }
                }
            },
        )
        skip_entry = {
            "site_url": SITE_URL,
            "route_pattern": "__aggregated_execution_tail__",
            "route_origin": "planned",
            "reason": "max_routes_per_site",
            "phase": "execution",
            "aggregated": True,
            "skipped_route_count": 4,
        }
        executed_route = {
            "site_url": SITE_URL,
            "route_pattern": ABOUT_URL,
            "route_family": "company/about",
            "section_guess": "about",
            "route_origin": "planned",
            "from_sample": False,
            "status": "executed",
            "page_records": 1,
            "document_records": 0,
            "record_count": 1,
            "content_fingerprint": record.content_fingerprint,
            "content_fingerprints": [record.content_fingerprint],
        }
        crawl_execution = {
            "visited_route_families": ["company/about"],
            "page_records": 77,
            "document_records": 55,
            "record_count": 132,
            "executed_route_count": 99,
            "skipped_route_count": 88,
            "executed_routes": [executed_route],
            "skipped_routes": [skip_entry],
            "document_queue": [],
            "policy_skips": [],
            "budget": {
                "sites": [
                    {
                        "site_url": SITE_URL,
                        "budget": {
                            "planned_routes": 5,
                        },
                    }
                ],
                "executed_routes": 42,
                "skipped_routes": 24,
            },
            "sites": [
                {
                    "site_url": SITE_URL,
                    "page_records": 9,
                    "document_records": 9,
                    "record_count": 18,
                    "visited_route_families": ["company/about"],
                    "executed_routes": [executed_route],
                    "skipped_routes": [skip_entry],
                    "executed_route_count": 11,
                    "skipped_route_count": 12,
                }
            ],
            "runtime_summary": {
                "page_records": 77,
                "document_records": 55,
                "record_count": 132,
                "executed_route_count": 99,
                "skipped_route_count": 88,
                "document_queue_count": 0,
                "visited_route_families": ["company/about"],
            },
            "dry_run": dry_run,
        }
        return [record], crawl_execution


def _route_family_from_record(record: ContentRecord) -> str:
    taxonomy = record.trace.get("page_signal_taxonomy", {})
    if isinstance(taxonomy, dict):
        route_family = str(taxonomy.get("route_family", "") or "").strip().lower()
        if route_family:
            return route_family
    parser_trace = record.trace.get("factory_site_parser", {})
    if isinstance(parser_trace, dict):
        route_family = str(parser_trace.get("route_family", "") or "").strip().lower()
        if route_family:
            return route_family
    return ""


def _require_mapping_attr(result: Any, attr_name: str) -> dict[str, Any]:
    payload = getattr(result, attr_name, None)
    _assert(isinstance(payload, dict), f"Expected parser result to expose {attr_name} as a dict.")
    return payload


def _require_int_attr(result: Any, attr_name: str) -> int:
    payload = getattr(result, attr_name, None)
    _assert(isinstance(payload, int), f"Expected parser result to expose {attr_name} as int.")
    return payload


def _require_list_attr(result: Any, attr_name: str) -> list[Any]:
    payload = getattr(result, attr_name, None)
    _assert(isinstance(payload, list), f"Expected parser result to expose {attr_name} as list.")
    return payload


def _require_non_empty_string(value: Any, label: str) -> str:
    text = str(value or "").strip()
    _assert(text, f"{label} must be populated.")
    return text


def _require_notes(result: Any) -> list[str]:
    notes = getattr(result, "notes", None)
    _assert(isinstance(notes, list), "Expected parser result to expose notes as list.")
    normalized_notes: list[str] = []
    for index, item in enumerate(notes, start=1):
        _assert(isinstance(item, str), f"result.notes[{index}] must be a string.")
        normalized_notes.append(item.strip())
    return normalized_notes


def _records_for_route(result: Any, route_pattern: str) -> list[ContentRecord]:
    matched: list[ContentRecord] = []
    for record in getattr(result, "content_records", []):
        crawl_trace = getattr(record, "trace", {}).get("factory_site_parser", {}).get("crawl", {})
        if not isinstance(crawl_trace, dict):
            continue
        if str(crawl_trace.get("route_pattern") or "").strip() == route_pattern:
            matched.append(record)
    return matched


def _records_for_site(result: Any, site_url: str) -> list[ContentRecord]:
    return [
        record
        for record in getattr(result, "content_records", [])
        if str(getattr(record, "site_url", "") or "").strip() == site_url
    ]


def _record_by_fingerprint(result: Any, fingerprint: str) -> ContentRecord:
    for record in getattr(result, "content_records", []):
        if str(getattr(record, "content_fingerprint", "") or "").strip() == fingerprint:
            return record
    raise RuntimeError(f"Expected content record for fingerprint {fingerprint}.")


def _records_by_fingerprints(result: Any, fingerprints: list[str]) -> list[ContentRecord]:
    return [_record_by_fingerprint(result, fingerprint) for fingerprint in fingerprints]


def _record_source_kind(record: ContentRecord) -> str:
    source_type = str(getattr(record, "source_type", "") or "").strip().lower()
    return "page" if source_type == "html" else "document"


def _canonical_route_mapping(result: Any, route_payload: dict[str, Any]) -> tuple[list[ContentRecord], list[str]]:
    fingerprint_values = route_payload.get("content_fingerprints")
    if isinstance(fingerprint_values, list):
        canonical_fingerprints = dedupe_preserve_order(
            str(value or "").strip()
            for value in fingerprint_values
            if str(value or "").strip()
        )
    else:
        canonical_fingerprints = []
    singular_fingerprint = str(route_payload.get("content_fingerprint") or "").strip()
    if singular_fingerprint and singular_fingerprint not in canonical_fingerprints:
        canonical_fingerprints.insert(0, singular_fingerprint)
    if canonical_fingerprints:
        return _records_by_fingerprints(result, canonical_fingerprints), canonical_fingerprints

    route_pattern = str(route_payload.get("route_pattern") or "").strip()
    route_records = _records_for_route(result, route_pattern)
    route_fingerprints = dedupe_preserve_order(
        str(getattr(record, "content_fingerprint", "") or "").strip()
        for record in route_records
        if str(getattr(record, "content_fingerprint", "") or "").strip()
    )
    return route_records, route_fingerprints


def _require_runtime_summary(crawl_execution: dict[str, Any]) -> dict[str, Any]:
    runtime_summary = crawl_execution.get("runtime_summary")
    _assert(isinstance(runtime_summary, dict) and runtime_summary, "Expected crawl_execution.runtime_summary.")
    return runtime_summary


def _require_parser_result_payloads(result: Any) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any], int, int, list[Any]]:
    relevance_summary = _require_mapping_attr(result, "relevance_summary")
    lead_assembly = _require_mapping_attr(result, "lead_assembly")
    crawl_execution = _require_mapping_attr(result, "crawl_execution")
    page_records = _require_int_attr(result, "page_records")
    document_records = _require_int_attr(result, "document_records")
    visited_route_families = _require_list_attr(result, "visited_route_families")

    skipped_routes = list(crawl_execution.get("skipped_routes", []))
    for index, item in enumerate(skipped_routes, start=1):
        _assert(isinstance(item, dict), f"skipped_routes[{index}] must be an object.")
        _require_non_empty_string(item.get("reason"), f"skipped_routes[{index}].reason")

    _require_runtime_summary(crawl_execution)
    return (
        relevance_summary,
        lead_assembly,
        crawl_execution,
        page_records,
        document_records,
        visited_route_families,
    )


def _assert_result_contract(
    result: Any,
    relevance_summary: dict[str, Any],
    lead_assembly: dict[str, Any],
    crawl_execution: dict[str, Any],
    *,
    page_records: int,
    document_records: int,
    visited_route_families: list[Any],
) -> None:
    _assert(
        page_records == relevance_summary.get("page_records") == crawl_execution.get("page_records"),
        "page_records must stay consistent across parser result, relevance_summary and crawl_execution.",
    )
    _assert(
        document_records == relevance_summary.get("document_records") == crawl_execution.get("document_records"),
        "document_records must stay consistent across parser result, relevance_summary and crawl_execution.",
    )
    _assert(
        visited_route_families == list(crawl_execution.get("visited_route_families", [])),
        "visited_route_families must stay consistent across parser result and crawl_execution.",
    )
    _assert(
        isinstance(lead_assembly.get("lead_evidence", []), list),
        "Parser result must expose lead_assembly.lead_evidence.",
    )
    canonical_record_count = len(getattr(result, "content_records", []))
    _assert(
        page_records + document_records == canonical_record_count,
        "Parser result page/document counters must match canonical content_records.",
    )
    if "record_count" in relevance_summary:
        _assert(
            relevance_summary.get("record_count") == canonical_record_count,
            "relevance_summary.record_count must match canonical content_records.",
        )
    if "record_count" in crawl_execution:
        _assert(
            crawl_execution.get("record_count") == canonical_record_count,
            "crawl_execution.record_count must match canonical content_records.",
        )


def _assert_runtime_summary(
    result: Any,
    crawl_execution: dict[str, Any],
    *,
    page_records: int,
    document_records: int,
    visited_route_families: list[Any],
) -> None:
    executed_routes = list(crawl_execution.get("executed_routes", []))
    skipped_routes = list(crawl_execution.get("skipped_routes", []))
    document_queue = list(crawl_execution.get("document_queue", []))
    sites = list(crawl_execution.get("sites", []))
    runtime_summary = _require_runtime_summary(crawl_execution)
    _assert(isinstance(executed_routes, list) and executed_routes, "Expected executed_routes in crawl_execution.")
    _assert(isinstance(document_queue, list), "Expected document_queue list in crawl_execution.")
    _assert(isinstance(sites, list), "Expected sites list in crawl_execution.")
    _assert(
        runtime_summary.get("page_records") == crawl_execution.get("page_records") == page_records,
        "runtime_summary.page_records must stay consistent with crawl_execution and parser result.",
    )
    _assert(
        runtime_summary.get("document_records") == crawl_execution.get("document_records") == document_records,
        "runtime_summary.document_records must stay consistent with crawl_execution and parser result.",
    )
    _assert(
        runtime_summary.get("record_count") == crawl_execution.get("record_count") == len(getattr(result, "content_records", [])),
        "runtime_summary.record_count must stay consistent with canonical content_records.",
    )
    _assert(
        runtime_summary.get("executed_route_count") == len(executed_routes),
        "runtime_summary.executed_route_count must match executed_routes length.",
    )
    _assert(
        runtime_summary.get("skipped_route_count") == len(skipped_routes),
        "runtime_summary.skipped_route_count must match skipped_routes length.",
    )
    _assert(
        runtime_summary.get("document_queue_count") == len(document_queue),
        "runtime_summary.document_queue_count must match document_queue length.",
    )
    _assert(
        runtime_summary.get("visited_route_families") == visited_route_families,
        "runtime_summary.visited_route_families must match crawl_execution.visited_route_families.",
    )
    _assert(
        crawl_execution.get("executed_route_count") == len(executed_routes),
        "crawl_execution.executed_route_count must be recomputed from executed_routes.",
    )
    _assert(
        crawl_execution.get("skipped_route_count") == len(skipped_routes),
        "crawl_execution.skipped_route_count must be recomputed from skipped_routes.",
    )
    budget = crawl_execution.get("budget")
    _assert(isinstance(budget, dict), "crawl_execution.budget must be present.")
    _assert(
        budget.get("executed_routes") == len(executed_routes),
        "crawl_execution.budget.executed_routes must stay aligned with executed_routes.",
    )
    _assert(
        budget.get("skipped_routes") == len(skipped_routes),
        "crawl_execution.budget.skipped_routes must stay aligned with skipped_routes.",
    )

    for index, item in enumerate(executed_routes, start=1):
        _assert(isinstance(item, dict), f"executed_routes[{index}] must be an object.")
        route_pattern = _require_non_empty_string(item.get("route_pattern"), f"executed_routes[{index}].route_pattern")
        route_origin = _require_non_empty_string(item.get("route_origin"), f"executed_routes[{index}].route_origin")
        status = _require_non_empty_string(item.get("status"), f"executed_routes[{index}].status")
        _assert(route_origin in {"sample", "planned", "homepage"}, f"executed_routes[{index}].route_origin must be normalized.")
        _assert(status == "executed", f"executed_routes[{index}].status must stay 'executed'.")
        _assert(isinstance(item.get("page_records"), int), f"executed_routes[{index}].page_records must be int.")
        _assert(isinstance(item.get("document_records"), int), f"executed_routes[{index}].document_records must be int.")
        if "from_sample" in item:
            _assert(
                bool(item["from_sample"]) == (route_origin == "sample"),
                f"executed_routes[{index}].from_sample must match route_origin.",
            )
        route_records, route_fingerprints = _canonical_route_mapping(result, item)
        _assert(route_records, f"executed_routes[{index}] must map to at least one canonical content record.")
        _assert(
            len(route_records) == item.get("page_records", 0) + item.get("document_records", 0),
            f"executed_routes[{index}] counters must match mapped canonical content records.",
        )
        _assert(
            all(str(record.content_fingerprint or "").strip() for record in route_records),
            f"executed_routes[{index}] mapped content records must have content_fingerprint.",
        )
        _assert(
            route_fingerprints,
            f"executed_routes[{index}] must expose canonical fingerprint mapping independent of route_pattern.",
        )
        if "record_count" in item:
            _assert(
                item.get("record_count") == len(route_records),
                f"executed_routes[{index}].record_count must match canonical mapped records.",
            )
        canonical_page_records = sum(1 for record in route_records if _record_source_kind(record) == "page")
        canonical_document_records = sum(1 for record in route_records if _record_source_kind(record) == "document")
        _assert(
            item.get("page_records") == canonical_page_records,
            f"executed_routes[{index}].page_records must match canonical mapped page records.",
        )
        _assert(
            item.get("document_records") == canonical_document_records,
            f"executed_routes[{index}].document_records must match canonical mapped document records.",
        )
        content_fingerprint = _require_non_empty_string(
            item.get("content_fingerprint"),
            f"executed_routes[{index}].content_fingerprint",
        )
        _assert(
            content_fingerprint in route_fingerprints,
            f"executed_routes[{index}].content_fingerprint must belong to canonical fingerprint mapping.",
        )
        content_fingerprints = item.get("content_fingerprints")
        _assert(
            isinstance(content_fingerprints, list) and content_fingerprints == route_fingerprints,
            f"executed_routes[{index}].content_fingerprints must match canonical fingerprint mapping.",
        )
        route_trace_records = _records_for_route(result, route_pattern)
        if route_trace_records:
            route_trace_fingerprints = dedupe_preserve_order(
                str(getattr(record, "content_fingerprint", "") or "").strip()
                for record in route_trace_records
                if str(getattr(record, "content_fingerprint", "") or "").strip()
            )
            _assert(
                all(fingerprint in route_fingerprints for fingerprint in route_trace_fingerprints),
                f"executed_routes[{index}] route-trace fingerprints must be included in canonical fingerprint mapping.",
            )

    for index, item in enumerate(document_queue, start=1):
        _assert(isinstance(item, dict), f"document_queue[{index}] must be an object.")
        route_origin = _require_non_empty_string(item.get("route_origin"), f"document_queue[{index}].route_origin")
        status = _require_non_empty_string(item.get("status"), f"document_queue[{index}].status")
        _require_non_empty_string(item.get("content_fingerprint"), f"document_queue[{index}].content_fingerprint")
        _assert(route_origin in {"sample", "planned", "homepage"}, f"document_queue[{index}].route_origin must be normalized.")
        _assert(status == "crawled", f"document_queue[{index}].status must stay 'crawled'.")
        if "from_sample" in item:
            _assert(
                bool(item["from_sample"]) == (route_origin == "sample"),
                f"document_queue[{index}].from_sample must match route_origin.",
            )
        record = _record_by_fingerprint(result, item["content_fingerprint"])
        crawl_trace = getattr(record, "trace", {}).get("factory_site_parser", {}).get("crawl", {})
        _assert(isinstance(crawl_trace, dict), f"document_queue[{index}] record must keep crawl trace.")
        _assert(
            str(crawl_trace.get("route_origin") or "").strip() == route_origin,
            f"document_queue[{index}] route_origin must match record crawl provenance.",
        )
        if "from_sample" in item:
            _assert(
                crawl_trace.get("from_sample") is item.get("from_sample"),
                f"document_queue[{index}] from_sample must match record crawl provenance.",
            )

    for index, item in enumerate(sites, start=1):
        _assert(isinstance(item, dict), f"sites[{index}] must be an object.")
        site_url = _require_non_empty_string(item.get("site_url"), f"sites[{index}].site_url")
        site_records = _records_for_site(result, site_url)
        canonical_page_records = sum(1 for record in site_records if _record_source_kind(record) == "page")
        canonical_document_records = sum(1 for record in site_records if _record_source_kind(record) == "document")
        _assert(
            item.get("page_records") == canonical_page_records,
            f"sites[{index}].page_records must match canonical site records.",
        )
        _assert(
            item.get("document_records") == canonical_document_records,
            f"sites[{index}].document_records must match canonical site records.",
        )
        if "record_count" in item:
            _assert(
                item.get("record_count") == len(site_records),
                f"sites[{index}].record_count must match canonical site records.",
            )
        site_executed_routes = item.get("executed_routes")
        site_skipped_routes = item.get("skipped_routes")
        _assert(isinstance(site_executed_routes, list), f"sites[{index}].executed_routes must be a list.")
        _assert(isinstance(site_skipped_routes, list), f"sites[{index}].skipped_routes must be a list.")
        _assert(
            item.get("executed_route_count") == len(site_executed_routes),
            f"sites[{index}].executed_route_count must match executed_routes length.",
        )
        _assert(
            item.get("skipped_route_count") == len(site_skipped_routes),
            f"sites[{index}].skipped_route_count must match skipped_routes length.",
        )


def _assert_notes_surface(
    result: Any,
    crawl_execution: dict[str, Any],
    *,
    page_records: int,
    document_records: int,
    visited_route_families: list[Any],
) -> None:
    notes = _require_notes(result)
    crawl_notes = [
        note
        for note in notes
        if note.lower().startswith("factory-site crawl ")
    ]
    canonical_notes = [
        note
        for note in crawl_notes
        if note.lower().startswith("factory-site crawl summary:")
    ]
    _assert(canonical_notes, "Expected canonical factory-site crawl summary note in parser result notes.")
    expected_canonical_note = (
        "factory-site crawl summary: "
        f"pages={page_records} | "
        f"documents={document_records} | "
        f"executed={len(crawl_execution.get('executed_routes', []))} | "
        f"skipped={len(crawl_execution.get('skipped_routes', []))} | "
        f"families={','.join(str(item) for item in visited_route_families) or 'none'}"
    )
    _assert(
        expected_canonical_note in canonical_notes,
        "Canonical factory-site crawl summary note must match top-level parser result counters.",
    )
    for index, note in enumerate(crawl_notes, start=1):
        normalized = note.lower()
        if normalized.startswith("factory-site crawl summary:"):
            continue
        _assert(
            "raw" in normalized,
            f"result.notes crawl note #{index} must be explicitly raw-marked when it is not canonical summary.",
        )
        if "pages=" in normalized or "documents=" in normalized:
            _assert(
                "raw_pages=" in normalized and "raw_documents=" in normalized,
                f"result.notes crawl note #{index} must use raw_pages/raw_documents and not masquerade as canonical counters.",
            )


def _assert_authoritative_lead_provenance(lead_assembly: dict[str, Any]) -> None:
    lead_evidence = lead_assembly.get("lead_evidence", [])
    _assert(isinstance(lead_evidence, list) and lead_evidence, "Expected lead_evidence from parser result.")
    assertable = [
        item
        for item in lead_evidence
        if isinstance(item, dict) and isinstance(item.get("provenance"), dict)
    ]
    _assert(assertable, "Expected lead_evidence items with provenance.")
    _assert(
        any(str(item["provenance"].get("route_origin") or "").strip() == "planned" for item in assertable),
        "Expected at least one authoritative planned provenance in lead_evidence.",
    )
    for index, item in enumerate(assertable, start=1):
        provenance = item["provenance"]
        route_origin = _require_non_empty_string(provenance.get("route_origin"), f"lead_evidence[{index}].provenance.route_origin")
        _assert(route_origin in {"sample", "planned", "homepage"}, f"lead_evidence[{index}] route_origin must be normalized.")
        source_kind = provenance.get("source_kind")
        if source_kind is not None:
            _assert(
                str(source_kind).strip() in {"page", "document"},
                f"lead_evidence[{index}].provenance.source_kind must be normalized when present.",
            )
        if "from_sample" in provenance:
            _assert(
                bool(provenance["from_sample"]) == (route_origin == "sample"),
                f"lead_evidence[{index}].provenance.from_sample must match route_origin.",
            )


def _assert_missing_top_level_attr_regression() -> None:
    base_result = _run_parser(planner=FakePlanner(), documents_stage=FakeDocumentsStage())
    required_attrs = (
        "relevance_summary",
        "lead_assembly",
        "crawl_execution",
        "page_records",
        "document_records",
        "visited_route_families",
    )
    for attr_name in required_attrs:
        broken_result = copy.copy(base_result)
        if hasattr(broken_result, attr_name):
            delattr(broken_result, attr_name)
        try:
            _require_parser_result_payloads(broken_result)
        except RuntimeError:
            continue
        raise RuntimeError(f"Smoke must fail immediately when parser result misses top-level attr {attr_name}.")


def _run_parser(*, planner: Any, documents_stage: Any) -> Any:
    return _run_parser_with_fetch_kwargs(
        planner=planner,
        documents_stage=documents_stage,
        fetch_stage_kwargs={},
    )


def _run_parser_with_fetch_kwargs(
    *,
    planner: Any,
    documents_stage: Any,
    fetch_stage_kwargs: dict[str, Any],
) -> Any:
    fetch_stage = FactorySiteFetchStage(
        FakeClient(),
        fetcher=FakeFetcher(),
        normalizer=FakeNormalizer(),
        documents_stage=documents_stage,
        **fetch_stage_kwargs,
    )
    parser = FactorySiteParser(
        FakeClient(),
        planner=planner,
        fetch_stage=fetch_stage,
        documents_stage=object(),
        okved_matcher=FakeOkvedMatcher(),
    )
    company = FactorySiteParserCompany(
        company_id="7701234567",
        company_name="Offline Smoke Plant",
        input_site=SITE_URL,
        candidate_sites=[SITE_URL],
    )
    return parser.parse(company)


def _run_parser_with_custom_fetch_stage(
    *,
    planner: Any,
    fetch_stage: Any,
) -> Any:
    parser = FactorySiteParser(
        FakeClient(),
        planner=planner,
        fetch_stage=fetch_stage,
        documents_stage=object(),
        okved_matcher=FakeOkvedMatcher(),
    )
    company = FactorySiteParserCompany(
        company_id="7701234567",
        company_name="Offline Smoke Plant",
        input_site=SITE_URL,
        candidate_sites=[SITE_URL],
    )
    return parser.parse(company)


class DuplicateDocumentStage(FakeDocumentsStage):
    def collect_direct_response(self, **kwargs: Any) -> list[ContentRecord]:
        records = super().collect_direct_response(**kwargs)
        _assert(records, "Duplicate document smoke fixture requires a base record.")
        return [records[0], copy.deepcopy(records[0])]


def _assert_legacy_fetch_stage_list_return_regression() -> None:
    result = _run_parser_with_custom_fetch_stage(
        planner=StaticPlanPlanner(
            _build_plan_for_routes(
                [
                    {
                        "route_pattern": ABOUT_URL,
                        "route_family": "company/about",
                        "section_guess": "about",
                    },
                    {
                        "route_pattern": DOCUMENT_URL,
                        "route_family": "docs/certificates",
                        "section_guess": "documents",
                    },
                ],
                notes_prefix="legacy-list",
            )
        ),
        fetch_stage=LegacyListFetchStage(),
    )
    (
        relevance_summary,
        lead_assembly,
        crawl_execution,
        page_records,
        document_records,
        visited_route_families,
    ) = _require_parser_result_payloads(result)
    _assert_result_contract(
        result,
        relevance_summary,
        lead_assembly,
        crawl_execution,
        page_records=page_records,
        document_records=document_records,
        visited_route_families=visited_route_families,
    )
    runtime_summary = _require_runtime_summary(crawl_execution)
    _assert(
        runtime_summary.get("record_count") == len(getattr(result, "content_records", [])),
        "Legacy list-return path must still build canonical runtime_summary.",
    )
    _assert(crawl_execution.get("executed_routes") == [], "Legacy list-return path must not require synthetic executed_routes.")
    _assert(crawl_execution.get("document_queue") == [], "Legacy list-return path must not invent document_queue.")
    _assert(
        isinstance(visited_route_families, list) and set(visited_route_families) == {"company/about", "docs/certificates"},
        "Legacy list-return path must still populate visited_route_families from canonical result.",
    )


def _assert_cross_route_duplicate_fingerprint_regression() -> None:
    result = _run_parser_with_custom_fetch_stage(
        planner=StaticPlanPlanner(
            _build_plan_for_routes(
                [
                    {
                        "route_pattern": DUPLICATE_ROUTE_DOCUMENT_URL_A,
                        "route_family": "docs/certificates",
                        "section_guess": "documents",
                    },
                    {
                        "route_pattern": DUPLICATE_ROUTE_DOCUMENT_URL_B,
                        "route_family": "docs/certificates",
                        "section_guess": "documents",
                    },
                ],
                notes_prefix="cross-route-duplicate",
            )
        ),
        fetch_stage=CrossRouteDuplicateFetchStage(),
    )
    (
        relevance_summary,
        lead_assembly,
        crawl_execution,
        page_records,
        document_records,
        visited_route_families,
    ) = _require_parser_result_payloads(result)
    _assert_result_contract(
        result,
        relevance_summary,
        lead_assembly,
        crawl_execution,
        page_records=page_records,
        document_records=document_records,
        visited_route_families=visited_route_families,
    )
    _assert_runtime_summary(
        result,
        crawl_execution,
        page_records=page_records,
        document_records=document_records,
        visited_route_families=visited_route_families,
    )
    executed_routes = list(crawl_execution.get("executed_routes", []))
    _assert(len(executed_routes) == 2, "Cross-route duplicate repro must keep both executed routes.")
    _assert(
        len(getattr(result, "content_records", [])) == 1 and document_records == 1,
        "Cross-route duplicate repro must keep one canonical record after dedupe.",
    )
    shared_fingerprints = []
    for index, item in enumerate(executed_routes, start=1):
        fingerprints = item.get("content_fingerprints")
        _assert(
            isinstance(fingerprints, list) and fingerprints,
            f"executed_routes[{index}] must keep non-empty canonical fingerprint mapping after cross-route dedupe.",
        )
        _assert(
            item.get("document_records") == 1 and item.get("record_count") == 1,
            f"executed_routes[{index}] must keep non-zero canonical counters after cross-route dedupe.",
        )
        shared_fingerprints.append(tuple(fingerprints))
    _assert(
        len(set(shared_fingerprints)) == 1,
        "Cross-route duplicate repro must allow two route_patterns to point at the same canonical fingerprint mapping.",
    )


def _assert_stale_prefilled_executed_route_count_regression() -> None:
    result = _run_parser_with_custom_fetch_stage(
        planner=StaticPlanPlanner(
            _build_plan_for_routes(
                [
                    {
                        "route_pattern": ABOUT_URL,
                        "route_family": "company/about",
                        "section_guess": "about",
                    }
                ],
                notes_prefix="stale-prefilled",
            )
        ),
        fetch_stage=StalePrefilledCountsFetchStage(),
    )
    (
        relevance_summary,
        lead_assembly,
        crawl_execution,
        page_records,
        document_records,
        visited_route_families,
    ) = _require_parser_result_payloads(result)
    _assert_result_contract(
        result,
        relevance_summary,
        lead_assembly,
        crawl_execution,
        page_records=page_records,
        document_records=document_records,
        visited_route_families=visited_route_families,
    )
    _assert_runtime_summary(
        result,
        crawl_execution,
        page_records=page_records,
        document_records=document_records,
        visited_route_families=visited_route_families,
    )
    _assert(crawl_execution.get("executed_route_count") == 1, "Stale executed_route_count must be normalized from executed_routes length.")
    _assert(crawl_execution.get("skipped_route_count") == 1, "Stale skipped_route_count must be normalized from skipped_routes length.")
    budget = crawl_execution.get("budget", {})
    _assert(budget.get("executed_routes") == 1, "Budget executed_routes must be normalized from executed_routes length.")
    _assert(budget.get("skipped_routes") == 1, "Budget skipped_routes must be normalized from skipped_routes length.")
    site = list(crawl_execution.get("sites", []))[0]
    _assert(site.get("executed_route_count") == 1, "Site executed_route_count must be normalized from site.executed_routes.")
    _assert(site.get("skipped_route_count") == 1, "Site skipped_route_count must be normalized from site.skipped_routes.")


def _assert_bounded_execution_payload_regression() -> None:
    result = _run_parser_with_fetch_kwargs(
        planner=LargePlanPlanner(route_count=25),
        documents_stage=FakeDocumentsStage(),
        fetch_stage_kwargs={"max_routes_per_site": 1},
    )
    (
        relevance_summary,
        lead_assembly,
        crawl_execution,
        page_records,
        document_records,
        visited_route_families,
    ) = _require_parser_result_payloads(result)
    _assert_result_contract(
        result,
        relevance_summary,
        lead_assembly,
        crawl_execution,
        page_records=page_records,
        document_records=document_records,
        visited_route_families=visited_route_families,
    )
    _assert_runtime_summary(
        result,
        crawl_execution,
        page_records=page_records,
        document_records=document_records,
        visited_route_families=visited_route_families,
    )
    _assert(
        len(crawl_execution.get("executed_routes", [])) == 1,
        "Bounded payload repro must execute only the effective route slice.",
    )
    max_route_skips = [
        item
        for item in crawl_execution.get("skipped_routes", [])
        if isinstance(item, dict) and str(item.get("reason") or "").strip() == "max_routes_per_site"
    ]
    _assert(
        len(max_route_skips) == 1,
        "Bounded payload repro must aggregate execution-tail skips instead of emitting one skip per tail route.",
    )
    tail_skip = max_route_skips[0]
    _assert(tail_skip.get("aggregated") is True, "Bounded payload tail skip must be explicitly aggregated.")
    _assert(
        tail_skip.get("skipped_route_count") == 24,
        "Bounded payload tail skip must expose the full aggregated tail size.",
    )
    preview = tail_skip.get("tail_route_patterns_preview")
    _assert(
        isinstance(preview, list) and len(preview) <= 3,
        "Bounded payload tail skip preview must stay bounded and not mirror the full plan tail.",
    )
    _assert(
        len(crawl_execution.get("skipped_routes", [])) == 1,
        "Bounded payload repro must keep skipped_routes payload bounded for execution-limit tail.",
    )


def _assert_max_routes_per_site_regression() -> None:
    result = _run_parser_with_fetch_kwargs(
        planner=FakePlanner(),
        documents_stage=FakeDocumentsStage(),
        fetch_stage_kwargs={"max_routes_per_site": 1},
    )
    (
        relevance_summary,
        lead_assembly,
        crawl_execution,
        page_records,
        document_records,
        visited_route_families,
    ) = _require_parser_result_payloads(result)

    _assert_result_contract(
        result,
        relevance_summary,
        lead_assembly,
        crawl_execution,
        page_records=page_records,
        document_records=document_records,
        visited_route_families=visited_route_families,
    )
    _assert_runtime_summary(
        result,
        crawl_execution,
        page_records=page_records,
        document_records=document_records,
        visited_route_families=visited_route_families,
    )
    _assert(
        len(crawl_execution.get("executed_routes", [])) == 1,
        "max_routes_per_site=1 must limit executed_routes to a single route.",
    )
    _assert(
        len(visited_route_families) == 1,
        "max_routes_per_site=1 must limit visited_route_families to a single family.",
    )
    max_route_skips = [
        item
        for item in crawl_execution.get("skipped_routes", [])
        if str(item.get("reason") or "").strip() == "max_routes_per_site"
    ]
    _assert(max_route_skips, "max_routes_per_site=1 must surface explicit max_routes_per_site skip entries.")
    _assert(
        all("skip_reason" not in item for item in max_route_skips),
        "max_routes_per_site skip entries must use stable reason key without legacy-only skip_reason.",
    )


def _assert_duplicate_content_fingerprint_regression() -> None:
    result = _run_parser(
        planner=SingleRoutePlanner(
            route_pattern=DOCUMENT_URL,
            sampled_urls=[],
        ),
        documents_stage=DuplicateDocumentStage(),
    )
    (
        relevance_summary,
        lead_assembly,
        crawl_execution,
        page_records,
        document_records,
        visited_route_families,
    ) = _require_parser_result_payloads(result)

    _assert_result_contract(
        result,
        relevance_summary,
        lead_assembly,
        crawl_execution,
        page_records=page_records,
        document_records=document_records,
        visited_route_families=visited_route_families,
    )
    _assert_runtime_summary(
        result,
        crawl_execution,
        page_records=page_records,
        document_records=document_records,
        visited_route_families=visited_route_families,
    )
    _assert(
        len(getattr(result, "content_records", [])) == 1,
        "Duplicate content_fingerprint repro must collapse to a single canonical content record.",
    )
    _assert(
        document_records == 1 == relevance_summary.get("document_records"),
        "Duplicate content_fingerprint repro must keep canonical document count at one.",
    )
    _assert(
        crawl_execution.get("document_records") == 1 and crawl_execution.get("runtime_summary", {}).get("document_records") == 1,
        "Duplicate content_fingerprint repro must keep crawl_execution canonical document count aligned.",
    )
    non_sample_fingerprints = relevance_summary.get("non_sample_record_fingerprints", [])
    _assert(
        isinstance(non_sample_fingerprints, list) and len(non_sample_fingerprints) == 1,
        "Duplicate content_fingerprint repro must keep one canonical non-sample fingerprint.",
    )
    raw_document_records = crawl_execution.get("raw_document_records")
    raw_record_count = crawl_execution.get("raw_record_count")
    if raw_document_records is not None or raw_record_count is not None:
        _assert(raw_document_records == 2, "raw_document_records must preserve pre-dedupe document count in duplicate repro.")
        _assert(raw_record_count == 2, "raw_record_count must preserve pre-dedupe record count in duplicate repro.")


def _assert_sample_document_regression() -> None:
    result = _run_parser(
        planner=SingleRoutePlanner(
            route_pattern=SAMPLE_DOCUMENT_URL,
            sampled_urls=[SAMPLE_DOCUMENT_URL],
        ),
        documents_stage=FakeDocumentsStage(),
    )
    relevance_summary, lead_assembly, _, _, _, _ = _require_parser_result_payloads(result)
    _assert(relevance_summary.get("non_sample_record_fingerprints") == [], "Sample document must not be counted as non-sample.")
    lead_evidence = lead_assembly.get("lead_evidence", [])
    _assert(isinstance(lead_evidence, list) and lead_evidence, "Sample document case requires lead_evidence.")
    item = lead_evidence[0]
    provenance = item.get("provenance", {})
    _assert(item.get("is_sample") is True, "Sample document lead evidence must keep is_sample=True.")
    _assert(item.get("is_non_sample") is False, "Sample document lead evidence must keep is_non_sample=False.")
    _assert(provenance.get("route_origin") == "sample", "Sample document provenance must stay authoritative sample.")
    _assert(provenance.get("from_sample") is True, "Sample document provenance must keep from_sample=True.")
    _assert(provenance.get("source_kind") == "document", "Sample document provenance must keep source_kind=document.")
    _assert(provenance.get("route_family") == "docs/certificates", "Sample document provenance must keep route_family.")


def _assert_conflicting_provenance_regression() -> None:
    result = _run_parser(
        planner=SingleRoutePlanner(
            route_pattern=CONFLICT_DOCUMENT_URL,
            sampled_urls=[],
        ),
        documents_stage=FakeDocumentsStage(),
    )
    relevance_summary, lead_assembly, _, _, _, _ = _require_parser_result_payloads(result)
    lead_evidence = lead_assembly.get("lead_evidence", [])
    _assert(isinstance(lead_evidence, list) and lead_evidence, "Conflicting provenance case requires lead_evidence.")
    item = lead_evidence[0]
    provenance = item.get("provenance", {})
    _assert(provenance.get("route_origin") == "planned", "Authoritative crawl provenance must beat legacy sample route_origin.")
    _assert(provenance.get("from_sample") is False, "Authoritative crawl provenance must beat legacy sample from_sample.")
    _assert(provenance.get("source_kind") == "document", "Authoritative crawl provenance must keep source_kind=document.")
    _assert(provenance.get("route_family") == "docs/certificates", "Authoritative crawl provenance must keep route_family.")
    _assert(item.get("is_sample") is False, "Conflicting provenance case must remain non-sample.")
    _assert(item.get("is_non_sample") is True, "Conflicting provenance case must remain non-sample.")
    _assert(relevance_summary.get("non_sample_record_fingerprints"), "Conflicting provenance case must contribute non-sample fingerprint.")


def main() -> int:
    result = _run_parser(planner=FakePlanner(), documents_stage=FakeDocumentsStage())
    (
        relevance_summary,
        lead_assembly,
        crawl_execution,
        page_records,
        document_records,
        visited_route_families,
    ) = _require_parser_result_payloads(result)

    _assert_result_contract(
        result,
        relevance_summary,
        lead_assembly,
        crawl_execution,
        page_records=page_records,
        document_records=document_records,
        visited_route_families=visited_route_families,
    )
    _assert_runtime_summary(
        result,
        crawl_execution,
        page_records=page_records,
        document_records=document_records,
        visited_route_families=visited_route_families,
    )
    _assert_notes_surface(
        result,
        crawl_execution,
        page_records=page_records,
        document_records=document_records,
        visited_route_families=visited_route_families,
    )
    _assert_authoritative_lead_provenance(lead_assembly)
    _assert(relevance_summary.get("page_records", 0) > SAMPLE_BASELINE, "Expected page_records above sample baseline.")
    _assert(relevance_summary.get("document_records", 0) >= 1, "Expected at least one document record.")
    _assert(
        len(visited_route_families) >= 3,
        "Expected at least three visited route families.",
    )
    skip_reasons = dedupe_preserve_order(
        str(item.get("reason") or "").strip()
        for item in crawl_execution.get("skipped_routes", [])
        if isinstance(item, dict) and str(item.get("reason") or "").strip()
    )
    _assert(
        any(reason in {"global_budget_exhausted", "policy_blocked_browser_only"} for reason in skip_reasons),
        "Expected at least one budget or policy skip reason.",
    )
    _assert(isinstance(crawl_execution.get("budget"), dict) and crawl_execution.get("budget"), "Expected budget summary in crawl execution.")
    _assert(relevance_summary.get("non_sample_evidence_count", 0) >= 1, "Expected non-sample lead evidence.")
    _assert(
        any(
            not item.get("is_sample", True) and item.get("source_kind") in {"page", "document"}
            for item in lead_assembly.get("lead_evidence", [])
        ),
        "Expected lead evidence from a non-sample page or document.",
    )

    _assert_missing_top_level_attr_regression()
    _assert_sample_document_regression()
    _assert_conflicting_provenance_regression()
    _assert_legacy_fetch_stage_list_return_regression()
    _assert_max_routes_per_site_regression()
    _assert_bounded_execution_payload_regression()
    _assert_duplicate_content_fingerprint_regression()
    _assert_cross_route_duplicate_fingerprint_regression()
    _assert_stale_prefilled_executed_route_count_regression()

    print(
        "PASS "
        f"page_records={relevance_summary['page_records']} "
        f"document_records={relevance_summary['document_records']} "
        f"visited_route_families={len(visited_route_families)} "
        f"skip_reasons={len(skip_reasons)} "
        f"executed_routes={len(crawl_execution.get('executed_routes', []))} "
        f"document_queue={len(crawl_execution.get('document_queue', []))} "
        f"non_sample_evidence_count={relevance_summary['non_sample_evidence_count']} "
        f"lead_families={','.join(lead_assembly['lead_families'])}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

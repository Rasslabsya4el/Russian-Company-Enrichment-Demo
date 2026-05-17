from __future__ import annotations

import os
from dataclasses import dataclass
from types import SimpleNamespace

from app.site_intelligence.common import INDUSTRIAL_POSITIVE_KEYWORDS, compact_text, dedupe_preserve_order, normalize_url, normalize_whitespace
from app.site_intelligence.factory_site_parser.models import FactorySiteParserCompany, FactorySitePlan
from app.site_intelligence.factory_site_parser.planner import FAMILY_RULES, FactorySitePlanner
from app.site_intelligence.models import ContentRecord, SiteProbe
from app.site_intelligence.relevance import classify_content_record, infer_lead_type_from_record
from app.site_intelligence.serialization import route_strategy_from_dict, site_probe_from_dict, site_probe_to_dict
from app.site_intelligence.site_authenticity import SiteAuthHelpers, SiteAuthenticityAnalyzer
from app.site_intelligence.strategy import StrategySelector


def _record(*, url: str, title: str, text: str, section_guess: str = "homepage") -> ContentRecord:
    return ContentRecord(
        company_id="company-1",
        site_url="https://example.com",
        url=url,
        source_type="html",
        title=title,
        raw_text=text,
        cleaned_text=text,
        section_guess=section_guess,
        extraction_method="requests",
        fetch_status="success",
        content_fingerprint=f"fp:{url}",
    )


def _keyword_found_in_text(text: str, keyword: str) -> bool:
    return keyword.lower().replace("ё", "е") in text.lower().replace("ё", "е")


def _site_auth_analyzer() -> SiteAuthenticityAnalyzer:
    helpers = SiteAuthHelpers(
        normalize_url=lambda value: value,
        normalize_whitespace=normalize_whitespace,
        parse_title_and_meta=lambda soup: {"title": "", "description": ""},
        dedupe_preserve_order=dedupe_preserve_order,
        extract_emails=lambda text: [],
        extract_phones=lambda text: [],
        extract_probable_addresses=lambda text: [],
        normalize_phone_values=lambda values: list(values) if isinstance(values, list) else [],
        normalize_address_values=lambda values: list(values) if isinstance(values, list) else [],
        normalize_phone_candidate=lambda value: value,
        company_tokens=lambda value: set(),
        normalized_phone_digits=lambda value: value,
        guess_registered_domain=lambda host: host,
        address_identity_tokens=lambda address: {"postals": set(), "tokens": set()},
        is_valid_russian_inn=lambda inn: True,
        keyword_found_in_text=_keyword_found_in_text,
        compact_text=compact_text,
        summarize_source_context=lambda value: value,
        looks_like_bot_gate=lambda response, text: False,
        contact_path_hints=(),
        contact_link_text_hints=(),
        industrial_positive_keywords=INDUSTRIAL_POSITIVE_KEYWORDS,
        industrial_negative_keywords={},
        generic_email_domains=set(),
        company_token_stopwords=set(),
        activity_token_stopwords=set(),
        non_corporate_domains=set(),
    )
    return SiteAuthenticityAnalyzer(client=object(), llm=object(), helpers=helpers)


def _activity_profile(text: str) -> tuple[SiteAuthenticityAnalyzer, dict[str, object]]:
    analyzer = _site_auth_analyzer()
    row = SimpleNamespace(company_name="")
    source_results = {"source": SimpleNamespace(snippets=[text], notes=[])}
    return analyzer, analyzer._build_activity_profile(row, source_results)


@dataclass
class _PlannerSmokeResponse:
    url: str
    text: str = ""
    status_code: int = 200
    headers: dict[str, str] | None = None
    content: bytes = b""
    encoding: str = "utf-8"

    def __post_init__(self) -> None:
        if self.headers is None:
            self.headers = {"Content-Type": "text/html; charset=utf-8"}
        if not self.content:
            self.content = self.text.encode(self.encoding, errors="ignore")


class _PlannerSmokeClient:
    def __init__(self, responses: dict[str, _PlannerSmokeResponse]) -> None:
        self._responses = responses
        self.requests: list[_PlannerSmokeRequest] = []

    def request(self, url: str, *, source: str, timeout: int) -> SimpleNamespace:
        response = self._responses.get(url)
        self.requests.append(
            _PlannerSmokeRequest(
                url=url,
                source=source,
                ok=response is not None,
                response_url=getattr(response, "url", "") if response is not None else "",
            )
        )
        return SimpleNamespace(ok=response is not None, response=response)


@dataclass
class _PlannerSmokeRequest:
    url: str
    source: str
    ok: bool
    response_url: str = ""


@dataclass
class _PlannerSmokeRun:
    plan: FactorySitePlan
    requests: list[_PlannerSmokeRequest]

    @property
    def request_order(self) -> list[str]:
        return [request.url for request in self.requests]

    @property
    def guessed_requests(self) -> list[_PlannerSmokeRequest]:
        return [request for request in self.requests if request.source == "crawl_planner_subdomain"]

    @property
    def guessed_request_order(self) -> list[str]:
        return [request.url for request in self.guessed_requests]

    @property
    def guessed_stop_url(self) -> str:
        return self.guessed_request_order[-1] if self.guessed_request_order else ""


class _StaticProber:
    def __init__(self, probe: SiteProbe) -> None:
        self._probe = probe

    def probe(self, site_url: str) -> SiteProbe:
        return self._probe


def _build_smoke_plan(
    *,
    html: str,
    sampled_urls: list[str],
    responses: dict[str, _PlannerSmokeResponse],
    env_updates: dict[str, str],
    with_trace: bool = False,
) -> FactorySitePlan | _PlannerSmokeRun:
    origin = "https://example.com/"
    merged_responses = {
        origin: _PlannerSmokeResponse(url=origin, text=html),
        **responses,
    }
    probe = SiteProbe(
        url=origin,
        final_url=origin,
        status="success",
        site_class="A",
        worth_crawling="true",
        html_ok=True,
        sampled_urls=sampled_urls,
    )

    previous_env = {key: os.environ.get(key) for key in env_updates}
    try:
        for key, value in env_updates.items():
            os.environ[key] = value

        client = _PlannerSmokeClient(merged_responses)
        planner = FactorySitePlanner(client, prober=_StaticProber(probe))
        company = FactorySiteParserCompany(company_id="1", company_name="Test", input_site=origin)
        plan = planner.plan(company, max_sites=1)[0]
        if with_trace:
            return _PlannerSmokeRun(plan=plan, requests=list(client.requests))
        return plan
    finally:
        for key, value in previous_env.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value


def _guessed_subdomain_url(subdomain: str) -> str:
    return f"https://{subdomain}.example.com/"


def _build_guessed_subdomain_smoke_plan(
    *,
    redirect_to_origin: tuple[str, ...] = (),
    same_domain_successes: dict[str, str] | None = None,
    hard_misses: tuple[str, ...] = (),
    real_successes: tuple[str, ...] = ("contacts", "docs"),
    origin_html: str = "<html><body>home</body></html>",
    sampled_urls: tuple[str, ...] = (),
    extra_responses: dict[str, _PlannerSmokeResponse] | None = None,
    with_trace: bool = False,
) -> FactorySitePlan | _PlannerSmokeRun:
    same_domain_successes = dict(same_domain_successes or {})
    scenario_membership: dict[str, str] = {}
    for scenario_name, subdomains in (
        ("redirect_to_origin", redirect_to_origin),
        ("same_domain_success", tuple(same_domain_successes)),
        ("hard_miss", hard_misses),
        ("real_success", real_successes),
    ):
        for subdomain in subdomains:
            previous = scenario_membership.setdefault(subdomain, scenario_name)
            if previous != scenario_name:
                raise AssertionError(f"subdomain {subdomain!r} assigned to both {previous} and {scenario_name}")

    responses: dict[str, _PlannerSmokeResponse] = {}
    for subdomain in redirect_to_origin:
        responses[_guessed_subdomain_url(subdomain)] = _PlannerSmokeResponse(
            url="https://example.com/",
            text="<html><body>redirect to origin</body></html>",
        )
    for subdomain, target_url in same_domain_successes.items():
        responses[_guessed_subdomain_url(subdomain)] = _PlannerSmokeResponse(
            url=target_url,
            text=f"<html><body>same domain {subdomain}</body></html>",
        )
    for subdomain in real_successes:
        responses[_guessed_subdomain_url(subdomain)] = _PlannerSmokeResponse(
            url=_guessed_subdomain_url(subdomain),
            text=f"<html><body>{subdomain}</body></html>",
        )
    if extra_responses:
        responses.update(extra_responses)

    return _build_smoke_plan(
        html=origin_html,
        sampled_urls=list(sampled_urls),
        responses=responses,
        env_updates={
            "FACTORY_SITE_PLANNER_GLOBAL_CRAWL_BUDGET": "4",
            "FACTORY_SITE_PLANNER_FAMILY_BUDGET_OVERRIDES": '{"company/about": 1, "contacts": 1, "production/products": 1, "docs/certificates": 1, "files": 1}',
            "FACTORY_SITE_PLANNER_DEPTH_CAPS": "{}",
            "FACTORY_SITE_PLANNER_HOST_CAPS": "{}",
            "FACTORY_SITE_PLANNER_PATH_PATTERN_CAPS": '{"example\\\\.com/$": 4}',
            "FACTORY_SITE_PLANNER_MAX_SUBDOMAIN_CHECKS": "12",
        },
        with_trace=with_trace,
    )


def test_strategy_marks_surplus_family_from_url_hints() -> None:
    selector = StrategySelector()
    probe = SiteProbe(
        url="https://example.com",
        final_url="https://example.com",
        status="success",
        site_class="A",
        sampled_urls=["https://example.com/realization/nelikvid"],
    )

    routes = selector.select("https://example.com", probe)
    surplus_route = next(route for route in routes if "nelikvid" in route.route_pattern)

    assert surplus_route.section_guess == "sales"
    assert surplus_route.route_family == "surplus/realization"


def test_surplus_page_stays_out_of_site_identity_match() -> None:
    record = _record(
        url="https://example.com/realization/skladskie-ostatki",
        title="Realization of surplus stock",
        text="nelikvid, metallolom, demontazh, vtorsyrie i nevostrebovannye tmc.",
    )

    classify_content_record(record)
    taxonomy = record.trace["page_signal_taxonomy"]

    assert taxonomy["route_family"] == "surplus/realization"
    assert taxonomy["lead_family"] == "surplus/realization"
    assert taxonomy["site_identity_match"] is False
    assert infer_lead_type_from_record(record) == "surplus/realization"


def test_identity_page_can_match_without_surplus_family() -> None:
    record = _record(
        url="https://factory.example.com/about",
        title="About the factory",
        text="factory metal constructions. in-house production, workshop and industrial equipment.",
        section_guess="about",
    )

    classify_content_record(record)
    taxonomy = record.trace["page_signal_taxonomy"]

    assert taxonomy["site_identity_match"] is True
    assert taxonomy["lead_family"] == "unknown"
    assert taxonomy["route_family"] == "about"


def test_mixed_page_keeps_surplus_family_and_identity_apart() -> None:
    record = _record(
        url="https://example.com/realization/demontazh",
        title="Factory surplus realization after demontazh",
        text="factory realizuiet skladskie ostatki posle demontazh of equipment.",
    )

    classify_content_record(record)
    taxonomy = record.trace["page_signal_taxonomy"]

    assert taxonomy["lead_family"] == "surplus/realization"
    assert taxonomy["site_identity_match"] is True


def test_planner_uses_surplus_realization_family_name() -> None:
    assert "surplus/realization" in FAMILY_RULES
    assert "sales/realization" not in FAMILY_RULES


def test_allows_deep_check_normalizes_worth_crawling() -> None:
    for raw_value, expected_value, expected_allows_deep_check in (
        (False, "false", False),
        ("false", "false", False),
        ("limited", "limited", True),
        ("true", "true", True),
    ):
        probe = site_probe_from_dict(
            {
                "url": "https://example.com/",
                "final_url": "https://example.com/",
                "status": "success",
                "site_class": "B",
                "worth_crawling": raw_value,
            }
        )

        assert probe.worth_crawling == expected_value
        assert site_probe_to_dict(probe)["worth_crawling"] == expected_value
        assert FactorySitePlan(site_url="https://example.com/", probe=probe).allows_deep_check is expected_allows_deep_check


def test_planner_queue_order_is_deterministic() -> None:
    origin = "https://example.com/"
    html = """
    <html><body>
    <nav>
    <a href="/about/">About</a>
    <a href="/contacts/">Contacts</a>
    <a href="/products/">Products</a>
    <a href="/products/metal/">Products Metal</a>
    <a href="/docs/certificate.pdf">Certificate</a>
    <a href="/files/catalog.pdf">Catalog PDF</a>
    <a href="/procurement/tenders/">Procurement</a>
    <a href="/sale/stock/">Surplus</a>
    </nav>
    </body></html>
    """
    responses = {
        origin: _PlannerSmokeResponse(url=origin, text=html),
        "https://example.com/about/": _PlannerSmokeResponse(url="https://example.com/about/", text="<html><body>about</body></html>"),
        "https://example.com/contacts/": _PlannerSmokeResponse(url="https://example.com/contacts/", text="<html><body>contacts</body></html>"),
        "https://example.com/products/": _PlannerSmokeResponse(url="https://example.com/products/", text="<html><body>products</body></html>"),
        "https://example.com/products/metal/": _PlannerSmokeResponse(url="https://example.com/products/metal/", text="<html><body>products metal</body></html>"),
        "https://example.com/procurement/tenders/": _PlannerSmokeResponse(url="https://example.com/procurement/tenders/", text="<html><body>procurement</body></html>"),
        "https://example.com/sale/stock/": _PlannerSmokeResponse(url="https://example.com/sale/stock/", text="<html><body>surplus</body></html>"),
        "https://example.com/docs/certificate.pdf": _PlannerSmokeResponse(
            url="https://example.com/docs/certificate.pdf",
            headers={"Content-Type": "application/pdf"},
            content=b"%PDF-1.4 test",
        ),
        "https://example.com/files/catalog.pdf": _PlannerSmokeResponse(
            url="https://example.com/files/catalog.pdf",
            headers={"Content-Type": "application/pdf"},
            content=b"%PDF-1.4 test",
        ),
    }
    probe = SiteProbe(
        url=origin,
        final_url=origin,
        status="success",
        site_class="A",
        worth_crawling="true",
        html_ok=True,
        sampled_urls=[
            "https://example.com/about/",
            "https://example.com/contacts/",
            "https://example.com/products/",
            "https://example.com/products/metal/",
            "https://example.com/docs/certificate.pdf",
            "https://example.com/files/catalog.pdf",
            "https://example.com/procurement/tenders/",
            "https://example.com/sale/stock/",
        ],
    )

    env_updates = {
        "FACTORY_SITE_PLANNER_GLOBAL_CRAWL_BUDGET": "7",
        "FACTORY_SITE_PLANNER_FAMILY_BUDGET_OVERRIDES": '{"company/about": 1, "contacts": 1, "production/products": 2, "docs/certificates": 1, "files": 1, "procurement": 1, "surplus/realization": 1}',
        "FACTORY_SITE_PLANNER_DEPTH_CAPS": "{}",
        "FACTORY_SITE_PLANNER_HOST_CAPS": "{}",
        "FACTORY_SITE_PLANNER_PATH_PATTERN_CAPS": '{"products": 1}',
    }
    previous_env = {key: os.environ.get(key) for key in env_updates}
    try:
        for key, value in env_updates.items():
            os.environ[key] = value

        planner = FactorySitePlanner(_PlannerSmokeClient(responses), prober=_StaticProber(probe))
        company = FactorySiteParserCompany(company_id="1", company_name="Test", input_site=origin)
        plan = planner.plan(company, max_sites=1)[0]
    finally:
        for key, value in previous_env.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value

    assert [route.route_pattern for route in plan.crawl_queue] == [
        "https://example.com/about/",
        "https://example.com/contacts/",
        "https://example.com/products/",
        "https://example.com/docs/certificate.pdf",
        "https://example.com/sale/stock/",
        "https://example.com/procurement/tenders/",
        "https://example.com/files/catalog.pdf",
    ]
    assert plan.budget_accounting is not None
    assert plan.budget_accounting.planned_routes == 7

    coverage_by_key = {item.coverage_key: item for item in plan.coverage}
    assert coverage_by_key["company/about"].covered is True
    assert coverage_by_key["contacts"].covered is True
    assert coverage_by_key["production/products"].covered is True
    assert coverage_by_key["docs/files"].covered is True
    assert coverage_by_key["docs/files"].selected_route_family == "docs/certificates"
    assert coverage_by_key["surplus/realization"].covered is True
    assert coverage_by_key["procurement"].covered is True

    skipped_by_url = {item.route_pattern: item.skip_reason for item in plan.budget_accounting.skipped_routes}
    assert skipped_by_url["https://example.com/products/metal/"] == "path_pattern_cap_reached"


def test_planner_budget_pressure_keeps_grouped_floor_consistent() -> None:
    origin = "https://example.com/"
    html = """
    <html><body>
    <nav>
    <a href="/about/">About</a>
    <a href="/contacts/">Contacts</a>
    <a href="/products/">Products</a>
    <a href="/docs/certificate.pdf">Certificate</a>
    <a href="/files/catalog.pdf">Catalog PDF</a>
    <a href="/procurement/tenders/">Procurement</a>
    </nav>
    </body></html>
    """
    responses = {
        origin: _PlannerSmokeResponse(url=origin, text=html),
        "https://example.com/about/": _PlannerSmokeResponse(url="https://example.com/about/", text="<html><body>about</body></html>"),
        "https://example.com/contacts/": _PlannerSmokeResponse(url="https://example.com/contacts/", text="<html><body>contacts</body></html>"),
        "https://example.com/products/": _PlannerSmokeResponse(url="https://example.com/products/", text="<html><body>products</body></html>"),
        "https://example.com/procurement/tenders/": _PlannerSmokeResponse(url="https://example.com/procurement/tenders/", text="<html><body>procurement</body></html>"),
        "https://example.com/docs/certificate.pdf": _PlannerSmokeResponse(
            url="https://example.com/docs/certificate.pdf",
            headers={"Content-Type": "application/pdf"},
            content=b"%PDF-1.4 test",
        ),
        "https://example.com/files/catalog.pdf": _PlannerSmokeResponse(
            url="https://example.com/files/catalog.pdf",
            headers={"Content-Type": "application/pdf"},
            content=b"%PDF-1.4 test",
        ),
    }
    probe = SiteProbe(
        url=origin,
        final_url=origin,
        status="success",
        site_class="A",
        worth_crawling="true",
        html_ok=True,
        sampled_urls=[
            "https://example.com/about/",
            "https://example.com/contacts/",
            "https://example.com/products/",
            "https://example.com/docs/certificate.pdf",
            "https://example.com/files/catalog.pdf",
            "https://example.com/procurement/tenders/",
        ],
    )

    env_updates = {
        "FACTORY_SITE_PLANNER_GLOBAL_CRAWL_BUDGET": "4",
        "FACTORY_SITE_PLANNER_FAMILY_BUDGET_OVERRIDES": '{"company/about": 1, "contacts": 1, "production/products": 1, "docs/certificates": 1, "files": 1, "procurement": 1}',
        "FACTORY_SITE_PLANNER_DEPTH_CAPS": "{}",
        "FACTORY_SITE_PLANNER_HOST_CAPS": "{}",
        "FACTORY_SITE_PLANNER_PATH_PATTERN_CAPS": "{}",
    }
    previous_env = {key: os.environ.get(key) for key in env_updates}
    try:
        for key, value in env_updates.items():
            os.environ[key] = value

        planner = FactorySitePlanner(_PlannerSmokeClient(responses), prober=_StaticProber(probe))
        company = FactorySiteParserCompany(company_id="1", company_name="Budget Pressure", input_site=origin)
        plan = planner.plan(company, max_sites=1)[0]
    finally:
        for key, value in previous_env.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value

    assert [route.route_pattern for route in plan.crawl_queue] == [
        "https://example.com/about/",
        "https://example.com/contacts/",
        "https://example.com/products/",
        "https://example.com/docs/certificate.pdf",
    ]
    assert plan.budget_accounting is not None
    assert plan.budget_accounting.planned_routes == 4

    coverage_by_key = {item.coverage_key: item for item in plan.coverage}
    assert coverage_by_key["docs/files"].covered is True
    assert coverage_by_key["docs/files"].selected_route_family == "docs/certificates"
    assert coverage_by_key["procurement"].covered is False
    assert coverage_by_key["procurement"].skip_reason == "global_budget_exhausted"

    assert plan.crawl_map is not None
    sections_by_family = {item.route_family: item for item in plan.crawl_map.sections}
    assert sections_by_family["docs/certificates"].required_floor is True
    assert sections_by_family["docs/certificates"].floor_met is True
    assert sections_by_family["docs/certificates"].coverage_status == "covered"
    assert sections_by_family["files"].required_floor is True
    assert sections_by_family["files"].floor_met is True
    assert sections_by_family["files"].coverage_status == "group_covered"
    assert sections_by_family["procurement"].required_floor is True
    assert sections_by_family["procurement"].floor_met is False
    assert sections_by_family["procurement"].coverage_status == "required_uncovered"

    family_budgets_by_family = {item.route_family: item for item in plan.budget_accounting.family_budgets}
    assert family_budgets_by_family["docs/certificates"].required_floor is True
    assert family_budgets_by_family["docs/certificates"].floor_met is True
    assert family_budgets_by_family["files"].required_floor is True
    assert family_budgets_by_family["files"].floor_met is True
    assert family_budgets_by_family["files"].planned_count == 0
    assert family_budgets_by_family["procurement"].required_floor is True
    assert family_budgets_by_family["procurement"].floor_met is False

    skipped_by_url = {item.route_pattern: item.skip_reason for item in plan.budget_accounting.skipped_routes}
    assert skipped_by_url["https://example.com/procurement/tenders/"] == "global_budget_exhausted"


def test_route_strategy_restores_from_accounting_key_only() -> None:
    strategy = route_strategy_from_dict({"accounting_key": "example.com:company/about"})

    assert strategy.route_family == "company/about"
    assert strategy.accounting_key == "example.com:company/about"
    assert strategy.counts_toward_coverage is True


def test_planner_docs_only_marks_files_sibling_as_group_covered() -> None:
    plan = _build_smoke_plan(
        html="""
        <html><body>
        <nav>
        <a href="/about/">About</a>
        <a href="/contacts/">Contacts</a>
        <a href="/products/">Products</a>
        <a href="/docs/certificate.pdf">Certificate</a>
        </nav>
        </body></html>
        """,
        sampled_urls=[
            "https://example.com/about/",
            "https://example.com/contacts/",
            "https://example.com/products/",
            "https://example.com/docs/certificate.pdf",
        ],
        responses={
            "https://example.com/about/": _PlannerSmokeResponse(url="https://example.com/about/", text="<html><body>about</body></html>"),
            "https://example.com/contacts/": _PlannerSmokeResponse(url="https://example.com/contacts/", text="<html><body>contacts</body></html>"),
            "https://example.com/products/": _PlannerSmokeResponse(url="https://example.com/products/", text="<html><body>products</body></html>"),
            "https://example.com/docs/certificate.pdf": _PlannerSmokeResponse(
                url="https://example.com/docs/certificate.pdf",
                headers={"Content-Type": "application/pdf"},
                content=b"%PDF-1.4 test",
            ),
        },
        env_updates={
            "FACTORY_SITE_PLANNER_GLOBAL_CRAWL_BUDGET": "4",
            "FACTORY_SITE_PLANNER_FAMILY_BUDGET_OVERRIDES": '{"company/about": 1, "contacts": 1, "production/products": 1, "docs/certificates": 1, "files": 1}',
            "FACTORY_SITE_PLANNER_DEPTH_CAPS": "{}",
            "FACTORY_SITE_PLANNER_HOST_CAPS": "{}",
            "FACTORY_SITE_PLANNER_PATH_PATTERN_CAPS": "{}",
        },
    )

    coverage_by_key = {item.coverage_key: item for item in plan.coverage}
    assert coverage_by_key["docs/files"].covered is True
    assert coverage_by_key["docs/files"].selected_route_family == "docs/certificates"

    assert plan.crawl_map is not None
    sections_by_family = {item.route_family: item for item in plan.crawl_map.sections}
    assert sections_by_family["docs/certificates"].coverage_status == "covered"
    assert sections_by_family["files"].discovered_urls == []
    assert sections_by_family["files"].coverage_status == "group_covered"
    assert sections_by_family["files"].required_floor is True
    assert sections_by_family["files"].floor_met is True


def test_planner_files_only_marks_docs_sibling_as_group_covered() -> None:
    plan = _build_smoke_plan(
        html="""
        <html><body>
        <nav>
        <a href="/about/">About</a>
        <a href="/contacts/">Contacts</a>
        <a href="/products/">Products</a>
        <a href="/files/catalog.pdf">Catalog PDF</a>
        </nav>
        </body></html>
        """,
        sampled_urls=[
            "https://example.com/about/",
            "https://example.com/contacts/",
            "https://example.com/products/",
            "https://example.com/files/catalog.pdf",
        ],
        responses={
            "https://example.com/about/": _PlannerSmokeResponse(url="https://example.com/about/", text="<html><body>about</body></html>"),
            "https://example.com/contacts/": _PlannerSmokeResponse(url="https://example.com/contacts/", text="<html><body>contacts</body></html>"),
            "https://example.com/products/": _PlannerSmokeResponse(url="https://example.com/products/", text="<html><body>products</body></html>"),
            "https://example.com/files/catalog.pdf": _PlannerSmokeResponse(
                url="https://example.com/files/catalog.pdf",
                headers={"Content-Type": "application/pdf"},
                content=b"%PDF-1.4 test",
            ),
        },
        env_updates={
            "FACTORY_SITE_PLANNER_GLOBAL_CRAWL_BUDGET": "4",
            "FACTORY_SITE_PLANNER_FAMILY_BUDGET_OVERRIDES": '{"company/about": 1, "contacts": 1, "production/products": 1, "docs/certificates": 1, "files": 1}',
            "FACTORY_SITE_PLANNER_DEPTH_CAPS": "{}",
            "FACTORY_SITE_PLANNER_HOST_CAPS": "{}",
            "FACTORY_SITE_PLANNER_PATH_PATTERN_CAPS": "{}",
        },
    )

    coverage_by_key = {item.coverage_key: item for item in plan.coverage}
    assert coverage_by_key["docs/files"].covered is True
    assert coverage_by_key["docs/files"].selected_route_family == "files"

    assert plan.crawl_map is not None
    sections_by_family = {item.route_family: item for item in plan.crawl_map.sections}
    assert sections_by_family["files"].coverage_status == "covered"
    assert sections_by_family["docs/certificates"].discovered_urls == []
    assert sections_by_family["docs/certificates"].coverage_status == "group_covered"
    assert sections_by_family["docs/certificates"].required_floor is True
    assert sections_by_family["docs/certificates"].floor_met is True


def test_guessed_subdomain_hard_misses_stop_tail_before_late_successes() -> None:
    run = _build_guessed_subdomain_smoke_plan(with_trace=True)
    plan = run.plan

    assert [route.route_pattern for route in plan.crawl_queue] == [
        "https://example.com/",
        "https://contacts.example.com/",
    ]
    assert run.guessed_request_order == [
        "https://contacts.example.com/",
        "https://office.example.com/",
        "https://catalog.example.com/",
    ]
    assert run.guessed_stop_url == "https://catalog.example.com/"
    assert "https://docs.example.com/" not in run.guessed_request_order

    coverage_by_key = {item.coverage_key: item for item in plan.coverage}
    assert coverage_by_key["docs/files"].discovered is False
    assert coverage_by_key["docs/files"].covered is False
    assert coverage_by_key["docs/files"].skip_reason == "not_discovered"

    assert plan.crawl_map is not None
    sections_by_family = {item.route_family: item for item in plan.crawl_map.sections}
    assert sections_by_family["docs/certificates"].discovered_urls == []
    assert sections_by_family["docs/certificates"].planned_urls == []

    assert plan.budget_accounting is not None
    assert plan.budget_accounting.planned_routes == 2
    family_budgets_by_family = {item.route_family: item for item in plan.budget_accounting.family_budgets}
    assert family_budgets_by_family["docs/certificates"].discovered_count == 0
    assert family_budgets_by_family["docs/certificates"].planned_count == 0


def test_guessed_subdomain_success_path_survives_later_hard_misses() -> None:
    run = _build_guessed_subdomain_smoke_plan(with_trace=True)
    plan = run.plan

    queued_routes_by_url = {route.route_pattern: route for route in plan.crawl_queue}
    assert "https://contacts.example.com/" in queued_routes_by_url
    assert queued_routes_by_url["https://contacts.example.com/"].discovery_sources == ["common_subdomain"]
    assert run.guessed_request_order == [
        "https://contacts.example.com/",
        "https://office.example.com/",
        "https://catalog.example.com/",
    ]
    assert run.guessed_stop_url == "https://catalog.example.com/"
    assert "https://docs.example.com/" not in run.guessed_request_order

    coverage_by_key = {item.coverage_key: item for item in plan.coverage}
    assert coverage_by_key["contacts"].covered is True
    assert coverage_by_key["contacts"].selected_route_family == "contacts"
    assert coverage_by_key["contacts"].selected_url == "https://contacts.example.com/"

    assert plan.crawl_map is not None
    sections_by_family = {item.route_family: item for item in plan.crawl_map.sections}
    assert sections_by_family["contacts"].planned_urls == ["https://contacts.example.com/"]
    assert sections_by_family["contacts"].discovery_sources == ["common_subdomain"]

    assert plan.budget_accounting is not None
    family_budgets_by_family = {item.route_family: item for item in plan.budget_accounting.family_budgets}
    assert family_budgets_by_family["contacts"].planned_count == 1
    assert family_budgets_by_family["contacts"].selected_urls == ["https://contacts.example.com/"]


def test_redirect_to_origin_does_not_retag_homepage_candidate() -> None:
    run = _build_guessed_subdomain_smoke_plan(
        redirect_to_origin=("contacts",),
        hard_misses=("office", "catalog"),
        real_successes=(),
        with_trace=True,
    )
    plan = run.plan

    assert [route.route_pattern for route in plan.crawl_queue] == ["https://example.com/"]
    assert run.guessed_request_order == [
        "https://contacts.example.com/",
        "https://office.example.com/",
        "https://catalog.example.com/",
    ]
    assert run.guessed_stop_url == "https://catalog.example.com/"
    assert run.guessed_requests[0].ok is True
    assert run.guessed_requests[0].response_url == "https://example.com/"

    queued_routes_by_url = {route.route_pattern: route for route in plan.crawl_queue}
    assert queued_routes_by_url["https://example.com/"].route_family == "company/about"
    assert "common_subdomain" not in queued_routes_by_url["https://example.com/"].discovery_sources

    coverage_by_key = {item.coverage_key: item for item in plan.coverage}
    assert coverage_by_key["company/about"].covered is True
    assert coverage_by_key["company/about"].selected_route_family == "company/about"
    assert coverage_by_key["company/about"].selected_url == "https://example.com/"
    assert coverage_by_key["contacts"].covered is False


def test_success_existing_candidate_does_not_corrupt_family() -> None:
    about_url = "https://example.com/about/"
    run = _build_guessed_subdomain_smoke_plan(
        same_domain_successes={"contacts": about_url},
        hard_misses=("office", "catalog"),
        real_successes=(),
        origin_html='<html><body><a href="/about/">About</a></body></html>',
        sampled_urls=(about_url,),
        extra_responses={
            about_url: _PlannerSmokeResponse(
                url=about_url,
                text="<html><body>about</body></html>",
            )
        },
        with_trace=True,
    )
    plan = run.plan

    assert [route.route_pattern for route in plan.crawl_queue] == [about_url]
    assert run.guessed_request_order == [
        "https://contacts.example.com/",
        "https://office.example.com/",
        "https://catalog.example.com/",
    ]
    assert run.guessed_requests[0].ok is True
    assert run.guessed_requests[0].response_url == about_url

    queued_routes_by_url = {route.route_pattern: route for route in plan.crawl_queue}
    assert queued_routes_by_url[about_url].route_family == "company/about"
    assert "common_subdomain" not in queued_routes_by_url[about_url].discovery_sources

    coverage_by_key = {item.coverage_key: item for item in plan.coverage}
    assert coverage_by_key["company/about"].covered is True
    assert coverage_by_key["company/about"].selected_route_family == "company/about"
    assert coverage_by_key["company/about"].selected_url == about_url
    assert coverage_by_key["contacts"].covered is False


def test_bounded_stop_contract_for_existing_same_domain_success() -> None:
    about_url = "https://example.com/about/"
    run = _build_guessed_subdomain_smoke_plan(
        same_domain_successes={"contacts": about_url},
        hard_misses=("office", "catalog"),
        real_successes=("products",),
        origin_html='<html><body><a href="/about/">About</a></body></html>',
        sampled_urls=(about_url,),
        extra_responses={
            about_url: _PlannerSmokeResponse(
                url=about_url,
                text="<html><body>about</body></html>",
            )
        },
        with_trace=True,
    )
    plan = run.plan

    assert [route.route_pattern for route in plan.crawl_queue] == [about_url]
    assert run.guessed_request_order == [
        "https://contacts.example.com/",
        "https://office.example.com/",
        "https://catalog.example.com/",
    ]
    assert run.guessed_stop_url == "https://catalog.example.com/"
    assert run.guessed_requests[0].ok is True
    assert run.guessed_requests[0].response_url == about_url
    assert "https://products.example.com/" not in run.guessed_request_order

    coverage_by_key = {item.coverage_key: item for item in plan.coverage}
    assert coverage_by_key["company/about"].covered is True
    assert coverage_by_key["company/about"].selected_route_family == "company/about"
    assert coverage_by_key["company/about"].selected_url == about_url
    assert coverage_by_key["production/products"].covered is False
    assert coverage_by_key["production/products"].skip_reason == "not_discovered"


def test_late_success_is_cut_only_after_real_success_and_two_hard_misses() -> None:
    run = _build_guessed_subdomain_smoke_plan(
        hard_misses=("office", "catalog"),
        real_successes=("contacts", "products"),
        with_trace=True,
    )
    plan = run.plan

    assert [route.route_pattern for route in plan.crawl_queue] == [
        "https://example.com/",
        "https://contacts.example.com/",
    ]
    assert run.guessed_request_order == [
        "https://contacts.example.com/",
        "https://office.example.com/",
        "https://catalog.example.com/",
    ]
    assert run.guessed_stop_url == "https://catalog.example.com/"
    assert "https://products.example.com/" not in run.guessed_request_order

    coverage_by_key = {item.coverage_key: item for item in plan.coverage}
    assert coverage_by_key["contacts"].covered is True
    assert coverage_by_key["production/products"].covered is False
    assert coverage_by_key["production/products"].skip_reason == "not_discovered"


def test_hard_misses_before_first_success_do_not_arm_stop() -> None:
    run = _build_guessed_subdomain_smoke_plan(
        hard_misses=("contacts", "office", "catalog", "products", "service", "services"),
        real_successes=("shop", "news"),
        with_trace=True,
    )
    plan = run.plan

    assert [route.route_pattern for route in plan.crawl_queue] == [
        "https://example.com/",
        "https://shop.example.com/",
    ]
    assert run.guessed_request_order == [
        "https://contacts.example.com/",
        "https://office.example.com/",
        "https://catalog.example.com/",
        "https://products.example.com/",
        "https://shop.example.com/",
        "https://service.example.com/",
        "https://services.example.com/",
    ]
    assert run.guessed_stop_url == "https://services.example.com/"
    assert run.guessed_request_order.index("https://shop.example.com/") == 4
    assert "https://news.example.com/" not in run.guessed_request_order

    coverage_by_key = {item.coverage_key: item for item in plan.coverage}
    assert coverage_by_key["production/products"].covered is True


def test_normalize_url_returns_empty_string_for_malformed_ipv6_like_url() -> None:
    assert normalize_url("http://[::1") == ""


def test_extract_sitemap_locs_falls_back_when_declared_encoding_is_invalid() -> None:
    planner = FactorySitePlanner(_PlannerSmokeClient({}), prober=SimpleNamespace(probe=lambda site_url: None))
    response = _PlannerSmokeResponse(
        url="https://example.com/sitemap.xml",
        headers={"Content-Type": "application/xml; charset=on"},
        content=(
            b"<?xml version=\"1.0\" encoding=\"on\"?>"
            b"<urlset>"
            b"<url><loc>https://example.com/about/</loc></url>"
            b"</urlset>"
        ),
        encoding="on",
    )

    assert planner._extract_sitemap_locs(response) == ["https://example.com/about/"]


def test_extract_sitemap_locs_keeps_valid_urls_for_normal_sitemap() -> None:
    planner = FactorySitePlanner(_PlannerSmokeClient({}), prober=SimpleNamespace(probe=lambda site_url: None))
    response = _PlannerSmokeResponse(
        url="https://example.com/sitemap.xml",
        headers={"Content-Type": "application/xml; charset=utf-8"},
        text=(
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>"
            "<urlset>"
            "<url><loc>https://example.com/about/</loc></url>"
            "<url><loc>https://example.com/catalog/</loc></url>"
            "</urlset>"
        ),
    )

    assert planner._extract_sitemap_locs(response) == [
        "https://example.com/about/",
        "https://example.com/catalog/",
    ]


def test_planner_ignores_invalid_sitemap_encoding_and_keeps_valid_locs() -> None:
    plan = _build_smoke_plan(
        html="<html><body></body></html>",
        sampled_urls=["https://example.com/robots.txt"],
        responses={
            "https://example.com/robots.txt": _PlannerSmokeResponse(
                url="https://example.com/robots.txt",
                text="User-agent: *\nSitemap: /sitemap.xml\n",
                headers={"Content-Type": "text/plain; charset=utf-8"},
            ),
            "https://example.com/sitemap.xml": _PlannerSmokeResponse(
                url="https://example.com/sitemap.xml",
                headers={"Content-Type": "application/xml; charset=on"},
                content=(
                    b"<?xml version=\"1.0\" encoding=\"on\"?>"
                    b"<urlset>"
                    b"<url><loc>https://example.com/about/</loc></url>"
                    b"</urlset>"
                ),
                encoding="on",
            ),
            "https://example.com/about/": _PlannerSmokeResponse(
                url="https://example.com/about/",
                text="<html><body>about</body></html>",
            ),
        },
        env_updates={
            "FACTORY_SITE_PLANNER_GLOBAL_CRAWL_BUDGET": "1",
            "FACTORY_SITE_PLANNER_FAMILY_BUDGET_OVERRIDES": '{"company/about": 1}',
            "FACTORY_SITE_PLANNER_DEPTH_CAPS": "{}",
            "FACTORY_SITE_PLANNER_HOST_CAPS": "{}",
            "FACTORY_SITE_PLANNER_PATH_PATTERN_CAPS": "{}",
        },
    )

    queued_urls = [route.route_pattern for route in plan.crawl_queue]
    assert "https://example.com/about/" in queued_urls

    assert plan.crawl_map is not None
    discovered_urls = [url for section in plan.crawl_map.sections for url in section.discovered_urls]
    assert "https://example.com/about/" in discovered_urls


def test_planner_skips_malformed_sitemap_urls_and_keeps_valid_locs() -> None:
    plan = _build_smoke_plan(
        html="<html><body></body></html>",
        sampled_urls=["https://example.com/robots.txt"],
        responses={
            "https://example.com/robots.txt": _PlannerSmokeResponse(
                url="https://example.com/robots.txt",
                text="User-agent: *\nSitemap: /sitemap.xml\n",
                headers={"Content-Type": "text/plain; charset=utf-8"},
            ),
            "https://example.com/sitemap.xml": _PlannerSmokeResponse(
                url="https://example.com/sitemap.xml",
                text=(
                    "<?xml version=\"1.0\" encoding=\"UTF-8\"?>"
                    "<urlset>"
                    "<url><loc>http://[::1</loc></url>"
                    "<url><loc>https://example.com/about/</loc></url>"
                    "</urlset>"
                ),
                headers={"Content-Type": "application/xml; charset=utf-8"},
            ),
            "https://example.com/about/": _PlannerSmokeResponse(
                url="https://example.com/about/",
                text="<html><body>about</body></html>",
            ),
        },
        env_updates={
            "FACTORY_SITE_PLANNER_GLOBAL_CRAWL_BUDGET": "1",
            "FACTORY_SITE_PLANNER_FAMILY_BUDGET_OVERRIDES": '{"company/about": 1}',
            "FACTORY_SITE_PLANNER_DEPTH_CAPS": "{}",
            "FACTORY_SITE_PLANNER_HOST_CAPS": "{}",
            "FACTORY_SITE_PLANNER_PATH_PATTERN_CAPS": "{}",
        },
    )

    queued_urls = [route.route_pattern for route in plan.crawl_queue]
    assert "https://example.com/about/" in queued_urls
    assert "http://[::1" not in queued_urls

    assert plan.crawl_map is not None
    discovered_urls = [url for section in plan.crawl_map.sections for url in section.discovered_urls]
    assert "https://example.com/about/" in discovered_urls
    assert "http://[::1" not in discovered_urls


def test_planner_skips_malformed_robots_sitemap_targets_and_keeps_valid_sitemaps() -> None:
    plan = _build_smoke_plan(
        html="<html><body></body></html>",
        sampled_urls=["https://example.com/robots.txt"],
        responses={
            "https://example.com/robots.txt": _PlannerSmokeResponse(
                url="https://example.com/robots.txt",
                text="User-agent: *\nSitemap: http://[::1\nSitemap: /custom-sitemap.xml\n",
                headers={"Content-Type": "text/plain; charset=utf-8"},
            ),
            "https://example.com/custom-sitemap.xml": _PlannerSmokeResponse(
                url="https://example.com/custom-sitemap.xml",
                text=(
                    "<?xml version=\"1.0\" encoding=\"UTF-8\"?>"
                    "<urlset>"
                    "<url><loc>https://example.com/about/</loc></url>"
                    "</urlset>"
                ),
                headers={"Content-Type": "application/xml; charset=utf-8"},
            ),
            "https://example.com/about/": _PlannerSmokeResponse(
                url="https://example.com/about/",
                text="<html><body>about</body></html>",
            ),
        },
        env_updates={
            "FACTORY_SITE_PLANNER_GLOBAL_CRAWL_BUDGET": "1",
            "FACTORY_SITE_PLANNER_FAMILY_BUDGET_OVERRIDES": '{"company/about": 1}',
            "FACTORY_SITE_PLANNER_DEPTH_CAPS": "{}",
            "FACTORY_SITE_PLANNER_HOST_CAPS": "{}",
            "FACTORY_SITE_PLANNER_PATH_PATTERN_CAPS": "{}",
        },
    )

    queued_urls = [route.route_pattern for route in plan.crawl_queue]
    assert "https://example.com/about/" in queued_urls
    assert "http://[::1" not in queued_urls

    assert plan.crawl_map is not None
    discovered_urls = [url for section in plan.crawl_map.sections for url in section.discovered_urls]
    assert "https://example.com/about/" in discovered_urls
    assert "http://[::1" not in discovered_urls


def test_activity_profile_filters_surplus_only_terms() -> None:
    text = (
        "\u0440\u0435\u0430\u043b\u0438\u0437\u0430\u0446\u0438\u044f "
        "\u0440\u0435\u0430\u043b\u0438\u0437\u0443\u0435\u043c "
        "\u043d\u0435\u043b\u0438\u043a\u0432\u0438\u0434\u044b "
        "\u043e\u0442\u0445\u043e\u0434\u044b "
        "\u0432\u0442\u043e\u0440\u0441\u044b\u0440\u044c\u0435 "
        "\u043c\u0435\u0442\u0430\u043b\u043b\u043e\u043b\u043e\u043c "
        "\u0441\u043a\u043b\u0430\u0434\u0441\u043a\u0438\u0435 \u043e\u0441\u0442\u0430\u0442\u043a\u0438 "
        "\u043d\u0435\u0432\u043e\u0441\u0442\u0440\u0435\u0431\u043e\u0432\u0430\u043d\u043d\u044b\u0435 \u0422\u041c\u0426 "
        "\u0434\u0435\u043c\u043e\u043d\u0442\u0430\u0436"
    )
    analyzer, activity_profile = _activity_profile(text)

    assert activity_profile["terms"] == []

    industrial = analyzer._industrial_score(text.lower(), activity_profile)
    assert "activity profile overlaps with site content" not in industrial["reasons"]
    assert not any(item.startswith("activity terms:") for item in industrial["evidence"])


def test_activity_profile_keeps_identity_terms_on_mixed_text() -> None:
    text = (
        "\u0437\u0430\u0432\u043e\u0434 "
        "\u043f\u0440\u043e\u0438\u0437\u0432\u043e\u0434\u0441\u0442\u0432\u043e "
        "\u0446\u0435\u0445 "
        "\u043c\u0435\u0442\u0430\u043b\u043b\u043e\u043a\u043e\u043d\u0441\u0442\u0440\u0443\u043a\u0446\u0438\u0438 "
        "\u043e\u0431\u043e\u0440\u0443\u0434\u043e\u0432\u0430\u043d\u0438\u0435 "
        "\u0440\u0435\u0430\u043b\u0438\u0437\u0430\u0446\u0438\u044f "
        "\u0434\u0435\u043c\u043e\u043d\u0442\u0430\u0436 "
        "\u043f\u0440\u043e\u0434\u0430\u0436\u0430 "
        "\u0441\u043a\u043b\u0430\u0434\u0441\u043a\u0438\u0445 \u043e\u0441\u0442\u0430\u0442\u043a\u043e\u0432"
    )
    analyzer, activity_profile = _activity_profile(text)

    terms = set(activity_profile["terms"])
    assert {
        "\u0437\u0430\u0432\u043e\u0434",
        "\u043f\u0440\u043e\u0438\u0437\u0432\u043e\u0434\u0441\u0442\u0432\u043e",
        "\u0446\u0435\u0445",
        "\u043c\u0435\u0442\u0430\u043b\u043b\u043e\u043a\u043e\u043d\u0441\u0442\u0440\u0443\u043a\u0446\u0438\u0438",
        "\u043e\u0431\u043e\u0440\u0443\u0434\u043e\u0432\u0430\u043d\u0438\u0435",
    }.issubset(terms)
    assert {
        "\u0440\u0435\u0430\u043b\u0438\u0437\u0430\u0446\u0438\u044f",
        "\u0434\u0435\u043c\u043e\u043d\u0442\u0430\u0436",
        "\u043e\u0441\u0442\u0430\u0442\u043a\u043e\u0432",
    }.isdisjoint(terms)

    industrial = analyzer._industrial_score(text.lower(), activity_profile)
    assert "activity profile overlaps with site content" in industrial["reasons"]


def test_activity_profile_identity_only_text_behaves_as_before() -> None:
    text = (
        "\u0437\u0430\u0432\u043e\u0434 "
        "\u043f\u0440\u043e\u0438\u0437\u0432\u043e\u0434\u0441\u0442\u0432\u043e "
        "\u0446\u0435\u0445 "
        "\u043c\u0435\u0442\u0430\u043b\u043b\u043e\u043a\u043e\u043d\u0441\u0442\u0440\u0443\u043a\u0446\u0438\u0438 "
        "\u043e\u0431\u043e\u0440\u0443\u0434\u043e\u0432\u0430\u043d\u0438\u0435"
    )
    analyzer, activity_profile = _activity_profile(text)

    terms = set(activity_profile["terms"])
    assert {
        "\u0437\u0430\u0432\u043e\u0434",
        "\u043f\u0440\u043e\u0438\u0437\u0432\u043e\u0434\u0441\u0442\u0432\u043e",
        "\u0446\u0435\u0445",
        "\u043c\u0435\u0442\u0430\u043b\u043b\u043e\u043a\u043e\u043d\u0441\u0442\u0440\u0443\u043a\u0446\u0438\u0438",
        "\u043e\u0431\u043e\u0440\u0443\u0434\u043e\u0432\u0430\u043d\u0438\u0435",
    }.issubset(terms)

    industrial = analyzer._industrial_score(text.lower(), activity_profile)
    assert "activity profile overlaps with site content" in industrial["reasons"]

from __future__ import annotations

import json
import os
import sys
import tempfile
import types
import unittest
from dataclasses import dataclass
from typing import Any
from unittest import mock

import requests

from app.site_intelligence.factory_site_parser import (
    FactorySiteParser,
    FactorySiteParserCompany,
    FactorySitePlan,
    FactorySiteParserResult,
)
from app.site_intelligence.fetcher import Fetcher
from app.site_intelligence.models import RouteStrategy, SiteProbe


@dataclass
class FakeOutcome:
    ok: bool
    status: str
    response: requests.Response | None = None
    error: str = ""


class FakeHttpClient:
    def __init__(self, mapping: dict[str, FakeOutcome]) -> None:
        self.mapping = mapping
        self.calls: list[dict[str, Any]] = []

    def request(self, url: str, *, source: str, timeout: int | None = None, **kwargs: Any) -> FakeOutcome:
        self.calls.append(
            {
                "url": url,
                "source": source,
                "timeout": timeout,
                "kwargs": dict(kwargs),
            }
        )
        return self.mapping.get(url, FakeOutcome(ok=False, status="request_error", error=f"missing fixture for {url}"))


def make_response(
    *,
    url: str,
    text: str,
    content_type: str = "text/html; charset=utf-8",
    encoding: str = "utf-8",
    status_code: int = 200,
) -> requests.Response:
    response = requests.Response()
    response.status_code = status_code
    response.url = url
    response.encoding = encoding
    response._content = text.encode(encoding, errors="ignore")
    response.headers["Content-Type"] = content_type
    response.history = []
    return response


def html_page(*, body: str, title: str = "Factory Test") -> str:
    return f"<html><head><title>{title}</title></head><body>{body}</body></html>"


def write_proxy_pool_file(tmpdir: str, *, proxies: list[dict[str, str]] | None = None) -> str:
    default_proxies = [
        {
            "id": "px-1",
            "host": "127.0.0.1",
            "port": "8080",
            "user": "demo_user",
            "password": "demo_password",
            "descr": "smoke-proxy",
        }
    ]
    payload = {
        "proxies": default_proxies if proxies is None else proxies
    }
    proxy_file = os.path.join(tmpdir, "proxy_pool.json")
    with open(proxy_file, "w", encoding="utf-8") as handle:
        json.dump(payload, handle)
    return proxy_file


class FakeGotoResponse:
    def __init__(self, status: int = 200) -> None:
        self.status = status


class FakePage:
    def __init__(self, *, url: str, html: str) -> None:
        self.url = url
        self._html = html

    def goto(self, url: str, *, wait_until: str, timeout: int) -> FakeGotoResponse:
        self.url = url
        return FakeGotoResponse(status=200)

    def content(self) -> str:
        return self._html

    def close(self) -> None:
        return None


class FakeContext:
    def __init__(self, page: FakePage) -> None:
        self._page = page

    def new_page(self) -> FakePage:
        return self._page

    def close(self) -> None:
        return None


class FakeBrowser:
    def __init__(self, *, page: FakePage, launch_log: list[dict[str, Any]]) -> None:
        self._page = page
        self._launch_log = launch_log

    def new_context(self, **_: Any) -> FakeContext:
        return FakeContext(self._page)

    def close(self) -> None:
        return None


class FakeChromium:
    def __init__(self, *, page_html: str) -> None:
        self.launch_log: list[dict[str, Any]] = []
        self._page_html = page_html

    def launch(self, **kwargs: Any) -> FakeBrowser:
        self.launch_log.append(dict(kwargs))
        return FakeBrowser(
            page=FakePage(url="https://factory.example/js", html=self._page_html),
            launch_log=self.launch_log,
        )


class FakePlaywrightManager:
    def __init__(self, chromium: FakeChromium) -> None:
        self._chromium = chromium

    def __enter__(self) -> Any:
        return types.SimpleNamespace(chromium=self._chromium)

    def __exit__(self, exc_type: Any, exc: Any, tb: Any) -> bool:
        return False


class FactorySiteParserSmokeTests(unittest.TestCase):
    def test_factory_site_parser_dry_run_smoke(self) -> None:
        homepage_url = "https://factory.example/"
        homepage_body = """
        <section>Metal fabrication and industrial production.</section>
        <p>Factory homepage only.</p>
        """
        fake_client = FakeHttpClient(
            {
                homepage_url: FakeOutcome(
                    ok=True,
                    status="success",
                    response=make_response(
                        url=homepage_url,
                        text=html_page(body=homepage_body, title="Factory Site"),
                    ),
                ),
            }
        )
        planner = mock.Mock()
        company = FactorySiteParserCompany(
            company_id="7700000000",
            company_name="Test Factory",
            candidate_sites=[homepage_url],
            known_okved_codes=["25.11"],
            activity_terms=["metal", "production"],
        )
        planner_plan = FactorySitePlan(
            site_url=homepage_url,
            probe=SiteProbe(
                url=homepage_url,
                final_url=homepage_url,
                status="success",
                site_class="B",
                worth_crawling="true",
                html_ok=True,
            ),
            notes=["planner did not produce executable crawl queue"],
        )
        planner.plan.return_value = [planner_plan]
        parser = FactorySiteParser(fake_client, planner=planner)

        with mock.patch.object(parser.fetch_stage, "fetch", wraps=parser.fetch_stage.fetch) as fetch_spy:
            result = parser.parse(company, dry_run=True)

        planner.plan.assert_called_once_with(company, max_sites=1)
        fetch_spy.assert_called_once()
        self.assertEqual(planner_plan.routes, [])
        self.assertEqual(planner_plan.notes, ["planner did not produce executable crawl queue"])
        self.assertTrue(fetch_spy.call_args.kwargs["dry_run"])
        self.assertIsInstance(result, FactorySiteParserResult)
        self.assertEqual(len(result.plans), 1)
        plan = result.plans[0]
        self.assertEqual(plan.site_url, homepage_url)
        self.assertEqual(len(plan.routes), 1)
        fallback_route = plan.routes[0]
        self.assertEqual(fallback_route.route_pattern, homepage_url)
        self.assertEqual(fallback_route.route_family, "company/about")
        self.assertEqual(fallback_route.section_guess, "about")
        self.assertIn("dry_run homepage fallback", fallback_route.reasons)
        self.assertIn("dry_run_fallback", fallback_route.discovery_sources)
        self.assertEqual(result.site_probes[0].status, "success")
        self.assertEqual(len(result.route_strategies), 1)
        self.assertEqual(result.route_strategies[0].route_pattern, homepage_url)
        self.assertIsInstance(result.relevance_summary, dict)
        self.assertIsInstance(result.lead_assembly, dict)
        self.assertIsInstance(result.crawl_execution, dict)
        self.assertIsInstance(result.visited_route_families, list)
        self.assertIsInstance(result.page_records, int)
        self.assertIsInstance(result.document_records, int)
        self.assertGreaterEqual(len(result.content_records), 1)
        self.assertGreaterEqual(len(plan.fetch_telemetry), 1)
        self.assertGreaterEqual(len(result.fetch_telemetry), 1)
        self.assertEqual(plan.fetch_telemetry, result.fetch_telemetry)
        self.assertTrue(all(item.url == homepage_url for item in plan.fetch_telemetry))
        self.assertTrue(
            any(
                item.route_family == fallback_route.route_family and item.section_name == fallback_route.section_guess
                for item in plan.fetch_telemetry
            )
        )
        self.assertTrue(plan.access_state)
        self.assertTrue(plan.breaker_mode)
        for field_name, expected_type in (
            ("access_state", str),
            ("block_class", str),
            ("anti_bot_reason", str),
            ("breaker_mode", str),
            ("manual_handoff_required", bool),
            ("challenge_detected", bool),
            ("session_reused", bool),
        ):
            self.assertTrue(hasattr(plan, field_name))
            self.assertIsInstance(getattr(plan, field_name), expected_type, field_name)
        self.assertIn("dry-run homepage fallback injected because planner returned empty queue", plan.notes)
        self.assertIn("planner did not produce executable crawl queue", result.notes)
        self.assertIn("dry-run homepage fallback injected because planner returned empty queue", result.notes)
        self.assertTrue(
            any(call["source"] == "route_fetch" and call["url"] == homepage_url for call in fake_client.calls)
        )

    def test_factory_site_parser_uses_parser_proxies_file_for_requests_telemetry(self) -> None:
        homepage_url = "https://factory.example/"
        long_body = "<p>" + ("factory data " * 40) + "</p>"
        fake_client = FakeHttpClient(
            {
                homepage_url: FakeOutcome(
                    ok=True,
                    status="success",
                    response=make_response(url=homepage_url, text=html_page(body=long_body, title="Factory")),
                ),
            }
        )

        with tempfile.TemporaryDirectory() as tmpdir:
            proxy_file = write_proxy_pool_file(tmpdir)
            with mock.patch.dict(os.environ, {"PARSER_PROXIES_FILE": proxy_file}, clear=False):
                parser = FactorySiteParser(fake_client)
                company = FactorySiteParserCompany(
                    company_id="7700000000",
                    company_name="Test Factory",
                    candidate_sites=[homepage_url],
                )

                result = parser.parse(company, dry_run=True)

        route_fetch_call = next(call for call in fake_client.calls if call["source"] == "route_fetch")
        proxy_selection = route_fetch_call["kwargs"]["proxy_selection"]
        expected_proxy_url = "http://" + "demo_user:demo_password" + "@127.0.0.1:8080"
        self.assertEqual(proxy_selection.proxy_id, "px-1")
        self.assertEqual(proxy_selection.proxy_label_or_id, "px-1")
        self.assertEqual(proxy_selection.requests_proxies["http"], expected_proxy_url)
        self.assertEqual(result.fetch_telemetry[0].proxy_mode, "proxy")
        self.assertEqual(result.fetch_telemetry[0].proxy_label_or_id, "px-1")
        self.assertFalse(result.fetch_telemetry[0].playwright_fallback_used)

    def test_fetcher_passes_proxy_to_playwright_and_marks_fallback(self) -> None:
        homepage_url = "https://factory.example/js"
        fake_client = FakeHttpClient(
            {
                homepage_url: FakeOutcome(
                    ok=True,
                    status="success",
                    response=make_response(url=homepage_url, text=html_page(body="<p>short</p>", title="JS")),
                ),
            }
        )
        chromium = FakeChromium(page_html=html_page(body="<p>" + ("rendered content " * 40) + "</p>", title="Rendered"))
        sync_api_module = types.ModuleType("playwright.sync_api")
        sync_api_module.TimeoutError = RuntimeError
        sync_api_module.sync_playwright = lambda: FakePlaywrightManager(chromium)
        playwright_module = types.ModuleType("playwright")
        playwright_module.sync_api = sync_api_module

        with tempfile.TemporaryDirectory() as tmpdir:
            proxy_file = write_proxy_pool_file(
                tmpdir,
                proxies=[
                    {
                        "id": "px-1",
                        "host": "127.0.0.1",
                        "port": "8080",
                        "user": "demo_user",
                        "password": "demo_password",
                        "descr": "smoke-proxy",
                    },
                ],
            )
            env = {
                "PARSER_PROXIES_FILE": proxy_file,
                "ENABLE_PLAYWRIGHT_SITE_FETCH": "1",
            }
            with mock.patch.dict(os.environ, env, clear=False):
                with mock.patch.dict(sys.modules, {"playwright": playwright_module, "playwright.sync_api": sync_api_module}):
                    fetcher = Fetcher(fake_client)
                    # Exercise the homepage route contract: requests first, then Playwright fallback.
                    # Empty route context would instead hit unknown-route policy denial.
                    response, status, notes = fetcher.fetch(
                        homepage_url,
                        "hybrid",
                        route_family="homepage",
                        section_name="homepage",
                    )

        self.assertIsNotNone(response)
        self.assertEqual(status, "success")
        self.assertIn("policy escalated requests path to playwright", notes)
        self.assertNotIn("blocked_no_alternative_proxy", notes)
        route_fetch_call = next(call for call in fake_client.calls if call["source"] == "route_fetch")
        proxy_selection = route_fetch_call["kwargs"]["proxy_selection"]
        self.assertEqual(proxy_selection.proxy_id, "px-1")
        self.assertTrue(chromium.launch_log)
        self.assertEqual(chromium.launch_log[0]["proxy"]["server"], "http://127.0.0.1:8080")
        self.assertEqual(chromium.launch_log[0]["proxy"]["username"], "demo_user")
        self.assertEqual(chromium.launch_log[0]["proxy"]["password"], "demo_password")
        self.assertIsNotNone(fetcher.last_fetch_telemetry)
        self.assertTrue(fetcher.last_fetch_telemetry.playwright_fallback_used)
        self.assertEqual(fetcher.last_fetch_telemetry.proxy_mode, "proxy")
        self.assertEqual(fetcher.last_fetch_telemetry.proxy_label_or_id, "px-1")

    def test_parser_keeps_heavy_fetch_embargo_for_non_trusted_site(self) -> None:
        homepage_url = "https://ambiguous.example/"
        document_url = "https://ambiguous.example/files/cert.pdf"
        homepage_body = """
        <p>Supplier portal.</p>
        <p>Catalog.</p>
        <a href="/files/cert.pdf">Download certificate</a>
        """
        fake_client = FakeHttpClient(
            {
                homepage_url: FakeOutcome(
                    ok=True,
                    status="success",
                    response=make_response(
                        url=homepage_url,
                        text=html_page(body=homepage_body, title="Supplier Portal"),
                    ),
                ),
            }
        )
        planner = mock.Mock()
        company = FactorySiteParserCompany(
            company_id="7700000000",
            company_name="Test Factory",
            candidate_sites=[homepage_url],
            known_okved_codes=["25.11"],
            activity_terms=["metal", "production"],
        )
        planner.plan.return_value = [
            FactorySitePlan(
                site_url=homepage_url,
                probe=SiteProbe(
                    url=homepage_url,
                    final_url=homepage_url,
                    status="success",
                    site_class="B",
                    worth_crawling="true",
                    html_ok=True,
                ),
                routes=[
                    RouteStrategy(
                        site_url=homepage_url,
                        route_pattern=homepage_url,
                        section_guess="about",
                        mode="hybrid",
                        confidence=0.91,
                        route_family="company/about",
                    ),
                    RouteStrategy(
                        site_url=homepage_url,
                        route_pattern=document_url,
                        section_guess="files",
                        mode="hybrid",
                        confidence=0.88,
                        route_family="files",
                    ),
                ],
            )
        ]
        parser = FactorySiteParser(fake_client, planner=planner)
        parser.fetch_stage.fetcher.playwright_enabled = True

        with mock.patch.object(
            parser.fetch_stage.fetcher,
            "_playwright_fetch",
            side_effect=AssertionError("playwright branch should stay embargoed before trusted state"),
        ) as playwright_spy:
            result = parser.parse(company)

        self.assertEqual(playwright_spy.call_count, 0)
        self.assertFalse(any(call["url"] == document_url for call in fake_client.calls))
        self.assertEqual(result.document_records, 0)
        self.assertEqual(result.plans[0].fetch_policy.heavy_fetch_embargo, True)
        self.assertNotEqual(result.plans[0].trust_state, "trusted")
        self.assertTrue(
            any(
                item.get("reason") == "trust_embargo_heavy_route"
                and item.get("route_pattern") == document_url
                for item in result.crawl_execution.get("skipped_routes", [])
            )
        )


if __name__ == "__main__":
    unittest.main()

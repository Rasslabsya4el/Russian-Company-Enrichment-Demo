from __future__ import annotations

import os
import unittest
from dataclasses import dataclass
from unittest.mock import patch

import requests

from app.site_intelligence.probe import SiteProber


@dataclass
class FakeOutcome:
    ok: bool
    status: str
    response: requests.Response | None = None
    error: str = ""


class FakeClient:
    def __init__(self, mapping: dict[str, FakeOutcome]) -> None:
        self.mapping = mapping
        self.calls: list[tuple[str, str, int | None]] = []

    def request(self, url: str, *, source: str, timeout: int | None = None) -> FakeOutcome:
        self.calls.append((url, source, timeout))
        return self.mapping.get(url, FakeOutcome(ok=False, status="request_error", error=f"missing fixture for {url}"))


def make_response(
    *,
    url: str,
    text: str,
    content_type: str = "text/html; charset=utf-8",
    encoding: str = "utf-8",
    status_code: int = 200,
    history: list[requests.Response] | None = None,
) -> requests.Response:
    response = requests.Response()
    response.status_code = status_code
    response.url = url
    response.encoding = encoding
    response._content = text.encode(encoding, errors="ignore")
    response.headers["Content-Type"] = content_type
    response.history = history or []
    return response


def html_page(*, body: str, title: str = "Test", description: str = "") -> str:
    description_tag = f'<meta name="description" content="{description}">' if description else ""
    return f"<html><head><title>{title}</title>{description_tag}</head><body>{body}</body></html>"


def outcome_from_html(
    url: str,
    body: str,
    *,
    title: str = "Test",
    content_type: str = "text/html; charset=utf-8",
    encoding: str = "utf-8",
) -> FakeOutcome:
    return FakeOutcome(
        ok=True,
        status="success",
        response=make_response(url=url, text=html_page(body=body, title=title), content_type=content_type, encoding=encoding),
    )


def missing_outcome(url: str) -> tuple[str, FakeOutcome]:
    return url, FakeOutcome(ok=False, status="request_error", error="missing")


def full_obvious_route_failures(domain: str) -> dict[str, FakeOutcome]:
    return dict(
        [
            missing_outcome(f"https://{domain}/robots.txt"),
            missing_outcome(f"https://{domain}/sitemap.xml"),
            missing_outcome(f"https://{domain}/contacts"),
            missing_outcome(f"https://{domain}/kontakt"),
            missing_outcome(f"https://{domain}/kontakty"),
            missing_outcome(f"https://{domain}/about"),
            missing_outcome(f"https://{domain}/o-kompanii"),
            missing_outcome(f"https://{domain}/company"),
            missing_outcome(f"https://{domain}/procurement"),
            missing_outcome(f"https://{domain}/zakupki"),
            missing_outcome(f"https://{domain}/tenders"),
            missing_outcome(f"https://{domain}/torgi"),
            missing_outcome(f"https://{domain}/news"),
            missing_outcome(f"https://{domain}/documents"),
            missing_outcome(f"https://{domain}/docs"),
        ]
    )


class SiteProberTests(unittest.TestCase):
    def test_site_probe_class_a_collects_full_obvious_routes(self) -> None:
        homepage_url = "https://factory.example/"
        routes = {
            "https://factory.example/contacts": outcome_from_html("https://factory.example/contacts", "<p>Sales contacts</p>"),
            "https://factory.example/about": outcome_from_html("https://factory.example/about", "<p>About company and metal structures production</p>"),
            "https://factory.example/procurement": outcome_from_html("https://factory.example/procurement", "<p>Procurement procedures</p>"),
            "https://factory.example/tenders": outcome_from_html("https://factory.example/tenders", "<p>Tenders and bids</p>"),
            "https://factory.example/news": outcome_from_html("https://factory.example/news", "<p>Factory news</p>"),
            "https://factory.example/documents": outcome_from_html("https://factory.example/documents", "<a href='/files/spec.pdf'>PDF</a>"),
        }
        homepage_body = """
        <nav>
          <a href="/contacts">Contacts</a>
          <a href="/about">About</a>
          <a href="/procurement">Procurement</a>
          <a href="/tenders">Tenders</a>
          <a href="/news">News</a>
          <a href="/documents">Documents</a>
          <a href="/production">Production</a>
          <a href="/factory">Factory</a>
          <a href="/services">Services</a>
          <a href="/files/spec.pdf">Download PDF</a>
        </nav>
        <section>{}</section>
        """.format("metal production " * 120)
        mapping = {
            homepage_url: FakeOutcome(
                ok=True,
                status="success",
                response=make_response(url=homepage_url, text=html_page(body=homepage_body, title="Factory Magistral")),
            ),
            "https://factory.example/robots.txt": FakeOutcome(
                ok=True,
                status="success",
                response=make_response(url="https://factory.example/robots.txt", text="User-agent: *\nAllow: /\n", content_type="text/plain; charset=utf-8"),
            ),
            "https://factory.example/sitemap.xml": FakeOutcome(
                ok=True,
                status="success",
                response=make_response(url="https://factory.example/sitemap.xml", text="<xml></xml>", content_type="application/xml; charset=utf-8"),
            ),
            **routes,
        }
        client = FakeClient(mapping)
        probe = SiteProber(client).probe(homepage_url)

        self.assertEqual(probe.status, "success")
        self.assertEqual(probe.site_class, "A")
        self.assertEqual(probe.worth_crawling, "true")
        self.assertEqual(probe.content_type, "text/html; charset=utf-8")
        self.assertEqual(probe.encoding, "utf-8")
        self.assertTrue(probe.robots_found)
        self.assertTrue(probe.sitemap_found)
        self.assertGreaterEqual(probe.internal_links_count, 9)
        self.assertGreaterEqual(probe.document_links_count, 1)
        self.assertGreaterEqual(len(probe.obvious_routes_attempted), 6)
        self.assertIn("https://factory.example/contacts", probe.obvious_routes_attempted)
        self.assertIn("https://factory.example/about", probe.obvious_routes_attempted)
        self.assertIn("https://factory.example/news", probe.obvious_routes_attempted)
        self.assertIn("https://factory.example/documents", probe.obvious_routes_attempted)
        self.assertNotIn("https://factory.example/contact", probe.obvious_routes_attempted)
        self.assertNotIn("https://factory.example/o-kompanii", probe.obvious_routes_attempted)
        self.assertNotIn("https://factory.example/press", probe.obvious_routes_attempted)
        self.assertNotIn("https://factory.example/document", probe.obvious_routes_attempted)
        self.assertIn(("https://factory.example/robots.txt", "site_probe_aux", 8), client.calls)
        self.assertIn(("https://factory.example/sitemap.xml", "site_probe_aux", 8), client.calls)
        self.assertIn("contacts", probe.key_sections)

    def test_site_probe_mixed_real_and_synthetic_routes_ignore_weak_slug_matches(self) -> None:
        homepage_url = "https://mixed-routes.example/"
        homepage_body = """
        <nav>
          <a href="/contacts">Contacts</a>
          <a href="/about">About</a>
          <a href="/newsletter">Newsletter</a>
          <a href="/productivity">Productivity</a>
        </nav>
        <section>{}</section>
        """.format("industrial production and factory supply " * 60)
        mapping = {
            homepage_url: FakeOutcome(
                ok=True,
                status="success",
                response=make_response(url=homepage_url, text=html_page(body=homepage_body, title="Mixed Routes Factory")),
            ),
            **full_obvious_route_failures("mixed-routes.example"),
            "https://mixed-routes.example/contacts": outcome_from_html("https://mixed-routes.example/contacts", "<p>Sales contacts</p>"),
            "https://mixed-routes.example/about": outcome_from_html("https://mixed-routes.example/about", "<p>About the factory</p>"),
        }

        with patch.dict(os.environ, {"SITE_PROBE_MAX_ROUTES_PER_SECTION": "1"}):
            probe = SiteProber(FakeClient(mapping)).probe(homepage_url)

        self.assertIn("https://mixed-routes.example/contacts", probe.obvious_routes_attempted)
        self.assertNotIn("https://mixed-routes.example/contact", probe.obvious_routes_attempted)
        self.assertIn("https://mixed-routes.example/about", probe.obvious_routes_attempted)
        self.assertNotIn("https://mixed-routes.example/o-kompanii", probe.obvious_routes_attempted)
        self.assertIn("https://mixed-routes.example/news", probe.obvious_routes_attempted)
        self.assertNotIn("https://mixed-routes.example/newsletter", probe.obvious_routes_attempted)
        self.assertIn("https://mixed-routes.example/products", probe.obvious_routes_attempted)
        self.assertNotIn("https://mixed-routes.example/productivity", probe.obvious_routes_attempted)
        self.assertIn("contacts", probe.key_sections)

    def test_site_probe_rejected_href_classes_do_not_enter_obvious_route_candidates(self) -> None:
        homepage_url = "https://rejected-hrefs.example/"
        homepage_body = """
        <nav>
          <a href="https://external.example/contacts">External contacts</a>
          <a href="mailto:sales@rejected-hrefs.example">Email contacts</a>
          <a href="tel:+74951234567">Phone contacts</a>
          <a href="javascript:void(0)">Javascript contacts</a>
          <a href="#contacts">Fragment contacts</a>
          <a href="/about">About</a>
        </nav>
        <section>{}</section>
        """.format("industrial production and procurement " * 60)
        mapping = {
            homepage_url: FakeOutcome(
                ok=True,
                status="success",
                response=make_response(url=homepage_url, text=html_page(body=homepage_body, title="Rejected Hrefs Factory")),
            ),
            **full_obvious_route_failures("rejected-hrefs.example"),
            "https://rejected-hrefs.example/about": outcome_from_html("https://rejected-hrefs.example/about", "<p>About the factory</p>"),
        }

        with patch.dict(os.environ, {"SITE_PROBE_MAX_ROUTES_PER_SECTION": "1"}):
            probe = SiteProber(FakeClient(mapping)).probe(homepage_url)

        self.assertIn("https://rejected-hrefs.example/contacts", probe.obvious_routes_attempted)
        self.assertIn("https://rejected-hrefs.example/about", probe.obvious_routes_attempted)
        self.assertNotIn("https://external.example/contacts", probe.obvious_routes_attempted)
        self.assertNotIn("mailto:sales@rejected-hrefs.example", probe.obvious_routes_attempted)
        self.assertNotIn("tel:+74951234567", probe.obvious_routes_attempted)
        self.assertNotIn("javascript:void(0)", probe.obvious_routes_attempted)
        self.assertNotIn("https://rejected-hrefs.example/#contacts", probe.obvious_routes_attempted)
        self.assertNotIn(homepage_url, probe.obvious_routes_attempted)

    def test_site_probe_purely_synthetic_routes_use_one_canonical_route_per_section(self) -> None:
        homepage_url = "https://fallback.example/"
        homepage_body = """
        <nav>
          <a href="/alpha">Alpha</a>
          <a href="/beta">Beta</a>
          <a href="/gamma">Gamma</a>
          <a href="/delta">Delta</a>
        </nav>
        <section>{}</section>
        """.format("industrial procurement and warehouse stock " * 40)
        mapping = {
            homepage_url: FakeOutcome(
                ok=True,
                status="success",
                response=make_response(url=homepage_url, text=html_page(body=homepage_body, title="Fallback Factory")),
            ),
            **full_obvious_route_failures("fallback.example"),
        }

        probe = SiteProber(FakeClient(mapping)).probe(homepage_url)

        self.assertEqual(
            probe.obvious_routes_attempted,
            [
                "https://fallback.example/sale",
                "https://fallback.example/contacts",
                "https://fallback.example/about",
                "https://fallback.example/products",
                "https://fallback.example/services",
                "https://fallback.example/procurement",
                "https://fallback.example/news",
                "https://fallback.example/documents",
                "https://fallback.example/vacancies",
                "https://fallback.example/branch",
                "https://fallback.example/files",
                "https://fallback.example/search",
            ],
        )
        self.assertNotIn("https://fallback.example/sales", probe.obvious_routes_attempted)
        self.assertNotIn("https://fallback.example/contact", probe.obvious_routes_attempted)
        self.assertNotIn("https://fallback.example/service", probe.obvious_routes_attempted)
        self.assertTrue(probe.html_ok)

    def test_site_probe_class_b_detects_legacy_cp1251_html(self) -> None:
        homepage_url = "https://legacy.example/"
        html = "<meta charset='windows-1251'><a href='/contacts'>Contacts</a><p>{}</p>".format("legacy factory " * 40)
        mapping = {
            homepage_url: FakeOutcome(
                ok=True,
                status="success",
                response=make_response(
                    url=homepage_url,
                    text=html_page(body=html, title="Legacy Factory"),
                    content_type="text/html; charset=windows-1251",
                    encoding="cp1251",
                ),
            ),
            **full_obvious_route_failures("legacy.example"),
            "https://legacy.example/contacts": outcome_from_html("https://legacy.example/contacts", "<p>Contacts</p>"),
        }
        probe = SiteProber(FakeClient(mapping)).probe(homepage_url)

        self.assertEqual(probe.status, "success")
        self.assertEqual(probe.site_class, "B")
        self.assertEqual(probe.encoding, "cp1251")
        self.assertTrue(probe.html_ok)

    def test_site_probe_class_c_detects_mixed_js(self) -> None:
        homepage_url = "https://mixed.example/"
        body = "<div id='root'></div><a href='/contacts'>Contacts</a><p>{}</p>".format("production and factory " * 60)
        mapping = {
            homepage_url: outcome_from_html(homepage_url, body, title="Mixed"),
            **full_obvious_route_failures("mixed.example"),
            "https://mixed.example/contacts": outcome_from_html("https://mixed.example/contacts", "<p>Contacts</p>"),
        }
        probe = SiteProber(FakeClient(mapping)).probe(homepage_url)

        self.assertEqual(probe.site_class, "C")
        self.assertFalse(probe.browser_required_default)
        self.assertIn(probe.worth_crawling, {"true", "limited"})

    def test_site_probe_class_d_detects_js_shell(self) -> None:
        homepage_url = "https://spa.example/"
        body = "<div id='root'></div><script>webpackChunkName='app'</script>"
        mapping = {
            homepage_url: outcome_from_html(homepage_url, body, title="SPA"),
            **full_obvious_route_failures("spa.example"),
        }
        probe = SiteProber(FakeClient(mapping)).probe(homepage_url)

        self.assertEqual(probe.site_class, "D")
        self.assertTrue(probe.browser_required_default)
        self.assertEqual(probe.worth_crawling, "limited")

    def test_site_probe_class_e_for_antibot_failure(self) -> None:
        homepage_url = "https://blocked.example/"
        probe = SiteProber(FakeClient({homepage_url: FakeOutcome(ok=False, status="bot_gate", error="captcha gate")})).probe(homepage_url)

        self.assertEqual(probe.site_class, "E")
        self.assertTrue(probe.anti_bot_detected)
        self.assertEqual(probe.failure_reason, "bot_gate")
        self.assertTrue(probe.browser_required_default)

    def test_site_probe_class_f_for_timeout_failure(self) -> None:
        homepage_url = "https://dead.example/"
        probe = SiteProber(
            FakeClient({homepage_url: FakeOutcome(ok=False, status="request_error", error="HTTPSConnectionPool timed out while reading")})
        ).probe(homepage_url)

        self.assertEqual(probe.site_class, "F")
        self.assertEqual(probe.worth_crawling, "false")
        self.assertEqual(probe.failure_reason, "request_error")
        self.assertEqual(probe.timeout_reason, "request_timeout")


if __name__ == "__main__":
    unittest.main()

from __future__ import annotations

import unittest

from app.site_intelligence.factory_site_parser.models import FactorySiteParserCompany, FactorySitePlan
from app.site_intelligence.factory_site_parser.okved_match import FactorySiteOkvedMatcher
from app.site_intelligence.factory_site_parser.store import FactorySiteStore
from app.site_intelligence.models import ContentRecord, SiteProbe


def make_company() -> FactorySiteParserCompany:
    return FactorySiteParserCompany(
        company_id="7700000000",
        company_name="Завод металлоконструкций Тест",
        candidate_sites=["https://factory.example/"],
        known_okved_codes=["25.11"],
        activity_terms=["металлоконструкции", "сварка", "производство"],
        source_snippets=[
            "Производство строительных металлоконструкций и сварных изделий.",
            "Собственный цех и выпуск продукции по ГОСТ.",
        ],
        source_notes=["Металлоконструкции для промышленных объектов и производственных площадок."],
    )


def make_record(
    *,
    url: str,
    title: str,
    body: str,
    fingerprint: str,
    section_guess: str = "",
) -> ContentRecord:
    return ContentRecord(
        company_id="7700000000",
        site_url="https://factory.example/",
        url=url,
        source_type="html",
        title=title,
        raw_text=body,
        cleaned_text=body,
        section_guess=section_guess,
        fetch_status="success",
        content_fingerprint=fingerprint,
    )


class FactorySiteOkvedMatchSmokeTests(unittest.TestCase):
    def setUp(self) -> None:
        self.matcher = FactorySiteOkvedMatcher()
        self.company = make_company()

    def test_real_factory_site_scores_strong_match_and_store_keeps_trace(self) -> None:
        record = make_record(
            url="https://factory.example/production",
            title="Производство металлоконструкций",
            body=(
                "Завод металлоконструкций. Собственное производство, производственный цех, "
                "выпускаем продукцию по ГОСТ. Производственные мощности, сварка и ОКВЭД 25.11."
            ),
            fingerprint="factory-positive",
            section_guess="production",
        )

        profile, matches = self.matcher.match_records(self.company, [record])
        match = matches[0]

        self.assertEqual(match.verdict, "strong_match")
        self.assertEqual(profile.site_match.verdict, "strong_match")
        self.assertTrue(any(item.signal_group == "industrial_identity_positive" for item in match.positive_evidence))
        self.assertTrue(any(item.signal_group == "product_or_process_positive" for item in match.positive_evidence))

        store = FactorySiteStore()
        result = store.build_result(
            company=self.company,
            plans=[FactorySitePlan(site_url="https://factory.example/", probe=SiteProbe(url="https://factory.example/", status="success"))],
            content_records=[record],
            okved_profile=profile,
            okved_matches=matches,
        )

        self.assertIsNotNone(result.okved_site_match)
        self.assertEqual(result.okved_site_match.verdict, "strong_match")
        trace_payload = result.content_records[0].trace["factory_site_parser"]["okved_match"]
        self.assertEqual(trace_payload["verdict"], "strong_match")
        self.assertTrue(trace_payload["positive_evidence"])
        self.assertIn("signal_breakdown", trace_payload)

    def test_dealer_reseller_page_scores_mismatch(self) -> None:
        record = make_record(
            url="https://factory.example/brands",
            title="Официальный дилер металлоконструкций",
            body=(
                "Официальный дилер. Каталог брендов, официальный партнер производителей, "
                "комплексные поставки и продажа оборудования со складской программой."
            ),
            fingerprint="dealer-negative",
            section_guess="dealers",
        )

        profile, matches = self.matcher.match_records(self.company, [record])
        match = matches[0]

        self.assertEqual(match.verdict, "mismatch")
        self.assertEqual(profile.site_match.verdict, "mismatch")
        self.assertTrue(any(item.signal_group == "dealer_negative" for item in match.negative_evidence))
        self.assertTrue(any(item.signal_group == "reseller_negative" for item in match.negative_evidence))

    def test_portal_catalog_page_scores_mismatch(self) -> None:
        record = make_record(
            url="https://factory.example/companies/test-factory",
            title="Карточка компании",
            body=(
                "Каталог предприятий. База поставщиков и карточка компании. "
                "Список компаний, поиск поставщиков и предприятия России."
            ),
            fingerprint="portal-negative",
            section_guess="catalog",
        )

        profile, matches = self.matcher.match_records(self.company, [record])
        match = matches[0]

        self.assertEqual(match.verdict, "mismatch")
        self.assertEqual(profile.site_match.verdict, "mismatch")
        self.assertTrue(any(item.signal_group == "portal_catalog_negative" for item in match.negative_evidence))

    def test_unrelated_service_page_scores_mismatch(self) -> None:
        record = make_record(
            url="https://factory.example/services/legal",
            title="Юридические услуги для бизнеса",
            body=(
                "Юридические услуги, бухгалтерское сопровождение, налоговый консалтинг, "
                "аудит и регистрация ООО."
            ),
            fingerprint="service-negative",
            section_guess="services",
        )

        profile, matches = self.matcher.match_records(self.company, [record])
        match = matches[0]

        self.assertEqual(match.verdict, "mismatch")
        self.assertEqual(profile.site_match.verdict, "mismatch")
        self.assertTrue(any(item.signal_group == "unrelated_service_negative" for item in match.negative_evidence))

    def test_generic_corporate_page_is_not_strong_match(self) -> None:
        record = make_record(
            url="https://factory.example/about",
            title="О компании",
            body=(
                "Завод металлоконструкций Тест. История компании, миссия, контакты и команда. "
                "Надежный партнер на рынке."
            ),
            fingerprint="generic-uncertain",
            section_guess="about",
        )

        profile, matches = self.matcher.match_records(self.company, [record])
        match = matches[0]

        self.assertNotEqual(match.verdict, "strong_match")
        self.assertIn(match.verdict, {"uncertain", "weak_match"})
        self.assertNotEqual(profile.site_match.verdict, "strong_match")
        self.assertIn(profile.site_match.verdict, {"uncertain", "weak_match"})


if __name__ == "__main__":
    unittest.main()

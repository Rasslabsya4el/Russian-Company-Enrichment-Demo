from __future__ import annotations

import json
import logging
import re
from pathlib import Path
from types import SimpleNamespace

import company_enrichment_core as core
from app.llm.benchmark_capture import BenchmarkAwareSiteAuthenticityAnalyzer, LLMBenchmarkCaptureConfig, LLMBenchmarkCaptureWriter
from app.site_intelligence.common import compact_text, dedupe_preserve_order, normalize_whitespace
from app.site_intelligence.factory_site_parser.models import FactorySitePlan
from app.site_intelligence.models import SiteProbe
from app.site_intelligence.preparse_trust_gate import run_gated_factory_site_parse
from app.site_intelligence.site_authenticity import SiteAuthHelpers, SiteAuthenticityAnalyzer, SiteDecision


class _Response:
    def __init__(self, *, url: str, text: str, status_code: int = 200) -> None:
        self.url = url
        self.text = text
        self.status_code = status_code


class _Client:
    def __init__(self, responses: dict[str, _Response]) -> None:
        self.responses = responses
        self.requests: list[str] = []

    def request(self, url: str, *, source: str, timeout: int) -> SimpleNamespace:
        self.requests.append(url)
        response = self.responses.get(url)
        if response is None:
            return SimpleNamespace(ok=False, response=None, status="missing", error="missing response")
        return SimpleNamespace(ok=True, response=response, status="success", error="")


class _FakeAnalyzer:
    def __init__(
        self,
        *,
        surface_decisions: dict[str, SiteDecision],
        final_decisions: dict[str, SiteDecision] | None = None,
    ) -> None:
        self.surface_decisions = surface_decisions
        self.final_decisions = final_decisions or {}
        self.h = SimpleNamespace(normalize_url=_normalize_url)

    def analyze_surface(
        self,
        row: SimpleNamespace,
        site_url: str,
        known_contacts: dict[str, list[str]],
        source_results: dict[str, object],
    ) -> SiteDecision:
        return self.surface_decisions[site_url]

    def analyze(
        self,
        row: SimpleNamespace,
        site_url: str,
        known_contacts: dict[str, list[str]],
        source_results: dict[str, object],
    ) -> SiteDecision:
        return self.final_decisions[site_url]


class _FakeParser:
    def __init__(
        self,
        plans: list[FactorySitePlan] | None = None,
        *,
        content_records: list[core.ContentRecord] | None = None,
        dry_run_content_records: list[core.ContentRecord] | None = None,
        dry_run_records_by_site: dict[str, list[core.ContentRecord]] | None = None,
    ) -> None:
        self.calls: list[object] = []
        self.plans = plans or []
        self.content_records = content_records or []
        self.dry_run_content_records = dry_run_content_records or []
        self.dry_run_records_by_site = {key: list(value) for key, value in (dry_run_records_by_site or {}).items()}

    def parse(self, company: object, *, dry_run: bool = False) -> SimpleNamespace:
        candidate_sites = list(getattr(company, "candidate_sites", []) or [])
        self.calls.append(
            SimpleNamespace(
                company=company,
                candidate_sites=candidate_sites,
                dry_run=dry_run,
            )
        )
        dry_run_records = self.dry_run_content_records
        if dry_run:
            site_key = candidate_sites[0] if candidate_sites else ""
            dry_run_records = self.dry_run_records_by_site.get(site_key, dry_run_records)
        return SimpleNamespace(
            company=company,
            plans=[] if dry_run else self.plans,
            site_probes=[],
            route_strategies=[],
            content_records=list(dry_run_records if dry_run else self.content_records),
            notes=[],
        )


def _normalize_url(value: str) -> str:
    text = str(value or "").strip()
    if not text.startswith("http"):
        return ""
    return text.rstrip("/") + "/"


def _guess_registered_domain(host: str) -> str:
    normalized = str(host or "").strip().lower()
    if normalized.startswith("www."):
        return normalized[4:]
    return normalized


def _keyword_found_in_text(text: str, keyword: str) -> bool:
    return keyword.lower().replace("ё", "е") in text.lower().replace("ё", "е")


def _company_tokens(value: str | None) -> set[str]:
    tokens = re.findall(r"[a-zа-яё]{3,}", str(value or "").lower(), flags=re.IGNORECASE)
    return {token.replace("ё", "е") for token in tokens if token not in {"ооо", "ао"}}


def _extract_emails(text: str) -> list[str]:
    return re.findall(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}", text)


def _extract_phones(text: str) -> list[str]:
    return re.findall(r"\+?\d[\d\-\(\) ]{8,}\d", text)


def _normalized_phone_digits(value: str) -> str:
    return "".join(ch for ch in str(value or "") if ch.isdigit())


def _parse_title_and_meta(soup: object) -> dict[str, str]:
    title_tag = getattr(soup, "title", None)
    title = normalize_whitespace(title_tag.get_text(" ", strip=True) if title_tag else "")
    return {"title": title, "description": ""}


def _summarize_source_context(payload: dict[str, object]) -> dict[str, object]:
    summary: dict[str, object] = {}
    for source_name, source in payload.items():
        summary[source_name] = {
            "snippets": list(getattr(source, "snippets", [])[:4]),
            "notes": list(getattr(source, "notes", [])[:4]),
        }
    return summary


def _build_surface_analyzer(
    responses: dict[str, _Response],
    *,
    llm: object | None = None,
    analyzer_cls: type[SiteAuthenticityAnalyzer] = SiteAuthenticityAnalyzer,
) -> tuple[SiteAuthenticityAnalyzer, _Client]:
    client = _Client(responses)
    helpers = SiteAuthHelpers(
        normalize_url=_normalize_url,
        normalize_whitespace=normalize_whitespace,
        parse_title_and_meta=_parse_title_and_meta,
        dedupe_preserve_order=dedupe_preserve_order,
        extract_emails=_extract_emails,
        extract_phones=_extract_phones,
        extract_probable_addresses=lambda text: [],
        normalize_phone_values=lambda values: dedupe_preserve_order(values or []),
        normalize_address_values=lambda values: dedupe_preserve_order(values or []),
        normalize_phone_candidate=lambda value: value,
        company_tokens=_company_tokens,
        normalized_phone_digits=_normalized_phone_digits,
        guess_registered_domain=_guess_registered_domain,
        address_identity_tokens=lambda address: {"postals": set(), "tokens": set()},
        is_valid_russian_inn=lambda inn: True,
        keyword_found_in_text=_keyword_found_in_text,
        compact_text=compact_text,
        summarize_source_context=_summarize_source_context,
        looks_like_bot_gate=lambda response, text: False,
        contact_path_hints=("contacts",),
        contact_link_text_hints=("contacts",),
        industrial_positive_keywords={"production": 1, "factory": 1, "industrial": 1},
        industrial_negative_keywords={},
        generic_email_domains={"gmail.com", "yandex.ru"},
        company_token_stopwords={"company"},
        activity_token_stopwords={"official"},
        non_corporate_domains=set(),
    )
    analyzer = analyzer_cls(client=client, llm=llm or object(), helpers=helpers)
    return analyzer, client


def _row(*, xlsx_site: str = "https://trusted.example/") -> SimpleNamespace:
    return SimpleNamespace(
        row_index=1,
        company_name="АО Пример Завод",
        inn="1234567890",
        xlsx_site=xlsx_site,
        xlsx_phone="",
        comment="",
    )


def _known_contacts(
    *,
    emails: list[str] | None = None,
    websites: list[str] | None = None,
) -> dict[str, list[str]]:
    return {
        "phones": [],
        "emails": list(emails if emails is not None else ["sales@trusted.example"]),
        "websites": list(websites if websites is not None else ["https://trusted.example/"]),
        "addresses": [],
    }


def _source_results() -> dict[str, object]:
    return {"source": SimpleNamespace(snippets=["industrial production"], notes=[])}


def _surface_decision(url: str, status: str) -> SiteDecision:
    return SiteDecision(
        url=url,
        final_url=url,
        status="success",
        decision_status=status,
        decision_source="cheap_preparse_gate",
    )


def _benchmark_capture(
    tmp_path: Path,
    *,
    force_stages: tuple[str, ...] = ("site_decision",),
) -> LLMBenchmarkCaptureWriter:
    return LLMBenchmarkCaptureWriter(
        LLMBenchmarkCaptureConfig(
            capture_dir=tmp_path / "fixtures",
            force_stages=frozenset(force_stages),
            capture_only=True,
            source_run_selection={"mode": "ordinals", "selected_ordinals": [1]},
        )
    )


def _read_jsonl(path: Path) -> list[dict[str, object]]:
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]


def _content_record(
    *,
    site_url: str,
    path: str = "/surplus",
    trust_state: str = "ambiguous",
) -> core.ContentRecord:
    url = f"{site_url.rstrip('/')}{path}"
    return core.ContentRecord(
        company_id="1234567890",
        site_id=site_url,
        site_url=site_url,
        url=url,
        source_type="html",
        source_url_or_file=url,
        section_guess="tenders",
        title="Surplus stock notice",
        text="Продажа неликвидов и складских остатков.",
        raw_text="Продажа неликвидов и складских остатков.",
        cleaned_text="Продажа неликвидов и складских остатков.",
        extraction_method="requests",
        fetch_status="success",
        content_fingerprint=f"fp:{url}",
        relevance_label="maybe_relevant",
        relevance_score=0.42,
        relevance_reasons=["family:direct_sale:surplus"],
        trace={
            "factory_site_parser": {
                "crawl": {
                    "site_url": site_url,
                    "trust_state": trust_state,
                    "trust_verdict": "uncertain" if trust_state != "trusted" else "strong_match",
                    "trust_summary": "site trust summary",
                }
            }
        },
    )


def test_analyze_surface_distinguishes_candidate_suspicious_and_rejected_without_extra_fetches() -> None:
    trusted_html = """
    <html>
    <head><title>АО Пример Завод</title></head>
    <body>
    <p>АО Пример Завод industrial production factory manufacturing.</p>
    <p>АО Пример Завод industrial production factory manufacturing.</p>
    <p>АО Пример Завод industrial production factory manufacturing.</p>
    <p>АО Пример Завод industrial production factory manufacturing.</p>
    <p>АО Пример Завод industrial production factory manufacturing.</p>
    <a href="/contacts/">Contacts</a>
    <a href="/about/">About</a>
    <a href="/products/">Products</a>
    <a href="/news/">News</a>
    </body>
    </html>
    """
    ambiguous_html = """
    <html>
    <head><title>Industrial equipment</title></head>
    <body>
    <p>Industrial production catalog for equipment suppliers and partners.</p>
    <p>Industrial production catalog for equipment suppliers and partners.</p>
    <p>Contact sales@ambiguous.example for offers.</p>
    <a href="/catalog/">Catalog</a>
    <a href="/contacts/">Contacts</a>
    </body>
    </html>
    """
    rejected_html = """
    <html>
    <head><title>Business directory</title></head>
    <body>
    <p>Business directory and catalog of companies for every industry.</p>
    </body>
    </html>
    """
    analyzer, client = _build_surface_analyzer(
        {
            "https://trusted.example/": _Response(url="https://trusted.example/", text=trusted_html),
            "https://ambiguous.example/": _Response(url="https://ambiguous.example/", text=ambiguous_html),
            "https://reject.example/": _Response(url="https://reject.example/", text=rejected_html),
            "https://trusted.example/contacts/": _Response(
                url="https://trusted.example/contacts/",
                text="<html><body>contacts page</body></html>",
            ),
        }
    )

    trusted = analyzer.analyze_surface(_row(), "https://trusted.example/", _known_contacts(), _source_results())
    ambiguous = analyzer.analyze_surface(_row(), "https://ambiguous.example/", _known_contacts(), _source_results())
    rejected = analyzer.analyze_surface(_row(), "https://reject.example/", _known_contacts(), _source_results())

    assert trusted.decision_status == "candidate"
    assert ambiguous.decision_status == "suspicious"
    assert rejected.decision_status == "rejected"
    assert client.requests == [
        "https://trusted.example/",
        "https://ambiguous.example/",
        "https://reject.example/",
    ]


def test_run_gated_factory_site_parse_skips_parser_when_no_site_is_trusted() -> None:
    row = _row()
    analyzer = _FakeAnalyzer(
        surface_decisions={
            "https://ambiguous.example/": _surface_decision("https://ambiguous.example/", "suspicious"),
            "https://reject.example/": _surface_decision("https://reject.example/", "rejected"),
        }
    )
    parser = _FakeParser()

    result = run_gated_factory_site_parse(
        row=row,
        candidate_sites=["https://ambiguous.example/", "https://reject.example/"],
        known_contacts=_known_contacts(),
        source_results=_source_results(),
        analyzer=analyzer,
        factory_site_parser=parser,
    )

    assert parser.calls == []
    assert [site.decision_status for site in result.validated_sites] == ["suspicious", "rejected"]
    assert [note.split(" gate=", 1)[0] for note in result.notes] == [
        "preparse trust gate skip_deep_parse site=https://ambiguous.example/",
        "preparse trust gate skip_deep_parse site=https://reject.example/",
    ]


def test_run_gated_factory_site_parse_passes_only_trusted_sites_to_deep_parse() -> None:
    row = _row()
    trusted_url = "https://trusted.example/"
    analyzer = _FakeAnalyzer(
        surface_decisions={
            trusted_url: _surface_decision(trusted_url, "candidate"),
            "https://ambiguous.example/": _surface_decision("https://ambiguous.example/", "suspicious"),
        },
        final_decisions={
            trusted_url: SiteDecision(
                url=trusted_url,
                final_url=trusted_url,
                status="success",
                decision_status="verified",
                belongs_to_company=True,
            )
        },
    )
    parser = _FakeParser(
        plans=[
            FactorySitePlan(
                site_url=trusted_url,
                probe=SiteProbe(
                    url=trusted_url,
                    final_url=trusted_url,
                    status="success",
                    site_class="A",
                    worth_crawling="true",
                ),
            )
        ]
    )

    result = run_gated_factory_site_parse(
        row=row,
        candidate_sites=[trusted_url, "https://ambiguous.example/"],
        known_contacts=_known_contacts(),
        source_results=_source_results(),
        analyzer=analyzer,
        factory_site_parser=parser,
    )

    assert len(parser.calls) == 1
    assert parser.calls[0].candidate_sites == [trusted_url]
    assert [site.decision_status for site in result.validated_sites] == ["suspicious", "verified"]


def test_run_gated_factory_site_parse_forced_site_decision_captures_surface_fixture(tmp_path: Path) -> None:
    rejected_html = """
    <html>
    <head><title>Business directory</title></head>
    <body>
    <p>Business directory and catalog of companies for every industry.</p>
    </body>
    </html>
    """
    progress_store = core.ProgressStore(tmp_path / "progress")
    analyzer, _ = _build_surface_analyzer(
        {"https://reject.example/": _Response(url="https://reject.example/", text=rejected_html)},
        llm=core.OpenAIDecider(
            logging.getLogger("test_preparse_forced_site_capture"),
            progress_store,
            benchmark_capture=_benchmark_capture(tmp_path),
        ),
        analyzer_cls=BenchmarkAwareSiteAuthenticityAnalyzer,
    )
    parser = _FakeParser()

    result = run_gated_factory_site_parse(
        row=_row(),
        candidate_sites=["https://reject.example/"],
        known_contacts=_known_contacts(),
        source_results=_source_results(),
        analyzer=analyzer,
        factory_site_parser=parser,
    )

    fixtures = _read_jsonl(tmp_path / "fixtures" / "site_decision_fixtures.jsonl")

    assert parser.calls == []
    assert [site.decision_status for site in result.validated_sites] == ["rejected"]
    assert len(fixtures) == 1
    fixture = fixtures[0]
    assert fixture["stage"] == "site_decision"
    assert fixture["replayable"] is True
    assert fixture["site_url"] == "https://reject.example/"
    assert fixture["would_call_in_prod"] is False
    assert fixture["prod_skip_reason"] == "decision_status_rejected"
    assert fixture["decision_source_context"]["capture_origin"] == "preparse_surface"
    assert fixture["compact_context"]["candidate_site"]["url"] == "https://reject.example/"
    assert fixture["source_run_selection"]["selected_ordinals"] == [1]


def test_run_gated_factory_site_parse_builds_synthetic_site_fixture_from_source_hints(tmp_path: Path) -> None:
    synthetic_html = """
    <html>
    <head><title>Benchmark Plant Official Site</title></head>
    <body>
    <p>Industrial production, warehouse surplus and procurement notices.</p>
    <a href="/contacts/">Contacts</a>
    </body>
    </html>
    """
    progress_store = core.ProgressStore(tmp_path / "progress")
    analyzer, _ = _build_surface_analyzer(
        {"https://synthetic.example/": _Response(url="https://synthetic.example/", text=synthetic_html)},
        llm=core.OpenAIDecider(
            logging.getLogger("test_preparse_synthetic_site_capture"),
            progress_store,
            benchmark_capture=_benchmark_capture(tmp_path),
        ),
        analyzer_cls=BenchmarkAwareSiteAuthenticityAnalyzer,
    )
    parser = _FakeParser()

    result = run_gated_factory_site_parse(
        row=_row(xlsx_site=""),
        candidate_sites=[],
        known_contacts=_known_contacts(emails=[], websites=[]),
        source_results={
            "source": SimpleNamespace(
                snippets=["Official website www.synthetic.example with industrial production details"],
                notes=[],
            )
        },
        analyzer=analyzer,
        factory_site_parser=parser,
    )

    fixtures = _read_jsonl(tmp_path / "fixtures" / "site_decision_fixtures.jsonl")

    assert parser.calls == []
    assert result.validated_sites == []
    assert len(fixtures) == 1
    fixture = fixtures[0]
    assert fixture["stage"] == "site_decision"
    assert fixture["replayable"] is True
    assert fixture["site_url"] == "https://synthetic.example/"
    assert fixture["would_call_in_prod"] is False
    assert fixture["prod_skip_reason"] == "no_candidate_site"
    assert fixture["benchmark_synthetic_candidate"] is True
    assert fixture["benchmark_capture_path"] == "preparse_trust_gate.site_decision.synthetic_candidate"
    assert fixture["synthetic_candidate_used"] is True
    assert fixture["forced_harvest_level"] == "none"
    assert fixture["decision_source_context"]["capture_origin"] == "benchmark_synthetic_candidate"
    assert fixture["decision_source_context"]["best_site"] == "https://synthetic.example/"


def test_run_gated_factory_site_parse_records_blocker_when_candidate_sites_missing(tmp_path: Path) -> None:
    progress_store = core.ProgressStore(tmp_path / "progress")
    analyzer = SimpleNamespace(
        llm=core.OpenAIDecider(
            logging.getLogger("test_preparse_no_candidate_blocker"),
            progress_store,
            benchmark_capture=_benchmark_capture(tmp_path),
        ),
        h=SimpleNamespace(normalize_url=_normalize_url),
    )
    parser = _FakeParser()

    result = run_gated_factory_site_parse(
        row=_row(xlsx_site=""),
        candidate_sites=[],
        known_contacts=_known_contacts(emails=[], websites=[]),
        source_results={"source": SimpleNamespace(snippets=["industrial production"], notes=[])},
        analyzer=analyzer,
        factory_site_parser=parser,
    )

    blockers = _read_jsonl(tmp_path / "fixtures" / "site_decision_blockers.jsonl")

    assert parser.calls == []
    assert result.validated_sites == []
    assert len(blockers) == 1
    blocker = blockers[0]
    assert blocker["stage"] == "site_decision"
    assert blocker["replayable"] is False
    assert blocker["blocker_reason"] == "no_candidate_site"
    assert blocker["would_call_in_prod"] is False
    assert blocker["site_url"] == ""
    assert blocker["benchmark_capture_path"] == "preparse_trust_gate.site_decision.synthetic_candidate"
    assert blocker["synthetic_candidate_used"] is False
    assert blocker["forced_harvest_level"] == "none"
    assert blocker["source_run_selection"]["selected_ordinals"] == [1]


def test_run_gated_factory_site_parse_forced_content_review_harvests_fixture_when_prod_has_no_records(tmp_path: Path) -> None:
    progress_store = core.ProgressStore(tmp_path / "progress")
    analyzer = _FakeAnalyzer(
        surface_decisions={
            "https://first.example/": _surface_decision("https://first.example/", "suspicious"),
            "https://second.example/": _surface_decision("https://second.example/", "suspicious"),
        }
    )
    analyzer.llm = core.OpenAIDecider(
        logging.getLogger("test_content_review_harvest_fixture"),
        progress_store,
        benchmark_capture=_benchmark_capture(tmp_path, force_stages=("content_review",)),
    )
    parser = _FakeParser(
        dry_run_records_by_site={
            "https://second.example/": [_content_record(site_url="https://second.example/", trust_state="ambiguous")]
        }
    )

    result = run_gated_factory_site_parse(
        row=_row(),
        candidate_sites=["https://first.example/", "https://second.example/"],
        known_contacts=_known_contacts(),
        source_results=_source_results(),
        analyzer=analyzer,
        factory_site_parser=parser,
    )

    fixtures = _read_jsonl(tmp_path / "fixtures" / "content_review_fixtures.jsonl")

    assert result.parsed_factory_sites.content_records == []
    assert len(parser.calls) == 2
    assert parser.calls[0].candidate_sites == ["https://first.example/"]
    assert parser.calls[0].dry_run is True
    assert parser.calls[1].candidate_sites == ["https://second.example/"]
    assert parser.calls[1].dry_run is True
    assert len(fixtures) == 1
    fixture = fixtures[0]
    assert fixture["stage"] == "content_review"
    assert fixture["replayable"] is True
    assert fixture["would_call_in_prod"] is False
    assert fixture["prod_skip_reason"] == "site_not_trusted"
    assert fixture["benchmark_forced_harvest"] is True
    assert fixture["site_url"] == "https://second.example/"
    assert fixture["benchmark_capture_path"] == "preparse_trust_gate.content_review.forced_harvest"
    assert fixture["synthetic_candidate_used"] is False
    assert fixture["forced_harvest_level"] == "widened_two_sites_requests_only"


def test_run_gated_factory_site_parse_forced_content_review_records_blocker_when_harvest_empty(tmp_path: Path) -> None:
    progress_store = core.ProgressStore(tmp_path / "progress")
    analyzer = _FakeAnalyzer(
        surface_decisions={
            "https://first.example/": _surface_decision("https://first.example/", "suspicious"),
            "https://second.example/": _surface_decision("https://second.example/", "suspicious"),
        }
    )
    analyzer.llm = core.OpenAIDecider(
        logging.getLogger("test_content_review_harvest_blocker"),
        progress_store,
        benchmark_capture=_benchmark_capture(tmp_path, force_stages=("content_review",)),
    )
    parser = _FakeParser()

    result = run_gated_factory_site_parse(
        row=_row(),
        candidate_sites=["https://first.example/", "https://second.example/"],
        known_contacts=_known_contacts(),
        source_results=_source_results(),
        analyzer=analyzer,
        factory_site_parser=parser,
    )

    blockers = _read_jsonl(tmp_path / "fixtures" / "content_review_blockers.jsonl")

    assert result.parsed_factory_sites.content_records == []
    assert len(parser.calls) == 2
    assert parser.calls[0].candidate_sites == ["https://first.example/"]
    assert parser.calls[0].dry_run is True
    assert parser.calls[1].candidate_sites == ["https://second.example/"]
    assert parser.calls[1].dry_run is True
    assert len(blockers) == 1
    blocker = blockers[0]
    assert blocker["stage"] == "content_review"
    assert blocker["replayable"] is False
    assert blocker["blocker_reason"] == "no_content_record"
    assert blocker["would_call_in_prod"] is False
    assert blocker["site_url"] == "https://first.example/"
    assert blocker["benchmark_capture_path"] == "preparse_trust_gate.content_review.forced_harvest"
    assert blocker["synthetic_candidate_used"] is False
    assert blocker["forced_harvest_level"] == "widened_two_sites_requests_only"


def test_run_gated_factory_site_parse_without_benchmark_keeps_no_candidate_path_unchanged(tmp_path: Path) -> None:
    progress_store = core.ProgressStore(tmp_path / "progress")
    analyzer, _ = _build_surface_analyzer(
        {"https://synthetic.example/": _Response(url="https://synthetic.example/", text="<html></html>")},
        llm=core.OpenAIDecider(logging.getLogger("test_preparse_no_candidate_regular_path"), progress_store),
        analyzer_cls=BenchmarkAwareSiteAuthenticityAnalyzer,
    )
    parser = _FakeParser()

    result = run_gated_factory_site_parse(
        row=_row(xlsx_site=""),
        candidate_sites=[],
        known_contacts=_known_contacts(emails=[], websites=[]),
        source_results={
            "source": SimpleNamespace(snippets=["Official website www.synthetic.example"], notes=[]),
        },
        analyzer=analyzer,
        factory_site_parser=parser,
    )

    assert parser.calls == []
    assert result.validated_sites == []
    assert not (tmp_path / "fixtures" / "site_decision_fixtures.jsonl").exists()
    assert not (tmp_path / "fixtures" / "site_decision_blockers.jsonl").exists()


def test_run_gated_factory_site_parse_without_benchmark_keeps_surface_path_unchanged(tmp_path: Path) -> None:
    rejected_html = """
    <html>
    <head><title>Business directory</title></head>
    <body>
    <p>Business directory and catalog of companies for every industry.</p>
    </body>
    </html>
    """
    progress_store = core.ProgressStore(tmp_path / "progress")
    analyzer, _ = _build_surface_analyzer(
        {"https://reject.example/": _Response(url="https://reject.example/", text=rejected_html)},
        llm=core.OpenAIDecider(logging.getLogger("test_preparse_regular_path"), progress_store),
        analyzer_cls=BenchmarkAwareSiteAuthenticityAnalyzer,
    )
    parser = _FakeParser()

    result = run_gated_factory_site_parse(
        row=_row(),
        candidate_sites=["https://reject.example/"],
        known_contacts=_known_contacts(),
        source_results=_source_results(),
        analyzer=analyzer,
        factory_site_parser=parser,
    )

    assert parser.calls == []
    assert [site.decision_status for site in result.validated_sites] == ["rejected"]
    assert not (tmp_path / "fixtures" / "site_decision_fixtures.jsonl").exists()
    assert not (tmp_path / "fixtures" / "site_decision_blockers.jsonl").exists()

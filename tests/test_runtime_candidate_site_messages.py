from __future__ import annotations

from types import SimpleNamespace

import pytest

import company_enrichment_core as core
import run_company_enrichment_pipeline as pipeline
from app.discovery.models import DomainCandidate, DomainResolution
from app.runtime import load_stage_messages


class _StaticSource:
    def __init__(self, source_name: str, *, status: str = "ok") -> None:
        self.source_name = source_name
        self._status = status

    def search(self, row: core.RowInput) -> core.SourceResult:
        return core.SourceResult(source=self.source_name, status=self._status)


def _rows(count: int) -> list[core.RowInput]:
    return [
        core.RowInput(
            row_index=idx + 1,
            inn=f"{idx:010d}",
            company_name=f"Company {idx}",
        )
        for idx in range(1, count + 1)
    ]


def _install_lightweight_run_stubs(
    monkeypatch: pytest.MonkeyPatch,
    *,
    rows: list[core.RowInput],
    domain_resolution: DomainResolution | None,
    candidate_sites: list[str],
) -> None:
    def fake_rate_limited_http_client(**kwargs):
        return SimpleNamespace(progress_store=kwargs["progress_store"])

    def fake_gated_parse(**kwargs):
        return SimpleNamespace(
            validated_sites=[],
            notes=[],
            parsed_factory_sites=SimpleNamespace(
                site_probes=[],
                route_strategies=[],
                content_records=[],
                notes=[],
            ),
        )

    monkeypatch.setattr(pipeline.core, "load_env_file", lambda _path: None)
    monkeypatch.setattr(pipeline.core, "load_rows_from_xlsx", lambda _path: rows)
    monkeypatch.setattr(pipeline.core, "RateLimitedHttpClient", fake_rate_limited_http_client)
    monkeypatch.setattr(core.ProgressStore, "_write_markdown_reports", lambda self, ordered_results=None: None)
    monkeypatch.setattr(pipeline, "ProxyPool", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(pipeline, "SparkSource", lambda _client: _StaticSource("spark"))
    monkeypatch.setattr(pipeline, "ZachestnyBiznesSource", lambda _client: _StaticSource("zachestnyibiznes"))
    monkeypatch.setattr(pipeline, "RusprofileSource", lambda _client: _StaticSource("rusprofile"))
    monkeypatch.setattr(pipeline, "CheckoSource", lambda _client: _StaticSource("checko"))
    monkeypatch.setattr(pipeline, "ListOrgSource", lambda _client, _snapshot: _StaticSource("list_org"))
    monkeypatch.setattr(pipeline, "FactorySiteParser", lambda *_args, **_kwargs: SimpleNamespace())
    monkeypatch.setattr(pipeline, "SiteAuthHelpers", lambda **_kwargs: SimpleNamespace())
    monkeypatch.setattr(
        pipeline,
        "BenchmarkAwareSiteAuthenticityAnalyzer",
        lambda *_args, **_kwargs: SimpleNamespace(llm=SimpleNamespace()),
    )
    monkeypatch.setattr(pipeline, "build_domain_resolution", lambda *_args, **_kwargs: domain_resolution)
    monkeypatch.setattr(pipeline, "choose_candidate_sites", lambda *_args, **_kwargs: list(candidate_sites))
    monkeypatch.setattr(pipeline, "run_gated_factory_site_parse", fake_gated_parse)
    monkeypatch.setattr(pipeline, "classify_content_record", lambda _record: None)
    monkeypatch.setattr(pipeline, "should_use_llm_record_review", lambda _record: False)
    monkeypatch.setattr(
        pipeline,
        "build_and_store_company_dossier",
        lambda *, result, output_dir: {"company_name": result.company_name},
    )
    monkeypatch.setattr(pipeline.core, "build_analysis_contacts", lambda *_args, **_kwargs: {})
    monkeypatch.setattr(pipeline.core, "merge_contacts", lambda *_args, **_kwargs: {})
    monkeypatch.setattr(pipeline.core, "build_trusted_contacts", lambda *_args, **_kwargs: {})
    monkeypatch.setattr(pipeline.core, "build_lead_cards", lambda *_args, **_kwargs: [])
    monkeypatch.setattr(pipeline.core, "build_site_refresh_plans", lambda *_args, **_kwargs: [])


def test_candidate_site_messages_use_resolution_metadata_when_available(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    rows = _rows(1)
    resolution = DomainResolution(
        inn=rows[0].inn,
        company_name=rows[0].company_name,
        status="verified",
        selected_primary_domain="https://alpha.example",
        selected_primary_status="verified",
        candidates=[
            DomainCandidate(
                url="https://alpha.example",
                domain="alpha.example",
                source="spark, zachestnyibiznes",
                confidence=0.88,
                status="verified",
            ),
            DomainCandidate(
                url="https://beta.example",
                domain="beta.example",
                source="spark",
                confidence=0.53,
                status="candidate",
            ),
        ],
    )
    _install_lightweight_run_stubs(
        monkeypatch,
        rows=rows,
        domain_resolution=resolution,
        candidate_sites=["https://alpha.example", "https://beta.example"],
    )
    output_dir = tmp_path / "output"

    exit_code = pipeline.run(
        pipeline.parse_args(
            [
                "--input",
                "input.xlsx",
                "--output-dir",
                str(output_dir),
                "--sources=spark,zachestnyibiznes",
                "--ordinals=1",
            ]
        )
    )

    assert exit_code == 0
    stage_messages = load_stage_messages(output_dir)
    candidate_messages = [
        message for message in stage_messages if message["message_type"] == "candidate_site_found"
    ]

    assert [message["payload"] for message in candidate_messages] == [
        {
            "candidate_status": "verified",
            "confidence": 0.88,
            "resolution_status": "verified",
            "selection_rank": 1,
            "selection_source": "spark, zachestnyibiznes",
            "site_url": "https://alpha.example",
        },
        {
            "candidate_status": "candidate",
            "confidence": 0.53,
            "resolution_status": "verified",
            "selection_rank": 2,
            "selection_source": "spark",
            "site_url": "https://beta.example",
        },
    ]


def test_resume_skip_path_does_not_emit_candidate_site_messages(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    rows = _rows(1)
    _install_lightweight_run_stubs(
        monkeypatch,
        rows=rows,
        domain_resolution=DomainResolution(
            inn=rows[0].inn,
            company_name=rows[0].company_name,
            status="candidate",
        ),
        candidate_sites=["https://resume.example"],
    )
    output_dir = tmp_path / "output"

    first_exit_code = pipeline.run(
        pipeline.parse_args(
            [
                "--input",
                "input.xlsx",
                "--output-dir",
                str(output_dir),
                "--sources=spark",
                "--ordinals=1",
            ]
        )
    )
    assert first_exit_code == 0
    first_stage_messages = load_stage_messages(output_dir)
    assert [message["message_type"] for message in first_stage_messages] == [
        "source_result_ready",
        "candidate_site_found",
        "deep_parse_done",
        "company_completed",
    ]

    resume_exit_code = pipeline.run(
        pipeline.parse_args(
            [
                "--input",
                "input.xlsx",
                "--output-dir",
                str(output_dir),
                "--resume",
                "--sources=spark",
                "--ordinals=1",
            ]
        )
    )

    assert resume_exit_code == 0
    assert load_stage_messages(output_dir) == first_stage_messages


def test_candidate_site_messages_fall_back_to_merged_contacts_source(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    rows = _rows(1)
    _install_lightweight_run_stubs(
        monkeypatch,
        rows=rows,
        domain_resolution=DomainResolution(
            inn=rows[0].inn,
            company_name=rows[0].company_name,
            status="candidate",
        ),
        candidate_sites=["https://fallback.example"],
    )
    output_dir = tmp_path / "output"

    exit_code = pipeline.run(
        pipeline.parse_args(
            [
                "--input",
                "input.xlsx",
                "--output-dir",
                str(output_dir),
                "--sources=spark",
                "--ordinals=1",
            ]
        )
    )

    assert exit_code == 0
    candidate_messages = [
        message
        for message in load_stage_messages(output_dir)
        if message["message_type"] == "candidate_site_found"
    ]
    assert len(candidate_messages) == 1
    assert candidate_messages[0]["run_id"]
    assert candidate_messages[0]["ts"]
    assert candidate_messages[0]["inn"] == "0000000001"
    assert candidate_messages[0]["row_index"] == 2
    assert candidate_messages[0]["stage"] == "candidate_site_selection"
    assert candidate_messages[0]["payload"] == {
        "resolution_status": "candidate",
        "selection_rank": 1,
        "selection_source": "merged_contacts",
        "site_url": "https://fallback.example",
    }

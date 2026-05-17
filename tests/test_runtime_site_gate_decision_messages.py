from __future__ import annotations

from dataclasses import dataclass
from types import SimpleNamespace

import pytest

import company_enrichment_core as core
import run_company_enrichment_pipeline as pipeline
from app.discovery.models import DomainResolution
from app.runtime import ProgressStore, load_stage_messages, stage_message_outbox_path


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


@dataclass
class _FakeSiteDecision:
    url: str
    final_url: str
    decision_status: str
    belongs_to_company: bool
    authenticity_score: float
    identity_score: float
    viability_score: float
    raw_html: str
    parser_blob: dict[str, object]
    domain_resolution: dict[str, object]
    runtime_counter: int
    reasons: list[str]


def _site_decision(**overrides):
    payload = {
        "url": "https://gate.example",
        "final_url": "https://gate.example/final",
        "decision_status": "candidate",
        "belongs_to_company": True,
        "authenticity_score": 0.9456,
        "identity_score": 0.8123,
        "viability_score": 0.7012,
        "raw_html": "<html>heavy</html>",
        "parser_blob": {"plans": [1, 2, 3]},
        "domain_resolution": {"selected_primary_domain": "https://gate.example"},
        "runtime_counter": 42,
        "reasons": ["should_not_leak"],
    }
    payload.update(overrides)
    return _FakeSiteDecision(**payload)


def _install_lightweight_run_stubs(
    monkeypatch: pytest.MonkeyPatch,
    *,
    rows: list[core.RowInput],
    candidate_sites: list[str],
    validated_sites: list[object],
) -> None:
    def fake_rate_limited_http_client(**kwargs):
        return SimpleNamespace(progress_store=kwargs["progress_store"])

    def fake_gated_parse(**kwargs):
        return SimpleNamespace(
            validated_sites=list(validated_sites),
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
    monkeypatch.setattr(
        pipeline,
        "build_domain_resolution",
        lambda *_args, **_kwargs: DomainResolution(
            inn=rows[0].inn,
            company_name=rows[0].company_name,
            status="candidate",
        ),
    )
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


def test_site_gate_decision_messages_use_scalar_decision_metadata(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    rows = _rows(1)
    _install_lightweight_run_stubs(
        monkeypatch,
        rows=rows,
        candidate_sites=["https://gate.example"],
        validated_sites=[_site_decision()],
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
    site_gate_messages = [
        message for message in load_stage_messages(output_dir) if message["message_type"] == "site_gate_decision"
    ]
    assert len(site_gate_messages) == 1
    assert site_gate_messages[0]["stage"] == "site_gate"
    assert site_gate_messages[0]["payload"] == {
        "authenticity_score": 0.946,
        "belongs_to_company": True,
        "decision_status": "candidate",
        "identity_score": 0.812,
        "site_url": "https://gate.example/final",
        "viability_score": 0.701,
    }
    assert all(not isinstance(value, (dict, list)) for value in site_gate_messages[0]["payload"].values())


def test_resume_skip_path_does_not_emit_site_gate_decision_messages(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    rows = _rows(1)
    _install_lightweight_run_stubs(
        monkeypatch,
        rows=rows,
        candidate_sites=["https://resume.example"],
        validated_sites=[_site_decision(url="https://resume.example", final_url="https://resume.example")],
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
        "site_gate_decision",
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


def test_progress_store_does_not_backfill_missing_private_outbox_from_runtime_state(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    rows = _rows(1)
    _install_lightweight_run_stubs(
        monkeypatch,
        rows=rows,
        candidate_sites=["https://nobackfill.example"],
        validated_sites=[_site_decision(url="https://nobackfill.example", final_url="https://nobackfill.example")],
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
    outbox_path = stage_message_outbox_path(output_dir)
    assert outbox_path.exists()
    outbox_path.unlink()

    ProgressStore(output_dir)

    assert not outbox_path.exists()
    assert load_stage_messages(output_dir) == []
    assert (output_dir / "runtime_state.json").exists()

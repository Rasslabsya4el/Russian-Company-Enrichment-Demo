from __future__ import annotations

import json
from dataclasses import dataclass
from types import SimpleNamespace

import pytest

import company_enrichment_core as core
import app.runtime.progress as runtime_progress
import run_company_enrichment_pipeline as pipeline
from app.discovery.models import DomainCandidate, DomainResolution
from app.runtime.queue_families import (
    build_deep_parse_queue_family_contour,
    build_downstream_worker_pool_contour,
    normalize_queue_family_contour,
)
from app.runtime import ProgressStore, load_stage_messages, stage_message_outbox_path


class _StaticSource:
    def __init__(self, source_name: str, *, status: str = "ok") -> None:
        self.source_name = source_name
        self._status = status

    def search(self, row: core.RowInput) -> core.SourceResult:
        return core.SourceResult(source=self.source_name, status=self._status)


class _EventWritingSource(_StaticSource):
    def __init__(self, source_name: str, progress_store: core.ProgressStore, event_factory) -> None:
        super().__init__(source_name)
        self._progress_store = progress_store
        self._event_factory = event_factory

    def search(self, row: core.RowInput) -> core.SourceResult:
        self._progress_store.append_event(self._event_factory(row=row, source_name=self.source_name))
        return super().search(row)


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


def _site_decision(**overrides):
    payload = {
        "url": "https://alpha.example",
        "final_url": "https://alpha.example/about",
        "decision_status": "candidate",
        "belongs_to_company": True,
        "authenticity_score": 0.9334,
        "identity_score": 0.8212,
        "viability_score": 0.7444,
    }
    payload.update(overrides)
    return _FakeSiteDecision(**payload)


def _host_runtime_event(*, row: core.RowInput, source_name: str) -> dict[str, object]:
    return {
        "ts": core.utc_now_iso(),
        "type": "route_fetch_attempt",
        "host": "spark-interfax.ru",
        "source": source_name,
        "status": "success",
        "cooldown_seconds": 15,
        "since_previous_request_seconds": 2.5,
        "proxy_label_or_id": "pool-a",
        "url": f"https://spark-interfax.ru/company/{row.inn}",
        "details": {"drop": True},
    }


def test_progress_store_persists_deferred_required_source_manifest_across_reload(tmp_path) -> None:
    output_dir = tmp_path / "output"
    progress = ProgressStore(output_dir)
    progress.run_started(
        input_path="input.xlsx",
        total_rows=1,
        selected_rows=1,
        selection_mode="ordinals",
        selected_ordinals=[1],
        start_from=1,
        end_at=1,
        active_sources=["spark"],
        continue_existing_run=False,
    )

    progress.record_deferred_required_source(
        {
            "source": "spark",
            "access_mode": "direct-default",
            "status": "request_error",
            "error": "Read timeout after finite Spark source retries",
            "row_index": 1,
            "inn": "0000000001",
            "company_name": "Company 1",
            "run_id": progress.run_metadata["run_id"],
            "first_seen_at": "2026-04-30T10:00:00+00:00",
            "last_seen_at": "2026-04-30T10:00:00+00:00",
        }
    )

    summary = json.loads((output_dir / "summary.json").read_text(encoding="utf-8"))
    assert summary["required_source_deferred_rows_total"] == 1
    assert summary["unresolved_required_source_rows"] == 1
    assert summary["required_source_deferred_rows_by_source"] == {"spark": 1}
    assert summary["required_source_deferred_rows_by_status"] == {"request_error": 1}

    runtime_state = json.loads((output_dir / "runtime_state.json").read_text(encoding="utf-8"))
    record = runtime_state["run"]["metadata"]["deferred_required_sources"]["records"]["0000000001::spark"]
    assert record["inn"] == "0000000001"
    assert record["source"] == "spark"
    assert record["resolution_status"] == "unresolved"

    reloaded = ProgressStore(output_dir)
    assert reloaded.summary["required_source_deferred_rows_total"] == 1
    assert reloaded.run_metadata["deferred_required_sources"]["records"]["0000000001::spark"] == record

    assert reloaded.mark_deferred_required_source_resolved(
        inn="0000000001",
        source_name="spark",
        source_status="ok",
        detail="retry promoted completed company result through explicit runtime ack",
    )
    resolved_summary = json.loads((output_dir / "summary.json").read_text(encoding="utf-8"))
    resolved_record = resolved_summary["deferred_required_sources"]["records"]["0000000001::spark"]
    assert resolved_summary["required_source_deferred_rows_total"] == 0
    assert resolved_summary["unresolved_required_source_rows"] == 0
    assert resolved_record["resolution_status"] == "resolved"
    assert resolved_record["resolution_source_status"] == "ok"

    resolved_runtime_state = json.loads((output_dir / "runtime_state.json").read_text(encoding="utf-8"))
    assert resolved_runtime_state["run"]["metadata"]["required_source_deferred_rows_total"] == 0
    assert (
        resolved_runtime_state["run"]["metadata"]["deferred_required_sources"]["records"]["0000000001::spark"]
        == resolved_record
    )


def test_materialized_public_outputs_update_existing_company_by_inn_not_row_order(tmp_path) -> None:
    output_dir = tmp_path / "output"
    progress = ProgressStore(output_dir)
    progress.run_started(
        input_path="input.xlsx",
        total_rows=2,
        selected_rows=2,
        selection_mode="window",
        selected_ordinals=[],
        start_from=1,
        end_at=2,
        active_sources=["spark"],
        continue_existing_run=False,
    )

    later_row = core.RowInput(row_index=5, inn="7700000005", company_name="Later Row")
    earlier_row = core.RowInput(row_index=2, inn="7700000002", company_name="Earlier Row")

    later_result = core.build_company_result(later_row)
    later_result.status = "completed"
    later_result.finished_at = core.utc_now_iso()
    later_result.sources["spark"] = core.SourceResult(source="spark", status="guest")
    progress.persist_completed_company_result(later_result, total_rows=2, processed_rows=1)

    earlier_result = core.build_company_result(earlier_row)
    earlier_result.status = "completed"
    earlier_result.finished_at = core.utc_now_iso()
    earlier_result.sources["spark"] = core.SourceResult(source="spark", status="guest")
    progress.persist_completed_company_result(earlier_result, total_rows=2, processed_rows=2)

    updated_later_result = core.build_company_result(later_row)
    updated_later_result.company_name = "Later Row Updated"
    updated_later_result.status = "completed"
    updated_later_result.finished_at = core.utc_now_iso()
    updated_later_result.sources["spark"] = core.SourceResult(source="spark", status="ok")
    progress.persist_completed_company_result(updated_later_result, total_rows=2, processed_rows=2)

    public_results = json.loads((output_dir / "results.json").read_text(encoding="utf-8"))
    assert [item["inn"] for item in public_results] == [earlier_row.inn, later_row.inn]
    assert [item["row_index"] for item in public_results] == [earlier_row.row_index, later_row.row_index]
    assert [item["company_name"] for item in public_results] == ["Earlier Row", "Later Row Updated"]
    assert public_results[1]["sources"]["spark"]["status"] == "ok"

    runtime_state = json.loads((output_dir / "runtime_state.json").read_text(encoding="utf-8"))
    assert [entry["company"]["inn"] for entry in runtime_state["company_entries"]] == [
        earlier_row.inn,
        later_row.inn,
    ]
    assert [entry["company"]["row_index"] for entry in runtime_state["company_entries"]] == [
        earlier_row.row_index,
        later_row.row_index,
    ]
    assert runtime_state["run"]["summary"]["completed_rows"] == 2
    assert len(progress.results) == 2
    assert progress.get(later_row.inn)["company_name"] == "Later Row Updated"


def _install_lightweight_run_stubs(
    monkeypatch: pytest.MonkeyPatch,
    *,
    rows: list[core.RowInput],
    domain_resolution: DomainResolution | None = None,
    candidate_sites: list[str] | None = None,
    validated_sites: list[object] | None = None,
    site_probes: list[object] | None = None,
    route_strategies: list[object] | None = None,
    content_records: list[object] | None = None,
    lead_cards: list[object] | None = None,
) -> None:
    def fake_rate_limited_http_client(**kwargs):
        return SimpleNamespace(progress_store=kwargs["progress_store"])

    deep_parse_decisions = list(validated_sites or [])
    deep_parse_sites: list[str] = []
    deep_parse_decisions_by_site: dict[str, object] = {}
    for decision in deep_parse_decisions:
        normalized_site = core.sanitize_website_url(
            getattr(decision, "final_url", "") or getattr(decision, "url", "")
        ) or str(getattr(decision, "final_url", "") or getattr(decision, "url", "") or "").strip()
        if not normalized_site:
            continue
        if normalized_site not in deep_parse_sites:
            deep_parse_sites.append(normalized_site)
        deep_parse_decisions_by_site[normalized_site] = decision

    def fake_gate(**kwargs):
        return SimpleNamespace(
            deep_parse_sites=list(deep_parse_sites),
            surface_only_decisions=[],
            trusted_surface_decisions_by_site=dict(deep_parse_decisions_by_site),
            notes=[],
        )

    def fake_parser_factory(*_args, **_kwargs):
        return SimpleNamespace(
            parse=lambda _company: SimpleNamespace(
                plans=[
                    SimpleNamespace(site_url=site_url, allows_deep_check=True)
                    for site_url in deep_parse_sites
                ],
                site_probes=list(site_probes or []),
                route_strategies=list(route_strategies or []),
                content_records=list(content_records or []),
                notes=[],
            )
        )

    def fake_analyzer_factory(*_args, **_kwargs):
        def analyze(_row, site_url, _known_contacts, _source_results):
            normalized_site = core.sanitize_website_url(site_url) or str(site_url or "").strip()
            return deep_parse_decisions_by_site.get(
                normalized_site,
                _site_decision(
                    url=normalized_site,
                    final_url=normalized_site,
                    decision_status="candidate",
                    belongs_to_company=True,
                ),
            )

        return SimpleNamespace(
            analyze=analyze,
            h=SimpleNamespace(
                normalize_url=lambda value: core.sanitize_website_url(value) or str(value or "").strip()
            ),
            llm=SimpleNamespace(
                should_force_benchmark_stage=lambda _stage: False,
                judge_content_record=lambda *_args, **_kwargs: None,
                capture_forced_content_review_fixture=lambda **_kwargs: None,
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
    monkeypatch.setattr(pipeline, "FactorySiteParser", fake_parser_factory)
    monkeypatch.setattr(pipeline, "SiteAuthHelpers", lambda **_kwargs: SimpleNamespace())
    monkeypatch.setattr(pipeline, "BenchmarkAwareSiteAuthenticityAnalyzer", fake_analyzer_factory)
    monkeypatch.setattr(pipeline, "build_domain_resolution", lambda *_args, **_kwargs: domain_resolution)
    monkeypatch.setattr(pipeline, "choose_candidate_sites", lambda *_args, **_kwargs: list(candidate_sites or []))
    monkeypatch.setattr(pipeline, "gate_candidate_sites_before_deep_parse", fake_gate)
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
    monkeypatch.setattr(pipeline.core, "build_lead_cards", lambda *_args, **_kwargs: list(lead_cards or []))
    monkeypatch.setattr(pipeline.core, "build_site_refresh_plans", lambda *_args, **_kwargs: [])


def _domain_resolution_for(
    row: core.RowInput,
    *,
    primary_url: str,
    secondary_url: str,
    primary_confidence: float,
    secondary_confidence: float,
) -> DomainResolution:
    return DomainResolution(
        inn=row.inn,
        company_name=row.company_name,
        status="verified",
        selected_primary_domain=primary_url,
        selected_primary_status="verified",
        candidates=[
            DomainCandidate(
                url=primary_url,
                domain=primary_url.removeprefix("https://"),
                source="spark, zachestnyibiznes",
                confidence=primary_confidence,
                status="verified",
            ),
            DomainCandidate(
                url=secondary_url,
                domain=secondary_url.removeprefix("https://"),
                source="spark",
                confidence=secondary_confidence,
                status="candidate",
            ),
        ],
    )


def _run_stage_message_pipeline(output_dir, *extra_args: str) -> int:
    return pipeline.run(
        pipeline.parse_args(
            [
                "--input",
                "input.xlsx",
                "--output-dir",
                str(output_dir),
                "--sources=spark,zachestnyibiznes",
                "--ordinals=1",
                *extra_args,
            ]
        )
    )


def test_progress_store_live_persist_defers_full_exports_until_run_finished(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    output_dir = tmp_path / "output"
    progress = ProgressStore(output_dir)
    progress.run_started(
        input_path="input.xlsx",
        total_rows=2,
        selected_rows=2,
        selection_mode="window",
        selected_ordinals=[],
        start_from=1,
        end_at=2,
        active_sources=["spark"],
        continue_existing_run=False,
    )

    export_calls: list[tuple[str, int]] = []
    company_report_calls: list[str] = []

    def fake_write_flat_csv(path, rows) -> None:
        export_calls.append(("csv", len(rows)))
        path.write_text("csv", encoding="utf-8")

    def fake_write_flat_xlsx(path, rows) -> None:
        export_calls.append(("xlsx", len(rows)))
        path.write_text("xlsx", encoding="utf-8")

    def fake_render_company_report(result) -> str:
        company_report_calls.append(str(result.get("inn", "") or ""))
        return f"# {result.get('company_name', '')}\n"

    monkeypatch.setattr(core, "write_flat_csv", fake_write_flat_csv)
    monkeypatch.setattr(core, "write_flat_xlsx", fake_write_flat_xlsx)
    monkeypatch.setattr(core, "render_company_report_markdown", fake_render_company_report)

    first_row, second_row = _rows(2)
    first_result = core.build_company_result(first_row)
    first_result.status = "completed"
    first_result.finished_at = core.utc_now_iso()
    first_result.sources["spark"] = core.SourceResult(source="spark", status="success")

    progress.persist_completed_company_result(first_result, total_rows=2, processed_rows=1)

    assert export_calls == []
    assert not (output_dir / "final_results.csv").exists()
    assert not (output_dir / "final_results.xlsx").exists()
    first_summary = json.loads((output_dir / "summary.json").read_text(encoding="utf-8"))
    assert first_summary["public_output_contract"]["terminal_run"] is False
    assert first_summary["public_output_contract"]["public_result_count"] == 1
    assert first_summary["public_output_contract"]["final_exports"] == {
        "state": "suppressed_until_run_finished",
        "available": False,
        "row_count": 0,
        "paths": ["final_results.csv", "final_results.xlsx"],
    }
    assert [item["inn"] for item in json.loads((output_dir / "results.json").read_text(encoding="utf-8"))] == [
        first_row.inn
    ]
    assert company_report_calls == [first_row.inn]
    assert any((output_dir / "company_reports").glob("*.md"))

    second_result = core.build_company_result(second_row)
    second_result.status = "completed"
    second_result.finished_at = core.utc_now_iso()
    second_result.sources["spark"] = core.SourceResult(source="spark", status="success")

    progress.persist_completed_company_result(second_result, total_rows=2, processed_rows=2)

    second_summary = json.loads((output_dir / "summary.json").read_text(encoding="utf-8"))
    assert second_summary["public_output_contract"]["terminal_run"] is False
    assert second_summary["public_output_contract"]["public_result_count"] == 2
    assert second_summary["public_output_contract"]["final_exports"]["state"] == "suppressed_until_run_finished"
    assert export_calls == []
    assert not (output_dir / "final_results.csv").exists()
    assert not (output_dir / "final_results.xlsx").exists()

    progress.run_finished(processed_rows=2)

    final_summary = json.loads((output_dir / "summary.json").read_text(encoding="utf-8"))
    assert final_summary["run_status"] == "completed"
    assert final_summary["finish_reason"] == "normal_completion"
    assert final_summary["public_output_contract"]["terminal_run"] is True
    assert final_summary["public_output_contract"]["final_exports"] == {
        "state": "terminal_completed",
        "available": True,
        "row_count": 2,
        "paths": ["final_results.csv", "final_results.xlsx"],
    }
    assert export_calls == [("csv", 2), ("xlsx", 2)]
    assert (output_dir / "final_results.csv").exists()
    assert (output_dir / "final_results.xlsx").exists()
    assert [item["inn"] for item in json.loads((output_dir / "results.json").read_text(encoding="utf-8"))] == [
        first_row.inn,
        second_row.inn,
    ]


def test_progress_store_reload_suppresses_stale_final_exports_for_non_terminal_run(tmp_path) -> None:
    output_dir = tmp_path / "output"
    progress = ProgressStore(output_dir)
    progress.run_started(
        input_path="input.xlsx",
        total_rows=3,
        selected_rows=3,
        selection_mode="window",
        selected_ordinals=[],
        start_from=1,
        end_at=3,
        active_sources=["spark"],
        continue_existing_run=False,
    )

    row = _rows(1)[0]
    result = core.build_company_result(row)
    result.status = "completed"
    result.finished_at = core.utc_now_iso()
    result.sources["spark"] = core.SourceResult(source="spark", status="success")
    progress.persist_completed_company_result(result, total_rows=3, processed_rows=1)

    (output_dir / "final_results.csv").write_text("stale terminal-looking csv", encoding="utf-8")
    (output_dir / "final_results.xlsx").write_text("stale terminal-looking xlsx", encoding="utf-8")

    reloaded = ProgressStore(output_dir)

    assert reloaded.get(row.inn) is not None
    assert not (output_dir / "final_results.csv").exists()
    assert not (output_dir / "final_results.xlsx").exists()
    summary = json.loads((output_dir / "summary.json").read_text(encoding="utf-8"))
    assert summary["run_status"] == "running"
    assert summary["completed_rows"] == 1
    assert summary["remaining_rows"] == 2
    assert summary["public_output_contract"] == {
        "contract_version": 1,
        "terminal_run": False,
        "run_finished_required_for_final_exports": True,
        "run_status": "running",
        "finish_reason": "",
        "public_result_count": 1,
        "checkpoint_only_count": 0,
        "remaining_rows": 2,
        "final_exports": {
            "state": "suppressed_until_run_finished",
            "available": False,
            "row_count": 0,
            "paths": ["final_results.csv", "final_results.xlsx"],
        },
        "warning": (
            "Non-terminal run: final_results.csv and final_results.xlsx "
            "are suppressed until run_finished is recorded."
        ),
    }


def _stage_outbox_cursor(output_dir) -> dict[str, object]:
    runtime_state = json.loads((output_dir / "runtime_state.json").read_text(encoding="utf-8"))
    return dict(runtime_state["run"]["metadata"]["stage_outbox_cursor"])


def _stage_handoffs(output_dir) -> dict[str, object]:
    runtime_state = json.loads((output_dir / "runtime_state.json").read_text(encoding="utf-8"))
    return dict(runtime_state["run"]["metadata"]["stage_handoffs"])


def _stage_pickups(output_dir) -> dict[str, object]:
    runtime_state = json.loads((output_dir / "runtime_state.json").read_text(encoding="utf-8"))
    return dict(runtime_state["run"]["metadata"]["stage_pickups"])


def _merge_stage_runtime_surfaces(payload: dict[str, object]) -> dict[str, object]:
    merged_companies = dict((payload.get("aggregator_site") or {}).get("companies") or {})
    for surface_key, surface_payload in payload.items():
        if surface_key == "aggregator_site" or not isinstance(surface_payload, dict):
            continue
        for inn, company in ((surface_payload.get("companies") or {}) if isinstance(surface_payload, dict) else {}).items():
            merged_companies[str(inn)] = company
    return {"aggregator_site": {"companies": merged_companies}}


def _stage_work_units(output_dir) -> dict[str, object]:
    runtime_state = json.loads((output_dir / "runtime_state.json").read_text(encoding="utf-8"))
    return _merge_stage_runtime_surfaces(dict(runtime_state["run"]["metadata"]["stage_work_units"]))


def _raw_stage_work_units(output_dir) -> dict[str, object]:
    runtime_state = json.loads((output_dir / "runtime_state.json").read_text(encoding="utf-8"))
    return dict(runtime_state["run"]["metadata"]["stage_work_units"])


def _stage_execution_evidence(output_dir) -> dict[str, object]:
    runtime_state = json.loads((output_dir / "runtime_state.json").read_text(encoding="utf-8"))
    return _merge_stage_runtime_surfaces(dict(runtime_state["run"]["metadata"].get("stage_execution_evidence", {})))


def _raw_stage_execution_evidence(output_dir) -> dict[str, object]:
    runtime_state = json.loads((output_dir / "runtime_state.json").read_text(encoding="utf-8"))
    return dict(runtime_state["run"]["metadata"].get("stage_execution_evidence", {}))


def _public_publish_state(output_dir) -> dict[str, object]:
    return json.loads((output_dir / "_runtime" / "public_publish_state.json").read_text(encoding="utf-8"))


def test_sequential_run_writes_stage_message_outbox_with_runtime_run_id(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    rows = _rows(1)
    _install_lightweight_run_stubs(
        monkeypatch,
        rows=rows,
        domain_resolution=_domain_resolution_for(
            rows[0],
            primary_url="https://alpha.example",
            secondary_url="https://beta.example",
            primary_confidence=0.88,
            secondary_confidence=0.53,
        ),
        candidate_sites=["https://alpha.example", "https://beta.example"],
        validated_sites=[_site_decision()],
        site_probes=[{"url": "https://alpha.example/about"}, {"url": "https://alpha.example/catalog"}],
        route_strategies=["primary", "fallback"],
        content_records=[
            {"kind": "html", "nested": {"drop": True}},
            {"kind": "attachment"},
            {"kind": "contact"},
        ],
        lead_cards=[{"title": "Lead 1"}],
    )
    output_dir = tmp_path / "output"
    time_points = iter([100.0, 100.75, 200.0, 200.25])
    monkeypatch.setattr(pipeline, "time", SimpleNamespace(time=lambda: next(time_points)))

    persist_stage_types: list[list[str]] = []
    original_persist = pipeline.ProgressStore.persist_completed_company_result

    def wrapped_persist(self, *args, **kwargs):
        persist_stage_types.append([item["message_type"] for item in load_stage_messages(self.output_dir)])
        result = original_persist(self, *args, **kwargs)
        persist_stage_types.append([item["message_type"] for item in load_stage_messages(self.output_dir)])
        return result

    monkeypatch.setattr(pipeline.ProgressStore, "persist_completed_company_result", wrapped_persist)

    exit_code = _run_stage_message_pipeline(output_dir)

    assert exit_code == 0

    outbox_path = stage_message_outbox_path(output_dir)
    assert outbox_path.exists()

    stage_messages = load_stage_messages(output_dir)
    assert [message["message_type"] for message in stage_messages] == [
        "source_result_ready",
        "source_result_ready",
        "candidate_site_found",
        "candidate_site_found",
        "site_gate_decision",
        "deep_parse_done",
        "company_completed",
    ]
    assert [message["stage"] for message in stage_messages] == [
        "source_collect",
        "source_collect",
        "candidate_site_selection",
        "candidate_site_selection",
        "site_gate",
        "deep_site_parse",
        "finalize_company",
    ]
    source_payloads = [message["payload"] for message in stage_messages[:2]]
    assert [
        {
            "duration_seconds": payload["duration_seconds"],
            "source": payload["source"],
            "status": payload["status"],
        }
        for payload in source_payloads
    ] == [
        {"duration_seconds": 0.75, "source": "spark", "status": "ok"},
        {"duration_seconds": 0.25, "source": "zachestnyibiznes", "status": "ok"},
    ]
    assert all(payload["started_at"] for payload in source_payloads)
    assert all(payload["finished_at"] for payload in source_payloads)
    assert [message["payload"] for message in stage_messages[2:4]] == [
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
    assert stage_messages[4]["payload"] == {
        "authenticity_score": 0.933,
        "belongs_to_company": True,
        "decision_status": "candidate",
        "identity_score": 0.821,
        "site_url": "https://alpha.example/about",
        "viability_score": 0.744,
    }
    assert stage_messages[5]["payload"] == {
        "content_records_count": 3,
        "decision_status": "candidate",
        "lead_cards_count": 1,
        "route_strategies_count": 2,
        "site_probes_count": 2,
        "site_url": "https://alpha.example/about",
    }
    assert all(not isinstance(value, (dict, list)) for value in stage_messages[5]["payload"].values())
    assert stage_messages[6]["payload"] == {
        "status": "completed",
        "updated_sources": ["spark", "zachestnyibiznes"],
    }
    assert persist_stage_types == [
        [
            "source_result_ready",
            "source_result_ready",
            "candidate_site_found",
            "candidate_site_found",
            "site_gate_decision",
            "deep_parse_done",
            "company_completed",
        ],
        [
            "source_result_ready",
            "source_result_ready",
            "candidate_site_found",
            "candidate_site_found",
            "site_gate_decision",
            "deep_parse_done",
            "company_completed",
        ],
    ]

    summary = json.loads((output_dir / "summary.json").read_text(encoding="utf-8"))
    runtime_state = json.loads((output_dir / "runtime_state.json").read_text(encoding="utf-8"))
    run_id = summary["run_id"]
    assert run_id == runtime_state["run"]["metadata"]["run_id"]
    assert runtime_state["run"]["metadata"]["stage_outbox_cursor"] == {
        "outbox_path": "_runtime/stage_messages.jsonl",
        "byte_offset": outbox_path.stat().st_size,
    }
    handoff_company = _stage_handoffs(output_dir)["aggregator_site"]["companies"]["0000000001"]
    assert handoff_company["row_index"] == 2
    assert handoff_company["source_results"] == [
        {
            **stage_messages[0]["payload"],
            "stage": stage_messages[0]["stage"],
            "ts": stage_messages[0]["ts"],
        },
        {
            **stage_messages[1]["payload"],
            "stage": stage_messages[1]["stage"],
            "ts": stage_messages[1]["ts"],
        },
    ]
    assert handoff_company["candidate_sites"] == [
        {
            **stage_messages[2]["payload"],
            "stage": stage_messages[2]["stage"],
            "ts": stage_messages[2]["ts"],
        },
        {
            **stage_messages[3]["payload"],
            "stage": stage_messages[3]["stage"],
            "ts": stage_messages[3]["ts"],
        },
    ]
    assert handoff_company["site_gate_decisions"] == [
        {
            **stage_messages[4]["payload"],
            "stage": stage_messages[4]["stage"],
            "ts": stage_messages[4]["ts"],
        }
    ]
    assert handoff_company["deep_parse_done"] == {
        **stage_messages[5]["payload"],
        "stage": stage_messages[5]["stage"],
        "ts": stage_messages[5]["ts"],
    }
    assert handoff_company["company_completed"] == {
        **stage_messages[6]["payload"],
        "stage": stage_messages[6]["stage"],
        "ts": stage_messages[6]["ts"],
    }
    assert {message["run_id"] for message in stage_messages} == {run_id}
    assert {message["inn"] for message in stage_messages} == {"0000000001"}
    assert {message["row_index"] for message in stage_messages} == {2}

    reloaded_progress = ProgressStore(output_dir)
    consumed_cursor = _stage_outbox_cursor(output_dir)
    assert reloaded_progress.materialize_unread_stage_handoffs() == []
    assert reloaded_progress.consume_unread_stage_messages() == []
    assert reloaded_progress.consume_unread_stage_messages() == []
    assert _stage_outbox_cursor(output_dir) == consumed_cursor

    results_log = [
        json.loads(line)
        for line in (output_dir / "results.jsonl").read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    assert len(results_log) == 1
    assert "message_type" not in results_log[0]

    event_types = [
        json.loads(line)["type"]
        for line in (output_dir / "events.jsonl").read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    assert event_types == ["run_started", "run_finished"]


def test_sequential_run_executes_explicit_stage_work_unit_and_acks_it(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    rows = _rows(1)
    _install_lightweight_run_stubs(
        monkeypatch,
        rows=rows,
        domain_resolution=_domain_resolution_for(
            rows[0],
            primary_url="https://alpha.example",
            secondary_url="https://beta.example",
            primary_confidence=0.88,
            secondary_confidence=0.53,
        ),
        candidate_sites=["https://alpha.example", "https://beta.example"],
        validated_sites=[_site_decision()],
    )
    output_dir = tmp_path / "output"
    time_points = iter([100.0, 100.75, 200.0, 200.25])
    monkeypatch.setattr(pipeline, "time", SimpleNamespace(time=lambda: next(time_points)))

    assert _run_stage_message_pipeline(output_dir) == 0

    handoff_company = _stage_handoffs(output_dir)["aggregator_site"]["companies"]["0000000001"]
    pickup_company = _stage_pickups(output_dir)["aggregator_site"]["companies"]["0000000001"]
    work_unit = _stage_work_units(output_dir)["aggregator_site"]["companies"]["0000000001"]

    assert pickup_company["queue_status"] == "ready"
    assert pickup_company["picked_up_handoff_fingerprint"] == pickup_company["handoff_fingerprint"]
    assert work_unit["work_status"] == "acked"
    assert work_unit["acknowledged_at"] != ""
    assert work_unit["execution_boundary"] == pipeline.DEEP_PARSE_EXECUTION_BOUNDARY
    assert work_unit["handoff_fingerprint"] != pickup_company["handoff_fingerprint"]
    assert [
        {
            "source": item["source"],
            "status": item["status"],
        }
        for item in work_unit["work_unit"]["source_results"]
    ] == [
        {"source": "spark", "status": "ok"},
        {"source": "zachestnyibiznes", "status": "ok"},
    ]
    assert [item["site_url"] for item in work_unit["work_unit"]["candidate_sites"]] == [
        item["site_url"] for item in handoff_company["candidate_sites"]
    ]
    assert work_unit["work_unit"]["updated_sources"] == ["spark", "zachestnyibiznes"]
    assert "company_completed" not in work_unit["work_unit"]
    assert _stage_execution_evidence(output_dir)["aggregator_site"]["companies"]["0000000001"] == work_unit
    assert ProgressStore(output_dir).pending_stage_work_units() == []
    assert ProgressStore(output_dir).consume_pending_stage_work_units() == []
    assert ProgressStore(output_dir).consume_pickup_ready_stage_handoffs() == []
    assert ProgressStore(output_dir).pickup_ready_stage_handoffs() == []
    public_outputs = json.dumps(
        {
            "summary": json.loads((output_dir / "summary.json").read_text(encoding="utf-8")),
            "results": json.loads((output_dir / "results.json").read_text(encoding="utf-8")),
        },
        ensure_ascii=False,
    )
    assert "stage_execution_evidence" not in public_outputs
    assert "execution_boundary" not in public_outputs
    assert "handoff_fingerprint" not in public_outputs


def test_fresh_run_writes_host_event_stage_message_with_narrow_payload(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    rows = _rows(1)
    _install_lightweight_run_stubs(
        monkeypatch,
        rows=rows,
        domain_resolution=_domain_resolution_for(
            rows[0],
            primary_url="https://alpha.example",
            secondary_url="https://beta.example",
            primary_confidence=0.88,
            secondary_confidence=0.53,
        ),
        candidate_sites=["https://alpha.example", "https://beta.example"],
        validated_sites=[_site_decision()],
    )
    monkeypatch.setattr(
        pipeline,
        "SparkSource",
        lambda client: _EventWritingSource("spark", client.progress_store, _host_runtime_event),
    )
    output_dir = tmp_path / "output"

    exit_code = _run_stage_message_pipeline(output_dir)

    assert exit_code == 0
    assert stage_message_outbox_path(output_dir).exists()

    stage_messages = load_stage_messages(output_dir)
    assert [message["message_type"] for message in stage_messages] == [
        "host_event",
        "source_result_ready",
        "source_result_ready",
        "candidate_site_found",
        "candidate_site_found",
        "site_gate_decision",
        "deep_parse_done",
        "company_completed",
    ]

    host_messages = [message for message in stage_messages if message["message_type"] == "host_event"]
    assert len(host_messages) == 1
    host_message = host_messages[0]
    assert host_message["stage"] == "runtime_host"
    assert host_message["inn"] == "host:spark-interfax.ru"
    assert host_message["row_index"] == 1
    assert host_message["payload"] == {
        "cooldown_seconds": 15,
        "event_type": "route_fetch_attempt",
        "host": "spark-interfax.ru",
        "interval_seconds": 2.5,
        "proxy_label": "pool-a",
        "source": "spark",
        "status": "success",
    }
    assert "url" not in host_message["payload"]
    assert "details" not in host_message["payload"]
    assert all(not isinstance(value, (dict, list)) for value in host_message["payload"].values())

    deep_parse_messages = [message for message in stage_messages if message["message_type"] == "deep_parse_done"]
    assert len(deep_parse_messages) == 1
    assert deep_parse_messages[0]["payload"] == {
        "content_records_count": 0,
        "decision_status": "candidate",
        "lead_cards_count": 0,
        "route_strategies_count": 0,
        "site_probes_count": 0,
        "site_url": "https://alpha.example/about",
    }
    assert all(not isinstance(value, (dict, list)) for value in deep_parse_messages[0]["payload"].values())

    summary = json.loads((output_dir / "summary.json").read_text(encoding="utf-8"))
    runtime_state = json.loads((output_dir / "runtime_state.json").read_text(encoding="utf-8"))
    run_id = summary["run_id"]
    assert run_id == runtime_state["run"]["metadata"]["run_id"]
    assert host_message["run_id"] == run_id

    results_log = [
        json.loads(line)
        for line in (output_dir / "results.jsonl").read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    assert len(results_log) == 1
    assert all("message_type" not in item and "stage" not in item for item in results_log)

    event_log = [
        json.loads(line)
        for line in (output_dir / "events.jsonl").read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    assert [item["type"] for item in event_log] == ["run_started", "route_fetch_attempt", "run_finished"]
    assert all("message_type" not in item and "stage" not in item for item in event_log)


def test_direct_default_prefetched_downstream_runtime_events_replay_before_site_gate_messages(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    rows = _rows(1)
    _install_lightweight_run_stubs(
        monkeypatch,
        rows=rows,
        domain_resolution=_domain_resolution_for(
            rows[0],
            primary_url="https://alpha.example",
            secondary_url="https://beta.example",
            primary_confidence=0.88,
            secondary_confidence=0.53,
        ),
        candidate_sites=["https://alpha.example", "https://beta.example"],
        validated_sites=[],
    )

    def fake_analyzer_factory(client, *_args, **_kwargs):
        return SimpleNamespace(
            client=client,
            h=SimpleNamespace(
                normalize_url=lambda value: core.sanitize_website_url(value) or str(value or "").strip()
            ),
            llm=SimpleNamespace(
                should_force_benchmark_stage=lambda _stage: False,
                judge_content_record=lambda *_args, **_kwargs: None,
                capture_forced_content_review_fixture=lambda **_kwargs: None,
            ),
        )

    def fake_gate(*, row, analyzer, **_kwargs):
        analyzer.client.progress_store.append_event(
            {
                "ts": core.utc_now_iso(),
                "type": "route_fetch_attempt",
                "host": "alpha.example",
                "source": "company_site",
                "status": "success",
                "cooldown_seconds": 9,
                "since_previous_request_seconds": 1.5,
                "proxy_label_or_id": "prefetch-a",
                "inn": row.inn,
            }
        )
        return SimpleNamespace(
            deep_parse_sites=[],
            surface_only_decisions=[
                _site_decision(url="https://alpha.example", final_url="https://alpha.example/about")
            ],
            trusted_surface_decisions_by_site={},
            notes=[],
        )

    monkeypatch.setattr(pipeline, "BenchmarkAwareSiteAuthenticityAnalyzer", fake_analyzer_factory)
    monkeypatch.setattr(pipeline, "gate_candidate_sites_before_deep_parse", fake_gate)
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
                "--company-concurrency=2",
            ]
        )
    )

    assert exit_code == 0
    stage_messages = load_stage_messages(output_dir)
    assert [message["message_type"] for message in stage_messages] == [
        "source_result_ready",
        "source_result_ready",
        "candidate_site_found",
        "candidate_site_found",
        "host_event",
        "site_gate_decision",
        "deep_parse_done",
        "company_completed",
    ]
    host_message = next(message for message in stage_messages if message["message_type"] == "host_event")
    assert host_message["stage"] == "runtime_host"
    assert host_message["inn"] == rows[0].inn
    assert host_message["payload"] == {
        "cooldown_seconds": 9,
        "event_type": "route_fetch_attempt",
        "host": "alpha.example",
        "interval_seconds": 1.5,
        "proxy_label": "prefetch-a",
        "source": "company_site",
        "status": "success",
    }


def test_progress_store_pickup_tracks_pending_ready_and_idempotent_versions(tmp_path) -> None:
    output_dir = tmp_path / "output"
    progress = ProgressStore(output_dir)
    progress.run_started(
        input_path="input.xlsx",
        total_rows=1,
        selected_rows=1,
        selection_mode="ordinals",
        selected_ordinals=[1],
        start_from=1,
        end_at=1,
        active_sources=["spark"],
        continue_existing_run=False,
    )

    progress.emit_stage_message(
        message_type="source_result_ready",
        stage="source_collect",
        inn="7700000001",
        row_index=1,
        payload={"source": "spark", "status": "ok"},
        ts="2026-04-18T10:00:00Z",
    )
    progress.materialize_unread_stage_handoffs()

    pending_pickup = _stage_pickups(output_dir)["aggregator_site"]["companies"]["7700000001"]
    assert pending_pickup["queue_status"] == "pending"
    assert pending_pickup["picked_up_handoff_fingerprint"] == ""
    assert progress.pickup_ready_stage_handoffs() == []

    progress.emit_stage_message(
        message_type="company_completed",
        stage="finalize_company",
        inn="7700000001",
        row_index=1,
        payload={"status": "completed", "updated_sources": ["spark"]},
        ts="2026-04-18T10:05:00Z",
    )
    progress.materialize_unread_stage_handoffs()

    ready_pickup = _stage_pickups(output_dir)["aggregator_site"]["companies"]["7700000001"]
    assert ready_pickup["queue_status"] == "ready"
    assert ready_pickup["picked_up_handoff_fingerprint"] == ""

    first_pickup = progress.pickup_ready_stage_handoffs()
    assert [item["inn"] for item in first_pickup] == ["7700000001"]
    assert first_pickup[0]["company_completed"]["status"] == "completed"

    picked_up_state = _stage_pickups(output_dir)["aggregator_site"]["companies"]["7700000001"]
    assert picked_up_state["picked_up_handoff_fingerprint"] == picked_up_state["handoff_fingerprint"]
    assert picked_up_state["last_picked_up_message_ts"] == "2026-04-18T10:05:00Z"
    assert progress.pickup_ready_stage_handoffs() == []

    progress.emit_stage_message(
        message_type="source_result_ready",
        stage="source_collect",
        inn="7700000001",
        row_index=1,
        payload={"source": "spark", "status": "refreshed"},
        ts="2026-04-18T10:10:00Z",
    )
    progress.materialize_unread_stage_handoffs()

    reopened_pickup = _stage_pickups(output_dir)["aggregator_site"]["companies"]["7700000001"]
    assert reopened_pickup["queue_status"] == "pending"
    assert reopened_pickup["picked_up_handoff_fingerprint"] != reopened_pickup["handoff_fingerprint"]
    assert progress.pickup_ready_stage_handoffs() == []

    progress.emit_stage_message(
        message_type="company_completed",
        stage="finalize_company",
        inn="7700000001",
        row_index=1,
        payload={"status": "completed", "updated_sources": ["spark"]},
        ts="2026-04-18T10:15:00Z",
    )
    progress.materialize_unread_stage_handoffs()

    second_pickup = progress.pickup_ready_stage_handoffs()
    assert [item["inn"] for item in second_pickup] == ["7700000001"]
    assert second_pickup[0]["source_results"] == [
        {
            "source": "spark",
            "stage": "source_collect",
            "status": "refreshed",
            "ts": "2026-04-18T10:10:00Z",
        }
    ]
    assert progress.pickup_ready_stage_handoffs() == []


def test_progress_store_consume_pickups_materializes_idempotent_work_units_and_acks(tmp_path) -> None:
    output_dir = tmp_path / "output"
    progress = ProgressStore(output_dir)
    progress.run_started(
        input_path="input.xlsx",
        total_rows=1,
        selected_rows=1,
        selection_mode="ordinals",
        selected_ordinals=[1],
        start_from=1,
        end_at=1,
        active_sources=["spark"],
        continue_existing_run=False,
    )

    progress.emit_stage_message(
        message_type="source_result_ready",
        stage="source_collect",
        inn="7700000001",
        row_index=1,
        payload={"source": "spark", "status": "ok"},
        ts="2026-04-18T10:00:00Z",
    )
    progress.materialize_unread_stage_handoffs()
    progress.emit_stage_message(
        message_type="company_completed",
        stage="finalize_company",
        inn="7700000001",
        row_index=1,
        payload={"status": "completed", "updated_sources": ["spark"]},
        ts="2026-04-18T10:05:00Z",
    )
    progress.materialize_unread_stage_handoffs()

    first_consume = progress.consume_pickup_ready_stage_handoffs()
    assert [item["inn"] for item in first_consume] == ["7700000001"]
    assert first_consume[0]["work_status"] == "pending"
    assert first_consume[0]["work_unit"]["company_completed"]["status"] == "completed"

    first_work_unit = _stage_work_units(output_dir)["aggregator_site"]["companies"]["7700000001"]
    assert first_work_unit["handoff_fingerprint"] == first_consume[0]["handoff_fingerprint"]
    assert first_work_unit["acknowledged_at"] == ""
    assert progress.consume_pickup_ready_stage_handoffs() == first_consume

    assert progress.ack_stage_handoff_work_unit(
        inn="7700000001",
        handoff_fingerprint=first_consume[0]["handoff_fingerprint"],
        acknowledged_at="2026-04-18T10:06:00Z",
    )
    acked_work_unit = _stage_work_units(output_dir)["aggregator_site"]["companies"]["7700000001"]
    assert acked_work_unit["work_status"] == "acked"
    assert acked_work_unit["acknowledged_at"] == "2026-04-18T10:06:00Z"
    assert progress.consume_pickup_ready_stage_handoffs() == []
    assert not progress.ack_stage_handoff_work_unit(
        inn="7700000001",
        handoff_fingerprint=first_consume[0]["handoff_fingerprint"],
        acknowledged_at="2026-04-18T10:07:00Z",
    )

    progress.emit_stage_message(
        message_type="source_result_ready",
        stage="source_collect",
        inn="7700000001",
        row_index=1,
        payload={"source": "spark", "status": "refreshed"},
        ts="2026-04-18T10:10:00Z",
    )
    progress.materialize_unread_stage_handoffs()
    progress.emit_stage_message(
        message_type="company_completed",
        stage="finalize_company",
        inn="7700000001",
        row_index=1,
        payload={"status": "completed", "updated_sources": ["spark"]},
        ts="2026-04-18T10:15:00Z",
    )
    progress.materialize_unread_stage_handoffs()

    second_consume = progress.consume_pickup_ready_stage_handoffs()
    assert [item["inn"] for item in second_consume] == ["7700000001"]
    assert second_consume[0]["handoff_fingerprint"] != first_consume[0]["handoff_fingerprint"]
    assert second_consume[0]["work_unit"]["source_results"] == [
        {
            "source": "spark",
            "stage": "source_collect",
            "status": "refreshed",
            "ts": "2026-04-18T10:10:00Z",
        }
    ]
    assert progress.consume_pickup_ready_stage_handoffs() == second_consume


def test_controlled_stop_before_explicit_boundary_preserves_pending_work_unit_for_resume(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    rows = _rows(1)
    output_dir = tmp_path / "output"
    validated_sites = [_site_decision()]
    _install_lightweight_run_stubs(
        monkeypatch,
        rows=rows,
        domain_resolution=_domain_resolution_for(
            rows[0],
            primary_url="https://alpha.example",
            secondary_url="https://beta.example",
            primary_confidence=0.88,
            secondary_confidence=0.53,
        ),
        candidate_sites=["https://alpha.example", "https://beta.example"],
        validated_sites=validated_sites,
    )

    original_materialize = pipeline.ProgressStore.materialize_stage_work_unit
    stop_requests = {"count": 0}

    def request_stop_after_aggregator_materialize(self, *args, **kwargs):
        work_unit = original_materialize(self, *args, **kwargs)
        if (
            kwargs.get("execution_boundary") == pipeline.AGGREGATOR_SITE_EXECUTION_BOUNDARY
            and stop_requests["count"] == 0
        ):
            stop_requests["count"] += 1
            self.request_controlled_stop(reason="operator requested stop before explicit boundary")
        return work_unit

    monkeypatch.setattr(
        pipeline.ProgressStore,
        "materialize_stage_work_unit",
        request_stop_after_aggregator_materialize,
    )

    assert _run_stage_message_pipeline(output_dir) == 0

    summary_before_resume = json.loads((output_dir / "summary.json").read_text(encoding="utf-8"))
    assert summary_before_resume["run_status"] == "controlled_stop"
    assert summary_before_resume["completed_rows"] == 0
    assert summary_before_resume["remaining_rows"] == 1

    stage_messages_before_resume = load_stage_messages(output_dir)
    assert [message["message_type"] for message in stage_messages_before_resume].count("source_result_ready") == 2
    assert [message["message_type"] for message in stage_messages_before_resume].count("candidate_site_found") == 2
    assert [message["message_type"] for message in stage_messages_before_resume].count("site_gate_decision") == 0
    assert [message["message_type"] for message in stage_messages_before_resume].count("deep_parse_done") == 0
    assert [message["message_type"] for message in stage_messages_before_resume].count("company_completed") == 0

    pending_before_resume = ProgressStore(output_dir).pending_stage_work_units(
        execution_boundary=pipeline.AGGREGATOR_SITE_EXECUTION_BOUNDARY,
    )
    assert [item["inn"] for item in pending_before_resume] == ["0000000001"]
    assert _stage_work_units(output_dir)["aggregator_site"]["companies"]["0000000001"]["work_status"] == "pending"

    assert _run_stage_message_pipeline(output_dir, "--resume") == 0

    summary_after_resume = json.loads((output_dir / "summary.json").read_text(encoding="utf-8"))
    assert summary_after_resume["run_status"] == "completed"
    assert summary_after_resume["finish_reason"] == "normal_completion"
    assert summary_after_resume["completed_rows"] == 1
    assert summary_after_resume["remaining_rows"] == 0

    stage_messages = load_stage_messages(output_dir)
    assert [message["message_type"] for message in stage_messages].count("source_result_ready") == 2
    assert [message["message_type"] for message in stage_messages].count("candidate_site_found") == 2
    assert [message["message_type"] for message in stage_messages].count("site_gate_decision") == 1
    assert [message["message_type"] for message in stage_messages].count("deep_parse_done") == 1
    assert [message["message_type"] for message in stage_messages].count("company_completed") == 1
    assert ProgressStore(output_dir).pending_stage_work_units() == []


def test_progress_store_replayed_explicit_work_unit_keeps_canonical_identity_after_ack(tmp_path) -> None:
    output_dir = tmp_path / "output"
    progress = ProgressStore(output_dir)
    progress.run_started(
        input_path="input.xlsx",
        total_rows=1,
        selected_rows=1,
        selection_mode="ordinals",
        selected_ordinals=[1],
        start_from=1,
        end_at=1,
        active_sources=["spark", "zachestnyibiznes"],
        continue_existing_run=False,
    )

    first_work_unit = progress.materialize_stage_work_unit(
        inn="7700000001",
        row_index=1,
        execution_boundary=pipeline.DEEP_PARSE_EXECUTION_BOUNDARY,
        work_unit_payload={
            "inn": "7700000001",
            "row_index": 1,
            "company_name": "Company 1",
            "source_results": [{"source": "spark", "status": "ok"}],
            "candidate_sites": [{"site_url": "https://alpha.example/catalog"}],
            "updated_sources": ["spark"],
            "known_contacts": {},
            "site_gate_decisions": [{"site_url": "https://alpha.example/about"}],
            "deep_parse_sites": ["https://alpha.example/about"],
            "gate_notes": [],
        },
    )
    assert first_work_unit["work_status"] == "pending"
    assert progress.ack_stage_handoff_work_unit(
        inn="7700000001",
        handoff_fingerprint=first_work_unit["handoff_fingerprint"],
        acknowledged_at="2026-04-18T10:06:00Z",
    )

    reloaded = ProgressStore(output_dir)
    replayed_work_unit = reloaded.materialize_stage_work_unit(
        inn="7700000001",
        row_index=1,
        execution_boundary=pipeline.DEEP_PARSE_EXECUTION_BOUNDARY,
        work_unit_payload={
            "inn": "7700000001",
            "row_index": 1,
            "company_name": "Company 1",
            "source_results": [{"source": "zachestnyibiznes", "status": "ok"}],
            "candidate_sites": [{"site_url": "https://alpha.example/about"}],
            "updated_sources": ["zachestnyibiznes"],
            "known_contacts": {},
            "site_gate_decisions": [{"site_url": "https://alpha.example/products"}],
            "deep_parse_sites": ["https://alpha.example/products"],
            "gate_notes": ["replayed"],
        },
    )

    raw_stage_work_units = _raw_stage_work_units(output_dir)
    stored_work_unit = raw_stage_work_units["deep_parse"]["companies"]["7700000001"]
    assert replayed_work_unit["handoff_fingerprint"] == first_work_unit["handoff_fingerprint"]
    assert replayed_work_unit["work_status"] == "acked"
    assert stored_work_unit["handoff_fingerprint"] == first_work_unit["handoff_fingerprint"]
    assert stored_work_unit["work_status"] == "acked"
    assert stored_work_unit["work_unit"]["candidate_sites"] == [{"site_url": "https://alpha.example/catalog"}]
    assert stored_work_unit["work_unit"]["deep_parse_sites"] == ["https://alpha.example/about"]
    assert raw_stage_work_units["aggregator_site"]["companies"] == {}
    assert reloaded.consume_pending_stage_work_units(
        execution_boundary=pipeline.DEEP_PARSE_EXECUTION_BOUNDARY,
    ) == []


def test_progress_store_replayed_explicit_work_unit_preserves_low_priority_queue_family_contour_after_ack(
    tmp_path,
) -> None:
    output_dir = tmp_path / "output"
    progress = ProgressStore(output_dir)
    progress.run_started(
        input_path="input.xlsx",
        total_rows=1,
        selected_rows=1,
        selection_mode="ordinals",
        selected_ordinals=[1],
        start_from=1,
        end_at=1,
        active_sources=["spark", "zachestnyibiznes"],
        continue_existing_run=False,
    )

    contour_payload = build_deep_parse_queue_family_contour().as_payload()
    first_work_unit = progress.materialize_stage_work_unit(
        inn="7700000001",
        row_index=1,
        execution_boundary=pipeline.DEEP_PARSE_EXECUTION_BOUNDARY,
        work_unit_payload={
            "inn": "7700000001",
            "row_index": 1,
            "company_name": "Company 1",
            "source_results": [{"source": "spark", "status": "ok"}],
            "candidate_sites": [{"site_url": "https://alpha.example/catalog"}],
            "updated_sources": ["spark"],
            "queue_family_contour": contour_payload,
            "known_contacts": {},
            "site_gate_decisions": [{"site_url": "https://alpha.example/about"}],
            "deep_parse_sites": ["https://alpha.example/about"],
            "gate_notes": [],
        },
    )

    raw_execution_evidence = _raw_stage_execution_evidence(output_dir)
    execution_evidence = raw_execution_evidence["deep_parse"]["companies"]["7700000001"]
    assert execution_evidence["work_unit"]["queue_family_contour"] == contour_payload
    assert normalize_queue_family_contour(execution_evidence["work_unit"]["queue_family_contour"]) == (
        build_deep_parse_queue_family_contour()
    )
    assert progress.ack_stage_handoff_work_unit(
        inn="7700000001",
        handoff_fingerprint=first_work_unit["handoff_fingerprint"],
        acknowledged_at="2026-04-18T10:06:00Z",
    )

    reloaded = ProgressStore(output_dir)
    replayed_work_unit = reloaded.materialize_stage_work_unit(
        inn="7700000001",
        row_index=1,
        execution_boundary=pipeline.DEEP_PARSE_EXECUTION_BOUNDARY,
        work_unit_payload={
            "inn": "7700000001",
            "row_index": 1,
            "company_name": "Company 1",
            "source_results": [{"source": "zachestnyibiznes", "status": "ok"}],
            "candidate_sites": [{"site_url": "https://alpha.example/catalog"}],
            "updated_sources": ["zachestnyibiznes"],
            "queue_family_contour": {
                "mainline": list(contour_payload["mainline"]),
                "low_priority": ["low_priority_llm"],
            },
            "known_contacts": {},
            "site_gate_decisions": [{"site_url": "https://alpha.example/about"}],
            "deep_parse_sites": ["https://alpha.example/about"],
            "gate_notes": ["replayed"],
        },
    )

    raw_stage_work_units = _raw_stage_work_units(output_dir)
    stored_work_unit = raw_stage_work_units["deep_parse"]["companies"]["7700000001"]
    stored_execution_evidence = _raw_stage_execution_evidence(output_dir)["deep_parse"]["companies"]["7700000001"]
    assert replayed_work_unit["handoff_fingerprint"] == first_work_unit["handoff_fingerprint"]
    assert replayed_work_unit["work_status"] == "acked"
    assert stored_work_unit["work_unit"]["queue_family_contour"] == contour_payload
    assert stored_execution_evidence["work_unit"]["queue_family_contour"] == contour_payload
    assert raw_stage_work_units["aggregator_site"]["companies"] == {}
    assert reloaded.consume_pending_stage_work_units(
        execution_boundary=pipeline.DEEP_PARSE_EXECUTION_BOUNDARY,
    ) == []


def test_progress_store_replayed_explicit_work_unit_preserves_downstream_worker_pool_contour_after_ack(
    tmp_path,
) -> None:
    output_dir = tmp_path / "output"
    progress = ProgressStore(output_dir)
    progress.run_started(
        input_path="input.xlsx",
        total_rows=1,
        selected_rows=1,
        selection_mode="ordinals",
        selected_ordinals=[1],
        start_from=1,
        end_at=1,
        active_sources=["spark", "zachestnyibiznes"],
        continue_existing_run=False,
        downstream_worker_pools=build_downstream_worker_pool_contour(company_concurrency_cap=2).as_payload(),
    )

    pool_payload = build_downstream_worker_pool_contour(company_concurrency_cap=2).as_payload()
    first_work_unit = progress.materialize_stage_work_unit(
        inn="7700000001",
        row_index=1,
        execution_boundary=pipeline.DEEP_PARSE_EXECUTION_BOUNDARY,
        work_unit_payload={
            "inn": "7700000001",
            "row_index": 1,
            "company_name": "Company 1",
            "source_results": [{"source": "spark", "status": "ok"}],
            "candidate_sites": [{"site_url": "https://alpha.example/catalog"}],
            "updated_sources": ["spark"],
            "queue_family_contour": build_deep_parse_queue_family_contour().as_payload(),
            "downstream_worker_pools": pool_payload,
            "known_contacts": {},
            "site_gate_decisions": [{"site_url": "https://alpha.example/about"}],
            "deep_parse_sites": ["https://alpha.example/about"],
            "gate_notes": [],
        },
    )
    assert progress.ack_stage_handoff_work_unit(
        inn="7700000001",
        handoff_fingerprint=first_work_unit["handoff_fingerprint"],
        acknowledged_at="2026-04-18T10:06:00Z",
    )

    reloaded = ProgressStore(output_dir)
    replayed_work_unit = reloaded.materialize_stage_work_unit(
        inn="7700000001",
        row_index=1,
        execution_boundary=pipeline.DEEP_PARSE_EXECUTION_BOUNDARY,
        work_unit_payload={
            "inn": "7700000001",
            "row_index": 1,
            "company_name": "Company 1",
            "source_results": [{"source": "zachestnyibiznes", "status": "ok"}],
            "candidate_sites": [{"site_url": "https://alpha.example/catalog"}],
            "updated_sources": ["zachestnyibiznes"],
            "queue_family_contour": build_deep_parse_queue_family_contour().as_payload(),
            "downstream_worker_pools": build_downstream_worker_pool_contour(
                company_concurrency_cap=3,
                low_priority_budget=2,
            ).as_payload(),
            "known_contacts": {},
            "site_gate_decisions": [{"site_url": "https://alpha.example/about"}],
            "deep_parse_sites": ["https://alpha.example/about"],
            "gate_notes": ["replayed"],
        },
    )

    raw_stage_work_units = _raw_stage_work_units(output_dir)
    stored_work_unit = raw_stage_work_units["deep_parse"]["companies"]["7700000001"]
    stored_execution_evidence = _raw_stage_execution_evidence(output_dir)["deep_parse"]["companies"]["7700000001"]
    assert replayed_work_unit["handoff_fingerprint"] == first_work_unit["handoff_fingerprint"]
    assert replayed_work_unit["work_status"] == "acked"
    assert stored_work_unit["work_unit"]["downstream_worker_pools"] == pool_payload
    assert stored_execution_evidence["work_unit"]["downstream_worker_pools"] == pool_payload
    assert raw_stage_work_units["aggregator_site"]["companies"] == {}
    assert reloaded.consume_pending_stage_work_units(
        execution_boundary=pipeline.DEEP_PARSE_EXECUTION_BOUNDARY,
    ) == []


def test_progress_store_pending_checkpointed_result_stays_private_until_explicit_ack(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    output_dir = tmp_path / "output"
    row = _rows(1)[0]
    monkeypatch.setattr(ProgressStore, "_write_markdown_reports", lambda self, ordered_results=None: None)

    progress = ProgressStore(output_dir)
    progress.run_started(
        input_path="input.xlsx",
        total_rows=1,
        selected_rows=1,
        selection_mode="ordinals",
        selected_ordinals=[1],
        start_from=1,
        end_at=1,
        active_sources=["spark", "zachestnyibiznes"],
        continue_existing_run=False,
    )

    work_unit = progress.materialize_stage_work_unit(
        inn=row.inn,
        row_index=row.row_index,
        execution_boundary=pipeline.DEEP_PARSE_EXECUTION_BOUNDARY,
        work_unit_payload={
            "inn": row.inn,
            "row_index": row.row_index,
            "company_name": row.company_name,
            "source_results": [{"source": "spark", "status": "ok"}],
            "candidate_sites": [{"site_url": "https://alpha.example/about"}],
            "updated_sources": ["spark"],
            "known_contacts": {},
            "site_gate_decisions": [{"site_url": "https://alpha.example/about"}],
            "deep_parse_sites": ["https://alpha.example/about"],
            "gate_notes": [],
        },
    )

    completed_result = core.build_company_result(row)
    completed_result.finished_at = "2026-04-23T09:57:47Z"
    completed_result.status = "completed"
    progress.merge_stage_work_unit_private_state(
        inn=row.inn,
        handoff_fingerprint=work_unit["handoff_fingerprint"],
        private_state_patch={
            runtime_progress.COMPLETED_COMPANY_RESULT_CHECKPOINT_KEY: core.serialize_company_result(completed_result),
        },
    )

    progress.run_finished(
        processed_rows=0,
        run_status=runtime_progress.RUN_STATUS_ABORTED,
        finish_reason=runtime_progress.RUN_FINISH_REASON_ABORTED,
        terminal_context={
            "checkpoint": "explicit_boundary",
            "inn": row.inn,
            "execution_boundary": pipeline.DEEP_PARSE_EXECUTION_BOUNDARY,
        },
        terminal_error={
            "type": "RuntimeError",
            "message": "synthetic abort before explicit ack",
        },
    )

    crashed_summary = json.loads((output_dir / "summary.json").read_text(encoding="utf-8"))
    assert crashed_summary["processed_rows"] == 0
    assert crashed_summary["completed_rows"] == 0
    assert crashed_summary["remaining_rows"] == 1
    assert json.loads((output_dir / "results.json").read_text(encoding="utf-8")) == []

    reloaded = ProgressStore(output_dir)
    assert reloaded.get(row.inn) is None
    pending = reloaded.pending_stage_work_units(
        execution_boundary=pipeline.DEEP_PARSE_EXECUTION_BOUNDARY,
    )
    assert [item["inn"] for item in pending] == [row.inn]
    assert pending[0]["work_status"] == "pending"
    assert isinstance(
        _raw_stage_work_units(output_dir)["deep_parse"]["companies"][row.inn]["private_state"]["completed_company_result"],
        dict,
    )


def test_progress_store_reload_repairs_pickup_ack_without_materialized_work_unit(tmp_path) -> None:
    output_dir = tmp_path / "output"
    progress = ProgressStore(output_dir)
    progress.run_started(
        input_path="input.xlsx",
        total_rows=1,
        selected_rows=1,
        selection_mode="ordinals",
        selected_ordinals=[1],
        start_from=1,
        end_at=1,
        active_sources=["spark"],
        continue_existing_run=False,
    )

    progress.emit_stage_message(
        message_type="source_result_ready",
        stage="source_collect",
        inn="7700000001",
        row_index=1,
        payload={"source": "spark", "status": "ok"},
        ts="2026-04-18T10:00:00Z",
    )
    progress.materialize_unread_stage_handoffs()
    progress.emit_stage_message(
        message_type="company_completed",
        stage="finalize_company",
        inn="7700000001",
        row_index=1,
        payload={"status": "completed", "updated_sources": ["spark"]},
        ts="2026-04-18T10:05:00Z",
    )
    progress.materialize_unread_stage_handoffs()

    picked_handoffs = progress.pickup_ready_stage_handoffs()
    assert [item["inn"] for item in picked_handoffs] == ["7700000001"]
    picked_up_state = _stage_pickups(output_dir)["aggregator_site"]["companies"]["7700000001"]
    assert picked_up_state["picked_up_handoff_fingerprint"] == picked_up_state["handoff_fingerprint"]
    runtime_state = json.loads((output_dir / "runtime_state.json").read_text(encoding="utf-8"))
    runtime_state["run"]["metadata"]["stage_work_units"] = {"aggregator_site": {"companies": {}}}
    (output_dir / "runtime_state.json").write_text(
        json.dumps(runtime_state, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    assert _stage_work_units(output_dir) == {"aggregator_site": {"companies": {}}}

    reloaded = ProgressStore(output_dir)
    repaired_work_unit = _stage_work_units(output_dir)["aggregator_site"]["companies"]["7700000001"]
    assert repaired_work_unit["work_status"] == "pending"
    assert repaired_work_unit["handoff_fingerprint"] == picked_up_state["handoff_fingerprint"]
    assert reloaded.pickup_ready_stage_handoffs() == []

    pending_after_reload = reloaded.consume_pickup_ready_stage_handoffs()
    assert [item["inn"] for item in pending_after_reload] == ["7700000001"]
    assert pending_after_reload[0]["handoff_fingerprint"] == repaired_work_unit["handoff_fingerprint"]
    assert reloaded.consume_pickup_ready_stage_handoffs() == pending_after_reload


def test_progress_store_reload_repairs_explicit_work_unit_from_execution_evidence(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    rows = _rows(1)
    _install_lightweight_run_stubs(
        monkeypatch,
        rows=rows,
        domain_resolution=_domain_resolution_for(
            rows[0],
            primary_url="https://alpha.example",
            secondary_url="https://beta.example",
            primary_confidence=0.88,
            secondary_confidence=0.53,
        ),
        candidate_sites=["https://alpha.example", "https://beta.example"],
        validated_sites=[_site_decision()],
    )
    output_dir = tmp_path / "output"
    time_points = iter([100.0, 100.75, 200.0, 200.25])
    monkeypatch.setattr(pipeline, "time", SimpleNamespace(time=lambda: next(time_points)))

    assert _run_stage_message_pipeline(output_dir) == 0

    original_work_unit = _stage_work_units(output_dir)["aggregator_site"]["companies"]["0000000001"]
    execution_evidence = _stage_execution_evidence(output_dir)["aggregator_site"]["companies"]["0000000001"]
    assert execution_evidence == original_work_unit

    runtime_state = json.loads((output_dir / "runtime_state.json").read_text(encoding="utf-8"))
    runtime_state["run"]["metadata"]["stage_work_units"] = {"aggregator_site": {"companies": {}}}
    (output_dir / "runtime_state.json").write_text(
        json.dumps(runtime_state, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    assert _stage_work_units(output_dir) == {"aggregator_site": {"companies": {}}}

    reloaded = ProgressStore(output_dir)
    assert reloaded.pending_stage_work_units() == []
    assert reloaded.consume_pickup_ready_stage_handoffs() == []

    repaired_work_unit = _stage_work_units(output_dir)["aggregator_site"]["companies"]["0000000001"]
    assert repaired_work_unit["execution_boundary"] == pipeline.DEEP_PARSE_EXECUTION_BOUNDARY
    assert repaired_work_unit["work_status"] == "acked"
    assert repaired_work_unit["acknowledged_at"] == original_work_unit["acknowledged_at"]
    assert repaired_work_unit["handoff_fingerprint"] == original_work_unit["handoff_fingerprint"]
    assert repaired_work_unit["work_unit"] == original_work_unit["work_unit"]
    assert "company_completed" not in repaired_work_unit["work_unit"]
    assert "deep_parse_done" not in repaired_work_unit["work_unit"]


def test_fresh_non_resume_rerun_replaces_stage_message_outbox(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    rows = _rows(1)
    output_dir = tmp_path / "output"

    _install_lightweight_run_stubs(
        monkeypatch,
        rows=rows,
        domain_resolution=_domain_resolution_for(
            rows[0],
            primary_url="https://alpha.example",
            secondary_url="https://beta.example",
            primary_confidence=0.88,
            secondary_confidence=0.53,
        ),
        candidate_sites=["https://alpha.example", "https://beta.example"],
        validated_sites=[_site_decision()],
    )
    first_run_times = iter([100.0, 100.75, 200.0, 200.25])
    monkeypatch.setattr(pipeline, "time", SimpleNamespace(time=lambda: next(first_run_times)))
    assert _run_stage_message_pipeline(output_dir) == 0

    run_1_messages = load_stage_messages(output_dir)
    run_1_id = json.loads((output_dir / "summary.json").read_text(encoding="utf-8"))["run_id"]
    consumed_run_1_cursor = _stage_outbox_cursor(output_dir)
    first_progress = ProgressStore(output_dir)
    first_run_pickup = _stage_pickups(output_dir)["aggregator_site"]["companies"]["0000000001"]
    first_run_work_unit = _stage_work_units(output_dir)["aggregator_site"]["companies"]["0000000001"]
    assert first_run_pickup["picked_up_handoff_fingerprint"] == first_run_pickup["handoff_fingerprint"]
    assert first_run_work_unit["work_status"] == "acked"
    assert first_progress.pending_stage_work_units() == []
    assert first_progress.consume_pickup_ready_stage_handoffs() == []
    assert {message["run_id"] for message in run_1_messages} == {run_1_id}
    assert consumed_run_1_cursor["byte_offset"] > 0

    _install_lightweight_run_stubs(
        monkeypatch,
        rows=rows,
        domain_resolution=_domain_resolution_for(
            rows[0],
            primary_url="https://alpha.example",
            secondary_url="https://beta.example",
            primary_confidence=0.88,
            secondary_confidence=0.53,
        ),
        candidate_sites=["https://alpha.example", "https://beta.example"],
        validated_sites=[_site_decision()],
    )
    second_run_times = iter([300.0, 301.5, 400.0, 400.5])
    monkeypatch.setattr(pipeline, "time", SimpleNamespace(time=lambda: next(second_run_times)))
    assert _run_stage_message_pipeline(output_dir) == 0

    stage_messages = load_stage_messages(output_dir)
    summary = json.loads((output_dir / "summary.json").read_text(encoding="utf-8"))
    runtime_state = json.loads((output_dir / "runtime_state.json").read_text(encoding="utf-8"))
    run_2_id = summary["run_id"]

    assert run_1_id != run_2_id
    assert run_2_id == runtime_state["run"]["metadata"]["run_id"]
    assert runtime_state["run"]["metadata"]["stage_outbox_cursor"] == {
        "outbox_path": "_runtime/stage_messages.jsonl",
        "byte_offset": stage_message_outbox_path(output_dir).stat().st_size,
    }
    handoff_company = _stage_handoffs(output_dir)["aggregator_site"]["companies"]["0000000001"]
    rerun_pickup = _stage_pickups(output_dir)["aggregator_site"]["companies"]["0000000001"]
    assert [item["site_url"] for item in handoff_company["candidate_sites"]] == [
        "https://alpha.example",
        "https://beta.example",
    ]
    assert rerun_pickup["queue_status"] == "ready"
    rerun_work_unit = _stage_work_units(output_dir)["aggregator_site"]["companies"]["0000000001"]
    assert rerun_pickup["picked_up_handoff_fingerprint"] == rerun_pickup["handoff_fingerprint"]
    assert rerun_work_unit["work_status"] == "acked"
    assert rerun_work_unit["execution_boundary"] == pipeline.DEEP_PARSE_EXECUTION_BOUNDARY
    assert rerun_work_unit["handoff_fingerprint"] != rerun_pickup["handoff_fingerprint"]
    assert rerun_work_unit["handoff_fingerprint"] != first_run_work_unit["handoff_fingerprint"]
    assert rerun_work_unit["work_unit"] == first_run_work_unit["work_unit"]
    assert [item["site_url"] for item in rerun_work_unit["work_unit"]["candidate_sites"]] == [
        "https://alpha.example",
        "https://beta.example",
    ]
    assert _stage_execution_evidence(output_dir)["aggregator_site"]["companies"]["0000000001"] == rerun_work_unit
    assert {message["run_id"] for message in stage_messages} == {run_2_id}
    assert len(stage_messages) == len(run_1_messages)
    assert [message["payload"]["site_url"] for message in stage_messages if message["message_type"] == "candidate_site_found"] == [
        "https://alpha.example",
        "https://beta.example",
    ]
    assert stage_messages[5]["payload"]["site_url"] == "https://alpha.example/about"

    results_log = [
        json.loads(line)
        for line in (output_dir / "results.jsonl").read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    event_log = [
        json.loads(line)
        for line in (output_dir / "events.jsonl").read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    assert len(results_log) == 1
    assert [event["type"] for event in event_log] == ["run_started", "run_finished"]
    assert {event["run_id"] for event in event_log} == {run_2_id}


def test_fresh_non_resume_rerun_resets_pending_queue_state(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    rows = _rows(1)
    output_dir = tmp_path / "output"
    first_candidate_sites = ["https://alpha.example", "https://beta.example"]
    second_candidate_sites = ["https://fresh.example", "https://fresh-2.example"]
    first_validated_sites = [
        _site_decision(
            url=first_candidate_sites[0],
            final_url=f"{first_candidate_sites[0]}/about",
        )
    ]
    second_validated_sites = [
        _site_decision(
            url=second_candidate_sites[0],
            final_url=f"{second_candidate_sites[0]}/about",
        )
    ]

    _install_lightweight_run_stubs(
        monkeypatch,
        rows=rows,
        domain_resolution=_domain_resolution_for(
            rows[0],
            primary_url=first_candidate_sites[0],
            secondary_url=first_candidate_sites[1],
            primary_confidence=0.88,
            secondary_confidence=0.53,
        ),
        candidate_sites=first_candidate_sites,
        validated_sites=first_validated_sites,
    )

    search_calls: list[str] = []

    class _CountingSource(_StaticSource):
        def search(self, row: core.RowInput) -> core.SourceResult:
            search_calls.append(self.source_name)
            return super().search(row)

    def crash_before_deep_parse(_company):
        raise RuntimeError("synthetic crash after deep_parse queue materialize")

    monkeypatch.setattr(pipeline, "SparkSource", lambda _client: _CountingSource("spark"))
    monkeypatch.setattr(pipeline, "ZachestnyBiznesSource", lambda _client: _CountingSource("zachestnyibiznes"))
    monkeypatch.setattr(
        pipeline,
        "FactorySiteParser",
        lambda *_args, **_kwargs: SimpleNamespace(parse=crash_before_deep_parse),
    )
    first_run_times = iter([100.0, 100.75, 200.0, 200.25])
    monkeypatch.setattr(pipeline, "time", SimpleNamespace(time=lambda: next(first_run_times)))

    with pytest.raises(RuntimeError, match="synthetic crash after deep_parse queue materialize"):
        _run_stage_message_pipeline(output_dir)

    first_run_work_unit = _stage_work_units(output_dir)["aggregator_site"]["companies"]["0000000001"]
    assert first_run_work_unit["work_status"] == "pending"
    assert first_run_work_unit["execution_boundary"] == pipeline.DEEP_PARSE_EXECUTION_BOUNDARY
    assert [item["site_url"] for item in first_run_work_unit["work_unit"]["candidate_sites"]] == first_candidate_sites
    assert [message["message_type"] for message in load_stage_messages(output_dir)] == [
        "source_result_ready",
        "source_result_ready",
        "candidate_site_found",
        "candidate_site_found",
        "site_gate_decision",
    ]

    _install_lightweight_run_stubs(
        monkeypatch,
        rows=rows,
        domain_resolution=_domain_resolution_for(
            rows[0],
            primary_url=second_candidate_sites[0],
            secondary_url=second_candidate_sites[1],
            primary_confidence=0.91,
            secondary_confidence=0.57,
        ),
        candidate_sites=second_candidate_sites,
        validated_sites=second_validated_sites,
    )

    def parse_after_rerun(company):
        assert company.candidate_sites == [f"{second_candidate_sites[0]}/about"]
        return SimpleNamespace(
            plans=[
                SimpleNamespace(site_url=f"{second_candidate_sites[0]}/about", allows_deep_check=True)
            ],
            site_probes=[],
            route_strategies=[],
            content_records=[],
            notes=[],
        )

    monkeypatch.setattr(pipeline, "SparkSource", lambda _client: _CountingSource("spark"))
    monkeypatch.setattr(pipeline, "ZachestnyBiznesSource", lambda _client: _CountingSource("zachestnyibiznes"))
    monkeypatch.setattr(
        pipeline,
        "FactorySiteParser",
        lambda *_args, **_kwargs: SimpleNamespace(parse=parse_after_rerun),
    )
    second_run_times = iter([300.0, 301.25, 400.0, 400.5])
    monkeypatch.setattr(pipeline, "time", SimpleNamespace(time=lambda: next(second_run_times)))

    assert _run_stage_message_pipeline(output_dir) == 0

    stage_messages = load_stage_messages(output_dir)
    assert [message["message_type"] for message in stage_messages].count("source_result_ready") == 2
    assert [message["message_type"] for message in stage_messages].count("candidate_site_found") == 2
    assert [message["payload"]["site_url"] for message in stage_messages if message["message_type"] == "candidate_site_found"] == second_candidate_sites
    assert [item["candidate_sites"] for item in json.loads((output_dir / "results.json").read_text(encoding="utf-8"))] == [
        second_candidate_sites
    ]
    rerun_work_unit = _stage_work_units(output_dir)["aggregator_site"]["companies"]["0000000001"]
    assert rerun_work_unit["work_status"] == "acked"
    assert rerun_work_unit["handoff_fingerprint"] != first_run_work_unit["handoff_fingerprint"]
    assert [item["site_url"] for item in rerun_work_unit["work_unit"]["candidate_sites"]] == second_candidate_sites
    assert ProgressStore(output_dir).pending_stage_work_units() == []
    assert search_calls == ["spark", "zachestnyibiznes", "spark", "zachestnyibiznes"]


def test_resume_run_preserves_existing_stage_message_outbox(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    rows = _rows(1)
    output_dir = tmp_path / "output"

    _install_lightweight_run_stubs(
        monkeypatch,
        rows=rows,
        domain_resolution=_domain_resolution_for(
            rows[0],
            primary_url="https://alpha.example",
            secondary_url="https://beta.example",
            primary_confidence=0.88,
            secondary_confidence=0.53,
        ),
        candidate_sites=["https://alpha.example", "https://beta.example"],
        validated_sites=[_site_decision()],
    )
    first_run_times = iter([100.0, 100.75, 200.0, 200.25])
    monkeypatch.setattr(pipeline, "time", SimpleNamespace(time=lambda: next(first_run_times)))
    assert _run_stage_message_pipeline(output_dir) == 0

    stage_messages_before_resume = load_stage_messages(output_dir)
    run_id_before_resume = json.loads((output_dir / "summary.json").read_text(encoding="utf-8"))["run_id"]
    cursor_before_resume = _stage_outbox_cursor(output_dir)
    handoffs_before_resume = _stage_handoffs(output_dir)
    work_units_before_resume = _stage_work_units(output_dir)
    first_run_work_unit = work_units_before_resume["aggregator_site"]["companies"]["0000000001"]
    assert first_run_work_unit["work_status"] == "acked"
    first_progress = ProgressStore(output_dir)
    pickups_before_resume = _stage_pickups(output_dir)
    assert first_progress.pending_stage_work_units() == []
    assert first_progress.consume_pickup_ready_stage_handoffs() == []
    assert [message["message_type"] for message in stage_messages_before_resume].count("deep_parse_done") == 1
    assert cursor_before_resume["byte_offset"] > 0

    assert _run_stage_message_pipeline(output_dir, "--resume") == 0

    stage_messages_after_resume = load_stage_messages(output_dir)
    run_id_after_resume = json.loads((output_dir / "summary.json").read_text(encoding="utf-8"))["run_id"]

    assert run_id_after_resume == run_id_before_resume
    assert [message["message_type"] for message in stage_messages_after_resume].count("deep_parse_done") == 1
    assert stage_messages_after_resume == stage_messages_before_resume
    assert _stage_outbox_cursor(output_dir) == cursor_before_resume
    assert _stage_handoffs(output_dir) == handoffs_before_resume
    assert _stage_pickups(output_dir) == pickups_before_resume
    assert _stage_work_units(output_dir) == work_units_before_resume
    assert ProgressStore(output_dir).materialize_unread_stage_handoffs() == []
    assert ProgressStore(output_dir).consume_unread_stage_messages() == []
    assert ProgressStore(output_dir).pending_stage_work_units() == []
    assert ProgressStore(output_dir).consume_pickup_ready_stage_handoffs() == []
    assert ProgressStore(output_dir).pickup_ready_stage_handoffs() == []


def test_resume_recovery_replays_downstream_from_explicit_work_unit_without_upstream_duplicates(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    rows = _rows(1)
    output_dir = tmp_path / "output"
    validated_sites = [_site_decision()]
    _install_lightweight_run_stubs(
        monkeypatch,
        rows=rows,
        domain_resolution=_domain_resolution_for(
            rows[0],
            primary_url="https://alpha.example",
            secondary_url="https://beta.example",
            primary_confidence=0.88,
            secondary_confidence=0.53,
        ),
        candidate_sites=["https://alpha.example", "https://beta.example"],
        validated_sites=validated_sites,
    )

    search_calls: list[str] = []
    parse_calls = {"count": 0}

    class _CountingSource(_StaticSource):
        def search(self, row: core.RowInput) -> core.SourceResult:
            search_calls.append(self.source_name)
            return super().search(row)

    def crash_then_parse(company):
        parse_calls["count"] += 1
        if parse_calls["count"] == 1:
            raise RuntimeError("synthetic crash after deep_parse queue materialize")
        assert company.candidate_sites == ["https://alpha.example/about"]
        return SimpleNamespace(
            plans=[SimpleNamespace(site_url="https://alpha.example/about", allows_deep_check=True)],
            site_probes=[],
            route_strategies=[],
            content_records=[],
            notes=[],
        )

    monkeypatch.setattr(pipeline, "SparkSource", lambda _client: _CountingSource("spark"))
    monkeypatch.setattr(pipeline, "ZachestnyBiznesSource", lambda _client: _CountingSource("zachestnyibiznes"))
    monkeypatch.setattr(
        pipeline,
        "FactorySiteParser",
        lambda *_args, **_kwargs: SimpleNamespace(parse=crash_then_parse),
    )
    time_points = iter([100.0, 100.75, 200.0, 200.25])
    monkeypatch.setattr(pipeline, "time", SimpleNamespace(time=lambda: next(time_points)))

    with pytest.raises(RuntimeError, match="synthetic crash after deep_parse queue materialize"):
        _run_stage_message_pipeline(output_dir)

    assert search_calls == ["spark", "zachestnyibiznes"]
    work_unit_before_resume = _stage_work_units(output_dir)["aggregator_site"]["companies"]["0000000001"]
    assert work_unit_before_resume["work_status"] == "pending"
    assert work_unit_before_resume["execution_boundary"] == pipeline.DEEP_PARSE_EXECUTION_BOUNDARY
    assert _stage_execution_evidence(output_dir)["aggregator_site"]["companies"]["0000000001"] == work_unit_before_resume
    pending_before_resume = ProgressStore(output_dir).consume_pending_stage_work_units(
        execution_boundary=pipeline.DEEP_PARSE_EXECUTION_BOUNDARY,
    )
    assert [item["inn"] for item in pending_before_resume] == ["0000000001"]
    assert [item["site_url"] for item in pending_before_resume[0]["work_unit"]["candidate_sites"]] == [
        "https://alpha.example",
        "https://beta.example",
    ]
    assert pending_before_resume[0]["work_unit"]["updated_sources"] == ["spark", "zachestnyibiznes"]

    runtime_state = json.loads((output_dir / "runtime_state.json").read_text(encoding="utf-8"))
    runtime_state["run"]["metadata"]["stage_handoffs"]["aggregator_site"]["companies"]["0000000001"]["candidate_sites"] = [
        {
            "site_url": "https://legacy.example",
            "selection_rank": 1,
            "selection_source": "mutated_handoff",
            "stage": "candidate_site_selection",
            "ts": "2026-04-18T10:05:00Z",
        }
    ]
    (output_dir / "runtime_state.json").write_text(
        json.dumps(runtime_state, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    assert _run_stage_message_pipeline(output_dir, "--resume") == 0

    assert search_calls == ["spark", "zachestnyibiznes"]
    assert parse_calls["count"] == 2
    assert [item["candidate_sites"] for item in json.loads((output_dir / "results.json").read_text(encoding="utf-8"))] == [
        ["https://alpha.example", "https://beta.example"]
    ]

    stage_messages = load_stage_messages(output_dir)
    assert [message["message_type"] for message in stage_messages].count("source_result_ready") == 2
    assert [message["message_type"] for message in stage_messages].count("candidate_site_found") == 2
    assert [message["message_type"] for message in stage_messages].count("deep_parse_done") == 1
    assert [message["message_type"] for message in stage_messages].count("company_completed") == 1

    recovered_work_unit = _stage_work_units(output_dir)["aggregator_site"]["companies"]["0000000001"]
    assert recovered_work_unit["work_status"] == "acked"
    assert ProgressStore(output_dir).pending_stage_work_units() == []


def test_resume_recovery_replays_after_persisted_site_gate_without_duplicate_stage_messages(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    rows = _rows(1)
    output_dir = tmp_path / "output"
    validated_sites = [_site_decision()]
    _install_lightweight_run_stubs(
        monkeypatch,
        rows=rows,
        domain_resolution=_domain_resolution_for(
            rows[0],
            primary_url="https://alpha.example",
            secondary_url="https://beta.example",
            primary_confidence=0.88,
            secondary_confidence=0.53,
        ),
        candidate_sites=["https://alpha.example", "https://beta.example"],
        validated_sites=validated_sites,
    )

    search_calls: list[str] = []
    parse_calls = {"count": 0}
    original_emit = pipeline.ProgressStore.emit_stage_message

    class _CountingSource(_StaticSource):
        def search(self, row: core.RowInput) -> core.SourceResult:
            search_calls.append(self.source_name)
            return super().search(row)

    def counting_parse(_company):
        parse_calls["count"] += 1
        return SimpleNamespace(
            plans=[SimpleNamespace(site_url="https://alpha.example/about", allows_deep_check=True)],
            site_probes=[],
            route_strategies=[],
            content_records=[],
            notes=[],
        )

    def crash_before_first_deep_parse_message(self, *args, **kwargs):
        if kwargs.get("message_type") == "deep_parse_done" and parse_calls["count"] == 1:
            raise RuntimeError("synthetic crash before deep_parse_done persist")
        return original_emit(self, *args, **kwargs)

    monkeypatch.setattr(pipeline, "SparkSource", lambda _client: _CountingSource("spark"))
    monkeypatch.setattr(pipeline, "ZachestnyBiznesSource", lambda _client: _CountingSource("zachestnyibiznes"))
    monkeypatch.setattr(
        pipeline,
        "FactorySiteParser",
        lambda *_args, **_kwargs: SimpleNamespace(parse=counting_parse),
    )
    monkeypatch.setattr(pipeline.ProgressStore, "emit_stage_message", crash_before_first_deep_parse_message)
    time_points = iter([100.0, 100.75, 200.0, 200.25])
    monkeypatch.setattr(pipeline, "time", SimpleNamespace(time=lambda: next(time_points)))

    with pytest.raises(RuntimeError, match="synthetic crash before deep_parse_done persist"):
        _run_stage_message_pipeline(output_dir)

    stage_messages_before_resume = load_stage_messages(output_dir)
    assert [message["message_type"] for message in stage_messages_before_resume].count("source_result_ready") == 2
    assert [message["message_type"] for message in stage_messages_before_resume].count("candidate_site_found") == 2
    assert [message["message_type"] for message in stage_messages_before_resume].count("site_gate_decision") == 1
    assert [message["message_type"] for message in stage_messages_before_resume].count("deep_parse_done") == 0
    assert search_calls == ["spark", "zachestnyibiznes"]
    assert parse_calls["count"] == 1
    assert _stage_work_units(output_dir)["aggregator_site"]["companies"]["0000000001"]["execution_boundary"] == pipeline.DEEP_PARSE_EXECUTION_BOUNDARY
    assert _stage_work_units(output_dir)["aggregator_site"]["companies"]["0000000001"]["work_status"] == "pending"

    assert _run_stage_message_pipeline(output_dir, "--resume") == 0

    assert search_calls == ["spark", "zachestnyibiznes"]
    assert parse_calls["count"] == 2
    stage_messages = load_stage_messages(output_dir)
    assert [message["message_type"] for message in stage_messages].count("source_result_ready") == 2
    assert [message["message_type"] for message in stage_messages].count("candidate_site_found") == 2
    assert [message["message_type"] for message in stage_messages].count("site_gate_decision") == 1
    assert [message["message_type"] for message in stage_messages].count("deep_parse_done") == 1
    assert [message["message_type"] for message in stage_messages].count("company_completed") == 1
    assert _stage_work_units(output_dir)["aggregator_site"]["companies"]["0000000001"]["work_status"] == "acked"
    assert ProgressStore(output_dir).pending_stage_work_units() == []


def test_resume_recovery_persists_checkpointed_result_without_rerunning_downstream(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    rows = _rows(1)
    output_dir = tmp_path / "output"
    validated_sites = [_site_decision()]
    _install_lightweight_run_stubs(
        monkeypatch,
        rows=rows,
        domain_resolution=_domain_resolution_for(
            rows[0],
            primary_url="https://alpha.example",
            secondary_url="https://beta.example",
            primary_confidence=0.88,
            secondary_confidence=0.53,
        ),
        candidate_sites=["https://alpha.example", "https://beta.example"],
        validated_sites=validated_sites,
    )

    search_calls: list[str] = []
    parse_calls = {"count": 0}
    persist_calls = {"count": 0}
    original_persist = pipeline.ProgressStore.persist_completed_company_result

    class _CountingSource(_StaticSource):
        def search(self, row: core.RowInput) -> core.SourceResult:
            search_calls.append(self.source_name)
            return super().search(row)

    def counting_parse(_company):
        parse_calls["count"] += 1
        return SimpleNamespace(
            plans=[SimpleNamespace(site_url="https://alpha.example/about", allows_deep_check=True)],
            site_probes=[],
            route_strategies=[],
            content_records=[],
            notes=[],
        )

    def fail_first_persist(self, *args, **kwargs):
        persist_calls["count"] += 1
        if persist_calls["count"] == 1:
            raise RuntimeError("synthetic crash after deep_parse_done persist")
        return original_persist(self, *args, **kwargs)

    monkeypatch.setattr(pipeline, "SparkSource", lambda _client: _CountingSource("spark"))
    monkeypatch.setattr(pipeline, "ZachestnyBiznesSource", lambda _client: _CountingSource("zachestnyibiznes"))
    monkeypatch.setattr(
        pipeline,
        "FactorySiteParser",
        lambda *_args, **_kwargs: SimpleNamespace(parse=counting_parse),
    )
    monkeypatch.setattr(pipeline.ProgressStore, "persist_completed_company_result", fail_first_persist)
    time_points = iter([100.0, 100.75, 200.0, 200.25])
    monkeypatch.setattr(pipeline, "time", SimpleNamespace(time=lambda: next(time_points)))

    with pytest.raises(RuntimeError, match="synthetic crash after deep_parse_done persist"):
        _run_stage_message_pipeline(output_dir)

    stage_messages_before_resume = load_stage_messages(output_dir)
    assert [message["message_type"] for message in stage_messages_before_resume].count("deep_parse_done") == 1
    assert [message["message_type"] for message in stage_messages_before_resume].count("company_completed") == 1
    assert search_calls == ["spark", "zachestnyibiznes"]
    assert parse_calls["count"] == 1
    checkpointed_work_unit = _stage_work_units(output_dir)["aggregator_site"]["companies"]["0000000001"]
    assert checkpointed_work_unit["execution_boundary"] == pipeline.DEEP_PARSE_EXECUTION_BOUNDARY
    assert checkpointed_work_unit["work_status"] == "acked"
    checkpoint_payload = checkpointed_work_unit.get("private_state", {})
    assert isinstance(checkpoint_payload.get("completed_company_result"), dict)
    crashed_summary = json.loads((output_dir / "summary.json").read_text(encoding="utf-8"))
    assert crashed_summary["processed_rows"] == 1
    assert crashed_summary["completed_rows"] == 1
    assert crashed_summary["remaining_rows"] == 0
    crashed_results = json.loads((output_dir / "results.json").read_text(encoding="utf-8"))
    assert [item["inn"] for item in crashed_results] == ["0000000001"]
    persisted_result = ProgressStore(output_dir).get("0000000001")
    assert persisted_result is not None
    assert persisted_result["status"] == "completed"

    assert _run_stage_message_pipeline(output_dir, "--resume") == 0

    assert search_calls == ["spark", "zachestnyibiznes"]
    assert parse_calls["count"] == 1
    stage_messages = load_stage_messages(output_dir)
    assert [message["message_type"] for message in stage_messages].count("deep_parse_done") == 1
    assert [message["message_type"] for message in stage_messages].count("company_completed") == 1
    rematerialized_results = json.loads((output_dir / "results.json").read_text(encoding="utf-8"))
    assert [item["inn"] for item in rematerialized_results] == ["0000000001"]
    assert not (output_dir / "results.jsonl").exists()
    assert _stage_work_units(output_dir)["aggregator_site"]["companies"]["0000000001"]["work_status"] == "acked"
    assert ProgressStore(output_dir).pending_stage_work_units() == []


def test_resume_recovery_emits_company_completed_from_canonical_result_after_crash_before_append_only_result_log(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    rows = _rows(1)
    output_dir = tmp_path / "output"
    validated_sites = [_site_decision()]
    _install_lightweight_run_stubs(
        monkeypatch,
        rows=rows,
        domain_resolution=_domain_resolution_for(
            rows[0],
            primary_url="https://alpha.example",
            secondary_url="https://beta.example",
            primary_confidence=0.88,
            secondary_confidence=0.53,
        ),
        candidate_sites=["https://alpha.example", "https://beta.example"],
        validated_sites=validated_sites,
    )

    search_calls: list[str] = []
    parse_calls = {"count": 0}
    results_jsonl_append_calls = {"count": 0}
    observed_runtime_state: dict[str, object] = {}
    original_append_jsonl = runtime_progress.append_jsonl

    class _CountingSource(_StaticSource):
        def search(self, row: core.RowInput) -> core.SourceResult:
            search_calls.append(self.source_name)
            return super().search(row)

    def counting_parse(_company):
        parse_calls["count"] += 1
        return SimpleNamespace(
            plans=[SimpleNamespace(site_url="https://alpha.example/about", allows_deep_check=True)],
            site_probes=[],
            route_strategies=[],
            content_records=[],
            notes=[],
        )

    def fail_first_results_jsonl_append(path, item):
        if path == output_dir / "results.jsonl":
            results_jsonl_append_calls["count"] += 1
            if results_jsonl_append_calls["count"] == 1:
                observed_runtime_state["payload"] = json.loads((output_dir / "runtime_state.json").read_text(encoding="utf-8"))
                raise RuntimeError("synthetic crash after canonical persist before append-only result log")
        return original_append_jsonl(path, item)

    monkeypatch.setattr(pipeline, "SparkSource", lambda _client: _CountingSource("spark"))
    monkeypatch.setattr(pipeline, "ZachestnyBiznesSource", lambda _client: _CountingSource("zachestnyibiznes"))
    monkeypatch.setattr(
        pipeline,
        "FactorySiteParser",
        lambda *_args, **_kwargs: SimpleNamespace(parse=counting_parse),
    )
    monkeypatch.setattr(runtime_progress, "append_jsonl", fail_first_results_jsonl_append)
    time_points = iter([100.0, 100.75, 200.0, 200.25])
    monkeypatch.setattr(pipeline, "time", SimpleNamespace(time=lambda: next(time_points)))

    with pytest.raises(RuntimeError, match="synthetic crash after canonical persist before append-only result log"):
        _run_stage_message_pipeline(output_dir)

    stage_messages_before_resume = load_stage_messages(output_dir)
    assert [message["message_type"] for message in stage_messages_before_resume].count("deep_parse_done") == 1
    assert [message["message_type"] for message in stage_messages_before_resume].count("company_completed") == 1
    assert search_calls == ["spark", "zachestnyibiznes"]
    assert parse_calls["count"] == 1
    assert results_jsonl_append_calls["count"] == 1
    checkpointed_runtime_state = observed_runtime_state["payload"]
    checkpointed_deep_parse_work_unit = checkpointed_runtime_state["run"]["metadata"]["stage_work_units"]["deep_parse"][
        "companies"
    ]["0000000001"]
    assert checkpointed_deep_parse_work_unit["execution_boundary"] == pipeline.DEEP_PARSE_EXECUTION_BOUNDARY
    assert checkpointed_deep_parse_work_unit["work_status"] == "acked"
    assert "completed_company_result" not in checkpointed_deep_parse_work_unit.get("private_state", {})
    checkpointed_aggregator_work_unit = checkpointed_runtime_state["run"]["metadata"]["stage_work_units"][
        "aggregator_site"
    ]["companies"]["0000000001"]
    assert checkpointed_aggregator_work_unit["execution_boundary"] == pipeline.AGGREGATOR_SITE_EXECUTION_BOUNDARY
    assert checkpointed_aggregator_work_unit["work_status"] == "acked"
    checkpointed_evidence = checkpointed_runtime_state["run"]["metadata"]["stage_execution_evidence"]["deep_parse"][
        "companies"
    ]["0000000001"]
    assert "completed_company_result" not in checkpointed_evidence.get("private_state", {})
    crashed_results = json.loads((output_dir / "results.json").read_text(encoding="utf-8"))
    assert [item["inn"] for item in crashed_results] == ["0000000001"]
    crashed_summary = json.loads((output_dir / "summary.json").read_text(encoding="utf-8"))
    assert crashed_summary["processed_rows"] == 1
    assert crashed_summary["completed_rows"] == 1
    assert crashed_summary["remaining_rows"] == 0
    assert not (output_dir / "results.jsonl").exists()

    persisted_result = ProgressStore(output_dir).get("0000000001")
    assert persisted_result is not None
    assert persisted_result["status"] == "completed"
    assert [item["inn"] for item in json.loads((output_dir / "results.json").read_text(encoding="utf-8"))] == [
        "0000000001"
    ]
    assert not (output_dir / "results.jsonl").exists()

    assert _run_stage_message_pipeline(output_dir, "--resume") == 0

    assert search_calls == ["spark", "zachestnyibiznes"]
    assert parse_calls["count"] == 1
    stage_messages = load_stage_messages(output_dir)
    assert [message["message_type"] for message in stage_messages].count("deep_parse_done") == 1
    assert [message["message_type"] for message in stage_messages].count("company_completed") == 1
    assert [item["inn"] for item in json.loads((output_dir / "results.json").read_text(encoding="utf-8"))] == [
        "0000000001"
    ]
    assert not (output_dir / "results.jsonl").exists()
    assert _stage_work_units(output_dir)["aggregator_site"]["companies"]["0000000001"]["work_status"] == "acked"
    assert ProgressStore(output_dir).pending_stage_work_units() == []


def test_resume_recovery_rebuilds_public_outputs_after_live_publish_crash_without_rerunning_sources(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    rows = _rows(1)
    output_dir = tmp_path / "output"
    validated_sites = [_site_decision()]
    _install_lightweight_run_stubs(
        monkeypatch,
        rows=rows,
        domain_resolution=_domain_resolution_for(
            rows[0],
            primary_url="https://alpha.example",
            secondary_url="https://beta.example",
            primary_confidence=0.88,
            secondary_confidence=0.53,
        ),
        candidate_sites=["https://alpha.example", "https://beta.example"],
        validated_sites=validated_sites,
        lead_cards=[{"title": "Lead 1"}],
    )

    search_calls: list[str] = []
    parse_calls = {"count": 0}
    publish_calls = {"count": 0}
    observed_public_snapshot: dict[str, object] = {}
    original_publish = runtime_progress.ProgressStore._publish_public_output_file

    class _CountingSource(_StaticSource):
        def search(self, row: core.RowInput) -> core.SourceResult:
            search_calls.append(self.source_name)
            return super().search(row)

    def counting_parse(_company):
        parse_calls["count"] += 1
        return SimpleNamespace(
            plans=[SimpleNamespace(site_url="https://alpha.example/about", allows_deep_check=True)],
            site_probes=[],
            route_strategies=[],
            content_records=[],
            notes=[],
        )

    def fail_before_second_public_publish(self, staged_path, final_path):
        publish_calls["count"] += 1
        if publish_calls["count"] == 2:
            observed_public_snapshot["publish_count_at_crash"] = publish_calls["count"]
            observed_public_snapshot["runtime_state"] = json.loads(
                (output_dir / "runtime_state.json").read_text(encoding="utf-8")
            )
            observed_public_snapshot["public_publish_state"] = _public_publish_state(output_dir)
            observed_public_snapshot["results"] = json.loads(
                (output_dir / "results.json").read_text(encoding="utf-8")
            )
            observed_public_snapshot["leads_exists"] = (output_dir / "leads.json").exists()
            observed_public_snapshot["summary_exists"] = (output_dir / "summary.json").exists()
            if observed_public_snapshot["summary_exists"]:
                observed_public_snapshot["summary"] = json.loads(
                    (output_dir / "summary.json").read_text(encoding="utf-8")
                )
            raise RuntimeError("synthetic crash during live public publish")
        return original_publish(self, staged_path, final_path)

    monkeypatch.setattr(pipeline, "SparkSource", lambda _client: _CountingSource("spark"))
    monkeypatch.setattr(pipeline, "ZachestnyBiznesSource", lambda _client: _CountingSource("zachestnyibiznes"))
    monkeypatch.setattr(
        pipeline,
        "FactorySiteParser",
        lambda *_args, **_kwargs: SimpleNamespace(parse=counting_parse),
    )
    monkeypatch.setattr(
        runtime_progress.ProgressStore,
        "_publish_public_output_file",
        fail_before_second_public_publish,
    )
    time_points = iter([100.0, 100.75, 200.0, 200.25])
    monkeypatch.setattr(pipeline, "time", SimpleNamespace(time=lambda: next(time_points)))

    with pytest.raises(RuntimeError, match="synthetic crash during live public publish"):
        _run_stage_message_pipeline(output_dir)

    stage_messages_before_resume = load_stage_messages(output_dir)
    assert [message["message_type"] for message in stage_messages_before_resume].count("deep_parse_done") == 1
    assert [message["message_type"] for message in stage_messages_before_resume].count("company_completed") == 1
    assert search_calls == ["spark", "zachestnyibiznes"]
    assert parse_calls["count"] == 1
    assert observed_public_snapshot["publish_count_at_crash"] == 2
    assert publish_calls["count"] >= 2
    crashed_runtime_state = observed_public_snapshot["runtime_state"]
    assert isinstance(crashed_runtime_state, dict)
    assert crashed_runtime_state["run"]["summary"]["processed_rows"] == 1
    assert crashed_runtime_state["run"]["summary"]["completed_rows"] == 1
    assert crashed_runtime_state["run"]["summary"]["remaining_rows"] == 0
    crashed_publish_state = observed_public_snapshot["public_publish_state"]
    assert isinstance(crashed_publish_state, dict)
    assert crashed_publish_state["active_generation_id"] != ""
    assert crashed_publish_state["active_generation_id"] != crashed_publish_state["committed_generation_id"]
    crashed_results = observed_public_snapshot["results"]
    assert isinstance(crashed_results, list)
    assert [item["inn"] for item in crashed_results] == ["0000000001"]
    assert observed_public_snapshot["leads_exists"] is False
    assert observed_public_snapshot["summary_exists"] is True
    crashed_summary = observed_public_snapshot["summary"]
    assert isinstance(crashed_summary, dict)
    assert crashed_summary["processed_rows"] == 0
    assert crashed_summary["completed_rows"] == 0
    assert crashed_summary["remaining_rows"] == 1
    assert not (output_dir / "results.jsonl").exists()

    persisted_result = ProgressStore(output_dir).get("0000000001")
    assert persisted_result is not None
    assert persisted_result["status"] == "completed"
    repaired_results = json.loads((output_dir / "results.json").read_text(encoding="utf-8"))
    assert [item["inn"] for item in repaired_results] == ["0000000001"]
    repaired_leads = json.loads((output_dir / "leads.json").read_text(encoding="utf-8"))
    assert len(repaired_leads) == 1
    assert repaired_leads[0]["title"] == "Lead 1"
    repaired_summary = json.loads((output_dir / "summary.json").read_text(encoding="utf-8"))
    assert repaired_summary["processed_rows"] == 1
    assert repaired_summary["completed_rows"] == 1
    assert repaired_summary["remaining_rows"] == 0
    assert repaired_summary["run_status"] == "aborted"
    assert repaired_summary["finish_reason"] == "aborted"
    assert repaired_summary["public_output_contract"]["terminal_run"] is True
    assert repaired_summary["public_output_contract"]["public_result_count"] == 1
    assert repaired_summary["public_output_contract"]["final_exports"] == {
        "state": "terminal_partial",
        "available": True,
        "row_count": 1,
        "paths": ["final_results.csv", "final_results.xlsx"],
    }
    repaired_publish_state = _public_publish_state(output_dir)
    assert repaired_publish_state["active_generation_id"] != ""
    assert repaired_publish_state["active_generation_id"] == repaired_publish_state[
        "committed_generation_id"
    ]
    assert not (output_dir / "results.jsonl").exists()

    assert _run_stage_message_pipeline(output_dir, "--resume") == 0

    assert search_calls == ["spark", "zachestnyibiznes"]
    assert parse_calls["count"] == 1
    stage_messages = load_stage_messages(output_dir)
    assert [message["message_type"] for message in stage_messages].count("deep_parse_done") == 1
    assert [message["message_type"] for message in stage_messages].count("company_completed") == 1
    assert [item["inn"] for item in json.loads((output_dir / "results.json").read_text(encoding="utf-8"))] == [
        "0000000001"
    ]
    resumed_leads = json.loads((output_dir / "leads.json").read_text(encoding="utf-8"))
    assert len(resumed_leads) == 1
    assert resumed_leads[0]["title"] == "Lead 1"
    resumed_summary = json.loads((output_dir / "summary.json").read_text(encoding="utf-8"))
    assert resumed_summary["run_status"] == "completed"
    assert resumed_summary["finish_reason"] == "normal_completion"
    assert resumed_summary["public_output_contract"]["terminal_run"] is True
    assert resumed_summary["public_output_contract"]["final_exports"] == {
        "state": "terminal_completed",
        "available": True,
        "row_count": 1,
        "paths": ["final_results.csv", "final_results.xlsx"],
    }
    assert not (output_dir / "results.jsonl").exists()
    assert _stage_work_units(output_dir)["aggregator_site"]["companies"]["0000000001"]["work_status"] == "acked"
    assert ProgressStore(output_dir).pending_stage_work_units() == []


def test_resume_recovery_acks_persisted_explicit_work_unit_without_rerunning_downstream(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    rows = _rows(1)
    output_dir = tmp_path / "output"
    validated_sites = [_site_decision()]
    _install_lightweight_run_stubs(
        monkeypatch,
        rows=rows,
        domain_resolution=_domain_resolution_for(
            rows[0],
            primary_url="https://alpha.example",
            secondary_url="https://beta.example",
            primary_confidence=0.88,
            secondary_confidence=0.53,
        ),
        candidate_sites=["https://alpha.example", "https://beta.example"],
        validated_sites=validated_sites,
    )

    search_calls: list[str] = []
    parse_calls = {"count": 0}
    ack_attempts = {"count": 0}

    class _CountingSource(_StaticSource):
        def search(self, row: core.RowInput) -> core.SourceResult:
            search_calls.append(self.source_name)
            return super().search(row)

    def counting_parse(_company):
        parse_calls["count"] += 1
        return SimpleNamespace(
            plans=[SimpleNamespace(site_url="https://alpha.example/about", allows_deep_check=True)],
            site_probes=[],
            route_strategies=[],
            content_records=[],
            notes=[],
        )

    original_ack = pipeline.ProgressStore.ack_stage_handoff_work_unit

    def fail_first_ack(self, *args, **kwargs):
        ack_attempts["count"] += 1
        if ack_attempts["count"] == 2:
            return False
        return original_ack(self, *args, **kwargs)

    monkeypatch.setattr(pipeline, "SparkSource", lambda _client: _CountingSource("spark"))
    monkeypatch.setattr(pipeline, "ZachestnyBiznesSource", lambda _client: _CountingSource("zachestnyibiznes"))
    monkeypatch.setattr(
        pipeline,
        "FactorySiteParser",
        lambda *_args, **_kwargs: SimpleNamespace(parse=counting_parse),
    )
    monkeypatch.setattr(pipeline.ProgressStore, "ack_stage_handoff_work_unit", fail_first_ack)
    time_points = iter([100.0, 100.75, 200.0, 200.25])
    monkeypatch.setattr(pipeline, "time", SimpleNamespace(time=lambda: next(time_points)))

    with pytest.raises(RuntimeError, match="Failed to ack deep_parse work unit"):
        _run_stage_message_pipeline(output_dir)

    stage_messages_before_resume = load_stage_messages(output_dir)
    assert search_calls == ["spark", "zachestnyibiznes"]
    assert parse_calls["count"] == 1
    assert _stage_work_units(output_dir)["aggregator_site"]["companies"]["0000000001"]["execution_boundary"] == pipeline.DEEP_PARSE_EXECUTION_BOUNDARY
    assert _stage_work_units(output_dir)["aggregator_site"]["companies"]["0000000001"]["work_status"] == "pending"

    assert _run_stage_message_pipeline(output_dir, "--resume") == 0

    assert search_calls == ["spark", "zachestnyibiznes"]
    assert parse_calls["count"] == 1
    assert load_stage_messages(output_dir) == stage_messages_before_resume
    assert _stage_work_units(output_dir)["aggregator_site"]["companies"]["0000000001"]["work_status"] == "acked"
    assert ProgressStore(output_dir).pending_stage_work_units() == []


def test_resume_skip_path_does_not_emit_new_host_event_messages(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    rows = _rows(1)
    _install_lightweight_run_stubs(
        monkeypatch,
        rows=rows,
        domain_resolution=_domain_resolution_for(
            rows[0],
            primary_url="https://alpha.example",
            secondary_url="https://beta.example",
            primary_confidence=0.88,
            secondary_confidence=0.53,
        ),
        candidate_sites=["https://alpha.example", "https://beta.example"],
        validated_sites=[_site_decision()],
    )
    monkeypatch.setattr(
        pipeline,
        "SparkSource",
        lambda client: _EventWritingSource("spark", client.progress_store, _host_runtime_event),
    )
    output_dir = tmp_path / "output"

    assert _run_stage_message_pipeline(output_dir) == 0
    stage_messages_before_resume = load_stage_messages(output_dir)
    assert [message["message_type"] for message in stage_messages_before_resume].count("host_event") == 1

    assert _run_stage_message_pipeline(output_dir, "--resume") == 0

    assert load_stage_messages(output_dir) == stage_messages_before_resume


def test_progress_store_reload_does_not_backfill_missing_stage_message_outbox(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    rows = _rows(1)
    _install_lightweight_run_stubs(
        monkeypatch,
        rows=rows,
        domain_resolution=_domain_resolution_for(
            rows[0],
            primary_url="https://alpha.example",
            secondary_url="https://beta.example",
            primary_confidence=0.88,
            secondary_confidence=0.53,
        ),
        candidate_sites=["https://alpha.example", "https://beta.example"],
        validated_sites=[_site_decision()],
    )
    monkeypatch.setattr(
        pipeline,
        "SparkSource",
        lambda client: _EventWritingSource("spark", client.progress_store, _host_runtime_event),
    )
    output_dir = tmp_path / "output"

    assert _run_stage_message_pipeline(output_dir) == 0
    ProgressStore(output_dir).consume_unread_stage_messages()
    cursor_before_missing_outbox = _stage_outbox_cursor(output_dir)
    outbox_path = stage_message_outbox_path(output_dir)
    assert outbox_path.exists()
    outbox_path.unlink()

    reloaded_progress = ProgressStore(output_dir)

    assert not outbox_path.exists()
    assert load_stage_messages(output_dir) == []
    assert reloaded_progress.consume_unread_stage_messages() == []
    assert _stage_outbox_cursor(output_dir) == cursor_before_missing_outbox

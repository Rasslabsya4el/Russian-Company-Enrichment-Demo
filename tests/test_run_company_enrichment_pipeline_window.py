from __future__ import annotations

import json
from concurrent.futures import Future
from threading import Event, Lock
from types import SimpleNamespace

import pytest

import company_enrichment_core as core
import run_company_enrichment_pipeline as pipeline
from app.runtime.bounded_executor import DirectDefaultBoundedExecutorPlan, open_company_source_search_executor
from app.runtime.host_governor import HostGovernorLedger
from app.runtime.required_source_deferred import (
    OUTCOME_DEFERRED_ROW_TRANSIENT,
    OUTCOME_NON_DEFER_FAIL_FAST,
    OUTCOME_SYSTEMIC_STOP,
    build_required_source_deferred_record,
    classify_required_source_outcome,
    mark_required_source_success,
    record_required_source_deferred,
    required_source_deferred_state,
)
from app.runtime.row_selection import resolve_row_selection
from app.site_intelligence.site_authenticity import SiteDecision


class _StaticSource:
    def __init__(self, source_name: str) -> None:
        self.source_name = source_name

    def search(self, row: core.RowInput) -> core.SourceResult:
        return core.SourceResult(source=self.source_name, status="ok")


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
    processed_companies: list[str],
    materialize_reports: bool = False,
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

    def fake_store_dossier(*, result: core.CompanyResult, output_dir) -> dict[str, object]:
        processed_companies.append(result.company_name)
        return {}

    monkeypatch.setattr(pipeline.core, "load_env_file", lambda _path: None)
    monkeypatch.setattr(pipeline.core, "load_rows_from_xlsx", lambda _path: rows)
    monkeypatch.setattr(pipeline.core, "RateLimitedHttpClient", fake_rate_limited_http_client)
    if not materialize_reports:
        monkeypatch.setattr(core.ProgressStore, "_write_markdown_reports", lambda self, ordered_results=None: None)
    monkeypatch.setattr(pipeline, "ProxyPool", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(pipeline, "SparkSource", lambda _client: _StaticSource("spark"))
    monkeypatch.setattr(pipeline, "ZachestnyBiznesSource", lambda _client: _StaticSource("zachestnyibiznes"))
    monkeypatch.setattr(pipeline, "RusprofileSource", lambda _client: _StaticSource("rusprofile"))
    monkeypatch.setattr(pipeline, "CheckoSource", lambda _client: _StaticSource("checko"))
    monkeypatch.setattr(pipeline, "ListOrgSource", lambda _client, _snapshot: _StaticSource("list_org"))
    monkeypatch.setattr(pipeline, "FactorySiteParser", lambda *_args, **_kwargs: SimpleNamespace())
    monkeypatch.setattr(pipeline, "SiteAuthHelpers", lambda **_kwargs: SimpleNamespace())
    monkeypatch.setattr(pipeline, "SiteAuthenticityAnalyzer", lambda *_args, **_kwargs: SimpleNamespace(llm=SimpleNamespace()))
    monkeypatch.setattr(
        pipeline,
        "BenchmarkAwareSiteAuthenticityAnalyzer",
        lambda *_args, **_kwargs: SimpleNamespace(llm=SimpleNamespace()),
    )
    monkeypatch.setattr(pipeline, "build_domain_resolution", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(pipeline, "choose_candidate_sites", lambda *_args, **_kwargs: [])
    monkeypatch.setattr(pipeline, "run_gated_factory_site_parse", fake_gated_parse, raising=False)
    monkeypatch.setattr(pipeline, "classify_content_record", lambda _record: None)
    monkeypatch.setattr(pipeline, "should_use_llm_record_review", lambda _record: False)
    monkeypatch.setattr(pipeline, "build_and_store_company_dossier", fake_store_dossier)
    monkeypatch.setattr(pipeline.core, "build_analysis_contacts", lambda *_args, **_kwargs: {})
    monkeypatch.setattr(pipeline.core, "merge_contacts", lambda *_args, **_kwargs: {})
    monkeypatch.setattr(pipeline.core, "build_trusted_contacts", lambda *_args, **_kwargs: {})
    monkeypatch.setattr(pipeline.core, "build_lead_cards", lambda *_args, **_kwargs: [])
    monkeypatch.setattr(pipeline.core, "build_site_refresh_plans", lambda *_args, **_kwargs: [])


def test_source_budget_pressure_payload_classifies_prefetch_wait_as_client_pressure() -> None:
    payload = pipeline._source_budget_pressure_payload(
        source_name="spark",
        duration_seconds=23.98,
        budget_seconds=12.0,
        runtime_events=[
            {
                "type": "request_ok",
                "source": "spark",
                "elapsed_seconds": 0.643,
                "since_previous_request_seconds": None,
            },
            {
                "type": "request_ok",
                "source": "spark",
                "elapsed_seconds": 0.721,
                "since_previous_request_seconds": 3.264,
            },
            {
                "type": "request_ok",
                "source": "checko",
                "elapsed_seconds": 1.0,
            },
        ],
    )

    assert payload["budget_pressure_class"] == "client_or_scheduler_wait_over_budget"
    assert payload["request_event_count"] == 2
    assert payload["request_elapsed_seconds"] == 1.364
    assert payload["non_request_wait_seconds"] == 22.616
    assert payload["max_since_previous_request_seconds"] == 3.264


def test_source_budget_pressure_payload_classifies_host_min_delay_wait() -> None:
    payload = pipeline._source_budget_pressure_payload(
        source_name="checko",
        duration_seconds=22.0,
        budget_seconds=12.0,
        runtime_events=[
            {
                "type": "host_min_delay_wait",
                "source": "checko",
                "wait_seconds": 19.25,
                "host_delay_lock_wait_seconds": 2.0,
                "total_wait_seconds": 21.25,
                "min_delay_seconds": 5.0,
                "since_previous_request_seconds": 0.5,
            },
            {
                "type": "request_ok",
                "source": "checko",
                "elapsed_seconds": 0.75,
                "since_previous_request_seconds": 0.5,
            },
        ],
    )

    assert payload["budget_pressure_class"] == "host_min_delay_or_policy_wait"
    assert payload["request_event_count"] == 1
    assert payload["policy_wait_event_count"] == 1
    assert payload["host_min_delay_wait_seconds"] == 21.25
    assert payload["max_host_min_delay_wait_seconds"] == 21.25
    assert payload["host_delay_lock_wait_seconds"] == 2.0
    assert payload["max_host_delay_lock_wait_seconds"] == 2.0
    assert payload["request_elapsed_seconds"] == 0.75
    assert payload["non_request_wait_seconds"] == 21.25


def test_run_does_not_publish_no_candidate_note_into_public_outputs(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    processed_companies: list[str] = []
    _install_lightweight_run_stubs(
        monkeypatch,
        rows=_rows(1),
        processed_companies=processed_companies,
        materialize_reports=True,
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
                "--company-concurrency",
                "2",
            ]
        )
    )

    assert exit_code == 0
    assert processed_companies == ["Company 1"]

    results = json.loads((output_dir / "results.json").read_text(encoding="utf-8"))
    results_log = [
        json.loads(line)
        for line in (output_dir / "results.jsonl").read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    company_report = next((output_dir / "company_reports").glob("*.md")).read_text(encoding="utf-8")

    assert [item["notes"] for item in results] == [[]]
    assert [item["notes"] for item in results_log] == [[]]
    assert pipeline.NO_CANDIDATE_SITE_NOTE not in company_report
    assert "Ð" not in json.dumps(results, ensure_ascii=False)
    assert "Ð" not in json.dumps(results_log, ensure_ascii=False)
    assert "Ð" not in company_report


def test_parse_args_parses_ordinals_and_dedupes_in_input_order() -> None:
    args = pipeline.parse_args(["--input", "input.xlsx", "--ordinals=31,38,31,52,38"])

    assert args.ordinals == [31, 38, 52]


def test_parse_args_resolves_module_concurrency_knobs() -> None:
    args = pipeline.parse_args(
        [
            "--input",
            "input.xlsx",
            "--company-concurrency",
            "8",
            "--source-concurrency",
            "2",
            "--candidate-site-concurrency",
            "7",
            "--deep-parse-concurrency",
            "6",
            "--factory-site-concurrency",
            "5",
            "--ocr-concurrency",
            "4",
            "--llm-concurrency",
            "3",
            "--extra-check-concurrency",
            "2",
        ]
    )

    assert args.company_concurrency == 8
    assert args.source_concurrency == 2
    assert args.candidate_site_concurrency == 7
    assert args.deep_parse_concurrency == 6
    assert args.factory_site_concurrency == 5
    assert args.ocr_concurrency == 4
    assert args.llm_concurrency == 3
    assert args.extra_check_concurrency == 2


def test_parse_args_module_concurrency_defaults_from_legacy_company_cap() -> None:
    args = pipeline.parse_args(["--input", "input.xlsx", "--company-concurrency", "5"])

    assert args.source_concurrency == 5
    assert args.candidate_site_concurrency == 5
    assert args.deep_parse_concurrency == 5
    assert args.factory_site_concurrency == 5
    assert args.ocr_concurrency == 1
    assert args.llm_concurrency == 1
    assert args.extra_check_concurrency == 1


def test_parse_args_rejects_invalid_module_concurrency(capsys: pytest.CaptureFixture[str]) -> None:
    with pytest.raises(SystemExit):
        pipeline.parse_args(["--input", "input.xlsx", "--source-concurrency", "0"])

    assert "--source-concurrency must be >= 1" in capsys.readouterr().err


def test_runtime_backpressure_policy_reports_enabled_downstream_prefetch_worker_count() -> None:
    policy = pipeline._build_runtime_backpressure_policy(
        active_source_names=["spark", "zachestnyibiznes", "rusprofile"],
        source_lane_scheduler={
            "effective_company_concurrency_cap": 2,
            "operator_concurrency_policy": {"source_concurrency": 2},
        },
        downstream_worker_pools={"per_stage_budget_map": {pipeline.FACTORY_SITE_STAGE_NAME: 10}},
        direct_default_executor_plan=DirectDefaultBoundedExecutorPlan(
            enabled=True,
            max_workers=2,
            active_sources=("spark", "zachestnyibiznes"),
        ),
        source_pending_rows=3,
        downstream_prefetch_queue_limit=8,
        downstream_prefetch_ready_drain_limit=4,
        downstream_prefetch_worker_count=10,
    )

    downstream_prefetch_policy = policy["downstream_prefetch"]
    assert downstream_prefetch_policy["enabled"] is True
    assert downstream_prefetch_policy["worker_count"] == 10
    assert downstream_prefetch_policy["pending_queue_limit"] == 8
    assert policy["direct_default_prefetch"]["ready_queue_limit"] == 3
    assert policy["source_prefetch"]["ready_queue_limit"] == 3


def test_runtime_backpressure_policy_reports_zero_workers_when_downstream_prefetch_disabled() -> None:
    policy = pipeline._build_runtime_backpressure_policy(
        active_source_names=["spark", "zachestnyibiznes", "rusprofile"],
        source_lane_scheduler={"effective_company_concurrency_cap": 2},
        downstream_worker_pools={"per_stage_budget_map": {pipeline.FACTORY_SITE_STAGE_NAME: 10}},
        direct_default_executor_plan=DirectDefaultBoundedExecutorPlan(
            enabled=True,
            max_workers=2,
            active_sources=("spark", "zachestnyibiznes"),
        ),
        source_pending_rows=3,
        downstream_prefetch_queue_limit=0,
        downstream_prefetch_ready_drain_limit=4,
        downstream_prefetch_worker_count=10,
    )

    downstream_prefetch_policy = policy["downstream_prefetch"]
    assert downstream_prefetch_policy["enabled"] is False
    assert downstream_prefetch_policy["worker_count"] == 0
    assert downstream_prefetch_policy["pending_queue_limit"] == 0


def test_source_prefetch_ready_queue_limit_scales_to_selected_surface() -> None:
    assert pipeline._resolve_source_prefetch_ready_queue_limit(
        2,
        source_pending_rows=3,
    ) == 3
    assert pipeline._resolve_source_prefetch_ready_queue_limit(
        2,
        source_pending_rows=100,
    ) == 100
    assert pipeline._resolve_source_prefetch_ready_queue_limit(
        2,
        source_pending_rows=865,
    ) == pipeline.SOURCE_PREFETCH_SELECTED_SURFACE_QUEUE_MAX


def test_prefetched_aggregator_site_execution_skips_deep_parse_for_configured_out_of_geo(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("PARSER_ALLOWED_GEO_BUCKETS", "core,outer_band")
    monkeypatch.setattr(
        pipeline.core,
        "lookup_settlement",
        lambda _address: SimpleNamespace(
            match_status="matched",
            source_address="Архангельская область, Вельск",
            matched_settlement="Вельск",
            matched_municipality="Вельский муниципальный район",
            matched_region="Архангельская область",
            geo_bucket="outside",
            geo_weight=0,
            inside_outer_polygon=False,
            inside_inner_polygon=False,
            distance_to_moscow_km=690.0,
            candidate_count=1,
            variant_count=1,
            distance_spread_km=0.0,
            ambiguous_geo_buckets=(),
        ),
    )
    monkeypatch.setattr(pipeline, "choose_candidate_sites", lambda *_args, **_kwargs: ["https://factory.example"])
    monkeypatch.setattr(pipeline, "build_domain_resolution", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(
        pipeline,
        "gate_candidate_sites_before_deep_parse",
        lambda **_kwargs: SimpleNamespace(
            deep_parse_sites=["https://factory.example"],
            surface_only_decisions=[SiteDecision(url="https://factory.example", final_url="https://factory.example")],
            trusted_surface_decisions_by_site={},
            notes=[],
        ),
    )
    source_results = {
        "spark": core.SourceResult(
            source="spark",
            status="ok",
            addresses=[core.ContactItem(value="Архангельская область, Вельск", source_url="", kind="address")],
        )
    }

    execution = pipeline._prepare_prefetched_aggregator_site_execution(
        row=core.RowInput(row_index=1, inn="1234567890", company_name="Out Geo Factory"),
        source_results=source_results,
        analyzer=SimpleNamespace(),
    )

    assert execution.deep_parse_sites == ()
    assert any("geo_out_of_scope" in note for note in execution.gate_notes)


def test_prefetched_aggregator_site_execution_preserves_deep_parse_without_allowed_geo(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("PARSER_ALLOWED_GEO_BUCKETS", raising=False)
    monkeypatch.delenv("PARSER_ALLOWED_GEO", raising=False)
    monkeypatch.setattr(pipeline, "choose_candidate_sites", lambda *_args, **_kwargs: ["https://factory.example"])
    monkeypatch.setattr(pipeline, "build_domain_resolution", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(
        pipeline,
        "gate_candidate_sites_before_deep_parse",
        lambda **_kwargs: SimpleNamespace(
            deep_parse_sites=["https://factory.example"],
            surface_only_decisions=[],
            trusted_surface_decisions_by_site={},
            notes=[],
        ),
    )

    execution = pipeline._prepare_prefetched_aggregator_site_execution(
        row=core.RowInput(row_index=1, inn="1234567890", company_name="Unknown Geo Factory"),
        source_results={},
        analyzer=SimpleNamespace(),
    )

    assert execution.deep_parse_sites == ("https://factory.example",)
    assert not any("geo_out_of_scope" in note for note in execution.gate_notes)


def test_prefetched_aggregator_site_execution_preserves_deep_parse_for_ambiguous_geo(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("PARSER_ALLOWED_GEO_BUCKETS", "core,outer_band")
    monkeypatch.setattr(
        pipeline.core,
        "lookup_settlement",
        lambda _address: SimpleNamespace(
            match_status="ambiguous",
            source_address="поселок Центральный",
            matched_settlement="",
            matched_municipality="",
            matched_region="",
            geo_bucket="outside",
            geo_weight=None,
            inside_outer_polygon=None,
            inside_inner_polygon=None,
            distance_to_moscow_km=None,
            candidate_count=2,
            variant_count=2,
            distance_spread_km=500.0,
            ambiguous_geo_buckets=("core", "outside"),
        ),
    )
    monkeypatch.setattr(pipeline, "choose_candidate_sites", lambda *_args, **_kwargs: ["https://factory.example"])
    monkeypatch.setattr(pipeline, "build_domain_resolution", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(
        pipeline,
        "gate_candidate_sites_before_deep_parse",
        lambda **_kwargs: SimpleNamespace(
            deep_parse_sites=["https://factory.example"],
            surface_only_decisions=[],
            trusted_surface_decisions_by_site={},
            notes=[],
        ),
    )
    source_results = {
        "spark": core.SourceResult(
            source="spark",
            status="ok",
            addresses=[core.ContactItem(value="поселок Центральный", source_url="", kind="address")],
        )
    }

    execution = pipeline._prepare_prefetched_aggregator_site_execution(
        row=core.RowInput(row_index=1, inn="1234567890", company_name="Ambiguous Geo Factory"),
        source_results=source_results,
        analyzer=SimpleNamespace(),
    )

    assert execution.deep_parse_sites == ("https://factory.example",)
    assert not any("geo_out_of_scope" in note for note in execution.gate_notes)


def test_parse_args_rejects_empty_ordinals(capsys: pytest.CaptureFixture[str]) -> None:
    with pytest.raises(SystemExit):
        pipeline.parse_args(["--input", "input.xlsx", "--ordinals="])

    assert "--ordinals must contain at least one ordinal" in capsys.readouterr().err


def test_parse_args_rejects_non_positive_ordinals(capsys: pytest.CaptureFixture[str]) -> None:
    with pytest.raises(SystemExit):
        pipeline.parse_args(["--input", "input.xlsx", "--ordinals=3,0,5"])

    assert "--ordinals must be >= 1" in capsys.readouterr().err


def test_parse_args_rejects_conflicting_selection_modes(capsys: pytest.CaptureFixture[str]) -> None:
    with pytest.raises(SystemExit):
        pipeline.parse_args(["--input", "input.xlsx", "--ordinals=3,4", "--start-from", "2", "--count", "5"])

    assert "--ordinals conflicts with --start-from/--count/--limit" in capsys.readouterr().err


def test_resolve_row_selection_returns_exact_sparse_rows_in_requested_order() -> None:
    selection = resolve_row_selection(_rows(10), start_from=1, count=None, ordinals=[5, 2, 5, 8])

    assert selection.mode == "ordinals"
    assert selection.selected_ordinals == [5, 2, 8]
    assert [row.company_name for row in selection.rows] == ["Company 5", "Company 2", "Company 8"]


def test_run_records_sparse_selection_in_artifacts_and_processes_only_requested_rows(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    processed_companies: list[str] = []
    _install_lightweight_run_stubs(monkeypatch, rows=_rows(6), processed_companies=processed_companies)
    output_dir = tmp_path / "output"

    exit_code = pipeline.run(
        pipeline.parse_args(
            [
                "--input",
                "input.xlsx",
                "--output-dir",
                str(output_dir),
                "--sources=spark",
                "--ordinals=4,2,4,5",
            ]
        )
    )

    assert exit_code == 0
    assert processed_companies == ["Company 4", "Company 2", "Company 5"]

    summary = json.loads((output_dir / "summary.json").read_text(encoding="utf-8"))
    assert summary["selection_mode"] == "ordinals"
    assert summary["selected_ordinals"] == [4, 2, 5]
    assert summary["rows_selected"] == 3
    assert summary["processed_rows"] == 3
    assert summary["completed_rows"] == 3
    assert summary["remaining_rows"] == 0
    assert summary["resume_skipped_rows"] == 0
    runtime_state = json.loads((output_dir / "runtime_state.json").read_text(encoding="utf-8"))
    assert runtime_state["runtime_state_contract_version"] == 2
    assert runtime_state["run"]["summary"]["selected_ordinals"] == [4, 2, 5]
    assert runtime_state["run"]["metadata"]["selected_ordinals"] == [4, 2, 5]
    assert {item["runtime"]["status"] for item in runtime_state["company_entries"]} == {"completed"}
    assert all("status" not in item["result"] for item in runtime_state["company_entries"])
    assert all("started_at" not in item["result"] for item in runtime_state["company_entries"])
    assert all("finished_at" not in item["result"] for item in runtime_state["company_entries"])

    results_log = [
        json.loads(line)
        for line in (output_dir / "results.jsonl").read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    assert [item["company_name"] for item in results_log] == ["Company 4", "Company 2", "Company 5"]

    results = json.loads((output_dir / "results.json").read_text(encoding="utf-8"))
    assert len(results) == 3
    assert [item["company_name"] for item in results] == ["Company 2", "Company 4", "Company 5"]
    assert {item["status"] for item in results} == {"completed"}
    assert {
        item["result"]["company_name"] for item in runtime_state["company_entries"]
    } == {"Company 2", "Company 4", "Company 5"}

    log_text = (output_dir / "run.log").read_text(encoding="utf-8")
    assert "mode=ordinals" in log_text
    assert "ordinals=4,2,5" in log_text


def test_resume_source_subset_contract_is_deterministic_and_summary_is_run_scoped(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    processed_companies: list[str] = []
    _install_lightweight_run_stubs(monkeypatch, rows=_rows(3), processed_companies=processed_companies)
    output_dir = tmp_path / "output"

    first_exit_code = pipeline.run(
        pipeline.parse_args(
            [
                "--input",
                "input.xlsx",
                "--output-dir",
                str(output_dir),
                "--sources=spark,zachestnyibiznes",
                "--ordinals=1,2",
            ]
        )
    )

    assert first_exit_code == 0
    assert processed_companies == ["Company 1", "Company 2"]

    resume_subset_exit_code = pipeline.run(
        pipeline.parse_args(
            [
                "--input",
                "input.xlsx",
                "--output-dir",
                str(output_dir),
                "--resume",
                "--sources=spark",
                "--ordinals=1,2",
            ]
        )
    )

    assert resume_subset_exit_code == 0
    assert processed_companies == ["Company 1", "Company 2"]
    subset_summary = json.loads((output_dir / "summary.json").read_text(encoding="utf-8"))
    assert subset_summary["processed_rows"] == 0
    assert subset_summary["completed_rows"] == 2
    assert subset_summary["remaining_rows"] == 0
    assert subset_summary["resume_skipped_rows"] == 2
    assert subset_summary["active_sources"] == ["spark"]

    expanded_subset_exit_code = pipeline.run(
        pipeline.parse_args(
            [
                "--input",
                "input.xlsx",
                "--output-dir",
                str(output_dir),
                "--resume",
                "--sources=spark,rusprofile",
                "--ordinals=1,2",
            ]
        )
    )

    assert expanded_subset_exit_code == 0
    assert processed_companies == ["Company 1", "Company 2", "Company 1", "Company 2"]
    expanded_summary = json.loads((output_dir / "summary.json").read_text(encoding="utf-8"))
    assert expanded_summary["processed_rows"] == 2
    assert expanded_summary["completed_rows"] == 2
    assert expanded_summary["remaining_rows"] == 0
    assert expanded_summary["resume_skipped_rows"] == 0
    assert expanded_summary["active_sources"] == ["spark", "rusprofile"]


def test_fresh_non_resume_run_resets_canonical_state_and_rebuilds_flat_outputs(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    processed_companies: list[str] = []
    _install_lightweight_run_stubs(monkeypatch, rows=_rows(4), processed_companies=processed_companies)
    output_dir = tmp_path / "output"

    first_exit_code = pipeline.run(
        pipeline.parse_args(
            [
                "--input",
                "input.xlsx",
                "--output-dir",
                str(output_dir),
                "--sources=spark",
                "--ordinals=1,2",
            ]
        )
    )

    assert first_exit_code == 0
    first_summary = json.loads((output_dir / "summary.json").read_text(encoding="utf-8"))
    first_runtime_state = json.loads((output_dir / "runtime_state.json").read_text(encoding="utf-8"))
    assert [item["company_name"] for item in json.loads((output_dir / "results.json").read_text(encoding="utf-8"))] == [
        "Company 1",
        "Company 2",
    ]
    assert [item["company"]["company_name"] for item in first_runtime_state["company_entries"]] == [
        "Company 1",
        "Company 2",
    ]

    second_exit_code = pipeline.run(
        pipeline.parse_args(
            [
                "--input",
                "input.xlsx",
                "--output-dir",
                str(output_dir),
                "--sources=spark",
                "--ordinals=4",
            ]
        )
    )

    assert second_exit_code == 0
    assert processed_companies == ["Company 1", "Company 2", "Company 4"]

    second_summary = json.loads((output_dir / "summary.json").read_text(encoding="utf-8"))
    second_runtime_state = json.loads((output_dir / "runtime_state.json").read_text(encoding="utf-8"))
    second_results = json.loads((output_dir / "results.json").read_text(encoding="utf-8"))
    second_results_log = [
        json.loads(line)
        for line in (output_dir / "results.jsonl").read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]

    assert second_summary["rows_selected"] == 1
    assert second_summary["processed_rows"] == 1
    assert second_summary["completed_rows"] == 1
    assert second_summary["remaining_rows"] == 0
    assert second_summary["resume_skipped_rows"] == 0
    assert second_summary["selected_ordinals"] == [4]
    assert second_summary["run_id"] != first_summary["run_id"]
    assert [item["company_name"] for item in second_results] == ["Company 4"]
    assert [item["company_name"] for item in second_results_log] == ["Company 4"]
    assert [item["company"]["company_name"] for item in second_runtime_state["company_entries"]] == ["Company 4"]
    assert second_runtime_state["run"]["metadata"]["run_id"] == second_summary["run_id"]
    assert second_runtime_state["run"]["metadata"]["run_id"] != first_runtime_state["run"]["metadata"]["run_id"]


def test_controlled_stop_publishes_partial_progress_without_complete_looking_summary(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    processed_companies: list[str] = []
    _install_lightweight_run_stubs(
        monkeypatch,
        rows=_rows(3),
        processed_companies=processed_companies,
        materialize_reports=True,
    )
    output_dir = tmp_path / "output"
    original_persist = core.ProgressStore.persist_completed_company_result
    persist_calls = {"count": 0}

    def request_stop_after_first_completion(self, *args, **kwargs):
        result = original_persist(self, *args, **kwargs)
        persist_calls["count"] += 1
        if persist_calls["count"] == 1:
            self.request_controlled_stop(reason="operator requested stop after first completed company")
        return result

    monkeypatch.setattr(core.ProgressStore, "persist_completed_company_result", request_stop_after_first_completion)

    exit_code = pipeline.run(
        pipeline.parse_args(
            [
                "--input",
                "input.xlsx",
                "--output-dir",
                str(output_dir),
                "--sources=spark,zachestnyibiznes",
                "--company-concurrency",
                "2",
            ]
        )
    )

    assert exit_code == 0
    assert processed_companies == ["Company 1"]

    summary = json.loads((output_dir / "summary.json").read_text(encoding="utf-8"))
    assert summary["processed_rows"] == 1
    assert summary["completed_rows"] == 1
    assert summary["remaining_rows"] == 2
    assert summary["run_status"] == "controlled_stop"
    assert summary["finish_reason"] == "controlled_stop"
    assert summary["stop_reason"] == "operator requested stop after first completed company"
    assert summary["stop_requested_at"] != ""
    assert summary["finished_at"] != ""
    assert summary["public_output_contract"]["terminal_run"] is True
    assert summary["public_output_contract"]["public_result_count"] == 1
    assert summary["public_output_contract"]["final_exports"] == {
        "state": "terminal_partial",
        "available": True,
        "row_count": 1,
        "paths": ["final_results.csv", "final_results.xlsx"],
    }

    runtime_state = json.loads((output_dir / "runtime_state.json").read_text(encoding="utf-8"))
    assert runtime_state["run"]["summary"]["run_status"] == "controlled_stop"
    assert runtime_state["run"]["metadata"]["run_status"] == "controlled_stop"
    assert runtime_state["run"]["metadata"]["finish_reason"] == "controlled_stop"
    assert runtime_state["run"]["metadata"]["stop_reason"] == "operator requested stop after first completed company"

    results = json.loads((output_dir / "results.json").read_text(encoding="utf-8"))
    assert [item["company_name"] for item in results] == ["Company 1"]

    report = (output_dir / "report.md").read_text(encoding="utf-8")
    insights = (output_dir / "insights.md").read_text(encoding="utf-8")
    leads = (output_dir / "leads.md").read_text(encoding="utf-8")
    assert "Run status: `controlled_stop`" in report
    assert "Run status: `controlled_stop`" in insights
    assert "Run status: `controlled_stop`" in leads
    assert "Finish reason: `controlled_stop`" in report
    assert "Public outputs include only completed companies" in report

    reloaded = core.ProgressStore(output_dir)
    reloaded_summary = json.loads((output_dir / "summary.json").read_text(encoding="utf-8"))
    reloaded_results = json.loads((output_dir / "results.json").read_text(encoding="utf-8"))
    assert reloaded_summary["run_status"] == "controlled_stop"
    assert reloaded_summary["remaining_rows"] == 2
    assert [item["company_name"] for item in reloaded_results] == ["Company 1"]
    assert reloaded.get("0000000001") is not None
    assert reloaded.get("0000000002") is None


def test_required_source_red_flag_stops_run_and_materializes_terminal_surfaces(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    processed_companies: list[str] = []
    search_calls: list[tuple[str, str]] = []
    _install_lightweight_run_stubs(
        monkeypatch,
        rows=_rows(2),
        processed_companies=processed_companies,
        materialize_reports=True,
    )

    class _RecordingSource(_StaticSource):
        def __init__(
            self,
            source_name: str,
            *,
            result_factory=None,
        ) -> None:
            super().__init__(source_name)
            self._result_factory = result_factory

        def search(self, row: core.RowInput) -> core.SourceResult:
            search_calls.append((self.source_name, row.inn))
            if self._result_factory is not None:
                return self._result_factory(row)
            return core.SourceResult(source=self.source_name, status="ok")

    def _checko_red_flag(_row: core.RowInput) -> core.SourceResult:
        return core.make_blocked_source_result(
            "checko",
            core.REQUEST_BLOCKED_NO_PROXY_NO_POOL_REASON,
            status=core.REQUEST_STATUS_BLOCKED_NO_PROXY,
        )

    monkeypatch.setattr(pipeline, "SparkSource", lambda _client: _RecordingSource("spark"))
    monkeypatch.setattr(pipeline, "ZachestnyBiznesSource", lambda _client: _RecordingSource("zachestnyibiznes"))
    monkeypatch.setattr(pipeline, "RusprofileSource", lambda _client: _RecordingSource("rusprofile"))
    monkeypatch.setattr(pipeline, "CheckoSource", lambda _client: _RecordingSource("checko", result_factory=_checko_red_flag))
    monkeypatch.setattr(pipeline, "ListOrgSource", lambda _client, _snapshot: _RecordingSource("list_org"))

    output_dir = tmp_path / "output"
    exit_code = pipeline.run(
        pipeline.parse_args(
            [
                "--input",
                "input.xlsx",
                "--output-dir",
                str(output_dir),
                "--sources=spark,zachestnyibiznes,rusprofile,checko,list_org",
                "--company-concurrency",
                "1",
            ]
        )
    )

    assert exit_code == 1
    assert processed_companies == []
    assert search_calls == [
        ("spark", "0000000001"),
        ("zachestnyibiznes", "0000000001"),
        ("rusprofile", "0000000001"),
        ("checko", "0000000001"),
    ]
    expected_terminal_reason = core.build_required_source_fail_fast_reason(
        "checko",
        core.REQUEST_STATUS_BLOCKED_NO_PROXY,
        access_mode="proxy-bound",
        detail=core.REQUEST_BLOCKED_NO_PROXY_NO_POOL_REASON,
    )

    summary = json.loads((output_dir / "summary.json").read_text(encoding="utf-8"))
    assert summary["processed_rows"] == 0
    assert summary["completed_rows"] == 0
    assert summary["remaining_rows"] == 2
    assert summary["run_status"] == pipeline.RUN_STATUS_FAILED_REQUIRED_SOURCE
    assert summary["finish_reason"] == pipeline.RUN_FINISH_REASON_REQUIRED_SOURCE
    assert summary["terminal_source"] == "checko"
    assert summary["terminal_source_status"] == core.REQUEST_STATUS_BLOCKED_NO_PROXY
    assert summary["terminal_source_access_mode"] == "proxy-bound"
    assert summary["terminal_error_type"] == pipeline.REQUIRED_SOURCE_TERMINAL_ERROR_TYPE
    assert summary["stop_reason"] == "required_source_red_flag"
    assert summary["terminal_error_message"] == expected_terminal_reason

    runtime_state = json.loads((output_dir / "runtime_state.json").read_text(encoding="utf-8"))
    assert runtime_state["run"]["summary"]["run_status"] == pipeline.RUN_STATUS_FAILED_REQUIRED_SOURCE
    assert runtime_state["run"]["summary"]["terminal_source"] == "checko"
    assert runtime_state["run"]["summary"]["stop_reason"] == "required_source_red_flag"
    assert runtime_state["run"]["metadata"]["run_status"] == pipeline.RUN_STATUS_FAILED_REQUIRED_SOURCE
    assert runtime_state["run"]["metadata"]["terminal_source_status"] == core.REQUEST_STATUS_BLOCKED_NO_PROXY
    assert runtime_state["run"]["metadata"]["terminal_source_access_mode"] == "proxy-bound"
    assert runtime_state["run"]["metadata"]["stop_reason"] == "required_source_red_flag"

    assert json.loads((output_dir / "results.json").read_text(encoding="utf-8")) == []
    assert json.loads((output_dir / "leads.json").read_text(encoding="utf-8")) == []
    public_publish_state = json.loads(
        (output_dir / "_runtime" / "public_publish_state.json").read_text(encoding="utf-8")
    )
    committed_generation_id = public_publish_state["committed_generation_id"]
    assert committed_generation_id != ""
    committed_summary = json.loads(
        (output_dir / "_runtime" / "public_generations" / committed_generation_id / "summary.json").read_text(
            encoding="utf-8"
        )
    )
    assert committed_summary == summary
    report = (output_dir / "report.md").read_text(encoding="utf-8")
    insights = (output_dir / "insights.md").read_text(encoding="utf-8")
    assert "Run status: `failed_required_source`" in report
    assert "Terminal source: `checko`" in report
    assert "Terminal source access mode: `proxy-bound`" in report
    assert "Run status: `failed_required_source`" in insights

    event_log = [
        json.loads(line)
        for line in (output_dir / "events.jsonl").read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    assert [item["type"] for item in event_log] == ["run_started", "required_source_red_flag", "run_finished"]
    assert event_log[1]["source"] == "checko"
    assert event_log[1]["source_status"] == core.REQUEST_STATUS_BLOCKED_NO_PROXY
    assert event_log[1]["source_access_mode"] == "proxy-bound"
    assert event_log[1]["reason"] == expected_terminal_reason
    assert event_log[2]["run_status"] == pipeline.RUN_STATUS_FAILED_REQUIRED_SOURCE
    assert event_log[2]["stop_reason"] == "required_source_red_flag"
    assert event_log[2]["terminal_source"] == "checko"
    assert event_log[2]["terminal_source_status"] == core.REQUEST_STATUS_BLOCKED_NO_PROXY


def test_required_source_transient_can_defer_row_and_continue_later_rows(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    processed_companies: list[str] = []
    search_calls: list[tuple[str, str]] = []
    _install_lightweight_run_stubs(monkeypatch, rows=_rows(2), processed_companies=processed_companies)

    class _RecordingSource(_StaticSource):
        def __init__(self, source_name: str, *, result_factory=None) -> None:
            super().__init__(source_name)
            self._result_factory = result_factory

        def search(self, row: core.RowInput) -> core.SourceResult:
            search_calls.append((self.source_name, row.inn))
            if self._result_factory is not None:
                return self._result_factory(row)
            return core.SourceResult(source=self.source_name, status="ok")

    def _spark_transient_once(row: core.RowInput) -> core.SourceResult:
        if row.inn == "0000000001":
            result = core.SourceResult(source="spark", status="request_error")
            result.errors.append("Read timeout after finite Spark source retries")
            return result
        return core.SourceResult(source="spark", status="ok")

    monkeypatch.setattr(pipeline, "SparkSource", lambda _client: _RecordingSource("spark", result_factory=_spark_transient_once))
    monkeypatch.setattr(pipeline, "ZachestnyBiznesSource", lambda _client: _RecordingSource("zachestnyibiznes"))

    output_dir = tmp_path / "output"
    exit_code = pipeline.run(
        pipeline.parse_args(
            [
                "--input",
                "input.xlsx",
                "--output-dir",
                str(output_dir),
                "--sources=spark,zachestnyibiznes",
                "--defer-required-source-transients",
                "--company-concurrency",
                "1",
            ]
        )
    )

    assert exit_code == 0
    assert processed_companies == ["Company 2"]
    assert search_calls == [
        ("spark", "0000000001"),
        ("spark", "0000000002"),
        ("zachestnyibiznes", "0000000002"),
    ]

    summary = json.loads((output_dir / "summary.json").read_text(encoding="utf-8"))
    assert summary["processed_rows"] == 1
    assert summary["completed_rows"] == 1
    assert summary["remaining_rows"] == 1
    assert summary["run_status"] == pipeline.RUN_STATUS_COMPLETED_WITH_DEFERRED_REQUIRED_SOURCES
    assert summary["finish_reason"] == pipeline.RUN_FINISH_REASON_DEFERRED_REQUIRED_SOURCES
    assert summary["required_source_deferred_rows_total"] == 1
    assert summary["unresolved_required_source_rows"] == 1
    assert summary["required_source_deferred_rows_by_source"] == {"spark": 1}
    assert summary["required_source_deferred_rows_by_status"] == {"request_error": 1}
    deferred_records = summary["deferred_required_sources"]["records"]
    assert list(deferred_records) == ["0000000001::spark"]
    assert deferred_records["0000000001::spark"]["inn"] == "0000000001"
    assert deferred_records["0000000001::spark"]["access_mode"] == "direct-default"
    assert deferred_records["0000000001::spark"]["status"] == "request_error"

    runtime_state = json.loads((output_dir / "runtime_state.json").read_text(encoding="utf-8"))
    assert runtime_state["run"]["summary"]["unresolved_required_source_rows"] == 1
    assert runtime_state["run"]["metadata"]["deferred_required_sources"]["records"] == deferred_records
    results = json.loads((output_dir / "results.json").read_text(encoding="utf-8"))
    assert [item["company_name"] for item in results] == ["Company 2"]
    assert {item["status"] for item in results} == {"completed"}

    event_types = [
        json.loads(line)["type"]
        for line in (output_dir / "events.jsonl").read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    assert "required_source_deferred" in event_types
    assert "required_source_red_flag" not in event_types


def test_spark_direct_timeout_burst_defers_until_later_success_without_clean_completion(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    processed_companies: list[str] = []
    search_calls: list[tuple[str, str]] = []
    timeout_inns = {"0000000001", "0000000002"}
    _install_lightweight_run_stubs(monkeypatch, rows=_rows(3), processed_companies=processed_companies)

    class _BurstSparkSource(_StaticSource):
        def search(self, row: core.RowInput) -> core.SourceResult:
            search_calls.append((self.source_name, row.inn))
            if row.inn in timeout_inns:
                result = core.SourceResult(source=self.source_name, status="request_error")
                result.errors.append("Read timeout after finite Spark source retries")
                return result
            return core.SourceResult(source=self.source_name, status="ok")

    monkeypatch.setattr(pipeline, "SparkSource", lambda _client: _BurstSparkSource("spark"))

    output_dir = tmp_path / "output"
    exit_code = pipeline.run(
        pipeline.parse_args(
            [
                "--input",
                "input.xlsx",
                "--output-dir",
                str(output_dir),
                "--sources=spark",
                "--defer-required-source-transients",
                "--company-concurrency",
                "1",
            ]
        )
    )

    assert exit_code == 0
    assert processed_companies == ["Company 3"]
    assert search_calls == [
        ("spark", "0000000001"),
        ("spark", "0000000002"),
        ("spark", "0000000003"),
    ]

    summary = json.loads((output_dir / "summary.json").read_text(encoding="utf-8"))
    assert summary["processed_rows"] == 1
    assert summary["completed_rows"] == 1
    assert summary["remaining_rows"] == 2
    assert summary["run_status"] == pipeline.RUN_STATUS_COMPLETED_WITH_DEFERRED_REQUIRED_SOURCES
    assert summary["finish_reason"] == pipeline.RUN_FINISH_REASON_DEFERRED_REQUIRED_SOURCES
    assert summary["required_source_deferred_rows_total"] == 2
    assert summary["unresolved_required_source_rows"] == 2
    assert summary["required_source_deferred_rows_by_source"] == {"spark": 2}
    assert summary["required_source_deferred_rows_by_status"] == {"request_error": 2}
    deferred_state = summary["deferred_required_sources"]
    assert list(deferred_state["records"]) == ["0000000001::spark", "0000000002::spark"]
    assert deferred_state["source_health"]["spark"]["success_after_deferred_count"] == 1
    assert deferred_state["source_health"]["spark"]["consecutive_deferred_rows"] == 0

    event_types = [
        json.loads(line)["type"]
        for line in (output_dir / "events.jsonl").read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    assert event_types.count("required_source_deferred") == 2
    assert "required_source_red_flag" not in event_types
    run_finished_event = [
        json.loads(line)
        for line in (output_dir / "events.jsonl").read_text(encoding="utf-8").splitlines()
        if line.strip() and json.loads(line)["type"] == "run_finished"
    ][0]
    assert run_finished_event["run_status"] == pipeline.RUN_STATUS_COMPLETED_WITH_DEFERRED_REQUIRED_SOURCES
    assert run_finished_event["finish_reason"] == pipeline.RUN_FINISH_REASON_DEFERRED_REQUIRED_SOURCES


def test_deferred_required_source_retries_same_process_after_later_success(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    processed_companies: list[str] = []
    search_calls: list[tuple[str, str]] = []
    attempts_by_inn: dict[str, int] = {}
    lock = Lock()
    _install_lightweight_run_stubs(monkeypatch, rows=_rows(2), processed_companies=processed_companies)

    class _RetryingSparkSource(_StaticSource):
        def search(self, row: core.RowInput) -> core.SourceResult:
            with lock:
                attempts_by_inn[row.inn] = attempts_by_inn.get(row.inn, 0) + 1
                attempt = attempts_by_inn[row.inn]
                search_calls.append((self.source_name, row.inn))
            if row.inn == "0000000001" and attempt == 1:
                result = core.SourceResult(source="spark", status="request_error")
                result.errors.append("Read timeout after finite Spark source retries")
                return result
            return core.SourceResult(source="spark", status="ok")

    monkeypatch.setattr(pipeline, "SparkSource", lambda _client: _RetryingSparkSource("spark"))

    output_dir = tmp_path / "output"
    exit_code = pipeline.run(
        pipeline.parse_args(
            [
                "--input",
                "input.xlsx",
                "--output-dir",
                str(output_dir),
                "--sources=spark",
                "--defer-required-source-transients",
                "--company-concurrency",
                "2",
            ]
        )
    )

    assert exit_code == 0
    assert attempts_by_inn == {"0000000001": 2, "0000000002": 1}
    assert processed_companies == ["Company 2", "Company 1"]
    assert search_calls.count(("spark", "0000000001")) == 2

    summary = json.loads((output_dir / "summary.json").read_text(encoding="utf-8"))
    assert summary["run_status"] == "completed"
    assert summary["required_source_deferred_rows_total"] == 0
    assert summary["unresolved_required_source_rows"] == 0
    record = summary["deferred_required_sources"]["records"]["0000000001::spark"]
    assert record["resolution_status"] == "resolved"
    assert record["resolution_source_status"] == "ok"

    results = json.loads((output_dir / "results.json").read_text(encoding="utf-8"))
    assert [item["company_name"] for item in results] == ["Company 1", "Company 2"]
    assert [item["row_index"] for item in results] == [row.row_index for row in _rows(2)]

    event_types = [
        json.loads(line)["type"]
        for line in (output_dir / "events.jsonl").read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    assert "required_source_deferred_retry_scheduled" in event_types
    assert "required_source_deferred_resolved" in event_types


def test_deferred_required_source_same_process_retry_preserves_source_results_and_report(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    processed_companies: list[str] = []
    attempts_by_inn: dict[str, int] = {}
    lock = Lock()
    _install_lightweight_run_stubs(
        monkeypatch,
        rows=_rows(2),
        processed_companies=processed_companies,
        materialize_reports=True,
    )

    class _RetryingRusprofileSource(_StaticSource):
        def search(self, row: core.RowInput) -> core.SourceResult:
            with lock:
                attempts_by_inn[row.inn] = attempts_by_inn.get(row.inn, 0) + 1
                attempt = attempts_by_inn[row.inn]
            if row.inn == "0000000001" and attempt == 1:
                result = core.SourceResult(source="rusprofile", status="request_error")
                result.errors.append("Read timeout after finite Rusprofile source retries")
                return result
            return core.SourceResult(source="rusprofile", status="ok")

    monkeypatch.setattr(pipeline, "RusprofileSource", lambda _client: _RetryingRusprofileSource("rusprofile"))

    output_dir = tmp_path / "output"
    exit_code = pipeline.run(
        pipeline.parse_args(
            [
                "--input",
                "input.xlsx",
                "--output-dir",
                str(output_dir),
                "--sources=spark,zachestnyibiznes,rusprofile",
                "--defer-required-source-transients",
                "--company-concurrency",
                "2",
            ]
        )
    )

    assert exit_code == 0
    assert attempts_by_inn == {"0000000001": 2, "0000000002": 1}
    assert processed_companies == ["Company 2", "Company 1"]

    summary = json.loads((output_dir / "summary.json").read_text(encoding="utf-8"))
    assert summary["run_status"] == "completed"
    assert summary["required_source_deferred_rows_total"] == 0
    assert summary["unresolved_required_source_rows"] == 0
    record = summary["deferred_required_sources"]["records"]["0000000001::rusprofile"]
    assert record["resolution_status"] == "resolved"
    assert record["resolution_source_status"] == "ok"

    expected_sources = ["rusprofile", "spark", "zachestnyibiznes"]
    results = json.loads((output_dir / "results.json").read_text(encoding="utf-8"))
    first_result = next(item for item in results if item["inn"] == "0000000001")
    assert sorted(first_result["sources"]) == expected_sources
    assert first_result["sources"]["spark"]["status"] == "ok"
    assert first_result["sources"]["zachestnyibiznes"]["status"] == "ok"
    assert first_result["sources"]["rusprofile"]["status"] == "ok"

    jsonl_results = [
        json.loads(line)
        for line in (output_dir / "results.jsonl").read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    first_jsonl_result = next(item for item in jsonl_results if item["inn"] == "0000000001")
    assert sorted(first_jsonl_result["sources"]) == expected_sources

    report_path = next((output_dir / "company_reports").glob("*0000000001*.md"))
    report = report_path.read_text(encoding="utf-8")
    assert "### spark" in report
    assert "### zachestnyibiznes" in report
    assert "### rusprofile" in report


def test_deferred_required_source_same_process_retry_keeps_unresolved_row_visible(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    processed_companies: list[str] = []
    attempts_by_inn: dict[str, int] = {}
    lock = Lock()
    _install_lightweight_run_stubs(monkeypatch, rows=_rows(2), processed_companies=processed_companies)

    class _StillFailingSparkSource(_StaticSource):
        def search(self, row: core.RowInput) -> core.SourceResult:
            with lock:
                attempts_by_inn[row.inn] = attempts_by_inn.get(row.inn, 0) + 1
            if row.inn == "0000000001":
                result = core.SourceResult(source="spark", status="request_error")
                result.errors.append("Read timeout after finite Spark source retries")
                return result
            return core.SourceResult(source="spark", status="ok")

    monkeypatch.setattr(pipeline, "SparkSource", lambda _client: _StillFailingSparkSource("spark"))

    output_dir = tmp_path / "output"
    exit_code = pipeline.run(
        pipeline.parse_args(
            [
                "--input",
                "input.xlsx",
                "--output-dir",
                str(output_dir),
                "--sources=spark",
                "--defer-required-source-transients",
                "--company-concurrency",
                "2",
            ]
        )
    )

    assert exit_code == 0
    assert attempts_by_inn == {"0000000001": 2, "0000000002": 1}
    assert processed_companies == ["Company 2"]

    summary = json.loads((output_dir / "summary.json").read_text(encoding="utf-8"))
    assert summary["run_status"] == pipeline.RUN_STATUS_COMPLETED_WITH_DEFERRED_REQUIRED_SOURCES
    assert summary["finish_reason"] == pipeline.RUN_FINISH_REASON_DEFERRED_REQUIRED_SOURCES
    assert summary["required_source_deferred_rows_total"] == 1
    assert summary["unresolved_required_source_rows"] == 1
    record = summary["deferred_required_sources"]["records"]["0000000001::spark"]
    assert record["resolution_status"] == "unresolved"
    assert record["status"] == "request_error"

    results = json.loads((output_dir / "results.json").read_text(encoding="utf-8"))
    assert [item["company_name"] for item in results] == ["Company 2"]

    event_types = [
        json.loads(line)["type"]
        for line in (output_dir / "events.jsonl").read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    assert "required_source_deferred_retry_scheduled" in event_types
    assert "required_source_deferred_resolved" not in event_types
    assert "required_source_red_flag" not in event_types


def test_spark_isolated_direct_timeouts_do_not_trip_total_deferred_cap_on_stubbed_surface(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    processed_companies: list[str] = []
    search_calls: list[tuple[str, str]] = []
    timeout_inns = {"0000000001", "0000000002", "0000000004"}
    _install_lightweight_run_stubs(monkeypatch, rows=_rows(5), processed_companies=processed_companies)

    class _IntermittentSparkSource(_StaticSource):
        def search(self, row: core.RowInput) -> core.SourceResult:
            search_calls.append((self.source_name, row.inn))
            if row.inn in timeout_inns:
                result = core.SourceResult(source=self.source_name, status="request_error")
                result.errors.append("Read timeout after finite Spark source retries")
                return result
            return core.SourceResult(source=self.source_name, status="ok")

    monkeypatch.setattr(pipeline, "SparkSource", lambda _client: _IntermittentSparkSource("spark"))

    output_dir = tmp_path / "output"
    exit_code = pipeline.run(
        pipeline.parse_args(
            [
                "--input",
                "input.xlsx",
                "--output-dir",
                str(output_dir),
                "--sources=spark",
                "--defer-required-source-transients",
                "--company-concurrency",
                "1",
            ]
        )
    )

    assert exit_code == 0
    assert len(search_calls) == 5
    assert processed_companies == ["Company 3", "Company 5"]

    summary = json.loads((output_dir / "summary.json").read_text(encoding="utf-8"))
    assert summary["processed_rows"] == 2
    assert summary["completed_rows"] == 2
    assert summary["remaining_rows"] == 3
    assert summary["run_status"] == pipeline.RUN_STATUS_COMPLETED_WITH_DEFERRED_REQUIRED_SOURCES
    assert summary["finish_reason"] == pipeline.RUN_FINISH_REASON_DEFERRED_REQUIRED_SOURCES
    assert summary["required_source_deferred_rows_total"] == 3
    assert summary["unresolved_required_source_rows"] == 3
    deferred_state = summary["deferred_required_sources"]
    assert list(deferred_state["records"]) == [
        "0000000001::spark",
        "0000000002::spark",
        "0000000004::spark",
    ]
    assert deferred_state["source_health"]["spark"]["success_after_deferred_count"] == 2
    assert deferred_state["source_health"]["spark"]["consecutive_deferred_rows"] == 0

    event_log = [
        json.loads(line)
        for line in (output_dir / "events.jsonl").read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    assert [item["type"] for item in event_log].count("required_source_deferred") == 3
    assert "required_source_red_flag" not in [item["type"] for item in event_log]
    assert event_log[-1]["type"] == "run_finished"
    assert event_log[-1]["run_status"] == pipeline.RUN_STATUS_COMPLETED_WITH_DEFERRED_REQUIRED_SOURCES
    assert event_log[-1]["finish_reason"] == pipeline.RUN_FINISH_REASON_DEFERRED_REQUIRED_SOURCES


def test_required_source_deferred_consecutive_cap_allows_isolated_third_timeout_on_100_row_surface() -> None:
    state = required_source_deferred_state()
    for inn in ("0000000001", "0000000002"):
        state = record_required_source_deferred(
            state,
            build_required_source_deferred_record(
                source="spark",
                access_mode="direct-default",
                status="request_error",
                error="Read timeout after finite Spark source retries",
                row_index=int(inn),
                inn=inn,
                company_name=f"Company {int(inn)}",
                run_id="run-1",
                now_iso="2026-05-03T00:00:00+00:00",
            ),
        )
    state, changed = mark_required_source_success(
        state,
        source="spark",
        now_iso="2026-05-03T00:01:00+00:00",
    )

    classification = classify_required_source_outcome(
        source="spark",
        access_mode="direct-default",
        status="request_error",
        detail="Read timeout after finite Spark source retries",
        defer_enabled=True,
        selected_rows=100,
        deferred_state=state,
    )

    assert changed is True
    assert classification.outcome == OUTCOME_DEFERRED_ROW_TRANSIENT
    assert "consecutive_deferred_count_for_source=1/2" in classification.reason
    assert "unresolved_deferred_count_for_source=3" in classification.reason


def test_required_source_dns_name_resolution_request_error_defers_only_when_enabled() -> None:
    detail = (
        "HTTPSConnectionPool(host='www.rusprofile.ru', port=443): "
        "Max retries exceeded with url: /search?query=7804309200 "
        "(Caused by NameResolutionError(\"Failed to resolve 'www.rusprofile.ru' "
        "([Errno 11001] getaddrinfo failed)\"))"
    )

    disabled = classify_required_source_outcome(
        source="rusprofile",
        access_mode=core.SESSION_BOUND_TRANSPORT,
        status="request_error",
        detail=detail,
        defer_enabled=False,
        selected_rows=6,
        deferred_state=required_source_deferred_state(),
    )
    enabled = classify_required_source_outcome(
        source="rusprofile",
        access_mode=core.SESSION_BOUND_TRANSPORT,
        status="request_error",
        detail=detail,
        defer_enabled=True,
        selected_rows=6,
        deferred_state=required_source_deferred_state(),
    )

    assert "rusprofile" in core.CANONICAL_REQUIRED_SOURCE_NAMES
    assert core.source_result_requires_run_fail_fast(
        "rusprofile",
        "request_error",
        access_mode=core.SESSION_BOUND_TRANSPORT,
    )
    assert disabled.outcome == OUTCOME_NON_DEFER_FAIL_FAST
    assert disabled.reason == "required source deferral is disabled"
    assert enabled.outcome == OUTCOME_DEFERRED_ROW_TRANSIENT
    assert "eligible required-source row transient" in enabled.reason


def test_required_source_rusprofile_connect_timeout_defers_only_when_enabled() -> None:
    detail = (
        "HTTPSConnectionPool(host='www.rusprofile.ru', port=443): "
        "Max retries exceeded with url: /search?query=7804309200 "
        "(Caused by ConnectTimeoutError("
        "'Connection to www.rusprofile.ru timed out. (connect timeout=18)'))"
    )

    disabled = classify_required_source_outcome(
        source="rusprofile",
        access_mode=core.SESSION_BOUND_TRANSPORT,
        status="request_error",
        detail=detail,
        defer_enabled=False,
        selected_rows=6,
        deferred_state=required_source_deferred_state(),
    )
    enabled = classify_required_source_outcome(
        source="rusprofile",
        access_mode=core.SESSION_BOUND_TRANSPORT,
        status="request_error",
        detail=detail,
        defer_enabled=True,
        selected_rows=6,
        deferred_state=required_source_deferred_state(),
    )

    assert core.source_result_requires_run_fail_fast(
        "rusprofile",
        "request_error",
        access_mode=core.SESSION_BOUND_TRANSPORT,
    )
    assert disabled.outcome == OUTCOME_NON_DEFER_FAIL_FAST
    assert disabled.reason == "required source deferral is disabled"
    assert enabled.outcome == OUTCOME_DEFERRED_ROW_TRANSIENT
    assert "eligible required-source row transient after finite source retries: request_error" in enabled.reason


def test_zachestnyibiznes_blocked_connect_timeout_defers_only_when_enabled() -> None:
    detail = (
        "HTTPSConnectionPool(host='zachestnyibiznes.ru', port=443): Max retries exceeded with url: "
        "/search?query=7703770101 (Caused by ConnectTimeoutError("
        "'Connection to zachestnyibiznes.ru timed out. (connect timeout=18)'))"
    )

    disabled = classify_required_source_outcome(
        source="zachestnyibiznes",
        access_mode=core.DIRECT_DEFAULT_TRANSPORT,
        status="blocked",
        detail=detail,
        defer_enabled=False,
        selected_rows=6,
        deferred_state=required_source_deferred_state(),
    )
    enabled = classify_required_source_outcome(
        source="zachestnyibiznes",
        access_mode=core.DIRECT_DEFAULT_TRANSPORT,
        status="blocked",
        detail=detail,
        defer_enabled=True,
        selected_rows=6,
        deferred_state=required_source_deferred_state(),
    )

    assert disabled.outcome == OUTCOME_NON_DEFER_FAIL_FAST
    assert disabled.reason == "required source deferral is disabled"
    assert enabled.outcome == OUTCOME_DEFERRED_ROW_TRANSIENT
    assert "eligible required-source row transient after finite source retries: blocked" in enabled.reason


def test_zachestnyibiznes_real_blocked_status_remains_source_wide_stop() -> None:
    classification = classify_required_source_outcome(
        source="zachestnyibiznes",
        access_mode=core.DIRECT_DEFAULT_TRANSPORT,
        status="blocked",
        detail="Search page is blocked by bot gate",
        defer_enabled=True,
        selected_rows=6,
        deferred_state=required_source_deferred_state(),
    )

    assert classification.outcome == OUTCOME_SYSTEMIC_STOP
    assert classification.reason == "source-wide required-source stop status: blocked"


def test_spark_direct_timeout_burst_fails_closed_at_deferred_cap(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    processed_companies: list[str] = []
    search_calls: list[tuple[str, str]] = []
    _install_lightweight_run_stubs(monkeypatch, rows=_rows(3), processed_companies=processed_companies)

    class _TimeoutSparkSource(_StaticSource):
        def search(self, row: core.RowInput) -> core.SourceResult:
            search_calls.append((self.source_name, row.inn))
            result = core.SourceResult(source=self.source_name, status="request_error")
            result.errors.append("Read timeout after finite Spark source retries")
            return result

    monkeypatch.setattr(pipeline, "SparkSource", lambda _client: _TimeoutSparkSource("spark"))

    output_dir = tmp_path / "output"
    exit_code = pipeline.run(
        pipeline.parse_args(
            [
                "--input",
                "input.xlsx",
                "--output-dir",
                str(output_dir),
                "--sources=spark",
                "--defer-required-source-transients",
                "--company-concurrency",
                "1",
            ]
        )
    )

    assert exit_code == 1
    assert processed_companies == []
    assert search_calls == [
        ("spark", "0000000001"),
        ("spark", "0000000002"),
        ("spark", "0000000003"),
    ]

    summary = json.loads((output_dir / "summary.json").read_text(encoding="utf-8"))
    assert summary["processed_rows"] == 0
    assert summary["completed_rows"] == 0
    assert summary["remaining_rows"] == 3
    assert summary["run_status"] == pipeline.RUN_STATUS_FAILED_REQUIRED_SOURCE
    assert summary["terminal_source"] == "spark"
    assert summary["terminal_source_status"] == "request_error"
    assert summary["required_source_deferred_rows_total"] == 2
    assert summary["unresolved_required_source_rows"] == 2

    event_log = [
        json.loads(line)
        for line in (output_dir / "events.jsonl").read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    assert [item["type"] for item in event_log] == [
        "run_started",
        "required_source_deferred",
        "required_source_deferred",
        "required_source_red_flag",
        "run_finished",
    ]
    assert event_log[3]["required_source_outcome"] == "systemic_stop"
    assert (
        event_log[3]["classification_reason"]
        == "deferred required-source consecutive cap exceeded for spark: consecutive=2 cap=2"
    )


def test_retry_deferred_required_sources_promotes_only_unresolved_row(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    processed_companies: list[str] = []
    search_calls: list[tuple[str, str]] = []
    spark_transient_inns = {"0000000001"}
    _install_lightweight_run_stubs(monkeypatch, rows=_rows(2), processed_companies=processed_companies)

    class _RecordingSparkSource(_StaticSource):
        def search(self, row: core.RowInput) -> core.SourceResult:
            search_calls.append((self.source_name, row.inn))
            if row.inn in spark_transient_inns:
                result = core.SourceResult(source="spark", status="request_error")
                result.errors.append("Read timeout after finite Spark source retries")
                return result
            return core.SourceResult(source="spark", status="ok")

    monkeypatch.setattr(pipeline, "SparkSource", lambda _client: _RecordingSparkSource("spark"))

    output_dir = tmp_path / "output"
    first_exit_code = pipeline.run(
        pipeline.parse_args(
            [
                "--input",
                "input.xlsx",
                "--output-dir",
                str(output_dir),
                "--sources=spark",
                "--defer-required-source-transients",
                "--company-concurrency",
                "1",
            ]
        )
    )

    assert first_exit_code == 0
    assert processed_companies == ["Company 2"]
    assert search_calls == [("spark", "0000000001"), ("spark", "0000000002")]

    spark_transient_inns.clear()
    second_exit_code = pipeline.run(
        pipeline.parse_args(
            [
                "--input",
                "input.xlsx",
                "--output-dir",
                str(output_dir),
                "--sources=spark",
                "--retry-deferred-required-sources",
                "--company-concurrency",
                "1",
            ]
        )
    )

    assert second_exit_code == 0
    assert processed_companies == ["Company 2", "Company 1"]
    assert search_calls == [
        ("spark", "0000000001"),
        ("spark", "0000000002"),
        ("spark", "0000000001"),
    ]

    summary = json.loads((output_dir / "summary.json").read_text(encoding="utf-8"))
    assert summary["rows_selected"] == 1
    assert summary["processed_rows"] == 1
    assert summary["completed_rows"] == 2
    assert summary["remaining_rows"] == 0
    assert summary["run_status"] == "completed"
    assert summary["required_source_deferred_rows_total"] == 0
    assert summary["unresolved_required_source_rows"] == 0
    record = summary["deferred_required_sources"]["records"]["0000000001::spark"]
    assert record["resolution_status"] == "resolved"
    assert record["resolution_source_status"] == "ok"

    runtime_state = json.loads((output_dir / "runtime_state.json").read_text(encoding="utf-8"))
    assert runtime_state["run"]["summary"]["unresolved_required_source_rows"] == 0
    assert runtime_state["run"]["metadata"]["required_source_deferred_rows_total"] == 0

    results = json.loads((output_dir / "results.json").read_text(encoding="utf-8"))
    assert [item["company_name"] for item in results] == ["Company 1", "Company 2"]
    assert [item["sources"]["spark"]["status"] for item in results] == ["ok", "ok"]

    event_types = [
        json.loads(line)["type"]
        for line in (output_dir / "events.jsonl").read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    assert "required_source_deferred_resolved" in event_types

    third_exit_code = pipeline.run(
        pipeline.parse_args(
            [
                "--input",
                "input.xlsx",
                "--output-dir",
                str(output_dir),
                "--sources=spark",
                "--retry-deferred-required-sources",
                "--company-concurrency",
                "1",
            ]
        )
    )

    assert third_exit_code == 0
    assert search_calls == [
        ("spark", "0000000001"),
        ("spark", "0000000002"),
        ("spark", "0000000001"),
    ]
    idempotent_summary = json.loads((output_dir / "summary.json").read_text(encoding="utf-8"))
    assert idempotent_summary["rows_selected"] == 0
    assert idempotent_summary["processed_rows"] == 0
    assert idempotent_summary["completed_rows"] == 2
    assert idempotent_summary["required_source_deferred_rows_total"] == 0
    idempotent_results = json.loads((output_dir / "results.json").read_text(encoding="utf-8"))
    assert [item["company_name"] for item in idempotent_results] == ["Company 1", "Company 2"]


def test_retry_deferred_required_sources_applies_window_before_unresolved_filter(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    processed_companies: list[str] = []
    search_calls: list[tuple[str, str]] = []
    spark_transient_inns = {"0000000002", "0000000004"}
    _install_lightweight_run_stubs(monkeypatch, rows=_rows(5), processed_companies=processed_companies)

    class _RecordingSparkSource(_StaticSource):
        def search(self, row: core.RowInput) -> core.SourceResult:
            search_calls.append((self.source_name, row.inn))
            if row.inn in spark_transient_inns:
                result = core.SourceResult(source="spark", status="request_error")
                result.errors.append("Read timeout after finite Spark source retries")
                return result
            return core.SourceResult(source="spark", status="ok")

    monkeypatch.setattr(pipeline, "SparkSource", lambda _client: _RecordingSparkSource("spark"))

    output_dir = tmp_path / "output"
    first_exit_code = pipeline.run(
        pipeline.parse_args(
            [
                "--input",
                "input.xlsx",
                "--output-dir",
                str(output_dir),
                "--sources=spark",
                "--defer-required-source-transients",
                "--company-concurrency",
                "1",
            ]
        )
    )

    assert first_exit_code == 0
    assert processed_companies == ["Company 1", "Company 3", "Company 5"]
    assert search_calls == [
        ("spark", "0000000001"),
        ("spark", "0000000002"),
        ("spark", "0000000003"),
        ("spark", "0000000004"),
        ("spark", "0000000005"),
    ]

    spark_transient_inns.clear()
    second_exit_code = pipeline.run(
        pipeline.parse_args(
            [
                "--input",
                "input.xlsx",
                "--output-dir",
                str(output_dir),
                "--sources=spark",
                "--retry-deferred-required-sources",
                "--start-from",
                "3",
                "--count",
                "2",
                "--company-concurrency",
                "1",
            ]
        )
    )

    assert second_exit_code == 0
    assert processed_companies == ["Company 1", "Company 3", "Company 5", "Company 4"]
    assert search_calls == [
        ("spark", "0000000001"),
        ("spark", "0000000002"),
        ("spark", "0000000003"),
        ("spark", "0000000004"),
        ("spark", "0000000005"),
        ("spark", "0000000004"),
    ]

    summary = json.loads((output_dir / "summary.json").read_text(encoding="utf-8"))
    assert summary["rows_selected"] == 1
    assert summary["processed_rows"] == 1
    assert summary["completed_rows"] == 4
    assert summary["run_status"] == "completed_with_deferred_required_sources"
    assert summary["required_source_deferred_rows_total"] == 1
    assert summary["unresolved_required_source_rows"] == 1
    deferred_records = summary["deferred_required_sources"]["records"]
    assert deferred_records["0000000002::spark"]["resolution_status"] == "unresolved"
    assert deferred_records["0000000004::spark"]["resolution_status"] == "resolved"


def test_deferred_required_source_without_later_success_fails_closed(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    processed_companies: list[str] = []
    _install_lightweight_run_stubs(monkeypatch, rows=_rows(1), processed_companies=processed_companies)

    class _TimeoutSource(_StaticSource):
        def search(self, row: core.RowInput) -> core.SourceResult:
            result = core.SourceResult(source=self.source_name, status="request_error")
            result.errors.append("Read timeout after finite source retries")
            return result

    monkeypatch.setattr(pipeline, "SparkSource", lambda _client: _TimeoutSource("spark"))

    output_dir = tmp_path / "output"
    exit_code = pipeline.run(
        pipeline.parse_args(
            [
                "--input",
                "input.xlsx",
                "--output-dir",
                str(output_dir),
                "--sources=spark",
                "--defer-required-source-transients",
                "--company-concurrency",
                "1",
            ]
        )
    )

    assert exit_code == 1
    assert processed_companies == []
    summary = json.loads((output_dir / "summary.json").read_text(encoding="utf-8"))
    assert summary["processed_rows"] == 0
    assert summary["completed_rows"] == 0
    assert summary["remaining_rows"] == 1
    assert summary["run_status"] == pipeline.RUN_STATUS_FAILED_REQUIRED_SOURCE
    assert summary["terminal_source"] == "spark"
    assert summary["terminal_source_status"] == "request_error"
    assert summary["required_source_deferred_rows_total"] == 1
    event_types = [
        json.loads(line)["type"]
        for line in (output_dir / "events.jsonl").read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    assert event_types == ["run_started", "required_source_deferred", "required_source_red_flag", "run_finished"]


def test_rusprofile_connect_timeout_without_later_success_fails_closed_and_stays_serial(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    processed_companies: list[str] = []
    _install_lightweight_run_stubs(monkeypatch, rows=_rows(1), processed_companies=processed_companies)

    class _ConnectTimeoutRusprofileSource(_StaticSource):
        def search(self, row: core.RowInput) -> core.SourceResult:
            result = core.SourceResult(source="rusprofile", status="request_error")
            result.errors.append(
                "HTTPSConnectionPool(host='www.rusprofile.ru', port=443): "
                "Max retries exceeded with url: /search?query=7804309200 "
                "(Caused by ConnectTimeoutError("
                "'Connection to www.rusprofile.ru timed out. (connect timeout=18)'))"
            )
            return result

    monkeypatch.setattr(
        pipeline,
        "RusprofileSource",
        lambda _client: _ConnectTimeoutRusprofileSource("rusprofile"),
    )

    output_dir = tmp_path / "output"
    exit_code = pipeline.run(
        pipeline.parse_args(
            [
                "--input",
                "input.xlsx",
                "--output-dir",
                str(output_dir),
                "--sources=rusprofile",
                "--defer-required-source-transients",
                "--company-concurrency",
                "1",
            ]
        )
    )

    assert exit_code == 1
    assert processed_companies == []
    summary = json.loads((output_dir / "summary.json").read_text(encoding="utf-8"))
    assert summary["run_status"] == pipeline.RUN_STATUS_FAILED_REQUIRED_SOURCE
    assert summary["terminal_source"] == "rusprofile"
    assert summary["terminal_source_status"] == "request_error"
    assert summary["required_source_deferred_rows_total"] == 1
    assert summary["unresolved_required_source_rows"] == 1
    record = summary["deferred_required_sources"]["records"]["0000000001::rusprofile"]
    assert record["resolution_status"] == "unresolved"
    assert "ConnectTimeoutError" in record["error"]

    event_types = [
        json.loads(line)["type"]
        for line in (output_dir / "events.jsonl").read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    assert event_types == ["run_started", "required_source_deferred", "required_source_red_flag", "run_finished"]
    assert "required_source_deferred_retry_scheduled" not in event_types


def test_required_source_transient_still_fails_when_deferral_disabled(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    processed_companies: list[str] = []
    _install_lightweight_run_stubs(monkeypatch, rows=_rows(2), processed_companies=processed_companies)

    class _TimeoutSource(_StaticSource):
        def search(self, row: core.RowInput) -> core.SourceResult:
            result = core.SourceResult(source=self.source_name, status="request_error")
            result.errors.append("Read timeout after finite source retries")
            return result

    monkeypatch.setattr(pipeline, "SparkSource", lambda _client: _TimeoutSource("spark"))

    output_dir = tmp_path / "output"
    exit_code = pipeline.run(
        pipeline.parse_args(
            [
                "--input",
                "input.xlsx",
                "--output-dir",
                str(output_dir),
                "--sources=spark",
                "--company-concurrency",
                "1",
            ]
        )
    )

    assert exit_code == 1
    assert processed_companies == []
    summary = json.loads((output_dir / "summary.json").read_text(encoding="utf-8"))
    assert summary["run_status"] == pipeline.RUN_STATUS_FAILED_REQUIRED_SOURCE
    assert summary["required_source_deferred_rows_total"] == 0
    event_types = [
        json.loads(line)["type"]
        for line in (output_dir / "events.jsonl").read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    assert "required_source_deferred" not in event_types
    assert "required_source_red_flag" in event_types


def test_cancelled_run_publishes_terminal_state_and_cleans_up_runtime_resources(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    processed_companies: list[str] = []
    _install_lightweight_run_stubs(
        monkeypatch,
        rows=_rows(2),
        processed_companies=processed_companies,
        materialize_reports=True,
    )
    output_dir = tmp_path / "output"
    cleanup_calls = {"executor": 0, "session": 0}

    class _FakeExecutor:
        def contains(self, row: core.RowInput) -> bool:
            return False

        def ensure_drained(self) -> None:
            return None

        def close(self) -> None:
            cleanup_calls["executor"] += 1

    def fake_rate_limited_http_client(**kwargs):
        def close_session() -> None:
            cleanup_calls["session"] += 1

        return SimpleNamespace(
            progress_store=kwargs["progress_store"],
            session=SimpleNamespace(close=close_session),
        )

    fake_executor = _FakeExecutor()
    monkeypatch.setattr(pipeline.core, "RateLimitedHttpClient", fake_rate_limited_http_client)
    monkeypatch.setattr(
        pipeline,
        "plan_direct_default_bounded_executor",
        lambda **kwargs: DirectDefaultBoundedExecutorPlan(
            enabled=True,
            max_workers=1,
            active_sources=tuple(kwargs["active_sources"]),
        ),
    )
    monkeypatch.setattr(
        pipeline,
        "open_company_source_search_executor",
        lambda **_kwargs: fake_executor,
    )

    original_persist = core.ProgressStore.persist_completed_company_result
    persist_calls = {"count": 0}

    def interrupt_after_first_persist(self, *args, **kwargs):
        result = original_persist(self, *args, **kwargs)
        persist_calls["count"] += 1
        if persist_calls["count"] == 1:
            raise KeyboardInterrupt()
        return result

    monkeypatch.setattr(core.ProgressStore, "persist_completed_company_result", interrupt_after_first_persist)

    with pytest.raises(KeyboardInterrupt):
        pipeline.run(
            pipeline.parse_args(
                [
                    "--input",
                    "input.xlsx",
                    "--output-dir",
                    str(output_dir),
                    "--sources=spark,zachestnyibiznes",
                    "--company-concurrency",
                    "2",
                ]
            )
        )

    assert processed_companies == ["Company 1"]
    assert cleanup_calls == {"executor": 1, "session": 1}

    summary = json.loads((output_dir / "summary.json").read_text(encoding="utf-8"))
    assert summary["processed_rows"] == 1
    assert summary["completed_rows"] == 1
    assert summary["remaining_rows"] == 1
    assert summary["run_status"] == "cancelled"
    assert summary["finish_reason"] == "cancelled"
    assert summary["terminal_checkpoint"] == "explicit_boundary"
    assert summary["terminal_inn"] == "0000000001"
    assert summary["terminal_boundary"] == pipeline.AGGREGATOR_SITE_EXECUTION_BOUNDARY
    assert summary["terminal_error_type"] == "KeyboardInterrupt"
    assert summary["terminal_error_message"] == "Run interrupted by operator"
    assert summary["stop_reason"] == ""
    assert summary["stop_requested_at"] == ""

    runtime_state = json.loads((output_dir / "runtime_state.json").read_text(encoding="utf-8"))
    assert runtime_state["run"]["summary"]["run_status"] == "cancelled"
    assert runtime_state["run"]["metadata"]["run_status"] == "cancelled"
    assert runtime_state["run"]["metadata"]["terminal_error_type"] == "KeyboardInterrupt"
    assert runtime_state["run"]["metadata"]["terminal_boundary"] == (
        pipeline.AGGREGATOR_SITE_EXECUTION_BOUNDARY
    )

    results = json.loads((output_dir / "results.json").read_text(encoding="utf-8"))
    assert [item["company_name"] for item in results] == ["Company 1"]


def test_aborted_run_publishes_terminal_state_without_looking_completed(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    processed_companies: list[str] = []
    _install_lightweight_run_stubs(
        monkeypatch,
        rows=_rows(2),
        processed_companies=processed_companies,
        materialize_reports=True,
    )
    output_dir = tmp_path / "output"
    original_persist = core.ProgressStore.persist_completed_company_result
    persist_calls = {"count": 0}

    def abort_after_first_persist(self, *args, **kwargs):
        result = original_persist(self, *args, **kwargs)
        persist_calls["count"] += 1
        if persist_calls["count"] == 1:
            raise RuntimeError("simulated abort after persist")
        return result

    monkeypatch.setattr(core.ProgressStore, "persist_completed_company_result", abort_after_first_persist)

    with pytest.raises(RuntimeError, match="simulated abort after persist"):
        pipeline.run(
            pipeline.parse_args(
                [
                    "--input",
                    "input.xlsx",
                    "--output-dir",
                    str(output_dir),
                    "--sources=spark,zachestnyibiznes",
                    "--company-concurrency",
                    "1",
                ]
            )
        )

    assert processed_companies == ["Company 1"]

    summary = json.loads((output_dir / "summary.json").read_text(encoding="utf-8"))
    assert summary["processed_rows"] == 1
    assert summary["completed_rows"] == 1
    assert summary["remaining_rows"] == 1
    assert summary["run_status"] == "aborted"
    assert summary["finish_reason"] == "aborted"
    assert summary["terminal_checkpoint"] == "explicit_boundary"
    assert summary["terminal_inn"] == "0000000001"
    assert summary["terminal_boundary"] == pipeline.AGGREGATOR_SITE_EXECUTION_BOUNDARY
    assert summary["terminal_error_type"] == "RuntimeError"
    assert summary["terminal_error_message"] == "simulated abort after persist"
    assert summary["stop_reason"] == ""

    runtime_state = json.loads((output_dir / "runtime_state.json").read_text(encoding="utf-8"))
    assert runtime_state["run"]["summary"]["run_status"] == "aborted"
    assert runtime_state["run"]["metadata"]["finish_reason"] == "aborted"
    assert runtime_state["run"]["metadata"]["terminal_error_message"] == "simulated abort after persist"

    results = json.loads((output_dir / "results.json").read_text(encoding="utf-8"))
    assert [item["company_name"] for item in results] == ["Company 1"]


def test_bounded_executor_close_cancels_governor_blocked_downstream_prepare() -> None:
    rows = _rows(1)
    search_finished = Event()
    prepare_called = {"value": False}
    forwarded_events: list[dict[str, object]] = []

    class _NotifyingSource(_StaticSource):
        def search(self, row: core.RowInput) -> core.SourceResult:
            result = super().search(row)
            search_finished.set()
            return result

    progress_store = SimpleNamespace(
        append_event=lambda event: forwarded_events.append(dict(event)),
        host_memory={},
    )
    shared_client = SimpleNamespace(progress_store=progress_store)
    ledger = HostGovernorLedger(active_poll_seconds=0.01)
    held_hosts = ledger.acquire(["alpha.example"])
    executor = open_company_source_search_executor(
        rows=rows,
        sources=[_NotifyingSource("spark")],
        shared_client=shared_client,
        worker_count=1,
        prepare_downstream=lambda **_kwargs: prepare_called.__setitem__("value", True),
        prepare_downstream_host_resolver=lambda **_kwargs: ("alpha.example",),
        downstream_host_ledger=ledger,
    )
    try:
        assert search_finished.wait(timeout=1)
        executor.close()
    finally:
        ledger.release(held_hosts)

    assert prepare_called["value"] is False
    assert shared_client.progress_store is progress_store
    assert forwarded_events == []


def test_run_overlaps_next_row_source_collect_with_prior_row_downstream_prefetch(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    processed_companies: list[str] = []
    _install_lightweight_run_stubs(
        monkeypatch,
        rows=_rows(2),
        processed_companies=processed_companies,
    )
    source_calls: list[tuple[str, str]] = []
    second_row_checko_started = Event()
    overlap_observed = {"value": False}

    class _RecordingSource(_StaticSource):
        def search(self, row: core.RowInput) -> core.SourceResult:
            source_calls.append((self.source_name, row.company_name))
            if self.source_name == "checko" and row.company_name == "Company 2":
                second_row_checko_started.set()
            return core.SourceResult(source=self.source_name, status="ok")

    monkeypatch.setattr(pipeline, "SparkSource", lambda _client: _RecordingSource("spark"))
    monkeypatch.setattr(pipeline, "ZachestnyBiznesSource", lambda _client: _RecordingSource("zachestnyibiznes"))
    monkeypatch.setattr(pipeline, "RusprofileSource", lambda _client: _RecordingSource("rusprofile"))
    monkeypatch.setattr(pipeline, "CheckoSource", lambda _client: _RecordingSource("checko"))
    monkeypatch.setattr(pipeline, "ListOrgSource", lambda _client, _snapshot: _RecordingSource("list_org"))

    def fake_prepare_prefetched_company_downstream_execution(**kwargs):
        row = kwargs["row"]
        if row.company_name == "Company 1":
            assert second_row_checko_started.wait(timeout=1), (
                "second-row serial source collection did not overlap first-row downstream prefetch"
            )
            overlap_observed["value"] = True
        return pipeline.PrefetchedCompanyDownstreamExecution(
            aggregator_execution=pipeline.PrefetchedAggregatorSiteExecution(
                candidate_site_payloads=(),
                site_gate_decision_payloads=(),
                deep_parse_sites=(),
                gate_notes=(),
                known_contacts={},
            )
        )

    monkeypatch.setattr(
        pipeline,
        "_prepare_prefetched_company_downstream_execution",
        fake_prepare_prefetched_company_downstream_execution,
    )

    output_dir = tmp_path / "output"
    exit_code = pipeline.run(
        pipeline.parse_args(
            [
                "--input",
                "input.xlsx",
                "--output-dir",
                str(output_dir),
                "--sources=spark,zachestnyibiznes,rusprofile,checko,list_org",
                "--company-concurrency",
                "2",
            ]
        )
    )

    assert exit_code == 0
    assert overlap_observed["value"] is True
    assert processed_companies == ["Company 1", "Company 2"]
    assert ("checko", "Company 2") in source_calls


def test_run_continuously_drains_ready_row_before_low_watermark_source_intake(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    monkeypatch.setattr(pipeline, "SOURCE_PREFETCH_SELECTED_SURFACE_QUEUE_MAX", 2)
    processed_companies: list[str] = []
    rows = _rows(17)
    _install_lightweight_run_stubs(
        monkeypatch,
        rows=rows,
        processed_companies=processed_companies,
    )
    source_started_companies: list[str] = []
    first_public_persist_source_count = {"value": 0}

    class _RecordingSource(_StaticSource):
        def search(self, row: core.RowInput) -> core.SourceResult:
            if self.source_name == "checko":
                source_started_companies.append(row.company_name)
            return core.SourceResult(source=self.source_name, status="ok")

    monkeypatch.setattr(pipeline, "SparkSource", lambda _client: _RecordingSource("spark"))
    monkeypatch.setattr(pipeline, "ZachestnyBiznesSource", lambda _client: _RecordingSource("zachestnyibiznes"))
    monkeypatch.setattr(pipeline, "RusprofileSource", lambda _client: _RecordingSource("rusprofile"))
    monkeypatch.setattr(pipeline, "CheckoSource", lambda _client: _RecordingSource("checko"))
    monkeypatch.setattr(pipeline, "ListOrgSource", lambda _client, _snapshot: _RecordingSource("list_org"))

    def fake_store_dossier(*, result: core.CompanyResult, output_dir) -> dict[str, object]:
        if not first_public_persist_source_count["value"]:
            first_public_persist_source_count["value"] = len(source_started_companies)
        processed_companies.append(result.company_name)
        return {}

    def fake_prepare_prefetched_company_downstream_execution(**kwargs):
        return pipeline.PrefetchedCompanyDownstreamExecution(
            aggregator_execution=pipeline.PrefetchedAggregatorSiteExecution(
                candidate_site_payloads=(),
                site_gate_decision_payloads=(),
                deep_parse_sites=(),
                gate_notes=(),
                known_contacts={},
            )
        )

    monkeypatch.setattr(pipeline, "build_and_store_company_dossier", fake_store_dossier)
    monkeypatch.setattr(
        pipeline,
        "_prepare_prefetched_company_downstream_execution",
        fake_prepare_prefetched_company_downstream_execution,
    )

    output_dir = tmp_path / "output"
    exit_code = pipeline.run(
        pipeline.parse_args(
            [
                "--input",
                "input.xlsx",
                "--output-dir",
                str(output_dir),
                "--sources=spark,zachestnyibiznes,rusprofile,checko,list_org",
                "--company-concurrency",
                "2",
            ]
        )
    )

    assert exit_code == 0
    assert source_started_companies == [row.company_name for row in rows]
    assert processed_companies == [row.company_name for row in rows]

    summary = json.loads((output_dir / "summary.json").read_text(encoding="utf-8"))
    downstream_prefetch_policy = summary["throughput_telemetry"]["backpressure_policy"]["downstream_prefetch"]
    assert downstream_prefetch_policy["enabled"] is True
    assert downstream_prefetch_policy["pending_queue_limit"] == len(rows)
    assert downstream_prefetch_policy["continuous_ready_drain_max_rows"] == 1
    assert first_public_persist_source_count["value"] == 1
    assert first_public_persist_source_count["value"] < downstream_prefetch_policy["ready_drain_low_watermark"]
    assert first_public_persist_source_count["value"] < downstream_prefetch_policy["ready_drain_limit"]
    assert first_public_persist_source_count["value"] < len(rows)


def test_source_prefetch_ready_queue_uses_selected_surface_limit_in_runtime_telemetry(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    rows = _rows(6)
    processed_companies: list[str] = []
    _install_lightweight_run_stubs(
        monkeypatch,
        rows=rows,
        processed_companies=processed_companies,
    )

    class _UsableProxyPool:
        def usable_count(self, *args, **kwargs) -> int:
            return 5

    monkeypatch.setattr(pipeline, "ProxyPool", lambda *_args, **_kwargs: _UsableProxyPool())

    output_dir = tmp_path / "output"
    exit_code = pipeline.run(
        pipeline.parse_args(
            [
                "--input",
                "input.xlsx",
                "--output-dir",
                str(output_dir),
                "--sources=spark,zachestnyibiznes,rusprofile,checko,list_org",
                "--company-concurrency",
                "2",
            ]
        )
    )

    assert exit_code == 0
    assert processed_companies == [row.company_name for row in rows]

    summary = json.loads((output_dir / "summary.json").read_text(encoding="utf-8"))
    telemetry = summary["throughput_telemetry"]
    assert telemetry["backpressure_policy"]["source_prefetch"]["ready_queue_limit"] == len(rows)
    assert telemetry["backpressure_policy"]["direct_default_prefetch"]["ready_queue_limit"] == len(rows)
    for source_name in ("spark", "zachestnyibiznes", "checko"):
        backpressure = telemetry["source_lanes"][source_name]["backpressure"]
        assert backpressure["ready_queue_limit"] == len(rows)
        assert backpressure["blocked_submissions"] == 0


def test_downstream_prefetch_workers_follow_downstream_budget_not_source_budget(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    rows = _rows(5)
    processed_companies: list[str] = []
    _install_lightweight_run_stubs(
        monkeypatch,
        rows=rows,
        processed_companies=processed_companies,
    )
    observed_executor_workers: list[int] = []

    def immediate_downstream_submit(**kwargs):
        executor = kwargs["executor"]
        observed_executor_workers.append(int(getattr(executor, "_max_workers", 0) or 0))
        future: Future[pipeline.PrefetchedCompanyDownstreamExecution] = Future()
        future.set_result(
            pipeline.PrefetchedCompanyDownstreamExecution(
                aggregator_execution=pipeline.PrefetchedAggregatorSiteExecution(
                    candidate_site_payloads=(),
                    site_gate_decision_payloads=(),
                    deep_parse_sites=(),
                    gate_notes=(),
                    known_contacts={},
                )
            )
        )
        return future

    monkeypatch.setattr(pipeline, "_submit_prefetched_company_downstream_batch", immediate_downstream_submit)

    output_dir = tmp_path / "output"
    exit_code = pipeline.run(
        pipeline.parse_args(
            [
                "--input",
                "input.xlsx",
                "--output-dir",
                str(output_dir),
                "--sources=spark,zachestnyibiznes,rusprofile,checko,list_org",
                "--company-concurrency",
                "4",
                "--source-concurrency",
                "2",
                "--factory-site-concurrency",
                "4",
                "--deep-parse-concurrency",
                "4",
            ]
        )
    )

    assert exit_code == 0
    assert observed_executor_workers
    assert max(observed_executor_workers) == 4
    summary = json.loads((output_dir / "summary.json").read_text(encoding="utf-8"))
    telemetry = summary["throughput_telemetry"]
    assert telemetry["source_lanes"]["spark"]["worker_lane_budget"] == 2
    assert telemetry["downstream_stage_pools"][pipeline.FACTORY_SITE_STAGE_NAME]["worker_budget"] == 4
    assert telemetry["downstream_stage_pools"][pipeline.DEEP_PARSE_STAGE_NAME]["worker_budget"] == 4
    assert telemetry["backpressure_policy"]["downstream_prefetch"]["worker_count"] == 4
    assert telemetry["backpressure_policy"]["resolved_concurrency"]["source_concurrency"] == 2
    assert processed_companies == [row.company_name for row in rows]


def test_downstream_prefetch_drains_ready_later_row_before_blocked_oldest(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    rows = _rows(3)
    processed_companies: list[str] = []
    output_dir = tmp_path / "output"
    _install_lightweight_run_stubs(
        monkeypatch,
        rows=rows,
        processed_companies=processed_companies,
    )
    release_first_row = Event()
    later_row_published_before_first_release = {"value": False}

    def fake_store_dossier(*, result: core.CompanyResult, output_dir) -> dict[str, object]:
        processed_companies.append(result.company_name)
        return {}

    original_upsert = core.ProgressStore.upsert

    def observe_unordered_publish(self, result: core.CompanyResult, *args, **kwargs) -> None:
        original_upsert(self, result, *args, **kwargs)
        if result.company_name != "Company 2" or release_first_row.is_set():
            return
        published_results = json.loads((output_dir / "results.json").read_text(encoding="utf-8"))
        assert [item["company_name"] for item in published_results] == ["Company 2"]
        assert published_results[0]["inn"] == rows[1].inn
        assert published_results[0]["row_index"] == rows[1].row_index

        runtime_state = json.loads((output_dir / "runtime_state.json").read_text(encoding="utf-8"))
        assert [entry["company"]["inn"] for entry in runtime_state["company_entries"]] == [rows[1].inn]
        work_unit = runtime_state["run"]["metadata"]["stage_work_units"]["aggregator_site"]["companies"][rows[1].inn]
        assert work_unit["work_status"] == "acked"

        later_row_published_before_first_release["value"] = True
        release_first_row.set()

    def fake_prepare_prefetched_company_downstream_execution(**kwargs):
        row = kwargs["row"]
        stage_span_recorder = kwargs.get("stage_span_recorder")
        if callable(stage_span_recorder):
            with stage_span_recorder(stage_name=pipeline.CANDIDATE_SITE_STAGE_NAME):
                pass
        if row.company_name == "Company 1":
            assert release_first_row.wait(timeout=5), "oldest downstream prefetch blocked ready later row drain"
        return pipeline.PrefetchedCompanyDownstreamExecution(
            aggregator_execution=pipeline.PrefetchedAggregatorSiteExecution(
                candidate_site_payloads=(),
                site_gate_decision_payloads=(),
                deep_parse_sites=(),
                gate_notes=(),
                known_contacts={},
            )
        )

    monkeypatch.setattr(pipeline, "build_and_store_company_dossier", fake_store_dossier)
    monkeypatch.setattr(core.ProgressStore, "upsert", observe_unordered_publish)
    monkeypatch.setattr(
        pipeline,
        "_prepare_prefetched_company_downstream_execution",
        fake_prepare_prefetched_company_downstream_execution,
    )

    exit_code = pipeline.run(
        pipeline.parse_args(
            [
                "--input",
                "input.xlsx",
                "--output-dir",
                str(output_dir),
                "--sources=spark,zachestnyibiznes,rusprofile,checko,list_org",
                "--company-concurrency",
                "2",
            ]
        )
    )

    assert exit_code == 0
    assert later_row_published_before_first_release["value"] is True
    assert processed_companies[0] == "Company 2"
    results = json.loads((output_dir / "results.json").read_text(encoding="utf-8"))
    assert [item["company_name"] for item in results] == ["Company 1", "Company 2", "Company 3"]
    assert [item["row_index"] for item in results] == [row.row_index for row in rows]


def test_completion_first_source_handoff_consumes_ready_later_batch_before_waiting_oldest(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    rows = _rows(3)
    processed_companies: list[str] = []
    output_dir = tmp_path / "output"
    _install_lightweight_run_stubs(
        monkeypatch,
        rows=rows,
        processed_companies=processed_companies,
    )
    monkeypatch.setattr(pipeline, "_resolve_downstream_prefetch_queue_limit", lambda *_args, **_kwargs: 1)
    monkeypatch.setattr(pipeline, "_resolve_source_prefetch_ready_queue_limit", lambda *_args, **_kwargs: 1)
    third_row_started = Event()

    class _HeadOfLineSparkSource(_StaticSource):
        def search(self, row: core.RowInput) -> core.SourceResult:
            if row.company_name == "Company 1":
                assert third_row_started.wait(timeout=10), (
                    "ready later source batch was not consumed while oldest source handoff waited"
                )
            if row.company_name == "Company 3":
                third_row_started.set()
            return core.SourceResult(source="spark", status="ok")

    monkeypatch.setattr(pipeline, "SparkSource", lambda _client: _HeadOfLineSparkSource("spark"))

    exit_code = pipeline.run(
        pipeline.parse_args(
            [
                "--input",
                "input.xlsx",
                "--output-dir",
                str(output_dir),
                "--sources=spark",
                "--company-concurrency",
                "2",
            ]
        )
    )

    assert exit_code == 0
    assert processed_companies == [row.company_name for row in rows]
    results = json.loads((output_dir / "results.json").read_text(encoding="utf-8"))
    assert [item["company_name"] for item in results] == [row.company_name for row in rows]
    assert [item["row_index"] for item in results] == [row.row_index for row in rows]

    summary = json.loads((output_dir / "summary.json").read_text(encoding="utf-8"))
    assert summary["throughput_telemetry"]["backpressure_policy"]["direct_default_prefetch"][
        "ready_queue_limit"
    ] == 1


def test_downstream_prefetch_continuous_drain_materializes_ready_rows_below_low_watermark(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    rows = _rows(6)
    processed_companies: list[str] = []
    monkeypatch.setattr(pipeline, "DOWNSTREAM_PREFETCH_PENDING_QUEUE_MAX", 4)
    _install_lightweight_run_stubs(
        monkeypatch,
        rows=rows,
        processed_companies=processed_companies,
    )
    source_started_companies: list[str] = []
    first_public_persist_source_count = {"value": 0}
    public_persist_source_counts: list[int] = []

    class _RecordingSource(_StaticSource):
        def search(self, row: core.RowInput) -> core.SourceResult:
            if self.source_name == "rusprofile":
                source_started_companies.append(row.company_name)
            return core.SourceResult(source=self.source_name, status="ok")

    def fake_store_dossier(*, result: core.CompanyResult, output_dir) -> dict[str, object]:
        public_persist_source_counts.append(len(source_started_companies))
        if not first_public_persist_source_count["value"]:
            first_public_persist_source_count["value"] = len(source_started_companies)
        processed_companies.append(result.company_name)
        return {}

    def immediate_downstream_submit(**_kwargs):
        future: Future[pipeline.PrefetchedCompanyDownstreamExecution] = Future()
        future.set_result(
            pipeline.PrefetchedCompanyDownstreamExecution(
                aggregator_execution=pipeline.PrefetchedAggregatorSiteExecution(
                    candidate_site_payloads=(),
                    site_gate_decision_payloads=(),
                    deep_parse_sites=(),
                    gate_notes=(),
                    known_contacts={},
                )
            )
        )
        return future

    monkeypatch.setattr(pipeline, "SparkSource", lambda _client: _RecordingSource("spark"))
    monkeypatch.setattr(pipeline, "ZachestnyBiznesSource", lambda _client: _RecordingSource("zachestnyibiznes"))
    monkeypatch.setattr(pipeline, "RusprofileSource", lambda _client: _RecordingSource("rusprofile"))
    monkeypatch.setattr(pipeline, "CheckoSource", lambda _client: _RecordingSource("checko"))
    monkeypatch.setattr(pipeline, "ListOrgSource", lambda _client, _snapshot: _RecordingSource("list_org"))
    monkeypatch.setattr(pipeline, "build_and_store_company_dossier", fake_store_dossier)
    monkeypatch.setattr(pipeline, "_submit_prefetched_company_downstream_batch", immediate_downstream_submit)

    output_dir = tmp_path / "output"
    exit_code = pipeline.run(
        pipeline.parse_args(
            [
                "--input",
                "input.xlsx",
                "--output-dir",
                str(output_dir),
                "--sources=spark,zachestnyibiznes,rusprofile,checko,list_org",
                "--company-concurrency",
                "2",
            ]
        )
    )

    assert exit_code == 0
    pending_queue_limit = pipeline._resolve_downstream_prefetch_queue_limit(
        2,
        selected_rows_count=len(rows),
    )
    ready_drain_limit = pipeline._resolve_downstream_prefetch_ready_drain_limit(
        2,
        pending_queue_limit=pending_queue_limit,
    )
    ready_drain_low_watermark = pipeline._resolve_downstream_prefetch_ready_drain_low_watermark(
        ready_drain_limit=ready_drain_limit,
    )
    assert pending_queue_limit == len(rows)
    assert ready_drain_low_watermark == ready_drain_limit // 2
    assert first_public_persist_source_count["value"] == 1
    assert first_public_persist_source_count["value"] < ready_drain_low_watermark
    assert first_public_persist_source_count["value"] < len(rows)
    assert first_public_persist_source_count["value"] < ready_drain_limit
    assert public_persist_source_counts[:3] == [1, 2, 3]
    assert processed_companies == [row.company_name for row in rows]
    results = json.loads((output_dir / "results.json").read_text(encoding="utf-8"))
    assert [item["company_name"] for item in results] == [row.company_name for row in rows]


def test_downstream_prefetch_post_materialized_continuous_drain_repeats_below_low_watermark(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    rows = _rows(8)
    processed_companies: list[str] = []
    monkeypatch.setattr(pipeline, "DOWNSTREAM_PREFETCH_PENDING_QUEUE_MAX", 4)
    _install_lightweight_run_stubs(
        monkeypatch,
        rows=rows,
        processed_companies=processed_companies,
    )
    source_started_companies: list[str] = []
    public_persist_source_counts: list[int] = []

    class _RecordingSource(_StaticSource):
        def search(self, row: core.RowInput) -> core.SourceResult:
            if self.source_name == "rusprofile":
                source_started_companies.append(row.company_name)
            return core.SourceResult(source=self.source_name, status="ok")

    def fake_store_dossier(*, result: core.CompanyResult, output_dir) -> dict[str, object]:
        public_persist_source_counts.append(len(source_started_companies))
        processed_companies.append(result.company_name)
        return {}

    def immediate_downstream_submit(**_kwargs):
        future: Future[pipeline.PrefetchedCompanyDownstreamExecution] = Future()
        future.set_result(
            pipeline.PrefetchedCompanyDownstreamExecution(
                aggregator_execution=pipeline.PrefetchedAggregatorSiteExecution(
                    candidate_site_payloads=(),
                    site_gate_decision_payloads=(),
                    deep_parse_sites=(),
                    gate_notes=(),
                    known_contacts={},
                )
            )
        )
        return future

    monkeypatch.setattr(pipeline, "SparkSource", lambda _client: _RecordingSource("spark"))
    monkeypatch.setattr(pipeline, "ZachestnyBiznesSource", lambda _client: _RecordingSource("zachestnyibiznes"))
    monkeypatch.setattr(pipeline, "RusprofileSource", lambda _client: _RecordingSource("rusprofile"))
    monkeypatch.setattr(pipeline, "CheckoSource", lambda _client: _RecordingSource("checko"))
    monkeypatch.setattr(pipeline, "ListOrgSource", lambda _client, _snapshot: _RecordingSource("list_org"))
    monkeypatch.setattr(pipeline, "build_and_store_company_dossier", fake_store_dossier)
    monkeypatch.setattr(pipeline, "_submit_prefetched_company_downstream_batch", immediate_downstream_submit)

    output_dir = tmp_path / "output"
    exit_code = pipeline.run(
        pipeline.parse_args(
            [
                "--input",
                "input.xlsx",
                "--output-dir",
                str(output_dir),
                "--sources=spark,zachestnyibiznes,rusprofile,checko,list_org",
                "--company-concurrency",
                "2",
            ]
        )
    )

    assert exit_code == 0
    pending_queue_limit = pipeline._resolve_downstream_prefetch_queue_limit(
        2,
        selected_rows_count=len(rows),
    )
    ready_drain_limit = pipeline._resolve_downstream_prefetch_ready_drain_limit(
        2,
        pending_queue_limit=pending_queue_limit,
    )
    ready_drain_low_watermark = pipeline._resolve_downstream_prefetch_ready_drain_low_watermark(
        ready_drain_limit=ready_drain_limit,
    )
    assert pending_queue_limit == len(rows)
    assert ready_drain_low_watermark == ready_drain_limit // 2
    assert public_persist_source_counts[:5] == [1, 2, 3, 4, 5]
    assert public_persist_source_counts[0] < ready_drain_low_watermark
    assert processed_companies == [row.company_name for row in rows]
    results = json.loads((output_dir / "results.json").read_text(encoding="utf-8"))
    assert [item["company_name"] for item in results] == [row.company_name for row in rows]


def test_downstream_prefetch_source_intake_wait_drains_ready_rows(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    rows = _rows(5)
    processed_companies: list[str] = []
    monkeypatch.setattr(pipeline, "CONTINUOUS_READY_DRAIN_MAX_ROWS", 0)
    monkeypatch.setattr(pipeline, "DOWNSTREAM_PREFETCH_PENDING_QUEUE_MAX", 8)
    _install_lightweight_run_stubs(
        monkeypatch,
        rows=rows,
        processed_companies=processed_companies,
    )
    fourth_row_source_waiting = Event()
    fourth_row_source_finished = Event()
    first_ready_row_persisted = Event()
    ready_persisted_during_source_wait = {"value": False}

    class _SourceIntakeWaitSource(_StaticSource):
        def search(self, row: core.RowInput) -> core.SourceResult:
            if self.source_name == "spark" and row.company_name == "Company 4":
                fourth_row_source_waiting.set()
                assert first_ready_row_persisted.wait(timeout=10), (
                    "ready downstream row waited behind source-intake handoff"
                )
                fourth_row_source_finished.set()
            return core.SourceResult(source=self.source_name, status="ok")

    def fake_store_dossier(*, result: core.CompanyResult, output_dir) -> dict[str, object]:
        if not fourth_row_source_finished.is_set():
            assert fourth_row_source_waiting.wait(timeout=10), (
                "source handoff wait was not reached before ready-row drain"
            )
            ready_persisted_during_source_wait["value"] = True
            first_ready_row_persisted.set()
        processed_companies.append(result.company_name)
        return {}

    def immediate_downstream_submit(**_kwargs):
        future: Future[pipeline.PrefetchedCompanyDownstreamExecution] = Future()
        future.set_result(
            pipeline.PrefetchedCompanyDownstreamExecution(
                aggregator_execution=pipeline.PrefetchedAggregatorSiteExecution(
                    candidate_site_payloads=(),
                    site_gate_decision_payloads=(),
                    deep_parse_sites=(),
                    gate_notes=(),
                    known_contacts={},
                )
            )
        )
        return future

    monkeypatch.setattr(pipeline, "SparkSource", lambda _client: _SourceIntakeWaitSource("spark"))
    monkeypatch.setattr(pipeline, "ZachestnyBiznesSource", lambda _client: _SourceIntakeWaitSource("zachestnyibiznes"))
    monkeypatch.setattr(pipeline, "RusprofileSource", lambda _client: _SourceIntakeWaitSource("rusprofile"))
    monkeypatch.setattr(pipeline, "CheckoSource", lambda _client: _SourceIntakeWaitSource("checko"))
    monkeypatch.setattr(pipeline, "ListOrgSource", lambda _client, _snapshot: _SourceIntakeWaitSource("list_org"))
    monkeypatch.setattr(pipeline, "build_and_store_company_dossier", fake_store_dossier)
    monkeypatch.setattr(pipeline, "_submit_prefetched_company_downstream_batch", immediate_downstream_submit)

    output_dir = tmp_path / "output"
    exit_code = pipeline.run(
        pipeline.parse_args(
            [
                "--input",
                "input.xlsx",
                "--output-dir",
                str(output_dir),
                "--sources=spark,zachestnyibiznes,rusprofile,checko,list_org",
                "--company-concurrency",
                "2",
            ]
        )
    )

    assert exit_code == 0
    assert ready_persisted_during_source_wait["value"] is True
    assert processed_companies == [row.company_name for row in rows]
    results = json.loads((output_dir / "results.json").read_text(encoding="utf-8"))
    assert [item["company_name"] for item in results] == [row.company_name for row in rows]


def test_downstream_prefetch_stale_ready_tail_drains_before_inline_source_wait(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    rows = _rows(2)
    processed_companies: list[str] = []
    monkeypatch.setattr(pipeline, "CONTINUOUS_READY_DRAIN_MAX_ROWS", 0)
    monkeypatch.setattr(pipeline, "READY_IDLE_SOURCE_WAIT_DRAIN_SECONDS", 0.0)
    _install_lightweight_run_stubs(
        monkeypatch,
        rows=rows,
        processed_companies=processed_companies,
    )
    second_row_inline_source_entered = Event()
    first_ready_persisted_before_inline_source = {"value": False}

    class _RecordingSource(_StaticSource):
        def search(self, row: core.RowInput) -> core.SourceResult:
            if self.source_name == "rusprofile" and row.company_name == "Company 2":
                second_row_inline_source_entered.set()
            return core.SourceResult(source=self.source_name, status="ok")

    def fake_store_dossier(*, result: core.CompanyResult, output_dir) -> dict[str, object]:
        if result.company_name == "Company 1" and not second_row_inline_source_entered.is_set():
            first_ready_persisted_before_inline_source["value"] = True
        processed_companies.append(result.company_name)
        return {}

    def immediate_downstream_submit(**_kwargs):
        future: Future[pipeline.PrefetchedCompanyDownstreamExecution] = Future()
        future.set_result(
            pipeline.PrefetchedCompanyDownstreamExecution(
                aggregator_execution=pipeline.PrefetchedAggregatorSiteExecution(
                    candidate_site_payloads=(),
                    site_gate_decision_payloads=(),
                    deep_parse_sites=(),
                    gate_notes=(),
                    known_contacts={},
                )
            )
        )
        return future

    monkeypatch.setattr(pipeline, "SparkSource", lambda _client: _RecordingSource("spark"))
    monkeypatch.setattr(pipeline, "ZachestnyBiznesSource", lambda _client: _RecordingSource("zachestnyibiznes"))
    monkeypatch.setattr(pipeline, "RusprofileSource", lambda _client: _RecordingSource("rusprofile"))
    monkeypatch.setattr(pipeline, "CheckoSource", lambda _client: _RecordingSource("checko"))
    monkeypatch.setattr(pipeline, "ListOrgSource", lambda _client, _snapshot: _RecordingSource("list_org"))
    monkeypatch.setattr(pipeline, "build_and_store_company_dossier", fake_store_dossier)
    monkeypatch.setattr(pipeline, "_submit_prefetched_company_downstream_batch", immediate_downstream_submit)

    output_dir = tmp_path / "output"
    exit_code = pipeline.run(
        pipeline.parse_args(
            [
                "--input",
                "input.xlsx",
                "--output-dir",
                str(output_dir),
                "--sources=spark,zachestnyibiznes,rusprofile,checko,list_org",
                "--company-concurrency",
                "2",
            ]
        )
    )

    assert exit_code == 0
    assert first_ready_persisted_before_inline_source["value"] is True
    assert processed_companies == [row.company_name for row in rows]
    results = json.loads((output_dir / "results.json").read_text(encoding="utf-8"))
    assert [item["company_name"] for item in results] == [row.company_name for row in rows]

    summary = json.loads((output_dir / "summary.json").read_text(encoding="utf-8"))
    downstream_prefetch_policy = summary["throughput_telemetry"]["backpressure_policy"]["downstream_prefetch"]
    assert downstream_prefetch_policy["ready_idle_source_wait_drain_seconds"] == 0.0
    assert downstream_prefetch_policy["ready_idle_source_wait_drain_max_rows"] == 1
    assert downstream_prefetch_policy["ready_drain_limit"] == 16
    assert downstream_prefetch_policy["ready_drain_low_watermark"] == 8


def test_downstream_prefetch_stale_ready_tail_drains_bounded_slice_before_inline_source_wait(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    rows = _rows(6)
    processed_companies: list[str] = []
    monkeypatch.setattr(pipeline, "CONTINUOUS_READY_DRAIN_MAX_ROWS", 0)
    monkeypatch.setattr(pipeline, "DOWNSTREAM_PREFETCH_PENDING_QUEUE_MAX", 6)
    monkeypatch.setattr(pipeline, "READY_IDLE_SOURCE_WAIT_DRAIN_SECONDS", 120.0)
    monkeypatch.setattr(pipeline, "READY_IDLE_SOURCE_WAIT_DRAIN_MAX_ROWS", 1)
    _install_lightweight_run_stubs(
        monkeypatch,
        rows=rows,
        processed_companies=processed_companies,
    )
    stale_probe_calls = {"count": 0}
    persisted_before_fourth_row_inline_source = {"count": -1}

    def stale_probe_after_three_ready_rows(*_args, **_kwargs) -> float:
        stale_probe_calls["count"] += 1
        return 999.0 if stale_probe_calls["count"] == 5 else 0.0

    class _RecordingSource(_StaticSource):
        def search(self, row: core.RowInput) -> core.SourceResult:
            if self.source_name == "rusprofile" and row.company_name == "Company 4":
                persisted_before_fourth_row_inline_source["count"] = len(processed_companies)
            return core.SourceResult(source=self.source_name, status="ok")

    def fake_store_dossier(*, result: core.CompanyResult, output_dir) -> dict[str, object]:
        processed_companies.append(result.company_name)
        return {}

    def immediate_downstream_submit(**_kwargs):
        future: Future[pipeline.PrefetchedCompanyDownstreamExecution] = Future()
        future.set_result(
            pipeline.PrefetchedCompanyDownstreamExecution(
                aggregator_execution=pipeline.PrefetchedAggregatorSiteExecution(
                    candidate_site_payloads=(),
                    site_gate_decision_payloads=(),
                    deep_parse_sites=(),
                    gate_notes=(),
                    known_contacts={},
                )
            )
        )
        return future

    monkeypatch.setattr(
        pipeline,
        "_oldest_ready_pending_downstream_idle_seconds",
        stale_probe_after_three_ready_rows,
    )
    monkeypatch.setattr(pipeline, "SparkSource", lambda _client: _RecordingSource("spark"))
    monkeypatch.setattr(pipeline, "ZachestnyBiznesSource", lambda _client: _RecordingSource("zachestnyibiznes"))
    monkeypatch.setattr(pipeline, "RusprofileSource", lambda _client: _RecordingSource("rusprofile"))
    monkeypatch.setattr(pipeline, "CheckoSource", lambda _client: _RecordingSource("checko"))
    monkeypatch.setattr(pipeline, "ListOrgSource", lambda _client, _snapshot: _RecordingSource("list_org"))
    monkeypatch.setattr(pipeline, "build_and_store_company_dossier", fake_store_dossier)
    monkeypatch.setattr(pipeline, "_submit_prefetched_company_downstream_batch", immediate_downstream_submit)

    output_dir = tmp_path / "output"
    exit_code = pipeline.run(
        pipeline.parse_args(
            [
                "--input",
                "input.xlsx",
                "--output-dir",
                str(output_dir),
                "--sources=spark,zachestnyibiznes,rusprofile,checko,list_org",
                "--company-concurrency",
                "2",
            ]
        )
    )

    assert exit_code == 0
    assert persisted_before_fourth_row_inline_source["count"] == 1
    assert processed_companies == [row.company_name for row in rows]


def test_downstream_prefetch_queue_limit_scales_to_selected_surface() -> None:
    assert pipeline._resolve_downstream_prefetch_queue_limit(2) == 16
    assert pipeline._resolve_downstream_prefetch_queue_limit(2, selected_rows_count=100) == 100
    assert pipeline._resolve_downstream_prefetch_queue_limit(2, selected_rows_count=865) == (
        pipeline.DOWNSTREAM_PREFETCH_SELECTED_SURFACE_QUEUE_MAX
    )
    assert pipeline._resolve_downstream_prefetch_ready_drain_limit(2, pending_queue_limit=16) == 16
    assert pipeline._resolve_downstream_prefetch_ready_drain_limit(2, pending_queue_limit=100) == 32
    assert pipeline._resolve_downstream_prefetch_ready_drain_low_watermark(
        ready_drain_limit=32,
    ) == 16


def test_wallclock_telemetry_exposes_active_clock_and_downstream_drain(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    processed_companies: list[str] = []
    rows = _rows(1)
    _install_lightweight_run_stubs(monkeypatch, rows=rows, processed_companies=processed_companies)

    output_dir = tmp_path / "output"
    exit_code = pipeline.run(
        pipeline.parse_args(
            [
                "--input",
                "input.xlsx",
                "--output-dir",
                str(output_dir),
                "--sources=spark,zachestnyibiznes",
            ]
        )
    )

    assert exit_code == 0
    summary = json.loads((output_dir / "summary.json").read_text(encoding="utf-8"))
    telemetry = summary["throughput_telemetry"]
    runtime_clock = telemetry["runtime_clock"]
    assert runtime_clock["wall_clock_elapsed_seconds"] >= runtime_clock["active_elapsed_seconds"]
    assert runtime_clock["external_pause_seconds"] <= runtime_clock["gap_threshold_seconds"]
    downstream_drain = telemetry["downstream_drain"]
    company = downstream_drain["companies"][rows[0].inn]
    assert pipeline.CANDIDATE_SITE_STAGE_NAME in company["stages"]
    assert company["stages"][pipeline.CANDIDATE_SITE_STAGE_NAME]["span_count"] >= 1
    assert company["final_ack"]["acknowledged_at"] != ""

import errno
import json
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from threading import Barrier, Lock
from types import SimpleNamespace

import pytest

import company_enrichment_core as core
import run_company_enrichment_pipeline as pipeline


def _patch_host_stats_replace_failure(
    monkeypatch: pytest.MonkeyPatch,
    path_type: type[Path],
    error_factory,
) -> None:
    original_replace = path_type.replace

    def failing_replace(self: Path, target: Path) -> Path:
        target_path = path_type(target)
        if (
            self.name.startswith("host_stats.json.")
            and self.name.endswith(".tmp")
            and target_path.name == "host_stats.json"
        ):
            raise error_factory(target_path)
        return original_replace(self, target)

    monkeypatch.setattr(path_type, "replace", failing_replace)


def _host_event(source_name: str = "spark") -> dict[str, object]:
    return {
        "ts": core.utc_now_iso(),
        "type": "request_ok",
        "host": "spark-interfax.ru",
        "source": source_name,
        "elapsed_seconds": 0.25,
        "since_previous_request_seconds": 1.0,
    }


class _EventWritingSource:
    def __init__(self, source_name: str, progress_store: core.ProgressStore) -> None:
        self.source_name = source_name
        self.progress_store = progress_store

    def search(self, row: core.RowInput) -> core.SourceResult:
        self.progress_store.append_event(_host_event(self.source_name))
        return core.SourceResult(source=self.source_name, status="success")


def test_core_progress_store_reexport_instantiates(tmp_path: Path) -> None:
    progress = core.ProgressStore(tmp_path / "progress")

    assert progress.output_dir == tmp_path / "progress"
    assert progress.runtime_state_json.name == "runtime_state.json"
    assert progress.results_json.name == "results.json"


def _rows(count: int) -> list[core.RowInput]:
    return [
        core.RowInput(
            row_index=index + 1,
            inn=f"{index:010d}",
            company_name=f"Company {index}",
        )
        for index in range(1, count + 1)
    ]


def _install_lightweight_run_stubs(
    monkeypatch: pytest.MonkeyPatch,
    *,
    rows: list[core.RowInput],
) -> None:
    def fake_rate_limited_http_client(**kwargs):
        return SimpleNamespace(progress_store=kwargs["progress_store"], min_delay_by_host=kwargs["min_delay_by_host"])

    def make_source(source_name: str):
        return lambda client: _EventWritingSource(source_name, client.progress_store)

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
    monkeypatch.setattr(pipeline, "SparkSource", make_source("spark"))
    monkeypatch.setattr(pipeline, "ZachestnyBiznesSource", make_source("zachestnyibiznes"))
    monkeypatch.setattr(pipeline, "RusprofileSource", make_source("rusprofile"))
    monkeypatch.setattr(pipeline, "CheckoSource", make_source("checko"))
    monkeypatch.setattr(pipeline, "ListOrgSource", lambda client, _snapshot: _EventWritingSource("list_org", client.progress_store))
    monkeypatch.setattr(pipeline, "FactorySiteParser", lambda *_args, **_kwargs: SimpleNamespace())
    monkeypatch.setattr(pipeline, "SiteAuthHelpers", lambda **_kwargs: SimpleNamespace())
    monkeypatch.setattr(
        pipeline,
        "BenchmarkAwareSiteAuthenticityAnalyzer",
        lambda *_args, **_kwargs: SimpleNamespace(llm=SimpleNamespace()),
    )
    monkeypatch.setattr(pipeline, "build_domain_resolution", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(pipeline, "choose_candidate_sites", lambda *_args, **_kwargs: [])
    monkeypatch.setattr(pipeline, "run_gated_factory_site_parse", fake_gated_parse)
    monkeypatch.setattr(pipeline, "classify_content_record", lambda _record: None)
    monkeypatch.setattr(pipeline, "should_use_llm_record_review", lambda _record: False)
    monkeypatch.setattr(pipeline, "build_and_store_company_dossier", lambda **_kwargs: {})
    monkeypatch.setattr(pipeline.core, "build_analysis_contacts", lambda *_args, **_kwargs: {})
    monkeypatch.setattr(pipeline.core, "merge_contacts", lambda *_args, **_kwargs: {})
    monkeypatch.setattr(pipeline.core, "build_trusted_contacts", lambda *_args, **_kwargs: {})
    monkeypatch.setattr(pipeline.core, "build_lead_cards", lambda *_args, **_kwargs: [])
    monkeypatch.setattr(pipeline.core, "build_site_refresh_plans", lambda *_args, **_kwargs: [])


def _seed_runtime_artifacts(
    monkeypatch: pytest.MonkeyPatch,
    output_dir: Path,
) -> tuple[core.ProgressStore, core.RowInput]:
    monkeypatch.setattr(core.ProgressStore, "_write_markdown_reports", lambda self, ordered_results=None: None)
    progress = core.ProgressStore(output_dir)
    row = core.RowInput(row_index=1, inn="7700000001", company_name="Factory 1")
    progress.run_started(
        input_path="input.xlsx",
        total_rows=1,
        selected_rows=1,
        selection_mode="window",
        selected_ordinals=[1],
        start_from=1,
        end_at=1,
        active_sources=["spark"],
        resume_skipped_rows=0,
    )
    result = core.build_company_result(row)
    result.status = "completed"
    result.finished_at = core.utc_now_iso()
    progress.persist_completed_company_result(result, total_rows=1, processed_rows=1)
    progress.append_event(_host_event())
    return progress, row


def test_append_event_continues_after_nonfatal_host_stats_replace_error(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
) -> None:
    _patch_host_stats_replace_failure(
        monkeypatch,
        type(tmp_path),
        lambda target: OSError(errno.EACCES, "sharing violation", str(target)),
    )
    progress = core.ProgressStore(tmp_path / "progress")

    with caplog.at_level("WARNING", logger="company_research_parser"):
        progress.append_event(_host_event())

    assert progress.host_stats["spark-interfax.ru"]["total_events"] == 1
    assert json.loads(progress.events_jsonl.read_text(encoding="utf-8"))["host"] == "spark-interfax.ru"
    runtime_state = json.loads(progress.runtime_state_json.read_text(encoding="utf-8"))
    assert runtime_state["run"]["host_stats"]["spark-interfax.ru"]["total_events"] == 1
    assert not progress.host_stats_json.exists()
    assert not any(progress.output_dir.glob("host_stats.json*.tmp"))
    assert any("Non-fatal host_stats persistence failure; continuing run" in record.message for record in caplog.records)


def test_update_throughput_telemetry_survives_overlapping_atomic_tmp_writes(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    progress = core.ProgressStore(tmp_path / "progress")
    original_write_text = type(tmp_path).write_text
    first_overlap_barrier = Barrier(2)
    active_paths: set[str] = set()
    state_lock = Lock()
    tmp_write_calls = {"count": 0}

    def collision_if_same_tmp_path(self: Path, data: str, *args, **kwargs):
        if self.name.endswith(".tmp"):
            with state_lock:
                tmp_write_calls["count"] += 1
                use_overlap_barrier = tmp_write_calls["count"] <= 2
            if use_overlap_barrier:
                first_overlap_barrier.wait(timeout=1)
            path_key = str(self)
            with state_lock:
                if path_key in active_paths:
                    raise PermissionError(errno.EACCES, "sharing violation", path_key)
                active_paths.add(path_key)
            try:
                time.sleep(0.05)
                return original_write_text(self, data, *args, **kwargs)
            finally:
                with state_lock:
                    active_paths.discard(path_key)
        return original_write_text(self, data, *args, **kwargs)

    monkeypatch.setattr(type(tmp_path), "write_text", collision_if_same_tmp_path)

    payloads = (
        {"source_lanes": {"spark": {"completed": 1}}},
        {"source_lanes": {"spark": {"completed": 2}}},
    )
    with ThreadPoolExecutor(max_workers=2) as executor:
        list(executor.map(progress.update_throughput_telemetry, payloads))

    summary = json.loads(progress.summary_json.read_text(encoding="utf-8"))
    telemetry = summary["throughput_telemetry"]["source_lanes"]["spark"]["completed"]

    assert telemetry in {1, 2}
    assert not any(progress.output_dir.glob("*.tmp"))


def test_pipeline_run_continues_when_host_stats_replace_raises_permission_error(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
) -> None:
    _install_lightweight_run_stubs(monkeypatch, rows=_rows(1))
    _patch_host_stats_replace_failure(
        monkeypatch,
        type(tmp_path),
        lambda target: PermissionError(errno.EACCES, "Access is denied", str(target)),
    )
    output_dir = tmp_path / "output"

    with caplog.at_level("WARNING", logger="company_research_parser"):
        exit_code = pipeline.run(
            pipeline.parse_args(
                [
                    "--input",
                    "input.xlsx",
                    "--output-dir",
                    str(output_dir),
                    "--sources=spark",
                ]
            )
        )

    assert exit_code == 0
    assert any("Non-fatal host_stats persistence failure; continuing run" in record.message for record in caplog.records)
    assert json.loads((output_dir / "summary.json").read_text(encoding="utf-8"))["completed_rows"] == 1
    results = json.loads((output_dir / "results.json").read_text(encoding="utf-8"))
    runtime_state = json.loads((output_dir / "runtime_state.json").read_text(encoding="utf-8"))
    assert [item["inn"] for item in results] == ["0000000001"]
    assert runtime_state["run"]["summary"]["completed_rows"] == 1
    assert runtime_state["run"]["host_stats"]["spark-interfax.ru"]["total_events"] == 1
    assert not (output_dir / "host_stats.json").exists()


def test_progress_store_falls_back_to_legacy_when_runtime_state_is_unreadable(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
) -> None:
    output_dir = tmp_path / "progress"
    _, row = _seed_runtime_artifacts(monkeypatch, output_dir)
    runtime_state_path = output_dir / "runtime_state.json"
    path_type = type(tmp_path)
    original_read_text = path_type.read_text

    def failing_read_text(self: Path, *args, **kwargs) -> str:
        if self == runtime_state_path:
            raise OSError(errno.EACCES, "runtime state locked", str(self))
        return original_read_text(self, *args, **kwargs)

    with monkeypatch.context() as runtime_state_ctx:
        runtime_state_ctx.setattr(path_type, "read_text", failing_read_text)
        with caplog.at_level("WARNING", logger="company_research_parser"):
            reloaded = core.ProgressStore(output_dir)

    assert reloaded.get(row.inn) is not None
    assert any("falling back to legacy artifacts" in record.message for record in caplog.records)
    repaired_runtime_state = json.loads(runtime_state_path.read_text(encoding="utf-8"))
    assert repaired_runtime_state["runtime_state_contract_version"] == 2
    assert repaired_runtime_state["run"]["summary"]["completed_rows"] == 1
    assert repaired_runtime_state["run"]["host_stats"]["spark-interfax.ru"]["total_events"] == 1


@pytest.mark.parametrize(
    ("runtime_state_payload", "expected_reason"),
    [
        ("{not-json", "malformed_json"),
        (
            json.dumps(
                {
                    "runtime_state_contract_version": 999,
                    "companies": [],
                    "summary": {},
                    "host_stats": {},
                }
            ),
            "incompatible_contract_version",
        ),
    ],
)
def test_progress_store_falls_back_to_legacy_when_runtime_state_is_invalid(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
    runtime_state_payload: str,
    expected_reason: str,
) -> None:
    output_dir = tmp_path / "progress"
    _, row = _seed_runtime_artifacts(monkeypatch, output_dir)
    runtime_state_path = output_dir / "runtime_state.json"
    runtime_state_path.write_text(runtime_state_payload, encoding="utf-8")

    with caplog.at_level("WARNING", logger="company_research_parser"):
        reloaded = core.ProgressStore(output_dir)

    assert reloaded.get(row.inn) is not None
    assert any(expected_reason in record.message for record in caplog.records)
    assert json.loads((output_dir / "results.json").read_text(encoding="utf-8"))[0]["inn"] == row.inn
    assert json.loads(runtime_state_path.read_text(encoding="utf-8"))["run"]["summary"]["completed_rows"] == 1


def test_progress_store_prefers_canonical_runtime_state_over_conflicting_legacy_artifacts(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    output_dir = tmp_path / "progress"
    _, row = _seed_runtime_artifacts(monkeypatch, output_dir)

    (output_dir / "results.json").write_text(
        json.dumps([{"inn": "0000009999", "company_name": "Legacy Only", "row_index": 9}], ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    (output_dir / "summary.json").write_text(
        json.dumps({"completed_rows": 999, "processed_rows": 999}, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    (output_dir / "host_stats.json").write_text(
        json.dumps({"legacy.example": {"total_events": 9}}, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    reloaded = core.ProgressStore(output_dir)

    assert reloaded.get(row.inn) is not None
    assert reloaded.get("0000009999") is None
    assert json.loads((output_dir / "results.json").read_text(encoding="utf-8"))[0]["inn"] == row.inn
    assert json.loads((output_dir / "summary.json").read_text(encoding="utf-8"))["completed_rows"] == 1
    assert json.loads((output_dir / "host_stats.json").read_text(encoding="utf-8"))["spark-interfax.ru"]["total_events"] == 1


def test_progress_store_rebuilds_derived_outputs_from_canonical_runtime_state(tmp_path: Path) -> None:
    output_dir = tmp_path / "progress"
    progress = core.ProgressStore(output_dir)
    row = core.RowInput(row_index=1, inn="7700000001", company_name="Factory 1")
    progress.run_started(
        input_path="input.xlsx",
        total_rows=1,
        selected_rows=1,
        selection_mode="window",
        selected_ordinals=[1],
        start_from=1,
        end_at=1,
        active_sources=["spark"],
    )
    result = core.build_company_result(row)
    result.status = "completed"
    result.finished_at = core.utc_now_iso()
    progress.persist_completed_company_result(result, total_rows=1, processed_rows=1)
    progress.append_event(_host_event())

    runtime_state = json.loads(progress.runtime_state_json.read_text(encoding="utf-8"))
    assert runtime_state["runtime_state_contract_version"] == 2
    assert runtime_state["run"]["summary"]["completed_rows"] == 1
    assert runtime_state["run"]["metadata"]["rows_selected"] == 1
    assert [item["company"]["inn"] for item in runtime_state["company_entries"]] == [row.inn]
    assert runtime_state["company_entries"][0]["result"]["inn"] == row.inn
    assert runtime_state["company_entries"][0]["runtime"]["status"] == "completed"
    assert runtime_state["company_entries"][0]["runtime"]["finished_at"] == result.finished_at
    assert "status" not in runtime_state["company_entries"][0]["result"]
    assert "started_at" not in runtime_state["company_entries"][0]["result"]
    assert "finished_at" not in runtime_state["company_entries"][0]["result"]
    assert progress.results_jsonl.exists()
    assert progress.events_jsonl.exists()

    for path in (
        progress.results_json,
        progress.leads_json,
        progress.summary_json,
        progress.availability_summary_json,
        progress.report_md,
        progress.leads_md,
        progress.insights_md,
        progress.final_results_csv,
        progress.final_results_xlsx,
        progress.results_jsonl,
        progress.events_jsonl,
    ):
        path.unlink(missing_ok=True)
    for existing in progress.company_reports_dir.glob("*.md"):
        existing.unlink(missing_ok=True)

    assert not progress.results_json.exists()
    assert not progress.summary_json.exists()

    reloaded = core.ProgressStore(output_dir)

    assert reloaded.get(row.inn) is not None
    assert reloaded.get(row.inn)["status"] == "completed"
    assert json.loads(progress.results_json.read_text(encoding="utf-8"))[0]["inn"] == row.inn
    summary = json.loads(progress.summary_json.read_text(encoding="utf-8"))
    assert summary["processed_rows"] == 1
    assert summary["completed_rows"] == 1
    assert summary["remaining_rows"] == 0
    assert summary["resume_skipped_rows"] == 0
    assert json.loads(progress.results_json.read_text(encoding="utf-8"))[0]["status"] == "completed"
    assert progress.availability_summary_json.exists()
    assert progress.report_md.read_text(encoding="utf-8").strip()
    assert progress.final_results_csv.exists()
    assert progress.final_results_xlsx.exists()
    assert any(progress.company_reports_dir.glob("*.md"))
    assert not progress.results_jsonl.exists()
    assert not progress.events_jsonl.exists()

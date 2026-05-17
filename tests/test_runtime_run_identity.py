from __future__ import annotations

import json
from pathlib import Path

import pytest

import company_enrichment_core as core


def _start_run(
    progress: core.ProgressStore,
    *,
    continue_existing_run: bool = False,
) -> None:
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
        continue_existing_run=continue_existing_run,
    )


def _events(path: Path) -> list[dict[str, object]]:
    return [
        json.loads(line)
        for line in path.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]


def test_progress_store_fresh_run_writes_run_id_without_results_contamination(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setattr(core.ProgressStore, "_write_markdown_reports", lambda self, ordered_results=None: None)
    output_dir = tmp_path / "output"
    progress = core.ProgressStore(output_dir)
    _start_run(progress)

    row = core.RowInput(row_index=1, inn="7700000001", company_name="Factory 1")
    result = core.build_company_result(row)
    result.status = "completed"
    result.finished_at = core.utc_now_iso()
    progress.persist_completed_company_result(result, total_rows=1, processed_rows=1)
    progress.run_finished(processed_rows=1)

    runtime_state = json.loads(progress.runtime_state_json.read_text(encoding="utf-8"))
    run_id = runtime_state["run"]["metadata"]["run_id"]
    assert run_id
    assert "run_id" not in runtime_state["run"]["summary"]

    summary = json.loads(progress.summary_json.read_text(encoding="utf-8"))
    assert summary["run_id"] == run_id

    events = _events(progress.events_jsonl)
    run_events = [item for item in events if item["type"] in {"run_started", "run_finished"}]
    assert [item["run_id"] for item in run_events] == [run_id, run_id]

    results = json.loads(progress.results_json.read_text(encoding="utf-8"))
    assert all("run_id" not in item for item in results)

    results_log = _events(progress.results_jsonl)
    assert all("run_id" not in item for item in results_log)

    assert [item["company"]["inn"] for item in runtime_state["company_entries"]] == [row.inn]
    assert all("run_id" not in item["result"] for item in runtime_state["company_entries"])


def test_progress_store_resume_preserves_existing_run_id(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setattr(core.ProgressStore, "_write_markdown_reports", lambda self, ordered_results=None: None)
    output_dir = tmp_path / "output"
    progress = core.ProgressStore(output_dir)
    _start_run(progress)
    progress.run_finished(processed_rows=0)

    initial_runtime_state = json.loads(progress.runtime_state_json.read_text(encoding="utf-8"))
    run_id = initial_runtime_state["run"]["metadata"]["run_id"]
    assert run_id

    reloaded = core.ProgressStore(output_dir)
    _start_run(reloaded, continue_existing_run=True)
    reloaded.run_finished(processed_rows=0)

    runtime_state = json.loads(reloaded.runtime_state_json.read_text(encoding="utf-8"))
    summary = json.loads(reloaded.summary_json.read_text(encoding="utf-8"))
    events = _events(reloaded.events_jsonl)

    assert runtime_state["run"]["metadata"]["run_id"] == run_id
    assert summary["run_id"] == run_id
    assert [item["run_id"] for item in events if item["type"] == "run_started"] == [run_id, run_id]
    assert [item["run_id"] for item in events if item["type"] == "run_finished"] == [run_id, run_id]


def test_progress_store_fresh_rerun_generates_new_run_id(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setattr(core.ProgressStore, "_write_markdown_reports", lambda self, ordered_results=None: None)
    output_dir = tmp_path / "output"
    progress = core.ProgressStore(output_dir)
    _start_run(progress)
    progress.run_finished(processed_rows=0)

    initial_runtime_state = json.loads(progress.runtime_state_json.read_text(encoding="utf-8"))
    first_run_id = initial_runtime_state["run"]["metadata"]["run_id"]
    assert first_run_id

    reloaded = core.ProgressStore(output_dir)
    _start_run(reloaded)
    reloaded.run_finished(processed_rows=0)

    runtime_state = json.loads(reloaded.runtime_state_json.read_text(encoding="utf-8"))
    summary = json.loads(reloaded.summary_json.read_text(encoding="utf-8"))
    events = _events(reloaded.events_jsonl)
    second_run_id = runtime_state["run"]["metadata"]["run_id"]

    assert second_run_id
    assert second_run_id != first_run_id
    assert summary["run_id"] == second_run_id
    assert [item["run_id"] for item in events if item["type"] == "run_started"] == [second_run_id]
    assert [item["run_id"] for item in events if item["type"] == "run_finished"] == [second_run_id]

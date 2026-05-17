from __future__ import annotations

import json
from pathlib import Path

import pytest

import company_enrichment_core as core
from app.runtime.state import runtime_state_snapshot_from_payload


def _legacy_runtime_state_payload() -> tuple[core.RowInput, dict[str, object]]:
    row = core.RowInput(row_index=1, inn="7700000001", company_name="Factory 1")
    result = core.build_company_result(row)
    result.status = "completed"
    result.finished_at = core.utc_now_iso()
    result.sources["spark"] = core.SourceResult(source="spark", status="success")
    result_payload = core.serialize_company_result(result)
    summary = {
        "updated_at": core.utc_now_iso(),
        "total_rows": 1,
        "rows_selected": 1,
        "selection_mode": "window",
        "selected_ordinals": [1],
        "start_from": 1,
        "end_at": 1,
        "active_sources": ["spark"],
        "processed_rows": 1,
        "completed_rows": 1,
        "remaining_rows": 0,
        "resume_skipped_rows": 0,
    }
    host_stats = {
        "spark-interfax.ru": {
            "first_seen": core.utc_now_iso(),
            "last_seen": core.utc_now_iso(),
            "total_events": 1,
            "event_types": {"request_ok": 1},
            "sources": {"spark": 1},
            "elapsed_seconds": {"count": 1, "sum": 0.25, "max": 0.25, "avg": 0.25},
            "interval_seconds": {"count": 1, "sum": 1.0, "min": 1.0, "max": 1.0, "avg": 1.0},
            "cooldown_seconds": {"count": 0, "sum": 0.0, "max": 0.0},
        }
    }
    return row, {
        "runtime_state_contract_version": 1,
        "companies": [result_payload],
        "summary": summary,
        "host_stats": host_stats,
    }


def _legacy_v2_runtime_state_payload() -> tuple[core.RowInput, dict[str, object], dict[str, object]]:
    row = core.RowInput(row_index=1, inn="7700000001", company_name="Factory 1")
    result = core.build_company_result(row)
    result.status = "completed"
    result.finished_at = core.utc_now_iso()
    result.sources["spark"] = core.SourceResult(source="spark", status="success")
    result_payload = core.serialize_company_result(result)
    runtime_payload = {
        "status": result.status,
        "updated_at": result.finished_at,
    }
    payload = {
        "runtime_state_contract_version": 2,
        "run": {
            "metadata": {
                "rows_selected": 1,
                "selection_mode": "window",
                "active_sources": ["spark"],
            },
            "summary": {
                "updated_at": result.finished_at,
                "rows_selected": 1,
                "selection_mode": "window",
                "active_sources": ["spark"],
                "processed_rows": 1,
                "completed_rows": 1,
                "remaining_rows": 0,
                "resume_skipped_rows": 0,
            },
            "host_stats": {},
        },
        "company_entries": [
            {
                "company": {
                    "inn": row.inn,
                    "row_index": row.row_index,
                    "company_name": row.company_name,
                },
                "runtime": runtime_payload,
                "result": result_payload,
            }
        ],
    }
    return row, payload, result_payload


def test_runtime_state_snapshot_from_v1_payload_derives_run_metadata() -> None:
    row, payload = _legacy_runtime_state_payload()

    snapshot, issue = runtime_state_snapshot_from_payload(
        payload,
        normalize_result_payload=core.normalize_company_result_payload,
    )

    assert issue is None
    assert snapshot is not None
    assert snapshot.results[row.inn]["inn"] == row.inn
    assert snapshot.summary["completed_rows"] == 1
    assert snapshot.host_stats["spark-interfax.ru"]["total_events"] == 1
    assert snapshot.run_metadata["input_path"] == ""
    assert snapshot.run_metadata["run_id"] == ""
    assert snapshot.run_metadata["rows_selected"] == 1
    assert snapshot.run_metadata["selection_mode"] == "window"
    assert snapshot.run_metadata["active_sources"] == ["spark"]


def test_progress_store_migrates_v1_runtime_state_and_preserves_resume_and_rematerialization(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    output_dir = tmp_path / "output"
    output_dir.mkdir()
    monkeypatch.setattr(core.ProgressStore, "_write_markdown_reports", lambda self, ordered_results=None: None)
    row, payload = _legacy_runtime_state_payload()
    (output_dir / "runtime_state.json").write_text(
        json.dumps(payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    progress = core.ProgressStore(output_dir)

    assert progress.get(row.inn) is not None
    assert core.should_skip_on_resume(progress.get(row.inn), ["spark"]) is True
    assert json.loads((output_dir / "results.json").read_text(encoding="utf-8"))[0]["inn"] == row.inn
    summary = json.loads((output_dir / "summary.json").read_text(encoding="utf-8"))
    assert summary["completed_rows"] == 1
    assert json.loads((output_dir / "host_stats.json").read_text(encoding="utf-8"))["spark-interfax.ru"]["total_events"] == 1

    runtime_state = json.loads((output_dir / "runtime_state.json").read_text(encoding="utf-8"))
    assert runtime_state["runtime_state_contract_version"] == 2
    assert runtime_state["run"]["summary"]["completed_rows"] == 1
    assert "run_id" not in runtime_state["run"]["summary"]
    assert runtime_state["run"]["metadata"]["run_id"]
    assert summary["run_id"] == runtime_state["run"]["metadata"]["run_id"]
    assert runtime_state["run"]["metadata"]["rows_selected"] == 1
    assert runtime_state["run"]["metadata"]["active_sources"] == ["spark"]
    assert [item["company"]["inn"] for item in runtime_state["company_entries"]] == [row.inn]
    assert runtime_state["company_entries"][0]["result"]["inn"] == row.inn
    assert runtime_state["company_entries"][0]["runtime"]["status"] == "completed"
    assert runtime_state["company_entries"][0]["runtime"]["started_at"]
    assert runtime_state["company_entries"][0]["runtime"]["finished_at"]
    assert "status" not in runtime_state["company_entries"][0]["result"]
    assert "started_at" not in runtime_state["company_entries"][0]["result"]
    assert "finished_at" not in runtime_state["company_entries"][0]["result"]
    assert not (output_dir / "results.jsonl").exists()
    assert not (output_dir / "events.jsonl").exists()


def test_progress_store_migrates_legacy_v2_company_entry_runtime_fields_to_canonical_runtime(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    output_dir = tmp_path / "output"
    output_dir.mkdir()
    monkeypatch.setattr(core.ProgressStore, "_write_markdown_reports", lambda self, ordered_results=None: None)
    row, payload, result_payload = _legacy_v2_runtime_state_payload()
    (output_dir / "runtime_state.json").write_text(
        json.dumps(payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    progress = core.ProgressStore(output_dir)

    loaded_result = progress.get(row.inn)
    assert loaded_result is not None
    assert loaded_result["status"] == result_payload["status"]
    assert loaded_result["started_at"] == result_payload["started_at"]
    assert loaded_result["finished_at"] == result_payload["finished_at"]
    assert core.should_skip_on_resume(loaded_result, ["spark"]) is True

    runtime_state = json.loads((output_dir / "runtime_state.json").read_text(encoding="utf-8"))
    summary = json.loads((output_dir / "summary.json").read_text(encoding="utf-8"))
    entry = runtime_state["company_entries"][0]
    assert runtime_state["run"]["metadata"]["run_id"]
    assert summary["run_id"] == runtime_state["run"]["metadata"]["run_id"]
    assert entry["runtime"]["status"] == "completed"
    assert entry["runtime"]["started_at"] == result_payload["started_at"]
    assert entry["runtime"]["finished_at"] == result_payload["finished_at"]
    assert entry["runtime"]["updated_at"] == result_payload["finished_at"]
    assert entry["result"]["inn"] == row.inn
    assert "status" not in entry["result"]
    assert "started_at" not in entry["result"]
    assert "finished_at" not in entry["result"]
    assert not (output_dir / "results.jsonl").exists()
    assert not (output_dir / "events.jsonl").exists()

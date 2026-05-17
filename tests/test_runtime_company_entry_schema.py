from __future__ import annotations

import json
from pathlib import Path

import pytest

import company_enrichment_core as core
import app.runtime.progress as runtime_progress
from app.runtime.queue_families import build_downstream_worker_pool_contour
from app.runtime.state import STAGE_EXECUTION_EVIDENCE_KEY, runtime_state_snapshot_from_payload


def _mojibake_text(value: str) -> str:
    return value.encode("utf-8").decode("latin-1")


def _assert_no_mojibake_markers(text: str) -> None:
    assert chr(0x00D0) not in text
    assert chr(0x00D1) not in text
    assert chr(0x00C3) not in text
    assert "â€”" not in text
    assert "â€¦" not in text
    assert all(not 0x80 <= ord(char) <= 0x9F for char in text)


def _public_publish_state(output_dir: Path) -> dict[str, str]:
    return json.loads(
        (output_dir / "_runtime" / "public_publish_state.json").read_text(encoding="utf-8")
    )


def _public_generation_snapshot_dir(output_dir: Path, generation_id: str) -> Path:
    return output_dir / "_runtime" / "public_generations" / generation_id


def _runtime_metadata_company(
    runtime_state: dict[str, object],
    *,
    metadata_key: str,
    inn: str,
) -> tuple[str, dict[str, object]]:
    metadata_payload = (
        ((runtime_state.get("run") or {}).get("metadata") or {}).get(metadata_key)
        if isinstance(runtime_state, dict)
        else None
    )
    if not isinstance(metadata_payload, dict):
        raise KeyError(inn)
    for surface_key, surface_payload in metadata_payload.items():
        if not isinstance(surface_payload, dict):
            continue
        companies = surface_payload.get("companies")
        if not isinstance(companies, dict):
            continue
        company_payload = companies.get(inn)
        if isinstance(company_payload, dict):
            return str(surface_key), company_payload
    raise KeyError(inn)


def _separated_runtime_state_payload() -> tuple[core.RowInput, dict[str, object], dict[str, object]]:
    row = core.RowInput(row_index=1, inn="7700000001", company_name="Factory 1")
    result = core.build_company_result(row)
    result.status = "completed"
    result.finished_at = core.utc_now_iso()
    result.sources["spark"] = core.SourceResult(source="spark", status="success")
    result_payload = core.serialize_company_result(result)
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
                "runtime": {
                    "status": result.status,
                    "started_at": result.started_at,
                    "finished_at": result.finished_at,
                    "updated_at": result.finished_at,
                },
                "result": {
                    key: value
                    for key, value in result_payload.items()
                    if key not in {"status", "started_at", "finished_at"}
                },
            }
        ],
    }
    return row, payload, result_payload


def _checkpointed_completed_runtime_state_payload() -> tuple[core.RowInput, dict[str, object], dict[str, object]]:
    row = core.RowInput(row_index=1, inn="7700000002", company_name="Factory 2")
    result = core.build_company_result(row)
    result.status = "completed"
    result.finished_at = core.utc_now_iso()
    result.sources["spark"] = core.SourceResult(source="spark", status="success")
    result_payload = core.serialize_company_result(result)
    checkpoint_work_unit = _completed_checkpoint_work_unit(
        row,
        result_payload,
        handoff_fingerprint="checkpoint-only",
        candidate_site="https://factory-2.example",
    )
    payload = {
        "runtime_state_contract_version": 2,
        "run": {
            "metadata": {
                "rows_selected": 1,
                "selection_mode": "window",
                "selected_ordinals": [1],
                "start_from": 1,
                "end_at": 1,
                "active_sources": ["spark"],
                "resume_skipped_rows": 0,
                "stage_work_units": {
                    "aggregator_site": {
                        "companies": {
                            row.inn: checkpoint_work_unit,
                        }
                    },
                },
                STAGE_EXECUTION_EVIDENCE_KEY: {
                    "aggregator_site": {
                        "companies": {
                            row.inn: checkpoint_work_unit,
                        }
                    },
                },
            },
            "summary": {
                "updated_at": result.finished_at,
                "rows_selected": 1,
                "selection_mode": "window",
                "selected_ordinals": [1],
                "active_sources": ["spark"],
                "processed_rows": 0,
                "completed_rows": 0,
                "remaining_rows": 1,
                "resume_skipped_rows": 0,
            },
            "host_stats": {},
        },
        "company_entries": [],
    }
    return row, payload, result_payload


def _explicit_work_unit_runtime_state_payload_with_mojibake() -> tuple[str, dict[str, object]]:
    inn = "7700000007"
    work_unit = {
        "inn": inn,
        "row_index": 1,
        "execution_boundary": "deep_site_parse",
        "work_status": "pending",
        "handoff_fingerprint": "explicit-mirror-mojibake",
        "last_message_ts": "2026-04-21T17:08:25+00:00",
        "acknowledged_at": "",
        "private_state": {},
        "work_unit": {
            "inn": inn,
            "row_index": 1,
            "company_name": "Factory 7",
            "candidate_sites": ["https://factory-7.example"],
            "deep_parse_sites": ["https://factory-7.example/about"],
            "site_gate_decisions": [
                {
                    "url": "https://factory-7.example/about",
                    "final_url": "https://factory-7.example/about",
                    "site_url": "https://factory-7.example/about",
                    "status": "success",
                    "belongs_to_company": True,
                    "decision_status": "verified",
                    "title": _mojibake_text("Сокол"),
                    "description": _mojibake_text("Первый слой"),
                }
            ],
        },
    }
    payload = {
        "runtime_state_contract_version": 2,
        "run": {
            "metadata": {
                "run_id": "runtime-state-mojibake",
                "input_path": "input.xlsx",
                "total_rows": 1,
                "rows_selected": 1,
                "selection_mode": "window",
                "selected_ordinals": [1],
                "start_from": 1,
                "end_at": 1,
                "active_sources": ["spark"],
                "resume_skipped_rows": 0,
                "stage_work_units": {
                    "aggregator_site": {
                        "companies": {
                            inn: work_unit,
                        }
                    }
                },
                STAGE_EXECUTION_EVIDENCE_KEY: {
                    "aggregator_site": {
                        "companies": {
                            inn: work_unit,
                        }
                    }
                },
            },
            "summary": {
                "updated_at": "2026-04-21T17:08:25+00:00",
                "rows_selected": 1,
                "selection_mode": "window",
                "selected_ordinals": [1],
                "active_sources": ["spark"],
                "processed_rows": 0,
                "completed_rows": 0,
                "remaining_rows": 1,
                "resume_skipped_rows": 0,
            },
            "host_stats": {},
        },
        "company_entries": [],
    }
    return inn, payload


def _separated_and_checkpointed_completed_runtime_state_payload() -> tuple[
    core.RowInput,
    dict[str, object],
    dict[str, object],
]:
    row, payload, result_payload = _separated_runtime_state_payload()
    checkpoint_work_unit = _completed_checkpoint_work_unit(
        row,
        result_payload,
        handoff_fingerprint="checkpoint-and-entry",
        candidate_site="https://factory-1.example",
    )
    payload["run"]["metadata"].update(
        {
            "selected_ordinals": [1],
            "start_from": 1,
            "end_at": 1,
            "resume_skipped_rows": 0,
            "stage_work_units": {
                "aggregator_site": {
                    "companies": {
                        row.inn: checkpoint_work_unit,
                    }
                }
            },
            STAGE_EXECUTION_EVIDENCE_KEY: {
                "aggregator_site": {
                    "companies": {
                        row.inn: checkpoint_work_unit,
                    }
                }
            },
        }
    )
    return row, payload, result_payload


def _completed_checkpoint_work_unit(
    row: core.RowInput,
    result_payload: dict[str, object],
    *,
    handoff_fingerprint: str,
    candidate_site: str,
) -> dict[str, object]:
    return {
        "inn": row.inn,
        "row_index": row.row_index,
        "execution_boundary": "deep_site_parse",
        "work_status": "pending",
        "handoff_fingerprint": handoff_fingerprint,
        "last_message_ts": str(result_payload["finished_at"]),
        "acknowledged_at": "",
        "private_state": {
            "completed_company_result": result_payload,
        },
        "work_unit": {
            "inn": row.inn,
            "row_index": row.row_index,
            "company_name": row.company_name,
            "candidate_sites": [candidate_site],
        },
    }


def _incomplete_runtime_state_payload() -> tuple[core.RowInput, dict[str, object], dict[str, object]]:
    row = core.RowInput(row_index=1, inn="7700000003", company_name="Factory 3")
    result = core.build_company_result(row)
    result.status = "running"
    result.sources["spark"] = core.SourceResult(source="spark", status="success")
    result_payload = core.serialize_company_result(result)
    payload = {
        "runtime_state_contract_version": 2,
        "run": {
            "metadata": {
                "rows_selected": 1,
                "selection_mode": "window",
                "active_sources": ["spark"],
            },
            "summary": {
                "updated_at": result.started_at,
                "rows_selected": 1,
                "selection_mode": "window",
                "active_sources": ["spark"],
                "processed_rows": 0,
                "completed_rows": 0,
                "remaining_rows": 1,
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
                "runtime": {
                    "status": result.status,
                    "started_at": result.started_at,
                    "finished_at": "",
                    "updated_at": result.started_at,
                },
                "result": {
                    key: value
                    for key, value in result_payload.items()
                    if key not in {"status", "started_at", "finished_at"}
                },
            }
        ],
    }
    return row, payload, result_payload


def test_runtime_state_snapshot_rehydrates_company_runtime_into_loaded_result() -> None:
    row, payload, result_payload = _separated_runtime_state_payload()

    snapshot, issue = runtime_state_snapshot_from_payload(
        payload,
        normalize_result_payload=core.normalize_company_result_payload,
    )

    assert issue is None
    assert snapshot is not None
    loaded_result = snapshot.results[row.inn]
    assert loaded_result["status"] == result_payload["status"]
    assert loaded_result["started_at"] == result_payload["started_at"]
    assert loaded_result["finished_at"] == result_payload["finished_at"]
    assert core.should_skip_on_resume(loaded_result, ["spark"]) is True


def test_runtime_state_snapshot_preserves_downstream_worker_pool_metadata() -> None:
    _, payload, _ = _separated_runtime_state_payload()
    downstream_worker_pools = build_downstream_worker_pool_contour(company_concurrency_cap=2).as_payload()
    payload["run"]["metadata"]["downstream_worker_pools"] = downstream_worker_pools
    payload["run"]["metadata"]["source_lane_scheduler"] = {
        "source_lane_contour": [],
        "downstream_worker_pools": downstream_worker_pools,
    }

    snapshot, issue = runtime_state_snapshot_from_payload(
        payload,
        normalize_result_payload=core.normalize_company_result_payload,
    )

    assert issue is None
    assert snapshot is not None
    assert snapshot.run_metadata["downstream_worker_pools"] == downstream_worker_pools
    assert snapshot.run_metadata["source_lane_scheduler"]["downstream_worker_pools"] == downstream_worker_pools


def test_progress_store_rematerializes_export_result_from_separated_company_runtime(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    output_dir = tmp_path / "output"
    output_dir.mkdir()
    monkeypatch.setattr(core.ProgressStore, "_write_markdown_reports", lambda self, ordered_results=None: None)
    row, payload, result_payload = _separated_runtime_state_payload()
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

    rematerialized_results = json.loads((output_dir / "results.json").read_text(encoding="utf-8"))
    assert rematerialized_results[0]["inn"] == row.inn
    assert rematerialized_results[0]["status"] == result_payload["status"]
    assert rematerialized_results[0]["started_at"] == result_payload["started_at"]
    assert rematerialized_results[0]["finished_at"] == result_payload["finished_at"]

    rematerialized_runtime_state = json.loads((output_dir / "runtime_state.json").read_text(encoding="utf-8"))
    entry = rematerialized_runtime_state["company_entries"][0]
    assert entry["runtime"]["status"] == result_payload["status"]
    assert entry["runtime"]["started_at"] == result_payload["started_at"]
    assert entry["runtime"]["finished_at"] == result_payload["finished_at"]
    assert "status" not in entry["result"]
    assert "started_at" not in entry["result"]
    assert "finished_at" not in entry["result"]
    assert not (output_dir / "results.jsonl").exists()
    assert not (output_dir / "events.jsonl").exists()


def test_progress_store_rematerializes_checkpointed_completed_result_into_public_outputs(
    tmp_path: Path,
) -> None:
    output_dir = tmp_path / "output"
    output_dir.mkdir()
    row, payload, result_payload = _checkpointed_completed_runtime_state_payload()
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

    rematerialized_results = json.loads((output_dir / "results.json").read_text(encoding="utf-8"))
    assert [item["inn"] for item in rematerialized_results] == [row.inn]
    assert rematerialized_results[0]["status"] == result_payload["status"]

    summary = json.loads((output_dir / "summary.json").read_text(encoding="utf-8"))
    assert summary["processed_rows"] == 1
    assert summary["completed_rows"] == 1
    assert summary["remaining_rows"] == 0
    assert summary["public_output_contract"]["terminal_run"] is False
    assert summary["public_output_contract"]["all_selected_completed"] is True
    assert summary["public_output_contract"]["final_exports"]["available"] is True
    assert summary["public_output_contract"]["final_exports"]["state"] == "all_selected_completed"

    assert json.loads((output_dir / "leads.json").read_text(encoding="utf-8")) == []
    assert (output_dir / "availability_summary.json").exists()
    assert row.inn in (output_dir / "report.md").read_text(encoding="utf-8")
    assert row.inn in (output_dir / "final_results.csv").read_text(encoding="utf-8-sig")
    assert (output_dir / "final_results.xlsx").exists()
    assert any((output_dir / "company_reports").glob("*.md"))

    rematerialized_runtime_state = json.loads((output_dir / "runtime_state.json").read_text(encoding="utf-8"))
    assert rematerialized_runtime_state["run"]["summary"]["processed_rows"] == 1
    assert rematerialized_runtime_state["run"]["summary"]["completed_rows"] == 1
    assert rematerialized_runtime_state["run"]["summary"]["remaining_rows"] == 0
    assert len(rematerialized_runtime_state["company_entries"]) == 1
    entry = rematerialized_runtime_state["company_entries"][0]
    assert entry["runtime"]["status"] == result_payload["status"]
    assert entry["runtime"]["started_at"] == result_payload["started_at"]
    assert entry["runtime"]["finished_at"] == result_payload["finished_at"]
    assert "status" not in entry["result"]
    assert "started_at" not in entry["result"]
    assert "finished_at" not in entry["result"]
    _, checkpoint_work_unit = _runtime_metadata_company(
        rematerialized_runtime_state,
        metadata_key="stage_work_units",
        inn=row.inn,
    )
    assert "completed_company_result" not in checkpoint_work_unit["private_state"]
    _, checkpoint_evidence = _runtime_metadata_company(
        rematerialized_runtime_state,
        metadata_key=STAGE_EXECUTION_EVIDENCE_KEY,
        inn=row.inn,
    )
    assert "completed_company_result" not in checkpoint_evidence["private_state"]
    assert not (output_dir / "results.jsonl").exists()
    assert not (output_dir / "events.jsonl").exists()

    reloaded = core.ProgressStore(output_dir)

    second_loaded_result = reloaded.get(row.inn)
    assert second_loaded_result is not None
    assert second_loaded_result["status"] == result_payload["status"]

    second_summary = json.loads((output_dir / "summary.json").read_text(encoding="utf-8"))
    assert second_summary["processed_rows"] == 1
    assert second_summary["completed_rows"] == 1
    assert second_summary["remaining_rows"] == 0

    second_runtime_state = json.loads((output_dir / "runtime_state.json").read_text(encoding="utf-8"))
    assert len(second_runtime_state["company_entries"]) == 1
    _, second_work_unit = _runtime_metadata_company(
        second_runtime_state,
        metadata_key="stage_work_units",
        inn=row.inn,
    )
    assert "completed_company_result" not in second_work_unit["private_state"]
    _, second_evidence = _runtime_metadata_company(
        second_runtime_state,
        metadata_key=STAGE_EXECUTION_EVIDENCE_KEY,
        inn=row.inn,
    )
    assert "completed_company_result" not in second_evidence["private_state"]


def test_progress_store_repairs_mojibake_in_public_outputs(tmp_path: Path) -> None:
    output_dir = tmp_path / "output"
    output_dir.mkdir()
    row, payload, result_payload = _checkpointed_completed_runtime_state_payload()
    result_payload["notes"] = [_mojibake_text("Не нашел кандидатов на сайт из агрегаторов и доменов почты")]
    result_payload["site_refresh_plans"] = [
        {
            "site_url": "https://factory-2.example",
            "cadence": "weekly",
            "next_due_at": "2026-04-28T15:25:44+00:00",
            "reason": _mojibake_text("сайт требует осторожного или частичного обхода"),
        }
    ]
    result_payload["validated_sites"] = [
        {
            "url": "https://factory-2.example",
            "status": "success",
            "belongs_to_company": True,
            "decision_status": "verified",
            "title": _mojibake_text("Сокол"),
            "description": _mojibake_text("Первый слой"),
        }
    ]
    (output_dir / "runtime_state.json").write_text(
        json.dumps(payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    core.ProgressStore(output_dir)

    rematerialized_results = json.loads((output_dir / "results.json").read_text(encoding="utf-8"))
    result_entry = rematerialized_results[0]
    assert result_entry["notes"] == ["Не нашел кандидатов на сайт из агрегаторов и доменов почты"]
    assert result_entry["site_refresh_plans"][0]["reason"] == "сайт требует осторожного или частичного обхода"
    assert result_entry["validated_sites"][0]["title"] == "Сокол"
    assert result_entry["validated_sites"][0]["description"] == "Первый слой"

    company_report = next((output_dir / "company_reports").glob("*.md")).read_text(encoding="utf-8")
    leads_report = (output_dir / "leads.md").read_text(encoding="utf-8")
    insights_report = (output_dir / "insights.md").read_text(encoding="utf-8")
    report_index = (output_dir / "report.md").read_text(encoding="utf-8")

    assert "Не нашел кандидатов на сайт из агрегаторов и доменов почты" in company_report
    assert "сайт требует осторожного или частичного обхода" in company_report
    assert "Лидов пока нет." in leads_report
    assert row.inn in report_index

    _assert_no_mojibake_markers(json.dumps(result_entry, ensure_ascii=False))
    _assert_no_mojibake_markers(company_report)
    _assert_no_mojibake_markers(leads_report)
    _assert_no_mojibake_markers(insights_report)
    _assert_no_mojibake_markers(report_index)


def test_progress_store_live_persist_repairs_mojibake_in_results_log_and_company_reports(tmp_path: Path) -> None:
    output_dir = tmp_path / "output"
    output_dir.mkdir()

    progress = core.ProgressStore(output_dir)
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

    row = core.RowInput(row_index=1, inn="7700000008", company_name="Factory 8")
    result = core.build_company_result(row)
    result.status = "completed"
    result.finished_at = core.utc_now_iso()
    result.input_comment = _mojibake_text("операторская заметка")
    result.sources["spark"] = core.SourceResult(
        source="spark",
        status="success",
        company_name_found=_mojibake_text("Тестовый завод"),
    )
    result.notes = [_mojibake_text("Не нашел кандидатов на сайт из агрегаторов и доменов почты")]
    result.site_refresh_plans = [
        core.SiteRefreshPlan(
            site_url="https://factory-8.example",
            cadence="weekly",
            next_due_at="2026-04-28T15:25:44+00:00",
            reason=_mojibake_text("сайт требует осторожного или частичного обхода"),
        )
    ]
    result.validated_sites = [
        core.site_decision_from_dict(
            {
                "url": "https://factory-8.example",
                "final_url": "https://factory-8.example",
                "status": "success",
                "belongs_to_company": True,
                "decision_status": "verified",
                "title": _mojibake_text("Сокол"),
                "description": _mojibake_text("Первый слой"),
                "reasons": [_mojibake_text("совпадает с доменом компании")],
            }
        )
    ]

    progress.persist_completed_company_result(result, total_rows=1, processed_rows=1)

    results = json.loads((output_dir / "results.json").read_text(encoding="utf-8"))
    results_log = [
        json.loads(line)
        for line in (output_dir / "results.jsonl").read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    company_report = next((output_dir / "company_reports").glob("*.md")).read_text(encoding="utf-8")

    assert len(results) == 1
    assert len(results_log) == 1

    result_entry = results[0]
    result_log_entry = results_log[0]

    for entry in (result_entry, result_log_entry):
        assert entry["input_comment"] == "операторская заметка"
        assert entry["notes"] == ["Не нашел кандидатов на сайт из агрегаторов и доменов почты"]
        assert entry["sources"]["spark"]["company_name_found"] == "Тестовый завод"
        assert entry["site_refresh_plans"][0]["reason"] == "сайт требует осторожного или частичного обхода"
        assert entry["validated_sites"][0]["title"] == "Сокол"
        assert entry["validated_sites"][0]["description"] == "Первый слой"
        assert entry["validated_sites"][0]["reasons"] == ["совпадает с доменом компании"]
        _assert_no_mojibake_markers(json.dumps(entry, ensure_ascii=False))

    assert "операторская заметка" in company_report
    assert "Тестовый завод" in company_report
    assert "Не нашел кандидатов на сайт из агрегаторов и доменов почты" in company_report
    assert "сайт требует осторожного или частичного обхода" in company_report
    assert "совпадает с доменом компании" in company_report
    _assert_no_mojibake_markers(company_report)


def test_progress_store_repairs_mojibake_in_explicit_work_unit_runtime_mirror(tmp_path: Path) -> None:
    output_dir = tmp_path / "output"
    output_dir.mkdir()
    inn, payload = _explicit_work_unit_runtime_state_payload_with_mojibake()
    (output_dir / "runtime_state.json").write_text(
        json.dumps(payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    progress = core.ProgressStore(output_dir)

    pending_work_units = progress.pending_stage_work_units(execution_boundary="deep_site_parse")
    assert [item["inn"] for item in pending_work_units] == [inn]
    assert pending_work_units[0]["work_unit"]["site_gate_decisions"][0]["title"] == "Сокол"
    assert pending_work_units[0]["work_unit"]["site_gate_decisions"][0]["description"] == "Первый слой"

    rematerialized_runtime_state = json.loads((output_dir / "runtime_state.json").read_text(encoding="utf-8"))
    for metadata_key in ("stage_work_units", STAGE_EXECUTION_EVIDENCE_KEY):
        _, work_unit = _runtime_metadata_company(
            rematerialized_runtime_state,
            metadata_key=metadata_key,
            inn=inn,
        )
        assert work_unit["work_unit"]["site_gate_decisions"][0]["title"] == "Сокол"
        assert work_unit["work_unit"]["site_gate_decisions"][0]["description"] == "Первый слой"
        _assert_no_mojibake_markers(json.dumps(work_unit["work_unit"], ensure_ascii=False))


def test_progress_store_does_not_duplicate_completed_result_already_present_in_company_entries(
    tmp_path: Path,
) -> None:
    output_dir = tmp_path / "output"
    output_dir.mkdir()
    row, payload, result_payload = _separated_and_checkpointed_completed_runtime_state_payload()
    (output_dir / "runtime_state.json").write_text(
        json.dumps(payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    progress = core.ProgressStore(output_dir)

    loaded_result = progress.get(row.inn)
    assert loaded_result is not None
    assert loaded_result["status"] == result_payload["status"]

    rematerialized_results = json.loads((output_dir / "results.json").read_text(encoding="utf-8"))
    assert [item["inn"] for item in rematerialized_results] == [row.inn]

    summary = json.loads((output_dir / "summary.json").read_text(encoding="utf-8"))
    assert summary["processed_rows"] == 1
    assert summary["completed_rows"] == 1
    assert summary["remaining_rows"] == 0

    rematerialized_runtime_state = json.loads((output_dir / "runtime_state.json").read_text(encoding="utf-8"))
    assert rematerialized_runtime_state["run"]["summary"]["processed_rows"] == 1
    assert rematerialized_runtime_state["run"]["summary"]["completed_rows"] == 1
    assert rematerialized_runtime_state["run"]["summary"]["remaining_rows"] == 0
    assert len(rematerialized_runtime_state["company_entries"]) == 1
    _, checkpoint_work_unit = _runtime_metadata_company(
        rematerialized_runtime_state,
        metadata_key="stage_work_units",
        inn=row.inn,
    )
    assert "completed_company_result" not in checkpoint_work_unit["private_state"]
    _, checkpoint_evidence = _runtime_metadata_company(
        rematerialized_runtime_state,
        metadata_key=STAGE_EXECUTION_EVIDENCE_KEY,
        inn=row.inn,
    )
    assert "completed_company_result" not in checkpoint_evidence["private_state"]


def test_progress_store_live_persist_publishes_canonical_state_without_cleanup_window(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    output_dir = tmp_path / "output"
    output_dir.mkdir()
    monkeypatch.setattr(core.ProgressStore, "_write_markdown_reports", lambda self, ordered_results=None: None)

    progress = core.ProgressStore(output_dir)
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

    row = core.RowInput(row_index=1, inn="7700000004", company_name="Factory 4")
    result = core.build_company_result(row)
    result.status = "completed"
    result.finished_at = core.utc_now_iso()
    result.sources["spark"] = core.SourceResult(source="spark", status="success")
    result_payload = core.serialize_company_result(result)

    work_unit = progress.materialize_stage_work_unit(
        inn=row.inn,
        row_index=row.row_index,
        execution_boundary="deep_site_parse",
        last_message_ts=result.finished_at,
        work_unit_payload={
            "inn": row.inn,
            "row_index": row.row_index,
            "company_name": row.company_name,
            "candidate_sites": ["https://factory-4.example"],
        },
    )
    progress.merge_stage_work_unit_private_state(
        inn=row.inn,
        handoff_fingerprint=str(work_unit["handoff_fingerprint"]),
        private_state_patch={"completed_company_result": result_payload},
        last_message_ts=result.finished_at,
    )

    checkpointed_runtime_state = json.loads((output_dir / "runtime_state.json").read_text(encoding="utf-8"))
    _, checkpoint_work_unit = _runtime_metadata_company(
        checkpointed_runtime_state,
        metadata_key="stage_work_units",
        inn=row.inn,
    )
    assert checkpoint_work_unit["private_state"]["completed_company_result"]["inn"] == row.inn
    _, checkpoint_evidence = _runtime_metadata_company(
        checkpointed_runtime_state,
        metadata_key=STAGE_EXECUTION_EVIDENCE_KEY,
        inn=row.inn,
    )
    assert checkpoint_evidence["private_state"]["completed_company_result"]["inn"] == row.inn

    observed_runtime_state: dict[str, object] = {}
    original_append_jsonl = runtime_progress.append_jsonl

    def fail_before_results_jsonl_append(path: Path, item: object) -> None:
        if Path(path) == output_dir / "results.jsonl":
            observed_runtime_state["payload"] = json.loads((output_dir / "runtime_state.json").read_text(encoding="utf-8"))
            raise RuntimeError("synthetic crash after canonical persist before append-only result log")
        original_append_jsonl(path, item)

    monkeypatch.setattr(runtime_progress, "append_jsonl", fail_before_results_jsonl_append)

    with pytest.raises(RuntimeError, match="synthetic crash after canonical persist before append-only result log"):
        progress.persist_completed_company_result(result, total_rows=1, processed_rows=1)

    crashed_runtime_state = observed_runtime_state["payload"]
    assert isinstance(crashed_runtime_state, dict)
    assert len(crashed_runtime_state["company_entries"]) == 1

    _, cleaned_work_unit = _runtime_metadata_company(
        crashed_runtime_state,
        metadata_key="stage_work_units",
        inn=row.inn,
    )
    assert "completed_company_result" not in cleaned_work_unit["private_state"]
    _, cleaned_evidence = _runtime_metadata_company(
        crashed_runtime_state,
        metadata_key=STAGE_EXECUTION_EVIDENCE_KEY,
        inn=row.inn,
    )
    assert "completed_company_result" not in cleaned_evidence["private_state"]
    crashed_results = json.loads((output_dir / "results.json").read_text(encoding="utf-8"))
    assert [item["inn"] for item in crashed_results] == [row.inn]
    assert crashed_results[0]["status"] == result_payload["status"]
    crashed_summary = json.loads((output_dir / "summary.json").read_text(encoding="utf-8"))
    assert crashed_summary["processed_rows"] == 1
    assert crashed_summary["completed_rows"] == 1
    assert crashed_summary["remaining_rows"] == 0
    assert not (output_dir / "results.jsonl").exists()

    reloaded = core.ProgressStore(output_dir)

    loaded_result = reloaded.get(row.inn)
    assert loaded_result is not None
    assert loaded_result["status"] == result_payload["status"]

    rematerialized_results = json.loads((output_dir / "results.json").read_text(encoding="utf-8"))
    assert [item["inn"] for item in rematerialized_results] == [row.inn]
    rematerialized_summary = json.loads((output_dir / "summary.json").read_text(encoding="utf-8"))
    assert rematerialized_summary["processed_rows"] == 1
    assert rematerialized_summary["completed_rows"] == 1
    assert rematerialized_summary["remaining_rows"] == 0
    assert not (output_dir / "results.jsonl").exists()

    rematerialized_runtime_state = json.loads((output_dir / "runtime_state.json").read_text(encoding="utf-8"))
    _, rematerialized_work_unit = _runtime_metadata_company(
        rematerialized_runtime_state,
        metadata_key="stage_work_units",
        inn=row.inn,
    )
    assert "completed_company_result" not in rematerialized_work_unit["private_state"]
    _, rematerialized_evidence = _runtime_metadata_company(
        rematerialized_runtime_state,
        metadata_key=STAGE_EXECUTION_EVIDENCE_KEY,
        inn=row.inn,
    )
    assert "completed_company_result" not in rematerialized_evidence["private_state"]


def test_progress_store_reload_converges_public_outputs_after_crash_between_neighboring_publish_steps(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    output_dir = tmp_path / "output"
    output_dir.mkdir()
    monkeypatch.setattr(core.ProgressStore, "_write_markdown_reports", lambda self, ordered_results=None: None)

    progress = core.ProgressStore(output_dir)
    progress.run_started(
        input_path="input.xlsx",
        total_rows=2,
        selected_rows=2,
        selection_mode="window",
        selected_ordinals=[1, 2],
        start_from=1,
        end_at=2,
        active_sources=["spark"],
    )

    first_row = core.RowInput(row_index=1, inn="7700000005", company_name="Factory 5")
    first_result = core.build_company_result(first_row)
    first_result.status = "completed"
    first_result.finished_at = core.utc_now_iso()
    first_result.sources["spark"] = core.SourceResult(source="spark", status="success")
    progress.persist_completed_company_result(first_result, total_rows=2, processed_rows=1)

    initial_results = json.loads((output_dir / "results.json").read_text(encoding="utf-8"))
    initial_leads = json.loads((output_dir / "leads.json").read_text(encoding="utf-8"))
    initial_summary = json.loads((output_dir / "summary.json").read_text(encoding="utf-8"))
    assert [item["inn"] for item in initial_results] == [first_row.inn]
    assert initial_leads == []
    assert initial_summary["processed_rows"] == 1
    assert initial_summary["completed_rows"] == 1
    assert initial_summary["remaining_rows"] == 1
    initial_publish_state = _public_publish_state(output_dir)
    assert initial_publish_state["active_generation_id"] != ""
    assert initial_publish_state["active_generation_id"] == initial_publish_state["committed_generation_id"]
    initial_generation_snapshot_dir = _public_generation_snapshot_dir(
        output_dir,
        initial_publish_state["committed_generation_id"],
    )
    assert initial_generation_snapshot_dir.exists()
    assert json.loads((initial_generation_snapshot_dir / "results.json").read_text(encoding="utf-8")) == initial_results
    assert json.loads((initial_generation_snapshot_dir / "leads.json").read_text(encoding="utf-8")) == initial_leads
    assert json.loads((initial_generation_snapshot_dir / "summary.json").read_text(encoding="utf-8")) == initial_summary

    second_row = core.RowInput(row_index=2, inn="7700000006", company_name="Factory 6")
    second_result = core.build_company_result(second_row)
    second_result.status = "completed"
    second_result.finished_at = core.utc_now_iso()
    second_result.sources["spark"] = core.SourceResult(source="spark", status="success")
    second_result.lead_cards = [{"title": "Lead 6"}]

    observed_public_snapshot: dict[str, object] = {}
    publish_calls = {"count": 0}
    original_publish = runtime_progress.ProgressStore._publish_public_output_file

    def fail_before_second_public_publish(self, staged_path: Path, final_path: Path) -> None:
        publish_calls["count"] += 1
        if publish_calls["count"] == 2:
            observed_public_snapshot["runtime_state"] = json.loads(
                (output_dir / "runtime_state.json").read_text(encoding="utf-8")
            )
            observed_public_snapshot["public_publish_state"] = _public_publish_state(output_dir)
            observed_public_snapshot["results"] = json.loads((output_dir / "results.json").read_text(encoding="utf-8"))
            observed_public_snapshot["leads"] = json.loads((output_dir / "leads.json").read_text(encoding="utf-8"))
            observed_public_snapshot["summary"] = json.loads((output_dir / "summary.json").read_text(encoding="utf-8"))
            raise RuntimeError("synthetic crash between neighboring public publish steps")
        original_publish(self, staged_path, final_path)

    monkeypatch.setattr(runtime_progress.ProgressStore, "_publish_public_output_file", fail_before_second_public_publish)

    with pytest.raises(RuntimeError, match="synthetic crash between neighboring public publish steps"):
        progress.persist_completed_company_result(second_result, total_rows=2, processed_rows=2)

    assert publish_calls["count"] == 2
    crashed_runtime_state = observed_public_snapshot["runtime_state"]
    assert isinstance(crashed_runtime_state, dict)
    assert len(crashed_runtime_state["company_entries"]) == 2
    assert crashed_runtime_state["run"]["summary"]["processed_rows"] == 2
    assert crashed_runtime_state["run"]["summary"]["completed_rows"] == 2
    assert crashed_runtime_state["run"]["summary"]["remaining_rows"] == 0

    crashed_results = observed_public_snapshot["results"]
    assert isinstance(crashed_results, list)
    assert [item["inn"] for item in crashed_results] == [first_row.inn, second_row.inn]

    crashed_leads = observed_public_snapshot["leads"]
    assert crashed_leads == []

    crashed_summary = observed_public_snapshot["summary"]
    assert isinstance(crashed_summary, dict)
    assert crashed_summary["processed_rows"] == 1
    assert crashed_summary["completed_rows"] == 1
    assert crashed_summary["remaining_rows"] == 1
    crashed_publish_state = observed_public_snapshot["public_publish_state"]
    assert isinstance(crashed_publish_state, dict)
    assert crashed_publish_state["active_generation_id"] != ""
    assert crashed_publish_state["active_generation_id"] != crashed_publish_state["committed_generation_id"]
    assert crashed_publish_state["committed_generation_id"] == initial_publish_state["committed_generation_id"]
    committed_generation_snapshot_dir = _public_generation_snapshot_dir(
        output_dir,
        crashed_publish_state["committed_generation_id"],
    )
    assert committed_generation_snapshot_dir.exists()
    assert json.loads((committed_generation_snapshot_dir / "results.json").read_text(encoding="utf-8")) == initial_results
    assert json.loads((committed_generation_snapshot_dir / "leads.json").read_text(encoding="utf-8")) == initial_leads
    assert json.loads((committed_generation_snapshot_dir / "summary.json").read_text(encoding="utf-8")) == initial_summary

    results_jsonl_before_reload = [
        json.loads(line)
        for line in (output_dir / "results.jsonl").read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    assert [item["inn"] for item in results_jsonl_before_reload] == [first_row.inn]

    monkeypatch.setattr(runtime_progress.ProgressStore, "_publish_public_output_file", original_publish)
    replayed_public_snapshot: dict[str, object] = {}
    original_restore = runtime_progress.ProgressStore._restore_rematerialized_artifacts_from_runtime_state

    def stop_after_committed_generation_replay(self) -> None:
        replayed_public_snapshot["results"] = json.loads((output_dir / "results.json").read_text(encoding="utf-8"))
        replayed_public_snapshot["leads"] = json.loads((output_dir / "leads.json").read_text(encoding="utf-8"))
        replayed_public_snapshot["summary"] = json.loads((output_dir / "summary.json").read_text(encoding="utf-8"))
        replayed_public_snapshot["public_publish_state"] = _public_publish_state(output_dir)
        raise RuntimeError("stop after committed generation replay")

    monkeypatch.setattr(
        runtime_progress.ProgressStore,
        "_restore_rematerialized_artifacts_from_runtime_state",
        stop_after_committed_generation_replay,
    )

    with pytest.raises(RuntimeError, match="stop after committed generation replay"):
        core.ProgressStore(output_dir)

    assert replayed_public_snapshot["results"] == initial_results
    assert replayed_public_snapshot["leads"] == initial_leads
    assert replayed_public_snapshot["summary"] == initial_summary
    assert replayed_public_snapshot["public_publish_state"] == crashed_publish_state
    results_jsonl_after_committed_replay = [
        json.loads(line)
        for line in (output_dir / "results.jsonl").read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    assert [item["inn"] for item in results_jsonl_after_committed_replay] == [first_row.inn]

    monkeypatch.setattr(
        runtime_progress.ProgressStore,
        "_restore_rematerialized_artifacts_from_runtime_state",
        original_restore,
    )
    reloaded = core.ProgressStore(output_dir)

    first_loaded_result = reloaded.get(first_row.inn)
    second_loaded_result = reloaded.get(second_row.inn)
    assert first_loaded_result is not None
    assert second_loaded_result is not None
    assert first_loaded_result["status"] == "completed"
    assert second_loaded_result["status"] == "completed"

    rematerialized_results = json.loads((output_dir / "results.json").read_text(encoding="utf-8"))
    assert [item["inn"] for item in rematerialized_results] == [first_row.inn, second_row.inn]

    rematerialized_leads = json.loads((output_dir / "leads.json").read_text(encoding="utf-8"))
    assert len(rematerialized_leads) == 1
    assert rematerialized_leads[0]["title"] == "Lead 6"
    assert rematerialized_leads[0]["company_row_index"] == second_row.row_index

    rematerialized_summary = json.loads((output_dir / "summary.json").read_text(encoding="utf-8"))
    assert rematerialized_summary["processed_rows"] == 2
    assert rematerialized_summary["completed_rows"] == 2
    assert rematerialized_summary["remaining_rows"] == 0
    rematerialized_publish_state = _public_publish_state(output_dir)
    assert rematerialized_publish_state["active_generation_id"] != ""
    assert rematerialized_publish_state["active_generation_id"] == rematerialized_publish_state[
        "committed_generation_id"
    ]
    assert rematerialized_publish_state["committed_generation_id"] != crashed_publish_state[
        "active_generation_id"
    ]

    results_jsonl_after_reload = [
        json.loads(line)
        for line in (output_dir / "results.jsonl").read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    assert [item["inn"] for item in results_jsonl_after_reload] == [first_row.inn]


def test_progress_store_keeps_incomplete_runtime_rows_out_of_public_outputs(
    tmp_path: Path,
) -> None:
    output_dir = tmp_path / "output"
    output_dir.mkdir()
    row, payload, result_payload = _incomplete_runtime_state_payload()
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

    summary = json.loads((output_dir / "summary.json").read_text(encoding="utf-8"))
    assert summary["processed_rows"] == 0
    assert summary["completed_rows"] == 0
    assert summary["remaining_rows"] == 1
    assert summary["public_output_contract"]["terminal_run"] is False
    assert summary["public_output_contract"]["all_selected_completed"] is False
    assert summary["public_output_contract"]["final_exports"]["available"] is False
    assert summary["public_output_contract"]["final_exports"]["state"] == "suppressed_until_run_finished"
    assert not (output_dir / "results.json").exists()
    assert not (output_dir / "leads.json").exists()
    assert not (output_dir / "availability_summary.json").exists()
    assert not (output_dir / "report.md").exists()
    assert not (output_dir / "leads.md").exists()
    assert not (output_dir / "insights.md").exists()
    assert not (output_dir / "final_results.csv").exists()
    assert not (output_dir / "final_results.xlsx").exists()
    assert not any((output_dir / "company_reports").glob("*.md"))


def test_progress_store_reload_preserves_controlled_stop_partial_progress_surfaces(
    tmp_path: Path,
) -> None:
    output_dir = tmp_path / "output"
    output_dir.mkdir()

    progress = core.ProgressStore(output_dir)
    progress.run_started(
        input_path="input.xlsx",
        total_rows=2,
        selected_rows=2,
        selection_mode="window",
        selected_ordinals=[1, 2],
        start_from=1,
        end_at=2,
        active_sources=["spark"],
    )

    first_row = core.RowInput(row_index=1, inn="7700000011", company_name="Factory 11")
    second_row = core.RowInput(row_index=2, inn="7700000012", company_name="Factory 12")

    first_result = core.build_company_result(first_row)
    first_result.status = "completed"
    first_result.finished_at = core.utc_now_iso()
    first_result.sources["spark"] = core.SourceResult(source="spark", status="success")
    progress.persist_completed_company_result(first_result, total_rows=2, processed_rows=1)

    progress.materialize_stage_work_unit(
        inn=second_row.inn,
        row_index=second_row.row_index,
        execution_boundary="deep_site_parse",
        work_unit_payload={
            "inn": second_row.inn,
            "row_index": second_row.row_index,
            "company_name": second_row.company_name,
            "candidate_sites": [{"site_url": "https://factory-12.example"}],
            "deep_parse_sites": ["https://factory-12.example/about"],
        },
    )

    stop_request = progress.request_controlled_stop(
        reason="operator requested safe stop",
        requested_at="2026-04-21T18:05:00+00:00",
    )
    consumed_stop_request = progress.consume_controlled_stop_request()
    assert consumed_stop_request == stop_request

    progress.run_finished(
        processed_rows=1,
        controlled_stop=True,
        stop_request=consumed_stop_request,
    )

    summary_before_reload = json.loads((output_dir / "summary.json").read_text(encoding="utf-8"))
    assert summary_before_reload["run_status"] == "controlled_stop"
    assert summary_before_reload["finish_reason"] == "controlled_stop"
    assert summary_before_reload["completed_rows"] == 1
    assert summary_before_reload["remaining_rows"] == 1
    assert summary_before_reload["stop_requested_at"] == "2026-04-21T18:05:00+00:00"
    assert summary_before_reload["stop_reason"] == "operator requested safe stop"

    reloaded = core.ProgressStore(output_dir)

    summary_after_reload = json.loads((output_dir / "summary.json").read_text(encoding="utf-8"))
    assert summary_after_reload["run_status"] == "controlled_stop"
    assert summary_after_reload["finish_reason"] == "controlled_stop"
    assert summary_after_reload["completed_rows"] == 1
    assert summary_after_reload["remaining_rows"] == 1
    assert summary_after_reload["stop_reason"] == "operator requested safe stop"

    report = (output_dir / "report.md").read_text(encoding="utf-8")
    insights = (output_dir / "insights.md").read_text(encoding="utf-8")
    leads = (output_dir / "leads.md").read_text(encoding="utf-8")
    assert "Run status: `controlled_stop`" in report
    assert "Run status: `controlled_stop`" in insights
    assert "Run status: `controlled_stop`" in leads
    assert "Public outputs include only completed companies" in report

    rematerialized_results = json.loads((output_dir / "results.json").read_text(encoding="utf-8"))
    assert [item["inn"] for item in rematerialized_results] == [first_row.inn]
    assert reloaded.get(first_row.inn) is not None
    assert reloaded.get(second_row.inn) is None

    pending_work_units = reloaded.pending_stage_work_units(execution_boundary="deep_site_parse")
    assert [item["inn"] for item in pending_work_units] == [second_row.inn]

    rematerialized_runtime_state = json.loads((output_dir / "runtime_state.json").read_text(encoding="utf-8"))
    assert rematerialized_runtime_state["run"]["summary"]["run_status"] == "controlled_stop"
    assert rematerialized_runtime_state["run"]["metadata"]["run_status"] == "controlled_stop"
    assert rematerialized_runtime_state["run"]["metadata"]["stop_reason"] == "operator requested safe stop"
    assert rematerialized_runtime_state["run"]["metadata"]["stage_work_units"]["deep_parse"]["companies"][
        second_row.inn
    ]["work_status"] == "pending"


def test_progress_store_reload_preserves_aborted_terminal_contract_fields(
    tmp_path: Path,
) -> None:
    output_dir = tmp_path / "output"
    output_dir.mkdir()

    progress = core.ProgressStore(output_dir)
    progress.run_started(
        input_path="input.xlsx",
        total_rows=2,
        selected_rows=2,
        selection_mode="window",
        selected_ordinals=[1, 2],
        start_from=1,
        end_at=2,
        active_sources=["spark"],
    )

    first_row = core.RowInput(row_index=1, inn="7700000021", company_name="Factory 21")
    first_result = core.build_company_result(first_row)
    first_result.status = "completed"
    first_result.finished_at = core.utc_now_iso()
    first_result.sources["spark"] = core.SourceResult(source="spark", status="success")
    progress.persist_completed_company_result(first_result, total_rows=2, processed_rows=1)

    progress.run_finished(
        processed_rows=0,
        run_status="aborted",
        finish_reason="aborted",
        terminal_context={
            "checkpoint": "explicit_boundary",
            "inn": first_row.inn,
            "execution_boundary": "aggregator_site",
        },
        terminal_error={
            "type": "RuntimeError",
            "message": "simulated abort after persist",
        },
    )

    summary_before_reload = json.loads((output_dir / "summary.json").read_text(encoding="utf-8"))
    assert summary_before_reload["processed_rows"] == 1
    assert summary_before_reload["completed_rows"] == 1
    assert summary_before_reload["remaining_rows"] == 1
    assert summary_before_reload["run_status"] == "aborted"
    assert summary_before_reload["finish_reason"] == "aborted"
    assert summary_before_reload["terminal_checkpoint"] == "explicit_boundary"
    assert summary_before_reload["terminal_inn"] == first_row.inn
    assert summary_before_reload["terminal_boundary"] == "aggregator_site"
    assert summary_before_reload["terminal_error_type"] == "RuntimeError"
    assert summary_before_reload["terminal_error_message"] == "simulated abort after persist"

    reloaded = core.ProgressStore(output_dir)

    summary_after_reload = json.loads((output_dir / "summary.json").read_text(encoding="utf-8"))
    assert summary_after_reload["processed_rows"] == 1
    assert summary_after_reload["run_status"] == "aborted"
    assert summary_after_reload["finish_reason"] == "aborted"
    assert summary_after_reload["terminal_checkpoint"] == "explicit_boundary"
    assert summary_after_reload["terminal_error_type"] == "RuntimeError"
    assert summary_after_reload["terminal_error_message"] == "simulated abort after persist"

    rematerialized_results = json.loads((output_dir / "results.json").read_text(encoding="utf-8"))
    assert [item["inn"] for item in rematerialized_results] == [first_row.inn]
    assert reloaded.get(first_row.inn) is not None

    rematerialized_runtime_state = json.loads((output_dir / "runtime_state.json").read_text(encoding="utf-8"))
    assert rematerialized_runtime_state["run"]["summary"]["run_status"] == "aborted"
    assert rematerialized_runtime_state["run"]["metadata"]["run_status"] == "aborted"
    assert rematerialized_runtime_state["run"]["metadata"]["terminal_checkpoint"] == "explicit_boundary"
    assert rematerialized_runtime_state["run"]["metadata"]["terminal_boundary"] == "aggregator_site"
    assert rematerialized_runtime_state["run"]["metadata"]["terminal_error_message"] == (
        "simulated abort after persist"
    )


def test_progress_store_reload_rematerializes_required_source_terminal_outputs_without_completed_results(
    tmp_path: Path,
) -> None:
    output_dir = tmp_path / "output"
    output_dir.mkdir()

    progress = core.ProgressStore(output_dir)
    progress.run_started(
        input_path="input.xlsx",
        total_rows=2,
        selected_rows=2,
        selection_mode="window",
        selected_ordinals=[1, 2],
        start_from=1,
        end_at=2,
        active_sources=["spark", "zachestnyibiznes", "rusprofile", "checko", "list_org"],
    )

    stop_reason = core.build_required_source_fail_fast_reason(
        "checko",
        core.REQUEST_STATUS_BLOCKED_NO_PROXY,
        access_mode="proxy-bound",
        detail=core.REQUEST_BLOCKED_NO_PROXY_NO_POOL_REASON,
    )
    progress.run_finished(
        processed_rows=0,
        run_status="failed_required_source",
        finish_reason="required_source_not_operational",
        stop_request={
            "requested_at": core.utc_now_iso(),
            "reason": stop_reason,
        },
        terminal_context={
            "checkpoint": "source_collect",
            "inn": "7700000099",
            "execution_boundary": "",
            "source": "checko",
            "source_status": core.REQUEST_STATUS_BLOCKED_NO_PROXY,
            "source_access_mode": "proxy-bound",
        },
        terminal_error={
            "type": "required_source_not_operational",
            "message": stop_reason,
        },
    )

    assert json.loads((output_dir / "results.json").read_text(encoding="utf-8")) == []
    assert json.loads((output_dir / "leads.json").read_text(encoding="utf-8")) == []

    for path in (
        output_dir / "summary.json",
        output_dir / "results.json",
        output_dir / "leads.json",
        output_dir / "availability_summary.json",
        output_dir / "report.md",
        output_dir / "leads.md",
        output_dir / "insights.md",
        output_dir / "final_results.csv",
        output_dir / "final_results.xlsx",
    ):
        path.unlink(missing_ok=True)

    reloaded = core.ProgressStore(output_dir)

    rematerialized_summary = json.loads((output_dir / "summary.json").read_text(encoding="utf-8"))
    assert rematerialized_summary["processed_rows"] == 0
    assert rematerialized_summary["completed_rows"] == 0
    assert rematerialized_summary["remaining_rows"] == 2
    assert rematerialized_summary["run_status"] == "failed_required_source"
    assert rematerialized_summary["finish_reason"] == "required_source_not_operational"
    assert rematerialized_summary["stop_reason"] == runtime_progress.REQUIRED_SOURCE_RED_FLAG_STOP_REASON
    assert rematerialized_summary["terminal_source"] == "checko"
    assert rematerialized_summary["terminal_source_status"] == core.REQUEST_STATUS_BLOCKED_NO_PROXY
    assert rematerialized_summary["terminal_source_access_mode"] == "proxy-bound"
    assert rematerialized_summary["terminal_error_message"] == stop_reason

    rematerialized_results = json.loads((output_dir / "results.json").read_text(encoding="utf-8"))
    assert rematerialized_results == []
    assert json.loads((output_dir / "leads.json").read_text(encoding="utf-8")) == []
    availability_summary = json.loads((output_dir / "availability_summary.json").read_text(encoding="utf-8"))
    assert availability_summary["sources"] == {}
    assert availability_summary["updated_at"] != ""

    report = (output_dir / "report.md").read_text(encoding="utf-8")
    insights = (output_dir / "insights.md").read_text(encoding="utf-8")
    assert "Run status: `failed_required_source`" in report
    assert "Terminal source: `checko`" in report
    assert "Terminal source access mode: `proxy-bound`" in report
    assert "Run status: `failed_required_source`" in insights

    rematerialized_runtime_state = json.loads((output_dir / "runtime_state.json").read_text(encoding="utf-8"))
    assert rematerialized_runtime_state["run"]["summary"]["run_status"] == "failed_required_source"
    assert rematerialized_runtime_state["run"]["summary"]["stop_reason"] == (
        runtime_progress.REQUIRED_SOURCE_RED_FLAG_STOP_REASON
    )
    assert rematerialized_runtime_state["run"]["summary"]["terminal_source"] == "checko"
    assert rematerialized_runtime_state["run"]["metadata"]["stop_reason"] == (
        runtime_progress.REQUIRED_SOURCE_RED_FLAG_STOP_REASON
    )
    assert rematerialized_runtime_state["run"]["metadata"]["terminal_source_status"] == (
        core.REQUEST_STATUS_BLOCKED_NO_PROXY
    )
    assert reloaded.get("7700000099") is None

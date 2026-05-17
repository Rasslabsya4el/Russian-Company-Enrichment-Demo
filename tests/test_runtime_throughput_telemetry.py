from __future__ import annotations

import json
import time
from datetime import datetime, timedelta, timezone
from threading import Event, Thread
from types import SimpleNamespace

import company_enrichment_core as core

from app.runtime import ProgressStore
from app.runtime.bounded_executor import open_company_source_search_executor
from app.runtime.queue_families import (
    CANDIDATE_SITE_STAGE_NAME,
    DEEP_PARSE_STAGE_NAME,
    EXTRA_CHECK_STAGE_NAME,
    FACTORY_SITE_STAGE_NAME,
    LLM_STAGE_NAME,
    OCR_STAGE_NAME,
    build_downstream_worker_pool_contour,
)
from app.runtime.stage_pools import (
    SourceLaneTelemetryLedger,
    StagePoolGovernor,
    build_throughput_telemetry_payload,
)
from app.runtime.work_units import AGGREGATOR_SITE_EXECUTION_BOUNDARY


def _source_lane_scheduler(*, source_name: str, capacity_boundary: str, worker_lane_budget: int) -> dict[str, object]:
    return {
        "effective_company_concurrency_cap": 1,
        "source_lane_contour": [
            {
                "source_name": source_name,
                "transport_policy": "live-network",
                "scheduler_lane": "serial_inline",
                "network_surface": "live-network",
                "contour_state": "serial_inline_only",
                "capacity_boundary": capacity_boundary,
                "reason": capacity_boundary,
                "requested_company_concurrency": 1,
                "source_capacity_cap": 1,
                "source_lane_budget": 1,
                "worker_lane_budget": worker_lane_budget,
                "host_cap": 1,
                "host_aliases": [],
            }
        ],
    }


def _rows(count: int) -> list[core.RowInput]:
    return [
        core.RowInput(
            row_index=index + 1,
            inn=f"{index + 1:010d}",
            company_name=f"Company {index + 1}",
        )
        for index in range(count)
    ]


def test_build_throughput_telemetry_payload_exposes_boundary_and_stage_blockers() -> None:
    source_lane_scheduler = {
        "effective_company_concurrency_cap": 1,
        "source_lane_contour": [
            {
                "source_name": "rusprofile",
                "transport_policy": "session-bound",
                "scheduler_lane": "session_serial_inline",
                "network_surface": "live-network",
                "contour_state": "serial_inline_only",
                "capacity_boundary": "session_bound_serial_lane",
                "reason": "session-bound source remains on the runner-owned serial lane",
                "requested_company_concurrency": 1,
                "source_capacity_cap": 1,
                "source_lane_budget": 1,
                "worker_lane_budget": 0,
                "host_cap": 1,
                "host_aliases": ["rusprofile.ru"],
            },
            {
                "source_name": "checko",
                "transport_policy": "proxy-bound",
                "scheduler_lane": "proxy_serial_inline",
                "network_surface": "live-network",
                "contour_state": "serial_inline_only",
                "capacity_boundary": "proxy_bound_serial_lane",
                "reason": "proxy-bound source remains on the runner-owned serial lane",
                "requested_company_concurrency": 1,
                "source_capacity_cap": 1,
                "source_lane_budget": 1,
                "worker_lane_budget": 0,
                "host_cap": 1,
                "host_aliases": ["checko.ru"],
            },
        ],
    }
    downstream_worker_pools = build_downstream_worker_pool_contour(company_concurrency_cap=1).as_payload()
    source_lane_telemetry = SourceLaneTelemetryLedger(source_lane_scheduler=source_lane_scheduler)
    source_lane_telemetry.seed_queue_depths({"rusprofile": 2, "checko": 1})
    source_lane_telemetry.update_backpressure(
        ["checko"],
        active=True,
        reason="ready_queue_limit",
        ready_queue_depth=1,
        ready_queue_limit=1,
        blocked_submissions_delta=1,
    )

    payload = build_throughput_telemetry_payload(
        source_lane_scheduler=source_lane_scheduler,
        downstream_worker_pools=downstream_worker_pools,
        source_lane_runtime=source_lane_telemetry.snapshot(),
        downstream_stage_runtime={
            DEEP_PARSE_STAGE_NAME: {
                "worker_budget": 1,
                "queue_depth": 1,
                "inflight": 1,
                "completed": 0,
                "wait_pressure": {
                    "active": True,
                    "waiters": 1,
                    "events": 1,
                    "last_reason": "stage_budget_saturated",
                },
            }
        },
        stage_backlog={
            CANDIDATE_SITE_STAGE_NAME: 1,
            DEEP_PARSE_STAGE_NAME: 2,
        },
        host_governor_runtime={
            "queue_depth": 1,
            "cooldown_hosts": 1,
            "wait_pressure": {
                "active": True,
                "waiters": 1,
                "events": 1,
                "last_reason": "cooldown_active",
            },
            "hosts": {
                "alpha.example": {
                    "active_leases": 0,
                    "cooldown_remaining_seconds": 9.0,
                }
            },
        },
        backpressure_policy={
            "source_set": ["rusprofile", "checko"],
            "drops_sources": False,
            "safe_only_fallback": False,
        },
        rows_completed=0,
    )

    assert payload["source_lanes"]["rusprofile"]["capacity_boundary"] == "session_bound_serial_lane"
    assert payload["source_lanes"]["rusprofile"]["queue_depth"] == 2
    assert payload["source_lanes"]["checko"]["backpressure"]["reason"] == "ready_queue_limit"
    assert payload["downstream_stage_pools"][DEEP_PARSE_STAGE_NAME]["queue_depth"] == 2
    blocked_reasons = {
        (item["scope"], item["name"], item["reason"])
        for item in payload["snapshot"]["blocked_on"]
    }
    assert ("source_lane", "rusprofile", "session_bound_serial_lane") in blocked_reasons
    assert ("source_lane", "checko", "ready_queue_limit") in blocked_reasons
    assert ("downstream_stage_pool", DEEP_PARSE_STAGE_NAME, "stage_pool_saturated") in blocked_reasons
    assert ("boundary_wait", "host_governor", "cooldown_active") in blocked_reasons


def test_downstream_worker_pool_contour_accepts_module_specific_budgets() -> None:
    contour = build_downstream_worker_pool_contour(
        company_concurrency_cap=3,
        candidate_site_concurrency=4,
        deep_parse_concurrency=5,
        factory_site_concurrency=6,
        ocr_concurrency=2,
        llm_concurrency=3,
        extra_check_concurrency=4,
    )

    budgets = contour.per_stage_budget_map()
    assert budgets[CANDIDATE_SITE_STAGE_NAME] == 4
    assert budgets[DEEP_PARSE_STAGE_NAME] == 5
    assert budgets[FACTORY_SITE_STAGE_NAME] == 6
    assert budgets[OCR_STAGE_NAME] == 2
    assert budgets[LLM_STAGE_NAME] == 3
    assert budgets[EXTRA_CHECK_STAGE_NAME] == 4


def test_stage_pool_governor_snapshot_tracks_wait_pressure_and_completed_work() -> None:
    governor = StagePoolGovernor(
        per_stage_budget_map={DEEP_PARSE_STAGE_NAME: 1},
        active_poll_seconds=0.01,
    )
    release_second_lease = Event()
    second_lease_entered = Event()

    def waiting_worker() -> None:
        with governor.lease(DEEP_PARSE_STAGE_NAME):
            second_lease_entered.set()
            release_second_lease.wait(timeout=1)

    with governor.lease(DEEP_PARSE_STAGE_NAME):
        worker = Thread(target=waiting_worker)
        worker.start()
        deadline = time.time() + 1.0
        waiting_snapshot = governor.snapshot()[DEEP_PARSE_STAGE_NAME]
        while time.time() < deadline:
            waiting_snapshot = governor.snapshot()[DEEP_PARSE_STAGE_NAME]
            if waiting_snapshot["wait_pressure"]["active"]:
                break
            time.sleep(0.01)
        assert waiting_snapshot["inflight"] == 1
        assert waiting_snapshot["queue_depth"] == 1
        assert waiting_snapshot["wait_pressure"]["waiters"] == 1
        time.sleep(0.05)
        release_second_lease.set()
    worker.join(timeout=1)
    assert second_lease_entered.is_set()

    final_snapshot = governor.snapshot()[DEEP_PARSE_STAGE_NAME]
    assert final_snapshot["inflight"] == 0
    assert final_snapshot["completed"] == 2
    assert final_snapshot["wait_pressure"]["events"] >= 1
    assert final_snapshot["wait_pressure"]["max_seconds"] > 0


def test_progress_store_persists_throughput_telemetry_surface_across_reload(tmp_path) -> None:
    output_dir = tmp_path / "output"
    progress = ProgressStore(output_dir)
    source_lane_scheduler = _source_lane_scheduler(
        source_name="spark",
        capacity_boundary="direct_default_worker_lane",
        worker_lane_budget=2,
    )
    downstream_worker_pools = build_downstream_worker_pool_contour(company_concurrency_cap=2).as_payload()
    source_lane_telemetry = SourceLaneTelemetryLedger(source_lane_scheduler=source_lane_scheduler)
    source_lane_telemetry.seed_queue_depths({"spark": 1})
    source_stage_governor = StagePoolGovernor(per_stage_budget_map={"spark": 2})
    downstream_stage_governor = StagePoolGovernor(
        per_stage_budget_map={
            CANDIDATE_SITE_STAGE_NAME: 2,
            DEEP_PARSE_STAGE_NAME: 2,
        }
    )
    initial_telemetry = build_throughput_telemetry_payload(
        source_lane_scheduler=source_lane_scheduler,
        downstream_worker_pools=downstream_worker_pools,
        source_lane_runtime=source_lane_telemetry.snapshot(),
        source_stage_runtime=source_stage_governor.snapshot(),
        downstream_stage_runtime=downstream_stage_governor.snapshot(),
        backpressure_policy={
            "source_set": ["spark"],
            "drops_sources": False,
            "safe_only_fallback": False,
        },
        rows_completed=0,
    )

    progress.run_started(
        input_path="input.xlsx",
        total_rows=1,
        selected_rows=1,
        selection_mode="ordinals",
        selected_ordinals=[1],
        start_from=1,
        end_at=1,
        active_sources=["spark"],
        source_lane_scheduler=source_lane_scheduler,
        downstream_worker_pools=downstream_worker_pools,
        throughput_telemetry=initial_telemetry,
    )
    progress.materialize_stage_work_unit(
        inn="7700000001",
        row_index=1,
        execution_boundary=AGGREGATOR_SITE_EXECUTION_BOUNDARY,
        work_unit_payload={
            "inn": "7700000001",
            "row_index": 1,
            "company_name": "Factory 1",
            "candidate_sites": [{"site_url": "https://factory-1.example"}],
        },
    )
    updated_telemetry = build_throughput_telemetry_payload(
        source_lane_scheduler=source_lane_scheduler,
        downstream_worker_pools=downstream_worker_pools,
        source_lane_runtime=source_lane_telemetry.snapshot(),
        source_stage_runtime=source_stage_governor.snapshot(),
        downstream_stage_runtime=downstream_stage_governor.snapshot(),
        stage_backlog={CANDIDATE_SITE_STAGE_NAME: 1},
        backpressure_policy={
            "source_set": ["spark"],
            "drops_sources": False,
            "safe_only_fallback": False,
        },
        rows_completed=0,
    )
    progress.update_throughput_telemetry(updated_telemetry)

    summary = json.loads((output_dir / "summary.json").read_text(encoding="utf-8"))
    runtime_state = json.loads((output_dir / "runtime_state.json").read_text(encoding="utf-8"))
    telemetry = summary["throughput_telemetry"]
    assert telemetry["downstream_stage_pools"][CANDIDATE_SITE_STAGE_NAME]["queue_depth"] == 1
    assert runtime_state["run"]["summary"]["throughput_telemetry"] == telemetry
    assert runtime_state["run"]["metadata"]["throughput_telemetry"] == telemetry

    ProgressStore(output_dir)
    reloaded_summary = json.loads((output_dir / "summary.json").read_text(encoding="utf-8"))
    assert reloaded_summary["throughput_telemetry"] == telemetry


def test_progress_store_tracks_runtime_clock_and_downstream_drain_spans(tmp_path) -> None:
    output_dir = tmp_path / "output"
    progress = ProgressStore(output_dir)
    source_lane_scheduler = _source_lane_scheduler(
        source_name="spark",
        capacity_boundary="direct_default_worker_lane",
        worker_lane_budget=1,
    )
    downstream_worker_pools = build_downstream_worker_pool_contour(company_concurrency_cap=1).as_payload()
    progress.run_started(
        input_path="input.xlsx",
        total_rows=1,
        selected_rows=1,
        selection_mode="ordinals",
        selected_ordinals=[1],
        start_from=1,
        end_at=1,
        active_sources=["spark"],
        source_lane_scheduler=source_lane_scheduler,
        downstream_worker_pools=downstream_worker_pools,
        throughput_telemetry=build_throughput_telemetry_payload(
            source_lane_scheduler=source_lane_scheduler,
            downstream_worker_pools=downstream_worker_pools,
            backpressure_policy={"source_set": ["spark"]},
            rows_completed=0,
        ),
    )
    progress.emit_stage_message(
        message_type="source_result_ready",
        stage="source_collect",
        inn="7700000001",
        row_index=1,
        payload={"source": "spark", "status": "ok", "duration_seconds": 7.5},
        ts=datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
    )
    work_unit = progress.materialize_stage_work_unit(
        inn="7700000001",
        row_index=1,
        execution_boundary=AGGREGATOR_SITE_EXECUTION_BOUNDARY,
        work_unit_payload={
            "inn": "7700000001",
            "row_index": 1,
            "company_name": "Factory 1",
            "candidate_sites": [{"site_url": "https://factory-1.example"}],
        },
    )
    started_at = datetime.now(timezone.utc).replace(microsecond=0)
    finished_at = started_at + timedelta(seconds=13)
    acknowledged_at = started_at + timedelta(seconds=20)
    progress.append_event(
        {
            "ts": finished_at.isoformat(),
            "type": "downstream_stage_span",
            "inn": "7700000001",
            "row_index": 1,
            "company_name": "Factory 1",
            "stage": CANDIDATE_SITE_STAGE_NAME,
            "execution_boundary": AGGREGATOR_SITE_EXECUTION_BOUNDARY,
            "handoff_fingerprint": work_unit["handoff_fingerprint"],
            "started_at": started_at.isoformat(),
            "finished_at": finished_at.isoformat(),
            "elapsed_seconds": 13.0,
            "status": "completed",
        }
    )
    assert progress.ack_stage_handoff_work_unit(
        inn="7700000001",
        handoff_fingerprint=work_unit["handoff_fingerprint"],
        acknowledged_at=acknowledged_at.isoformat(),
    )

    runtime_state = json.loads((output_dir / "runtime_state.json").read_text(encoding="utf-8"))
    telemetry = runtime_state["run"]["metadata"]["throughput_telemetry"]
    runtime_clock = telemetry["runtime_clock"]
    assert runtime_clock["wall_clock_elapsed_seconds"] >= runtime_clock["active_elapsed_seconds"]
    assert runtime_clock["acceptance_grade_speed_evidence"] is True
    source_collection = telemetry["source_collection"]
    source_slow = source_collection["slow_summary"]
    assert source_slow["top_company_source_collection"][0]["inn"] == "7700000001"
    assert source_slow["top_company_source_collection"][0]["total_duration_seconds"] == 7.5
    assert source_slow["source_totals_by_source"][0]["source"] == "spark"
    downstream_drain = telemetry["downstream_drain"]
    company = downstream_drain["companies"]["7700000001"]
    assert company["stages"][CANDIDATE_SITE_STAGE_NAME]["total_elapsed_seconds"] == 13.0
    assert company["stage_execution"]["total_elapsed_seconds"] == 13.0
    assert company["actual_stage_execution_seconds"] == 13.0
    assert company["final_ack"]["elapsed_since_first_stage_seconds"] == 20.0
    assert downstream_drain["largest_stage_span"]["stage"] == CANDIDATE_SITE_STAGE_NAME
    slow_summary = downstream_drain["slow_summary"]
    assert slow_summary["top_company_stage_execution"][0]["inn"] == "7700000001"
    assert slow_summary["top_company_stage_execution"][0]["dominant_stage"]["stage"] == CANDIDATE_SITE_STAGE_NAME
    assert slow_summary["stage_totals_by_stage"][0]["stage"] == CANDIDATE_SITE_STAGE_NAME


def test_progress_store_source_collection_reports_interval_union(tmp_path) -> None:
    output_dir = tmp_path / "output"
    progress = ProgressStore(output_dir)
    source_lane_scheduler = _source_lane_scheduler(
        source_name="checko",
        capacity_boundary="proxy_bound_worker_lane",
        worker_lane_budget=2,
    )
    progress.run_started(
        input_path="input.xlsx",
        total_rows=1,
        selected_rows=1,
        selection_mode="ordinals",
        selected_ordinals=[1],
        start_from=1,
        end_at=1,
        active_sources=["spark", "checko"],
        source_lane_scheduler=source_lane_scheduler,
        downstream_worker_pools=build_downstream_worker_pool_contour(company_concurrency_cap=1).as_payload(),
        throughput_telemetry=build_throughput_telemetry_payload(
            source_lane_scheduler=source_lane_scheduler,
            rows_completed=0,
        ),
    )
    started_at = datetime(2026, 5, 7, 10, 0, 0, tzinfo=timezone.utc)
    progress.emit_stage_message(
        message_type="source_result_ready",
        stage="source_collect",
        inn="7700000001",
        row_index=1,
        payload={
            "source": "spark",
            "status": "ok",
            "duration_seconds": 7.5,
            "started_at": started_at.isoformat(),
            "finished_at": (started_at + timedelta(seconds=7.5)).isoformat(),
        },
        ts=(started_at + timedelta(seconds=7.5)).isoformat(),
    )
    progress.emit_stage_message(
        message_type="source_result_ready",
        stage="source_collect",
        inn="7700000001",
        row_index=1,
        payload={
            "source": "checko",
            "status": "success",
            "duration_seconds": 5.0,
            "started_at": (started_at + timedelta(seconds=3)).isoformat(),
            "finished_at": (started_at + timedelta(seconds=8)).isoformat(),
        },
        ts=(started_at + timedelta(seconds=8)).isoformat(),
    )

    runtime_state = json.loads((output_dir / "runtime_state.json").read_text(encoding="utf-8"))
    source_collection = runtime_state["run"]["metadata"]["throughput_telemetry"]["source_collection"]
    company_collection = source_collection["companies"]["7700000001"]["source_collection"]
    assert company_collection["total_duration_seconds"] == 12.5
    assert company_collection["wall_clock_elapsed_seconds"] == 8.0
    assert company_collection["additive_overlap_seconds"] == 4.5
    assert company_collection["interval_count"] == 2
    totals = {
        item["source"]: item
        for item in source_collection["slow_summary"]["source_totals_by_source"]
    }
    assert totals["spark"]["wall_clock_elapsed_seconds"] == 7.5
    assert totals["checko"]["wall_clock_elapsed_seconds"] == 5.0
    assert totals["checko"]["interval_count"] == 1


def test_progress_store_separates_ordered_ack_final_drain_and_public_materialization_waits(tmp_path) -> None:
    output_dir = tmp_path / "output"
    progress = ProgressStore(output_dir)
    downstream_worker_pools = build_downstream_worker_pool_contour(company_concurrency_cap=1).as_payload()
    progress.run_started(
        input_path="input.xlsx",
        total_rows=2,
        selected_rows=2,
        selection_mode="ordinals",
        selected_ordinals=[1, 2],
        start_from=1,
        end_at=2,
        active_sources=["spark"],
        downstream_worker_pools=downstream_worker_pools,
        throughput_telemetry=build_throughput_telemetry_payload(
            downstream_worker_pools=downstream_worker_pools,
            rows_completed=0,
        ),
    )
    work_unit = progress.materialize_stage_work_unit(
        inn="7700000002",
        row_index=2,
        execution_boundary=AGGREGATOR_SITE_EXECUTION_BOUNDARY,
        work_unit_payload={
            "inn": "7700000002",
            "row_index": 2,
            "company_name": "Cheap Later Factory",
            "candidate_sites": [{"site_url": "https://factory-2.example"}],
        },
    )
    started_at = datetime(2026, 4, 28, 10, 0, 0, tzinfo=timezone.utc)
    finished_at = started_at + timedelta(seconds=1)
    ordered_drain_started_at = started_at + timedelta(seconds=125)
    final_drain_finished_at = started_at + timedelta(seconds=126)
    public_started_at = started_at + timedelta(seconds=127)
    public_finished_at = started_at + timedelta(seconds=129)
    acknowledged_at = started_at + timedelta(seconds=130)

    progress.append_event(
        {
            "ts": finished_at.isoformat(),
            "type": "downstream_stage_span",
            "inn": "7700000002",
            "row_index": 2,
            "company_name": "Cheap Later Factory",
            "stage": CANDIDATE_SITE_STAGE_NAME,
            "execution_boundary": AGGREGATOR_SITE_EXECUTION_BOUNDARY,
            "handoff_fingerprint": work_unit["handoff_fingerprint"],
            "started_at": started_at.isoformat(),
            "finished_at": finished_at.isoformat(),
            "elapsed_seconds": 1.0,
            "status": "completed",
        }
    )
    assert progress.ack_stage_handoff_work_unit(
        inn="7700000002",
        handoff_fingerprint=work_unit["handoff_fingerprint"],
        acknowledged_at=acknowledged_at.isoformat(),
    )
    assert progress.record_downstream_finalization_timing(
        inn="7700000002",
        row_index=2,
        company_name="Cheap Later Factory",
        handoff_fingerprint=work_unit["handoff_fingerprint"],
        ordered_drain_started_at=ordered_drain_started_at.isoformat(),
        downstream_ready_at=finished_at.isoformat(),
        final_drain_wait_started_at=ordered_drain_started_at.isoformat(),
        final_drain_wait_finished_at=final_drain_finished_at.isoformat(),
        final_drain_wait_seconds=1.0,
        public_materialization_started_at=public_started_at.isoformat(),
        public_materialization_finished_at=public_finished_at.isoformat(),
    )
    overlapping_work_unit = progress.materialize_stage_work_unit(
        inn="7700000003",
        row_index=3,
        execution_boundary=AGGREGATOR_SITE_EXECUTION_BOUNDARY,
        work_unit_payload={
            "inn": "7700000003",
            "row_index": 3,
            "company_name": "Overlapping Later Factory",
            "candidate_sites": [{"site_url": "https://factory-3.example"}],
        },
    )
    overlapping_started_at = started_at + timedelta(seconds=2)
    overlapping_finished_at = started_at + timedelta(seconds=3)
    overlapping_ordered_drain_started_at = started_at + timedelta(seconds=63)
    overlapping_final_drain_finished_at = started_at + timedelta(seconds=64)
    overlapping_public_started_at = started_at + timedelta(seconds=65)
    overlapping_public_finished_at = started_at + timedelta(seconds=66)
    overlapping_acknowledged_at = started_at + timedelta(seconds=67)
    progress.append_event(
        {
            "ts": overlapping_finished_at.isoformat(),
            "type": "downstream_stage_span",
            "inn": "7700000003",
            "row_index": 3,
            "company_name": "Overlapping Later Factory",
            "stage": CANDIDATE_SITE_STAGE_NAME,
            "execution_boundary": AGGREGATOR_SITE_EXECUTION_BOUNDARY,
            "handoff_fingerprint": overlapping_work_unit["handoff_fingerprint"],
            "started_at": overlapping_started_at.isoformat(),
            "finished_at": overlapping_finished_at.isoformat(),
            "elapsed_seconds": 1.0,
            "status": "completed",
        }
    )
    assert progress.ack_stage_handoff_work_unit(
        inn="7700000003",
        handoff_fingerprint=overlapping_work_unit["handoff_fingerprint"],
        acknowledged_at=overlapping_acknowledged_at.isoformat(),
    )
    assert progress.record_downstream_finalization_timing(
        inn="7700000003",
        row_index=3,
        company_name="Overlapping Later Factory",
        handoff_fingerprint=overlapping_work_unit["handoff_fingerprint"],
        ordered_drain_started_at=overlapping_ordered_drain_started_at.isoformat(),
        downstream_ready_at=overlapping_finished_at.isoformat(),
        final_drain_wait_started_at=overlapping_ordered_drain_started_at.isoformat(),
        final_drain_wait_finished_at=overlapping_final_drain_finished_at.isoformat(),
        final_drain_wait_seconds=1.0,
        public_materialization_started_at=overlapping_public_started_at.isoformat(),
        public_materialization_finished_at=overlapping_public_finished_at.isoformat(),
    )

    runtime_state = json.loads((output_dir / "runtime_state.json").read_text(encoding="utf-8"))
    downstream_drain = runtime_state["run"]["metadata"]["throughput_telemetry"]["downstream_drain"]
    company = downstream_drain["companies"]["7700000002"]
    assert company["stage_execution"]["total_elapsed_seconds"] == 1.0
    assert company["ordered_ack_wait"]["elapsed_seconds"] == 124.0
    assert company["final_drain_wait"]["elapsed_seconds"] == 1.0
    assert company["public_materialization"]["elapsed_seconds"] == 2.0
    assert company["final_ack"]["elapsed_since_last_stage_seconds"] == 129.0
    assert downstream_drain["largest_ordered_ack_wait"]["inn"] == "7700000002"
    assert downstream_drain["largest_final_drain_wait"]["inn"] == "7700000002"
    assert downstream_drain["largest_public_materialization"]["inn"] == "7700000002"
    phase_totals = {
        item["phase"]: item
        for item in downstream_drain["slow_summary"]["phase_totals_by_phase"]
    }
    ordered_ack_total = phase_totals["ordered_ack_wait"]
    assert ordered_ack_total["aggregate_mode"] == "per_company_additive"
    assert ordered_ack_total["elapsed_seconds_semantics"] == "sum_of_per_company_wait_intervals"
    assert ordered_ack_total["wall_clock_aggregate_mode"] == "overlap_adjusted_interval_union"
    assert ordered_ack_total["total_elapsed_seconds"] == 184.0
    assert ordered_ack_total["wall_clock_elapsed_seconds"] == 124.0
    assert ordered_ack_total["additive_overlap_seconds"] == 60.0
    assert ordered_ack_total["interval_count"] == 2


def test_progress_store_public_materialization_phase_timing_exposes_persistence_work(tmp_path) -> None:
    output_dir = tmp_path / "output"
    progress = ProgressStore(output_dir)
    downstream_worker_pools = build_downstream_worker_pool_contour(company_concurrency_cap=1).as_payload()
    progress.run_started(
        input_path="input.xlsx",
        total_rows=2,
        selected_rows=2,
        selection_mode="ordinals",
        selected_ordinals=[1, 2],
        start_from=1,
        end_at=2,
        active_sources=["spark"],
        downstream_worker_pools=downstream_worker_pools,
        throughput_telemetry=build_throughput_telemetry_payload(
            downstream_worker_pools=downstream_worker_pools,
            rows_completed=0,
        ),
    )

    row = core.RowInput(row_index=1, inn="7700000001", company_name="Phase Timed Factory")
    result = core.build_company_result(row)
    result.status = "completed"
    result.finished_at = "2026-05-07T10:00:04+00:00"
    result.sources["spark"] = core.SourceResult(source="spark", status="ok")
    phase_timing = progress.persist_completed_company_result(result, total_rows=2, processed_rows=1)
    phase_names = {item["phase"] for item in phase_timing["phase_breakdown"]}

    assert "persist_runtime_state" in phase_names
    assert "public_outputs.write_results_json" in phase_names
    assert "public_outputs.write_runtime_metadata" in phase_names
    assert "public_outputs.write_incremental_reports" in phase_names
    assert "append_results_jsonl" in phase_names

    public_started_at = datetime(2026, 5, 7, 10, 0, 5, tzinfo=timezone.utc)
    assert progress.record_downstream_finalization_timing(
        inn=row.inn,
        row_index=row.row_index,
        company_name=row.company_name,
        public_materialization_started_at=public_started_at.isoformat(),
        public_materialization_finished_at=(public_started_at + timedelta(seconds=4)).isoformat(),
        public_materialization_phase_timing=phase_timing,
    )

    runtime_state = json.loads((output_dir / "runtime_state.json").read_text(encoding="utf-8"))
    downstream_drain = runtime_state["run"]["metadata"]["throughput_telemetry"]["downstream_drain"]
    public_materialization = downstream_drain["companies"][row.inn]["public_materialization"]
    assert public_materialization["elapsed_seconds"] == 4.0
    assert public_materialization["phase_timing"]["phase_breakdown"] == phase_timing["phase_breakdown"]

    phase_totals = {
        item["phase"]: item
        for item in downstream_drain["slow_summary"]["public_materialization_phase_totals_by_phase"]
    }
    assert phase_totals["persist_runtime_state"]["sample_count"] == 1
    assert phase_totals["public_outputs.write_results_json"]["sample_count"] == 1
    assert phase_totals["public_outputs.write_incremental_reports"]["sample_count"] == 1


def test_progress_store_batches_buffered_runtime_event_replay_persistence(
    monkeypatch,
    tmp_path,
) -> None:
    output_dir = tmp_path / "output"
    progress = ProgressStore(output_dir)
    downstream_worker_pools = build_downstream_worker_pool_contour(company_concurrency_cap=2).as_payload()
    progress.run_started(
        input_path="input.xlsx",
        total_rows=1,
        selected_rows=1,
        selection_mode="ordinals",
        selected_ordinals=[1],
        start_from=1,
        end_at=1,
        active_sources=["spark"],
        downstream_worker_pools=downstream_worker_pools,
        throughput_telemetry=build_throughput_telemetry_payload(
            downstream_worker_pools=downstream_worker_pools,
            rows_completed=0,
        ),
    )
    persist_calls = []
    materialize_calls = []
    original_persist = progress._persist_runtime_state
    original_materialize = progress._materialize_runtime_metadata

    def counted_persist(*args, **kwargs):
        persist_calls.append(1)
        return original_persist(*args, **kwargs)

    def counted_materialize(*args, **kwargs):
        materialize_calls.append(1)
        return original_materialize(*args, **kwargs)

    monkeypatch.setattr(progress, "_persist_runtime_state", counted_persist)
    monkeypatch.setattr(progress, "_materialize_runtime_metadata", counted_materialize)
    started_at = datetime(2026, 5, 7, 10, 0, 0, tzinfo=timezone.utc)

    progress.append_events(
        [
            {
                "ts": (started_at + timedelta(seconds=3)).isoformat(),
                "type": "downstream_stage_span",
                "inn": "7700000001",
                "row_index": 1,
                "company_name": "Buffered Factory",
                "stage": CANDIDATE_SITE_STAGE_NAME,
                "started_at": started_at.isoformat(),
                "finished_at": (started_at + timedelta(seconds=3)).isoformat(),
                "elapsed_seconds": 3.0,
                "status": "completed",
            },
            {
                "ts": (started_at + timedelta(seconds=8)).isoformat(),
                "type": "downstream_stage_span",
                "inn": "7700000001",
                "row_index": 1,
                "company_name": "Buffered Factory",
                "stage": DEEP_PARSE_STAGE_NAME,
                "started_at": (started_at + timedelta(seconds=3)).isoformat(),
                "finished_at": (started_at + timedelta(seconds=8)).isoformat(),
                "elapsed_seconds": 5.0,
                "status": "completed",
            },
            {
                "ts": (started_at + timedelta(seconds=9)).isoformat(),
                "type": "request_ok",
                "source": "factory_site",
                "host": "factory.example",
                "url": "https://factory.example/",
                "status_code": 200,
                "elapsed_seconds": 0.25,
            },
        ]
    )

    assert len(persist_calls) == 1
    assert len(materialize_calls) == 1
    summary = json.loads((output_dir / "summary.json").read_text(encoding="utf-8"))
    replay_telemetry = summary["throughput_telemetry"]["runtime_event_replay"]
    assert replay_telemetry["buffered_replay_batch_count"] == 1
    assert replay_telemetry["buffered_replayed_event_count"] == 3
    assert replay_telemetry["deferred_event_count"] == 3
    assert replay_telemetry["deferred_state_update_event_count"] == 3
    assert replay_telemetry["deferred_batch_persist_count"] == 1
    assert replay_telemetry["deferred_state_batch_persist_count"] == 1
    assert replay_telemetry["deferred_telemetry_only_batch_persist_count"] == 0
    assert replay_telemetry["eager_event_persist_count"] == 0
    assert replay_telemetry["last_buffered_replay_batch"]["event_count"] == 3
    assert replay_telemetry["last_buffered_replay_batch"]["persistence_reason"] == "state_updates"
    runtime_state = json.loads((output_dir / "runtime_state.json").read_text(encoding="utf-8"))
    assert runtime_state["run"]["metadata"]["throughput_telemetry"]["runtime_event_replay"] == replay_telemetry
    company = runtime_state["run"]["metadata"]["throughput_telemetry"]["downstream_drain"]["companies"][
        "7700000001"
    ]
    assert company["stage_execution"]["total_elapsed_seconds"] == 8.0
    assert company["stages"][CANDIDATE_SITE_STAGE_NAME]["total_elapsed_seconds"] == 3.0
    assert company["stages"][DEEP_PARSE_STAGE_NAME]["total_elapsed_seconds"] == 5.0
    event_lines = [
        json.loads(line)
        for line in (output_dir / "events.jsonl").read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    assert any(item["type"] == "request_ok" for item in event_lines)


class _FastSource:
    def __init__(self, source_name: str) -> None:
        self.source_name = source_name

    def search(self, row: core.RowInput) -> core.SourceResult:
        return core.SourceResult(source=self.source_name, status="ok")


def test_bounded_executor_ready_queue_backpressure_is_observable() -> None:
    rows = _rows(3)
    source_lane_scheduler = _source_lane_scheduler(
        source_name="spark",
        capacity_boundary="direct_default_worker_lane",
        worker_lane_budget=1,
    )
    source_lane_telemetry = SourceLaneTelemetryLedger(source_lane_scheduler=source_lane_scheduler)
    source_lane_telemetry.seed_queue_depths({"spark": len(rows)})
    shared_client = SimpleNamespace(
        progress_store=SimpleNamespace(append_event=lambda _event: None),
    )
    executor = open_company_source_search_executor(
        rows=rows,
        sources=[_FastSource("spark")],
        shared_client=shared_client,
        worker_count=1,
        source_lane_telemetry=source_lane_telemetry,
        max_ready_queue_depth=1,
    )
    try:
        deadline = time.time() + 1.0
        spark_snapshot = source_lane_telemetry.snapshot()["spark"]
        while time.time() < deadline:
            spark_snapshot = source_lane_telemetry.snapshot()["spark"]
            if spark_snapshot["backpressure"]["active"]:
                break
            time.sleep(0.01)
        assert spark_snapshot["backpressure"]["active"] is True
        assert spark_snapshot["backpressure"]["reason"] == "ready_queue_limit"
        assert spark_snapshot["backpressure"]["blocked_submissions"] >= 1
        assert spark_snapshot["backpressure"]["ready_queue_limit"] == 1
        assert spark_snapshot["queue_depth"] >= 1

        first_batch = executor.take(rows[0])
        assert first_batch.inn == rows[0].inn
    finally:
        executor.close()

    closed_snapshot = source_lane_telemetry.snapshot()["spark"]
    assert closed_snapshot["backpressure"]["active"] is False

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

import company_enrichment_core as core
from app.runtime.host_memory import (
    HOST_MEMORY_SIGNAL_BOT_GATE,
    HOST_MEMORY_SIGNAL_CHALLENGE,
    HOST_MEMORY_SIGNAL_COOLDOWN,
    HOST_MEMORY_SIGNAL_HTTP_403,
    HOST_MEMORY_SIGNAL_HTTP_429,
    recent_governor_signal_proxy_labels,
    recent_host_proxy_outcomes,
    update_host_memory_from_event_payload,
)
from app.runtime.host_governor import resolve_host_governor_preflight
from app.runtime.host_governor import HostGovernorLedger


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


def _host_event(
    *,
    ts: str,
    status: str,
    proxy_label_or_id: str,
    cooldown_seconds: int = 0,
    anti_bot_reason: str = "",
    block_class: str = "",
    http_status: int | None = None,
    challenge_detected: bool = False,
) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "ts": ts,
        "type": "route_fetch_attempt",
        "event_type": "route_fetch_attempt",
        "host": "spark-interfax.ru",
        "source": "factory_site_fetch",
        "status": status,
        "proxy_label_or_id": proxy_label_or_id,
        "cooldown_seconds": cooldown_seconds,
        "transport_selected": "requests",
        "transport_final": "requests",
    }
    if anti_bot_reason:
        payload["anti_bot_reason"] = anti_bot_reason
    if block_class:
        payload["block_class"] = block_class
    if http_status is not None:
        payload["http_status"] = http_status
    if challenge_detected:
        payload["challenge_detected"] = True
    return payload


def test_progress_store_host_memory_persists_on_resume_and_resets_on_fresh_run(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setattr(core.ProgressStore, "_write_markdown_reports", lambda self, ordered_results=None: None)
    output_dir = tmp_path / "output"
    progress = core.ProgressStore(output_dir)
    _start_run(progress)

    progress.append_event(
        _host_event(
            ts="2026-04-19T10:00:00Z",
            status="http_429",
            proxy_label_or_id="proxy-a",
            cooldown_seconds=30,
            anti_bot_reason="http_429",
            block_class="RATE_LIMIT",
            http_status=429,
        )
    )
    progress.append_event(
        _host_event(
            ts="2026-04-19T10:00:10Z",
            status="bot_gate",
            proxy_label_or_id="proxy-b",
            anti_bot_reason="bot_gate",
            block_class="SOFT_BLOCK",
        )
    )

    runtime_state = json.loads(progress.runtime_state_json.read_text(encoding="utf-8"))
    assert runtime_state["run"]["host_stats"]["spark-interfax.ru"]["total_events"] == 2
    assert runtime_state["run"]["host_memory"]["spark-interfax.ru"]["recent_attempts"][-1]["proxy_label_or_id"] == "proxy-b"
    assert progress.recent_governor_signal_proxy_labels("spark-interfax.ru") == ["proxy-b", "proxy-a"]

    reloaded = core.ProgressStore(output_dir)
    _start_run(reloaded, continue_existing_run=True)

    recent_rate_limits = reloaded.recent_host_proxy_outcomes(
        "spark-interfax.ru",
        signal_tags={HOST_MEMORY_SIGNAL_HTTP_429},
        limit=1,
    )
    assert recent_rate_limits[0]["proxy_label_or_id"] == "proxy-a"
    assert recent_rate_limits[0]["cooldown_seconds"] == 30.0
    assert reloaded.recent_governor_signal_proxy_labels("spark-interfax.ru") == ["proxy-b", "proxy-a"]

    fresh_run = core.ProgressStore(output_dir)
    _start_run(fresh_run, continue_existing_run=False)

    fresh_runtime_state = json.loads(fresh_run.runtime_state_json.read_text(encoding="utf-8"))
    assert fresh_run.recent_host_proxy_outcomes("spark-interfax.ru") == []
    assert fresh_run.recent_governor_signal_proxy_labels("spark-interfax.ru") == []
    assert fresh_runtime_state["run"]["host_memory"] == {}


def test_host_memory_helpers_filter_recent_problem_proxy_signals() -> None:
    state: dict[str, Any] = {}
    update_host_memory_from_event_payload(
        state,
        _host_event(
            ts="2026-04-19T11:00:00Z",
            status="http_403",
            proxy_label_or_id="proxy-c",
            anti_bot_reason="http_403",
            block_class="HARD_BAN",
            http_status=403,
        ),
        ts="2026-04-19T11:00:00Z",
    )
    update_host_memory_from_event_payload(
        state,
        _host_event(
            ts="2026-04-19T11:00:05Z",
            status="challenge_page",
            proxy_label_or_id="proxy-d",
            cooldown_seconds=45,
            anti_bot_reason="challenge_page",
            block_class="SOFT_BLOCK",
            challenge_detected=True,
        ),
        ts="2026-04-19T11:00:05Z",
    )
    update_host_memory_from_event_payload(
        state,
        _host_event(
            ts="2026-04-19T11:00:10Z",
            status="success",
            proxy_label_or_id="proxy-e",
        ),
        ts="2026-04-19T11:00:10Z",
    )

    problem_outcomes = recent_host_proxy_outcomes(
        state,
        "spark-interfax.ru",
        signal_tags={
            HOST_MEMORY_SIGNAL_HTTP_403,
            HOST_MEMORY_SIGNAL_CHALLENGE,
            HOST_MEMORY_SIGNAL_COOLDOWN,
            HOST_MEMORY_SIGNAL_BOT_GATE,
            HOST_MEMORY_SIGNAL_HTTP_429,
        },
    )

    assert [item["proxy_label_or_id"] for item in problem_outcomes] == ["proxy-d", "proxy-c"]
    assert problem_outcomes[0]["signal_tags"] == [HOST_MEMORY_SIGNAL_COOLDOWN, HOST_MEMORY_SIGNAL_CHALLENGE]
    assert problem_outcomes[1]["signal_tags"] == [HOST_MEMORY_SIGNAL_HTTP_403]
    assert recent_governor_signal_proxy_labels(state, "spark-interfax.ru") == ["proxy-d", "proxy-c"]


def test_host_governor_preflight_late_success_clears_same_proxy_rotation_debt() -> None:
    state: dict[str, Any] = {}
    update_host_memory_from_event_payload(
        state,
        _host_event(
            ts="2026-04-19T11:00:00Z",
            status="bot_gate",
            proxy_label_or_id="proxy-a",
            anti_bot_reason="bot_gate",
            block_class="SOFT_BLOCK",
        ),
        ts="2026-04-19T11:00:00Z",
    )
    update_host_memory_from_event_payload(
        state,
        _host_event(
            ts="2026-04-19T11:00:10Z",
            status="success",
            proxy_label_or_id="proxy-a",
        ),
        ts="2026-04-19T11:00:10Z",
    )

    assert recent_governor_signal_proxy_labels(state, "spark-interfax.ru") == []

    preflight = resolve_host_governor_preflight(
        state,
        "spark-interfax.ru",
        now_ts=1_776_596_420.0,  # 2026-04-19T11:00:20Z
    )

    assert preflight.host == "spark-interfax.ru"
    assert preflight.cooldown_active is False
    assert preflight.cooldown_remaining_seconds == 0
    assert preflight.avoid_proxy_labels_or_ids == ()
    assert preflight.relevant_signal_tags == ()


def test_host_governor_preflight_uses_runtime_host_memory_for_cooldown_and_proxy_avoidance() -> None:
    state: dict[str, Any] = {}
    update_host_memory_from_event_payload(
        state,
        _host_event(
            ts="2026-04-19T11:00:00Z",
            status="http_429",
            proxy_label_or_id="proxy-a",
            cooldown_seconds=60,
            anti_bot_reason="http_429",
            block_class="RATE_LIMIT",
            http_status=429,
        ),
        ts="2026-04-19T11:00:00Z",
    )
    update_host_memory_from_event_payload(
        state,
        _host_event(
            ts="2026-04-19T11:00:05Z",
            status="bot_gate",
            proxy_label_or_id="proxy-b",
            cooldown_seconds=45,
            anti_bot_reason="bot_gate",
            block_class="SOFT_BLOCK",
        ),
        ts="2026-04-19T11:00:05Z",
    )
    update_host_memory_from_event_payload(
        state,
        _host_event(
            ts="2026-04-19T11:00:10Z",
            status="success",
            proxy_label_or_id="proxy-c",
        ),
        ts="2026-04-19T11:00:10Z",
    )

    preflight = resolve_host_governor_preflight(
        state,
        "spark-interfax.ru",
        now_ts=1_776_596_420.0,  # 2026-04-19T11:00:20Z
    )

    assert preflight.host == "spark-interfax.ru"
    assert preflight.cooldown_active is True
    assert preflight.cooldown_remaining_seconds == 40
    assert preflight.avoid_proxy_labels_or_ids == ("proxy-b", "proxy-a")
    assert preflight.relevant_signal_tags == (
        HOST_MEMORY_SIGNAL_COOLDOWN,
        HOST_MEMORY_SIGNAL_BOT_GATE,
        HOST_MEMORY_SIGNAL_HTTP_429,
    )


def test_host_governor_ledger_waits_on_persisted_and_runtime_cooldowns() -> None:
    state: dict[str, Any] = {}
    update_host_memory_from_event_payload(
        state,
        _host_event(
            ts="2026-04-19T11:00:00Z",
            status="http_429",
            proxy_label_or_id="proxy-a",
            cooldown_seconds=60,
            anti_bot_reason="http_429",
            block_class="RATE_LIMIT",
            http_status=429,
        ),
        ts="2026-04-19T11:00:00Z",
    )

    fake_now = {"value": 1_776_596_410.0}  # 2026-04-19T11:00:10Z

    def now_fn() -> float:
        return float(fake_now["value"])

    def sleep_fn(seconds: float) -> None:
        fake_now["value"] += float(seconds)

    ledger = HostGovernorLedger(
        persisted_host_memory=lambda: state,
        now_fn=now_fn,
        sleep_fn=sleep_fn,
        active_poll_seconds=0.001,
    )

    first_started_at = fake_now["value"]
    first_hosts = ledger.acquire(["spark-interfax.ru"])
    assert first_hosts == ("spark-interfax.ru",)
    assert fake_now["value"] - first_started_at == 50.0
    assert ledger.snapshot()["spark-interfax.ru"]["active_leases"] == 1
    assert ledger.snapshot()["spark-interfax.ru"]["cooldown_remaining_seconds"] == 0.0

    ledger.release(
        first_hosts,
        runtime_events=[
            {
                "ts": "2026-04-19T11:01:00Z",
                "host": "spark-interfax.ru",
                "cooldown_seconds": 30,
            }
        ],
    )
    assert ledger.snapshot()["spark-interfax.ru"]["active_leases"] == 0
    assert ledger.snapshot()["spark-interfax.ru"]["cooldown_remaining_seconds"] == 30.0

    second_started_at = fake_now["value"]
    second_hosts = ledger.acquire(["spark-interfax.ru"])
    assert second_hosts == ("spark-interfax.ru",)
    assert fake_now["value"] - second_started_at == 30.0
    ledger.release(second_hosts)

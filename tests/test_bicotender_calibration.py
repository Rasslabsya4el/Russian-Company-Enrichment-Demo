from __future__ import annotations

import logging
from argparse import Namespace

from app.runtime import ProxyPool
from app.sources.bicotender import BicotenderFetchResponse, BicotenderKeywordBatch, BicotenderSearchQuery
from scripts.calibration import calibrate_bicotender


def _args(tmp_path, **overrides: object) -> Namespace:
    values = {
        "inn": ["5022055500"],
        "delays": "12,8",
        "attempts_per_delay": 1,
        "max_requests": 4,
        "request_timeout": 1,
        "cooldown_429_seconds": 3600,
        "cooldown_bot_seconds": 5400,
        "continue_after_block": False,
        "output_dir": str(tmp_path),
        "proxy_strategy": None,
        "keyword_batches": "",
        "env_file": "",
    }
    values.update(overrides)
    return Namespace(**values)


def _keyword_batches() -> tuple[BicotenderKeywordBatch, ...]:
    return (
        BicotenderKeywordBatch(
            index=1,
            terms=("металлолом",),
            keywords="металлолом",
            char_count=len("металлолом"),
        ),
    )


def test_strict_proxy_preflight_blocks_when_pool_is_absent(tmp_path) -> None:
    proxy_pool = calibrate_bicotender.create_proxy_pool(
        fallback_file=tmp_path / "missing_proxy_pool.json",
    )

    attempts, summaries, recommendation, proxy_preflight = calibrate_bicotender.run_calibration(
        args=_args(tmp_path),
        proxy_pool=proxy_pool,
        keyword_batches=_keyword_batches(),
        suite_dir=tmp_path,
        logger=logging.getLogger("test_bicotender_calibration_absent_proxy"),
    )

    assert attempts == []
    assert summaries == []
    assert proxy_preflight["status"] == "blocked"
    assert proxy_preflight["reason"] == "no_usable_proxy_pool_for_strict_proxy_calibration"
    assert "DELAY_BICOTENDER_SECONDS=12" in recommendation.env_lines
    assert "COOLDOWN_429_SECONDS=3600" in recommendation.env_lines


def test_proxy_summary_redacts_raw_proxy_url_and_credentials() -> None:
    proxy_url = "http://" + "demo_user:demo_password" + "@127.0.0.1:8080"
    proxy_pool = ProxyPool(proxy_url)

    summary = calibrate_bicotender.safe_proxy_summary(proxy_pool)
    serialized = str(summary)

    assert "demo_password" not in serialized
    assert "demo_user" not in serialized
    assert "127.0.0.1" not in serialized
    assert "8080" not in serialized
    assert summary["items"][0]["proxy_label_or_id"].startswith("proxy-")


def test_planned_queries_use_bicotender_query_helpers() -> None:
    queries = calibrate_bicotender.planned_queries_for_inn(
        inn="5022055500",
        keyword_batches=_keyword_batches(),
    )

    assert [query[0] for query in queries] == ["inn_only_preflight", "keyword_batch"]
    assert queries[0][2].keywords == ""
    assert queries[1][1] == 1
    assert queries[1][2].keywords == "металлолом"
    assert queries[1][2].to_url().startswith("https://www.bicotender.ru/tender/search/?")
    assert "company%5Binn%5D=5022055500" in queries[1][2].to_url()


def test_hard_block_without_clean_bucket_does_not_claim_exact_threshold() -> None:
    recommendation = calibrate_bicotender.build_recommendation(
        [
            calibrate_bicotender.DelayBucketSummary(
                delay_seconds=12.0,
                total_attempts=1,
                clean=0,
                zero_result=0,
                blocked=1,
                status_429=0,
                status_403=0,
                bot_gate=1,
                ambiguous_no_row=0,
                static_marker=1,
                request_error=0,
                median_elapsed_seconds=1.0,
                p95_elapsed_seconds=1.0,
                status_counts={"bot_gate": 1},
            )
        ],
        cooldown_429_seconds=3600,
        proxy_ban_cooldown_seconds=300,
    )

    assert recommendation.exact_ban_threshold_known is False
    assert recommendation.confidence == "unsafe_first_bucket"
    assert recommendation.env_lines == [
        "DELAY_BICOTENDER_SECONDS=12",
        "PARSER_PROXY_BAN_COOLDOWN_SECONDS=1800",
        "COOLDOWN_429_SECONDS=3600",
    ]


def test_static_marker_with_usable_rows_suppresses_false_bot_gate() -> None:
    query = BicotenderSearchQuery(inn="5022055500")
    html = """
    <html>
      <body>
        <script src="/assets/captcha-modal.js"></script>
        <div class="modal">captcha can be shown inside a dismissible login widget</div>
        <form><input name="company[inn]" value="5022055500"></form>
        <div>Найдено 1 тендер</div>
        <article class="tender-card">
          <span>Тендер №329295757</span>
          <a href="/masinostroenie/realizuet-vagony-tender329295757.html">Реализует вагоны</a>
        </article>
      </body>
    </html>
    """

    record = calibrate_bicotender.attempt_from_response(
        timestamp="2026-05-15T00:00:00+00:00",
        delay_seconds=12.0,
        attempt_no=1,
        request_no=1,
        inn="5022055500",
        query_kind="inn_only_preflight",
        batch_index=None,
        batch_char_count=None,
        query=query,
        positive_terms=(),
        response=BicotenderFetchResponse(
            html=html,
            http_status=200,
            final_url=query.to_url(),
            error="Bot/captcha gate detected",
        ),
        trace=calibrate_bicotender.TraceRecord(
            requested_url=query.to_url(),
            request_status="bot_gate",
            http_status=200,
            final_url=query.to_url(),
            elapsed_seconds=1.0,
            proxy_mode="proxy",
            proxy_label_or_id="proxy-test",
            error="Bot/captcha gate detected",
        ),
    )
    summary = calibrate_bicotender.summarize_delay_bucket(12.0, [record])

    assert record.request_status == "ok_static_marker"
    assert record.static_marker is True
    assert record.hard_challenge is False
    assert record.access_status == "usable_public_list"
    assert record.access_reason == "static_access_marker_present_but_public_rows_usable"
    assert "Bot/captcha gate detected" not in record.error_or_reason
    assert calibrate_bicotender.record_is_clean(record) is True
    assert calibrate_bicotender.record_is_hard_block(record) is False
    assert summary.clean == 1
    assert summary.zero_result == 0
    assert summary.bot_gate == 0
    assert summary.blocked == 0
    assert summary.ambiguous_no_row == 0
    assert summary.static_marker == 1


def test_static_marker_zero_results_suppresses_false_bot_gate() -> None:
    query = BicotenderSearchQuery(inn="5022055500", keywords="металлолом")
    html = """
    <html>
      <body>
        <script src="/assets/captcha-modal.js"></script>
        <div class="modal">captcha can be shown inside a dismissible login widget</div>
        <form>
          <input name="company[inn]" value="5022055500">
          <input name="keywords" value="металлолом">
        </form>
        <div class="summary">Найдено 0 тендеров</div>
      </body>
    </html>
    """

    record = calibrate_bicotender.attempt_from_response(
        timestamp="2026-05-15T00:00:00+00:00",
        delay_seconds=30.0,
        attempt_no=1,
        request_no=1,
        inn="5022055500",
        query_kind="keyword_batch",
        batch_index=1,
        batch_char_count=len("металлолом"),
        query=query,
        positive_terms=("металлолом",),
        response=BicotenderFetchResponse(
            html=html,
            http_status=200,
            final_url=query.to_url(),
            error="Bot/captcha gate detected",
        ),
        trace=calibrate_bicotender.TraceRecord(
            requested_url=query.to_url(),
            request_status="bot_gate",
            http_status=200,
            final_url=query.to_url(),
            elapsed_seconds=1.0,
            proxy_mode="proxy",
            proxy_label_or_id="proxy-test",
            error="Bot/captcha gate detected",
        ),
    )
    summary = calibrate_bicotender.summarize_delay_bucket(30.0, [record])

    assert record.request_status == "ok_zero_results_static_marker"
    assert record.query_applied is True
    assert record.visible_count == 0
    assert record.parsed_item_count == 0
    assert record.static_marker is True
    assert record.hard_challenge is False
    assert record.access_status == "usable_public_list"
    assert record.access_reason == "static_access_marker_present_but_query_applied_zero_results"
    assert record.classification_status == "no_signal"
    assert record.classification_reason == "applied_public_list_query_returned_zero_results"
    assert "Bot/captcha gate detected" not in record.error_or_reason
    assert calibrate_bicotender.record_is_clean(record) is True
    assert calibrate_bicotender.record_is_hard_block(record) is False
    assert summary.clean == 1
    assert summary.zero_result == 1
    assert summary.blocked == 0
    assert summary.bot_gate == 0
    assert summary.ambiguous_no_row == 0
    assert summary.static_marker == 1
    assert summary.status_counts == {"ok_zero_results_static_marker": 1}


def test_real_hard_challenge_still_counts_as_bot_gate() -> None:
    query = BicotenderSearchQuery(inn="5022055500")
    html = """
    <html><body>
      <h1>Подтвердите, что вы не робот</h1>
      <div>captcha</div>
    </body></html>
    """

    record = calibrate_bicotender.attempt_from_response(
        timestamp="2026-05-15T00:00:00+00:00",
        delay_seconds=12.0,
        attempt_no=1,
        request_no=1,
        inn="5022055500",
        query_kind="inn_only_preflight",
        batch_index=None,
        batch_char_count=None,
        query=query,
        positive_terms=(),
        response=BicotenderFetchResponse(
            html=html,
            http_status=200,
            final_url=query.to_url(),
            error="Bot/captcha gate detected",
        ),
        trace=calibrate_bicotender.TraceRecord(
            requested_url=query.to_url(),
            request_status="bot_gate",
            http_status=200,
            final_url=query.to_url(),
            elapsed_seconds=1.0,
            proxy_mode="proxy",
            proxy_label_or_id="proxy-test",
            error="Bot/captcha gate detected",
        ),
    )
    summary = calibrate_bicotender.summarize_delay_bucket(12.0, [record])

    assert record.request_status == "bot_gate"
    assert record.hard_challenge is True
    assert record.access_status == "blocked_protected_source"
    assert calibrate_bicotender.record_is_clean(record) is False
    assert calibrate_bicotender.record_is_hard_block(record) is True
    assert summary.clean == 0
    assert summary.zero_result == 0
    assert summary.bot_gate == 1
    assert summary.blocked == 1
    assert summary.ambiguous_no_row == 0

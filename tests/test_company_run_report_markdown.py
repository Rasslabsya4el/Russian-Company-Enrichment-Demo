from __future__ import annotations

import company_enrichment_core as core


def _assert_no_mojibake_markers(text: str) -> None:
    assert chr(0x00D0) not in text
    assert chr(0x00D1) not in text
    assert chr(0x00C3) not in text
    assert "â€”" not in text
    assert "â€¦" not in text
    assert all(not 0x80 <= ord(char) <= 0x9F for char in text)


def _make_result() -> dict[str, object]:
    return {
        "row_index": 1,
        "inn": "",
        "company_name": "",
        "status": "",
        "sources": {},
        "domain_resolution": {},
        "site_probes": [],
        "lead_cards": [],
        "trusted_contacts": {},
        "merged_contacts": {},
        "candidate_sites": [],
        "validated_sites": [],
        "profile": {
            "summary": {
                "inn": "7707083893",
                "company_name": "Тестовый завод",
                "processing_status": "completed",
                "domain_resolution_status": "verified",
                "lead_count": 2,
            },
            "contacts": {
                "trusted": {
                    "phones": ["+7 495 000-00-00"],
                    "emails": ["sales@example.com"],
                    "websites": [],
                    "addresses": [],
                },
                "raw": {
                    "phones": [],
                    "emails": [],
                    "websites": ["https://raw.example/"],
                    "addresses": [],
                },
            },
            "sites": {
                "primary_domain": "",
                "confirmed_sites": ["https://confirmed.example/"],
                "site_classes": [],
            },
            "signals": {
                "geo": {"match_status": "unknown"},
                "naming": {"signal_status": "unknown"},
            },
        },
    }


def test_render_index_report_markdown_renders_utf8_russian_labels_without_mojibake() -> None:
    markdown = core.render_index_report_markdown(
        [_make_result()],
        summary={
            "completed_rows": 1,
            "throughput_telemetry": {
                "source_collection": {
                    "slow_summary": {
                        "top_company_source_collection": [
                            {
                                "inn": "7707083893",
                                "row_index": 1,
                                "total_duration_seconds": 7.5,
                                "slowest_source": {"source": "spark"},
                            }
                        ],
                    }
                },
                "downstream_drain": {
                    "slow_summary": {
                        "top_company_stage_execution": [
                            {
                                "inn": "7707083893",
                                "row_index": 1,
                                "total_elapsed_seconds": 12.25,
                                "dominant_stage": {"stage": "factory_site"},
                            }
                        ],
                        "stage_totals_by_stage": [
                            {"stage": "factory_site", "total_elapsed_seconds": 12.25}
                        ],
                    }
                },
            },
        },
        availability_summary={
            "sources": {
                "checko": {
                    "phones": {"open": 1},
                }
            }
        },
        host_stats={
            "checko.ru": {
                "total_events": 1,
                "event_types": {"request_ok": 1},
                "interval_seconds": {},
                "cooldown_seconds": {},
            }
        },
    )

    assert "## Доступность полей по агрегаторам" in markdown
    assert "## Поведение хостов" in markdown
    assert "## Runtime Timing" in markdown
    assert "Top source rows: row=1 inn=7707083893 total=7.50s slowest=spark" in markdown
    assert "Top downstream rows: row=1 inn=7707083893 total=12.25s dominant=factory_site" in markdown
    assert "Downstream stage totals: factory_site=12.25s" in markdown
    assert "## Компании" in markdown
    assert "Summary total_rows: `—`" in markdown
    assert "avg_interval=—" in markdown
    assert "probes=`—`" in markdown
    assert "primary=—" in markdown
    assert "[7707083893 — Тестовый завод](company_reports/0001-7707083893.md)" in markdown
    assert "статус: `completed`" in markdown
    assert "trusted телефоны: `1`" in markdown
    assert "trusted сайты: `0`" in markdown
    assert "raw сайты: `1`" in markdown
    assert "подтверждено сайтов: `1`" in markdown
    _assert_no_mojibake_markers(markdown)


def test_render_index_report_markdown_renders_runtime_timing_summary() -> None:
    markdown = core.render_index_report_markdown(
        [_make_result()],
        summary={
            "completed_rows": 1,
            "throughput_telemetry": {
                "source_collection": {
                    "slow_summary": {
                        "top_company_source_collection": [
                            {
                                "inn": "7707083893",
                                "row_index": 1,
                                "total_duration_seconds": 7.5,
                                "slowest_source": {"source": "spark"},
                            }
                        ],
                    }
                },
                "downstream_drain": {
                    "slow_summary": {
                        "top_company_stage_execution": [
                            {
                                "inn": "7707083893",
                                "row_index": 1,
                                "total_elapsed_seconds": 12.25,
                                "dominant_stage": {"stage": "factory_site"},
                            }
                        ],
                        "stage_totals_by_stage": [
                            {"stage": "factory_site", "total_elapsed_seconds": 12.25}
                        ],
                        "phase_totals_by_phase": [
                            {"phase": "ordered_ack_wait", "total_elapsed_seconds": 4.0}
                        ],
                    }
                },
            },
        },
        availability_summary={},
        host_stats={},
    )

    assert "## Runtime Timing" in markdown
    assert "Top source rows: row=1 inn=7707083893 total=7.50s slowest=spark" in markdown
    assert "Top downstream rows: row=1 inn=7707083893 total=12.25s dominant=factory_site" in markdown
    assert "Downstream stage totals: factory_site=12.25s" in markdown
    assert "Downstream wait totals: ordered_ack_wait=4.00s" in markdown
    _assert_no_mojibake_markers(markdown)


def test_render_company_report_markdown_renders_utf8_russian_labels_without_mojibake() -> None:
    result = _make_result()
    result.update(
        {
            "row_index": 7,
            "inn": "7707083893",
            "company_name": "Тестовый завод",
            "status": "completed",
            "started_at": "2026-04-21T15:25:35+00:00",
            "finished_at": "2026-04-21T15:25:44+00:00",
            "input_comment": "операторская заметка",
            "notes": ["Не нашел кандидатов на сайт из агрегаторов и доменов почты"],
            "candidate_sites": ["https://confirmed.example/"],
            "site_refresh_plans": [
                {
                    "site_url": "https://confirmed.example/",
                    "cadence": "weekly",
                    "next_due_at": "2026-04-28T15:25:44+00:00",
                    "reason": "сайт требует осторожного или частичного обхода",
                }
            ],
            "sources": {
                "spark": {
                    "source": "spark",
                    "status": "success",
                    "search_url": "https://spark.example/search",
                    "listing_url": "https://spark.example/listing",
                    "entity_url": "https://spark.example/entity",
                    "company_name_found": "Тестовый завод",
                    "phones": [],
                    "emails": [],
                    "websites": [],
                    "addresses": [],
                    "availability": {},
                    "masked_rows": [],
                    "links": [],
                    "notes": [],
                    "errors": [],
                    "snippets": [],
                }
            },
            "domain_resolution": {
                "status": "verified",
                "selected_primary_domain": "https://confirmed.example/",
                "selected_primary_status": "verified",
                "candidates": [
                    {
                        "url": "https://confirmed.example/",
                        "status": "verified",
                        "confidence": 0.99,
                        "source": "zachestnyibiznes",
                        "evidence": ["домен подтвержден"],
                    }
                ],
            },
            "validated_sites": [
                {
                    "url": "https://confirmed.example/",
                    "final_url": "https://confirmed.example/",
                    "status": "success",
                    "decision_status": "verified",
                    "belongs_to_company": True,
                    "industrial_relevance": "high",
                    "identity_score": 0.9,
                    "industrial_score": 0.8,
                    "decision_source": "heuristics",
                    "reasons": ["совпадает с доменом компании"],
                    "errors": [],
                    "llm_result": {},
                    "extracted_phones": [],
                    "extracted_emails": [],
                    "extracted_addresses": [],
                    "evidence": [],
                    "hard_negative_hits": [],
                    "fetched_pages": [],
                }
            ],
        }
    )

    markdown = core.render_company_report_markdown(result)

    assert "- Строка XLSX: `7`" in markdown
    assert "- Статус: `completed`" in markdown
    assert "- Комментарий: операторская заметка" in markdown
    assert "## Trusted Contacts" in markdown
    assert "- Телефоны: `+7 495 000-00-00`" in markdown
    assert "- Сайты: —" in markdown
    assert "## Сайты" in markdown
    assert "- Кандидаты: [https://confirmed.example/](https://confirmed.example/)" in markdown
    assert "- Подтвержденные сайты: [https://confirmed.example/](https://confirmed.example/)" in markdown
    assert "### Проверка сайтов" in markdown
    assert "## Источники" in markdown
    assert "- Название в источнике: Тестовый завод" in markdown
    assert "Не нашел кандидатов на сайт из агрегаторов и доменов почты" in markdown
    assert "сайт требует осторожного или частичного обхода" in markdown
    _assert_no_mojibake_markers(markdown)

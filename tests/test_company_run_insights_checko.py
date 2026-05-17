from __future__ import annotations

from collections.abc import Iterable

import company_enrichment_core as core


def _make_source_payload(
    status: str,
    *,
    availability: dict[str, dict[str, object]] | None = None,
) -> dict[str, object]:
    return {
        "status": status,
        "availability": availability or {},
    }


def _make_result(
    row_index: int,
    inn: str,
    company_name: str,
    sources: dict[str, dict[str, object]],
) -> dict[str, object]:
    return {
        "row_index": row_index,
        "inn": inn,
        "company_name": company_name,
        "status": "completed",
        "sources": sources,
        "domain_resolution": {"status": "not_found"},
        "site_probes": [],
        "lead_cards": [],
        "trusted_contacts": {},
        "merged_contacts": {},
        "candidate_sites": [],
        "validated_sites": [],
    }


def _render_insights(
    ordered_results: list[dict[str, object]],
    availability_summary: dict[str, object],
) -> str:
    return core.render_run_insights_markdown(
        ordered_results,
        summary={"completed_rows": len(ordered_results)},
        availability_summary=availability_summary,
        host_stats={},
    )


def _assert_source_present_in_all_sections(markdown: str, source_name: str) -> None:
    assert f"- `{source_name}`:" in markdown
    assert f"- `{source_name}` |" in markdown
    assert f"- `{source_name}`\n  - `phones`:" in markdown


def _assert_sources_absent(markdown: str, source_names: Iterable[str]) -> None:
    for source_name in source_names:
        assert f"`{source_name}`" not in markdown


def test_render_run_insights_markdown_uses_checko_for_checko_only_run() -> None:
    ordered_results = [
        _make_result(
            1,
            "7707083893",
            "Checko Only Company",
            {
                "checko": _make_source_payload(
                    "success",
                    availability={
                        "phones": core.build_field_availability_payload("open", open_count=1),
                        "emails": core.build_field_availability_payload("masked", reason="subscription"),
                    },
                )
            },
        )
    ]
    availability_summary = {
        "sources": {
            "checko": {
                "phones": {"open": 1, "masked": 0, "absent": 0, "blocked": 0, "unknown": 0},
                "emails": {"open": 0, "masked": 1, "absent": 0, "blocked": 0, "unknown": 0},
            }
        }
    }

    markdown = _render_insights(ordered_results, availability_summary)

    _assert_source_present_in_all_sections(markdown, "checko")
    _assert_sources_absent(markdown, ("spark", "zachestnyibiznes", "rusprofile", "list_org"))


def test_render_run_insights_markdown_keeps_all_present_sources_for_mixed_run() -> None:
    ordered_results = [
        _make_result(
            1,
            "7707083893",
            "Mixed Source One",
            {
                "checko": _make_source_payload(
                    "success",
                    availability={"phones": core.build_field_availability_payload("open", open_count=1)},
                ),
                "spark": _make_source_payload(
                    "blocked",
                    availability={"phones": core.build_field_availability_payload("blocked", reason="captcha")},
                ),
            },
        ),
        _make_result(
            2,
            "7813252159",
            "Mixed Source Two",
            {
                "rusprofile": _make_source_payload(
                    "success",
                    availability={"phones": core.build_field_availability_payload("masked", reason="subscription")},
                )
            },
        ),
    ]
    availability_summary = {
        "sources": {
            "checko": {
                "phones": {"open": 1, "masked": 0, "absent": 0, "blocked": 0, "unknown": 0},
            },
            "spark": {
                "phones": {"open": 0, "masked": 0, "absent": 0, "blocked": 1, "unknown": 0},
            },
            "rusprofile": {
                "phones": {"open": 0, "masked": 1, "absent": 0, "blocked": 0, "unknown": 0},
            },
        }
    }

    markdown = _render_insights(ordered_results, availability_summary)

    for source_name in ("checko", "rusprofile", "spark"):
        _assert_source_present_in_all_sections(markdown, source_name)
    _assert_sources_absent(markdown, ("zachestnyibiznes", "list_org"))

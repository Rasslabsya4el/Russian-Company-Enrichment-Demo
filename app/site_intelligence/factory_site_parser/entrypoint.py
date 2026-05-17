from __future__ import annotations

from dataclasses import replace
from pathlib import Path
from typing import Any

from app.runtime import ProxyPool
from app.site_intelligence.common import dedupe_preserve_order, normalize_url
from app.site_intelligence.models import RouteStrategy
from app.site_intelligence.strategy import queue_name_for_route, route_caps

from .client import FactorySiteParserClient
from .documents import FactorySiteDocumentsStage
from .fetch import FactorySiteFetchStage
from .models import (
    FACTORY_SITE_TRUST_STATE_AMBIGUOUS,
    FACTORY_SITE_TRUST_STATE_TRUSTED,
    FactorySiteParserCompany,
    FactorySitePlan,
    FactorySiteParserResult,
    factory_site_trust_state_from_verdict,
)
from .okved_match import FactorySiteOkvedMatcher
from .planner import FactorySitePlanner
from .store import FactorySiteStore

_DRY_RUN_HOMEPAGE_FALLBACK_FAMILY = "company/about"
_DRY_RUN_HOMEPAGE_FALLBACK_SECTION = "about"
_DRY_RUN_HOMEPAGE_FALLBACK_NOTE = "dry-run homepage fallback injected because planner returned empty queue"
_FETCH_RUNTIME_NOTE_PREFIXES = ("factory-site fetch:", "factory-site raw crawl execution:")


def _require_mapping_attr(result: FactorySiteParserResult, attr_name: str) -> dict[str, Any]:
    value = getattr(result, attr_name, None)
    if not isinstance(value, dict):
        raise TypeError(f"FactorySiteStore.build_result() must set result.{attr_name} as dict.")
    return value


def _require_list_attr(result: FactorySiteParserResult, attr_name: str) -> list[Any]:
    value = getattr(result, attr_name, None)
    if not isinstance(value, list):
        raise TypeError(f"FactorySiteStore.build_result() must set result.{attr_name} as list.")
    return value


def _build_dry_run_homepage_route(plan: FactorySitePlan) -> RouteStrategy:
    max_depth, host_cap, path_pattern_cap = route_caps(
        site_url=plan.site_url,
        route_url=plan.site_url,
        section=_DRY_RUN_HOMEPAGE_FALLBACK_SECTION,
    )
    return RouteStrategy(
        site_url=plan.site_url,
        route_pattern=plan.site_url,
        section_guess=_DRY_RUN_HOMEPAGE_FALLBACK_SECTION,
        mode="requests",
        confidence=0.51,
        route_family=_DRY_RUN_HOMEPAGE_FALLBACK_FAMILY,
        priority=1,
        crawl_budget=1,
        queue_name=queue_name_for_route(mode="requests", section=_DRY_RUN_HOMEPAGE_FALLBACK_SECTION),
        accounting_key=(
            f"{host_cap}:{_DRY_RUN_HOMEPAGE_FALLBACK_FAMILY}" if host_cap else _DRY_RUN_HOMEPAGE_FALLBACK_FAMILY
        ),
        mandatory=False,
        counts_toward_coverage=False,
        max_depth=max_depth,
        host_cap=host_cap,
        path_pattern_cap=path_pattern_cap,
        reasons=[
            "dry_run homepage fallback",
            f"probe_status={getattr(plan.probe, 'status', '') or 'unknown'}",
            f"probe_site_class={getattr(plan.probe, 'site_class', '') or 'unknown'}",
        ],
        discovery_sources=["homepage", "dry_run_fallback"],
    )


def _with_dry_run_homepage_fallback(plans: list[FactorySitePlan], *, dry_run: bool) -> list[FactorySitePlan]:
    if not dry_run:
        return plans
    updated_plans: list[FactorySitePlan] = []
    for plan in plans:
        if getattr(plan.probe, "status", "") != "success" or plan.routes or not plan.site_url:
            updated_plans.append(plan)
            continue
        notes = list(plan.notes)
        if _DRY_RUN_HOMEPAGE_FALLBACK_NOTE not in notes:
            notes.append(_DRY_RUN_HOMEPAGE_FALLBACK_NOTE)
        updated_plans.append(replace(plan, routes=[_build_dry_run_homepage_route(plan)], notes=notes))
    return updated_plans


def _normalize_fetch_output(fetch_output: Any) -> tuple[list[Any], dict[str, Any] | None]:
    if isinstance(fetch_output, tuple):
        if len(fetch_output) != 2 or not isinstance(fetch_output[1], dict):
            raise TypeError("FactorySiteFetchStage.fetch() must return (content_records, crawl_execution).")
        return fetch_output[0], fetch_output[1]
    if isinstance(fetch_output, list):
        # Backward compatibility for injected legacy fetch stages that only return content records.
        return fetch_output, None
    raise TypeError(
        "FactorySiteFetchStage.fetch() must return (content_records, crawl_execution) "
        "or a legacy list[ContentRecord]."
    )


def _site_key(value: Any) -> str:
    normalized = normalize_url(str(value or "").strip())
    return normalized or str(value or "").strip()


def _record_site_key(record: Any) -> str:
    parser_trace = getattr(record, "trace", {}).get("factory_site_parser", {})
    if isinstance(parser_trace, dict):
        crawl_trace = parser_trace.get("crawl", {})
        if isinstance(crawl_trace, dict):
            site_key = _site_key(crawl_trace.get("site_url"))
            if site_key:
                return site_key
    return _site_key(getattr(record, "site_url", ""))


def _group_records_by_site(records: list[Any]) -> dict[str, list[Any]]:
    grouped: dict[str, list[Any]] = {}
    for record in records:
        site_key = _record_site_key(record)
        if not site_key:
            continue
        grouped.setdefault(site_key, []).append(record)
    return grouped


def _select_records_for_sites(records: list[Any], *, site_keys: set[str]) -> list[Any]:
    return [record for record in records if _record_site_key(record) in site_keys]


def _entry_site_key(payload: Any) -> str:
    if isinstance(payload, dict):
        return _site_key(payload.get("site_url"))
    return ""


def _slice_crawl_execution(crawl_execution: dict[str, Any] | None, *, site_keys: set[str]) -> dict[str, Any] | None:
    if not isinstance(crawl_execution, dict) or not site_keys:
        return None
    executed_routes = [dict(item) for item in (crawl_execution.get("executed_routes", []) or []) if _entry_site_key(item) in site_keys]
    skipped_routes = [dict(item) for item in (crawl_execution.get("skipped_routes", []) or []) if _entry_site_key(item) in site_keys]
    policy_skips = [dict(item) for item in (crawl_execution.get("policy_skips", []) or []) if _entry_site_key(item) in site_keys]
    document_queue = [dict(item) for item in (crawl_execution.get("document_queue", []) or []) if _entry_site_key(item) in site_keys]
    sites = [dict(item) for item in (crawl_execution.get("sites", []) or []) if _entry_site_key(item) in site_keys]
    budget = dict(crawl_execution.get("budget") or {})
    budget["sites"] = [dict(item) for item in (budget.get("sites", []) or []) if _entry_site_key(item) in site_keys]
    visited_route_families = dedupe_preserve_order(
        str(item.get("route_family", "") or "").strip()
        for item in executed_routes
        if str(item.get("route_family", "") or "").strip()
    )
    page_records = sum(int(item.get("page_records", 0) or 0) for item in executed_routes)
    document_records = sum(int(item.get("document_records", 0) or 0) for item in executed_routes)
    payload = {
        "visited_route_families": list(visited_route_families),
        "page_records": page_records,
        "document_records": document_records,
        "record_count": page_records + document_records,
        "skipped_routes": skipped_routes,
        "budget": budget,
        "policy_skips": policy_skips,
        "non_sample_record_fingerprints": [],
        "executed_routes": executed_routes,
        "document_queue": document_queue,
        "sites": sites,
        "dry_run": bool(crawl_execution.get("dry_run", False)),
        "trust_embargo": bool(crawl_execution.get("trust_embargo", False)),
    }
    payload["executed_route_count"] = len(executed_routes)
    payload["skipped_route_count"] = len(skipped_routes)
    payload["budget"]["executed_routes"] = payload["executed_route_count"]
    payload["budget"]["skipped_routes"] = payload["skipped_route_count"]
    payload["runtime_summary"] = {
        "record_count": payload["record_count"],
        "page_records": payload["page_records"],
        "document_records": payload["document_records"],
        "visited_route_families": list(payload["visited_route_families"]),
        "executed_route_count": payload["executed_route_count"],
        "skipped_route_count": payload["skipped_route_count"],
        "document_queue_count": len(payload["document_queue"]),
        "policy_skip_count": len(payload["policy_skips"]),
        "non_sample_record_count": 0,
    }
    return payload


def _collect_non_sample_record_fingerprints(records: list[Any]) -> list[str]:
    fingerprints: list[str] = []
    seen: set[str] = set()
    for record in records:
        parser_trace = getattr(record, "trace", {}).get("factory_site_parser", {})
        crawl_trace = parser_trace.get("crawl", {}) if isinstance(parser_trace, dict) else {}
        if isinstance(crawl_trace, dict) and str(crawl_trace.get("route_origin", "") or "") == "sample":
            continue
        fingerprint = getattr(record, "content_fingerprint", "") or (
            f"{getattr(record, 'url', '')}|{getattr(record, 'fetch_status', '')}|{getattr(record, 'source_type', '')}"
        )
        if fingerprint in seen:
            continue
        seen.add(fingerprint)
        fingerprints.append(fingerprint)
    return fingerprints


def _merge_crawl_executions(parts: list[dict[str, Any] | None], *, content_records: list[Any]) -> dict[str, Any] | None:
    payloads = [part for part in parts if isinstance(part, dict)]
    if not payloads:
        return None
    executed_routes = [dict(item) for part in payloads for item in (part.get("executed_routes", []) or [])]
    skipped_routes = [dict(item) for part in payloads for item in (part.get("skipped_routes", []) or [])]
    policy_skips = [dict(item) for part in payloads for item in (part.get("policy_skips", []) or [])]
    document_queue = [dict(item) for part in payloads for item in (part.get("document_queue", []) or [])]
    sites = [dict(item) for part in payloads for item in (part.get("sites", []) or [])]
    budget_sites = [
        dict(item)
        for part in payloads
        for item in ((part.get("budget") or {}).get("sites", []) or [])
    ]
    visited_route_families = dedupe_preserve_order(
        [
            *(
                str(item.get("route_family", "") or "").strip()
                for item in executed_routes
                if str(item.get("route_family", "") or "").strip()
            ),
            *(
                family
                for part in payloads
                for family in (part.get("visited_route_families", []) or [])
                if str(family or "").strip()
            ),
        ]
    )
    page_records = sum(int(part.get("page_records", 0) or 0) for part in payloads)
    document_records = sum(int(part.get("document_records", 0) or 0) for part in payloads)
    non_sample_record_fingerprints = _collect_non_sample_record_fingerprints(content_records)
    payload = {
        "visited_route_families": list(visited_route_families),
        "page_records": page_records,
        "document_records": document_records,
        "record_count": len(content_records),
        "skipped_routes": skipped_routes,
        "budget": {
            "sites": budget_sites,
        },
        "policy_skips": policy_skips,
        "non_sample_record_fingerprints": non_sample_record_fingerprints,
        "executed_routes": executed_routes,
        "document_queue": document_queue,
        "sites": sites,
        "dry_run": all(bool(part.get("dry_run", False)) for part in payloads),
        "trust_embargo": any(bool(part.get("trust_embargo", False)) for part in payloads),
    }
    payload["executed_route_count"] = len(executed_routes)
    payload["skipped_route_count"] = len(skipped_routes)
    payload["budget"]["executed_routes"] = payload["executed_route_count"]
    payload["budget"]["skipped_routes"] = payload["skipped_route_count"]
    payload["runtime_summary"] = {
        "record_count": payload["record_count"],
        "page_records": payload["page_records"],
        "document_records": payload["document_records"],
        "visited_route_families": list(payload["visited_route_families"]),
        "executed_route_count": payload["executed_route_count"],
        "skipped_route_count": payload["skipped_route_count"],
        "document_queue_count": len(payload["document_queue"]),
        "policy_skip_count": len(payload["policy_skips"]),
        "non_sample_record_count": len(non_sample_record_fingerprints),
    }
    return payload


def _apply_plan_trust_gate(
    plan: FactorySitePlan,
    *,
    trust_state: str,
    trust_verdict: str,
    trust_summary: str,
    cheap_record_count: int,
) -> None:
    plan.fetch_policy.trust_state = trust_state
    plan.fetch_policy.trust_verdict = trust_verdict
    plan.fetch_policy.trust_summary = trust_summary
    plan.fetch_policy.heavy_fetch_embargo = True
    plan.notes.append(
        "factory-site trust gate: "
        f"state={trust_state} | "
        f"verdict={trust_verdict or 'unknown'} | "
        f"cheap_records={cheap_record_count} | "
        f"heavy_fetch={'eligible' if trust_state == FACTORY_SITE_TRUST_STATE_TRUSTED else 'embargoed'}"
    )


def _reset_plan_runtime_after_embargo(plan: FactorySitePlan) -> None:
    plan.fetch_telemetry.clear()
    plan.access_state = ""
    plan.block_class = ""
    plan.anti_bot_reason = ""
    plan.breaker_mode = "normal"
    plan.manual_handoff_required = False
    plan.challenge_detected = False
    plan.session_reused = False
    plan.notes = [
        note
        for note in plan.notes
        if not any(note.startswith(prefix) for prefix in _FETCH_RUNTIME_NOTE_PREFIXES)
    ]


class FactorySiteParser:
    def __init__(
        self,
        client: Any,
        *,
        proxy_pool: ProxyPool | None = None,
        attachments_root: Path | None = None,
        planner: FactorySitePlanner | None = None,
        fetch_stage: FactorySiteFetchStage | None = None,
        documents_stage: FactorySiteDocumentsStage | None = None,
        okved_matcher: FactorySiteOkvedMatcher | None = None,
        store: FactorySiteStore | None = None,
        max_sites: int | None = None,
        max_routes_per_site: int = 4,
    ) -> None:
        self.client = FactorySiteParserClient(client, proxy_pool=proxy_pool)
        self.documents_stage = documents_stage or FactorySiteDocumentsStage(self.client, storage_root=attachments_root)
        self.planner = planner or FactorySitePlanner(self.client)
        self.fetch_stage = fetch_stage or FactorySiteFetchStage(
            self.client,
            proxy_pool=self.client.proxy_pool,
            documents_stage=self.documents_stage,
            max_routes_per_site=max_routes_per_site,
        )
        self.okved_matcher = okved_matcher or FactorySiteOkvedMatcher()
        self.store = store or FactorySiteStore()
        self.max_sites = max_sites

    def parse(self, company: FactorySiteParserCompany, *, dry_run: bool = False) -> FactorySiteParserResult:
        max_sites = 1 if dry_run else self.max_sites
        plans = self.planner.plan(company, max_sites=max_sites)
        plans = _with_dry_run_homepage_fallback(plans, dry_run=dry_run)
        content_records, crawl_execution = _normalize_fetch_output(
            self.fetch_stage.fetch(company, plans, dry_run=dry_run, trust_embargo=True)
        )

        cheap_records_by_site = _group_records_by_site(content_records)
        trusted_site_keys: set[str] = set()
        for plan in plans:
            site_records = cheap_records_by_site.get(_site_key(plan.site_url), [])
            site_profile, _site_matches = self.okved_matcher.match_records(company, site_records)
            site_match = site_profile.site_match if site_profile is not None else None
            trust_verdict = getattr(site_match, "verdict", "")
            trust_summary = getattr(site_match, "summary", "")
            trust_state = factory_site_trust_state_from_verdict(trust_verdict)
            if trust_state not in {FACTORY_SITE_TRUST_STATE_TRUSTED, FACTORY_SITE_TRUST_STATE_AMBIGUOUS}:
                trust_state = FACTORY_SITE_TRUST_STATE_AMBIGUOUS if not trust_verdict else trust_state
            _apply_plan_trust_gate(
                plan,
                trust_state=trust_state,
                trust_verdict=trust_verdict,
                trust_summary=trust_summary,
                cheap_record_count=len(site_records),
            )
            if trust_state == FACTORY_SITE_TRUST_STATE_TRUSTED:
                trusted_site_keys.add(_site_key(plan.site_url))

        final_content_records = list(content_records)
        final_crawl_execution = _merge_crawl_executions([crawl_execution], content_records=final_content_records)
        if trusted_site_keys and not dry_run:
            trusted_plans = [plan for plan in plans if _site_key(plan.site_url) in trusted_site_keys]
            for plan in trusted_plans:
                _reset_plan_runtime_after_embargo(plan)
                plan.fetch_policy.heavy_fetch_embargo = False
                plan.notes.append("factory-site trust gate: heavy fetch unlocked for trusted site")
            trusted_content_records, trusted_crawl_execution = _normalize_fetch_output(
                self.fetch_stage.fetch(company, trusted_plans, dry_run=dry_run, trust_embargo=False)
            )
            non_trusted_site_keys = {_site_key(plan.site_url) for plan in plans if _site_key(plan.site_url) not in trusted_site_keys}
            non_trusted_records = _select_records_for_sites(content_records, site_keys=non_trusted_site_keys)
            non_trusted_crawl_execution = _slice_crawl_execution(crawl_execution, site_keys=non_trusted_site_keys)
            final_content_records = [*non_trusted_records, *trusted_content_records]
            final_crawl_execution = _merge_crawl_executions(
                [non_trusted_crawl_execution, trusted_crawl_execution],
                content_records=final_content_records,
            )
        okved_profile, okved_matches = self.okved_matcher.match_records(company, final_content_records)
        result = self.store.build_result(
            company=company,
            plans=plans,
            content_records=final_content_records,
            okved_profile=okved_profile,
            okved_matches=okved_matches,
            crawl_execution=final_crawl_execution,
        )
        _require_mapping_attr(result, "relevance_summary")
        _require_mapping_attr(result, "lead_assembly")
        _require_mapping_attr(result, "crawl_execution")
        _require_list_attr(result, "visited_route_families")
        if not isinstance(getattr(result, "page_records", None), int):
            raise TypeError("FactorySiteStore.build_result() must set result.page_records as int.")
        if not isinstance(getattr(result, "document_records", None), int):
            raise TypeError("FactorySiteStore.build_result() must set result.document_records as int.")
        return result


__all__ = ["FactorySiteParser"]

from __future__ import annotations

from contextlib import contextmanager
from dataclasses import asdict, is_dataclass
from typing import Any

from app.runtime import ProxyPool
from app.site_intelligence.antibot import (
    ACCESS_STATE_BLOCKED,
    ACCESS_STATE_COMPLETED_WITH_CONTENT,
    ACCESS_STATE_MANUAL_HANDOFF_REQUIRED,
    ACCESS_STATE_PAUSED_BY_BREAKER,
    ACCESS_STATE_RECOVERED,
    block_class_priority,
    breaker_mode_rank,
)
from app.site_intelligence import Fetcher, Normalizer
from app.site_intelligence.fetcher import FetchResult, FetchTelemetry
from app.site_intelligence.models import ContentRecord

from .documents import FactorySiteDocumentsStage
from .models import (
    FactorySiteParserCompany,
    FactorySitePlan,
    route_mode_with_browser_embargo,
    route_requires_trusted_fetch,
)


class FactorySiteFetchStage:
    def __init__(
        self,
        client: object,
        *,
        proxy_pool: ProxyPool | None = None,
        fetcher: Fetcher | None = None,
        normalizer: Normalizer | None = None,
        documents_stage: FactorySiteDocumentsStage | None = None,
        max_routes_per_site: int = 4,
    ) -> None:
        self.fetcher = fetcher or Fetcher(client, proxy_pool=proxy_pool)
        self.normalizer = normalizer or Normalizer()
        self.documents_stage = documents_stage or FactorySiteDocumentsStage(client)
        self.max_routes_per_site = max(1, max_routes_per_site)

    def fetch(
        self,
        company: FactorySiteParserCompany,
        plans: list[FactorySitePlan],
        *,
        dry_run: bool = False,
        trust_embargo: bool = False,
    ) -> tuple[list[ContentRecord], dict[str, Any]]:
        records: list[ContentRecord] = []
        collector = self.documents_stage.build_collector(company.company_id)
        crawl_execution: dict[str, Any] = {
            "visited_route_families": [],
            "page_records": 0,
            "document_records": 0,
            "record_count": 0,
            "skipped_routes": [],
            "budget": {
                "sites": [],
                "executed_routes": 0,
                "skipped_routes": 0,
            },
            "policy_skips": [],
            "non_sample_record_fingerprints": [],
            "executed_routes": [],
            "document_queue": [],
            "sites": [],
            "dry_run": dry_run,
            "trust_embargo": trust_embargo,
        }
        visited_route_families: set[str] = set()
        non_sample_record_fingerprints: set[str] = set()

        with self._playwright_embargo(enabled=trust_embargo):
            for plan in plans:
                route_results: list[FetchResult] = []
                seen_route_patterns: set[str] = set()
                executed_routes_for_site = 0
                execution_route_limit = self._execution_route_limit(dry_run=dry_run)
                site_execution: dict[str, Any] = {
                    "site_url": plan.site_url,
                    "planned_routes": len(plan.routes),
                    "executed_routes": [],
                    "skipped_routes": [],
                    "visited_route_families": [],
                    "page_records": 0,
                    "document_records": 0,
                    "budget": self._serialize_value(getattr(plan, "budget_accounting", None)),
                    "trust_embargo": trust_embargo,
                }
                crawl_execution["budget"]["sites"].append(
                    {
                        "site_url": plan.site_url,
                        "budget": site_execution["budget"],
                    }
                )
                planner_skips = self._collect_planner_skips(plan)
                for skip_entry in planner_skips:
                    crawl_execution["skipped_routes"].append(skip_entry)
                    site_execution["skipped_routes"].append(skip_entry)
                    if self._is_policy_skip(skip_entry):
                        crawl_execution["policy_skips"].append(skip_entry)

                for index, route in enumerate(plan.routes):
                    if trust_embargo and route_requires_trusted_fetch(route):
                        skip_entry = self._build_skip_entry(
                            plan=plan,
                            route=route,
                            reason="trust_embargo_heavy_route",
                            phase="execution",
                        )
                        skip_entry["trust_embargo"] = True
                        skip_entry["requested_mode"] = getattr(route, "mode", "")
                        crawl_execution["skipped_routes"].append(skip_entry)
                        site_execution["skipped_routes"].append(skip_entry)
                        crawl_execution["policy_skips"].append(skip_entry)
                        continue
                    if executed_routes_for_site >= execution_route_limit:
                        skip_entry = self._build_aggregated_tail_skip(
                            plan=plan,
                            routes=plan.routes[index:],
                            reason="dry_run" if dry_run else "max_routes_per_site",
                            phase="execution",
                        )
                        if skip_entry is not None:
                            crawl_execution["skipped_routes"].append(skip_entry)
                            site_execution["skipped_routes"].append(skip_entry)
                        break
                    if route.route_pattern in seen_route_patterns:
                        skip_entry = self._build_skip_entry(
                            plan=plan,
                            route=route,
                            reason="duplicate_route_pattern",
                            phase="execution",
                        )
                        crawl_execution["skipped_routes"].append(skip_entry)
                        site_execution["skipped_routes"].append(skip_entry)
                        continue
                    seen_route_patterns.add(route.route_pattern)
                    route_origin = self._route_origin(plan, route)
                    effective_mode = route_mode_with_browser_embargo(route, browser_embargo=trust_embargo)
                    route_execution = {
                        "site_url": plan.site_url,
                        "route_pattern": route.route_pattern,
                        "route_family": route.route_family,
                        "section_guess": route.section_guess,
                        "route_origin": route_origin,
                        "from_sample": route_origin == "sample",
                        "dry_run": dry_run,
                        "status": "executed",
                        "page_records": 0,
                        "document_records": 0,
                        "trust_embargo": trust_embargo,
                        "requested_mode": getattr(route, "mode", ""),
                        "fetch_mode": effective_mode,
                        "browser_embargoed": trust_embargo and effective_mode != getattr(route, "mode", ""),
                        "document_embargoed": trust_embargo,
                    }

                    response, fetch_status, fetch_notes = self.fetcher.fetch(
                        route.route_pattern,
                        effective_mode,
                        route_family=route.route_family,
                        section_name=route.section_guess,
                    )
                    fetch_result = self.fetcher.last_fetch_result
                    telemetry = self.fetcher.last_fetch_telemetry
                    if fetch_result is not None:
                        route_results.append(fetch_result)
                        plan.fetch_telemetry.extend(fetch_result.attempts)
                        route_execution["access_state"] = fetch_result.access_state
                        route_execution["blocked_by_policy"] = fetch_result.blocked_by_policy
                        route_execution["manual_handoff_required"] = fetch_result.manual_handoff_required
                        route_execution["transport_final"] = fetch_result.transport_final
                        route_execution["escalation_reason"] = fetch_result.escalation_reason
                        if fetch_result.blocked_by_policy:
                            crawl_execution["policy_skips"].append(
                                self._build_skip_entry(
                                    plan=plan,
                                    route=route,
                                    reason=fetch_result.escalation_reason or "policy_blocked",
                                    phase="fetch",
                                )
                            )
                    elif telemetry is not None:
                        plan.fetch_telemetry.append(telemetry)
                    route_execution["fetch_status"] = fetch_status
                    if response is not None and getattr(response, "url", None):
                        route_execution["response_url"] = response.url
                    direct_records: list[ContentRecord] = []
                    if not trust_embargo:
                        direct_records = self.documents_stage.collect_direct_response(
                            collector=collector,
                            company_id=company.company_id,
                            site_url=plan.site_url,
                            response=response,
                            source_url=(response.url if response else route.route_pattern),
                            referrer_url=plan.site_url,
                            section_guess=route.section_guess,
                            route_family=route.route_family,
                        )
                    if direct_records:
                        self._attach_fetch_trace(direct_records, fetch_result, telemetry)
                        self._attach_crawl_trace(
                            direct_records,
                            plan=plan,
                            route=route,
                            route_execution=route_execution,
                            source_kind="document",
                            route_origin=route_origin,
                        )
                        records.extend(direct_records)
                        route_execution["content_fingerprint"] = self._primary_record_fingerprint(direct_records)
                        route_execution["content_fingerprints"] = self._record_fingerprints(direct_records)
                        route_execution["document_records"] = len(direct_records)
                        site_execution["document_records"] += len(direct_records)
                        crawl_execution["document_records"] += len(direct_records)
                        executed_routes_for_site += 1
                        self._track_document_queue(
                            crawl_execution,
                            plan=plan,
                            route=route,
                            route_origin=route_origin,
                            records=direct_records,
                        )
                        if route.route_family:
                            visited_route_families.add(route.route_family)
                            site_execution["visited_route_families"].append(route.route_family)
                        crawl_execution["executed_routes"].append(dict(route_execution))
                        site_execution["executed_routes"].append(dict(route_execution))
                        if route_origin != "sample":
                            self._collect_record_fingerprints(non_sample_record_fingerprints, direct_records)
                        continue

                    record = self.normalizer.normalize_html_record(
                        company_id=company.company_id,
                        site_url=plan.site_url,
                        route=route,
                        response=response,
                        fetch_status=fetch_status,
                        notes=fetch_notes,
                    )
                    self._attach_fetch_trace([record], fetch_result, telemetry)
                    self._attach_crawl_trace(
                        [record],
                        plan=plan,
                        route=route,
                        route_execution=route_execution,
                        source_kind="page",
                        route_origin=route_origin,
                    )
                    records.append(record)
                    route_execution["page_records"] = 1
                    site_execution["page_records"] += 1
                    crawl_execution["page_records"] += 1
                    attachment_records: list[ContentRecord] = []
                    if not trust_embargo:
                        attachment_records = self.documents_stage.collect_html_attachments(
                            collector=collector,
                            company_id=company.company_id,
                            site_url=plan.site_url,
                            response=response,
                            fetch_status=fetch_status,
                            section_guess=route.section_guess,
                            route_family=route.route_family,
                        )
                    self._attach_fetch_trace(attachment_records, fetch_result, telemetry)
                    self._attach_crawl_trace(
                        attachment_records,
                        plan=plan,
                        route=route,
                        route_execution=route_execution,
                        source_kind="document",
                        route_origin=route_origin,
                    )
                    records.extend(attachment_records)
                    route_records = [record, *attachment_records]
                    route_execution["content_fingerprint"] = self._primary_record_fingerprint(route_records)
                    route_execution["content_fingerprints"] = self._record_fingerprints(route_records)
                    route_execution["document_records"] = len(attachment_records)
                    site_execution["document_records"] += len(attachment_records)
                    crawl_execution["document_records"] += len(attachment_records)
                    executed_routes_for_site += 1
                    self._track_document_queue(
                        crawl_execution,
                        plan=plan,
                        route=route,
                        route_origin=route_origin,
                        records=attachment_records,
                    )
                    if route.route_family:
                        visited_route_families.add(route.route_family)
                        site_execution["visited_route_families"].append(route.route_family)
                    crawl_execution["executed_routes"].append(dict(route_execution))
                    site_execution["executed_routes"].append(dict(route_execution))
                    if route_origin != "sample":
                        self._collect_record_fingerprints(non_sample_record_fingerprints, [record, *attachment_records])
                    continue
                self._finalize_plan_access_state(plan, route_results)
                site_execution["visited_route_families"] = sorted(set(site_execution["visited_route_families"]))
                site_execution["executed_route_count"] = len(site_execution["executed_routes"])
                site_execution["skipped_route_count"] = len(site_execution["skipped_routes"])
                crawl_execution["sites"].append(site_execution)
                plan.notes.append(
                    "factory-site raw crawl execution: "
                    f"planned_routes={site_execution['planned_routes']} | "
                    f"raw_executed={site_execution['executed_route_count']} | "
                    f"raw_skipped={site_execution['skipped_route_count']} | "
                    f"raw_pages={site_execution['page_records']} | "
                    f"raw_documents={site_execution['document_records']} | "
                    f"raw_families={','.join(site_execution['visited_route_families']) or 'none'} | "
                    f"trust_embargo={'true' if trust_embargo else 'false'}"
                )

        crawl_execution["visited_route_families"] = sorted(visited_route_families)
        crawl_execution["non_sample_record_fingerprints"] = sorted(non_sample_record_fingerprints)
        crawl_execution["record_count"] = len(records)
        crawl_execution["executed_route_count"] = len(crawl_execution["executed_routes"])
        crawl_execution["skipped_route_count"] = len(crawl_execution["skipped_routes"])
        crawl_execution["budget"]["executed_routes"] = len(crawl_execution["executed_routes"])
        crawl_execution["budget"]["skipped_routes"] = len(crawl_execution["skipped_routes"])
        crawl_execution["runtime_summary"] = {
            "record_count": crawl_execution["record_count"],
            "page_records": crawl_execution["page_records"],
            "document_records": crawl_execution["document_records"],
            "visited_route_families": list(crawl_execution["visited_route_families"]),
            "executed_route_count": crawl_execution["executed_route_count"],
            "skipped_route_count": crawl_execution["skipped_route_count"],
            "document_queue_count": len(crawl_execution["document_queue"]),
            "policy_skip_count": len(crawl_execution["policy_skips"]),
            "non_sample_record_count": len(crawl_execution["non_sample_record_fingerprints"]),
        }
        return records, crawl_execution

    def _execution_route_limit(self, *, dry_run: bool) -> int:
        return 1 if dry_run else self.max_routes_per_site

    @contextmanager
    def _playwright_embargo(self, *, enabled: bool) -> Any:
        if not enabled:
            yield
            return
        previous = self.fetcher.playwright_enabled
        self.fetcher.playwright_enabled = False
        try:
            yield
        finally:
            self.fetcher.playwright_enabled = previous

    def _attach_fetch_trace(
        self,
        records: list[ContentRecord],
        fetch_result: FetchResult | None,
        telemetry: FetchTelemetry | None,
    ) -> None:
        if telemetry is None and fetch_result is None:
            return
        telemetry_payload = asdict(telemetry) if telemetry is not None else {}
        attempt_payloads = [item.to_trace() for item in (fetch_result.attempts if fetch_result is not None else [])]
        for record in records:
            record.trace.setdefault("factory_site_parser", {})
            if telemetry_payload:
                record.trace["factory_site_parser"]["fetch"] = dict(telemetry_payload)
            if fetch_result is not None:
                record.trace["factory_site_parser"]["access_state"] = fetch_result.access_state
                record.trace["factory_site_parser"]["manual_handoff_required"] = fetch_result.manual_handoff_required
                record.trace["factory_site_parser"]["transport_selected"] = fetch_result.transport_selected
                record.trace["factory_site_parser"]["transport_final"] = fetch_result.transport_final
                record.trace["factory_site_parser"]["escalation_reason"] = fetch_result.escalation_reason
                record.trace["factory_site_parser"]["blocked_by_policy"] = fetch_result.blocked_by_policy
                if attempt_payloads:
                    record.trace["factory_site_parser"]["fetch_attempts"] = list(attempt_payloads)

    def _finalize_plan_access_state(self, plan: FactorySitePlan, route_results: list[FetchResult]) -> None:
        if not route_results:
            return
        states = [result.access_state for result in route_results if result.access_state]
        if ACCESS_STATE_RECOVERED in states:
            plan.access_state = ACCESS_STATE_RECOVERED
        elif ACCESS_STATE_COMPLETED_WITH_CONTENT in states:
            plan.access_state = ACCESS_STATE_COMPLETED_WITH_CONTENT
        elif ACCESS_STATE_MANUAL_HANDOFF_REQUIRED in states:
            plan.access_state = ACCESS_STATE_MANUAL_HANDOFF_REQUIRED
        elif ACCESS_STATE_PAUSED_BY_BREAKER in states:
            plan.access_state = ACCESS_STATE_PAUSED_BY_BREAKER
        else:
            plan.access_state = ACCESS_STATE_BLOCKED

        block_classes = [result.block_class for result in route_results if result.block_class]
        if block_classes:
            plan.block_class = max(block_classes, key=block_class_priority)
        anti_bot_reasons = [result.anti_bot_reason for result in route_results if result.anti_bot_reason]
        if anti_bot_reasons:
            plan.anti_bot_reason = anti_bot_reasons[0]
        plan.breaker_mode = max(
            (result.breaker_mode for result in route_results if result.breaker_mode),
            key=breaker_mode_rank,
            default="normal",
        )
        plan.manual_handoff_required = any(result.manual_handoff_required for result in route_results)
        plan.challenge_detected = any(result.challenge_detected for result in route_results)
        plan.session_reused = any(result.session_reused for result in route_results)
        note_parts = [f"access_state={plan.access_state}"]
        if plan.block_class:
            note_parts.append(f"block_class={plan.block_class}")
        if plan.anti_bot_reason:
            note_parts.append(f"reason={plan.anti_bot_reason}")
        if plan.breaker_mode and plan.breaker_mode != "normal":
            note_parts.append(f"breaker={plan.breaker_mode}")
        if plan.session_reused:
            note_parts.append("session_reused=true")
        last_result = route_results[-1]
        if last_result.transport_final:
            note_parts.append(f"transport={last_result.transport_final}")
        if last_result.escalation_reason:
            note_parts.append(f"policy={last_result.escalation_reason}")
        if last_result.blocked_by_policy:
            note_parts.append("blocked_by_policy=true")
        plan.notes.append("factory-site fetch: " + " | ".join(note_parts))

    def _attach_crawl_trace(
        self,
        records: list[ContentRecord],
        *,
        plan: FactorySitePlan,
        route: Any,
        route_execution: dict[str, Any],
        source_kind: str,
        route_origin: str,
    ) -> None:
        for record in records:
            record.trace.setdefault("factory_site_parser", {})
            record.trace["factory_site_parser"]["crawl"] = {
                "site_url": plan.site_url,
                "route_pattern": route.route_pattern,
                "route_family": route.route_family,
                "section_guess": route.section_guess,
                "route_origin": route_origin,
                "from_sample": route_origin == "sample",
                "source_kind": source_kind,
                "status": route_execution.get("status", "executed"),
                "fetch_status": route_execution.get("fetch_status"),
                "blocked_by_policy": route_execution.get("blocked_by_policy", False),
                "access_state": route_execution.get("access_state"),
                "trust_state": getattr(plan.fetch_policy, "trust_state", ""),
                "trust_verdict": getattr(plan.fetch_policy, "trust_verdict", ""),
                "trust_summary": getattr(plan.fetch_policy, "trust_summary", ""),
                "heavy_fetch_embargo": bool(getattr(plan.fetch_policy, "heavy_fetch_embargo", False)),
            }

    def _track_document_queue(
        self,
        crawl_execution: dict[str, Any],
        *,
        plan: FactorySitePlan,
        route: Any,
        route_origin: str,
        records: list[ContentRecord],
    ) -> None:
        for record in records:
            crawl_execution["document_queue"].append(
                {
                    "site_url": plan.site_url,
                    "route_pattern": route.route_pattern,
                    "route_family": route.route_family,
                    "route_origin": route_origin,
                    "from_sample": route_origin == "sample",
                    "url": record.url,
                    "content_fingerprint": record.content_fingerprint,
                    "status": "crawled",
                    "fetch_status": record.fetch_status,
                }
            )

    def _collect_record_fingerprints(self, bucket: set[str], records: list[ContentRecord]) -> None:
        for record in records:
            fingerprint = record.content_fingerprint or f"{record.url}|{record.fetch_status}|{record.source_type}"
            bucket.add(fingerprint)

    def _primary_record_fingerprint(self, records: list[ContentRecord]) -> str | None:
        fingerprints = self._record_fingerprints(records)
        return fingerprints[0] if fingerprints else None

    def _record_fingerprints(self, records: list[ContentRecord]) -> list[str]:
        fingerprints: list[str] = []
        for record in records:
            fingerprint = record.content_fingerprint or f"{record.url}|{record.fetch_status}|{record.source_type}"
            fingerprints.append(fingerprint)
        return fingerprints

    def _route_origin(self, plan: FactorySitePlan, route: Any) -> str:
        sampled_urls = set(getattr(getattr(plan, "probe", None), "sampled_urls", []) or [])
        if route.route_pattern in sampled_urls:
            return "sample"
        if route.route_pattern == plan.site_url:
            return "homepage"
        return "planned"

    def _collect_planner_skips(self, plan: FactorySitePlan) -> list[dict[str, Any]]:
        budget_accounting = getattr(plan, "budget_accounting", None)
        skipped_routes = getattr(budget_accounting, "skipped_routes", None)
        if skipped_routes is None:
            skipped_routes = getattr(getattr(plan, "crawl_map", None), "skipped_routes", None)
        result: list[dict[str, Any]] = []
        for item in skipped_routes or []:
            payload = self._serialize_value(item)
            if isinstance(payload, dict):
                result.append(self._normalize_skip_entry(plan=plan, payload=payload, phase="planning"))
            else:
                result.append(
                    self._normalize_skip_entry(
                        plan=plan,
                        payload={"details": payload},
                        phase="planning",
                    )
                )
        return result

    def _build_skip_entry(
        self,
        *,
        plan: FactorySitePlan,
        route: Any,
        reason: str,
        phase: str,
    ) -> dict[str, Any]:
        route_origin = self._route_origin(plan, route)
        return self._normalize_skip_entry(
            plan=plan,
            payload={
                "route_pattern": route.route_pattern,
                "route_family": route.route_family,
                "section_guess": route.section_guess,
                "route_origin": route_origin,
                "from_sample": route_origin == "sample",
            },
            reason=reason,
            phase=phase,
        )

    def _build_aggregated_tail_skip(
        self,
        *,
        plan: FactorySitePlan,
        routes: list[Any],
        reason: str,
        phase: str,
    ) -> dict[str, Any] | None:
        if not routes:
            return None
        route_families = sorted({route.route_family for route in routes if getattr(route, "route_family", None)})
        route_origins = {self._route_origin(plan, route) for route in routes}
        if len(route_origins) == 1:
            route_origin = next(iter(route_origins))
        else:
            route_origin = "mixed"
        return self._normalize_skip_entry(
            plan=plan,
            payload={
                "route_pattern": "__aggregated_execution_tail__",
                "route_family": route_families[0] if len(route_families) == 1 else None,
                "route_origin": route_origin,
                "from_sample": route_origin == "sample",
                "aggregated": True,
                "skipped_route_count": len(routes),
                "tail_route_patterns_preview": [route.route_pattern for route in routes[:3]],
                "tail_route_families": route_families,
                "tail_route_family_count": len(route_families),
            },
            reason=reason,
            phase=phase,
        )

    def _is_policy_skip(self, skip_entry: dict[str, Any]) -> bool:
        reason = str(skip_entry.get("reason") or skip_entry.get("skip_reason") or "").lower()
        return any(token in reason for token in ("policy", "robot", "breaker", "handoff", "blocked", "embargo"))

    def _normalize_skip_entry(
        self,
        *,
        plan: FactorySitePlan,
        payload: dict[str, Any],
        phase: str,
        reason: str | None = None,
    ) -> dict[str, Any]:
        entry = dict(payload)
        legacy_reason = entry.pop("skip_reason", None)
        canonical_reason = reason or entry.get("reason") or legacy_reason or "unspecified"
        entry["site_url"] = entry.get("site_url") or plan.site_url
        entry["phase"] = entry.get("phase") or phase
        entry["reason"] = str(canonical_reason)
        return entry

    def _serialize_value(self, value: Any) -> Any:
        if value is None:
            return None
        if is_dataclass(value):
            return asdict(value)
        if isinstance(value, dict):
            return {key: self._serialize_value(item) for key, item in value.items()}
        if isinstance(value, (list, tuple, set)):
            return [self._serialize_value(item) for item in value]
        if hasattr(value, "__dict__"):
            return {
                key: self._serialize_value(item)
                for key, item in vars(value).items()
                if not key.startswith("_")
            }
        return value


__all__ = ["FactorySiteFetchStage"]

from __future__ import annotations

from dataclasses import asdict
from typing import Any

from app.site_intelligence.common import dedupe_preserve_order
from app.site_intelligence.models import ContentRecord
from app.site_intelligence.relevance import summarize_record_set

from .models import (
    FactorySiteOkvedMatch,
    FactorySiteOkvedProfile,
    FactorySiteParserCompany,
    FactorySiteParserResult,
    FactorySitePlan,
)


def _build_lead_assembly(relevance_summary: dict[str, Any]) -> dict[str, Any]:
    lead_evidence = list(relevance_summary.get("lead_evidence", []))
    return {
        "mode": "full_record_set",
        "record_count": relevance_summary.get("record_count", 0),
        "page_records": relevance_summary.get("page_records", 0),
        "document_records": relevance_summary.get("document_records", 0),
        "route_families": list(relevance_summary.get("route_families", [])),
        "lead_families": list(relevance_summary.get("lead_families", [])),
        "lead_evidence_count": len(lead_evidence),
        "lead_evidence": lead_evidence,
        "non_sample_evidence_count": relevance_summary.get("non_sample_evidence_count", 0),
        "relevant_record_fingerprints": list(relevance_summary.get("relevant_record_fingerprints", [])),
        "non_sample_record_fingerprints": list(relevance_summary.get("non_sample_record_fingerprints", [])),
    }


def _is_ambiguous_crawl_note(note: Any) -> bool:
    if not isinstance(note, str):
        return False
    normalized = note.strip().lower()
    if not normalized.startswith("factory-site crawl "):
        return False
    return not normalized.startswith("factory-site crawl summary:")


class FactorySiteStore:
    def build_result(
        self,
        *,
        company: FactorySiteParserCompany,
        plans: list[FactorySitePlan],
        content_records: list[ContentRecord],
        okved_profile: FactorySiteOkvedProfile,
        okved_matches: list[FactorySiteOkvedMatch],
        crawl_execution: dict[str, Any] | None = None,
    ) -> FactorySiteParserResult:
        match_by_fingerprint = {
            match.record_fingerprint: match
            for match in okved_matches
            if match.record_fingerprint
        }
        normalized_records = self._dedupe_records(content_records)
        for record in normalized_records:
            match = match_by_fingerprint.get(record.content_fingerprint)
            if not match:
                continue
            record.trace.setdefault("factory_site_parser", {})
            record.trace["factory_site_parser"]["okved_match"] = {
                "score": match.score,
                "verdict": match.verdict,
                "positive_score": match.positive_score,
                "negative_score": match.negative_score,
                "positive_evidence": [asdict(item) for item in match.positive_evidence],
                "negative_evidence": [asdict(item) for item in match.negative_evidence],
                "matched_okved_codes": list(match.matched_okved_codes),
                "matched_terms": list(match.matched_terms),
                "summary": match.summary,
                "signal_breakdown": dict(match.signal_breakdown),
            }

        relevance_summary = summarize_record_set(normalized_records)
        crawl_execution_payload = self._normalize_crawl_execution(
            crawl_execution,
            relevance_summary=relevance_summary,
            normalized_records=normalized_records,
        )
        notes = dedupe_preserve_order(
            note
            for plan in plans
            for note in plan.notes
            if not _is_ambiguous_crawl_note(note)
        )
        if crawl_execution is not None:
            notes = dedupe_preserve_order(
                [
                    *notes,
                    "factory-site crawl summary: "
                    f"pages={crawl_execution_payload.get('page_records', 0)} | "
                    f"documents={crawl_execution_payload.get('document_records', 0)} | "
                    f"executed={crawl_execution_payload.get('executed_route_count', 0)} | "
                    f"skipped={crawl_execution_payload.get('skipped_route_count', 0)} | "
                    f"families={','.join(crawl_execution_payload.get('visited_route_families', [])) or 'none'}",
                ]
            )
        lead_assembly = _build_lead_assembly(relevance_summary)
        page_records = int(crawl_execution_payload.get("page_records", relevance_summary.get("page_records", 0)) or 0)
        document_records = int(crawl_execution_payload.get("document_records", relevance_summary.get("document_records", 0)) or 0)
        visited_route_families = dedupe_preserve_order(
            crawl_execution_payload.get("visited_route_families")
            or relevance_summary.get("route_families", [])
        )
        result = FactorySiteParserResult(
            company=company,
            plans=plans,
            site_probes=[plan.probe for plan in plans],
            route_strategies=[route for plan in plans for route in plan.routes],
            crawl_maps=[plan.crawl_map for plan in plans if plan.crawl_map is not None],
            content_records=normalized_records,
            fetch_telemetry=[item for plan in plans for item in plan.fetch_telemetry],
            okved_profile=okved_profile,
            okved_matches=okved_matches,
            okved_site_match=okved_profile.site_match if okved_profile else None,
            notes=notes,
        )
        setattr(result, "relevance_summary", relevance_summary)
        setattr(result, "lead_assembly", lead_assembly)
        setattr(result, "crawl_execution", crawl_execution_payload)
        setattr(result, "page_records", page_records)
        setattr(result, "document_records", document_records)
        setattr(result, "visited_route_families", list(visited_route_families))
        return result

    def _normalize_crawl_execution(
        self,
        crawl_execution: dict[str, Any] | None,
        *,
        relevance_summary: dict[str, Any],
        normalized_records: list[ContentRecord],
    ) -> dict[str, Any]:
        payload = dict(crawl_execution) if isinstance(crawl_execution, dict) else {}
        route_record_map = self._group_records_by_route(normalized_records)
        fingerprint_record_map = self._group_records_by_fingerprint(normalized_records)
        site_record_map = self._group_records_by_site(normalized_records)
        payload["executed_routes"] = self._normalize_executed_routes(
            payload.get("executed_routes", []) or [],
            route_record_map=route_record_map,
            fingerprint_record_map=fingerprint_record_map,
        )
        payload["skipped_routes"] = self._normalize_skipped_routes(payload.get("skipped_routes", []) or [])
        payload["document_queue"] = list(payload.get("document_queue", []) or [])
        payload["policy_skips"] = self._normalize_skipped_routes(payload.get("policy_skips", []) or [])
        visited_route_families = self._normalize_visited_route_families(
            payload["executed_routes"],
            fallback_families=payload.get("visited_route_families") or relevance_summary.get("route_families", []),
        )
        payload["visited_route_families"] = list(visited_route_families)
        payload["sites"] = self._normalize_site_executions(
            payload.get("sites", []) or [],
            site_record_map=site_record_map,
            executed_routes=payload["executed_routes"],
            skipped_routes=payload["skipped_routes"],
        )

        raw_non_sample_record_fingerprints = dedupe_preserve_order(
            payload.get("raw_non_sample_record_fingerprints")
            or payload.get("non_sample_record_fingerprints", [])
            or []
        )
        payload["raw_non_sample_record_fingerprints"] = list(raw_non_sample_record_fingerprints)
        payload["non_sample_record_fingerprints"] = list(
            relevance_summary.get("non_sample_record_fingerprints", raw_non_sample_record_fingerprints)
        )
        payload["normalized_non_sample_record_fingerprints"] = list(payload["non_sample_record_fingerprints"])

        raw_page_records = int(payload.get("raw_page_records", payload.get("page_records", 0)) or 0)
        raw_document_records = int(payload.get("raw_document_records", payload.get("document_records", 0)) or 0)
        raw_record_count = int(
            payload.get("raw_record_count", payload.get("record_count", raw_page_records + raw_document_records)) or 0
        )
        payload["raw_page_records"] = raw_page_records
        payload["raw_document_records"] = raw_document_records
        payload["raw_record_count"] = raw_record_count

        payload["page_records"] = int(relevance_summary.get("page_records", raw_page_records) or 0)
        payload["document_records"] = int(relevance_summary.get("document_records", raw_document_records) or 0)
        payload["record_count"] = int(relevance_summary.get("record_count", raw_record_count) or 0)
        payload["normalized_page_records"] = payload["page_records"]
        payload["normalized_document_records"] = payload["document_records"]
        payload["normalized_record_count"] = payload["record_count"]
        payload["executed_route_count"] = len(payload["executed_routes"])
        payload["skipped_route_count"] = len(payload["skipped_routes"])
        budget = dict(payload.get("budget") or {})
        budget["sites"] = list(budget.get("sites", []) or [])
        budget["executed_routes"] = payload["executed_route_count"]
        budget["skipped_routes"] = payload["skipped_route_count"]
        payload["budget"] = budget
        payload["runtime_summary"] = {
            "record_count": payload["record_count"],
            "page_records": payload["page_records"],
            "document_records": payload["document_records"],
            "raw_record_count": payload["raw_record_count"],
            "raw_page_records": payload["raw_page_records"],
            "raw_document_records": payload["raw_document_records"],
            "normalized_record_count": payload["normalized_record_count"],
            "normalized_page_records": payload["normalized_page_records"],
            "normalized_document_records": payload["normalized_document_records"],
            "raw_non_sample_record_count": len(payload["raw_non_sample_record_fingerprints"]),
            "normalized_non_sample_record_count": len(payload["normalized_non_sample_record_fingerprints"]),
            "visited_route_families": list(payload["visited_route_families"]),
            "executed_route_count": payload["executed_route_count"],
            "skipped_route_count": payload["skipped_route_count"],
            "document_queue_count": len(payload["document_queue"]),
            "policy_skip_count": len(payload["policy_skips"]),
            "non_sample_record_count": len(payload["non_sample_record_fingerprints"]),
        }
        return payload

    def _normalize_executed_routes(
        self,
        executed_routes: list[Any],
        *,
        route_record_map: dict[tuple[str, str], list[ContentRecord]],
        fingerprint_record_map: dict[str, ContentRecord],
    ) -> list[dict[str, Any]]:
        normalized: list[dict[str, Any]] = []
        for item in executed_routes:
            payload = dict(item) if isinstance(item, dict) else {"value": item}
            route_key = (
                str(payload.get("site_url", "") or ""),
                str(payload.get("route_pattern", "") or ""),
            )
            raw_content_fingerprints = dedupe_preserve_order(
                str(value or "").strip()
                for value in (
                    payload.get("raw_content_fingerprints")
                    or payload.get("content_fingerprints")
                    or [payload.get("content_fingerprint")]
                )
                if str(value or "").strip()
            )
            route_records = self._resolve_executed_route_records(
                payload,
                route_record_map=route_record_map,
                fingerprint_record_map=fingerprint_record_map,
                raw_content_fingerprints=raw_content_fingerprints,
            )
            raw_page_records = int(payload.get("raw_page_records", payload.get("page_records", 0)) or 0)
            raw_document_records = int(payload.get("raw_document_records", payload.get("document_records", 0)) or 0)
            raw_record_count = int(payload.get("raw_record_count", raw_page_records + raw_document_records) or 0)
            canonical_fingerprints = dedupe_preserve_order(
                self._record_fingerprint(record)
                for record in route_records
                if self._record_fingerprint(record)
            )
            canonical_page_records = sum(1 for record in route_records if self._record_source_kind(record) == "page")
            canonical_document_records = sum(1 for record in route_records if self._record_source_kind(record) == "document")

            payload["raw_page_records"] = raw_page_records
            payload["raw_document_records"] = raw_document_records
            payload["raw_record_count"] = raw_record_count
            payload["raw_content_fingerprints"] = list(raw_content_fingerprints)
            payload["page_records"] = canonical_page_records
            payload["document_records"] = canonical_document_records
            payload["record_count"] = len(route_records)
            payload["normalized_page_records"] = canonical_page_records
            payload["normalized_document_records"] = canonical_document_records
            payload["normalized_record_count"] = len(route_records)
            payload["content_fingerprints"] = list(canonical_fingerprints)
            payload["content_fingerprint"] = canonical_fingerprints[0] if canonical_fingerprints else ""
            payload["content_mapping_scope"] = "canonical_fingerprint"
            normalized.append(payload)
        return normalized

    def _normalize_site_executions(
        self,
        sites: list[Any],
        *,
        site_record_map: dict[str, list[ContentRecord]],
        executed_routes: list[dict[str, Any]],
        skipped_routes: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        executed_routes_by_site: dict[str, list[dict[str, Any]]] = {}
        for route in executed_routes:
            site_url = str(route.get("site_url", "") or "")
            if not site_url:
                continue
            executed_routes_by_site.setdefault(site_url, []).append(route)

        skipped_routes_by_site: dict[str, list[dict[str, Any]]] = {}
        for route in skipped_routes:
            site_url = str(route.get("site_url", "") or "")
            if not site_url:
                continue
            skipped_routes_by_site.setdefault(site_url, []).append(route)

        site_payloads: dict[str, dict[str, Any]] = {}
        site_order: list[str] = []
        for item in sites:
            payload = dict(item) if isinstance(item, dict) else {"value": item}
            site_url = str(payload.get("site_url", "") or "")
            if not site_url:
                continue
            if site_url not in site_payloads:
                site_order.append(site_url)
            site_payloads[site_url] = payload
        for site_url in [*executed_routes_by_site.keys(), *skipped_routes_by_site.keys(), *site_record_map.keys()]:
            if site_url and site_url not in site_payloads:
                site_order.append(site_url)
                site_payloads[site_url] = {"site_url": site_url}

        normalized: list[dict[str, Any]] = []
        for site_url in site_order:
            payload = dict(site_payloads.get(site_url) or {"site_url": site_url})
            site_records = list(site_record_map.get(site_url, []))
            raw_page_records = int(payload.get("raw_page_records", payload.get("page_records", 0)) or 0)
            raw_document_records = int(payload.get("raw_document_records", payload.get("document_records", 0)) or 0)
            raw_record_count = int(payload.get("raw_record_count", raw_page_records + raw_document_records) or 0)
            canonical_page_records = sum(1 for record in site_records if self._record_source_kind(record) == "page")
            canonical_document_records = sum(1 for record in site_records if self._record_source_kind(record) == "document")
            site_routes = list(executed_routes_by_site.get(site_url, []))
            site_skips = list(skipped_routes_by_site.get(site_url, []))
            visited_route_families = dedupe_preserve_order(
                [
                    str(route.get("route_family", "") or "").strip()
                    for route in site_routes
                    if str(route.get("route_family", "") or "").strip()
                ]
                or payload.get("visited_route_families")
                or [
                    self._record_route_family(record)
                    for record in site_records
                    if self._record_route_family(record)
                ]
            )

            payload["raw_page_records"] = raw_page_records
            payload["raw_document_records"] = raw_document_records
            payload["raw_record_count"] = raw_record_count
            payload["page_records"] = canonical_page_records
            payload["document_records"] = canonical_document_records
            payload["record_count"] = len(site_records)
            payload["normalized_page_records"] = canonical_page_records
            payload["normalized_document_records"] = canonical_document_records
            payload["normalized_record_count"] = len(site_records)
            payload["visited_route_families"] = list(visited_route_families)
            payload["executed_routes"] = site_routes
            payload["executed_route_count"] = len(site_routes)
            payload["skipped_routes"] = site_skips
            payload["skipped_route_count"] = len(site_skips)
            normalized.append(payload)
        return normalized

    def _normalize_skipped_routes(self, skipped_routes: list[Any]) -> list[dict[str, Any]]:
        normalized: list[dict[str, Any]] = []
        for item in skipped_routes:
            payload = dict(item) if isinstance(item, dict) else {"value": item}
            normalized.append(payload)
        return normalized

    def _normalize_visited_route_families(
        self,
        executed_routes: list[dict[str, Any]],
        *,
        fallback_families: list[Any] | tuple[Any, ...] | None,
    ) -> list[str]:
        route_families = dedupe_preserve_order(
            str(item.get("route_family", "") or "").strip()
            for item in executed_routes
            if isinstance(item, dict) and str(item.get("route_family", "") or "").strip()
        )
        if route_families:
            return list(route_families)
        return dedupe_preserve_order(
            str(item or "").strip()
            for item in (fallback_families or [])
            if str(item or "").strip()
        )

    def _resolve_executed_route_records(
        self,
        payload: dict[str, Any],
        *,
        route_record_map: dict[tuple[str, str], list[ContentRecord]],
        fingerprint_record_map: dict[str, ContentRecord],
        raw_content_fingerprints: list[str],
    ) -> list[ContentRecord]:
        fingerprint_records = [
            fingerprint_record_map[fingerprint]
            for fingerprint in raw_content_fingerprints
            if fingerprint in fingerprint_record_map
        ]
        if fingerprint_records:
            return list(fingerprint_records)
        route_key = (
            str(payload.get("site_url", "") or ""),
            str(payload.get("route_pattern", "") or ""),
        )
        return list(route_record_map.get(route_key, []))

    def _group_records_by_route(self, records: list[ContentRecord]) -> dict[tuple[str, str], list[ContentRecord]]:
        grouped: dict[tuple[str, str], list[ContentRecord]] = {}
        for record in records:
            crawl_trace = self._crawl_trace(record)
            route_pattern = str(crawl_trace.get("route_pattern", "") or "")
            if not route_pattern:
                continue
            site_url = str(crawl_trace.get("site_url", "") or record.site_url or "")
            key = (site_url, route_pattern)
            grouped.setdefault(key, []).append(record)
        return grouped

    def _group_records_by_site(self, records: list[ContentRecord]) -> dict[str, list[ContentRecord]]:
        grouped: dict[str, list[ContentRecord]] = {}
        for record in records:
            crawl_trace = self._crawl_trace(record)
            site_url = str(crawl_trace.get("site_url", "") or record.site_url or "")
            if not site_url:
                continue
            grouped.setdefault(site_url, []).append(record)
        return grouped

    def _group_records_by_fingerprint(self, records: list[ContentRecord]) -> dict[str, ContentRecord]:
        grouped: dict[str, ContentRecord] = {}
        for record in records:
            fingerprint = self._record_fingerprint(record)
            if not fingerprint or fingerprint in grouped:
                continue
            grouped[fingerprint] = record
        return grouped

    def _crawl_trace(self, record: ContentRecord) -> dict[str, Any]:
        parser_trace = record.trace.get("factory_site_parser", {})
        if not isinstance(parser_trace, dict):
            return {}
        crawl_trace = parser_trace.get("crawl", {})
        return crawl_trace if isinstance(crawl_trace, dict) else {}

    def _record_source_kind(self, record: ContentRecord) -> str:
        crawl_trace = self._crawl_trace(record)
        source_kind = str(crawl_trace.get("source_kind", "") or "").strip().lower()
        if source_kind in {"page", "document"}:
            return source_kind
        source_type = str(record.source_type or "").strip().lower()
        if source_type == "html":
            return "page"
        return "document"

    def _record_route_family(self, record: ContentRecord) -> str:
        crawl_trace = self._crawl_trace(record)
        route_family = str(crawl_trace.get("route_family", "") or "").strip()
        if route_family:
            return route_family
        parser_trace = record.trace.get("factory_site_parser", {})
        if isinstance(parser_trace, dict):
            route_family = str(parser_trace.get("route_family", "") or "").strip()
            if route_family:
                return route_family
        taxonomy = record.trace.get("page_signal_taxonomy", {})
        if isinstance(taxonomy, dict):
            return str(taxonomy.get("route_family", "") or "").strip()
        return ""

    def _record_fingerprint(self, record: ContentRecord) -> str:
        return record.content_fingerprint or f"{record.url}|{record.fetch_status}|{record.source_type}"

    def _dedupe_records(self, records: list[ContentRecord]) -> list[ContentRecord]:
        result: list[ContentRecord] = []
        seen: set[str] = set()
        for record in records:
            key = self._record_fingerprint(record)
            if key in seen:
                continue
            seen.add(key)
            result.append(record)
        return result


__all__ = ["FactorySiteStore"]

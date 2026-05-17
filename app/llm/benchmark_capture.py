from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from app.site_intelligence.site_authenticity import SiteAuthenticityAnalyzer, should_use_llm_review


SUPPORTED_LLM_BENCHMARK_STAGES = ("site_decision", "content_review")
_SUPPORTED_LLM_BENCHMARK_STAGE_SET = frozenset(SUPPORTED_LLM_BENCHMARK_STAGES)
CONTENT_REVIEW_BENCHMARK_RECORD_LIMIT = 3


def _normalize_text(value: Any) -> str:
    return " ".join(str(value or "").split())


def _jsonable(payload: Any) -> Any:
    return json.loads(json.dumps(payload, ensure_ascii=False))


def _apply_capture_metadata(
    payload: dict[str, Any],
    *,
    benchmark_capture_path: str,
    synthetic_candidate_used: bool,
    forced_harvest_level: str,
) -> None:
    payload["benchmark_capture_path"] = str(benchmark_capture_path or "")
    payload["synthetic_candidate_used"] = bool(synthetic_candidate_used)
    payload["forced_harvest_level"] = str(forced_harvest_level or "none")


def _record_trust_state(record: Any) -> str:
    trace = getattr(record, "trace", {})
    if not isinstance(trace, dict):
        return ""
    parser_trace = trace.get("factory_site_parser")
    if not isinstance(parser_trace, dict):
        return ""
    crawl_trace = parser_trace.get("crawl")
    if not isinstance(crawl_trace, dict):
        return ""
    return _normalize_text(crawl_trace.get("trust_state", "")).lower()


def _record_identity_key(record: Any) -> str:
    fingerprint = _normalize_text(getattr(record, "content_fingerprint", ""))
    if fingerprint:
        return fingerprint
    url = _normalize_text(getattr(record, "url", ""))
    if url:
        return url
    return _normalize_text(getattr(record, "source_url_or_file", ""))


def parse_llm_benchmark_force_stages(raw_value: str) -> frozenset[str]:
    normalized = _normalize_text(raw_value)
    if not normalized:
        return frozenset()
    stages: list[str] = []
    for raw_stage in normalized.split(","):
        stage = raw_stage.strip()
        if not stage:
            continue
        if stage not in _SUPPORTED_LLM_BENCHMARK_STAGE_SET:
            supported = ", ".join(SUPPORTED_LLM_BENCHMARK_STAGES)
            raise ValueError(f"--llm-benchmark-force-stages supports only: {supported}")
        if stage not in stages:
            stages.append(stage)
    return frozenset(stages)


def describe_site_decision_prod_skip_reason(
    *,
    decision_status: str,
    authenticity_score: float,
    hard_negative_hits: list[str],
    identity_flags: dict[str, Any],
) -> str:
    if decision_status not in {"candidate", "suspicious"}:
        return f"decision_status_{decision_status or 'unknown'}"
    if authenticity_score >= 0.82:
        return "high_authenticity_score"
    if hard_negative_hits and not identity_flags.get("inn_match") and not identity_flags.get("domain_matches_email"):
        return "hard_negative_without_identity_anchor"
    if identity_flags.get("inn_match") and authenticity_score >= 0.62:
        return "inn_match_high_authenticity"
    return "llm_gate_not_selected"


def describe_content_review_prod_skip_reason(record: Any, *, default_reason: str = "") -> str:
    trust_state = _record_trust_state(record)
    if trust_state and trust_state != "trusted":
        return "site_not_trusted"
    if default_reason:
        return default_reason
    fetch_status = _normalize_text(getattr(record, "fetch_status", ""))
    if fetch_status != "success":
        return f"fetch_status_{fetch_status or 'unknown'}"
    relevance_label = _normalize_text(getattr(record, "relevance_label", ""))
    if relevance_label != "maybe_relevant":
        return f"heuristic_relevance_label_{relevance_label or 'unknown'}"
    if not _normalize_text(getattr(record, "cleaned_text", "")):
        return "empty_cleaned_text"
    return "llm_gate_not_selected"


def describe_content_review_harvest_prod_skip_reason(*, had_deep_parse_sites: bool) -> str:
    if not had_deep_parse_sites:
        return "site_not_trusted"
    return "deep_parse_no_content_record"


def select_content_review_benchmark_records(
    records: list[Any],
    *,
    limit: int = CONTENT_REVIEW_BENCHMARK_RECORD_LIMIT,
) -> list[Any]:
    deduped_records: list[Any] = []
    seen_keys: set[str] = set()
    for record in records or []:
        key = _record_identity_key(record)
        if key and key in seen_keys:
            continue
        if key:
            seen_keys.add(key)
        deduped_records.append(record)

    relevance_rank = {
        "maybe_relevant": 0,
        "likely_relevant": 1,
        "unknown": 2,
        "irrelevant": 3,
    }
    ranked_records = sorted(
        deduped_records,
        key=lambda record: (
            0 if _normalize_text(getattr(record, "fetch_status", "")) == "success" else 1,
            0 if _normalize_text(getattr(record, "cleaned_text", "")) else 1,
            relevance_rank.get(_normalize_text(getattr(record, "relevance_label", "")), 4),
            -float(getattr(record, "relevance_score", 0.0) or 0.0),
            _record_identity_key(record),
        ),
    )
    return ranked_records[:max(0, int(limit))]


@dataclass(frozen=True)
class LLMBenchmarkCaptureConfig:
    capture_dir: Path
    source_run_selection: dict[str, Any]
    force_stages: frozenset[str] = field(default_factory=frozenset)
    capture_only: bool = False

    def captures_stage(self, stage: str) -> bool:
        return stage in _SUPPORTED_LLM_BENCHMARK_STAGE_SET

    def forces_stage(self, stage: str) -> bool:
        return stage in self.force_stages


class LLMBenchmarkCaptureWriter:
    def __init__(self, config: LLMBenchmarkCaptureConfig) -> None:
        self.config = config

    @property
    def capture_only(self) -> bool:
        return self.config.capture_only

    def captures_stage(self, stage: str) -> bool:
        return self.config.captures_stage(stage)

    def forces_stage(self, stage: str) -> bool:
        return self.config.forces_stage(stage)

    def fixture_path(self, stage: str) -> Path:
        return self.config.capture_dir / f"{stage}_fixtures.jsonl"

    def blocker_path(self, stage: str) -> Path:
        return self.config.capture_dir / f"{stage}_blockers.jsonl"

    def append_fixture(
        self,
        *,
        stage: str,
        row: Any,
        url: str,
        site_url: str,
        request_body_template: dict[str, Any],
        would_call_in_prod: bool,
        prod_skip_reason: str,
        trust_state: str,
        decision_source_context: dict[str, Any],
        compact_context: dict[str, Any] | None = None,
        benchmark_forced_harvest: bool = False,
        benchmark_capture_path: str = "",
        synthetic_candidate_used: bool = False,
        forced_harvest_level: str = "none",
        benchmark_synthetic_candidate: bool = False,
    ) -> dict[str, Any]:
        if not self.captures_stage(stage):
            raise ValueError(f"Unsupported benchmark capture stage: {stage}")
        ordinal = int(getattr(row, "row_index", 0) or 0)
        fixture: dict[str, Any] = {
            "stage": stage,
            "replayable": True,
            "ordinal": ordinal,
            "row_index": ordinal,
            "inn": str(getattr(row, "inn", "") or ""),
            "company_name": str(getattr(row, "company_name", "") or ""),
            "url": str(url or ""),
            "site_url": str(site_url or url or ""),
            "request_body_template": _jsonable(request_body_template),
            "would_call_in_prod": bool(would_call_in_prod),
            "prod_skip_reason": str(prod_skip_reason or ""),
            "trust_state": str(trust_state or ""),
            "decision_source_context": _jsonable(decision_source_context),
            "source_run_selection": _jsonable(self.config.source_run_selection),
        }
        _apply_capture_metadata(
            fixture,
            benchmark_capture_path=benchmark_capture_path,
            synthetic_candidate_used=synthetic_candidate_used,
            forced_harvest_level=forced_harvest_level,
        )
        if compact_context is not None:
            fixture["compact_context"] = _jsonable(compact_context)
        if benchmark_forced_harvest:
            fixture["benchmark_forced_harvest"] = True
        if benchmark_synthetic_candidate:
            fixture["benchmark_synthetic_candidate"] = True
        fixture_hash = hashlib.sha1(
            json.dumps(fixture, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")
        ).hexdigest()
        fixture["fixture_hash"] = fixture_hash
        fixture_path = self.fixture_path(stage)
        fixture_path.parent.mkdir(parents=True, exist_ok=True)
        with fixture_path.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(fixture, ensure_ascii=False))
            handle.write("\n")
        return fixture

    def append_blocker(
        self,
        *,
        stage: str,
        row: Any,
        blocker_reason: str,
        would_call_in_prod: bool,
        site_url: str = "",
        benchmark_capture_path: str = "",
        synthetic_candidate_used: bool = False,
        forced_harvest_level: str = "none",
    ) -> dict[str, Any]:
        if not self.captures_stage(stage):
            raise ValueError(f"Unsupported benchmark capture stage: {stage}")
        ordinal = int(getattr(row, "row_index", 0) or 0)
        blocker = {
            "stage": stage,
            "replayable": False,
            "ordinal": ordinal,
            "row_index": ordinal,
            "inn": str(getattr(row, "inn", "") or ""),
            "company_name": str(getattr(row, "company_name", "") or ""),
            "site_url": str(site_url or ""),
            "blocker_reason": str(blocker_reason or ""),
            "would_call_in_prod": bool(would_call_in_prod),
            "source_run_selection": _jsonable(self.config.source_run_selection),
        }
        _apply_capture_metadata(
            blocker,
            benchmark_capture_path=benchmark_capture_path,
            synthetic_candidate_used=synthetic_candidate_used,
            forced_harvest_level=forced_harvest_level,
        )
        blocker_path = self.blocker_path(stage)
        blocker_path.parent.mkdir(parents=True, exist_ok=True)
        with blocker_path.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(blocker, ensure_ascii=False))
            handle.write("\n")
        return blocker


class BenchmarkAwareSiteAuthenticityAnalyzer(SiteAuthenticityAnalyzer):
    def _site_decision_source_context(
        self,
        *,
        decision: Any,
        identity_flags: dict[str, Any],
        capture_origin: str,
    ) -> dict[str, Any]:
        return {
            "decision_status": decision.decision_status,
            "decision_source": decision.decision_source,
            "authenticity_score": round(decision.authenticity_score, 3),
            "identity_score": round(decision.identity_score, 3),
            "viability_score": round(decision.viability_score, 3),
            "industrial_score": round(decision.industrial_score, 3),
            "hard_negative_hits": list(decision.hard_negative_hits[:5]),
            "identity_flags": dict(identity_flags),
            "capture_origin": capture_origin,
        }

    def _maybe_capture_site_decision_fixture(
        self,
        *,
        row: Any,
        site_url: str,
        decision: Any,
        source_results: dict[str, Any],
        known_contacts: dict[str, list[str]],
        combined_text: str,
        identity: dict[str, Any],
        industrial: dict[str, Any],
        identity_flags: dict[str, Any],
        capture_origin: str,
        capture_when_prod_would_call: bool,
    ) -> None:
        should_force_stage = getattr(self.llm, "should_force_benchmark_stage", None)
        if not callable(should_force_stage) or not should_force_stage("site_decision"):
            return

        would_call_in_prod = should_use_llm_review(
            decision.decision_status,
            decision.authenticity_score,
            decision.hard_negative_hits,
            identity_flags,
        )
        if would_call_in_prod and not capture_when_prod_would_call:
            return

        llm_context = self._build_llm_context(
            row=row,
            decision=decision,
            source_results=source_results,
            known_contacts=known_contacts,
            combined_text=combined_text,
            identity=identity,
            industrial=industrial,
        )
        capture_site_fixture = getattr(self.llm, "capture_site_decision_fixture", None)
        prod_skip_reason = (
            ""
            if would_call_in_prod
            else describe_site_decision_prod_skip_reason(
                decision_status=decision.decision_status,
                authenticity_score=decision.authenticity_score,
                hard_negative_hits=decision.hard_negative_hits,
                identity_flags=identity_flags,
            )
        )
        decision_source_context = self._site_decision_source_context(
            decision=decision,
            identity_flags=identity_flags,
            capture_origin=capture_origin,
        )
        if callable(capture_site_fixture):
            capture_site_fixture(
                row=row,
                site_url=decision.final_url or self.h.normalize_url(site_url),
                compressed_context=llm_context,
                trust_state=decision.decision_status,
                would_call_in_prod=would_call_in_prod,
                prod_skip_reason=prod_skip_reason,
                decision_source_context=decision_source_context,
            )
            return

        capture_forced_site_fixture = getattr(self.llm, "capture_forced_site_decision_fixture", None)
        if not would_call_in_prod and callable(capture_forced_site_fixture):
            capture_forced_site_fixture(
                row=row,
                site_url=decision.final_url or self.h.normalize_url(site_url),
                compressed_context=llm_context,
                trust_state=decision.decision_status,
                prod_skip_reason=prod_skip_reason,
                decision_source_context=decision_source_context,
            )

    def analyze_surface(
        self,
        row: Any,
        site_url: str,
        known_contacts: dict[str, list[str]],
        source_results: dict[str, Any],
    ):
        decision, combined_text, identity, industrial = self._evaluate_site(
            row,
            site_url,
            known_contacts,
            source_results,
            allow_extra_pages=False,
        )
        decision.decision_source = "cheap_preparse_gate"
        decision.belongs_to_company = False

        if decision.status != "success":
            decision.decision_status = "rejected" if decision.status == "invalid_url" else "suspicious"
            gate_reason = (
                "cheap trust gate rejected candidate before deep parse"
                if decision.decision_status == "rejected"
                else "cheap trust gate kept candidate surface-only before deep parse"
            )
            if gate_reason not in decision.reasons:
                decision.reasons.append(gate_reason)
            return decision

        identity_flags = identity.get("flags") or {}
        decision.decision_status = self._derive_preparse_decision_status(
            decision.authenticity_score,
            decision.identity_score,
            decision.viability_score,
            identity_flags,
            decision.hard_negative_hits,
            decision.extracted_phones,
            decision.extracted_emails,
        )
        gate_reason = {
            "candidate": "cheap trust gate allowed deep parse",
            "suspicious": "cheap trust gate kept candidate surface-only before deep parse",
            "rejected": "cheap trust gate rejected candidate before deep parse",
        }.get(decision.decision_status, "cheap trust gate applied")
        if gate_reason not in decision.reasons:
            decision.reasons.append(gate_reason)

        self._maybe_capture_site_decision_fixture(
            row=row,
            site_url=site_url,
            decision=decision,
            source_results=source_results,
            known_contacts=known_contacts,
            combined_text=combined_text,
            identity=identity,
            industrial=industrial,
            identity_flags=identity_flags,
            capture_origin="preparse_surface",
            capture_when_prod_would_call=True,
        )
        return decision

    def analyze(
        self,
        row: Any,
        site_url: str,
        known_contacts: dict[str, list[str]],
        source_results: dict[str, Any],
    ):
        decision, combined_text, identity, industrial = self._evaluate_site(
            row,
            site_url,
            known_contacts,
            source_results,
            allow_extra_pages=True,
        )
        if decision.status != "success":
            return decision

        identity_flags = identity.get("flags") or {}
        decision.decision_status = self._derive_site_decision_status(
            decision.authenticity_score,
            decision.identity_score,
            identity_flags,
            decision.hard_negative_hits,
        )
        decision.belongs_to_company = decision.decision_status == "verified"

        if should_use_llm_review(
            decision.decision_status,
            decision.authenticity_score,
            decision.hard_negative_hits,
            identity_flags,
        ):
            llm_context = self._build_llm_context(
                row=row,
                decision=decision,
                source_results=source_results,
                known_contacts=known_contacts,
                combined_text=combined_text,
                identity=identity,
                industrial=industrial,
            )
            llm_result = self.llm.decide(
                row,
                decision.final_url or self.h.normalize_url(site_url),
                llm_context,
                trust_state=decision.decision_status,
                decision_source_context={
                    "decision_status": decision.decision_status,
                    "decision_source": decision.decision_source,
                    "authenticity_score": round(decision.authenticity_score, 3),
                    "identity_score": round(decision.identity_score, 3),
                    "viability_score": round(decision.viability_score, 3),
                    "industrial_score": round(decision.industrial_score, 3),
                    "hard_negative_hits": list(decision.hard_negative_hits[:5]),
                    "identity_flags": dict(identity_flags),
                },
            )
            if llm_result:
                decision.llm_result = llm_result
                decision.decision_source = "llm_assisted"
                llm_confidence = float(llm_result.get("confidence", 0.0) or 0.0)
                if llm_confidence >= 0.55:
                    llm_belongs = bool(llm_result.get("belongs_to_company", decision.belongs_to_company))
                    if llm_belongs:
                        if decision.decision_status == "suspicious":
                            decision.decision_status = "candidate"
                        if decision.authenticity_score >= 0.62:
                            decision.decision_status = "verified"
                    else:
                        decision.decision_status = "suspicious" if decision.authenticity_score >= 0.5 else "rejected"
                    decision.belongs_to_company = decision.decision_status == "verified"
                    decision.industrial_relevance = llm_result.get("industrial_relevance", decision.industrial_relevance)
                reason = self.h.normalize_whitespace(str(llm_result.get("reason", "")))
                if reason:
                    decision.reasons.append(f"LLM: {reason}")
            return decision

        self._maybe_capture_site_decision_fixture(
            row=row,
            site_url=site_url,
            decision=decision,
            source_results=source_results,
            known_contacts=known_contacts,
            combined_text=combined_text,
            identity=identity,
            industrial=industrial,
            identity_flags=identity_flags,
            capture_origin="deep_parse",
            capture_when_prod_would_call=False,
        )

        return decision

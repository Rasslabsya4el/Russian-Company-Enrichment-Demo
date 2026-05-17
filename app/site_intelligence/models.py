from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from app.documents.content import NormalizedContentRecord


def normalize_worth_crawling(value: object) -> str:
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, (int, float)):
        return "true" if value else "false"
    normalized = str(value or "").strip().lower()
    if normalized in {"true", "limited", "false"}:
        return normalized
    if normalized in {"1", "yes", "y", "on"}:
        return "true"
    if normalized in {"0", "no", "n", "off", ""}:
        return "false"
    return "limited"


@dataclass
class SiteProbe:
    url: str
    final_url: str = ""
    status: str = ""
    http_status: int | None = None
    content_type: str = ""
    encoding: str = ""
    site_class: str = "F"
    worth_crawling: str = "false"
    browser_required_default: bool = False
    anti_bot_detected: bool = False
    block_class: str = ""
    anti_bot_reason: str = ""
    challenge_detected: bool = False
    html_ok: bool = False
    robots_found: bool = False
    sitemap_found: bool = False
    internal_links_count: int = 0
    document_links_count: int = 0
    text_length: int = 0
    redirect_count: int = 0
    key_sections: list[str] = field(default_factory=list)
    sampled_urls: list[str] = field(default_factory=list)
    obvious_routes_attempted: list[str] = field(default_factory=list)
    cms_guess: str = ""
    failure_reason: str = ""
    timeout_reason: str = ""
    notes: list[str] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)
    transport_selected: str = ""
    transport_final: str = ""
    blocked_by_policy: bool = False
    escalation_reason: str = ""
    normalized_symptoms: list[str] = field(default_factory=list)
    policy_hints: dict[str, Any] = field(default_factory=dict)

    def normalized_worth_crawling(self) -> str:
        return normalize_worth_crawling(self.worth_crawling)

    def is_worth_crawling(self) -> bool:
        return self.normalized_worth_crawling() != "false"


@dataclass
class RouteStrategy:
    site_url: str
    route_pattern: str
    section_guess: str
    mode: str
    confidence: float
    route_family: str = ""
    priority: int = 0
    crawl_budget: int = 1
    queue_name: str = ""
    accounting_key: str = ""
    mandatory: bool = False
    counts_toward_coverage: bool = False
    skip_reason: str = ""
    max_depth: int | None = None
    host_cap: str = ""
    path_pattern_cap: str = ""
    reasons: list[str] = field(default_factory=list)
    discovery_sources: list[str] = field(default_factory=list)

    def effective_queue_name(self) -> str:
        if self.queue_name:
            return self.queue_name
        if self.mode == "playwright":
            return "browser"
        if self.mode == "hybrid":
            return "hybrid"
        if self.mode == "requests":
            return "http"
        return "skip"

    def effective_accounting_key(self) -> str:
        return self.accounting_key or self.route_family or "unclassified"

    def effective_skip_reason(self) -> str:
        if self.skip_reason:
            return self.skip_reason
        if self.mode == "skip" and self.reasons:
            return self.reasons[0]
        return ""

ContentRecord = NormalizedContentRecord

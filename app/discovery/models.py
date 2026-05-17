from __future__ import annotations

from dataclasses import dataclass, field


@dataclass
class DomainCandidate:
    url: str
    domain: str
    source: str
    confidence: float
    status: str
    evidence: list[str] = field(default_factory=list)


@dataclass
class DomainResolution:
    inn: str
    company_name: str
    status: str = "not_found"
    selected_primary_domain: str = ""
    selected_primary_status: str = ""
    candidates: list[DomainCandidate] = field(default_factory=list)
    notes: list[str] = field(default_factory=list)

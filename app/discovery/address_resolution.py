from __future__ import annotations

from collections.abc import Callable, Iterable
from dataclasses import dataclass

from .geo_lookup import LookupMatchStatus, lookup_settlement


AddressSanitizer = Callable[[str], str]

_LOOKUP_STATUS_RANK = {
    "matched": 3,
    "ambiguous": 2,
    "not_found": 1,
    "lookup_unavailable": 1,
    "no_address": 0,
}


@dataclass(frozen=True)
class AddressEnrichment:
    raw_value: str
    sanitized_value: str
    lookup_status: LookupMatchStatus
    matched_settlement: str = ""
    matched_municipality: str = ""
    matched_region: str = ""
    candidate_count: int = 0

    @property
    def dedupe_key(self) -> str:
        return self.sanitized_value.lower()

    @property
    def lookup_status_rank(self) -> int:
        return _LOOKUP_STATUS_RANK.get(self.lookup_status, 0)

    @property
    def locality_detail_rank(self) -> tuple[int, int, int, int]:
        return (
            int(bool(self.matched_settlement)),
            int(bool(self.matched_municipality)),
            int(bool(self.matched_region)),
            -max(int(self.candidate_count or 0) - 1, 0),
        )


def enrich_address_candidate(raw_value: str | None, *, sanitizer: AddressSanitizer) -> AddressEnrichment:
    raw_text = "" if raw_value is None else str(raw_value).strip()
    sanitized_value = sanitizer(raw_text)
    lookup_result = lookup_settlement(sanitized_value)
    return AddressEnrichment(
        raw_value=raw_text,
        sanitized_value=sanitized_value,
        lookup_status=lookup_result.match_status,
        matched_settlement=lookup_result.matched_settlement,
        matched_municipality=lookup_result.matched_municipality,
        matched_region=lookup_result.matched_region,
        candidate_count=int(lookup_result.candidate_count or 0),
    )


def enrich_address_candidates(
    values: Iterable[str | None],
    *,
    sanitizer: AddressSanitizer,
) -> list[AddressEnrichment]:
    enriched_candidates: list[AddressEnrichment] = []
    seen: set[str] = set()
    for raw_value in values:
        candidate = enrich_address_candidate(raw_value, sanitizer=sanitizer)
        if not candidate.sanitized_value:
            continue
        if candidate.dedupe_key in seen:
            continue
        seen.add(candidate.dedupe_key)
        enriched_candidates.append(candidate)
    return enriched_candidates


__all__ = [
    "AddressEnrichment",
    "enrich_address_candidate",
    "enrich_address_candidates",
]

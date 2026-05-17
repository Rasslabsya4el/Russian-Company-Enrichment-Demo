from __future__ import annotations

import json
import os
import re
from collections import Counter, defaultdict
from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field
from functools import lru_cache
from pathlib import Path
from statistics import median
from typing import Any, Literal

from .geo_zone import (
    DEFAULT_MOSCOW_GEO_ZONE,
    GEO_BUCKET_CORE,
    GEO_BUCKET_OUTER_BAND,
    GEO_BUCKET_OUTSIDE,
    GeoBucket,
    classify,
)


LOOKUP_MATCH_MATCHED = "matched"
LOOKUP_MATCH_AMBIGUOUS = "ambiguous"
LOOKUP_MATCH_NOT_FOUND = "not_found"
LOOKUP_MATCH_NO_ADDRESS = "no_address"
LOOKUP_MATCH_UNAVAILABLE = "lookup_unavailable"

LookupMatchStatus = Literal[
    "matched",
    "ambiguous",
    "not_found",
    "no_address",
    "lookup_unavailable",
]

GEO_LOOKUP_ASSET_SCHEMA_VERSION = "moscow_geo_lookup.v2"
GEO_LOOKUP_SOURCE_CONTRACT = "moscow_geo_zone_settlements.csv"
GEO_LOOKUP_ASSET_ENV_VAR = "MOSCOW_GEO_LOOKUP_ASSET_PATH"
DEFAULT_GEO_LOOKUP_ASSET_PATH = Path(__file__).resolve().parent / "data" / "moscow_geo_lookup.json"
DEFAULT_GEO_LOOKUP_OVERRIDE_PATH = Path(__file__).resolve().parent / "data" / "moscow_geo_lookup_overrides.json"
GEO_LOOKUP_OVERRIDE_SCHEMA_VERSION = "moscow_geo_lookup_overrides.v1"
_SUPPORTED_GEO_LOOKUP_ASSET_SCHEMA_VERSIONS = frozenset({"moscow_geo_lookup.v1", GEO_LOOKUP_ASSET_SCHEMA_VERSION})

_NON_ALNUM_RE = re.compile(r"[^0-9a-zа-я]+", flags=re.IGNORECASE)
_MULTISPACE_RE = re.compile(r"\s+")
_CYRILLIC_OR_LATIN_RE = re.compile(r"[a-zа-я]", flags=re.IGNORECASE)
_PRE_TOKEN_REPLACEMENTS: tuple[tuple[re.Pattern[str], str], ...] = (
    (re.compile(r"\bр\s*[-.]?\s*н\b", flags=re.IGNORECASE), " район "),
    (re.compile(r"\bг\.\s*(?=[a-zа-я])", flags=re.IGNORECASE), " город "),
    (re.compile(r"\bгор\.\s*(?=[a-zа-я])", flags=re.IGNORECASE), " город "),
    (re.compile(r"\bобл\.\b", flags=re.IGNORECASE), " область "),
    (re.compile(r"\bпос\.\s*(?=[a-zа-я])", flags=re.IGNORECASE), " поселок "),
    (re.compile(r"\bс\.\s*(?=[a-zа-я])", flags=re.IGNORECASE), " село "),
    (re.compile(r"\bд\.\s*(?=[a-zа-я])", flags=re.IGNORECASE), " деревня "),
    (re.compile(r"\bдер\.\s*(?=[a-zа-я])", flags=re.IGNORECASE), " деревня "),
)
_GENERIC_TOKENS = frozenset(
    {
        "город",
        "область",
        "район",
        "поселок",
        "село",
        "деревня",
    }
)
_VALID_GEO_BUCKETS = frozenset({GEO_BUCKET_CORE, GEO_BUCKET_OUTER_BAND, GEO_BUCKET_OUTSIDE})


@dataclass(frozen=True)
class GeoLookupRecord:
    settlement: str
    municipality: str
    region: str
    settlement_type: str
    full_name: str
    aliases: tuple[str, ...]
    geo_bucket: GeoBucket
    geo_weight: int
    inside_outer_polygon: bool
    inside_inner_polygon: bool
    distance_to_moscow_km: float
    region_normalized: str
    municipality_normalized: str
    variant_count: int = 1
    distance_spread_km: float = 0.0
    source_priority: int = 0


@dataclass(frozen=True)
class GeoLookupAmbiguousRecord:
    settlement: str
    municipality: str
    region: str
    settlement_type: str
    full_name: str
    aliases: tuple[str, ...]
    geo_buckets: tuple[GeoBucket, ...]
    variant_count: int
    distance_spread_km: float
    region_normalized: str
    municipality_normalized: str


@dataclass(frozen=True)
class GeoLookupResult:
    match_status: LookupMatchStatus
    source_address: str
    matched_settlement: str = ""
    matched_municipality: str = ""
    matched_region: str = ""
    geo_bucket: GeoBucket | None = None
    geo_weight: int | None = None
    inside_outer_polygon: bool | None = None
    inside_inner_polygon: bool | None = None
    distance_to_moscow_km: float | None = None
    candidate_count: int = 0
    variant_count: int = 0
    distance_spread_km: float | None = None
    ambiguous_geo_buckets: tuple[GeoBucket, ...] = ()


LookupCandidate = GeoLookupRecord | GeoLookupAmbiguousRecord


_FEDERAL_CITY_ADMIN_PATTERNS: tuple[re.Pattern[str], ...] = (
    re.compile(
        "\\b\\u0432\\u043d\\s*[-.]?\\s*\\u0442\\u0435\\u0440\\s*[-.]?\\s*\\u0433\\b",
        flags=re.IGNORECASE,
    ),
    re.compile(
        "\\b\\u0432\\u043d\\u0443\\u0442\\u0440\\u0438\\u0433\\u043e\\u0440\\u043e\\u0434\\u0441\\u043a\\w*\\s+"
        "\\u0442\\u0435\\u0440\\u0440\\u0438\\u0442\\u043e\\u0440\\w*(?:\\s+\\u0433\\u043e\\u0440\\u043e\\u0434\\w*)?\\b",
        flags=re.IGNORECASE,
    ),
    re.compile(
        "\\b\\u043c\\u0443\\u043d\\u0438\\u0446\\u0438\\u043f\\u0430\\u043b\\u044c\\u043d\\w*\\s+\\u043e\\u043a\\u0440\\u0443\\u0433\\b",
        flags=re.IGNORECASE,
    ),
)
_LookupScore = tuple[int, int, int, int, int, int]


@dataclass(frozen=True)
class _ScoredLookupMatch:
    score: _LookupScore
    record: LookupCandidate
    matched_aliases: tuple[str, ...]


class GeoLookupIndex:
    def __init__(
        self,
        records: Iterable[GeoLookupRecord],
        ambiguous_records: Iterable[GeoLookupAmbiguousRecord] = (),
        *,
        schema_version: str = GEO_LOOKUP_ASSET_SCHEMA_VERSION,
        asset_path: str = "",
    ) -> None:
        self.records = tuple(records)
        self.ambiguous_records = tuple(ambiguous_records)
        self.schema_version = schema_version
        self.asset_path = asset_path
        self._candidates: tuple[LookupCandidate, ...] = self.records + self.ambiguous_records
        token_index: dict[str, set[int]] = {}
        for record_id, record in enumerate(self._candidates):
            for alias in record.aliases:
                for token in alias.split():
                    if token in _GENERIC_TOKENS:
                        continue
                    token_index.setdefault(token, set()).add(record_id)
        self._token_index = {token: tuple(sorted(ids)) for token, ids in token_index.items()}

    def lookup(self, address_text: str | None) -> GeoLookupResult:
        source_address = clean_source_address(address_text)
        if not source_address:
            return GeoLookupResult(
                match_status=LOOKUP_MATCH_NO_ADDRESS,
                source_address=source_address,
            )

        normalized_address = normalize_lookup_text(source_address)
        candidate_ids = self._candidate_ids(normalized_address)
        if not candidate_ids:
            return GeoLookupResult(
                match_status=LOOKUP_MATCH_NOT_FOUND,
                source_address=source_address,
            )

        padded_address = _pad_phrase(normalized_address)
        ranked_matches: list[_ScoredLookupMatch] = []
        for candidate_id in candidate_ids:
            record = self._candidates[candidate_id]
            matched_aliases = [alias for alias in record.aliases if _contains_phrase(padded_address, alias)]
            if not matched_aliases:
                continue
            best_alias = max(matched_aliases, key=_alias_rank)
            municipality_hit = int(bool(record.municipality_normalized) and _contains_phrase(padded_address, record.municipality_normalized))
            region_hit = int(bool(record.region_normalized) and _contains_phrase(padded_address, record.region_normalized))
            alias_tokens, alias_length = _alias_rank(best_alias)
            ranked_matches.append(
                _ScoredLookupMatch(
                    score=(
                        municipality_hit + region_hit,
                        municipality_hit,
                        region_hit,
                        getattr(record, "source_priority", 0),
                        alias_tokens,
                        alias_length,
                    ),
                    record=record,
                    matched_aliases=tuple(matched_aliases),
                )
            )

        if not ranked_matches:
            return GeoLookupResult(
                match_status=LOOKUP_MATCH_NOT_FOUND,
                source_address=source_address,
            )

        best_score = max(match.score for match in ranked_matches)
        best_records = _best_lookup_records(ranked_matches, best_score=best_score)

        if len(best_records) != 1:
            resolved_result = _resolve_federal_city_match(
                ranked_matches,
                best_records=best_records,
                source_address=source_address,
            )
            if resolved_result is not None:
                return resolved_result
            return GeoLookupResult(
                match_status=LOOKUP_MATCH_AMBIGUOUS,
                source_address=source_address,
                candidate_count=len(best_records),
            )

        return _geo_lookup_result_from_candidate(best_records[0], source_address=source_address)

    def _candidate_ids(self, normalized_address: str) -> tuple[int, ...]:
        candidate_ids: set[int] = set()
        for token in set(normalized_address.split()):
            candidate_ids.update(self._token_index.get(token, ()))
        return tuple(sorted(candidate_ids))


def _best_lookup_records(ranked_matches: Iterable[_ScoredLookupMatch], *, best_score: _LookupScore) -> list[LookupCandidate]:
    best_records: list[LookupCandidate] = []
    seen_keys: set[tuple[str, str, str, str]] = set()
    for match in ranked_matches:
        if match.score != best_score:
            continue
        record = match.record
        record_key = _lookup_record_key(record)
        if record_key in seen_keys:
            continue
        seen_keys.add(record_key)
        best_records.append(record)
    return best_records


def _geo_lookup_result_from_candidate(candidate: LookupCandidate, *, source_address: str) -> GeoLookupResult:
    if isinstance(candidate, GeoLookupAmbiguousRecord):
        return GeoLookupResult(
            match_status=LOOKUP_MATCH_AMBIGUOUS,
            source_address=source_address,
            matched_settlement=candidate.settlement,
            matched_municipality=candidate.municipality,
            matched_region=candidate.region,
            candidate_count=candidate.variant_count,
            variant_count=candidate.variant_count,
            distance_spread_km=candidate.distance_spread_km,
            ambiguous_geo_buckets=candidate.geo_buckets,
        )
    return GeoLookupResult(
        match_status=LOOKUP_MATCH_MATCHED,
        source_address=source_address,
        matched_settlement=candidate.settlement,
        matched_municipality=candidate.municipality,
        matched_region=candidate.region,
        geo_bucket=candidate.geo_bucket,
        geo_weight=candidate.geo_weight,
        inside_outer_polygon=candidate.inside_outer_polygon,
        inside_inner_polygon=candidate.inside_inner_polygon,
        distance_to_moscow_km=candidate.distance_to_moscow_km,
        candidate_count=1,
        variant_count=candidate.variant_count,
        distance_spread_km=candidate.distance_spread_km,
    )


def _resolve_federal_city_match(
    ranked_matches: Iterable[_ScoredLookupMatch],
    *,
    best_records: list[LookupCandidate],
    source_address: str,
) -> GeoLookupResult | None:
    federal_city_records = _federal_city_records(best_records)
    if not federal_city_records:
        return None
    locality_record = _find_federal_city_locality_match(
        ranked_matches,
        federal_city_records=federal_city_records,
    )
    if locality_record is not None:
        return _geo_lookup_result_from_candidate(locality_record, source_address=source_address)
    if not _can_collapse_federal_city_records(federal_city_records):
        return None
    return _collapsed_federal_city_result(source_address=source_address, federal_city_records=federal_city_records)


def _federal_city_records(records: list[LookupCandidate]) -> tuple[GeoLookupRecord, ...]:
    if len(records) < 2 or any(not isinstance(record, GeoLookupRecord) for record in records):
        return ()
    geo_records = tuple(record for record in records if isinstance(record, GeoLookupRecord))
    region_normalized = geo_records[0].region_normalized
    settlement_normalized = normalize_lookup_text(geo_records[0].settlement)
    if not region_normalized or settlement_normalized != region_normalized:
        return ()
    if len({record.municipality_normalized for record in geo_records if record.municipality_normalized}) < 2:
        return ()
    if any(
        not _is_federal_city_record(record)
        or record.region_normalized != region_normalized
        or normalize_lookup_text(record.settlement) != settlement_normalized
        for record in geo_records
    ):
        return ()
    return geo_records


def _find_federal_city_locality_match(
    ranked_matches: Iterable[_ScoredLookupMatch],
    *,
    federal_city_records: tuple[GeoLookupRecord, ...],
) -> GeoLookupRecord | None:
    region_normalized = federal_city_records[0].region_normalized
    federal_city_aliases = {alias for record in federal_city_records for alias in record.aliases}
    locality_matches: list[tuple[tuple[int, int, int, int, int, int, int, int], GeoLookupRecord]] = []
    for match in ranked_matches:
        record = match.record
        if not isinstance(record, GeoLookupRecord):
            continue
        if record.region_normalized != region_normalized or _is_federal_city_record(record):
            continue
        locality_aliases = tuple(alias for alias in match.matched_aliases if alias not in federal_city_aliases)
        if not locality_aliases:
            continue
        alias_tokens, alias_length = max((_alias_rank(alias) for alias in locality_aliases), default=(0, 0))
        locality_matches.append(((*match.score, alias_tokens, alias_length), record))

    if not locality_matches:
        return None

    best_score = max(score for score, _ in locality_matches)
    best_records: list[GeoLookupRecord] = []
    seen_keys: set[tuple[str, str, str, str]] = set()
    for score, record in locality_matches:
        if score != best_score:
            continue
        record_key = _lookup_record_key(record)
        if record_key in seen_keys:
            continue
        seen_keys.add(record_key)
        best_records.append(record)
    return best_records[0] if len(best_records) == 1 else None


def _is_federal_city_record(record: GeoLookupRecord) -> bool:
    return bool(record.municipality_normalized) and normalize_lookup_text(record.settlement) == record.region_normalized


def _can_collapse_federal_city_records(federal_city_records: tuple[GeoLookupRecord, ...]) -> bool:
    semantics = {
        (
            record.geo_bucket,
            record.geo_weight,
            record.inside_outer_polygon,
            record.inside_inner_polygon,
        )
        for record in federal_city_records
    }
    return len(semantics) == 1


def _collapsed_federal_city_result(
    *,
    source_address: str,
    federal_city_records: tuple[GeoLookupRecord, ...],
) -> GeoLookupResult:
    representative = federal_city_records[0]
    distances = [record.distance_to_moscow_km for record in federal_city_records]
    distance_to_moscow_km = round(float(median(distances)), 2) if distances else representative.distance_to_moscow_km
    distance_spread_km = round(max(distances) - min(distances), 2) if distances else representative.distance_spread_km
    return GeoLookupResult(
        match_status=LOOKUP_MATCH_MATCHED,
        source_address=source_address,
        matched_settlement=representative.settlement,
        matched_municipality="",
        matched_region=representative.region,
        geo_bucket=representative.geo_bucket,
        geo_weight=representative.geo_weight,
        inside_outer_polygon=representative.inside_outer_polygon,
        inside_inner_polygon=representative.inside_inner_polygon,
        distance_to_moscow_km=distance_to_moscow_km,
        candidate_count=1,
        variant_count=sum(record.variant_count for record in federal_city_records),
        distance_spread_km=distance_spread_km,
    )


def clean_source_address(value: str | None) -> str:
    if value is None:
        return ""
    return " ".join(str(value).replace("\xa0", " ").split())


def normalize_lookup_text(value: str | None) -> str:
    cleaned = clean_source_address(value).lower().replace("ё", "е")
    if not cleaned:
        return ""
    for pattern, replacement in _PRE_TOKEN_REPLACEMENTS:
        cleaned = pattern.sub(replacement, cleaned)
    cleaned = _normalize_federal_city_admin_tokens(cleaned)
    tokens = _NON_ALNUM_RE.sub(" ", cleaned).split()
    normalized_tokens: list[str] = []
    for index, token in enumerate(tokens):
        next_token = tokens[index + 1] if index + 1 < len(tokens) else ""
        normalized_tokens.append(_canonicalize_token(token, next_token=next_token))
    return _MULTISPACE_RE.sub(" ", " ".join(normalized_tokens)).strip()


def _normalize_federal_city_admin_tokens(value: str) -> str:
    normalized = value
    for pattern in _FEDERAL_CITY_ADMIN_PATTERNS:
        normalized = pattern.sub(" ", normalized)
    return normalized


def lookup_settlement(
    address_text: str | None,
    *,
    asset_path: str | os.PathLike[str] | None = None,
    index: GeoLookupIndex | None = None,
) -> GeoLookupResult:
    source_address = clean_source_address(address_text)
    if not source_address:
        return GeoLookupResult(
            match_status=LOOKUP_MATCH_NO_ADDRESS,
            source_address=source_address,
        )
    resolved_index = index
    if resolved_index is None:
        try:
            resolved_index = load_geo_lookup_index(asset_path)
        except (FileNotFoundError, OSError, ValueError, json.JSONDecodeError):
            return GeoLookupResult(
                match_status=LOOKUP_MATCH_UNAVAILABLE,
                source_address=source_address,
            )
    return resolved_index.lookup(source_address)


def resolve_geo_lookup_asset_path(asset_path: str | os.PathLike[str] | None = None) -> Path:
    raw_path = asset_path or os.environ.get(GEO_LOOKUP_ASSET_ENV_VAR) or DEFAULT_GEO_LOOKUP_ASSET_PATH
    return Path(raw_path).expanduser().resolve()


def load_geo_lookup_index(asset_path: str | os.PathLike[str] | None = None) -> GeoLookupIndex:
    resolved_path = resolve_geo_lookup_asset_path(asset_path)
    return _load_geo_lookup_index_cached(str(resolved_path))


@lru_cache(maxsize=8)
def _load_geo_lookup_index_cached(asset_path: str) -> GeoLookupIndex:
    payload_path = Path(asset_path)
    if not payload_path.is_file():
        raise FileNotFoundError(f"Geo lookup asset does not exist: {payload_path}")
    payload = json.loads(payload_path.read_text(encoding="utf-8"))
    schema_version = str(payload.get("schema_version") or "")
    if schema_version not in _SUPPORTED_GEO_LOOKUP_ASSET_SCHEMA_VERSIONS:
        raise ValueError(f"Unsupported geo lookup asset schema: {schema_version!r}")
    raw_records = payload.get("records")
    if not isinstance(raw_records, list):
        raise ValueError("Geo lookup asset must contain a records list")
    records = [record_from_payload(raw_record) for raw_record in raw_records]
    raw_ambiguous_records = payload.get("ambiguous_records", [])
    if schema_version == "moscow_geo_lookup.v1":
        ambiguous_records: list[GeoLookupAmbiguousRecord] = []
    else:
        if not isinstance(raw_ambiguous_records, list):
            raise ValueError("Geo lookup asset ambiguous_records must be a list")
        ambiguous_records = [ambiguous_record_from_payload(raw_record) for raw_record in raw_ambiguous_records]
    return GeoLookupIndex(
        records,
        ambiguous_records,
        schema_version=schema_version,
        asset_path=str(payload_path),
    )


def record_from_payload(payload: Mapping[str, Any]) -> GeoLookupRecord:
    aliases = payload.get("aliases")
    if not isinstance(aliases, list) or not aliases:
        raise ValueError("Geo lookup record aliases must be a non-empty list")
    geo_bucket = str(payload.get("geo_bucket") or "")
    if geo_bucket not in _VALID_GEO_BUCKETS:
        raise ValueError(f"Unsupported geo bucket: {geo_bucket!r}")

    settlement = clean_source_address(payload.get("settlement"))
    municipality = clean_source_address(payload.get("municipality"))
    region = clean_source_address(payload.get("region"))
    settlement_type = clean_source_address(payload.get("settlement_type"))
    full_name = clean_source_address(payload.get("full_name"))
    normalized_aliases = tuple(_sorted_unique(normalize_lookup_text(alias) for alias in aliases if normalize_lookup_text(alias)))
    if not settlement or not normalized_aliases:
        raise ValueError("Geo lookup record requires settlement and aliases")
    return GeoLookupRecord(
        settlement=settlement,
        municipality=municipality,
        region=region,
        settlement_type=settlement_type,
        full_name=full_name or settlement,
        aliases=normalized_aliases,
        geo_bucket=geo_bucket,
        geo_weight=_parse_int(payload.get("geo_weight")),
        inside_outer_polygon=_parse_bool(payload.get("inside_outer_polygon")),
        inside_inner_polygon=_parse_bool(payload.get("inside_inner_polygon")),
        distance_to_moscow_km=_parse_float(payload.get("distance_to_moscow_km")),
        variant_count=max(_parse_int(payload.get("variant_count")), 1),
        distance_spread_km=_parse_float(payload.get("distance_spread_km")),
        region_normalized=normalize_lookup_text(region),
        municipality_normalized=normalize_lookup_text(municipality),
        source_priority=_parse_int(payload.get("source_priority")),
    )


def ambiguous_record_from_payload(payload: Mapping[str, Any]) -> GeoLookupAmbiguousRecord:
    aliases = payload.get("aliases")
    if not isinstance(aliases, list) or not aliases:
        raise ValueError("Geo lookup ambiguous record aliases must be a non-empty list")
    geo_buckets = payload.get("geo_buckets")
    if not isinstance(geo_buckets, list) or not geo_buckets:
        raise ValueError("Geo lookup ambiguous record geo_buckets must be a non-empty list")
    raw_geo_buckets = [str(bucket) for bucket in geo_buckets]
    if any(bucket not in _VALID_GEO_BUCKETS for bucket in raw_geo_buckets):
        raise ValueError(f"Unsupported geo buckets for ambiguous record: {geo_buckets!r}")
    normalized_geo_buckets = tuple(_sorted_geo_buckets(raw_geo_buckets))

    settlement = clean_source_address(payload.get("settlement"))
    municipality = clean_source_address(payload.get("municipality"))
    region = clean_source_address(payload.get("region"))
    settlement_type = clean_source_address(payload.get("settlement_type"))
    full_name = clean_source_address(payload.get("full_name"))
    normalized_aliases = tuple(_sorted_unique(normalize_lookup_text(alias) for alias in aliases if normalize_lookup_text(alias)))
    if not settlement or not normalized_aliases:
        raise ValueError("Geo lookup ambiguous record requires settlement and aliases")
    return GeoLookupAmbiguousRecord(
        settlement=settlement,
        municipality=municipality,
        region=region,
        settlement_type=settlement_type,
        full_name=full_name or settlement,
        aliases=normalized_aliases,
        geo_buckets=normalized_geo_buckets,
        variant_count=max(_parse_int(payload.get("variant_count")), 1),
        distance_spread_km=_parse_float(payload.get("distance_spread_km")),
        region_normalized=normalize_lookup_text(region),
        municipality_normalized=normalize_lookup_text(municipality),
    )


def build_geo_lookup_asset_payload(
    rows: Iterable[Mapping[str, Any]],
    *,
    source_contract: str = GEO_LOOKUP_SOURCE_CONTRACT,
    override_records: Iterable[Mapping[str, Any]] | None = None,
) -> dict[str, Any]:
    grouped_records: dict[tuple[str, str, str, str], _ClusterAccumulator] = defaultdict(_ClusterAccumulator)
    input_row_count = 0
    for row_number, raw_row in enumerate(rows, start=1):
        input_row_count = row_number
        region = clean_source_address(raw_row.get("region"))
        municipality = clean_source_address(raw_row.get("municipality"))
        settlement = clean_source_address(raw_row.get("settlement"))
        settlement_type = clean_source_address(raw_row.get("settlement_type") or raw_row.get("type"))
        if not settlement:
            raise ValueError(f"row {row_number}: settlement is required")
        full_name = clean_source_address(raw_row.get("full_name")) or _compose_full_name(settlement_type, settlement)
        classification = _classification_from_row(raw_row, row_number=row_number)
        aliases = _build_record_aliases(
            settlement=settlement,
            settlement_type=settlement_type,
            full_name=full_name,
        )
        record_key = (
            normalize_lookup_text(region),
            normalize_lookup_text(municipality),
            normalize_lookup_text(settlement),
            normalize_lookup_text(settlement_type),
        )
        grouped_records[record_key].add(
            settlement=settlement,
            municipality=municipality,
            region=region,
            settlement_type=settlement_type,
            full_name=full_name,
            aliases=aliases,
            classification=classification,
        )

    records_payload: list[dict[str, Any]] = []
    ambiguous_records_payload: list[dict[str, Any]] = []
    duplicate_cluster_count = 0
    auto_merged_cluster_count = 0
    same_bucket_conflict_cluster_count = 0
    identical_duplicate_cluster_count = 0
    ambiguous_cluster_count = 0
    override_entry_count = 0

    for _, cluster in sorted(grouped_records.items(), key=lambda item: item[0]):
        if cluster.variant_count > 1:
            duplicate_cluster_count += 1
        if cluster.is_ambiguous:
            ambiguous_cluster_count += 1
            ambiguous_records_payload.append(cluster.to_ambiguous_payload())
            continue
        if cluster.variant_count > 1:
            auto_merged_cluster_count += 1
            if cluster.has_conflicting_classification:
                same_bucket_conflict_cluster_count += 1
            else:
                identical_duplicate_cluster_count += 1
        records_payload.append(cluster.to_record_payload())

    for override_record in _resolve_override_records(override_records):
        records_payload.append(override_record)
        override_entry_count += 1

    records_payload.sort(key=_payload_record_sort_key)

    return {
        "schema_version": GEO_LOOKUP_ASSET_SCHEMA_VERSION,
        "source_contract": source_contract,
        "input_row_count": input_row_count,
        "record_count": len(records_payload),
        "ambiguous_record_count": len(ambiguous_records_payload),
        "build_stats": {
            "cluster_count": len(grouped_records),
            "duplicate_cluster_count": duplicate_cluster_count,
            "auto_merged_cluster_count": auto_merged_cluster_count,
            "same_bucket_conflict_cluster_count": same_bucket_conflict_cluster_count,
            "identical_duplicate_cluster_count": identical_duplicate_cluster_count,
            "ambiguous_cluster_count": ambiguous_cluster_count,
            "override_entry_count": override_entry_count,
        },
        "records": records_payload,
        "ambiguous_records": ambiguous_records_payload,
    }


def write_geo_lookup_asset(
    output_path: str | os.PathLike[str],
    rows: Iterable[Mapping[str, Any]],
    *,
    source_contract: str = GEO_LOOKUP_SOURCE_CONTRACT,
    override_records: Iterable[Mapping[str, Any]] | None = None,
) -> dict[str, Any]:
    payload = build_geo_lookup_asset_payload(
        rows,
        source_contract=source_contract,
        override_records=override_records,
    )
    destination = Path(output_path).expanduser().resolve()
    destination.parent.mkdir(parents=True, exist_ok=True)
    destination.write_text(
        json.dumps(payload, ensure_ascii=False, separators=(",", ":")) + "\n",
        encoding="utf-8",
    )
    return payload


def _classification_from_row(row: Mapping[str, Any], *, row_number: int):
    latitude = _maybe_float(row.get("latitude_dd"))
    longitude = _maybe_float(row.get("longitude_dd"))
    raw_geo_bucket = clean_source_address(row.get("geo_bucket"))
    if raw_geo_bucket in _VALID_GEO_BUCKETS:
        return _ClassificationValue(
            geo_bucket=raw_geo_bucket,
            geo_weight=_parse_int(row.get("geo_weight")),
            inside_outer_polygon=_parse_bool(row.get("inside_outer_polygon")),
            inside_inner_polygon=_parse_bool(row.get("inside_inner_polygon")),
            distance_to_moscow_km=_parse_float(row.get("distance_to_moscow_km")),
        )
    if latitude is None or longitude is None:
        raise ValueError(f"row {row_number}: classification is missing and latitude/longitude are unavailable")
    return _ClassificationValue.from_geo_zone(classify(latitude, longitude))


@dataclass(frozen=True)
class _ClassificationValue:
    geo_bucket: GeoBucket
    geo_weight: int
    inside_outer_polygon: bool
    inside_inner_polygon: bool
    distance_to_moscow_km: float

    @classmethod
    def from_geo_zone(cls, value: Any) -> "_ClassificationValue":
        return cls(
            geo_bucket=value.geo_bucket,
            geo_weight=value.geo_weight,
            inside_outer_polygon=value.inside_outer_polygon,
            inside_inner_polygon=value.inside_inner_polygon,
            distance_to_moscow_km=value.distance_to_moscow_km,
        )


@dataclass
class _ClusterAccumulator:
    region_values: Counter[str] = field(default_factory=Counter)
    municipality_values: Counter[str] = field(default_factory=Counter)
    settlement_values: Counter[str] = field(default_factory=Counter)
    settlement_type_values: Counter[str] = field(default_factory=Counter)
    full_name_values: Counter[str] = field(default_factory=Counter)
    aliases: set[str] = field(default_factory=set)
    classifications: list[_ClassificationValue] = field(default_factory=list)
    classification_signatures: set[tuple[GeoBucket, int, bool, bool, float]] = field(default_factory=set)
    geo_buckets: set[GeoBucket] = field(default_factory=set)

    def add(
        self,
        *,
        settlement: str,
        municipality: str,
        region: str,
        settlement_type: str,
        full_name: str,
        aliases: Iterable[str],
        classification: _ClassificationValue,
    ) -> None:
        self.region_values.update(_counter_input(region))
        self.municipality_values.update(_counter_input(municipality))
        self.settlement_values.update(_counter_input(settlement))
        self.settlement_type_values.update(_counter_input(settlement_type))
        self.full_name_values.update(_counter_input(full_name))
        self.aliases.update(alias for alias in aliases if alias)
        self.classifications.append(classification)
        self.classification_signatures.add(_classification_signature(classification))
        self.geo_buckets.add(classification.geo_bucket)

    @property
    def variant_count(self) -> int:
        return len(self.classifications)

    @property
    def is_ambiguous(self) -> bool:
        return len(self.geo_buckets) > 1

    @property
    def has_conflicting_classification(self) -> bool:
        return len(self.classification_signatures) > 1

    @property
    def canonical_identity(self) -> dict[str, str]:
        settlement = _choose_canonical_text(self.settlement_values)
        settlement_type = _choose_canonical_text(self.settlement_type_values)
        full_name = _choose_canonical_text(self.full_name_values) or _compose_full_name(settlement_type, settlement)
        return {
            "settlement": settlement,
            "municipality": _choose_canonical_text(self.municipality_values),
            "region": _choose_canonical_text(self.region_values),
            "settlement_type": settlement_type,
            "full_name": full_name,
        }

    @property
    def distance_spread_km(self) -> float:
        distances = [classification.distance_to_moscow_km for classification in self.classifications]
        if not distances:
            return 0.0
        return round(max(distances) - min(distances), 2)

    def to_record_payload(self) -> dict[str, Any]:
        identity = self.canonical_identity
        bucket = next(iter(self.geo_buckets))
        semantics = _classification_for_bucket(bucket)
        distances = [classification.distance_to_moscow_km for classification in self.classifications]
        identity["aliases"] = _sorted_unique(
            self.aliases
            | set(
                _build_record_aliases(
                    settlement=identity["settlement"],
                    settlement_type=identity["settlement_type"],
                    full_name=identity["full_name"],
                )
            )
        )
        return {
            "settlement": identity["settlement"],
            "municipality": identity["municipality"],
            "region": identity["region"],
            "settlement_type": identity["settlement_type"],
            "full_name": identity["full_name"],
            "aliases": identity["aliases"],
            "geo_bucket": semantics.geo_bucket,
            "geo_weight": semantics.geo_weight,
            "inside_outer_polygon": semantics.inside_outer_polygon,
            "inside_inner_polygon": semantics.inside_inner_polygon,
            "distance_to_moscow_km": round(float(median(distances)), 2),
            "variant_count": self.variant_count,
            "distance_spread_km": self.distance_spread_km,
        }

    def to_ambiguous_payload(self) -> dict[str, Any]:
        identity = self.canonical_identity
        identity["aliases"] = _sorted_unique(
            self.aliases
            | set(
                _build_record_aliases(
                    settlement=identity["settlement"],
                    settlement_type=identity["settlement_type"],
                    full_name=identity["full_name"],
                )
            )
        )
        return {
            "settlement": identity["settlement"],
            "municipality": identity["municipality"],
            "region": identity["region"],
            "settlement_type": identity["settlement_type"],
            "full_name": identity["full_name"],
            "aliases": identity["aliases"],
            "geo_buckets": _sorted_geo_buckets(self.geo_buckets),
            "variant_count": self.variant_count,
            "distance_spread_km": self.distance_spread_km,
        }


def _compose_full_name(settlement_type: str, settlement: str) -> str:
    if settlement_type and settlement:
        return f"{settlement_type} {settlement}"
    return settlement or settlement_type


def _build_record_aliases(*, settlement: str, settlement_type: str, full_name: str) -> tuple[str, ...]:
    aliases = [
        normalize_lookup_text(settlement),
        normalize_lookup_text(full_name),
    ]
    if settlement_type and settlement:
        aliases.append(normalize_lookup_text(f"{settlement_type} {settlement}"))
    return tuple(_sorted_unique(alias for alias in aliases if alias and alias not in _GENERIC_TOKENS))


def _canonicalize_token(token: str, *, next_token: str) -> str:
    if token in {"г", "гор", "город"} and _looks_like_name_token(next_token):
        return "город"
    if token in {"обл", "область"}:
        return "область"
    if token in {"рн", "район"}:
        return "район"
    if token in {"пос", "поселок"} and _looks_like_name_token(next_token):
        return "поселок"
    if token in {"с", "село"} and _looks_like_name_token(next_token):
        return "село"
    if token in {"д", "дер", "деревня"} and _looks_like_name_token(next_token):
        return "деревня"
    return token


def _looks_like_name_token(token: str) -> bool:
    return bool(token) and bool(_CYRILLIC_OR_LATIN_RE.search(token))


def _pad_phrase(value: str) -> str:
    return f" {value.strip()} "


def _contains_phrase(padded_haystack: str, phrase: str) -> bool:
    return f" {phrase} " in padded_haystack


def _alias_rank(alias: str) -> tuple[int, int]:
    tokens = alias.split()
    return (len(tokens), len(alias))


def _lookup_record_key(record: LookupCandidate) -> tuple[str, str, str, str]:
    return (
        record.region_normalized,
        record.municipality_normalized,
        normalize_lookup_text(record.settlement),
        normalize_lookup_text(record.settlement_type),
    )


def _classification_signature(classification: _ClassificationValue) -> tuple[GeoBucket, int, bool, bool, float]:
    return (
        classification.geo_bucket,
        classification.geo_weight,
        classification.inside_outer_polygon,
        classification.inside_inner_polygon,
        round(classification.distance_to_moscow_km, 2),
    )


def _classification_for_bucket(bucket: GeoBucket) -> _ClassificationValue:
    if bucket == GEO_BUCKET_CORE:
        return _ClassificationValue(
            geo_bucket=GEO_BUCKET_CORE,
            geo_weight=DEFAULT_MOSCOW_GEO_ZONE.core_weight,
            inside_outer_polygon=True,
            inside_inner_polygon=True,
            distance_to_moscow_km=0.0,
        )
    if bucket == GEO_BUCKET_OUTER_BAND:
        return _ClassificationValue(
            geo_bucket=GEO_BUCKET_OUTER_BAND,
            geo_weight=DEFAULT_MOSCOW_GEO_ZONE.outer_band_weight,
            inside_outer_polygon=True,
            inside_inner_polygon=False,
            distance_to_moscow_km=0.0,
        )
    return _ClassificationValue(
        geo_bucket=GEO_BUCKET_OUTSIDE,
        geo_weight=DEFAULT_MOSCOW_GEO_ZONE.outside_weight,
        inside_outer_polygon=False,
        inside_inner_polygon=False,
        distance_to_moscow_km=0.0,
    )


def _counter_input(value: str) -> Counter[str]:
    cleaned = clean_source_address(value)
    return Counter({cleaned: 1}) if cleaned else Counter()


def _choose_canonical_text(values: Counter[str]) -> str:
    if not values:
        return ""
    return max(values, key=lambda item: (values[item], len(item), item))


def _sorted_geo_buckets(values: Iterable[GeoBucket | str]) -> list[GeoBucket]:
    bucket_order = {
        GEO_BUCKET_CORE: 0,
        GEO_BUCKET_OUTER_BAND: 1,
        GEO_BUCKET_OUTSIDE: 2,
    }
    normalized = {str(value) for value in values if str(value) in bucket_order}
    return sorted(normalized, key=lambda item: bucket_order[item])


def _resolve_override_records(override_records: Iterable[Mapping[str, Any]] | None) -> list[dict[str, Any]]:
    if override_records is None:
        override_records = _load_default_geo_lookup_override_records()
    return [_override_record_to_payload(record) for record in override_records]


def _load_default_geo_lookup_override_records() -> list[Mapping[str, Any]]:
    if not DEFAULT_GEO_LOOKUP_OVERRIDE_PATH.is_file():
        return []
    payload = json.loads(DEFAULT_GEO_LOOKUP_OVERRIDE_PATH.read_text(encoding="utf-8"))
    schema_version = str(payload.get("schema_version") or "")
    if schema_version != GEO_LOOKUP_OVERRIDE_SCHEMA_VERSION:
        raise ValueError(f"Unsupported geo lookup override schema: {schema_version!r}")
    raw_records = payload.get("records")
    if not isinstance(raw_records, list):
        raise ValueError("Geo lookup override manifest must contain a records list")
    return raw_records


def _override_record_to_payload(payload: Mapping[str, Any]) -> dict[str, Any]:
    settlement = clean_source_address(payload.get("settlement"))
    municipality = clean_source_address(payload.get("municipality"))
    region = clean_source_address(payload.get("region"))
    settlement_type = clean_source_address(payload.get("settlement_type"))
    if not settlement:
        raise ValueError("Geo lookup override record settlement is required")
    full_name = clean_source_address(payload.get("full_name")) or _compose_full_name(settlement_type, settlement)
    latitude = _maybe_float(payload.get("latitude_dd"))
    longitude = _maybe_float(payload.get("longitude_dd"))
    if latitude is None or longitude is None:
        raise ValueError(f"Geo lookup override record requires latitude/longitude: {settlement!r}")
    classification = _ClassificationValue.from_geo_zone(classify(latitude, longitude))
    aliases = payload.get("aliases")
    if isinstance(aliases, list) and aliases:
        normalized_aliases = _sorted_unique(normalize_lookup_text(alias) for alias in aliases if normalize_lookup_text(alias))
    else:
        normalized_aliases = _sorted_unique(
            _build_record_aliases(
                settlement=settlement,
                settlement_type=settlement_type,
                full_name=full_name,
            )
        )
    return {
        "settlement": settlement,
        "municipality": municipality,
        "region": region,
        "settlement_type": settlement_type,
        "full_name": full_name,
        "aliases": normalized_aliases,
        "geo_bucket": classification.geo_bucket,
        "geo_weight": classification.geo_weight,
        "inside_outer_polygon": classification.inside_outer_polygon,
        "inside_inner_polygon": classification.inside_inner_polygon,
        "distance_to_moscow_km": round(classification.distance_to_moscow_km, 2),
        "variant_count": 1,
        "distance_spread_km": 0.0,
        "source_priority": max(_parse_int(payload.get("source_priority")), 0),
    }


def _payload_record_sort_key(payload: Mapping[str, Any]) -> tuple[str, str, str, str]:
    return (
        normalize_lookup_text(payload.get("region")),
        normalize_lookup_text(payload.get("municipality")),
        normalize_lookup_text(payload.get("settlement")),
        normalize_lookup_text(payload.get("settlement_type")),
    )


def _sorted_unique(values: Iterable[str]) -> list[str]:
    normalized = {value for value in values if value}
    return sorted(normalized, key=lambda item: (-len(item.split()), -len(item), item))


def _parse_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    cleaned = clean_source_address(value).lower()
    return cleaned in {"1", "true", "yes"}


def _parse_int(value: Any) -> int:
    cleaned = clean_source_address(value)
    if not cleaned:
        return 0
    return int(float(cleaned.replace(",", ".")))


def _maybe_float(value: Any) -> float | None:
    cleaned = clean_source_address(value)
    if not cleaned:
        return None
    return float(cleaned.replace(",", "."))


def _parse_float(value: Any) -> float:
    parsed = _maybe_float(value)
    return parsed if parsed is not None else 0.0


__all__ = [
    "DEFAULT_GEO_LOOKUP_ASSET_PATH",
    "DEFAULT_GEO_LOOKUP_OVERRIDE_PATH",
    "GEO_LOOKUP_ASSET_ENV_VAR",
    "GEO_LOOKUP_ASSET_SCHEMA_VERSION",
    "GEO_LOOKUP_OVERRIDE_SCHEMA_VERSION",
    "GeoLookupIndex",
    "GeoLookupAmbiguousRecord",
    "GeoLookupRecord",
    "GeoLookupResult",
    "LOOKUP_MATCH_AMBIGUOUS",
    "LOOKUP_MATCH_MATCHED",
    "LOOKUP_MATCH_NOT_FOUND",
    "LOOKUP_MATCH_NO_ADDRESS",
    "LOOKUP_MATCH_UNAVAILABLE",
    "LookupMatchStatus",
    "build_geo_lookup_asset_payload",
    "clean_source_address",
    "load_geo_lookup_index",
    "lookup_settlement",
    "normalize_lookup_text",
    "resolve_geo_lookup_asset_path",
    "write_geo_lookup_asset",
]

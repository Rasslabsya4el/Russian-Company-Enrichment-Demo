from __future__ import annotations

from dataclasses import dataclass
from typing import Mapping

from app.discovery.geo_lookup import LOOKUP_MATCH_MATCHED
from app.discovery.geo_zone import GEO_BUCKET_CORE, GEO_BUCKET_OUTER_BAND, GEO_BUCKET_OUTSIDE


GEO_DEEP_PARSE_SKIP_REASON = "geo_out_of_scope"
GEO_BUCKET_ALIASES = {
    "core": (GEO_BUCKET_CORE,),
    "inner": (GEO_BUCKET_CORE,),
    "moscow_core": (GEO_BUCKET_CORE,),
    "outer": (GEO_BUCKET_OUTER_BAND,),
    "outer_band": (GEO_BUCKET_OUTER_BAND,),
    "moscow_outer": (GEO_BUCKET_CORE, GEO_BUCKET_OUTER_BAND),
    "moscow": (GEO_BUCKET_CORE, GEO_BUCKET_OUTER_BAND),
    "outside": (GEO_BUCKET_OUTSIDE,),
}


@dataclass(frozen=True, slots=True)
class GeoDeepParseAdmissionDecision:
    allowed_buckets: tuple[str, ...]
    geo_bucket: str = ""
    match_status: str = ""
    source_address: str = ""
    matched_region: str = ""
    matched_municipality: str = ""
    matched_settlement: str = ""
    skip_deep_parse: bool = False
    reason: str = ""
    note: str = ""


def parse_allowed_geo_buckets(value: object) -> tuple[str, ...]:
    if isinstance(value, str):
        tokens = value.replace(";", ",").replace("|", ",").split(",")
    elif isinstance(value, (list, tuple, set, frozenset)):
        tokens = list(value)
    else:
        tokens = []
    buckets: list[str] = []
    for token in tokens:
        normalized = str(token or "").strip().lower().replace("-", "_").replace(" ", "_")
        for bucket in GEO_BUCKET_ALIASES.get(normalized, (normalized,)):
            if bucket in {GEO_BUCKET_CORE, GEO_BUCKET_OUTER_BAND, GEO_BUCKET_OUTSIDE} and bucket not in buckets:
                buckets.append(bucket)
    return tuple(buckets)


def evaluate_geo_deep_parse_admission(
    *,
    geo_signal: Mapping[str, object],
    allowed_buckets: tuple[str, ...],
) -> GeoDeepParseAdmissionDecision:
    normalized_allowed_buckets = parse_allowed_geo_buckets(allowed_buckets)
    match_status = _compact_text(geo_signal.get("match_status"))
    geo_bucket = _compact_text(geo_signal.get("geo_bucket"))
    source_address = _compact_text(geo_signal.get("source_address"))
    matched_region = _compact_text(geo_signal.get("matched_region"))
    matched_municipality = _compact_text(geo_signal.get("matched_municipality"))
    matched_settlement = _compact_text(geo_signal.get("matched_settlement"))
    if (
        not normalized_allowed_buckets
        or match_status != LOOKUP_MATCH_MATCHED
        or not geo_bucket
        or geo_bucket in normalized_allowed_buckets
    ):
        return GeoDeepParseAdmissionDecision(
            allowed_buckets=normalized_allowed_buckets,
            geo_bucket=geo_bucket,
            match_status=match_status,
            source_address=source_address,
            matched_region=matched_region,
            matched_municipality=matched_municipality,
            matched_settlement=matched_settlement,
        )
    note_parts = [
        GEO_DEEP_PARSE_SKIP_REASON,
        f"geo_bucket={geo_bucket}",
        f"allowed_geo_buckets={','.join(normalized_allowed_buckets)}",
    ]
    if source_address:
        note_parts.append(f"source_address={source_address}")
    matched_location = ", ".join(
        item for item in (matched_region, matched_municipality, matched_settlement) if item
    )
    if matched_location:
        note_parts.append(f"matched_location={matched_location}")
    return GeoDeepParseAdmissionDecision(
        allowed_buckets=normalized_allowed_buckets,
        geo_bucket=geo_bucket,
        match_status=match_status,
        source_address=source_address,
        matched_region=matched_region,
        matched_municipality=matched_municipality,
        matched_settlement=matched_settlement,
        skip_deep_parse=True,
        reason=GEO_DEEP_PARSE_SKIP_REASON,
        note="; ".join(note_parts),
    )


def _compact_text(value: object) -> str:
    return " ".join(str(value or "").strip().split())

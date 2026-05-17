from __future__ import annotations

from collections.abc import Mapping
from dataclasses import asdict, dataclass, field
from typing import Any


COMPANY_PROFILE_SCHEMA_VERSION = "company_profile.v1"
COMPANY_OUTPUT_CONTRACT_VERSION = "company_result.v2"


def _clean_text(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


def _first_text(*values: Any) -> str:
    for value in values:
        text = _clean_text(value)
        if text:
            return text
    return ""


def _first_present(*values: Any) -> Any:
    for value in values:
        if value not in (None, ""):
            return value
    return None


def _string_list(value: Any) -> list[str]:
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return []
        if "|" in text:
            items = [part.strip() for part in text.split("|")]
        else:
            items = [text]
    elif isinstance(value, (list, tuple, set)):
        items = value
    elif value is None:
        return []
    else:
        items = [value]
    normalized: list[str] = []
    for item in items:
        text = _clean_text(item)
        if text and text not in normalized:
            normalized.append(text)
    return normalized


def _int_value(*values: Any) -> int:
    for value in values:
        if value in (None, ""):
            continue
        try:
            return int(value)
        except (TypeError, ValueError):
            continue
    return 0


def _optional_int_value(*values: Any) -> int | None:
    for value in values:
        if value in (None, ""):
            continue
        try:
            return int(value)
        except (TypeError, ValueError):
            continue
    return None


def _optional_float_value(*values: Any) -> float | None:
    for value in values:
        if value in (None, ""):
            continue
        try:
            return float(value)
        except (TypeError, ValueError):
            continue
    return None


def _optional_bool_value(*values: Any) -> bool | None:
    for value in values:
        if value in (None, ""):
            continue
        if isinstance(value, bool):
            return value
        text = _clean_text(value).lower()
        if text in {"1", "true", "yes"}:
            return True
        if text in {"0", "false", "no"}:
            return False
    return None


@dataclass
class CompanyProfileSummary:
    inn: str = ""
    company_name: str = ""
    processing_status: str = ""
    domain_resolution_status: str = ""
    lead_count: int = 0
    decision_summary: str = ""
    issues: list[str] = field(default_factory=list)


@dataclass
class CompanyProfileContactSelection:
    value: str = ""
    sources: list[str] = field(default_factory=list)


@dataclass
class CompanyProfileContactSet:
    phones: list[str] = field(default_factory=list)
    emails: list[str] = field(default_factory=list)
    websites: list[str] = field(default_factory=list)
    addresses: list[str] = field(default_factory=list)


@dataclass
class CompanyProfileContacts:
    trusted: CompanyProfileContactSet = field(default_factory=CompanyProfileContactSet)
    raw: CompanyProfileContactSet = field(default_factory=CompanyProfileContactSet)
    best_phone: CompanyProfileContactSelection = field(default_factory=CompanyProfileContactSelection)
    best_email: CompanyProfileContactSelection = field(default_factory=CompanyProfileContactSelection)
    best_address: CompanyProfileContactSelection = field(default_factory=CompanyProfileContactSelection)


@dataclass
class CompanyProfileSites:
    primary_domain: str = ""
    best_site: str = ""
    best_site_status: str = ""
    best_site_sources: list[str] = field(default_factory=list)
    candidate_sites: list[str] = field(default_factory=list)
    confirmed_sites: list[str] = field(default_factory=list)
    site_classes: list[str] = field(default_factory=list)
    worth_crawling: list[str] = field(default_factory=list)


@dataclass
class CompanyProfileGeoSignal:
    match_status: str = ""
    source_address: str = ""
    matched_settlement: str = ""
    matched_municipality: str = ""
    matched_region: str = ""
    geo_bucket: str = ""
    geo_weight: int | None = None
    inside_outer_polygon: bool | None = None
    inside_inner_polygon: bool | None = None
    distance_to_moscow_km: float | None = None
    candidate_count: int = 0
    variant_count: int = 0
    distance_spread_km: float | None = None
    ambiguous_geo_buckets: list[str] = field(default_factory=list)


@dataclass
class CompanyProfileNamingSignal:
    signal_status: str = ""
    source_name: str = ""
    verdict: str = "none"
    risk_weight: int = 0
    matched_markers: list[str] = field(default_factory=list)
    reason_codes: list[str] = field(default_factory=list)


@dataclass
class CompanyProfileSignals:
    geo: CompanyProfileGeoSignal = field(default_factory=CompanyProfileGeoSignal)
    naming: CompanyProfileNamingSignal = field(default_factory=CompanyProfileNamingSignal)


@dataclass
class CompanyProfile:
    schema_version: str = COMPANY_PROFILE_SCHEMA_VERSION
    summary: CompanyProfileSummary = field(default_factory=CompanyProfileSummary)
    contacts: CompanyProfileContacts = field(default_factory=CompanyProfileContacts)
    sites: CompanyProfileSites = field(default_factory=CompanyProfileSites)
    signals: CompanyProfileSignals = field(default_factory=CompanyProfileSignals)


def _contact_selection_from_payload(
    payload: CompanyProfileContactSelection | Mapping[str, Any] | Any | None,
    *,
    sources_fallback: Any = None,
) -> CompanyProfileContactSelection:
    if isinstance(payload, CompanyProfileContactSelection):
        return payload
    if isinstance(payload, Mapping):
        return CompanyProfileContactSelection(
            value=_first_text(payload.get("value"), payload.get("selected"), payload.get("best")),
            sources=_string_list(payload.get("sources") or sources_fallback),
        )
    return CompanyProfileContactSelection(
        value=_clean_text(payload),
        sources=_string_list(sources_fallback),
    )


def _contact_set_from_payload(payload: CompanyProfileContactSet | Mapping[str, Any] | None) -> CompanyProfileContactSet:
    if isinstance(payload, CompanyProfileContactSet):
        return payload
    source = payload if isinstance(payload, Mapping) else {}
    return CompanyProfileContactSet(
        phones=_string_list(source.get("phones")),
        emails=_string_list(source.get("emails")),
        websites=_string_list(source.get("websites")),
        addresses=_string_list(source.get("addresses")),
    )


def _summary_from_payload(payload: CompanyProfileSummary | Mapping[str, Any] | None) -> CompanyProfileSummary:
    if isinstance(payload, CompanyProfileSummary):
        return payload
    source = payload if isinstance(payload, Mapping) else {}
    return CompanyProfileSummary(
        inn=_clean_text(source.get("inn")),
        company_name=_first_text(source.get("company_name"), source.get("company_name_found")),
        processing_status=_first_text(source.get("processing_status"), source.get("status")),
        domain_resolution_status=_first_text(source.get("domain_resolution_status"), source.get("domain_status")),
        lead_count=_int_value(source.get("lead_count")),
        decision_summary=_clean_text(source.get("decision_summary")),
        issues=_string_list(source.get("issues")),
    )


def _contacts_from_payload(payload: CompanyProfileContacts | Mapping[str, Any] | None) -> CompanyProfileContacts:
    if isinstance(payload, CompanyProfileContacts):
        return payload
    source = payload if isinstance(payload, Mapping) else {}
    return CompanyProfileContacts(
        trusted=_contact_set_from_payload(
            source.get("trusted") or source.get("known_contacts") or source.get("trusted_contacts") or source.get("contacts_open")
        ),
        raw=_contact_set_from_payload(
            source.get("raw") or source.get("raw_contacts") or source.get("merged_contacts")
        ),
        best_phone=_contact_selection_from_payload(
            source.get("best_phone"),
            sources_fallback=source.get("best_phone_sources"),
        ),
        best_email=_contact_selection_from_payload(
            source.get("best_email"),
            sources_fallback=source.get("best_email_sources"),
        ),
        best_address=_contact_selection_from_payload(
            source.get("best_address"),
            sources_fallback=source.get("best_address_sources"),
        ),
    )


def _sites_from_payload(payload: CompanyProfileSites | Mapping[str, Any] | None) -> CompanyProfileSites:
    if isinstance(payload, CompanyProfileSites):
        return payload
    source = payload if isinstance(payload, Mapping) else {}
    return CompanyProfileSites(
        primary_domain=_first_text(source.get("primary_domain"), source.get("site_id")),
        best_site=_first_text(source.get("best_site"), source.get("site_url"), source.get("url")),
        best_site_status=_clean_text(source.get("best_site_status")),
        best_site_sources=_string_list(source.get("best_site_sources")),
        candidate_sites=_string_list(source.get("candidate_sites")),
        confirmed_sites=_string_list(source.get("confirmed_sites")),
        site_classes=_string_list(source.get("site_classes")),
        worth_crawling=_string_list(source.get("worth_crawling")),
    )


def _geo_signal_from_payload(payload: CompanyProfileGeoSignal | Mapping[str, Any] | None) -> CompanyProfileGeoSignal:
    if isinstance(payload, CompanyProfileGeoSignal):
        return payload
    source = payload if isinstance(payload, Mapping) else {}
    return CompanyProfileGeoSignal(
        match_status=_first_text(source.get("match_status"), source.get("status")),
        source_address=_clean_text(source.get("source_address")),
        matched_settlement=_clean_text(source.get("matched_settlement")),
        matched_municipality=_clean_text(source.get("matched_municipality")),
        matched_region=_clean_text(source.get("matched_region")),
        geo_bucket=_clean_text(source.get("geo_bucket")),
        geo_weight=_optional_int_value(source.get("geo_weight")),
        inside_outer_polygon=_optional_bool_value(source.get("inside_outer_polygon")),
        inside_inner_polygon=_optional_bool_value(source.get("inside_inner_polygon")),
        distance_to_moscow_km=_optional_float_value(source.get("distance_to_moscow_km")),
        candidate_count=_int_value(source.get("candidate_count")),
        variant_count=_int_value(source.get("variant_count")),
        distance_spread_km=_optional_float_value(source.get("distance_spread_km")),
        ambiguous_geo_buckets=_string_list(source.get("ambiguous_geo_buckets") or source.get("geo_buckets")),
    )


def _naming_signal_from_payload(
    payload: CompanyProfileNamingSignal | Mapping[str, Any] | None,
) -> CompanyProfileNamingSignal:
    if isinstance(payload, CompanyProfileNamingSignal):
        return payload
    source = payload if isinstance(payload, Mapping) else {}
    return CompanyProfileNamingSignal(
        signal_status=_first_text(source.get("signal_status"), source.get("status")),
        source_name=_clean_text(source.get("source_name")),
        verdict=_first_text(source.get("verdict"), "none"),
        risk_weight=_int_value(source.get("risk_weight")),
        matched_markers=_string_list(source.get("matched_markers")),
        reason_codes=_string_list(source.get("reason_codes")),
    )


def _signals_from_payload(payload: CompanyProfileSignals | Mapping[str, Any] | None) -> CompanyProfileSignals:
    if isinstance(payload, CompanyProfileSignals):
        return payload
    source = payload if isinstance(payload, Mapping) else {}
    return CompanyProfileSignals(
        geo=_geo_signal_from_payload(source.get("geo")),
        naming=_naming_signal_from_payload(source.get("naming")),
    )


def assemble_company_profile(
    *,
    summary: CompanyProfileSummary | Mapping[str, Any] | None = None,
    contacts: CompanyProfileContacts | Mapping[str, Any] | None = None,
    sites: CompanyProfileSites | Mapping[str, Any] | None = None,
    signals: CompanyProfileSignals | Mapping[str, Any] | None = None,
    schema_version: str = COMPANY_PROFILE_SCHEMA_VERSION,
) -> CompanyProfile:
    return CompanyProfile(
        schema_version=_clean_text(schema_version) or COMPANY_PROFILE_SCHEMA_VERSION,
        summary=_summary_from_payload(summary),
        contacts=_contacts_from_payload(contacts),
        sites=_sites_from_payload(sites),
        signals=_signals_from_payload(signals),
    )


def company_profile_from_dict(payload: Mapping[str, Any] | None) -> CompanyProfile:
    source = payload if isinstance(payload, Mapping) else {}
    summary_block = source.get("summary") if isinstance(source.get("summary"), Mapping) else {}
    contacts_block = source.get("contacts") if isinstance(source.get("contacts"), Mapping) else {}
    sites_block = source.get("sites") if isinstance(source.get("sites"), Mapping) else {}
    signals_block = source.get("signals") if isinstance(source.get("signals"), Mapping) else {}
    geo_block = signals_block.get("geo") if isinstance(signals_block.get("geo"), Mapping) else {}
    naming_block = signals_block.get("naming") if isinstance(signals_block.get("naming"), Mapping) else {}
    return assemble_company_profile(
        schema_version=_first_text(source.get("schema_version"), COMPANY_PROFILE_SCHEMA_VERSION),
        summary={
            "inn": _first_text(summary_block.get("inn"), source.get("inn")),
            "company_name": _first_text(
                summary_block.get("company_name"),
                source.get("company_name"),
                source.get("company_name_found"),
            ),
            "processing_status": _first_text(
                summary_block.get("processing_status"),
                source.get("processing_status"),
                source.get("status"),
            ),
            "domain_resolution_status": _first_text(
                summary_block.get("domain_resolution_status"),
                source.get("domain_resolution_status"),
                source.get("domain_status"),
            ),
            "lead_count": _int_value(summary_block.get("lead_count"), source.get("lead_count")),
            "decision_summary": _first_text(
                summary_block.get("decision_summary"),
                source.get("decision_summary"),
            ),
            "issues": summary_block.get("issues") or source.get("issues"),
        },
        contacts={
            "trusted": contacts_block.get("trusted")
            or source.get("known_contacts")
            or source.get("trusted_contacts")
            or source.get("contacts_open"),
            "raw": contacts_block.get("raw")
            or source.get("raw_contacts")
            or source.get("merged_contacts")
            or {
                "websites": source.get("websites"),
                "phones": source.get("phones"),
                "emails": source.get("emails"),
                "addresses": source.get("addresses"),
            },
            "best_phone": contacts_block.get("best_phone") or source.get("best_phone"),
            "best_phone_sources": contacts_block.get("best_phone_sources")
            or source.get("best_phone_sources"),
            "best_email": contacts_block.get("best_email") or source.get("best_email"),
            "best_email_sources": contacts_block.get("best_email_sources")
            or source.get("best_email_sources"),
            "best_address": contacts_block.get("best_address") or source.get("best_address"),
            "best_address_sources": contacts_block.get("best_address_sources")
            or source.get("best_address_sources"),
        },
        sites={
            "primary_domain": _first_text(
                sites_block.get("primary_domain"),
                source.get("primary_domain"),
                source.get("site_id"),
            ),
            "best_site": _first_text(
                sites_block.get("best_site"),
                source.get("best_site"),
                source.get("site_url"),
                source.get("url"),
            ),
            "best_site_status": _first_text(
                sites_block.get("best_site_status"),
                source.get("best_site_status"),
            ),
            "best_site_sources": sites_block.get("best_site_sources")
            or source.get("best_site_sources"),
            "candidate_sites": sites_block.get("candidate_sites")
            or source.get("candidate_sites"),
            "confirmed_sites": sites_block.get("confirmed_sites")
            or source.get("confirmed_sites"),
            "site_classes": sites_block.get("site_classes") or source.get("site_classes"),
            "worth_crawling": sites_block.get("worth_crawling") or source.get("worth_crawling"),
        },
        signals={
            "geo": {
                "match_status": _first_text(
                    geo_block.get("match_status"),
                    source.get("geo_match_status"),
                    geo_block.get("status"),
                ),
                "source_address": _first_text(
                    geo_block.get("source_address"),
                    source.get("geo_source_address"),
                ),
                "matched_settlement": _first_text(
                    geo_block.get("matched_settlement"),
                    source.get("geo_matched_settlement"),
                ),
                "matched_municipality": _first_text(
                    geo_block.get("matched_municipality"),
                    source.get("geo_matched_municipality"),
                ),
                "matched_region": _first_text(
                    geo_block.get("matched_region"),
                    source.get("geo_matched_region"),
                ),
                "geo_bucket": _first_text(
                    geo_block.get("geo_bucket"),
                    source.get("geo_bucket"),
                ),
                "geo_weight": _first_present(
                    geo_block.get("geo_weight"),
                    source.get("geo_weight"),
                ),
                "inside_outer_polygon": _first_present(
                    geo_block.get("inside_outer_polygon"),
                    source.get("geo_inside_outer_polygon"),
                ),
                "inside_inner_polygon": _first_present(
                    geo_block.get("inside_inner_polygon"),
                    source.get("geo_inside_inner_polygon"),
                ),
                "distance_to_moscow_km": _first_present(
                    geo_block.get("distance_to_moscow_km"),
                    source.get("geo_distance_to_moscow_km"),
                ),
                "candidate_count": _first_present(
                    geo_block.get("candidate_count"),
                    source.get("geo_candidate_count"),
                ),
                "variant_count": _first_present(
                    geo_block.get("variant_count"),
                    source.get("geo_variant_count"),
                ),
                "distance_spread_km": _first_present(
                    geo_block.get("distance_spread_km"),
                    source.get("geo_distance_spread_km"),
                ),
                "ambiguous_geo_buckets": geo_block.get("ambiguous_geo_buckets")
                or source.get("geo_ambiguous_buckets")
                or geo_block.get("geo_buckets"),
            },
            "naming": {
                "signal_status": _first_text(
                    naming_block.get("signal_status"),
                    source.get("naming_signal_status"),
                    naming_block.get("status"),
                ),
                "source_name": _first_text(
                    naming_block.get("source_name"),
                    source.get("naming_source_name"),
                ),
                "verdict": _first_text(
                    naming_block.get("verdict"),
                    source.get("naming_verdict"),
                    "none",
                ),
                "risk_weight": _first_present(
                    naming_block.get("risk_weight"),
                    source.get("naming_risk_weight"),
                ),
                "matched_markers": naming_block.get("matched_markers")
                or source.get("naming_matched_markers"),
                "reason_codes": naming_block.get("reason_codes")
                or source.get("naming_reason_codes"),
            },
        },
    )


def company_profile_to_dict(profile: CompanyProfile | Mapping[str, Any] | None) -> dict[str, Any]:
    if isinstance(profile, CompanyProfile):
        return asdict(profile)
    return asdict(company_profile_from_dict(profile))

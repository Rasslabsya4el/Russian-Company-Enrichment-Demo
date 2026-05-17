from __future__ import annotations

import hashlib
import importlib
import json
from collections.abc import Mapping, Sequence
from typing import Any
from urllib.parse import urlparse

from .handoff import AGGREGATOR_SITE_HANDOFF_KEY, normalize_stage_handoff_state
from .handoff_queue import (
    QUEUE_STATUS_READY,
    stage_handoff_fingerprint,
    synchronize_stage_handoff_pickup_state,
)


WORK_STATUS_PENDING = "pending"
WORK_STATUS_ACKED = "acked"
AGGREGATOR_SITE_EXECUTION_BOUNDARY = "aggregator_site_candidate_selection"
DEEP_PARSE_EXECUTION_BOUNDARY = "deep_site_parse"
DEEP_PARSE_WORK_UNIT_SURFACE_KEY = "deep_parse"
EXPLICIT_EXECUTION_BOUNDARIES = frozenset(
    {
        AGGREGATOR_SITE_EXECUTION_BOUNDARY,
        DEEP_PARSE_EXECUTION_BOUNDARY,
    }
)
EXPLICIT_EXECUTION_BOUNDARY = AGGREGATOR_SITE_EXECUTION_BOUNDARY
STAGE_WORK_UNIT_SURFACE_KEYS = (
    AGGREGATOR_SITE_HANDOFF_KEY,
    DEEP_PARSE_WORK_UNIT_SURFACE_KEY,
)
_ALLOWED_WORK_STATUSES = frozenset({WORK_STATUS_PENDING, WORK_STATUS_ACKED})
_COMPOUND_REGISTERED_DOMAIN_SUFFIXES = frozenset(
    {"com.ru", "net.ru", "org.ru", "gov.ru", "edu.ru", "spb.ru", "msk.ru", "co.uk", "com.tr"}
)


def _core():
    return importlib.import_module("company_enrichment_core")


def build_stage_work_unit_state() -> dict[str, Any]:
    return {
        surface_key: {"companies": {}}
        for surface_key in STAGE_WORK_UNIT_SURFACE_KEYS
    }


def normalize_stage_work_unit_state(payload: Any) -> dict[str, Any]:
    root = dict(payload) if isinstance(payload, Mapping) else {}
    normalized_state = build_stage_work_unit_state()
    for surface_key in STAGE_WORK_UNIT_SURFACE_KEYS:
        surface_payload = root.get(surface_key)
        companies_payload = surface_payload.get("companies") if isinstance(surface_payload, Mapping) else {}
        if not isinstance(companies_payload, Mapping):
            continue
        for raw_inn in sorted(companies_payload.keys(), key=lambda item: str(item)):
            normalized_company = _normalize_company_work_unit(raw_inn, companies_payload[raw_inn])
            if normalized_company is None:
                continue
            canonical_surface_key = _stage_work_unit_surface_key(normalized_company)
            normalized_state[canonical_surface_key]["companies"][normalized_company["inn"]] = normalized_company
    return normalized_state


def normalize_explicit_stage_execution_state(payload: Any) -> dict[str, Any]:
    normalized_state = normalize_stage_work_unit_state(payload)
    explicit_state = build_stage_work_unit_state()
    for surface_key in STAGE_WORK_UNIT_SURFACE_KEYS:
        companies = explicit_state[surface_key]["companies"]
        for inn, company in normalized_state[surface_key]["companies"].items():
            if not _is_explicit_execution_work_unit(company):
                continue
            companies[inn] = _clone_json_value(company)
    return explicit_state


def materialize_pickup_ready_stage_work_units(
    work_unit_state: Mapping[str, Any] | None,
    ready_handoffs: Sequence[Mapping[str, Any]],
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    normalized_state = normalize_stage_work_unit_state(work_unit_state)
    companies = normalized_state[AGGREGATOR_SITE_HANDOFF_KEY]["companies"]
    materialized_work_units: list[dict[str, Any]] = []

    for handoff_company in _ordered_handoff_companies(ready_handoffs):
        normalized_handoff = _normalize_handoff_company(handoff_company.get("inn"), handoff_company)
        if normalized_handoff is None:
            continue
        inn = normalized_handoff["inn"]
        existing_work_unit = companies.get(inn)
        if _is_explicit_execution_work_unit(existing_work_unit):
            continue
        next_work_unit = _build_company_work_unit(normalized_handoff)
        if existing_work_unit is not None and existing_work_unit["handoff_fingerprint"] == next_work_unit["handoff_fingerprint"]:
            continue
        companies[inn] = next_work_unit
        materialized_work_units.append(_clone_json_value(next_work_unit))
    return materialized_work_units, normalized_state


def upsert_stage_work_unit(
    work_unit_state: Mapping[str, Any] | None,
    *,
    inn: str,
    row_index: Any,
    work_unit_payload: Mapping[str, Any] | None,
    execution_boundary: str = EXPLICIT_EXECUTION_BOUNDARY,
    last_message_ts: Any = "",
    fingerprint_scope: Any = "",
) -> tuple[bool, dict[str, Any], dict[str, Any]]:
    normalized_state = normalize_stage_work_unit_state(work_unit_state)
    companies = normalized_state[_stage_work_unit_surface_key_for_execution_boundary(execution_boundary)]["companies"]
    normalized_inn = str(inn or "").strip()
    normalized_payload = _normalize_work_unit_payload(normalized_inn, work_unit_payload)
    if not normalized_inn or normalized_payload is None:
        return False, normalized_state, {}

    next_work_unit = _build_explicit_company_work_unit(
        inn=normalized_inn,
        row_index=row_index,
        last_message_ts=last_message_ts,
        work_unit_payload=normalized_payload,
        execution_boundary=execution_boundary,
        fingerprint_scope=fingerprint_scope,
    )
    existing_work_unit = companies.get(normalized_inn)
    if _should_preserve_existing_stage_work_unit(existing_work_unit, next_work_unit):
        return False, normalized_state, _clone_json_value(existing_work_unit)

    companies[normalized_inn] = next_work_unit
    return True, normalized_state, _clone_json_value(next_work_unit)


def upsert_explicit_stage_execution_work_unit(
    execution_state: Mapping[str, Any] | None,
    work_unit: Mapping[str, Any] | None,
) -> tuple[bool, dict[str, Any]]:
    normalized_state = normalize_explicit_stage_execution_state(execution_state)
    normalized_work_unit = _normalize_company_work_unit(
        (work_unit or {}).get("inn"),
        work_unit,
    )
    if normalized_work_unit is None or not _is_explicit_execution_work_unit(normalized_work_unit):
        return False, normalized_state

    companies = normalized_state[_stage_work_unit_surface_key(normalized_work_unit)]["companies"]
    inn = normalized_work_unit["inn"]
    if _should_preserve_existing_stage_work_unit(companies.get(inn), normalized_work_unit):
        return False, normalized_state
    if companies.get(inn) == normalized_work_unit:
        return False, normalized_state
    companies[inn] = normalized_work_unit
    return True, normalized_state


def synchronize_stage_work_unit_state(
    work_unit_state: Mapping[str, Any] | None,
    handoff_state: Mapping[str, Any] | None,
    pickup_state: Mapping[str, Any] | None,
    explicit_execution_state: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    normalized_work_units = normalize_stage_work_unit_state(work_unit_state)
    normalized_explicit_execution = normalize_explicit_stage_execution_state(explicit_execution_state)
    normalized_handoffs = normalize_stage_handoff_state(handoff_state)
    normalized_pickups = synchronize_stage_handoff_pickup_state(pickup_state, normalized_handoffs)
    handoff_companies = normalized_handoffs[AGGREGATOR_SITE_HANDOFF_KEY]["companies"]
    pickup_companies = normalized_pickups[AGGREGATOR_SITE_HANDOFF_KEY]["companies"]
    existing_aggregator_companies = normalized_work_units[AGGREGATOR_SITE_HANDOFF_KEY]["companies"]
    explicit_aggregator_companies = normalized_explicit_execution[AGGREGATOR_SITE_HANDOFF_KEY]["companies"]
    existing_deep_parse_companies = normalized_work_units[DEEP_PARSE_WORK_UNIT_SURFACE_KEY]["companies"]
    explicit_deep_parse_companies = normalized_explicit_execution[DEEP_PARSE_WORK_UNIT_SURFACE_KEY]["companies"]

    next_aggregator_companies = {
        inn: _clone_json_value(company)
        for inn, company in existing_aggregator_companies.items()
    }
    for inn in _ordered_company_inns(handoff_companies):
        repaired_company = _repair_company_work_unit(
            handoff_company=handoff_companies[inn],
            pickup_company=pickup_companies.get(inn),
            existing_work_unit=existing_aggregator_companies.get(inn),
            explicit_execution_work_unit=explicit_aggregator_companies.get(inn),
        )
        if repaired_company is not None:
            next_aggregator_companies[inn] = repaired_company
    for inn, company in explicit_aggregator_companies.items():
        next_aggregator_companies[inn] = _clone_json_value(company)
    next_deep_parse_companies = {
        inn: _clone_json_value(company)
        for inn, company in existing_deep_parse_companies.items()
    }
    for inn, company in explicit_deep_parse_companies.items():
        next_deep_parse_companies[inn] = _clone_json_value(company)
    return {
        AGGREGATOR_SITE_HANDOFF_KEY: {"companies": next_aggregator_companies},
        DEEP_PARSE_WORK_UNIT_SURFACE_KEY: {"companies": next_deep_parse_companies},
    }


def pending_stage_work_units(
    work_unit_state: Mapping[str, Any] | None,
    *,
    execution_boundary: str | None = None,
    inns: Sequence[str] | None = None,
) -> list[dict[str, Any]]:
    normalized_state = normalize_stage_work_unit_state(work_unit_state)
    allowed_inns = {
        str(inn or "").strip()
        for inn in (inns or [])
        if str(inn or "").strip()
    }
    pending_items: list[dict[str, Any]] = []
    surface_keys = (
        (_stage_work_unit_surface_key_for_execution_boundary(execution_boundary),)
        if execution_boundary in EXPLICIT_EXECUTION_BOUNDARIES
        else STAGE_WORK_UNIT_SURFACE_KEYS
    )
    for surface_key in surface_keys:
        companies = normalized_state[surface_key]["companies"]
        for inn in _ordered_company_inns(companies):
            company = companies[inn]
            if allowed_inns and inn not in allowed_inns:
                continue
            if company["work_status"] != WORK_STATUS_PENDING:
                continue
            if execution_boundary is not None and str(company.get("execution_boundary") or "") != execution_boundary:
                continue
            pending_items.append(_clone_json_value(company))
    return pending_items


def acknowledge_stage_work_unit(
    work_unit_state: Mapping[str, Any] | None,
    *,
    inn: str,
    handoff_fingerprint: str,
    acknowledged_at: Any = "",
) -> tuple[bool, dict[str, Any]]:
    normalized_state = normalize_stage_work_unit_state(work_unit_state)
    normalized_inn = str(inn or "").strip()
    normalized_fingerprint = str(handoff_fingerprint or "").strip()
    if not normalized_inn or not normalized_fingerprint:
        return False, normalized_state

    surface_key, company = _locate_stage_work_unit(
        normalized_state,
        inn=normalized_inn,
        handoff_fingerprint=normalized_fingerprint,
    )
    if company is None or surface_key is None:
        return False, normalized_state
    if company["work_status"] == WORK_STATUS_ACKED:
        return False, normalized_state

    company["work_status"] = WORK_STATUS_ACKED
    company["acknowledged_at"] = str(acknowledged_at or "")
    return True, normalized_state


def merge_stage_work_unit_private_state(
    work_unit_state: Mapping[str, Any] | None,
    *,
    inn: str,
    handoff_fingerprint: str,
    private_state_patch: Mapping[str, Any] | None,
    last_message_ts: Any | None = None,
) -> tuple[bool, dict[str, Any], dict[str, Any]]:
    normalized_state = normalize_stage_work_unit_state(work_unit_state)
    normalized_inn = str(inn or "").strip()
    normalized_fingerprint = str(handoff_fingerprint or "").strip()
    if not normalized_inn or not normalized_fingerprint or not isinstance(private_state_patch, Mapping):
        return False, normalized_state, {}

    surface_key, company = _locate_stage_work_unit(
        normalized_state,
        inn=normalized_inn,
        handoff_fingerprint=normalized_fingerprint,
    )
    if company is None or surface_key is None or not _is_explicit_execution_work_unit(company):
        return False, normalized_state, {}

    next_private_state = _normalize_private_state(company.get("private_state"))
    changed = False
    for raw_key, raw_value in private_state_patch.items():
        key = str(raw_key)
        value = _clone_json_value(raw_value)
        if next_private_state.get(key) == value:
            continue
        next_private_state[key] = value
        changed = True

    next_company = _clone_json_value(company)
    next_company["private_state"] = next_private_state
    if last_message_ts is not None:
        normalized_last_message_ts = str(last_message_ts or "")
        if str(next_company.get("last_message_ts") or "") != normalized_last_message_ts:
            next_company["last_message_ts"] = normalized_last_message_ts
            changed = True

    if not changed:
        return False, normalized_state, _clone_json_value(company)

    normalized_state[surface_key]["companies"][normalized_inn] = next_company
    return True, normalized_state, _clone_json_value(next_company)


def _normalize_company_work_unit(raw_inn: Any, payload: Any) -> dict[str, Any] | None:
    company_payload = dict(payload) if isinstance(payload, Mapping) else {}
    inn = str(company_payload.get("inn") or raw_inn or "").strip()
    if not inn:
        return None

    normalized_work_unit = _normalize_work_unit_payload(inn, company_payload.get("work_unit"))
    if normalized_work_unit is None:
        return None

    work_status = str(company_payload.get("work_status") or "").strip().lower()
    if work_status not in _ALLOWED_WORK_STATUSES:
        work_status = WORK_STATUS_PENDING
    execution_boundary = str(company_payload.get("execution_boundary") or "")
    stored_fingerprint = str(company_payload.get("handoff_fingerprint") or "").strip()
    handoff_fingerprint = stage_work_unit_fingerprint(
        normalized_work_unit,
        execution_boundary=execution_boundary if execution_boundary in EXPLICIT_EXECUTION_BOUNDARIES else "",
    )
    if execution_boundary in EXPLICIT_EXECUTION_BOUNDARIES and stored_fingerprint:
        handoff_fingerprint = stored_fingerprint
    return {
        "inn": inn,
        "row_index": _normalize_row_index(company_payload.get("row_index", normalized_work_unit.get("row_index"))),
        "execution_boundary": execution_boundary,
        "work_status": work_status,
        "handoff_fingerprint": handoff_fingerprint,
        "last_message_ts": str(company_payload.get("last_message_ts", normalized_work_unit.get("last_message_ts", "")) or ""),
        "acknowledged_at": str(company_payload.get("acknowledged_at") or ""),
        "private_state": _normalize_private_state(company_payload.get("private_state")),
        "work_unit": normalized_work_unit,
    }


def _build_company_work_unit(handoff_company: Mapping[str, Any]) -> dict[str, Any]:
    normalized_handoff = _normalize_handoff_company(handoff_company.get("inn"), handoff_company)
    if normalized_handoff is None:
        raise ValueError("work unit requires normalized handoff payload")
    return _build_stage_work_unit_record(
        execution_boundary="",
        work_unit_payload=normalized_handoff,
        row_index=normalized_handoff.get("row_index"),
        last_message_ts=normalized_handoff.get("last_message_ts", ""),
    )


def _build_explicit_company_work_unit(
    *,
    inn: str,
    row_index: Any,
    last_message_ts: Any,
    work_unit_payload: Mapping[str, Any],
    execution_boundary: str = EXPLICIT_EXECUTION_BOUNDARY,
    fingerprint_scope: Any = "",
) -> dict[str, Any]:
    normalized_payload = _normalize_work_unit_payload(inn, work_unit_payload)
    if normalized_payload is None:
        raise ValueError("explicit work unit requires mapping payload")
    normalized_row_index = _normalize_row_index(row_index or normalized_payload.get("row_index"))
    if normalized_row_index > 0:
        normalized_payload["row_index"] = normalized_row_index
    return _build_stage_work_unit_record(
        execution_boundary=execution_boundary,
        work_unit_payload=normalized_payload,
        row_index=normalized_row_index,
        last_message_ts=last_message_ts,
        fingerprint_scope=fingerprint_scope,
    )


def _build_stage_work_unit_record(
    *,
    execution_boundary: str,
    work_unit_payload: Mapping[str, Any],
    row_index: Any,
    last_message_ts: Any,
    fingerprint_scope: Any = "",
) -> dict[str, Any]:
    normalized_payload = _normalize_work_unit_payload(work_unit_payload.get("inn"), work_unit_payload)
    if normalized_payload is None:
        raise ValueError("work unit requires normalized payload")
    return {
        "inn": normalized_payload["inn"],
        "row_index": _normalize_row_index(row_index or normalized_payload.get("row_index")),
        "execution_boundary": execution_boundary,
        "work_status": WORK_STATUS_PENDING,
        "handoff_fingerprint": stage_work_unit_fingerprint(
            normalized_payload,
            execution_boundary=execution_boundary,
            scope=fingerprint_scope,
        ),
        "last_message_ts": str(last_message_ts or normalized_payload.get("last_message_ts") or ""),
        "acknowledged_at": "",
        "work_unit": _clone_json_value(normalized_payload),
    }


def _normalize_handoff_company(raw_inn: Any, payload: Any) -> dict[str, Any] | None:
    normalized_handoffs = normalize_stage_handoff_state(
        {AGGREGATOR_SITE_HANDOFF_KEY: {"companies": {str(raw_inn or ""): payload}}}
    )
    companies = normalized_handoffs[AGGREGATOR_SITE_HANDOFF_KEY]["companies"]
    if not companies:
        return None
    first_inn = next(iter(companies))
    return _clone_json_value(companies[first_inn])


def _normalize_work_unit_payload(raw_inn: Any, payload: Any) -> dict[str, Any] | None:
    if not isinstance(payload, Mapping):
        return None
    normalized_payload = _repair_json_value(_clone_json_value(payload))
    inn = str(normalized_payload.get("inn") or raw_inn or "").strip()
    if not inn:
        return None
    normalized_payload["inn"] = inn
    if "row_index" in normalized_payload:
        normalized_payload["row_index"] = _normalize_row_index(normalized_payload.get("row_index"))
    return normalized_payload


def stage_work_unit_fingerprint(
    work_unit_payload: Mapping[str, Any],
    *,
    execution_boundary: str = "",
    scope: Any = "",
) -> str:
    normalized_execution_boundary = str(execution_boundary or "")
    normalized_scope = str(scope or "")
    normalized_payload: Any = _clone_json_value(work_unit_payload)
    if normalized_execution_boundary in EXPLICIT_EXECUTION_BOUNDARIES:
        normalized_payload = _build_explicit_stage_work_unit_identity(
            normalized_payload,
            execution_boundary=normalized_execution_boundary,
            scope=normalized_scope,
        )
    elif normalized_execution_boundary or normalized_scope:
        normalized_payload = {
            "execution_boundary": normalized_execution_boundary,
            "scope": normalized_scope,
            "work_unit": normalized_payload,
        }
    payload = json.dumps(
        normalized_payload,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _ordered_handoff_companies(handoffs: Sequence[Mapping[str, Any]]) -> list[Mapping[str, Any]]:
    normalized_handoffs: list[Mapping[str, Any]] = []
    for handoff_company in handoffs:
        if not isinstance(handoff_company, Mapping):
            continue
        normalized_handoffs.append(handoff_company)
    return sorted(
        normalized_handoffs,
        key=lambda company: (
            _normalize_row_index(company.get("row_index")),
            str(company.get("inn") or ""),
        ),
    )


def _ordered_company_inns(companies: Mapping[str, Mapping[str, Any]]) -> list[str]:
    return sorted(
        companies.keys(),
        key=lambda inn: (
            _normalize_row_index(companies[inn].get("row_index")),
            str(inn),
        ),
    )


def _repair_company_work_unit(
    *,
    handoff_company: Mapping[str, Any],
    pickup_company: Mapping[str, Any] | None,
    existing_work_unit: Mapping[str, Any] | None,
    explicit_execution_work_unit: Mapping[str, Any] | None = None,
) -> dict[str, Any] | None:
    if _is_explicit_execution_work_unit(explicit_execution_work_unit):
        return _clone_json_value(explicit_execution_work_unit)
    if _is_explicit_execution_work_unit(existing_work_unit):
        return _clone_json_value(existing_work_unit)
    if pickup_company is None:
        return None
    handoff_fingerprint = stage_handoff_fingerprint(handoff_company)
    if pickup_company.get("queue_status") != QUEUE_STATUS_READY:
        return None
    if str(pickup_company.get("picked_up_handoff_fingerprint") or "") != handoff_fingerprint:
        return None
    if (
        existing_work_unit is not None
        and str(existing_work_unit.get("handoff_fingerprint") or "") == handoff_fingerprint
    ):
        return None
    return _build_company_work_unit(handoff_company)


def _is_explicit_execution_work_unit(payload: Mapping[str, Any] | None) -> bool:
    return str((payload or {}).get("execution_boundary") or "") in EXPLICIT_EXECUTION_BOUNDARIES


def _stage_work_unit_surface_key(payload: Mapping[str, Any] | None) -> str:
    execution_boundary = str((payload or {}).get("execution_boundary") or "")
    return _stage_work_unit_surface_key_for_execution_boundary(execution_boundary)


def _stage_work_unit_surface_key_for_execution_boundary(execution_boundary: str | None) -> str:
    if str(execution_boundary or "") == DEEP_PARSE_EXECUTION_BOUNDARY:
        return DEEP_PARSE_WORK_UNIT_SURFACE_KEY
    return AGGREGATOR_SITE_HANDOFF_KEY


def _locate_stage_work_unit(
    work_unit_state: Mapping[str, Any],
    *,
    inn: str,
    handoff_fingerprint: str,
) -> tuple[str | None, dict[str, Any] | None]:
    for surface_key in STAGE_WORK_UNIT_SURFACE_KEYS:
        company = work_unit_state[surface_key]["companies"].get(inn)
        if not isinstance(company, dict):
            continue
        if str(company.get("handoff_fingerprint") or "") != handoff_fingerprint:
            continue
        return surface_key, company
    return None, None


def _same_stage_work_unit(
    current_work_unit: Mapping[str, Any] | None,
    next_work_unit: Mapping[str, Any],
) -> bool:
    if current_work_unit is None:
        return False
    return (
        str(current_work_unit.get("execution_boundary") or "") == str(next_work_unit.get("execution_boundary") or "")
        and str(current_work_unit.get("handoff_fingerprint") or "") == str(next_work_unit.get("handoff_fingerprint") or "")
    )


def _should_preserve_existing_stage_work_unit(
    current_work_unit: Mapping[str, Any] | None,
    next_work_unit: Mapping[str, Any],
) -> bool:
    if not _same_stage_work_unit(current_work_unit, next_work_unit):
        return False
    if str(next_work_unit.get("execution_boundary") or "") == DEEP_PARSE_EXECUTION_BOUNDARY:
        return True
    return _clone_json_value((current_work_unit or {}).get("work_unit")) == _clone_json_value(
        next_work_unit.get("work_unit")
    )


def _build_explicit_stage_work_unit_identity(
    work_unit_payload: Mapping[str, Any],
    *,
    execution_boundary: str,
    scope: str,
) -> dict[str, Any]:
    host_keys = _explicit_stage_work_unit_host_keys(
        work_unit_payload,
        execution_boundary=execution_boundary,
    )
    return {
        "execution_boundary": execution_boundary,
        "scope": scope,
        "company_key": _explicit_stage_work_unit_company_key(work_unit_payload),
        "host_keys": host_keys,
        "domain_keys": _explicit_stage_work_unit_domain_keys(host_keys),
    }


def _explicit_stage_work_unit_company_key(work_unit_payload: Mapping[str, Any]) -> str:
    inn = str(work_unit_payload.get("inn") or "").strip()
    if inn:
        return f"inn:{inn}"
    company_name = str(work_unit_payload.get("company_name") or "").strip().lower()
    if company_name:
        return f"name:{company_name}"
    row_index = _normalize_row_index(work_unit_payload.get("row_index"))
    if row_index > 0:
        return f"row:{row_index}"
    return ""


def _explicit_stage_work_unit_host_keys(
    work_unit_payload: Mapping[str, Any],
    *,
    execution_boundary: str,
) -> list[str]:
    host_keys: list[str] = []
    seen_hosts: set[str] = set()
    for site_value in _iter_explicit_stage_identity_site_values(
        work_unit_payload,
        execution_boundary=execution_boundary,
    ):
        host = _stage_identity_host(site_value)
        if not host or host in seen_hosts:
            continue
        seen_hosts.add(host)
        host_keys.append(host)
    return sorted(host_keys)


def _explicit_stage_work_unit_domain_keys(host_keys: Sequence[str]) -> list[str]:
    domain_keys: list[str] = []
    seen_domains: set[str] = set()
    for host in host_keys:
        domain = _guess_registered_domain(host)
        if not domain or domain in seen_domains:
            continue
        seen_domains.add(domain)
        domain_keys.append(domain)
    return sorted(domain_keys)


def _iter_explicit_stage_identity_site_values(
    work_unit_payload: Mapping[str, Any],
    *,
    execution_boundary: str,
) -> list[Any]:
    if execution_boundary == DEEP_PARSE_EXECUTION_BOUNDARY:
        deep_parse_sites = work_unit_payload.get("deep_parse_sites")
        if isinstance(deep_parse_sites, Sequence) and not isinstance(deep_parse_sites, (str, bytes)):
            values = [item for item in deep_parse_sites if item not in (None, "")]
            if values:
                return values
    candidate_sites = work_unit_payload.get("candidate_sites")
    if isinstance(candidate_sites, Sequence) and not isinstance(candidate_sites, (str, bytes)):
        return [item for item in candidate_sites if item not in (None, "")]
    return []


def _stage_identity_host(site_value: Any) -> str:
    raw_site_value = site_value
    if isinstance(site_value, Mapping):
        raw_site_value = (
            site_value.get("site_url")
            or site_value.get("final_url")
            or site_value.get("url")
        )
    normalized_site = str(raw_site_value or "").strip()
    if not normalized_site:
        return ""
    if normalized_site.startswith("//"):
        normalized_site = "https:" + normalized_site
    if "://" not in normalized_site:
        normalized_site = "https://" + normalized_site
    parsed = urlparse(normalized_site)
    return str(parsed.hostname or "").strip().lower().strip(".")


def _guess_registered_domain(host: str) -> str:
    normalized_host = str(host or "").strip().lower().strip(".")
    if not normalized_host:
        return ""
    parts = normalized_host.split(".")
    if len(parts) <= 2:
        return normalized_host
    suffix = ".".join(parts[-2:])
    if suffix in _COMPOUND_REGISTERED_DOMAIN_SUFFIXES and len(parts) >= 3:
        return ".".join(parts[-3:])
    return ".".join(parts[-2:])


def _normalize_private_state(value: Any) -> dict[str, Any]:
    if not isinstance(value, Mapping):
        return {}
    return _repair_json_value(_clone_json_value(value))


def _repair_json_value(value: Any) -> Any:
    return _core().repair_output_value(value)


def _clone_json_value(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {str(key): _clone_json_value(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_clone_json_value(item) for item in value]
    if isinstance(value, tuple):
        return [_clone_json_value(item) for item in value]
    return value


def _normalize_row_index(value: Any) -> int:
    try:
        normalized = int(value)
    except (TypeError, ValueError):
        return 0
    return normalized if normalized > 0 else 0


__all__ = [
    "AGGREGATOR_SITE_EXECUTION_BOUNDARY",
    "DEEP_PARSE_WORK_UNIT_SURFACE_KEY",
    "DEEP_PARSE_EXECUTION_BOUNDARY",
    "STAGE_WORK_UNIT_SURFACE_KEYS",
    "WORK_STATUS_ACKED",
    "WORK_STATUS_PENDING",
    "acknowledge_stage_work_unit",
    "build_stage_work_unit_state",
    "materialize_pickup_ready_stage_work_units",
    "merge_stage_work_unit_private_state",
    "normalize_explicit_stage_execution_state",
    "normalize_stage_work_unit_state",
    "pending_stage_work_units",
    "stage_work_unit_fingerprint",
    "synchronize_stage_work_unit_state",
    "upsert_explicit_stage_execution_work_unit",
    "upsert_stage_work_unit",
]

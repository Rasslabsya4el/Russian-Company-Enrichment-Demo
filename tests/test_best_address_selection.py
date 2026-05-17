from __future__ import annotations

import app.discovery.address_resolution as address_resolution
import company_enrichment_core as core


def _base_result() -> dict[str, object]:
    return {
        "row_index": 1,
        "inn": "7700000000",
        "company_name": 'ООО "Тестовый завод"',
        "status": "completed",
        "trusted_contacts": {
            "phones": [],
            "emails": [],
            "websites": [],
            "addresses": [],
        },
        "merged_contacts": {
            "phones": [],
            "emails": [],
            "websites": [],
            "addresses": [],
        },
        "validated_sites": [],
        "domain_resolution": {},
        "sources": {},
        "profile": {},
    }


def _set_source_addresses(
    result: dict[str, object],
    addresses_by_source: dict[str, list[str]],
) -> dict[str, object]:
    result["sources"] = {
        source_name: {
            "status": "success",
            "addresses": [{"value": value} for value in values],
        }
        for source_name, values in addresses_by_source.items()
    }
    return result


def _source_result_with_addresses(source_name: str, values: list[str]) -> core.SourceResult:
    return core.SourceResult(
        source=source_name,
        status="success",
        addresses=[
            core.ContactItem(
                value=value,
                source_url="",
                kind="addresses",
            )
            for value in values
        ],
    )


def test_best_address_prefers_full_address_over_region_only_surface() -> None:
    full_address = "404106, Волгоградская обл., г. Волжский, пр-кт Ленина, 308К"
    region_only = "404106, Волгоградская обл"
    result = _set_source_addresses(
        _base_result(),
        {
            "checko": [full_address],
            "rusprofile": [region_only],
        },
    )
    result["trusted_contacts"]["addresses"] = [region_only]
    result["merged_contacts"]["addresses"] = [region_only, full_address]

    best = core.choose_best_company_contacts(result)
    assert best["best_address"] == full_address
    assert best["best_address_sources"] == "checko"

    profile = core.assemble_company_profile_payload(result)
    assert profile["contacts"]["best_address"]["value"] == full_address
    assert profile["signals"]["geo"]["match_status"] == "matched"


def test_best_address_prefers_richer_locality_evidence_over_truncated_surface() -> None:
    truncated_address = "д. Марьино, тер. КПО Поварово"
    full_address = "д. Марьино, тер. КПО Поварово, г. Солнечногорск"
    result = _set_source_addresses(
        _base_result(),
        {
            "checko": [full_address],
            "spark": [truncated_address],
        },
    )
    result["trusted_contacts"]["addresses"] = [truncated_address]
    result["merged_contacts"]["addresses"] = [truncated_address, full_address]

    best = core.choose_best_company_contacts(result)
    assert best["best_address"] == full_address


def test_best_address_prefers_normal_address_over_garbage_metrics_string() -> None:
    garbage_address = "г. Москва, производительность 5000 т/год, товарный бетон марки М300"
    normal_address = "г. Москва, ул. Тверская, д. 7"
    result = _set_source_addresses(
        _base_result(),
        {
            "checko": [normal_address],
            "zachestnyibiznes": [garbage_address],
        },
    )
    result["trusted_contacts"]["addresses"] = [garbage_address]
    result["merged_contacts"]["addresses"] = [garbage_address, normal_address]

    best = core.choose_best_company_contacts(result)
    assert best["best_address"] == normal_address


def test_best_address_fix_does_not_change_phone_or_email_selection() -> None:
    result = _base_result()
    consensus_phone = "+7 495 000-00-00"
    fallback_phone = "+7 495 111-11-11"
    consensus_email = "sales@example.com"
    fallback_email = "info@example.com"
    result["sources"] = {
        "checko": {
            "status": "success",
            "phones": [{"value": consensus_phone}],
            "emails": [{"value": consensus_email}],
            "addresses": [],
        },
        "rusprofile": {
            "status": "success",
            "phones": [{"value": consensus_phone}, {"value": fallback_phone}],
            "emails": [{"value": fallback_email}],
            "addresses": [],
        },
        "spark": {
            "status": "success",
            "phones": [],
            "emails": [{"value": consensus_email}],
            "addresses": [],
        },
    }
    result["trusted_contacts"]["phones"] = [consensus_phone]
    result["trusted_contacts"]["emails"] = [consensus_email]
    result["merged_contacts"]["phones"] = [fallback_phone, consensus_phone]
    result["merged_contacts"]["emails"] = [fallback_email, consensus_email]

    best = core.choose_best_company_contacts(result)
    assert best["best_phone"] == consensus_phone
    assert best["best_email"] == consensus_email


def test_address_enrichment_keeps_raw_full_address_and_oracle_fields(monkeypatch) -> None:
    raw_address = "  141401, Московская обл., г. Химки, ул. Заводская, д. 1  "
    sanitized_address = "141401, Московская обл., г. Химки, ул. Заводская, д. 1"

    def fake_lookup(address: str) -> core.GeoLookupResult:
        assert address == sanitized_address
        return core.GeoLookupResult(
            match_status="matched",
            source_address=address,
            matched_settlement="Химки",
            matched_municipality="городской округ Химки",
            matched_region="Московская область",
            candidate_count=1,
        )

    monkeypatch.setattr(address_resolution, "lookup_settlement", fake_lookup)

    enriched = address_resolution.enrich_address_candidates(
        [raw_address],
        sanitizer=core.sanitize_address_candidate,
    )

    assert len(enriched) == 1
    assert enriched[0] == address_resolution.AddressEnrichment(
        raw_value=raw_address.strip(),
        sanitized_value=sanitized_address,
        lookup_status="matched",
        matched_settlement="Химки",
        matched_municipality="городской округ Химки",
        matched_region="Московская область",
        candidate_count=1,
    )


def test_best_address_prefers_richer_locality_oracle_when_surface_is_tied(monkeypatch) -> None:
    coarse_address = "Московская область, город Королев"
    locality_address = "Московская область, город Химки"
    result = _set_source_addresses(
        _base_result(),
        {
            "checko": [coarse_address],
            "spark": [locality_address],
        },
    )
    result["trusted_contacts"]["addresses"] = [coarse_address]
    result["merged_contacts"]["addresses"] = [coarse_address, locality_address]

    oracle_results = {
        coarse_address: core.GeoLookupResult(
            match_status="matched",
            source_address=coarse_address,
            matched_settlement="Королев",
            matched_municipality="",
            matched_region="Московская область",
            candidate_count=1,
        ),
        locality_address: core.GeoLookupResult(
            match_status="matched",
            source_address=locality_address,
            matched_settlement="Химки",
            matched_municipality="городской округ Химки",
            matched_region="Московская область",
            candidate_count=1,
        ),
    }

    monkeypatch.setattr(address_resolution, "lookup_settlement", lambda address: oracle_results[address])

    best = core.choose_best_company_contacts(result)
    assert best["best_address"] == locality_address

    profile = core.assemble_company_profile_payload(result)
    assert profile["contacts"]["best_address"]["value"] == locality_address


def test_sanitize_address_candidate_keeps_real_single_letter_suffix_variants() -> None:
    assert (
        core.sanitize_address_candidate(
            "404106, Волгоградская область, город Волжский, пр-кт им Ленина, д.308 к"
        )
        == "404106, Волгоградская область, город Волжский, пр-кт им Ленина, д.308к"
    )
    assert (
        core.sanitize_address_candidate(
            "191144, город Санкт-Петербург, 7-Я Советская ул, д. 44 литера Б, помещ. 6- н"
        )
        == "191144, город Санкт-Петербург, 7-Я Советская ул, д. 44 литера Б, помещ. 6-н"
    )
    assert (
        core.sanitize_address_candidate(
            "117246, город Москва, Научный проезд, д. 19, помещ. 6 д"
        )
        == "117246, город Москва, Научный проезд, д. 19, помещ. 6д"
    )


def test_real_fixture_3435037788_prefers_clean_address_after_merge_normalization() -> None:
    spark_address = "404106, Волгоградская обл., г. Волжский, проспект Им Ленина, д. 308К На карте"
    rusprofile_address = "404106, Волгоградская область, город Волжский, пр-кт им Ленина, д.308 к"
    list_org_address = "404106, ВОЛГОГРАДСКАЯ ОБЛАСТЬ, Г. ВОЛЖСКИЙ, ПР-КТ ИМ ЛЕНИНА, Д.308 К"
    source_results = {
        "spark": _source_result_with_addresses("spark", [spark_address]),
        "rusprofile": _source_result_with_addresses("rusprofile", [rusprofile_address]),
        "list_org": _source_result_with_addresses("list_org", [list_org_address]),
    }
    row = core.RowInput(
        row_index=39,
        inn="3435037788",
        company_name='ООО "ВРХТ РТД"',
    )

    merged = core.merge_contacts(source_results, row)
    assert merged["addresses"] == [
        "404106, Волгоградская обл., г. Волжский, проспект Им Ленина, д. 308К",
        "404106, Волгоградская область, город Волжский, пр-кт им Ленина, д.308к",
        "404106, ВОЛГОГРАДСКАЯ ОБЛАСТЬ, Г. ВОЛЖСКИЙ, ПР-КТ ИМ ЛЕНИНА, Д.308К",
    ]

    result = _set_source_addresses(
        _base_result(),
        {
            "spark": [spark_address],
            "rusprofile": [rusprofile_address],
            "list_org": [list_org_address],
        },
    )
    result["inn"] = "3435037788"
    result["company_name"] = 'ООО "ВРХТ РТД"'
    result["merged_contacts"]["addresses"] = merged["addresses"]
    result["trusted_contacts"]["addresses"] = core.build_trusted_contacts(row, source_results, merged, [])["addresses"]

    best = core.choose_best_company_contacts(result)
    assert best["best_address"] == "404106, Волгоградская область, город Волжский, пр-кт им Ленина, д.308к"

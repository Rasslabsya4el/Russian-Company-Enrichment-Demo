from __future__ import annotations

import pytest

import company_enrichment_core as core


REGRESSION_ADDRESSES = (
    "119192, г. Москва, ул. Винницкая, д. 4",
    "196084, г. Санкт-Петербург, ул. Цветочная, д. 18, литер А",
    "141401, Московская обл., г. Химки, ул. Заводская, д. 1, к. Б",
)

VALID_BUILDING_SUFFIX_ADDRESS = "117246, г. Москва, Научный проезд, д. 17, стр. 3З"


def _source_result(source_name: str, address: str) -> core.SourceResult:
    return core.SourceResult(
        source=source_name,
        status="success",
        addresses=[
            core.ContactItem(
                value=address,
                source_url=f"https://{source_name}.example.test/company",
                kind="address",
            )
        ],
    )


def _result_payload(
    source_results: dict[str, core.SourceResult],
    merged_contacts: dict[str, list[str]],
    trusted_contacts: dict[str, list[str]],
) -> dict[str, object]:
    return {
        "row_index": 1,
        "inn": "7700000000",
        "company_name": 'ООО "Тестовый завод"',
        "status": "completed",
        "sources": {
            source_name: {
                "status": source.status,
                "addresses": [{"value": item.value} for item in source.addresses],
            }
            for source_name, source in source_results.items()
        },
        "trusted_contacts": trusted_contacts,
        "merged_contacts": merged_contacts,
        "validated_sites": [],
        "domain_resolution": {},
        "candidate_sites": [],
        "site_probes": [],
        "lead_cards": [],
        "profile": {},
    }


@pytest.mark.parametrize("address", [*REGRESSION_ADDRESSES, VALID_BUILDING_SUFFIX_ADDRESS])
def test_sanitize_address_candidate_keeps_valid_suffixes_and_word_boundaries(address: str) -> None:
    assert core.sanitize_address_candidate(address) == address


@pytest.mark.parametrize("address", REGRESSION_ADDRESSES)
def test_address_survives_merge_trusted_best_and_geo_path(address: str) -> None:
    row = core.RowInput(
        row_index=1,
        inn="7700000000",
        company_name='ООО "Тестовый завод"',
    )
    source_results = {
        "list_org": _source_result("list_org", address),
        "spark": _source_result("spark", address),
    }

    merged_contacts = core.merge_contacts(source_results, row)
    assert merged_contacts["addresses"] == [address]

    trusted_contacts = core.build_trusted_contacts(row, source_results, merged_contacts, [])
    assert trusted_contacts["addresses"] == [address]

    result = _result_payload(source_results, merged_contacts, trusted_contacts)

    best = core.choose_best_company_contacts(result)
    assert best["best_address"] == address

    geo_signal = core.build_geo_signal_payload(result, best_address=best["best_address"])
    assert geo_signal["source_address"] == address


def test_sanitize_address_candidate_still_cuts_real_junk() -> None:
    with_phone_suffix = "115114, г. Москва, Дербеневская наб., д. 7, стр. 2, телефон +7 (495) 000-00-00"
    bank_details_line = "115114, г. Москва, р/с 40702810900000000001"

    assert core.sanitize_address_candidate(with_phone_suffix) == "115114, г. Москва, Дербеневская наб., д. 7, стр. 2"
    assert core.sanitize_address_candidate(bank_details_line) == ""

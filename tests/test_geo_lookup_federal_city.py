from __future__ import annotations

import pytest

from app.discovery.geo_lookup import GeoLookupIndex, lookup_settlement, record_from_payload


SPB_CITY = "\u0421\u0430\u043d\u043a\u0442-\u041f\u0435\u0442\u0435\u0440\u0431\u0443\u0440\u0433"
MOSCOW_CITY = "\u041c\u043e\u0441\u043a\u0432\u0430"
SPB_REAL_ADDRESSES = (
    "192148, \u0433. \u0421\u0430\u043d\u043a\u0442-\u041f\u0435\u0442\u0435\u0440\u0431\u0443\u0440\u0433, "
    "\u0443\u043b. \u041f\u0438\u043d\u0435\u0433\u0438\u043d\u0430, \u0434. 4 \u041d\u0430 \u043a\u0430\u0440\u0442\u0435",
    "198206, \u0413.\u0421\u0410\u041d\u041a\u0422-\u041f\u0415\u0422\u0415\u0420\u0411\u0423\u0420\u0413, "
    "\u0428. \u041f\u0415\u0422\u0415\u0420\u0413\u041e\u0424\u0421\u041a\u041e\u0415, \u0414. 73, \u041a. 9 "
    "\u041b\u0418\u0422\u0415\u0420 \u0410\u0411, \u041f\u041e\u041c\u0415\u0429. 1-\u041d",
)


def _record(
    *,
    settlement: str,
    municipality: str,
    region: str,
    settlement_type: str,
    full_name: str,
    distance_to_moscow_km: float = 634.18,
) -> object:
    return record_from_payload(
        {
            "settlement": settlement,
            "municipality": municipality,
            "region": region,
            "settlement_type": settlement_type,
            "full_name": full_name,
            "aliases": [settlement, full_name],
            "geo_bucket": "outside",
            "geo_weight": 0,
            "inside_outer_polygon": False,
            "inside_inner_polygon": False,
            "distance_to_moscow_km": distance_to_moscow_km,
            "variant_count": 1,
            "distance_spread_km": 0.0,
        }
    )


def _spb_federal_city_index() -> GeoLookupIndex:
    return GeoLookupIndex(
        [
            _record(
                settlement=SPB_CITY,
                municipality="\u0412\u044b\u0431\u043e\u0440\u0433\u0441\u043a\u0438\u0439",
                region=SPB_CITY,
                settlement_type="\u0433",
                full_name=f"\u0433 {SPB_CITY}",
            ),
            _record(
                settlement=SPB_CITY,
                municipality="\u041f\u0443\u0448\u043a\u0438\u043d\u0441\u043a\u0438\u0439",
                region=SPB_CITY,
                settlement_type="\u0433",
                full_name=f"\u0433 {SPB_CITY}",
            ),
            _record(
                settlement="\u041f\u0430\u0440\u0433\u043e\u043b\u043e\u0432\u043e",
                municipality="\u0412\u044b\u0431\u043e\u0440\u0433\u0441\u043a\u0438\u0439",
                region=SPB_CITY,
                settlement_type="\u043f",
                full_name="\u043f \u041f\u0430\u0440\u0433\u043e\u043b\u043e\u0432\u043e",
            ),
            _record(
                settlement="\u0428\u0443\u0448\u0430\u0440\u044b",
                municipality="\u041f\u0443\u0448\u043a\u0438\u043d\u0441\u043a\u0438\u0439",
                region=SPB_CITY,
                settlement_type="\u043f",
                full_name="\u043f \u0428\u0443\u0448\u0430\u0440\u044b",
            ),
        ]
    )


@pytest.mark.parametrize("address", SPB_REAL_ADDRESSES)
def test_real_spb_addresses_no_longer_return_federal_city_ambiguity(address: str) -> None:
    result = lookup_settlement(address)

    assert result.match_status == "matched"
    assert result.matched_settlement == SPB_CITY
    assert result.matched_region == SPB_CITY
    assert result.matched_municipality == ""
    assert result.geo_bucket == "outside"
    assert result.candidate_count == 1


@pytest.mark.parametrize(
    ("address", "expected_settlement", "expected_municipality"),
    [
        (
            "194362, \u0433. \u0421\u0430\u043d\u043a\u0442-\u041f\u0435\u0442\u0435\u0440\u0431\u0443\u0440\u0433, "
            "\u0432\u043d.\u0442\u0435\u0440.\u0433. \u043f\u043e\u0441\u0435\u043b\u043e\u043a "
            "\u041f\u0430\u0440\u0433\u043e\u043b\u043e\u0432\u043e, \u043f\u043e\u0441. \u041f\u0430\u0440\u0433\u043e\u043b\u043e\u0432\u043e",
            "\u041f\u0430\u0440\u0433\u043e\u043b\u043e\u0432\u043e",
            "\u0412\u044b\u0431\u043e\u0440\u0433\u0441\u043a\u0438\u0439",
        ),
        (
            "196626, \u0433. \u0421\u0430\u043d\u043a\u0442-\u041f\u0435\u0442\u0435\u0440\u0431\u0443\u0440\u0433, "
            "\u0432\u043d.\u0442\u0435\u0440.\u0433. \u043c\u0443\u043d\u0438\u0446\u0438\u043f\u0430\u043b\u044c\u043d\u044b\u0439 "
            "\u043e\u043a\u0440\u0443\u0433 \u0428\u0443\u0448\u0430\u0440\u044b, \u043f\u043e\u0441. \u0428\u0443\u0448\u0430\u0440\u044b",
            "\u0428\u0443\u0448\u0430\u0440\u044b",
            "\u041f\u0443\u0448\u043a\u0438\u043d\u0441\u043a\u0438\u0439",
        ),
    ],
)
def test_federal_city_locality_hints_beat_generic_city_aliases(
    address: str,
    expected_settlement: str,
    expected_municipality: str,
) -> None:
    result = lookup_settlement(address, index=_spb_federal_city_index())

    assert result.match_status == "matched"
    assert result.matched_settlement == expected_settlement
    assert result.matched_municipality == expected_municipality
    assert result.matched_region == SPB_CITY
    assert result.geo_bucket == "outside"


def test_moscow_city_only_lookup_keeps_existing_core_bucket_semantics() -> None:
    result = lookup_settlement(f"\u0433 {MOSCOW_CITY}")

    assert result.match_status == "matched"
    assert result.matched_settlement == MOSCOW_CITY
    assert result.matched_municipality == "\u0413\u043e\u0440\u043e\u0434 \u041c\u043e\u0441\u043a\u0432\u0430"
    assert result.geo_bucket == "core"

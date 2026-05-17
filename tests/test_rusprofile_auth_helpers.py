from __future__ import annotations

from bs4 import BeautifulSoup

from app.sources.rusprofile import (
    detect_logged_in,
    extract_rusprofile_websites,
    merge_addresses_prefer_complete,
)


def test_detect_logged_in_from_main_info_data_user() -> None:
    html = '<div id="main_info" data-user="true"></div>'
    assert detect_logged_in(html) == (True, "true")


def test_extract_rusprofile_websites_ignores_footer_noise() -> None:
    soup = BeautifulSoup(
        """
        <div id="contacts-row">
          <a itemprop="url" href="https://company.ru/">company.ru</a>
        </div>
        <footer>
          <a href="https://baturin.ru/">baturin.ru</a>
        </footer>
        """,
        "html.parser",
    )
    assert extract_rusprofile_websites(soup) == ["https://company.ru/"]


def test_merge_addresses_prefer_complete_repairs_split_house_number() -> None:
    merged = merge_addresses_prefer_complete(
        [
            "125252, город Москва, Чапаевский пер., д.1 4",
            "125252, город Москва, Чапаевский пер.",
        ]
    )
    assert merged == ["125252, город Москва, Чапаевский пер., д.14"]

from __future__ import annotations

import json
from urllib.parse import parse_qs, urlparse

import pytest

from app.sources import bicotender


def test_url_builder_includes_public_search_contract_params() -> None:
    query = bicotender.BicotenderSearchQuery(
        inn="7701234567",
        keywords="металлолом [труба-б/у]3",
        nokeywords="строительство",
        trade_types=("2",),
        status_ids=("3",),
        on_page=20,
    )

    url = query.to_url()
    params = parse_qs(urlparse(url).query)

    assert url.startswith("https://www.bicotender.ru/tender/search/?")
    assert "%D0%BC%D0%B5%D1%82%D0%B0%D0%BB%D0%BB%D0%BE%D0%BB%D0%BE%D0%BC" in url
    assert params["submit"] == ["Искать"]
    assert params["company[inn]"] == ["7701234567"]
    assert params["keywords"] == ["металлолом [труба-б/у]3"]
    assert params["nokeywords"] == ["строительство"]
    assert params["tradeType[]"] == ["2"]
    assert params["status_id[]"] == ["3"]
    assert params["order"] == ["bcHitCountUniq DESC"]
    assert params["on_page"] == ["20"]


def test_default_query_does_not_add_trade_or_status_filters() -> None:
    query = bicotender.BicotenderSearchQuery(
        inn="7701234567",
        keywords="металлолом",
    )

    params = parse_qs(urlparse(query.to_url()).query)

    assert params["submit"] == ["Искать"]
    assert params["company[inn]"] == ["7701234567"]
    assert params["keywords"] == ["металлолом"]
    assert "tradeType[]" not in params
    assert "status_id[]" not in params


def test_explicit_trade_and_status_filters_are_serialized() -> None:
    query = bicotender.BicotenderSearchQuery(
        inn="7701234567",
        trade_types=("2",),
        status_ids=("3", "4"),
    )

    params = parse_qs(urlparse(query.to_url()).query)

    assert params["tradeType[]"] == ["2"]
    assert params["status_id[]"] == ["3", "4"]


def test_keyword_parser_preserves_bicotender_dsl_terms_and_warns_on_suspicious_group() -> None:
    parsed = bicotender.parse_bicotender_keyword_dsl(
        "[строительство жилого]2 труба-б/у лом* отходы. б/у лом < отходы [черных-металлов]2"
    )

    assert parsed.normalized_terms == (
        "[строительство жилого]2",
        "труба-б/у",
        "лом*",
        "отходы.",
        "б/у",
        "лом",
        "<",
        "отходы",
        "[черных-металлов]2",
    )
    suspicious = parsed.terms[-1]
    assert suspicious.normalized == "[черных-металлов]2"
    assert "hyphen_inside_proximity_group" in suspicious.warnings
    assert any("[черных-металлов]2" in warning for warning in parsed.warnings)


def test_load_keyword_batches_supports_accepted_artifact_shape(tmp_path) -> None:
    text = "[черных-металлов]2 металлолом лом металлолом"
    path = tmp_path / "keyword_batches.json"
    path.write_text(
        json.dumps(
            {
                "repaired_terms": ("[черных-металлов]2", "металлолом", "лом", "металлолом"),
                "batches": (
                    {
                        "id": "batch_001",
                        "text": text,
                        "char_count": len(text),
                        "term_count": 4,
                        "raw_term_indexes": (0, 1, 2, 3),
                        "theme_labels": ("metal_scrap_general",),
                    },
                ),
            },
            ensure_ascii=False,
        ),
        encoding="utf-8-sig",
    )

    batches = bicotender.load_keyword_batches_from_json(str(path))

    assert len(batches) == 1
    assert batches[0].index == 1
    assert batches[0].keywords == text
    assert batches[0].char_count == len(text)
    assert batches[0].terms == ("[черных-металлов]2", "металлолом", "лом", "металлолом")


@pytest.mark.parametrize(
    ("batch_patch", "message"),
    (
        ({"text": ""}, "no usable keyword text"),
        ({"raw_term_indexes": (0, 99)}, "outside repaired_terms"),
        ({"term_count": 1}, "term_count disagrees"),
        ({"char_count": 999}, "char_count disagrees"),
    ),
)
def test_load_keyword_batches_rejects_invalid_accepted_artifact_shape(
    tmp_path,
    batch_patch: dict[str, object],
    message: str,
) -> None:
    text = "металлолом неликвиды"
    batch = {
        "id": "batch_001",
        "text": text,
        "char_count": len(text),
        "term_count": 2,
        "raw_term_indexes": (0, 1),
    }
    batch.update(batch_patch)
    path = tmp_path / "keyword_batches.json"
    path.write_text(
        json.dumps(
            {
                "repaired_terms": ("металлолом", "неликвиды"),
                "batches": (batch,),
            },
            ensure_ascii=False,
        ),
        encoding="utf-8-sig",
    )

    with pytest.raises(ValueError, match=message):
        bicotender.load_keyword_batches_from_json(str(path))


def test_query_planner_preflights_before_keyword_batches_and_preserves_group_boundaries() -> None:
    keywords = " ".join(
        (
            "[строительство жилого]2",
            "металлолом",
            "неликвиды",
            "труба-б/у",
            "вагоны",
            "детали",
        )
    )

    plan = bicotender.build_bicotender_query_plan(
        inn="7701234567",
        positive_keywords=keywords,
        keyword_cap=35,
    )

    assert plan.preflight.kind == "inn_only_preflight"
    assert plan.preflight.query.keywords == ""
    assert [batch.kind for batch in plan.keyword_batches]
    assert all(batch.query.keywords for batch in plan.keyword_batches)
    assert all(len(batch.query.keywords) <= 35 for batch in plan.keyword_batches)
    assert "[строительство жилого]2" in plan.keyword_batches[0].terms
    assert "[строительство жилого]2" in plan.keyword_batches[0].query.keywords


def test_query_planner_skips_keyword_batches_after_zero_result_applied_preflight_unless_forced() -> None:
    preflight = bicotender.BicotenderListEvidence(query_applied=True, total_count=0, visible_count=0)

    plan = bicotender.build_bicotender_query_plan(
        inn="7701234567",
        positive_keywords="металлолом неликвиды",
        preflight_evidence=preflight,
    )
    forced = bicotender.build_bicotender_query_plan(
        inn="7701234567",
        positive_keywords="металлолом неликвиды",
        preflight_evidence=preflight,
        force_keyword_batches=True,
    )

    assert plan.keyword_batches == ()
    assert [batch.skip_reason for batch in plan.skipped_keyword_batches] == [
        "inn_only_preflight_zero_applied_results"
    ]
    assert len(forced.keyword_batches) == 1


def test_parser_confirms_echoed_query_and_extracts_visible_result_list_evidence() -> None:
    query = bicotender.BicotenderSearchQuery(
        inn="7701234567",
        keywords="металлолом",
        nokeywords="строительство",
    )
    html = """
    <html>
      <body>
        <form id="search">
          <input name="company[inn]" value="7701234567">
          <input name="keywords" value="металлолом">
          <input name="nokeywords" value="строительство">
          <input type="checkbox" name="tradeType[]" value="2" checked>
          <select name="status_id[]"><option value="3" selected>Активные</option></select>
        </form>
        <div class="summary">Найдено 2 тендера. Архив: 1</div>
        <article class="tender-card">
          <a href="/tender/1234567">Продажа металлолома и труб б/у</a>
          <span>Регион: Москва Отрасль: Металлургия Дата: 12.05.2026 Цена: 100 000 руб. Процедура: Продажа</span>
          <a href="/files/doc123.pdf">Документы</a>
        </article>
        <article class="tender-card">
          <a href="/tender/7654321">Реализация неликвидов</a>
          <span>Регион: Тула Процедура: Аукцион</span>
        </article>
      </body>
    </html>
    """

    evidence = bicotender.parse_bicotender_result_list(
        html,
        expected_query=query,
        final_url="https://www.bicotender.ru/tender/search/?x=1",
        positive_terms=("металлолом", "неликвиды"),
    )

    assert evidence.query_applied is True
    assert evidence.total_count == 2
    assert evidence.archive_count == 1
    assert evidence.visible_count == 2
    assert evidence.detail_accessed is False
    assert evidence.documents_accessed is False
    first = evidence.items[0]
    assert first.tender_id == "1234567"
    assert first.title == "Продажа металлолома и труб б/у"
    assert first.region == "Москва"
    assert first.industry == "Металлургия"
    assert first.date_text == "12.05.2026"
    assert first.price_text == "100 000 руб."
    assert first.procedure_text == "Продажа"
    assert first.document_marker is True
    assert first.detail_fetched is False
    assert first.documents_accessed is False
    assert first.evidence_quality == "list_page_only"


def test_parser_does_not_treat_global_unfiltered_surface_as_applied_query() -> None:
    query = bicotender.BicotenderSearchQuery(inn="7701234567", keywords="металлолом")
    html = """
    <html><body>
      <div class="summary">Найдено 10 тендеров</div>
      <article class="tender-card"><a href="/tender/9999">Любой тендер</a></article>
    </body></html>
    """

    evidence = bicotender.parse_bicotender_result_list(html, expected_query=query)

    assert evidence.query_applied is False
    assert evidence.total_count == 10
    assert evidence.visible_count == 1


def test_parser_ignores_live_navigation_links_and_keeps_real_tender_rows() -> None:
    query = bicotender.BicotenderSearchQuery(inn="1650032058")
    html = """
    <html>
      <body>
        <form>
          <input name="company[inn]" value="1650032058">
          <input type="checkbox" name="tradeType[]" value="2" checked>
          <select name="status_id[]"><option value="3" selected>Активные</option></select>
        </form>
        <div class="summary">Найдено 179 тендеров</div>
        <nav>
          <a href="/tender/search/?company%5Binn%5D=1650032058">+ Расширенный поиск</a>
          <a href="/mezhdunarodnye-tender/">Международные тендеры</a>
        </nav>
        <article class="tender-card">
          <span>Тендер №329295757</span>
          <a href="/masinostroenie/ooo-pzdt-servis-realizuet-vagony-poluvagony-ed-id-nabereznye-celny-tender329295757.html">
            Публичное акционерное общество КАМАЗ объявляет тендер: ООО реализует вагоны, полувагоны 7 ед.
          </a>
        </article>
      </body>
    </html>
    """

    evidence = bicotender.parse_bicotender_result_list(html, expected_query=query)

    assert evidence.query_applied is True
    assert evidence.total_count == 179
    assert evidence.visible_count == 1
    assert [(item.tender_id, item.title) for item in evidence.items] == [
        (
            "329295757",
            "Публичное акционерное общество КАМАЗ объявляет тендер: ООО реализует вагоны, полувагоны 7 ед.",
        )
    ]


def test_parser_extracts_live_like_table_rows_with_full_list_evidence() -> None:
    query = bicotender.BicotenderSearchQuery(inn="5022055500", keywords="металлолом")
    html = """
    <html>
      <body>
        <form>
          <input name="company[inn]" value="5022055500">
          <input name="keywords" value="металлолом">
        </form>
        <div class="summary">Найдено 2 тендера</div>
        <table>
          <tr class="search-result">
            <td><a href="/site/registration/Popup">Подключить тестовый доступ</a></td>
            <td>
              <span>Тендер №111111</span>
              <span class="tender-name">Реализация ломов черных металлов</span>
              <a href="/metally/realizaciia-lomov-tender111111.html">Подробнее</a>
            </td>
            <td>Период показа: 01.05.2026 - 15.05.2026</td>
            <td>Регион: Московская область</td>
            <td>Отрасли: Металлы, сырье</td>
            <td>Стоимость: 125 000 руб.</td>
            <td>Тип торгов: Продажа</td>
          </tr>
          <tr class="search-result">
            <td>
              <span>Тендер №222222</span>
              <a href="/oborudovanie/realizaciia-stanka-tender222222.html">Реализация станка б/у</a>
            </td>
            <td>Дата начала: 02.05.2026 Регион: Тула Отрасль: Машиностроение Цена: 50 000 руб. Процедура: Аукцион</td>
          </tr>
        </table>
      </body>
    </html>
    """

    evidence = bicotender.parse_bicotender_result_list(
        html,
        expected_query=query,
        positive_terms=("металлолом", "лом", "б/у", "станок"),
    )

    assert evidence.query_applied is True
    assert evidence.visible_count == 2
    first, second = evidence.items
    assert first.tender_id == "111111"
    assert first.title == "Реализация ломов черных металлов"
    assert first.detail_url.endswith("/metally/realizaciia-lomov-tender111111.html")
    assert first.date_text == "01.05.2026 - 15.05.2026"
    assert first.region == "Московская область"
    assert first.industry == "Металлы, сырье"
    assert first.price_text == "125 000 руб."
    assert first.procedure_text == "Продажа"
    assert first.matched_positive_terms == ("лом",)
    assert first.evidence_quality == "list_page_only"
    assert first.detail_fetched is False
    assert first.documents_accessed is False
    assert "Подключить тестовый доступ" not in first.title
    assert second.title == "Реализация станка б/у"
    assert second.region == "Тула"
    assert second.industry == "Машиностроение"
    assert second.price_text == "50 000 руб."
    assert second.procedure_text == "Аукцион"


def test_parser_uses_whole_live_table_row_for_positions_fields_and_phrase_relevance() -> None:
    query = bicotender.BicotenderSearchQuery(inn="5022055500", keywords="стальная-проволока труба")
    html = """
    <html>
      <body>
        <form>
          <input name="company[inn]" value="5022055500">
          <input name="keywords" value="стальная-проволока труба">
        </form>
        <div class="summary">Найдено 2 тендера</div>
        <table class="result-table">
          <tr class="search-result">
            <td><a href="/site/registration/Popup">Подключить тестовый доступ</a></td>
            <td>
              <div class="tender-title">
                <span>Тендер №323092628</span>
                <span>Поставка материалов для ремонта оборудования</span>
                <a href="/metally-metalloizdeliia/postavka-materialov-tender323092628.html">Подробнее</a>
              </div>
              <div class="positions">Позиции Проволока стальная вязальная, крепеж</div>
            </td>
            <td><div>Период показа</div><div>01.05.2026 - 15.05.2026</div></td>
            <td><div>Регион</div><div>Московская область</div></td>
            <td><div>Категория</div><div>Металлы, метизная продукция</div></td>
            <td><div>НМЦ</div><div>125 000 руб.</div></td>
            <td><div>Тип процедуры</div><div>Запрос предложений</div></td>
          </tr>
          <tr class="search-result">
            <td>
              <div class="tender-title">
                <span>Тендер №326155988</span>
                <a href="/elektrotexnika/postavka-elektrofurnitury-tender326155988.html">Поставка электрофурнитуры</a>
              </div>
              <div class="positions">Позиции Труба гофрированная ПНД, муфта, крепеж</div>
            </td>
            <td>Дата публикации 03.05.2026</td>
            <td>Регион Тула</td>
            <td>Отрасль Электротехника</td>
            <td>Цена 50 000 руб.</td>
            <td>Процедура Аукцион</td>
          </tr>
        </table>
      </body>
    </html>
    """

    evidence = bicotender.parse_bicotender_result_list(
        html,
        expected_query=query,
        positive_terms=("стальная-проволока", "труба"),
    )

    assert evidence.query_applied is True
    assert evidence.visible_count == 2
    first, second = evidence.items
    assert first.tender_id == "323092628"
    assert first.title == "Поставка материалов для ремонта оборудования"
    assert "Подключить тестовый доступ" not in first.title
    assert "Позиции Проволока стальная вязальная" in first.snippet
    assert first.date_text == "01.05.2026 - 15.05.2026"
    assert first.region == "Московская область"
    assert first.industry == "Металлы, метизная продукция"
    assert first.price_text == "125 000 руб."
    assert first.procedure_text == "Запрос предложений"
    assert first.matched_positive_terms == ("стальная-проволока",)
    assert first.evidence_quality == "list_page_only"
    assert first.detail_fetched is False
    assert first.documents_accessed is False

    assert second.tender_id == "326155988"
    assert second.title == "Поставка электрофурнитуры"
    assert "Позиции Труба гофрированная" in second.snippet
    assert second.date_text == "03.05.2026"
    assert second.region == "Тула"
    assert second.industry == "Электротехника"
    assert second.price_text == "50 000 руб."
    assert second.procedure_text == "Аукцион"
    assert second.matched_positive_terms == ("труба",)
    assert bicotender.classify_bicotender_signal(
        evidence,
        positive_terms=("стальная-проволока", "труба"),
    ).status == "visible_public_items"


def test_parser_extracts_unlabeled_live_table_granular_fields() -> None:
    query = bicotender.BicotenderSearchQuery(inn="5022055500", keywords="стальная-проволока труба")
    html = """
    <html>
      <body>
        <form>
          <input name="company[inn]" value="5022055500">
          <input name="keywords" value="стальная-проволока труба">
        </form>
        <div class="summary">Найдено 2 тендера</div>
        <table class="result-table">
          <tr class="search-result">
            <td><a href="/site/registration/Popup">Подключить тестовый доступ</a></td>
            <td>
              <span>Тендер №323092628</span>
              <a href="/metally-metalloizdeliia/postavka-materialov-tender323092628.html">
                Поставка материалов для ремонта оборудования
              </a>
              <div>Позиции Проволока стальная вязальная, крепеж</div>
            </td>
            <td>Электронный аукцион #323092628</td>
            <td>6 320 000 Руб.</td>
            <td>срок истек 05.02.2026 13.02.2026</td>
            <td>Закупки Центральный ФО / Московская область / Коломна</td>
            <td>Металлы, металлоизделия / Металлоизделия и металлоконструкции</td>
          </tr>
          <tr class="search-result">
            <td>
              <span>Тендер №326155988</span>
              <a href="/elektrotexnika/postavka-elektrofurnitury-tender326155988.html">
                Поставка электрофурнитуры
              </a>
              <div>Позиции Труба гофрированная ПНД, муфта, крепеж</div>
            </td>
            <td>Запрос предложений #326155988</td>
            <td>См. док.</td>
            <td>срок истек 19.03.2026 17.04.2026</td>
            <td>Закупки Центральный ФО / Московская область / Коломна</td>
            <td>Электротехника / Электротехнические изделия</td>
          </tr>
        </table>
      </body>
    </html>
    """

    evidence = bicotender.parse_bicotender_result_list(
        html,
        expected_query=query,
        positive_terms=("стальная-проволока", "труба"),
    )

    assert evidence.query_applied is True
    assert evidence.visible_count == 2
    first, second = evidence.items
    assert first.title == "Поставка материалов для ремонта оборудования"
    assert "Подключить тестовый доступ" not in first.title
    assert "Позиции Проволока стальная" in first.snippet
    assert first.procedure_text == "Электронный аукцион"
    assert first.price_text == "6 320 000 Руб."
    assert first.date_text == "срок истек 05.02.2026 13.02.2026"
    assert first.region == "Закупки Центральный ФО / Московская область / Коломна"
    assert first.industry == "Металлы, металлоизделия / Металлоизделия и металлоконструкции"
    assert first.matched_positive_terms == ("стальная-проволока",)
    assert first.evidence_quality == "list_page_only"
    assert first.detail_fetched is False
    assert first.documents_accessed is False

    assert second.title == "Поставка электрофурнитуры"
    assert "Позиции Труба гофрированная" in second.snippet
    assert second.procedure_text == "Запрос предложений"
    assert second.price_text == "См. док."
    assert second.date_text == "срок истек 19.03.2026 17.04.2026"
    assert second.region == "Закупки Центральный ФО / Московская область / Коломна"
    assert second.industry == "Электротехника / Электротехнические изделия"
    assert second.matched_positive_terms == ("труба",)
    assert bicotender.classify_bicotender_signal(
        evidence,
        positive_terms=("стальная-проволока", "труба"),
    ).status == "visible_public_items"


def test_match_terms_is_token_aware_for_short_scrap_terms() -> None:
    assert bicotender._match_terms("МПК Коломенский. Поставка электрофурнитуры", ("лом",)) == ()
    assert bicotender._match_terms("Дипломы и переломные конструкции", ("лом",)) == ()
    assert bicotender._match_terms("Продажа лома", ("лом",)) == ("лом",)
    assert bicotender._match_terms("Реализация ломов черных металлов", ("лом",)) == ("лом",)
    assert bicotender._match_terms("Продажа металлолома", ("металлолом",)) == ("металлолом",)
    assert bicotender._match_terms("Позиции: Проволока стальная вязальная", ("стальная-проволока",)) == (
        "стальная-проволока",
    )
    assert bicotender._match_terms("Иглы швейные из черных металлов", ("[черных-металлов]2",)) == (
        "[черных-металлов]2",
    )


def test_parser_matches_visible_list_snippet_not_company_substring() -> None:
    query = bicotender.BicotenderSearchQuery(inn="5022055500", keywords="труба")
    html = """
    <html>
      <body>
        <form>
          <input name="company[inn]" value="5022055500">
          <input name="keywords" value="труба">
        </form>
        <div class="summary">Найдено 1 тендер</div>
        <article class="tender-card">
          <span>Тендер №326155988</span>
          <a href="/elektrotexnika/postavka-elektrofurnitury-tender326155988.html">
            МПК Коломенский объявляет тендер: Поставка электрофурнитуры
          </a>
          <div class="positions">Позиции: Труба гофрированная ПНД, муфта, крепеж</div>
        </article>
      </body>
    </html>
    """

    evidence = bicotender.parse_bicotender_result_list(
        html,
        expected_query=query,
        positive_terms=("труба",),
    )

    assert evidence.visible_count == 1
    item = evidence.items[0]
    assert item.tender_id == "326155988"
    assert item.title == "МПК Коломенский объявляет тендер: Поставка электрофурнитуры"
    assert "Труба гофрированная" in item.snippet
    assert item.matched_positive_terms == ("труба",)
    assert bicotender.classify_bicotender_signal(evidence, positive_terms=("труба",)).status == "visible_public_items"


def test_parser_does_not_match_short_term_inside_company_name_without_visible_snippet() -> None:
    query = bicotender.BicotenderSearchQuery(inn="5022055500", keywords="лом")
    html = """
    <html>
      <body>
        <form>
          <input name="company[inn]" value="5022055500">
          <input name="keywords" value="лом">
        </form>
        <div class="summary">Найдено 1 тендер</div>
        <article class="tender-card">
          <span>Тендер №326155988</span>
          <a href="/elektrotexnika/postavka-elektrofurnitury-tender326155988.html">
            МПК Коломенский объявляет тендер: Поставка электрофурнитуры
          </a>
        </article>
      </body>
    </html>
    """

    evidence = bicotender.parse_bicotender_result_list(
        html,
        expected_query=query,
        positive_terms=("лом",),
    )

    assert evidence.visible_count == 1
    assert evidence.items[0].matched_positive_terms == ()
    classification = bicotender.classify_bicotender_signal(evidence, positive_terms=("лом",))
    assert classification.status == "visible_public_items"
    assert classification.reason == "applied_public_list_query_has_visible_rows"


def test_visible_off_topic_row_is_saved_without_relevance_filtering() -> None:
    query = bicotender.BicotenderSearchQuery(inn="5012034567", keywords="металлолом")
    html = """
    <html>
      <body>
        <form>
          <input name="company[inn]" value="5012034567">
          <input name="keywords" value="металлолом">
        </form>
        <div class="summary">Найдено 1 тендер</div>
        <article class="tender-card">
          <span>Тендер №328823336</span>
          <a href="/kancelyarskie-tovary/postavka-kancelyarskih-tovarov-tender328823336.html">
            Поставка канцелярских товаров
          </a>
          <div>Позиции: бумага офисная, папки, ручки шариковые</div>
        </article>
      </body>
    </html>
    """

    evidence = bicotender.parse_bicotender_result_list(
        html,
        expected_query=query,
        positive_terms=("металлолом", "лом", "труба"),
    )

    assert evidence.visible_count == 1
    assert evidence.items[0].tender_id == "328823336"
    assert evidence.items[0].title == "Поставка канцелярских товаров"
    assert evidence.items[0].matched_positive_terms == ()
    assert bicotender.classify_bicotender_signal(
        evidence,
        positive_terms=("металлолом", "лом", "труба"),
    ).status == "visible_public_items"


def test_existing_visible_positive_cases_keep_matched_term_annotations_only() -> None:
    evidence = bicotender.BicotenderListEvidence(
        query_applied=True,
        total_count=2,
        visible_count=2,
        items=(
            bicotender.BicotenderListItem(
                tender_id="111111",
                title="Продажа металлолома",
                snippet="Продажа металлолома",
            ),
            bicotender.BicotenderListItem(
                tender_id="222222",
                title="Реализация ломов черных металлов",
                snippet="Реализация ломов черных металлов",
            ),
        ),
    )

    classification = bicotender.classify_bicotender_signal(
        evidence,
        positive_terms=("металлолом", "лом"),
    )

    assert classification.status == "visible_public_items"
    assert classification.reason == "applied_public_list_query_has_visible_rows"
    assert classification.matched_tender_ids == ("111111", "222222")


def test_classifier_returns_conservative_statuses() -> None:
    positive = bicotender.BicotenderListEvidence(
        query_applied=True,
        total_count=1,
        visible_count=1,
        items=(
            bicotender.BicotenderListItem(
                tender_id="123",
                title="Продажа металлолома",
                snippet="Продажа металлолома",
            ),
        ),
    )
    no_signal = bicotender.BicotenderListEvidence(query_applied=True, total_count=0, visible_count=0)
    review = bicotender.BicotenderListEvidence(query_applied=False, total_count=5, visible_count=1)
    blocked = bicotender.BicotenderListEvidence(
        query_applied=True,
        total_count=25,
        visible_count=0,
        registration_markers=("зарегистрируйтесь",),
    )
    source_error = bicotender.BicotenderListEvidence(query_applied=True, http_status=500, errors=("boom",))

    assert bicotender.classify_bicotender_signal(positive).status == "visible_public_items"
    assert bicotender.classify_bicotender_signal(no_signal).status == "no_signal"
    assert bicotender.classify_bicotender_signal(review).status == "review"
    assert bicotender.classify_bicotender_signal(blocked).status == "blocked_public_limit"
    assert bicotender.classify_bicotender_signal(source_error).status == "source_error"


def test_classifier_does_not_block_on_login_marker_when_public_rows_are_visible() -> None:
    evidence = bicotender.BicotenderListEvidence(
        query_applied=True,
        total_count=25,
        visible_count=1,
        registration_markers=("войдите",),
        items=(
            bicotender.BicotenderListItem(
                tender_id="123456",
                title="Поставка офисной мебели",
                snippet="Поставка офисной мебели",
            ),
        ),
    )

    classification = bicotender.classify_bicotender_signal(evidence)

    assert classification.status == "visible_public_items"
    assert classification.reason == "applied_public_list_query_has_visible_rows"


def test_public_access_assessment_allows_static_captcha_marker_with_usable_rows() -> None:
    query = bicotender.BicotenderSearchQuery(inn="1650032058")
    html = """
    <html>
      <body>
        <script src="/assets/captcha-modal.js"></script>
        <div class="modal">captcha can be shown inside a dismissible login widget</div>
        <form>
          <input name="company[inn]" value="1650032058">
          <input type="checkbox" name="tradeType[]" value="2" checked>
          <select name="status_id[]"><option value="3" selected>Активные</option></select>
        </form>
        <div>Найдено 1 тендер</div>
        <article class="tender-card">
          <span>Тендер №329295757</span>
          <a href="/masinostroenie/realizuet-vagony-tender329295757.html">Реализует вагоны</a>
        </article>
      </body>
    </html>
    """
    evidence = bicotender.parse_bicotender_result_list(html, expected_query=query)

    assessment = bicotender.assess_bicotender_public_access(html, evidence)

    assert assessment.status == "usable_public_list"
    assert assessment.blocked is False
    assert assessment.captcha_marker_present is True
    assert assessment.reason == "static_access_marker_present_but_public_rows_usable"


def test_public_access_assessment_treats_static_marker_zero_rows_as_no_results() -> None:
    query = bicotender.BicotenderSearchQuery(inn="1650032058", keywords="металлолом")
    html = """
    <html>
      <body>
        <script src="/assets/captcha-modal.js"></script>
        <div class="modal">captcha can be shown inside a dismissible login widget</div>
        <form>
          <input name="company[inn]" value="1650032058">
          <input name="keywords" value="металлолом">
        </form>
        <div class="summary">Найдено 0 тендеров</div>
      </body>
    </html>
    """
    evidence = bicotender.parse_bicotender_result_list(html, expected_query=query)

    assessment = bicotender.assess_bicotender_public_access(html, evidence)

    assert evidence.query_applied is True
    assert evidence.visible_count == 0
    classification = bicotender.classify_bicotender_signal(evidence)

    assert assessment.status == "usable_public_list"
    assert assessment.blocked is False
    assert assessment.captcha_marker_present is True
    assert assessment.reason == "static_access_marker_present_but_query_applied_zero_results"
    assert classification.status == "no_signal"
    assert classification.reason == "applied_public_list_query_returned_zero_results"


def test_public_access_assessment_blocks_real_challenge_or_protected_http() -> None:
    query = bicotender.BicotenderSearchQuery(inn="1650032058")
    challenge_html = """
    <html><body>
      <h1>Подтвердите, что вы не робот</h1>
      <div>captcha</div>
    </body></html>
    """
    challenge_evidence = bicotender.parse_bicotender_result_list(challenge_html, expected_query=query)
    protected_http_evidence = bicotender.BicotenderListEvidence(
        query_applied=True,
        http_status=429,
        visible_count=1,
        items=(bicotender.BicotenderListItem(tender_id="123456", title="Реализует вагоны"),),
    )

    challenge = bicotender.assess_bicotender_public_access(challenge_html, challenge_evidence)
    protected_http = bicotender.assess_bicotender_public_access("", protected_http_evidence)

    assert challenge.status == "blocked_protected_source"
    assert challenge.blocked is True
    assert challenge.reason == "hard_challenge_or_access_denied_without_usable_public_rows"
    assert protected_http.status == "blocked_protected_source"
    assert protected_http.reason == "http_429_protected_stop"


def test_injected_fetch_runner_stops_after_clean_zero_inn_preflight_before_keyword_batches() -> None:
    calls: list[str] = []

    def fetcher(query: bicotender.BicotenderSearchQuery) -> bicotender.BicotenderFetchResponse:
        calls.append(query.keywords or "preflight")
        html = """
        <form><input name="company[inn]" value="7701234567"></form>
        <div>Найдено 0 тендеров</div>
        """
        return bicotender.BicotenderFetchResponse(html=html)

    result = bicotender.fetch_bicotender_public_signal(
        inn="7701234567",
        positive_keywords="металлолом неликвиды труба",
        fetcher=fetcher,
        keyword_cap=12,
    )

    assert calls == ["preflight"]
    assert result.classification.status == "no_public_items_by_inn"
    assert result.classification.reason == "inn_only_preflight_returned_no_usable_public_items"
    assert result.preflight.search_url
    assert result.preflight.query_kind == "inn_only_preflight"
    assert result.preflight.visible_count == 0
    assert result.batch_evidence == ()
    assert len(result.skipped_keyword_batches) == 3
    assert result.operator_summary.primary_status == "0 visible keyword items across 3 keyword batches"
    assert result.operator_summary.preflight_status == "no public items by INN"


def test_injected_fetch_runner_runs_all_keyword_batches_and_preserves_batch_evidence() -> None:
    calls: list[str] = []
    batches = (
        bicotender.BicotenderKeywordBatch(
            index=1,
            terms=("металлолом",),
            keywords="металлолом",
            char_count=len("металлолом"),
        ),
        bicotender.BicotenderKeywordBatch(
            index=2,
            terms=("труба",),
            keywords="труба",
            char_count=len("труба"),
        ),
        bicotender.BicotenderKeywordBatch(
            index=3,
            terms=("штамп",),
            keywords="штамп",
            char_count=len("штамп"),
        ),
    )

    def fetcher(query: bicotender.BicotenderSearchQuery) -> bicotender.BicotenderFetchResponse:
        calls.append(query.keywords or "preflight")
        if not query.keywords:
            html = """
            <form><input name="company[inn]" value="7701234567"></form>
            <div>Найдено 1 тендер</div>
            <article class="tender-card"><a href="/tender/555555">Продажа металлолома</a></article>
            """
            return bicotender.BicotenderFetchResponse(html=html)
        if query.keywords == "металлолом":
            return bicotender.BicotenderFetchResponse(
                html=f"""
            <form>
              <input name="company[inn]" value="7701234567">
              <input name="keywords" value="{query.keywords}">
            </form>
            <div>Найдено 1 тендер</div>
            <article class="tender-card">
              <span>Тендер №1234567</span>
              <a href="/metally/prodazha-metalloloma-tender1234567.html">Продажа металлолома</a>
              <span>Регион: Москва Отрасль: Металлургия Дата: 12.05.2026 Цена: 100 000 руб. Процедура: Продажа</span>
            </article>
            """
            )
        if query.keywords == "труба":
            return bicotender.BicotenderFetchResponse(
                html=f"""
            <form>
              <input name="company[inn]" value="7701234567">
              <input name="keywords" value="{query.keywords}">
            </form>
            <div>Найдено 0 тендеров</div>
            """
            )
        return bicotender.BicotenderFetchResponse(html="", http_status=500, error="source_timeout")

    result = bicotender.fetch_bicotender_public_signal(
        inn="7701234567",
        positive_keywords=batches,
        fetcher=fetcher,
        stop_early=True,
    )

    assert calls == ["preflight", "металлолом", "труба", "штамп"]
    assert result.stopped_early is False
    assert len(result.batch_evidence) == 3
    assert result.classification.status == "partial_source_error"
    assert result.classification.matched_batch_index is None

    first_batch = result.batch_evidence[0]
    first_params = parse_qs(urlparse(first_batch.search_url).query)
    assert first_batch.query_kind == "keyword_batch"
    assert first_batch.batch_index == 1
    assert first_batch.batch_char_count == len("металлолом")
    assert first_batch.classification_status == "visible_public_items"
    assert first_batch.access_status == "usable_public_list"
    assert first_batch.total_count == 1
    assert first_batch.visible_count == 1
    assert first_batch.parsed_item_count == 1
    assert first_params["company[inn]"] == ["7701234567"]
    assert first_params["keywords"] == ["металлолом"]

    item = first_batch.items[0]
    assert item.title == "Продажа металлолома"
    assert item.detail_url.endswith("/metally/prodazha-metalloloma-tender1234567.html")
    assert "Цена: 100 000 руб." in item.snippet
    assert item.date_text == "12.05.2026"
    assert item.region == "Москва"
    assert item.industry == "Металлургия"
    assert item.price_text == "100 000 руб."
    assert item.procedure_text == "Продажа"
    assert item.matched_positive_terms == ("металлолом",)
    assert item.evidence_quality == "list_page_only"
    assert item.detail_fetched is False
    assert item.documents_accessed is False

    assert result.batch_evidence[1].classification_status == "no_signal"
    assert result.batch_evidence[1].total_count == 0
    assert result.batch_evidence[2].classification_status == "source_error"
    assert result.batch_evidence[2].errors == ("http_status=500", "source_timeout")

    summary = result.operator_summary
    assert summary.primary_status == "partial source error with 1 visible keyword item across 3 keyword batches"
    assert summary.preflight_status == "INN preflight: 1 visible public item of 1 total"
    assert summary.technical_internal_status == "partial_source_error"
    assert summary.source_state == "source_issue"
    assert [batch.primary_status for batch in summary.batches] == [
        "batch 1: 1 visible item",
        "batch 2: 0 visible items",
        "batch 3: 0 visible items",
    ]
    assert summary.batches[0].search_url == first_batch.search_url
    assert summary.batches[0].matched_positive_tender_ids == ("1234567",)
    assert summary.batches[1].technical_internal_status == "no_signal"
    assert summary.batches[2].technical_internal_status == "source_error"
    assert summary.batches[2].source_state == "source_issue"
    assert "source_timeout" in summary.batches[2].access_note
    primary_statuses = (summary.primary_status, *(batch.primary_status for batch in summary.batches))
    for status in primary_statuses:
        assert "has_relevant_trade_signal" not in status
        assert "review" not in status
        assert "source_error" not in status
        assert "partial_source_error" not in status


def test_injected_fetch_runner_reports_no_keyword_items_after_clean_zero_batches() -> None:
    calls: list[str] = []
    batches = (
        bicotender.BicotenderKeywordBatch(index=1, terms=("металлолом",), keywords="металлолом", char_count=10),
        bicotender.BicotenderKeywordBatch(index=2, terms=("труба",), keywords="труба", char_count=5),
        bicotender.BicotenderKeywordBatch(index=3, terms=("штамп",), keywords="штамп", char_count=5),
    )

    def fetcher(query: bicotender.BicotenderSearchQuery) -> bicotender.BicotenderFetchResponse:
        calls.append(query.keywords or "preflight")
        if not query.keywords:
            html = """
            <form><input name="company[inn]" value="7701234567"></form>
            <div>Найдено 1 тендер</div>
            <article class="tender-card"><a href="/tender/555555">Продажа металлолома</a></article>
            """
        else:
            html = f"""
            <form>
              <input name="company[inn]" value="7701234567">
              <input name="keywords" value="{query.keywords}">
            </form>
            <div>Найдено 0 тендеров</div>
            """
        return bicotender.BicotenderFetchResponse(html=html)

    result = bicotender.fetch_bicotender_public_signal(
        inn="7701234567",
        positive_keywords=batches,
        fetcher=fetcher,
    )

    assert calls == ["preflight", "металлолом", "труба", "штамп"]
    assert result.classification.status == "no_keyword_items_after_inn_preflight"
    assert result.classification.reason == "all_keyword_batches_returned_clean_zero_results_after_inn_preflight"
    assert result.preflight.classification_status == "visible_public_items"
    assert [batch.classification_status for batch in result.batch_evidence] == ["no_signal", "no_signal", "no_signal"]
    assert result.operator_summary.primary_status == "0 visible keyword items across 3 keyword batches"
    assert [batch.primary_status for batch in result.operator_summary.batches] == [
        "batch 1: 0 visible items",
        "batch 2: 0 visible items",
        "batch 3: 0 visible items",
    ]


def test_injected_fetch_runner_reports_partial_source_error_when_preflight_rows_and_batch_error_coexist() -> None:
    batches = (
        bicotender.BicotenderKeywordBatch(index=1, terms=("металлолом",), keywords="металлолом", char_count=10),
        bicotender.BicotenderKeywordBatch(index=2, terms=("штамп",), keywords="штамп", char_count=5),
    )

    def fetcher(query: bicotender.BicotenderSearchQuery) -> bicotender.BicotenderFetchResponse:
        if not query.keywords:
            html = """
            <form><input name="company[inn]" value="7701234567"></form>
            <div>Найдено 1 тендер</div>
            <article class="tender-card"><a href="/tender/555555">Поставка электрофурнитуры</a></article>
            """
            return bicotender.BicotenderFetchResponse(html=html)
        if query.keywords == "металлолом":
            html = """
            <form>
              <input name="company[inn]" value="7701234567">
              <input name="keywords" value="металлолом">
            </form>
            <div>Найдено 0 тендеров</div>
            """
            return bicotender.BicotenderFetchResponse(html=html)
        return bicotender.BicotenderFetchResponse(html="", http_status=500, error="source_timeout")

    result = bicotender.fetch_bicotender_public_signal(
        inn="7701234567",
        positive_keywords=batches,
        fetcher=fetcher,
    )

    assert result.classification.status == "partial_source_error"
    assert result.classification.reason == "usable_keyword_batch_evidence_with_one_or_more_source_errors"
    assert result.preflight.visible_count == 1
    assert [batch.classification_status for batch in result.batch_evidence] == ["no_signal", "source_error"]
    summary = result.operator_summary
    assert summary.primary_status == "partial source error with 0 visible keyword items across 2 keyword batches"
    assert summary.preflight_status == "INN preflight: 1 visible public item of 1 total"
    assert summary.technical_internal_status == "partial_source_error"
    assert summary.source_state == "source_issue"


def test_injected_fetch_runner_fails_closed_on_hard_challenge_without_rows() -> None:
    def fetcher(query: bicotender.BicotenderSearchQuery) -> bicotender.BicotenderFetchResponse:
        html = """
        <html><body>
          <h1>Подтвердите, что вы не робот</h1>
          <div>captcha</div>
        </body></html>
        """
        return bicotender.BicotenderFetchResponse(html=html)

    result = bicotender.fetch_bicotender_public_signal(
        inn="7701234567",
        positive_keywords="металлолом",
        fetcher=fetcher,
    )

    assert result.classification.status == "source_error"
    assert result.preflight.errors == ("hard_challenge_or_access_denied_without_usable_public_rows",)
    assert result.batch_evidence == ()


def test_injected_fetch_runner_treats_static_marker_zero_rows_as_no_public_items() -> None:
    def fetcher(query: bicotender.BicotenderSearchQuery) -> bicotender.BicotenderFetchResponse:
        html = """
        <html><body>
          <script src="/assets/captcha-modal.js"></script>
          <div class="modal">captcha can be shown inside a dismissible login widget</div>
          <form><input name="company[inn]" value="7701234567"></form>
          <div>Найдено 0 тендеров</div>
        </body></html>
        """
        return bicotender.BicotenderFetchResponse(html=html)

    result = bicotender.fetch_bicotender_public_signal(
        inn="7701234567",
        positive_keywords="металлолом",
        fetcher=fetcher,
    )

    assert result.preflight.query_applied is True
    assert result.preflight.visible_count == 0
    assert result.preflight.errors == ()
    assert result.preflight.access_status == "usable_public_list"
    assert result.preflight.access_reason == "static_access_marker_present_but_query_applied_zero_results"
    assert result.preflight.classification_status == "no_signal"
    assert result.preflight.classification_reason == "applied_public_list_query_returned_zero_results"
    assert result.classification.status == "no_public_items_by_inn"
    assert result.classification.reason == "inn_only_preflight_returned_no_usable_public_items"
    assert result.batch_evidence == ()
    assert result.operator_summary.source_state == "ok"

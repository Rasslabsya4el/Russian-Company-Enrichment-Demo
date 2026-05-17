from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass, field, replace
from datetime import datetime, timezone
from typing import Any
from urllib.parse import urlencode, urljoin, urlparse

from bs4 import BeautifulSoup, Tag

import company_enrichment_core as core
from .base import BaseSource


BICOTENDER_SOURCE_NAME = "bicotender"
BICOTENDER_OPERATOR_SCHEMA_VERSION = "bicotender_public_list.v2"
BICOTENDER_AVAILABILITY_KEY = "trade_signal"
BICOTENDER_PROFILE_SIGNAL_KEY = "bicotender_public_list"
BICOTENDER_SEARCH_URL = "https://www.bicotender.ru/tender/search/"
BICOTENDER_SUBMIT_VALUE = "Искать"
BICOTENDER_DEFAULT_ORDER = "bcHitCountUniq DESC"
BICOTENDER_DEFAULT_TRADE_TYPES: tuple[str, ...] = ()
BICOTENDER_DEFAULT_STATUS_IDS: tuple[str, ...] = ()
BICOTENDER_DEFAULT_KEYWORD_CAP = 2400
BICOTENDER_EVIDENCE_QUALITY = "list_page_only"
BICOTENDER_PUBLIC_LIST_BOUNDARY_NOTE = (
    "Bicotender public-list evidence only; tender detail pages and documents were not fetched."
)
BICOTENDER_RELEVANCE_MODE = "no_filter_visible_public_list_passthrough"
BICOTENDER_RELEVANT_COUNT_COMPAT_NOTE = (
    "deprecated_compat_equals_visible_public_list_count_no_relevance_filter"
)

BICOTENDER_STATUS_VISIBLE_ITEMS = "visible_public_items"
BICOTENDER_STATUS_NO_SIGNAL = "no_signal"
BICOTENDER_STATUS_REVIEW = "review"
BICOTENDER_STATUS_BLOCKED_PUBLIC_LIMIT = "blocked_public_limit"
BICOTENDER_STATUS_SOURCE_ERROR = "source_error"
BICOTENDER_STATUS_PARTIAL_SOURCE_ERROR = "partial_source_error"
BICOTENDER_STATUS_NO_PUBLIC_ITEMS_BY_INN = "no_public_items_by_inn"
BICOTENDER_STATUS_NO_KEYWORD_ITEMS_AFTER_INN_PREFLIGHT = "no_keyword_items_after_inn_preflight"

BICOTENDER_PUBLIC_ACCESS_USABLE = "usable_public_list"
BICOTENDER_PUBLIC_ACCESS_BLOCKED = "blocked_protected_source"
BICOTENDER_PUBLIC_ACCESS_REVIEW = "review"

_TOKEN_RE = re.compile(r"[0-9a-zа-яё]+(?:/[0-9a-zа-яё]+)?", re.IGNORECASE)
_SHORT_TOKEN_FORMS: dict[str, tuple[str, ...]] = {
    "лом": ("лом", "лома", "ломов", "ломом", "ломы", "ломе", "ломах", "ломами"),
}
_DATE_RE = re.compile(r"\b\d{1,2}\.\d{1,2}\.\d{4}\b")
_PRICE_RE = re.compile(
    r"\b(?:\d{1,3}(?:[\s\u00a0]\d{3})+|\d+)(?:[,.]\d+)?\s*(?:руб\.?|₽)(?=$|\s|[),.;])",
    re.IGNORECASE,
)
_PROCEDURE_MARKERS = (
    "электронный аукцион",
    "запрос предложений",
    "запрос котировок",
    "аукцион",
    "конкурс",
    "торги",
    "редукцион",
    "продажа",
)
_REGION_MARKERS = (
    " фо",
    "область",
    "край",
    "республика",
    "округ",
    "автоном",
    "город",
    " г ",
)
_CELL_SERVICE_ONLY_TITLES = (
    "подключить тестовый доступ",
    "тестовый доступ",
    "расширенный поиск",
    "международные тендеры",
    "войти",
    "зарегистрироваться",
    "подробнее",
)

_QUERY_LABELS = (
    "Регион",
    "Регион поставки",
    "Отрасль",
    "Отрасли",
    "Категория",
    "Категории",
    "Дата",
    "Дата публикации",
    "Дата начала",
    "Дата окончания",
    "Период",
    "Период показа",
    "Окончание",
    "Начало",
    "Срок выполнения",
    "Цена",
    "Сумма",
    "Стоимость",
    "НМЦ",
    "Процедура",
    "Тип процедуры",
    "Тип торгов",
    "Тип",
    "Вид процедуры",
    "Статус",
    "Позиция",
    "Позиции",
    "Документы",
    "Файлы",
)
_SERVICE_LINK_TITLES = (
    "подключить тестовый доступ",
    "тестовый доступ",
    "расширенный поиск",
    "международные тендеры",
    "см. док",
    "документы",
    "файлы",
    "подробнее",
    "войти",
    "зарегистрироваться",
)
_DEFAULT_POSITIVE_SIGNAL_TERMS = (
    "металлолом",
    "лом",
    "отход",
    "неликвид",
    "б/у",
    "вагон",
    "труба",
    "детал",
    "тмц",
    "мтр",
    "демонтаж",
    "скрап",
    "scrap",
    "surplus",
)
_REGISTRATION_MARKERS = (
    "зарегистрируйтесь",
    "зарегистрированным пользователям",
    "после регистрации",
    "для просмотра необходима регистрация",
    "войдите",
    "только для зарегистрированных",
)
_CAPTCHA_MARKERS = (
    "captcha",
    "recaptcha",
    "g-recaptcha",
    "hcaptcha",
    "капча",
    "введите символы",
    "подтвердите что вы не робот",
    "подтвердите, что вы не робот",
)
_HARD_CHALLENGE_MARKERS = (
    "подтвердите что вы не робот",
    "подтвердите, что вы не робот",
    "введите символы",
)
_HARD_ACCESS_DENIED_MARKERS = (
    "доступ запрещен",
    "доступ ограничен",
    "access denied",
    "forbidden",
    "для просмотра необходима регистрация",
    "только для зарегистрированных",
    "необходима авторизация",
)


@dataclass(frozen=True)
class BicotenderSearchQuery:
    inn: str
    keywords: str = ""
    nokeywords: str = ""
    trade_types: tuple[str, ...] = BICOTENDER_DEFAULT_TRADE_TYPES
    status_ids: tuple[str, ...] = BICOTENDER_DEFAULT_STATUS_IDS
    order: str = BICOTENDER_DEFAULT_ORDER
    on_page: int | None = None

    def param_pairs(self) -> list[tuple[str, str]]:
        inn = _digits_only(self.inn)
        if not inn:
            raise ValueError("Bicotender query requires one INN")

        pairs: list[tuple[str, str]] = [
            ("submit", BICOTENDER_SUBMIT_VALUE),
            ("company[inn]", inn),
        ]
        if self.keywords:
            pairs.append(("keywords", self.keywords))
        if self.nokeywords:
            pairs.append(("nokeywords", self.nokeywords))
        for trade_type in self.trade_types:
            pairs.append(("tradeType[]", str(trade_type)))
        for status_id in self.status_ids:
            pairs.append(("status_id[]", str(status_id)))
        if self.order:
            pairs.append(("order", self.order))
        if self.on_page is not None:
            if self.on_page < 1 or self.on_page > 50:
                raise ValueError("Bicotender on_page must stay in a conservative 1..50 range")
            pairs.append(("on_page", str(self.on_page)))
        return pairs

    def params_map(self) -> dict[str, tuple[str, ...]]:
        return _pairs_to_map(self.param_pairs())

    def to_url(self) -> str:
        return f"{BICOTENDER_SEARCH_URL}?{urlencode(self.param_pairs())}"

    def query_hash(self) -> str:
        payload = json.dumps(self.param_pairs(), ensure_ascii=False, separators=(",", ":"))
        return hashlib.sha256(payload.encode("utf-8")).hexdigest()


@dataclass(frozen=True)
class BicotenderKeywordTerm:
    raw: str
    normalized: str
    warnings: tuple[str, ...] = ()


@dataclass(frozen=True)
class BicotenderKeywordParseResult:
    terms: tuple[BicotenderKeywordTerm, ...]
    warnings: tuple[str, ...] = ()

    @property
    def normalized_terms(self) -> tuple[str, ...]:
        return tuple(term.normalized for term in self.terms)

    @property
    def keywords(self) -> str:
        return " ".join(self.normalized_terms)


@dataclass(frozen=True)
class BicotenderKeywordBatch:
    index: int
    terms: tuple[str, ...]
    keywords: str
    char_count: int
    warnings: tuple[str, ...] = ()


@dataclass(frozen=True)
class BicotenderPlannedQuery:
    kind: str
    query: BicotenderSearchQuery
    batch_index: int | None = None
    batch_char_count: int | None = None
    terms: tuple[str, ...] = ()
    skipped: bool = False
    skip_reason: str = ""


@dataclass(frozen=True)
class BicotenderQueryPlan:
    preflight: BicotenderPlannedQuery
    keyword_batches: tuple[BicotenderPlannedQuery, ...]
    skipped_keyword_batches: tuple[BicotenderPlannedQuery, ...] = ()
    warnings: tuple[str, ...] = ()


@dataclass(frozen=True)
class BicotenderListItem:
    tender_id: str = ""
    title: str = ""
    detail_url: str = ""
    snippet: str = ""
    region: str = ""
    industry: str = ""
    date_text: str = ""
    price_text: str = ""
    procedure_text: str = ""
    registration_marker: bool = False
    detail_marker: bool = False
    document_marker: bool = False
    matched_positive_terms: tuple[str, ...] = ()
    matched_negative_terms: tuple[str, ...] = ()
    evidence_quality: str = BICOTENDER_EVIDENCE_QUALITY
    detail_fetched: bool = False
    documents_accessed: bool = False


@dataclass(frozen=True)
class _BicotenderItemFields:
    region: str = ""
    industry: str = ""
    date_text: str = ""
    price_text: str = ""
    procedure_text: str = ""


@dataclass(frozen=True)
class BicotenderListEvidence:
    source: str = "bicotender"
    access_mode: str = "public_no_login"
    queried_at: str = ""
    http_status: int | None = None
    final_url: str = BICOTENDER_SEARCH_URL
    search_url: str = ""
    query_kind: str = ""
    batch_index: int | None = None
    batch_char_count: int | None = None
    classification_status: str = ""
    classification_reason: str = ""
    access_status: str = ""
    access_reason: str = ""
    request_params: Mapping[str, tuple[str, ...]] = field(default_factory=dict)
    query_hash: str = ""
    query_applied: bool = False
    echoed_query: Mapping[str, tuple[str, ...]] = field(default_factory=dict)
    total_count: int | None = None
    archive_count: int | None = None
    visible_count: int = 0
    parsed_item_count: int = 0
    registration_markers: tuple[str, ...] = ()
    items: tuple[BicotenderListItem, ...] = ()
    detail_accessed: bool = False
    documents_accessed: bool = False
    evidence_quality: str = BICOTENDER_EVIDENCE_QUALITY
    errors: tuple[str, ...] = ()


@dataclass(frozen=True)
class BicotenderSignalClassification:
    status: str
    reason: str
    evidence_quality: str = BICOTENDER_EVIDENCE_QUALITY
    matched_tender_ids: tuple[str, ...] = ()
    matched_batch_index: int | None = None


@dataclass(frozen=True)
class BicotenderFetchResponse:
    html: str
    http_status: int | None = 200
    final_url: str = BICOTENDER_SEARCH_URL
    error: str = ""
    transport_status: str = ""
    proxy_mode: str = ""
    proxy_label: str = ""
    proxy_id: str = ""


@dataclass(frozen=True)
class BicotenderPublicSignalRun:
    preflight: BicotenderListEvidence
    batch_evidence: tuple[BicotenderListEvidence, ...]
    classification: BicotenderSignalClassification
    skipped_keyword_batches: tuple[BicotenderPlannedQuery, ...] = ()
    stopped_early: bool = False

    @property
    def operator_summary(self) -> "BicotenderOperatorSummary":
        return summarize_bicotender_operator_status(self)


@dataclass(frozen=True)
class BicotenderOperatorBatchStatus:
    batch_index: int | None
    primary_status: str
    visible_count: int
    matched_positive_count: int = 0
    search_url: str = ""
    source_state: str = ""
    access_note: str = ""
    technical_internal_status: str = ""
    technical_internal_reason: str = ""
    matched_positive_tender_ids: tuple[str, ...] = ()


@dataclass(frozen=True)
class BicotenderOperatorSummary:
    primary_status: str
    visible_count: int
    keyword_batch_count: int
    preflight_status: str
    preflight_visible_count: int
    preflight_total_count: int | None = None
    matched_positive_count: int = 0
    source_state: str = ""
    access_note: str = ""
    technical_internal_status: str = ""
    technical_internal_reason: str = ""
    batches: tuple[BicotenderOperatorBatchStatus, ...] = ()


@dataclass(frozen=True)
class BicotenderPublicAccessAssessment:
    status: str
    reason: str
    http_status: int | None = None
    query_applied: bool = False
    visible_count: int = 0
    total_count: int | None = None
    captcha_marker_present: bool = False
    login_marker_present: bool = False
    registration_redirect: bool = False
    evidence_quality: str = BICOTENDER_EVIDENCE_QUALITY

    @property
    def blocked(self) -> bool:
        return self.status == BICOTENDER_PUBLIC_ACCESS_BLOCKED


def parse_bicotender_keyword_dsl(text: str) -> BicotenderKeywordParseResult:
    raw_terms = _split_keyword_dsl(text)
    terms: list[BicotenderKeywordTerm] = []
    all_warnings: list[str] = []
    for raw in raw_terms:
        normalized = _normalize_keyword_term(raw)
        warnings = _keyword_term_warnings(normalized)
        if warnings:
            all_warnings.extend(f"{normalized}: {warning}" for warning in warnings)
        terms.append(BicotenderKeywordTerm(raw=raw, normalized=normalized, warnings=warnings))
    return BicotenderKeywordParseResult(terms=tuple(terms), warnings=tuple(all_warnings))


def batch_bicotender_keywords(
    terms: str | Iterable[str | BicotenderKeywordTerm],
    *,
    cap: int = BICOTENDER_DEFAULT_KEYWORD_CAP,
) -> tuple[BicotenderKeywordBatch, ...]:
    normalized_terms, warnings = _coerce_terms(terms)
    if cap < 1:
        raise ValueError("Bicotender keyword cap must be positive")

    batches: list[BicotenderKeywordBatch] = []
    current: list[str] = []
    for term in normalized_terms:
        if len(term) > cap:
            raise ValueError(f"Bicotender keyword term exceeds cap without a safe split: {term}")
        candidate = " ".join((*current, term)) if current else term
        if current and len(candidate) > cap:
            keywords = " ".join(current)
            batches.append(
                BicotenderKeywordBatch(
                    index=len(batches) + 1,
                    terms=tuple(current),
                    keywords=keywords,
                    char_count=len(keywords),
                )
            )
            current = [term]
        else:
            current.append(term)

    if current:
        keywords = " ".join(current)
        batches.append(
            BicotenderKeywordBatch(
                index=len(batches) + 1,
                terms=tuple(current),
                keywords=keywords,
                char_count=len(keywords),
            )
        )

    if warnings:
        return tuple(
            BicotenderKeywordBatch(
                index=batch.index,
                terms=batch.terms,
                keywords=batch.keywords,
                char_count=batch.char_count,
                warnings=tuple(warnings) if batch.index == 1 else (),
            )
            for batch in batches
        )
    return tuple(batches)


def _coerce_keyword_batches(
    terms: str | BicotenderKeywordBatch | Iterable[str | BicotenderKeywordTerm | BicotenderKeywordBatch],
    *,
    cap: int,
) -> tuple[BicotenderKeywordBatch, ...]:
    if isinstance(terms, BicotenderKeywordBatch):
        return (terms,)
    if isinstance(terms, str):
        return batch_bicotender_keywords(terms, cap=cap)
    materialized = tuple(terms)
    if materialized and all(isinstance(term, BicotenderKeywordBatch) for term in materialized):
        return tuple(term for term in materialized if isinstance(term, BicotenderKeywordBatch))
    return batch_bicotender_keywords(materialized, cap=cap)


def load_keyword_batches_from_json(path: str) -> tuple[BicotenderKeywordBatch, ...]:
    with open(path, "r", encoding="utf-8-sig") as handle:
        payload = json.load(handle)
    repaired_terms = tuple(str(term) for term in payload.get("repaired_terms", ()))
    batches: list[BicotenderKeywordBatch] = []
    for index, batch in enumerate(payload.get("batches", ()), start=1):
        keywords = _load_keyword_batch_text(batch)
        terms = _load_keyword_batch_terms(batch, repaired_terms=repaired_terms, keywords=keywords)
        char_count = _load_keyword_batch_char_count(batch, keywords=keywords)
        declared_term_count = batch.get("term_count")
        if declared_term_count is not None and int(declared_term_count) != len(terms):
            raise ValueError(
                "Bicotender keyword batch term_count disagrees with reconstructed terms: "
                f"{declared_term_count} != {len(terms)}"
            )
        batches.append(
            BicotenderKeywordBatch(
                index=_load_keyword_batch_index(batch, fallback=index),
                terms=terms,
                keywords=keywords,
                char_count=char_count,
            )
        )
    return tuple(batches)


def build_bicotender_query_plan(
    *,
    inn: str,
    positive_keywords: str | BicotenderKeywordBatch | Iterable[str | BicotenderKeywordTerm | BicotenderKeywordBatch],
    nokeywords: str = "",
    keyword_cap: int = BICOTENDER_DEFAULT_KEYWORD_CAP,
    trade_types: Sequence[str] = BICOTENDER_DEFAULT_TRADE_TYPES,
    status_ids: Sequence[str] = BICOTENDER_DEFAULT_STATUS_IDS,
    order: str = BICOTENDER_DEFAULT_ORDER,
    on_page: int | None = None,
    preflight_evidence: BicotenderListEvidence | None = None,
    force_keyword_batches: bool = False,
) -> BicotenderQueryPlan:
    preflight_query = BicotenderSearchQuery(
        inn=inn,
        trade_types=tuple(trade_types),
        status_ids=tuple(status_ids),
        order=order,
        on_page=on_page,
    )
    preflight = BicotenderPlannedQuery(kind="inn_only_preflight", query=preflight_query)
    keyword_batches = _coerce_keyword_batches(positive_keywords, cap=keyword_cap)

    planned_batches = tuple(
        BicotenderPlannedQuery(
            kind="keyword_batch",
            query=BicotenderSearchQuery(
                inn=inn,
                keywords=batch.keywords,
                nokeywords=nokeywords,
                trade_types=tuple(trade_types),
                status_ids=tuple(status_ids),
                order=order,
                on_page=on_page,
            ),
            batch_index=batch.index,
            batch_char_count=batch.char_count,
            terms=batch.terms,
        )
        for batch in keyword_batches
    )

    warnings = tuple(warning for batch in keyword_batches for warning in batch.warnings)
    if (
        preflight_evidence is not None
        and preflight_evidence.query_applied
        and not preflight_evidence.errors
        and preflight_evidence.visible_count == 0
        and not force_keyword_batches
    ):
        skipped = tuple(
            BicotenderPlannedQuery(
                kind=batch.kind,
                query=batch.query,
                batch_index=batch.batch_index,
                batch_char_count=batch.batch_char_count,
                terms=batch.terms,
                skipped=True,
                skip_reason="inn_only_preflight_zero_applied_results",
            )
            for batch in planned_batches
        )
        return BicotenderQueryPlan(
            preflight=preflight,
            keyword_batches=(),
            skipped_keyword_batches=skipped,
            warnings=warnings,
        )

    return BicotenderQueryPlan(preflight=preflight, keyword_batches=planned_batches, warnings=warnings)


def parse_bicotender_result_list(
    html: str,
    *,
    expected_query: BicotenderSearchQuery | None = None,
    http_status: int | None = 200,
    final_url: str = BICOTENDER_SEARCH_URL,
    queried_at: datetime | str | None = None,
    query_kind: str = "",
    batch_index: int | None = None,
    batch_char_count: int | None = None,
    positive_terms: Iterable[str] = (),
    negative_terms: Iterable[str] = (),
) -> BicotenderListEvidence:
    soup = BeautifulSoup(html or "", "html.parser")
    page_text = _normalize_space(soup.get_text(" ", strip=True))
    echoed_query = _extract_form_values(soup)
    query_applied = _query_was_applied(echoed_query, expected_query)
    registration_markers = tuple(marker for marker in _REGISTRATION_MARKERS if marker in page_text.lower())
    items = _extract_result_items(
        soup,
        final_url=final_url,
        positive_terms=tuple(positive_terms),
        negative_terms=tuple(negative_terms),
    )

    total_count = _extract_count(page_text, "total")
    archive_count = _extract_count(page_text, "archive")
    request_params = expected_query.params_map() if expected_query is not None else {}
    query_hash = expected_query.query_hash() if expected_query is not None else ""
    timestamp = queried_at
    if timestamp is None:
        timestamp = datetime.now(timezone.utc).isoformat()
    elif isinstance(timestamp, datetime):
        timestamp = timestamp.isoformat()

    return BicotenderListEvidence(
        queried_at=str(timestamp),
        http_status=http_status,
        final_url=final_url,
        search_url=expected_query.to_url() if expected_query is not None else "",
        query_kind=query_kind,
        batch_index=batch_index,
        batch_char_count=batch_char_count,
        request_params=request_params,
        query_hash=query_hash,
        query_applied=query_applied,
        echoed_query=echoed_query,
        total_count=total_count,
        archive_count=archive_count,
        visible_count=len(items),
        parsed_item_count=len(items),
        registration_markers=registration_markers,
        items=items,
        errors=() if not _http_is_error(http_status) else (f"http_status={http_status}",),
    )


def classify_bicotender_signal(
    evidence: BicotenderListEvidence,
    *,
    positive_terms: Iterable[str] = _DEFAULT_POSITIVE_SIGNAL_TERMS,
    negative_terms: Iterable[str] = (),
    batch_index: int | None = None,
) -> BicotenderSignalClassification:
    if evidence.errors or _http_is_error(evidence.http_status):
        return BicotenderSignalClassification(
            status=BICOTENDER_STATUS_SOURCE_ERROR,
            reason="public_list_request_failed",
            matched_batch_index=batch_index,
        )

    if not evidence.query_applied:
        return BicotenderSignalClassification(
            status=BICOTENDER_STATUS_REVIEW,
            reason="query_not_confirmed_on_public_list_page",
            matched_batch_index=batch_index,
        )

    if evidence.registration_markers and not evidence.visible_count:
        return BicotenderSignalClassification(
            status=BICOTENDER_STATUS_BLOCKED_PUBLIC_LIMIT,
            reason="public_list_page_indicates_registration_limited_results",
            matched_batch_index=batch_index,
        )

    count = _evidence_count(evidence)
    if count == 0:
        return BicotenderSignalClassification(
            status=BICOTENDER_STATUS_NO_SIGNAL,
            reason="applied_public_list_query_returned_zero_results",
            matched_batch_index=batch_index,
        )

    positive_matches = _items_with_positive_signal(evidence.items, positive_terms, negative_terms)
    return BicotenderSignalClassification(
        status=BICOTENDER_STATUS_VISIBLE_ITEMS,
        reason="applied_public_list_query_has_visible_rows",
        matched_tender_ids=tuple(item.tender_id for item in positive_matches if item.tender_id),
        matched_batch_index=batch_index,
    )


def assess_bicotender_public_access(
    html: str,
    evidence: BicotenderListEvidence,
    *,
    final_url: str = BICOTENDER_SEARCH_URL,
) -> BicotenderPublicAccessAssessment:
    page_text = _normalize_space(BeautifulSoup(html or "", "html.parser").get_text(" ", strip=True)).lower()
    marker_surface = f"{page_text} {(html or '').lower()}"
    captcha_marker_present = any(marker in marker_surface for marker in _CAPTCHA_MARKERS)
    login_marker_present = bool(evidence.registration_markers) or any(
        marker in marker_surface for marker in _REGISTRATION_MARKERS
    )
    hard_challenge_marker_present = any(marker in marker_surface for marker in _HARD_CHALLENGE_MARKERS)
    hard_access_denied_marker_present = any(marker in marker_surface for marker in _HARD_ACCESS_DENIED_MARKERS)
    parsed_final_url = urlparse(final_url or evidence.final_url or "")
    registration_redirect = "registration" in (parsed_final_url.path or "").lower()

    if evidence.http_status in (403, 429):
        return BicotenderPublicAccessAssessment(
            status=BICOTENDER_PUBLIC_ACCESS_BLOCKED,
            reason=f"http_{evidence.http_status}_protected_stop",
            http_status=evidence.http_status,
            query_applied=evidence.query_applied,
            visible_count=evidence.visible_count,
            total_count=evidence.total_count,
            captcha_marker_present=captcha_marker_present,
            login_marker_present=login_marker_present,
            registration_redirect=registration_redirect,
        )

    if registration_redirect:
        return BicotenderPublicAccessAssessment(
            status=BICOTENDER_PUBLIC_ACCESS_BLOCKED,
            reason="registration_redirect_stop",
            http_status=evidence.http_status,
            query_applied=evidence.query_applied,
            visible_count=evidence.visible_count,
            total_count=evidence.total_count,
            captcha_marker_present=captcha_marker_present,
            login_marker_present=login_marker_present,
            registration_redirect=registration_redirect,
        )

    usable_public_list = evidence.query_applied and evidence.visible_count > 0
    if usable_public_list:
        reason = "public_list_query_applied_with_real_visible_rows"
        if captcha_marker_present or login_marker_present:
            reason = "static_access_marker_present_but_public_rows_usable"
        return BicotenderPublicAccessAssessment(
            status=BICOTENDER_PUBLIC_ACCESS_USABLE,
            reason=reason,
            http_status=evidence.http_status,
            query_applied=evidence.query_applied,
            visible_count=evidence.visible_count,
            total_count=evidence.total_count,
            captcha_marker_present=captcha_marker_present,
            login_marker_present=login_marker_present,
            registration_redirect=registration_redirect,
        )

    zero_result_public_list = (
        evidence.query_applied
        and evidence.visible_count == 0
        and evidence.parsed_item_count == 0
        and _evidence_count(evidence) == 0
    )
    if zero_result_public_list and not (hard_challenge_marker_present or hard_access_denied_marker_present):
        reason = "public_list_query_applied_zero_results"
        if captcha_marker_present or login_marker_present:
            reason = "static_access_marker_present_but_query_applied_zero_results"
        return BicotenderPublicAccessAssessment(
            status=BICOTENDER_PUBLIC_ACCESS_USABLE,
            reason=reason,
            http_status=evidence.http_status,
            query_applied=evidence.query_applied,
            visible_count=evidence.visible_count,
            total_count=evidence.total_count,
            captcha_marker_present=captcha_marker_present,
            login_marker_present=login_marker_present,
            registration_redirect=registration_redirect,
        )

    hard_block_without_rows = hard_access_denied_marker_present or hard_challenge_marker_present
    if hard_block_without_rows:
        return BicotenderPublicAccessAssessment(
            status=BICOTENDER_PUBLIC_ACCESS_BLOCKED,
            reason="hard_challenge_or_access_denied_without_usable_public_rows",
            http_status=evidence.http_status,
            query_applied=evidence.query_applied,
            visible_count=evidence.visible_count,
            total_count=evidence.total_count,
            captcha_marker_present=captcha_marker_present,
            login_marker_present=login_marker_present,
            registration_redirect=registration_redirect,
        )

    if captcha_marker_present or login_marker_present:
        return BicotenderPublicAccessAssessment(
            status=BICOTENDER_PUBLIC_ACCESS_REVIEW,
            reason="ambiguous_no_usable_public_rows_with_static_access_marker",
            http_status=evidence.http_status,
            query_applied=evidence.query_applied,
            visible_count=evidence.visible_count,
            total_count=evidence.total_count,
            captcha_marker_present=captcha_marker_present,
            login_marker_present=login_marker_present,
            registration_redirect=registration_redirect,
        )

    return BicotenderPublicAccessAssessment(
        status=BICOTENDER_PUBLIC_ACCESS_REVIEW,
        reason="public_list_evidence_not_confirmed",
        http_status=evidence.http_status,
        query_applied=evidence.query_applied,
        visible_count=evidence.visible_count,
        total_count=evidence.total_count,
        captcha_marker_present=captcha_marker_present,
        login_marker_present=login_marker_present,
        registration_redirect=registration_redirect,
    )


def fetch_bicotender_public_signal(
    *,
    inn: str,
    positive_keywords: str | BicotenderKeywordBatch | Iterable[str | BicotenderKeywordTerm | BicotenderKeywordBatch],
    fetcher: Callable[[BicotenderSearchQuery], BicotenderFetchResponse | str],
    nokeywords: str = "",
    keyword_cap: int = BICOTENDER_DEFAULT_KEYWORD_CAP,
    force_keyword_batches: bool = False,
    stop_early: bool = True,
) -> BicotenderPublicSignalRun:
    initial_plan = build_bicotender_query_plan(
        inn=inn,
        positive_keywords=positive_keywords,
        nokeywords=nokeywords,
        keyword_cap=keyword_cap,
        force_keyword_batches=True,
    )
    preflight_response = _coerce_fetch_response(fetcher(initial_plan.preflight.query))
    preflight_evidence = parse_bicotender_result_list(
        preflight_response.html,
        expected_query=initial_plan.preflight.query,
        http_status=preflight_response.http_status,
        final_url=preflight_response.final_url,
        query_kind=initial_plan.preflight.kind,
    )
    preflight_access = assess_bicotender_public_access(
        preflight_response.html,
        preflight_evidence,
        final_url=preflight_response.final_url,
    )
    preflight_transport_error = _public_list_transport_error(preflight_response, preflight_access)
    if preflight_transport_error:
        preflight_evidence = _with_evidence_errors(preflight_evidence, (preflight_transport_error,))
    preflight_access_error = _public_access_error_reason(preflight_access)
    if preflight_access_error:
        preflight_evidence = _with_evidence_errors(preflight_evidence, (preflight_access_error,))

    plan = build_bicotender_query_plan(
        inn=inn,
        positive_keywords=positive_keywords,
        nokeywords=nokeywords,
        keyword_cap=keyword_cap,
        preflight_evidence=preflight_evidence,
        force_keyword_batches=force_keyword_batches,
    )
    preflight_classification = classify_bicotender_signal(preflight_evidence)
    preflight_evidence = _with_evidence_status(
        preflight_evidence,
        access=preflight_access,
        classification=preflight_classification,
    )
    if preflight_classification.status == BICOTENDER_STATUS_SOURCE_ERROR:
        return BicotenderPublicSignalRun(
            preflight=preflight_evidence,
            batch_evidence=(),
            classification=preflight_classification,
            skipped_keyword_batches=plan.skipped_keyword_batches,
        )
    if not plan.keyword_batches:
        if plan.skipped_keyword_batches:
            classification = BicotenderSignalClassification(
                status=BICOTENDER_STATUS_NO_PUBLIC_ITEMS_BY_INN,
                reason="inn_only_preflight_returned_no_usable_public_items",
            )
        else:
            classification = preflight_classification
        return BicotenderPublicSignalRun(
            preflight=preflight_evidence,
            batch_evidence=(),
            classification=classification,
            skipped_keyword_batches=plan.skipped_keyword_batches,
        )

    batch_evidence: list[BicotenderListEvidence] = []
    batch_classifications: list[BicotenderSignalClassification] = []
    negative_terms = parse_bicotender_keyword_dsl(nokeywords).normalized_terms
    for planned in plan.keyword_batches:
        response = _coerce_fetch_response(fetcher(planned.query))
        evidence = parse_bicotender_result_list(
            response.html,
            expected_query=planned.query,
            http_status=response.http_status,
            final_url=response.final_url,
            query_kind=planned.kind,
            batch_index=planned.batch_index,
            batch_char_count=planned.batch_char_count,
            positive_terms=planned.terms,
            negative_terms=negative_terms,
        )
        access = assess_bicotender_public_access(response.html, evidence, final_url=response.final_url)
        transport_error = _public_list_transport_error(response, access)
        if transport_error:
            evidence = _with_evidence_errors(evidence, (transport_error,))
        access_error = _public_access_error_reason(access)
        if access_error:
            evidence = _with_evidence_errors(evidence, (access_error,))
        classification = classify_bicotender_signal(
            evidence,
            positive_terms=planned.terms,
            negative_terms=negative_terms,
            batch_index=planned.batch_index,
        )
        batch_classifications.append(classification)
        evidence = _with_evidence_status(evidence, access=access, classification=classification)
        batch_evidence.append(evidence)

    overall = _combine_batch_classifications(
        batch_classifications,
        batch_evidence,
        preflight_evidence=preflight_evidence,
    )
    return BicotenderPublicSignalRun(
        preflight=preflight_evidence,
        batch_evidence=tuple(batch_evidence),
        classification=overall,
        stopped_early=False,
    )


def summarize_bicotender_operator_status(run: BicotenderPublicSignalRun) -> BicotenderOperatorSummary:
    batches = tuple(_operator_batch_status(evidence) for evidence in run.batch_evidence)
    keyword_batch_count = len(batches) or len(run.skipped_keyword_batches)
    visible_count = sum(batch.visible_count for batch in batches)
    matched_positive_count = sum(batch.matched_positive_count for batch in batches)
    source_state = _combine_operator_source_states(
        tuple(batch.source_state for batch in batches),
        fallback=_operator_source_state(run.preflight),
    )
    access_note = _combine_operator_notes(
        tuple(batch.access_note for batch in batches if batch.access_note)
        or ((run.preflight.access_reason,) if run.preflight.access_reason else ())
    )
    return BicotenderOperatorSummary(
        primary_status=_operator_company_status_text(
            visible_count,
            keyword_batch_count=keyword_batch_count,
            source_state=source_state,
        ),
        visible_count=visible_count,
        keyword_batch_count=keyword_batch_count,
        preflight_status=_operator_preflight_status_text(run.preflight),
        preflight_visible_count=run.preflight.visible_count,
        preflight_total_count=run.preflight.total_count,
        matched_positive_count=matched_positive_count,
        source_state=source_state,
        access_note=access_note,
        technical_internal_status=run.classification.status,
        technical_internal_reason=run.classification.reason,
        batches=batches,
    )


def _response_hosts(*urls: str) -> tuple[str, ...]:
    hosts: list[str] = []
    for url in urls:
        host = urlparse(url or "").netloc.lower()
        if host and host not in hosts:
            hosts.append(host)
    return tuple(hosts)


class BicotenderSource(BaseSource):
    source_name = BICOTENDER_SOURCE_NAME

    def __init__(
        self,
        client: core.RateLimitedHttpClient,
        *,
        keyword_batches: Sequence[BicotenderKeywordBatch],
        nokeywords: str = "",
        fetcher: Callable[[BicotenderSearchQuery], BicotenderFetchResponse | str] | None = None,
    ) -> None:
        super().__init__(client)
        self.keyword_batches = tuple(keyword_batches)
        self.nokeywords = nokeywords
        self._injected_fetcher = fetcher

    def search(self, row: core.RowInput) -> core.SourceResult:
        result = core.SourceResult(source=self.source_name, status="skipped")
        inn = core.normalize_inn(row.inn)
        if not core.is_valid_russian_inn(inn):
            result.status = "skipped_no_valid_inn"
            result.notes.append("Bicotender skipped: valid Russian INN is required")
            return result
        if not self.keyword_batches:
            result.status = "source_issue"
            result.errors.append("bicotender_keyword_batches_missing")
            result.notes.append(BICOTENDER_PUBLIC_LIST_BOUNDARY_NOTE)
            return result

        try:
            run = fetch_bicotender_public_signal(
                inn=inn,
                positive_keywords=self.keyword_batches,
                nokeywords=self.nokeywords,
                fetcher=self._injected_fetcher or self._fetch_query,
            )
        except Exception as exc:
            result.status = "source_issue"
            result.errors.append(f"bicotender_fetch_failed: {exc}")
            result.notes.append(BICOTENDER_PUBLIC_LIST_BOUNDARY_NOTE)
            return result

        return source_result_from_bicotender_run(run)

    def _fetch_query(self, query: BicotenderSearchQuery) -> BicotenderFetchResponse:
        outcome = self.client.request(
            query.to_url(),
            source=self.source_name,
        )
        response = outcome.response
        html = response.text if response is not None else ""
        http_status = response.status_code if response is not None else None
        final_url = response.url if response is not None else query.to_url()
        error = "" if outcome.ok else (outcome.error or outcome.status)
        fetch_response = BicotenderFetchResponse(
            html=html,
            http_status=http_status,
            final_url=final_url,
            error=error,
            transport_status=outcome.status,
            proxy_mode=outcome.proxy_mode,
            proxy_label=outcome.proxy_label,
            proxy_id=outcome.proxy_id,
        )
        self._clear_host_cooldown_after_usable_static_marker(query, fetch_response)
        return fetch_response

    def _clear_host_cooldown_after_usable_static_marker(
        self,
        query: BicotenderSearchQuery,
        response: BicotenderFetchResponse,
    ) -> None:
        recover_soft_blocked_success = getattr(self.client, "recover_soft_blocked_success", None)
        clear_host_cooldown = getattr(self.client, "clear_host_cooldown", None)
        if response.transport_status != "bot_gate":
            return
        try:
            evidence = parse_bicotender_result_list(
                response.html,
                expected_query=query,
                http_status=response.http_status,
                final_url=response.final_url,
            )
            access = assess_bicotender_public_access(response.html, evidence, final_url=response.final_url)
        except Exception:
            return
        if not _static_marker_bot_gate_has_usable_public_list(response, access):
            return
        hosts = _response_hosts(query.to_url(), response.final_url)
        if callable(recover_soft_blocked_success):
            recover_soft_blocked_success(
                source=self.source_name,
                url=query.to_url(),
                final_url=response.final_url,
                proxy_label=response.proxy_label,
                proxy_id=response.proxy_id,
                reason=access.reason,
            )
            return
        if hosts and callable(clear_host_cooldown):
            clear_host_cooldown(*hosts)


def source_result_from_bicotender_run(run: BicotenderPublicSignalRun) -> core.SourceResult:
    payload = bicotender_public_signal_run_to_payload(run)
    result = core.SourceResult(
        source=BICOTENDER_SOURCE_NAME,
        status=_bicotender_source_result_status(run),
        search_url=run.preflight.search_url,
        listing_url=run.preflight.search_url,
        http_status=run.preflight.http_status,
    )
    result.availability[BICOTENDER_AVAILABILITY_KEY] = payload
    result.links = _dedupe_non_empty(
        [
            run.preflight.search_url,
            *(evidence.search_url for evidence in run.batch_evidence),
            *(
                item.detail_url
                for evidence in (run.preflight, *run.batch_evidence)
                for item in evidence.items
            ),
        ]
    )
    result.notes = _dedupe_non_empty(
        [
            payload["operator_summary"]["primary_status"],
            BICOTENDER_PUBLIC_LIST_BOUNDARY_NOTE,
            payload["operator_summary"].get("access_note", ""),
        ]
    )
    result.errors = _dedupe_non_empty(
        [
            *run.preflight.errors,
            *(error for evidence in run.batch_evidence for error in evidence.errors),
        ]
    )
    result.snippets = _dedupe_non_empty(
        [
            item.snippet or item.title
            for evidence in (run.preflight, *run.batch_evidence)
            for item in evidence.items
        ]
    )
    return result


def bicotender_public_signal_run_to_payload(run: BicotenderPublicSignalRun) -> dict[str, Any]:
    summary = run.operator_summary
    planned_keyword_batch_count = len(run.batch_evidence) + len(run.skipped_keyword_batches)
    if planned_keyword_batch_count == 0:
        planned_keyword_batch_count = summary.keyword_batch_count
    batch_status_by_index = {
        batch.batch_index: batch
        for batch in summary.batches
        if batch.batch_index is not None
    }
    return {
        "schema_version": BICOTENDER_OPERATOR_SCHEMA_VERSION,
        "source": BICOTENDER_SOURCE_NAME,
        "access_mode": "public_no_login",
        "evidence_boundary": "public_list_only",
        "evidence_note": BICOTENDER_PUBLIC_LIST_BOUNDARY_NOTE,
        "relevance_mode": BICOTENDER_RELEVANCE_MODE,
        "primary_status": summary.primary_status,
        "visible_public_list_count": summary.visible_count,
        "raw_visible_public_list_count": summary.visible_count,
        "relevant_count": summary.visible_count,
        "relevant_count_semantics": BICOTENDER_RELEVANT_COUNT_COMPAT_NOTE,
        "visible_count": summary.visible_count,
        "planned_keyword_batch_count": planned_keyword_batch_count,
        "executed_keyword_batch_count": len(run.batch_evidence),
        "keyword_batches_skipped_count": len(run.skipped_keyword_batches),
        "source_state": summary.source_state,
        "access_note": summary.access_note,
        "operator_summary": _operator_summary_payload(
            summary,
            planned_keyword_batch_count=planned_keyword_batch_count,
            executed_keyword_batch_count=len(run.batch_evidence),
        ),
        "preflight": _preflight_payload(run.preflight),
        "keyword_batches": [
            _keyword_batch_payload(evidence, batch_status_by_index.get(evidence.batch_index))
            for evidence in run.batch_evidence
        ],
        "keyword_batches_skipped": [
            _skipped_keyword_batch_payload(planned)
            for planned in run.skipped_keyword_batches
        ],
        "technical_internal": {
            "classification_status": run.classification.status,
            "classification_reason": run.classification.reason,
            "matched_tender_ids": list(run.classification.matched_tender_ids),
            "matched_batch_index": run.classification.matched_batch_index,
            "matched_positive_count": summary.matched_positive_count,
            "stopped_early": bool(run.stopped_early),
        },
    }


def _operator_summary_payload(
    summary: BicotenderOperatorSummary,
    *,
    planned_keyword_batch_count: int,
    executed_keyword_batch_count: int,
) -> dict[str, Any]:
    return {
        "primary_status": summary.primary_status,
        "relevance_mode": BICOTENDER_RELEVANCE_MODE,
        "visible_public_list_count": summary.visible_count,
        "raw_visible_public_list_count": summary.visible_count,
        "relevant_count": summary.visible_count,
        "relevant_count_semantics": BICOTENDER_RELEVANT_COUNT_COMPAT_NOTE,
        "visible_count": summary.visible_count,
        "planned_keyword_batch_count": planned_keyword_batch_count,
        "executed_keyword_batch_count": executed_keyword_batch_count,
        "keyword_batch_count": summary.keyword_batch_count,
        "preflight_status": summary.preflight_status,
        "preflight_visible_count": summary.preflight_visible_count,
        "preflight_total_count": summary.preflight_total_count,
        "technical_internal_matched_positive_count": summary.matched_positive_count,
        "source_state": summary.source_state,
        "access_note": summary.access_note,
        "technical_internal_status": summary.technical_internal_status,
        "technical_internal_reason": summary.technical_internal_reason,
        "batches": [_operator_batch_payload(batch) for batch in summary.batches],
    }


def _operator_batch_payload(batch: BicotenderOperatorBatchStatus) -> dict[str, Any]:
    return {
        "batch_index": batch.batch_index,
        "primary_status": batch.primary_status,
        "visible_count": batch.visible_count,
        "search_url": batch.search_url,
        "source_state": batch.source_state,
        "access_note": batch.access_note,
        "technical_internal_status": batch.technical_internal_status,
        "technical_internal_reason": batch.technical_internal_reason,
        "technical_internal_matched_positive_count": batch.matched_positive_count,
        "technical_internal_matched_positive_tender_ids": list(batch.matched_positive_tender_ids),
    }


def _preflight_payload(evidence: BicotenderListEvidence) -> dict[str, Any]:
    return {
        "primary_status": _operator_preflight_status_text(evidence),
        "source_state": _operator_source_state(evidence),
        "access_note": _operator_access_note(evidence),
        **_evidence_common_payload(evidence),
    }


def _keyword_batch_payload(
    evidence: BicotenderListEvidence,
    batch_status: BicotenderOperatorBatchStatus | None,
) -> dict[str, Any]:
    if batch_status is None:
        batch_status = _operator_batch_status(evidence)
    return {
        "batch_index": evidence.batch_index,
        "primary_status": batch_status.primary_status,
        "visible_count": batch_status.visible_count,
        "source_state": batch_status.source_state,
        "access_note": batch_status.access_note,
        **_evidence_common_payload(evidence),
    }


def _evidence_common_payload(evidence: BicotenderListEvidence) -> dict[str, Any]:
    return {
        "query_kind": evidence.query_kind,
        "batch_char_count": evidence.batch_char_count,
        "search_url": evidence.search_url,
        "source": evidence.source,
        "access_mode": evidence.access_mode,
        "source_state": _operator_source_state(evidence),
        "access_note": _operator_access_note(evidence),
        "source_state_raw": evidence.access_status,
        "access_reason": evidence.access_reason,
        "source_url": evidence.final_url,
        "http_status": evidence.http_status,
        "query_applied": bool(evidence.query_applied),
        "total_count": evidence.total_count,
        "archive_count": evidence.archive_count,
        "visible_count": evidence.visible_count,
        "parsed_item_count": evidence.parsed_item_count,
        "request_params": _mapping_tuple_values_to_lists(evidence.request_params),
        "echoed_query": _mapping_tuple_values_to_lists(evidence.echoed_query),
        "query_hash": evidence.query_hash,
        "registration_markers": list(evidence.registration_markers),
        "evidence_quality": evidence.evidence_quality,
        "detail_fetched": bool(evidence.detail_accessed),
        "documents_accessed": bool(evidence.documents_accessed),
        "errors": list(evidence.errors),
        "technical_internal_status": evidence.classification_status,
        "technical_internal_reason": evidence.classification_reason,
        "items": [_item_payload(item) for item in evidence.items],
    }


def _item_payload(item: BicotenderListItem) -> dict[str, Any]:
    return {
        "tender_id": item.tender_id,
        "item_url": item.detail_url,
        "detail_url": item.detail_url,
        "title": item.title,
        "snippet": item.snippet,
        "list_text": item.snippet,
        "date_text": item.date_text,
        "region": item.region,
        "geo": item.region,
        "industry": item.industry,
        "category": item.industry,
        "price_text": item.price_text,
        "procedure_text": item.procedure_text,
        "matched_positive_terms": list(item.matched_positive_terms),
        "matched_negative_terms": list(item.matched_negative_terms),
        "evidence_quality": item.evidence_quality,
        "detail_fetched": bool(item.detail_fetched),
        "documents_accessed": bool(item.documents_accessed),
    }


def _skipped_keyword_batch_payload(planned: BicotenderPlannedQuery) -> dict[str, Any]:
    label = f"batch {planned.batch_index}" if planned.batch_index is not None else "batch"
    return {
        "batch_index": planned.batch_index,
        "primary_status": f"{label}: skipped",
        "skip_reason": planned.skip_reason,
        "search_url": planned.query.to_url(),
        "batch_char_count": planned.batch_char_count,
        "terms": list(planned.terms),
        "query_kind": planned.kind,
    }


def _mapping_tuple_values_to_lists(value: Mapping[str, Sequence[str]]) -> dict[str, list[str]]:
    return {str(key): [str(item) for item in items] for key, items in value.items()}


def _bicotender_source_result_status(run: BicotenderPublicSignalRun) -> str:
    if run.classification.status == BICOTENDER_STATUS_PARTIAL_SOURCE_ERROR:
        return "partial_success"
    if run.classification.status in {BICOTENDER_STATUS_SOURCE_ERROR, BICOTENDER_STATUS_BLOCKED_PUBLIC_LIMIT}:
        return "source_issue"
    return "success"


def _dedupe_non_empty(values: Iterable[str]) -> list[str]:
    result: list[str] = []
    for value in values:
        text = str(value or "").strip()
        if text and text not in result:
            result.append(text)
    return result


def _public_access_error_reason(access: BicotenderPublicAccessAssessment) -> str:
    if access.blocked:
        return access.reason
    if access.reason == "ambiguous_no_usable_public_rows_with_static_access_marker":
        return access.reason
    return ""


def _public_list_transport_error(
    response: BicotenderFetchResponse,
    access: BicotenderPublicAccessAssessment,
) -> str:
    if not response.error:
        return ""
    if _static_marker_bot_gate_has_usable_public_list(response, access):
        return ""
    return response.error


def _static_marker_bot_gate_has_usable_public_list(
    response: BicotenderFetchResponse,
    access: BicotenderPublicAccessAssessment,
) -> bool:
    return (
        response.transport_status == "bot_gate"
        and response.http_status == 200
        and access.status == BICOTENDER_PUBLIC_ACCESS_USABLE
        and (access.captcha_marker_present or access.login_marker_present)
        and access.query_applied
    )


def _split_keyword_dsl(text: str) -> tuple[str, ...]:
    terms: list[str] = []
    current: list[str] = []
    bracket_depth = 0
    for char in text or "":
        if char == "[":
            bracket_depth += 1
            current.append(char)
            continue
        if char == "]" and bracket_depth:
            bracket_depth -= 1
            current.append(char)
            continue
        if char.isspace() and bracket_depth == 0:
            if current:
                terms.append("".join(current))
                current = []
            continue
        current.append(char)
    if current:
        terms.append("".join(current))
    return tuple(term for term in terms if term.strip())


def _load_keyword_batch_index(batch: Mapping[str, Any], *, fallback: int) -> int:
    batch_id = str(batch.get("id") or "")
    match = re.search(r"(\d+)$", batch_id)
    if match:
        return int(match.group(1))
    return int(batch.get("batch_index") or batch.get("index") or fallback)


def _load_keyword_batch_text(batch: Mapping[str, Any]) -> str:
    keywords = _normalize_space(
        str(batch.get("text") or batch.get("query") or batch.get("keywords") or "")
    )
    if not keywords:
        terms = tuple(str(term).strip() for term in batch.get("terms", ()) if str(term).strip())
        keywords = " ".join(terms)
    if not keywords:
        raise ValueError("Bicotender keyword batch has no usable keyword text")
    return keywords


def _load_keyword_batch_terms(
    batch: Mapping[str, Any],
    *,
    repaired_terms: Sequence[str],
    keywords: str,
) -> tuple[str, ...]:
    raw_indexes = batch.get("raw_term_indexes")
    if raw_indexes is not None:
        if not repaired_terms:
            raise ValueError("Bicotender keyword batch raw_term_indexes require root repaired_terms")
        terms: list[str] = []
        for raw_index in raw_indexes:
            term_index = int(raw_index)
            if term_index < 0 or term_index >= len(repaired_terms):
                raise ValueError(
                    "Bicotender keyword batch raw_term_indexes point outside repaired_terms: "
                    f"{term_index}"
                )
            terms.append(repaired_terms[term_index])
        return tuple(terms)

    explicit_terms = tuple(str(term).strip() for term in batch.get("terms", ()) if str(term).strip())
    if explicit_terms:
        return explicit_terms
    return parse_bicotender_keyword_dsl(keywords).normalized_terms


def _load_keyword_batch_char_count(batch: Mapping[str, Any], *, keywords: str) -> int:
    declared = batch.get("char_count")
    actual = len(keywords)
    if declared is None:
        return actual
    declared_int = int(declared)
    if declared_int != actual:
        raise ValueError(
            "Bicotender keyword batch char_count disagrees with keyword text length: "
            f"{declared_int} != {actual}"
        )
    return declared_int


def _normalize_keyword_term(term: str) -> str:
    return _normalize_space(term)


def _keyword_term_warnings(term: str) -> tuple[str, ...]:
    warnings: list[str] = []
    if term.count("[") != term.count("]"):
        warnings.append("unbalanced_bracket_group")
    bracket_match = re.match(r"^\[([^\]]+)\](\d*)$", term)
    if term.startswith("[") and bracket_match is None:
        warnings.append("malformed_bracket_group")
    if bracket_match is not None:
        body, distance = bracket_match.groups()
        if not distance:
            warnings.append("bracket_group_without_distance")
        if "-" in body:
            warnings.append("hyphen_inside_proximity_group")
    if "<" in term and term != "<":
        warnings.append("order_operator_embedded_in_term")
    if any(ord(char) < 32 for char in term):
        warnings.append("control_character_in_term")
    return tuple(warnings)


def _coerce_terms(
    terms: str | Iterable[str | BicotenderKeywordTerm],
) -> tuple[tuple[str, ...], tuple[str, ...]]:
    if isinstance(terms, str):
        parsed = parse_bicotender_keyword_dsl(terms)
        return parsed.normalized_terms, parsed.warnings

    normalized: list[str] = []
    warnings: list[str] = []
    for term in terms:
        if isinstance(term, BicotenderKeywordTerm):
            normalized_term = term.normalized
            warnings.extend(f"{normalized_term}: {warning}" for warning in term.warnings)
        else:
            normalized_term = _normalize_keyword_term(str(term))
            warnings.extend(f"{normalized_term}: {warning}" for warning in _keyword_term_warnings(normalized_term))
        if normalized_term:
            normalized.append(normalized_term)
    return tuple(normalized), tuple(warnings)


def _extract_form_values(soup: BeautifulSoup) -> dict[str, tuple[str, ...]]:
    values: dict[str, list[str]] = {}
    for field in soup.find_all(("input", "textarea", "select")):
        if not isinstance(field, Tag):
            continue
        name = str(field.get("name") or "").strip()
        if not name:
            continue
        field_values = _field_values(field)
        if field_values:
            values.setdefault(name, []).extend(field_values)
    return {name: tuple(items) for name, items in values.items()}


def _field_values(field: Tag) -> tuple[str, ...]:
    if field.name == "textarea":
        return (_normalize_space(field.get_text(" ", strip=True)),)
    if field.name == "select":
        selected = [
            str(option.get("value") or option.get_text(" ", strip=True))
            for option in field.find_all("option")
            if option.has_attr("selected")
        ]
        return tuple(_normalize_space(value) for value in selected if _normalize_space(value))

    field_type = str(field.get("type") or "").lower()
    if field_type in {"checkbox", "radio"} and not field.has_attr("checked"):
        return ()
    value = _normalize_space(str(field.get("value") or ""))
    return (value,) if value else ()


def _query_was_applied(
    echoed_query: Mapping[str, tuple[str, ...]],
    expected_query: BicotenderSearchQuery | None,
) -> bool:
    if expected_query is None:
        return bool(echoed_query)

    expected = expected_query.params_map()
    if not _values_include(echoed_query.get("company[inn]", ()), expected.get("company[inn]", ())):
        return False
    for required_name in ("keywords", "nokeywords"):
        expected_values = expected.get(required_name, ())
        if expected_values and not _values_include(echoed_query.get(required_name, ()), expected_values):
            return False
    for optional_name in ("tradeType[]", "status_id[]"):
        actual_values = echoed_query.get(optional_name, ())
        expected_values = expected.get(optional_name, ())
        if actual_values and expected_values and not _values_include(actual_values, expected_values):
            return False
    return True


def _values_include(actual: Sequence[str], expected: Sequence[str]) -> bool:
    actual_norm = {_normalize_space(value) for value in actual}
    expected_norm = {_normalize_space(value) for value in expected}
    return bool(expected_norm) and expected_norm.issubset(actual_norm)


def _extract_result_items(
    soup: BeautifulSoup,
    *,
    final_url: str,
    positive_terms: tuple[str, ...],
    negative_terms: tuple[str, ...],
) -> tuple[BicotenderListItem, ...]:
    items: list[BicotenderListItem] = []
    seen: set[str] = set()
    for anchor in soup.find_all("a", href=True):
        if not isinstance(anchor, Tag):
            continue
        href = str(anchor.get("href") or "")
        anchor_title = _normalize_space(anchor.get_text(" ", strip=True))
        container = _nearest_result_container(anchor)
        snippet = _normalize_space(container.get_text(" ", strip=True) if container is not None else anchor_title)
        tender_id = _extract_tender_id(href, snippet)
        if not _is_public_tender_result_link(href, tender_id, snippet):
            continue
        title = _extract_item_title(anchor, container, tender_id=tender_id)
        detail_url = urljoin(final_url, href)
        dedupe_key = tender_id or detail_url
        if dedupe_key in seen:
            continue
        seen.add(dedupe_key)
        lowered_snippet = snippet.lower()
        fields = _extract_item_fields(snippet, container, tender_id=tender_id)
        item = BicotenderListItem(
            tender_id=tender_id,
            title=title,
            detail_url=detail_url,
            snippet=snippet,
            region=fields.region,
            industry=fields.industry,
            date_text=fields.date_text,
            price_text=fields.price_text,
            procedure_text=fields.procedure_text,
            registration_marker=any(marker in lowered_snippet for marker in _REGISTRATION_MARKERS),
            detail_marker=True,
            document_marker=_has_document_marker(container or anchor),
            matched_positive_terms=_match_terms(_item_visible_text(title, snippet), positive_terms),
            matched_negative_terms=_match_terms(_item_visible_text(title, snippet), negative_terms),
        )
        items.append(item)
    return tuple(items)


def _nearest_result_container(anchor: Tag) -> Tag | None:
    row = anchor.find_parent("tr")
    if isinstance(row, Tag):
        return row

    current = anchor
    for _ in range(6):
        parent = current.parent
        if not isinstance(parent, Tag):
            return current
        class_text = " ".join(str(value) for value in parent.get("class", ())).lower()
        if any(marker in class_text for marker in ("tender", "result", "lot", "item", "card")):
            return parent
        if parent.name in {"li", "article", "tr"}:
            return parent
        current = parent
    return current


def _extract_item_title(anchor: Tag, container: Tag | None, *, tender_id: str) -> str:
    title = _normalize_space(anchor.get_text(" ", strip=True))
    if title and not _is_service_link_title(title):
        return title

    if container is None:
        return title

    container_text = _normalize_space(container.get_text(" ", strip=True))
    for candidate in container.find_all("a", href=True):
        if not isinstance(candidate, Tag):
            continue
        candidate_title = _normalize_space(candidate.get_text(" ", strip=True))
        if not candidate_title or _is_service_link_title(candidate_title):
            continue
        candidate_id = _extract_tender_id(str(candidate.get("href") or ""), container_text)
        if candidate_id == tender_id:
            return candidate_title
    return _fallback_title_from_snippet(container_text, tender_id=tender_id)


def _is_service_link_title(title: str) -> bool:
    lowered = _normalize_space(title).lower().lstrip("+").strip()
    return any(marker in lowered for marker in _SERVICE_LINK_TITLES)


def _fallback_title_from_snippet(snippet: str, *, tender_id: str) -> str:
    text = _normalize_space(snippet)
    if tender_id:
        text = re.sub(rf"\bТендер\s*№\s*{re.escape(tender_id)}\b", "", text, flags=re.IGNORECASE)
    for marker in _SERVICE_LINK_TITLES:
        text = re.sub(re.escape(marker), "", text, flags=re.IGNORECASE)
    stop_labels = _label_regex(_QUERY_LABELS)
    text = re.split(
        rf"\s+(?:{stop_labels})(?:\s*[:\-]|\s+)",
        text,
        maxsplit=1,
        flags=re.IGNORECASE,
    )[0]
    return _normalize_space(text)


def _extract_item_fields(snippet: str, container: Tag | None, *, tender_id: str) -> _BicotenderItemFields:
    cells = _visible_row_cells(container)
    region, region_index = _extract_region(snippet, cells)
    return _BicotenderItemFields(
        region=region,
        industry=_extract_industry(snippet, cells, region_index=region_index),
        date_text=_extract_date_text(snippet, cells),
        price_text=_extract_price_text(snippet, cells),
        procedure_text=_extract_procedure_text(snippet, cells, tender_id=tender_id),
    )


def _visible_row_cells(container: Tag | None) -> tuple[str, ...]:
    if not isinstance(container, Tag) or container.name != "tr":
        return ()

    cells: list[str] = []
    for cell in container.find_all(("td", "th"), recursive=False):
        if not isinstance(cell, Tag):
            continue
        text = _normalize_space(cell.get_text(" ", strip=True))
        if not text or _is_service_only_cell_text(text):
            continue
        cells.append(text)
    return tuple(cells)


def _is_service_only_cell_text(text: str) -> bool:
    lowered = _normalize_space(text).lower().strip(" .:+-")
    return any(lowered == marker for marker in _CELL_SERVICE_ONLY_TITLES)


def _extract_region(snippet: str, cells: Sequence[str]) -> tuple[str, int | None]:
    labeled = _extract_labeled_value(snippet, ("Регион", "Регион поставки"))
    if labeled:
        return labeled, None

    for index, cell in enumerate(cells):
        cell_labeled = _extract_labeled_value(cell, ("Регион", "Регион поставки"))
        if cell_labeled:
            return cell_labeled, index

    for index, cell in enumerate(cells):
        if _looks_like_region_cell(cell):
            if index > 0 and _normalize_space(cells[index - 1]).lower() == "закупки":
                return _normalize_space(f"Закупки {cell}"), index
            return cell, index
        if _normalize_space(cell).lower() == "закупки" and index + 1 < len(cells):
            next_cell = cells[index + 1]
            if _looks_like_region_cell(next_cell):
                return _normalize_space(f"{cell} {next_cell}"), index + 1
    return "", None


def _extract_industry(snippet: str, cells: Sequence[str], *, region_index: int | None) -> str:
    labeled = _extract_labeled_value(snippet, ("Отрасль", "Отрасли", "Категория", "Категории"))
    if labeled:
        return labeled

    for cell in cells:
        cell_labeled = _extract_labeled_value(cell, ("Отрасль", "Отрасли", "Категория", "Категории"))
        if cell_labeled:
            return cell_labeled

    for index, cell in enumerate(cells):
        if region_index is not None and index <= region_index:
            continue
        if _looks_like_industry_cell(cell):
            return cell
    return ""


def _extract_date_text(snippet: str, cells: Sequence[str]) -> str:
    labels = (
        "Период показа",
        "Дата начала",
        "Дата окончания",
        "Дата публикации",
        "Срок выполнения",
        "Окончание",
        "Дата",
        "Период",
        "Начало",
    )
    labeled = _extract_labeled_value(snippet, labels)
    if labeled:
        return labeled

    for cell in cells:
        cell_labeled = _extract_labeled_value(cell, labels)
        if cell_labeled:
            return cell_labeled
        date_text = _extract_unlabeled_date_text(cell)
        if date_text:
            return date_text
    return ""


def _extract_price_text(snippet: str, cells: Sequence[str]) -> str:
    labels = ("Цена", "Сумма", "Стоимость", "НМЦ")
    labeled = _extract_labeled_value(snippet, labels)
    if labeled:
        return labeled

    for cell in cells:
        cell_labeled = _extract_labeled_value(cell, labels)
        if cell_labeled:
            return cell_labeled
        price_text = _extract_unlabeled_price_text(cell)
        if price_text:
            return price_text
    return ""


def _extract_procedure_text(snippet: str, cells: Sequence[str], *, tender_id: str) -> str:
    labels = ("Процедура", "Тип процедуры", "Тип торгов", "Вид процедуры", "Тип")
    labeled = _extract_labeled_value(snippet, labels)
    if labeled:
        return _clean_procedure_text(labeled, tender_id=tender_id)

    for cell in cells:
        cell_labeled = _extract_labeled_value(cell, labels)
        if cell_labeled:
            return _clean_procedure_text(cell_labeled, tender_id=tender_id)
        if _looks_like_procedure_cell(cell):
            return _clean_procedure_text(cell, tender_id=tender_id)
    return ""


def _extract_labeled_value(text: str, labels: Sequence[str]) -> str:
    stop_labels = _label_regex(_QUERY_LABELS)
    terminal_markers = r"Документы|Файлы"
    for label in labels:
        match = re.search(
            rf"{_label_boundary()}{re.escape(label)}{_label_boundary(after=True)}"
            rf"\s*(?:[:\-]\s*|\s+)(.+?)"
            rf"(?=\s+(?:{stop_labels})(?:\s*[:\-]|\s+)|\s+(?:{terminal_markers})\b|$)",
            text,
            flags=re.IGNORECASE,
        )
        if match:
            return _normalize_space(match.group(1))
    return ""


def _extract_unlabeled_date_text(text: str) -> str:
    dates = tuple(_DATE_RE.finditer(text))
    if not dates:
        return ""

    lowered = text.lower()
    if "срок" in lowered:
        start = lowered.find("срок")
        return _normalize_space(text[start : dates[-1].end()])

    if len(dates) >= 2:
        between = text[dates[0].end() : dates[1].start()]
        separator = " - " if "-" in between or "–" in between else " "
        return _normalize_space(f"{dates[0].group(0)}{separator}{dates[1].group(0)}")
    return dates[0].group(0)


def _extract_unlabeled_price_text(text: str) -> str:
    match = _PRICE_RE.search(text)
    if match:
        return _normalize_space(match.group(0))
    if re.search(r"\bсм\.\s*док\.?\b", text, flags=re.IGNORECASE):
        return "См. док."
    return ""


def _looks_like_procedure_cell(text: str) -> bool:
    lowered = text.lower()
    return any(marker in lowered for marker in _PROCEDURE_MARKERS)


def _clean_procedure_text(text: str, *, tender_id: str) -> str:
    cleaned = _normalize_space(text)
    if tender_id:
        cleaned = re.sub(rf"#\s*{re.escape(tender_id)}\b", "", cleaned)
        cleaned = re.sub(rf"\bТендер\s*№\s*{re.escape(tender_id)}\b", "", cleaned, flags=re.IGNORECASE)
    return _normalize_space(cleaned.strip("#-: "))


def _looks_like_region_cell(text: str) -> bool:
    lowered = f" {_normalize_space(text).lower()} "
    if " / " not in lowered:
        return False
    return any(marker in lowered for marker in _REGION_MARKERS)


def _looks_like_industry_cell(text: str) -> bool:
    lowered = f" {_normalize_space(text).lower()} "
    if " / " not in lowered:
        return False
    if _looks_like_region_cell(text) or _extract_unlabeled_date_text(text) or _extract_unlabeled_price_text(text):
        return False
    if _looks_like_procedure_cell(text) or lowered.strip() == "закупки":
        return False
    return True


def _label_regex(labels: Sequence[str]) -> str:
    return "|".join(re.escape(label) for label in sorted(labels, key=len, reverse=True))


def _label_boundary(*, after: bool = False) -> str:
    chars = r"0-9A-Za-zА-Яа-яЁё"
    if after:
        return rf"(?![{chars}])"
    return rf"(?<![{chars}])"


def _has_document_marker(tag: Tag) -> bool:
    text = _normalize_space(tag.get_text(" ", strip=True)).lower()
    if "документ" in text or "файл" in text:
        return True
    for anchor in tag.find_all("a", href=True):
        href = str(anchor.get("href") or "").lower()
        if any(marker in href for marker in ("doc", "file", "download", "attachment")):
            return True
    return False


def _is_public_tender_result_link(href: str, tender_id: str, snippet: str) -> bool:
    if not tender_id:
        return False
    parsed = urlparse(href or "")
    path = (parsed.path or "").lower()
    if not path:
        return False
    if path.startswith("/tender/search") or path == "/tender/search/":
        return False
    if "mezhdunarodnye-tender" in path:
        return False
    if not _href_has_tender_detail_pattern(path):
        return False
    return True


def _extract_tender_id(href: str, visible_text: str = "") -> str:
    visible_match = re.search(r"тендер\s*№\s*(\d{4,})", visible_text or "", flags=re.IGNORECASE)
    if visible_match:
        return visible_match.group(1)
    parsed = urlparse(href or "")
    path = parsed.path or ""
    patterns = (
        r"-tender(\d{4,})\.html$",
        r"/tender/(\d{4,})(?:/)?$",
    )
    for pattern in patterns:
        match = re.search(pattern, path, flags=re.IGNORECASE)
        if match:
            return match.group(1)
    return ""


def _href_has_tender_detail_pattern(path: str) -> bool:
    return bool(
        re.search(r"-tender\d{4,}\.html$", path, flags=re.IGNORECASE)
        or re.search(r"/tender/\d{4,}/?$", path, flags=re.IGNORECASE)
    )


def _extract_count(text: str, count_type: str) -> int | None:
    if count_type == "archive":
        patterns = (r"архив[^\d]{0,30}(\d[\d\s]*)",)
    else:
        patterns = (
            r"найден[оа]?[^0-9]{0,30}(\d[\d\s]*)",
            r"всего[^0-9]{0,30}(\d[\d\s]*)",
            r"результат(?:ов|ы)?[^0-9]{0,30}(\d[\d\s]*)",
        )
    lowered = text.lower()
    for pattern in patterns:
        match = re.search(pattern, lowered, flags=re.IGNORECASE)
        if match:
            return int(re.sub(r"\D", "", match.group(1)))
    return None


def _items_with_positive_signal(
    items: Sequence[BicotenderListItem],
    positive_terms: Iterable[str],
    negative_terms: Iterable[str],
) -> tuple[BicotenderListItem, ...]:
    positive = tuple(positive_terms) or _DEFAULT_POSITIVE_SIGNAL_TERMS
    negative = tuple(negative_terms)
    matches: list[BicotenderListItem] = []
    for item in items:
        text = _item_visible_text(item.title, item.snippet)
        positive_matches = item.matched_positive_terms or _match_terms(text, positive)
        negative_matches = item.matched_negative_terms or _match_terms(text, negative)
        if positive_matches and not negative_matches:
            matches.append(item)
    return tuple(matches)


def _item_visible_text(*parts: str) -> str:
    return _normalize_space(" ".join(part for part in parts if part))


def _match_terms(text: str, terms: Iterable[str]) -> tuple[str, ...]:
    haystack = _searchable_text(text)
    haystack_tokens = _search_tokens(haystack)
    matches: list[str] = []
    for term in terms:
        normalized = _searchable_term(str(term))
        if normalized and _term_matches_tokens(normalized, haystack_tokens):
            matches.append(str(term))
    return tuple(matches)


def _searchable_text(text: str) -> str:
    return _normalize_space(text).lower().replace("-", " ")


def _searchable_term(term: str) -> str:
    cleaned = _normalize_space(term).lower()
    bracket_match = re.match(r"^\[([^\]]+)\]\d*$", cleaned)
    if bracket_match:
        cleaned = bracket_match.group(1)
    cleaned = cleaned.strip(".")
    cleaned = cleaned.replace("*", "")
    cleaned = cleaned.replace("-", " ")
    if cleaned == "<":
        return ""
    return _normalize_space(cleaned)


def _search_tokens(text: str) -> tuple[str, ...]:
    return tuple(_TOKEN_RE.findall(_searchable_text(text)))


def _term_matches_tokens(term: str, haystack_tokens: Sequence[str]) -> bool:
    term_tokens = _search_tokens(term)
    if not term_tokens:
        return False
    if len(term_tokens) == 1:
        return any(_token_matches_term(token, term_tokens[0]) for token in haystack_tokens)

    width = len(term_tokens)
    for start in range(0, len(haystack_tokens) - width + 1):
        if all(
            _token_matches_term(haystack_tokens[start + offset], term_token)
            for offset, term_token in enumerate(term_tokens)
        ):
            return True
    if _unordered_phrase_matches(term_tokens, haystack_tokens):
        return True
    return False


def _token_matches_term(token: str, term: str) -> bool:
    if token == term:
        return True

    short_forms = _SHORT_TOKEN_FORMS.get(term)
    if short_forms is not None:
        return token in short_forms

    if "/" in term or len(term) < 4:
        return False
    return token.startswith(term)


def _unordered_phrase_matches(term_tokens: Sequence[str], haystack_tokens: Sequence[str]) -> bool:
    if len(term_tokens) != 2 or any(len(token) < 4 or "/" in token for token in term_tokens):
        return False

    width = len(term_tokens)
    for start in range(0, len(haystack_tokens) - width + 1):
        window = haystack_tokens[start : start + width]
        unmatched = list(term_tokens)
        for token in window:
            matched_index = next(
                (index for index, term in enumerate(unmatched) if _token_matches_term(token, term)),
                None,
            )
            if matched_index is None:
                break
            unmatched.pop(matched_index)
        if not unmatched:
            return True
    return False


def _combine_batch_classifications(
    classifications: Sequence[BicotenderSignalClassification],
    evidences: Sequence[BicotenderListEvidence] = (),
    *,
    preflight_evidence: BicotenderListEvidence | None = None,
) -> BicotenderSignalClassification:
    if not classifications:
        return BicotenderSignalClassification(status=BICOTENDER_STATUS_REVIEW, reason="no_keyword_batch_evidence")
    has_source_error = any(
        classification.status in {BICOTENDER_STATUS_SOURCE_ERROR, BICOTENDER_STATUS_BLOCKED_PUBLIC_LIMIT}
        for classification in classifications
    )
    has_visible_evidence = any(evidence.visible_count > 0 for evidence in evidences) or (
        preflight_evidence is not None and preflight_evidence.visible_count > 0
    )
    if has_source_error and has_visible_evidence:
        return BicotenderSignalClassification(
            status=BICOTENDER_STATUS_PARTIAL_SOURCE_ERROR,
            reason="usable_keyword_batch_evidence_with_one_or_more_source_errors",
        )
    if has_source_error:
        for classification in classifications:
            if classification.status in {BICOTENDER_STATUS_SOURCE_ERROR, BICOTENDER_STATUS_BLOCKED_PUBLIC_LIMIT}:
                return classification
    visible_classifications = tuple(
        classification for classification in classifications if classification.status == BICOTENDER_STATUS_VISIBLE_ITEMS
    )
    if visible_classifications:
        matched_tender_ids: list[str] = []
        matched_batch_index: int | None = None
        for classification in visible_classifications:
            if matched_batch_index is None and classification.matched_batch_index is not None:
                matched_batch_index = classification.matched_batch_index
            for tender_id in classification.matched_tender_ids:
                if tender_id and tender_id not in matched_tender_ids:
                    matched_tender_ids.append(tender_id)
        return BicotenderSignalClassification(
            status=BICOTENDER_STATUS_VISIBLE_ITEMS,
            reason="one_or_more_keyword_batches_have_visible_public_items",
            matched_tender_ids=tuple(matched_tender_ids),
            matched_batch_index=matched_batch_index,
        )
    for classification in classifications:
        if classification.status == BICOTENDER_STATUS_REVIEW:
            return classification
    if all(classification.status == BICOTENDER_STATUS_NO_SIGNAL for classification in classifications):
        return BicotenderSignalClassification(
            status=BICOTENDER_STATUS_NO_KEYWORD_ITEMS_AFTER_INN_PREFLIGHT,
            reason="all_keyword_batches_returned_clean_zero_results_after_inn_preflight",
        )
    return BicotenderSignalClassification(
        status=BICOTENDER_STATUS_NO_SIGNAL,
        reason="all_applied_keyword_batches_returned_no_signal",
    )


def _operator_batch_status(evidence: BicotenderListEvidence) -> BicotenderOperatorBatchStatus:
    visible_count = _operator_visible_count(evidence)
    matched_positive_items = tuple(item for item in evidence.items if item.matched_positive_terms)
    batch_index = evidence.batch_index
    label = f"batch {batch_index}" if batch_index is not None else "batch"
    return BicotenderOperatorBatchStatus(
        batch_index=batch_index,
        primary_status=f"{label}: {_visible_item_count_text(visible_count)}",
        visible_count=visible_count,
        matched_positive_count=len(matched_positive_items),
        search_url=evidence.search_url,
        source_state=_operator_source_state(evidence),
        access_note=_operator_access_note(evidence),
        technical_internal_status=evidence.classification_status,
        technical_internal_reason=evidence.classification_reason,
        matched_positive_tender_ids=tuple(item.tender_id for item in matched_positive_items if item.tender_id),
    )


def _operator_visible_count(evidence: BicotenderListEvidence) -> int:
    if evidence.parsed_item_count:
        return evidence.parsed_item_count
    if evidence.items:
        return len(evidence.items)
    return evidence.visible_count


def _operator_source_state(evidence: BicotenderListEvidence) -> str:
    if evidence.errors:
        return "source_issue"
    if not evidence.query_applied:
        return "query_unconfirmed"
    if evidence.access_reason == "ambiguous_no_usable_public_rows_with_static_access_marker":
        return "ambiguous"
    return "ok"


def _operator_access_note(evidence: BicotenderListEvidence) -> str:
    if evidence.errors:
        return _combine_operator_notes(evidence.errors)
    return evidence.access_reason


def _combine_operator_source_states(states: Sequence[str], *, fallback: str = "ok") -> str:
    materialized = tuple(state for state in states if state)
    if not materialized:
        return fallback
    if any(state == "source_issue" for state in materialized):
        return "source_issue"
    if any(state == "ambiguous" for state in materialized):
        return "ambiguous"
    if any(state == "query_unconfirmed" for state in materialized):
        return "query_unconfirmed"
    return "ok"


def _combine_operator_notes(notes: Sequence[str]) -> str:
    unique: list[str] = []
    for note in notes:
        if note and note not in unique:
            unique.append(note)
    return "; ".join(unique)


def _visible_item_count_text(count: int) -> str:
    noun = "item" if count == 1 else "items"
    return f"{count} visible {noun}"


def _visible_keyword_item_count_text(count: int) -> str:
    noun = "item" if count == 1 else "items"
    return f"{count} visible keyword {noun}"


def _visible_public_item_count_text(count: int) -> str:
    noun = "item" if count == 1 else "items"
    return f"{count} visible public {noun}"


def _operator_company_status_text(
    visible_count: int,
    *,
    keyword_batch_count: int,
    source_state: str = "ok",
) -> str:
    count_text = _visible_keyword_item_count_text(visible_count)
    suffix = f"{count_text} across {keyword_batch_count} keyword batches"
    if source_state == "source_issue":
        return f"partial source error with {suffix}"
    return suffix


def _operator_preflight_status_text(evidence: BicotenderListEvidence) -> str:
    if evidence.query_applied and not evidence.errors and evidence.visible_count == 0:
        return "no public items by INN"
    total = f" of {evidence.total_count} total" if evidence.total_count is not None else ""
    return f"INN preflight: {_visible_public_item_count_text(evidence.visible_count)}{total}"


def _coerce_fetch_response(response: BicotenderFetchResponse | str) -> BicotenderFetchResponse:
    if isinstance(response, BicotenderFetchResponse):
        return response
    return BicotenderFetchResponse(html=str(response))


def _with_evidence_errors(
    evidence: BicotenderListEvidence,
    errors: tuple[str, ...],
) -> BicotenderListEvidence:
    return replace(evidence, errors=tuple((*evidence.errors, *errors)))


def _with_evidence_status(
    evidence: BicotenderListEvidence,
    *,
    access: BicotenderPublicAccessAssessment,
    classification: BicotenderSignalClassification,
) -> BicotenderListEvidence:
    return replace(
        evidence,
        access_status=access.status,
        access_reason=access.reason,
        classification_status=classification.status,
        classification_reason=classification.reason,
    )


def _evidence_count(evidence: BicotenderListEvidence) -> int | None:
    if evidence.total_count is not None:
        return evidence.total_count
    return evidence.visible_count


def _http_is_error(http_status: int | None) -> bool:
    return http_status is not None and http_status >= 400


def _pairs_to_map(pairs: Sequence[tuple[str, str]]) -> dict[str, tuple[str, ...]]:
    grouped: dict[str, list[str]] = {}
    for name, value in pairs:
        grouped.setdefault(name, []).append(value)
    return {name: tuple(values) for name, values in grouped.items()}


def _digits_only(value: str) -> str:
    return re.sub(r"\D", "", value or "")


def _normalize_space(value: str) -> str:
    return re.sub(r"\s+", " ", value or "").strip()

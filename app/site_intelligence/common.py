from __future__ import annotations

import re
from typing import Iterable
from urllib.parse import urlparse

from bs4 import BeautifulSoup


def _flatten_route_hints(*route_groups: dict[str, tuple[str, ...]]) -> list[tuple[str, str]]:
    flattened: list[tuple[str, str]] = []
    for route_group in route_groups:
        for section_name, hints in route_group.items():
            flattened.extend((hint, section_name) for hint in hints)
    return flattened


def _merge_keyword_groups(*keyword_groups: dict[str, int]) -> dict[str, int]:
    merged: dict[str, int] = {}
    for keyword_group in keyword_groups:
        for keyword, weight in keyword_group.items():
            merged[keyword] = max(weight, merged.get(keyword, 0))
    return merged


CORPORATE_ROUTE_HINTS: dict[str, tuple[str, ...]] = {
    "contacts": ("/contacts", "/contact", "/kontakt", "/kontakty", "контакт", "feedback", "rekvizit"),
    "about": ("/about", "/o-kompanii", "/company", "о компании", "about-us", "enterprise"),
    "products": ("/products", "/product", "/catalog", "/produk", "product", "catalog", "продук", "каталог"),
    "services": ("/services", "/service", "/uslug", "service", "услуг", "сервис"),
    "procurement": ("/procurement", "/purchases", "/zakupki", "/zakup", "/tenders", "/tender", "/torgi", "/torg"),
    "news": ("/news", "/press", "/blog", "news", "новост", "пресс"),
    "documents": ("/documents", "/document", "/docs", "/cert", "/sert", "/certificate", "сертифик", "документ"),
    "vacancies": ("/vacancies", "/career", "/jobs", "/vacanc", "/job", "ваканс", "карьер", "работ"),
    "branches": ("/branch", "/warehouse", "/office", "/filial", "/sklad", "филиал", "офис"),
    "files": ("/files", "/download"),
    "search": ("/search", "search?", "поиск", "query="),
}

SURPLUS_ROUTE_HINTS: dict[str, tuple[str, ...]] = {
    "sales": (
        "/sale",
        "/sales",
        "/realiz",
        "/nelikvid",
        "/metallolom",
        "/scrap",
        "/demontazh",
        "/mtr",
        "/tmc",
        "реализ",
        "продаж",
        "неликвид",
        "остатк",
        "металлолом",
        "лом",
        "невостреб",
        "демонтаж",
        "отход",
        "вторсыр",
        "вторич",
        "tmc",
        "мтр",
    ),
}

SITE_PROBE_ROUTE_HINTS = _flatten_route_hints(SURPLUS_ROUTE_HINTS, CORPORATE_ROUTE_HINTS)

DOCUMENT_EXTENSIONS = (".pdf", ".doc", ".docx", ".xls", ".xlsx", ".csv", ".txt", ".json", ".zip", ".rar", ".7z")

SPA_MARKERS = (
    "data-reactroot",
    "__next_data__",
    'id="__next"',
    "id='__next'",
    'id="root"',
    "id='root'",
    'id="app"',
    "id='app'",
    "ng-version",
    "webpack",
    "vite",
    "nuxt",
    "vue",
)

CMS_SIGNATURES: list[tuple[str, str]] = [
    ("1c-bitrix", "bitrix"),
    ("bitrix", "bitrix"),
    ("wp-content", "wordpress"),
    ("wordpress", "wordpress"),
    ("drupal-settings-json", "drupal"),
    ("joomla", "joomla"),
    ("/typo3/", "typo3"),
    (".aspx", "aspnet"),
    ("asp.net", "aspnet"),
]

SITE_IDENTITY_KEYWORDS = {
    "завод": 3,
    "предприят": 2,
    "производ": 3,
    "изготов": 2,
    "переработ": 3,
    "цех": 2,
    "комбинат": 3,
    "фабрик": 3,
    "литейн": 3,
    "металл": 2,
    "металлоконструк": 3,
    "сталь": 2,
    "труба": 2,
    "арматур": 2,
    "листовой": 2,
    "переплав": 2,
    "станк": 2,
    "оборудован": 1,
    "машиностро": 3,
    "промышлен": 2,
    "конвейер": 1,
    "manufactur": 3,
    "factory": 3,
    "plant": 3,
    "processing": 3,
}

LEAD_FAMILY_KEYWORDS: dict[str, dict[str, int]] = {
    "procurement": {
        "тендер": 3,
        "торги": 3,
        "закупк": 3,
        "аукцион": 3,
        "конкурс": 2,
        "запрос предложений": 2,
        "лот": 2,
        "извещение": 2,
        "документация": 1,
        "протокол": 1,
    },
    "surplus/realization": {
        "реализац": 3,
        "лом": 2,
        "вторсыр": 2,
        "отход": 2,
        "неликвид": 3,
        "списан": 2,
        "демонтаж": 2,
        "металлоконструк": 3,
        "имущество": 1,
        "оборудован": 1,
        "складские остатки": 3,
        "невостребованные тмц": 3,
        "вторич": 2,
        "металлолом": 3,
    },
    "direct_sale": {
        "продаж": 2,
        "реализуем": 3,
        "коммерческое предложение": 2,
        "прайс": 1,
    },
}

INDUSTRIAL_POSITIVE_KEYWORDS = SITE_IDENTITY_KEYWORDS

LEAD_POSITIVE_KEYWORDS = _merge_keyword_groups(*LEAD_FAMILY_KEYWORDS.values())

SURPLUS_ROUTE_FAMILY = "surplus/realization"
SECTION_ROUTE_FAMILY_MAP = {"sales": SURPLUS_ROUTE_FAMILY}
CORPORATE_IDENTITY_SECTIONS = frozenset({"about", "contacts", "products", "documents", "branches"})
CORPORATE_IDENTITY_ROUTE_FAMILIES = frozenset(
    {"company/about", "contacts", "production/products", "docs/certificates", "branches/warehouses"}
)
SURPLUS_ONLY_KEYWORDS = frozenset(
    keyword for keyword in LEAD_FAMILY_KEYWORDS[SURPLUS_ROUTE_FAMILY] if keyword not in SITE_IDENTITY_KEYWORDS
)

site_identity_keywords = SITE_IDENTITY_KEYWORDS
corporate_route_hints = CORPORATE_ROUTE_HINTS
surplus_route_hints = SURPLUS_ROUTE_HINTS
lead_family_keywords = LEAD_FAMILY_KEYWORDS
surplus_only_keywords = SURPLUS_ONLY_KEYWORDS


def route_family_for_section(section_name: str | None) -> str:
    normalized = normalize_whitespace(section_name).lower()
    if not normalized:
        return ""
    return SECTION_ROUTE_FAMILY_MAP.get(normalized, normalized)


def route_supports_site_identity(*, section_name: str | None = None, route_family: str | None = None) -> bool:
    normalized_section = normalize_whitespace(section_name).lower()
    normalized_family = normalize_whitespace(route_family).lower()
    return normalized_section in CORPORATE_IDENTITY_SECTIONS or normalized_family in CORPORATE_IDENTITY_ROUTE_FAMILIES

LEAD_NEGATIVE_KEYWORDS = {
    "ваканс": 3,
    "политика конфиденциальности": 3,
    "пользовательское соглашение": 3,
    "cookie": 2,
    "о компании": 1,
    "контакты": 1,
    "новости компании": 1,
}


def normalize_whitespace(value: str | None) -> str:
    if not value:
        return ""
    return re.sub(r"\s+", " ", value).strip()


def guess_registered_domain(host: str) -> str:
    host = host.lower().strip(".")
    if not host:
        return ""
    parts = host.split(".")
    if len(parts) <= 2:
        return host
    compound_suffixes = {"com.ru", "net.ru", "org.ru", "gov.ru", "edu.ru", "spb.ru", "msk.ru", "co.uk", "com.tr"}
    suffix = ".".join(parts[-2:])
    if suffix in compound_suffixes and len(parts) >= 3:
        return ".".join(parts[-3:])
    return ".".join(parts[-2:])


def normalize_url(url: str) -> str:
    url = normalize_whitespace(url)
    if not url:
        return ""
    if url.startswith("//"):
        url = "https:" + url
    if not re.match(r"^[a-z]+://", url, flags=re.IGNORECASE):
        url = "https://" + url
    try:
        parsed = urlparse(url)
    except ValueError:
        return ""
    if not parsed.netloc:
        return ""
    return parsed._replace(fragment="").geturl()


def dedupe_preserve_order(values: Iterable[str]) -> list[str]:
    result: list[str] = []
    seen: set[str] = set()
    for value in values:
        if not value:
            continue
        if value in seen:
            continue
        seen.add(value)
        result.append(value)
    return result


def compact_text(value: str | None, limit: int = 220) -> str:
    text = normalize_whitespace(value)
    if not text:
        return ""
    if len(text) <= limit:
        return text
    return text[: max(limit - 1, 0)].rstrip() + "…"


def parse_title_and_meta(soup: BeautifulSoup) -> dict[str, str]:
    title = normalize_whitespace(soup.title.get_text(" ", strip=True) if soup.title else "")
    description = ""
    for selector in ['meta[name="description"]', 'meta[property="og:description"]']:
        tag = soup.select_one(selector)
        if tag and tag.get("content"):
            description = normalize_whitespace(tag["content"])
            if description:
                break
    return {"title": title, "description": description}

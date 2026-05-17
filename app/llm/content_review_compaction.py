from __future__ import annotations

import re
from dataclasses import dataclass

DEFAULT_CONTENT_REVIEW_EXCERPT_CHARS = 1000

_BLOCK_SPLIT_RE = re.compile(r"\s+(?:[|•●▪◦›»/]{1,3})\s+")
_SENTENCE_SPLIT_RE = re.compile(r"(?<=[.!?;])\s+(?=[A-ZА-ЯЁ0-9\"«(])")
_TOKEN_RE = re.compile(r"[A-Za-zА-Яа-яЁё0-9]{2,}")
_PHONE_RE = re.compile(r"(?:\+?\d[\d\-\s()]{8,}\d)")
_EMAIL_RE = re.compile(r"[A-Z0-9._%+\-]+@[A-Z0-9.\-]+\.[A-Z]{2,}", flags=re.IGNORECASE)

_PRIMARY_SIGNAL_STEMS = (
    "реализац",
    "реализуем",
    "продаж",
    "sale",
    "surplus",
    "stock",
    "scrap",
    "неликвид",
    "невостреб",
    "остатк",
    "излиш",
    "tmc",
    "mtr",
    "тмц",
    "мтр",
    "лом",
    "вторсыр",
    "вторич",
    "отход",
    "демонтаж",
    "оборудован",
    "станок",
    "линия",
    "агрегат",
    "металлоконструк",
)
_PRICE_SIGNAL_STEMS = ("цен", "стоимост", "прайс", "price", "руб", "rur", "usd", "eur")
_SPEC_SIGNAL_STEMS = (
    "характерист",
    "спецификац",
    "spec",
    "модель",
    "марка",
    "гост",
    "размер",
    "мощност",
    "квт",
    "тонн",
    "тонна",
    "кг",
    "мм",
    "комплект",
)
_CONTACT_SIGNAL_STEMS = (
    "контакт",
    "contact",
    "тел",
    "phone",
    "email",
    "e-mail",
    "почт",
    "manager",
    "sales@",
)
_BOILERPLATE_PHRASES = (
    "cookie",
    "cookies",
    "политика конфиден",
    "конфиденциальност",
    "обработк персональн",
    "пользовательское соглаш",
    "согласие на обработку",
    "all rights reserved",
    "все права защищены",
    "copyright",
    "карта сайта",
    "sitemap",
)
_NAV_TOKENS = frozenset(
    {
        "главная",
        "home",
        "о компании",
        "about",
        "компания",
        "catalog",
        "каталог",
        "products",
        "product",
        "продукция",
        "services",
        "service",
        "услуги",
        "news",
        "новости",
        "contacts",
        "contact",
        "контакты",
        "feedback",
        "search",
        "поиск",
        "menu",
        "меню",
        "sitemap",
        "карта сайта",
        "login",
        "войти",
        "личный кабинет",
    }
)
_TITLE_STOPWORDS = frozenset(
    {
        "page",
        "about",
        "company",
        "industrial",
        "plant",
        "group",
        "главная",
        "страница",
        "компания",
        "компании",
        "завод",
        "группа",
        "официальный",
        "сайт",
    }
)


@dataclass(frozen=True, slots=True)
class ContentReviewExcerpt:
    text: str
    original_length: int
    final_length: int
    compacted: bool


@dataclass(frozen=True, slots=True)
class _Candidate:
    index: int
    text: str
    score: int


def build_content_review_excerpt(
    *,
    title: str | None,
    cleaned_text: str | None,
    max_chars: int = DEFAULT_CONTENT_REVIEW_EXCERPT_CHARS,
) -> ContentReviewExcerpt:
    normalized_source = _normalize_whitespace(cleaned_text)
    original_length = len(normalized_source)
    if not normalized_source or max_chars <= 0:
        return ContentReviewExcerpt(text="", original_length=original_length, final_length=0, compacted=bool(normalized_source))

    title_tokens = _title_tokens(title)
    candidates = _build_candidates(cleaned_text or "", title_tokens=title_tokens)
    excerpt = _assemble_excerpt(candidates, fallback_text=normalized_source, max_chars=max_chars)
    return ContentReviewExcerpt(
        text=excerpt,
        original_length=original_length,
        final_length=len(excerpt),
        compacted=excerpt != normalized_source,
    )


def _assemble_excerpt(candidates: list[_Candidate], *, fallback_text: str, max_chars: int) -> str:
    if not candidates:
        return _trim_to_limit(fallback_text, max_chars)

    ranked = sorted(candidates, key=lambda candidate: (-candidate.score, candidate.index, len(candidate.text)))
    selected_indices: set[int] = set()
    current_length = 0

    for candidate in ranked:
        if candidate.score < 3 and selected_indices:
            continue
        projected_length = _projected_length(current_length, candidate.text)
        if projected_length > max_chars:
            continue
        selected_indices.add(candidate.index)
        current_length = projected_length
        if current_length >= int(max_chars * 0.72):
            break

    for candidate in candidates:
        if candidate.index in selected_indices:
            continue
        if candidate.score < 0 and selected_indices:
            continue
        projected_length = _projected_length(current_length, candidate.text)
        if projected_length > max_chars:
            continue
        selected_indices.add(candidate.index)
        current_length = projected_length
        if current_length >= max_chars:
            break

    if not selected_indices:
        return _trim_to_limit(fallback_text, max_chars)

    ordered = [candidate.text for candidate in candidates if candidate.index in selected_indices]
    return _trim_to_limit("\n".join(ordered), max_chars)


def _build_candidates(text: str, *, title_tokens: set[str]) -> list[_Candidate]:
    candidates: list[_Candidate] = []
    seen_signatures: set[str] = set()

    for raw_chunk in _iter_chunks(text):
        chunk = _normalize_whitespace(raw_chunk)
        if not chunk:
            continue
        signature = chunk.casefold()
        if signature in seen_signatures:
            continue
        seen_signatures.add(signature)

        score = _score_chunk(chunk, index=len(candidates), title_tokens=title_tokens)
        if score <= -50:
            continue
        candidates.append(_Candidate(index=len(candidates), text=chunk, score=score))

    return candidates


def _iter_chunks(text: str) -> list[str]:
    normalized = text.replace("\r\n", "\n").replace("\r", "\n")
    raw_lines = [line for line in normalized.split("\n") if _normalize_whitespace(line)]
    units = raw_lines if len(raw_lines) > 2 else [normalized]

    chunks: list[str] = []
    for unit in units:
        inline_parts = _BLOCK_SPLIT_RE.split(unit) if _BLOCK_SPLIT_RE.search(unit) else [unit]
        for part in inline_parts:
            cleaned = _normalize_whitespace(part)
            if not cleaned:
                continue
            if len(cleaned) <= 260:
                chunks.append(cleaned)
                continue
            sentences = _SENTENCE_SPLIT_RE.split(cleaned)
            if len(sentences) == 1:
                chunks.append(cleaned)
                continue
            chunks.extend(sentence for sentence in sentences if _normalize_whitespace(sentence))
    return chunks


def _score_chunk(text: str, *, index: int, title_tokens: set[str]) -> int:
    lowered = text.casefold()
    if _is_boilerplate(text, lowered):
        return -100

    score = 0
    primary_hits = _match_count(lowered, _PRIMARY_SIGNAL_STEMS)
    price_hits = _match_count(lowered, _PRICE_SIGNAL_STEMS)
    spec_hits = _match_count(lowered, _SPEC_SIGNAL_STEMS)
    has_contact = _has_contact_signal(text, lowered)
    is_header = _looks_like_header(text, lowered)

    if primary_hits:
        score += 6 + min(primary_hits, 4)
    if price_hits:
        score += 3 + min(price_hits, 2)
    if spec_hits:
        score += 2 + min(spec_hits, 2)
    if has_contact:
        score += 4
    if is_header:
        score += 4

    if title_tokens:
        overlap = len(title_tokens.intersection(_title_tokens(text)))
        score += min(overlap, 3)

    if ":" in text and len(text) <= 220:
        score += 1
    if re.search(r"\d", text):
        score += 1
    if 30 <= len(text) <= 220:
        score += 1
    if index < 2:
        score += 2
    if len(text) > 280:
        score -= 1

    return score


def _is_boilerplate(text: str, lowered: str) -> bool:
    if _has_priority_signal(text, lowered):
        return False

    if any(phrase in lowered for phrase in _BOILERPLATE_PHRASES):
        return True

    nav_hits = sum(1 for token in _NAV_TOKENS if token in lowered)
    words = _TOKEN_RE.findall(lowered)
    if nav_hits >= 3 and len(text) <= 180:
        return True
    if nav_hits >= 2 and len(words) <= 6:
        return True
    if len(words) <= 2 and lowered in _NAV_TOKENS:
        return True
    return False


def _has_priority_signal(text: str, lowered: str) -> bool:
    return bool(
        _match_count(lowered, _PRIMARY_SIGNAL_STEMS)
        or _match_count(lowered, _PRICE_SIGNAL_STEMS)
        or _match_count(lowered, _SPEC_SIGNAL_STEMS)
        or _has_contact_signal(text, lowered)
    )


def _has_contact_signal(text: str, lowered: str) -> bool:
    return bool(
        _PHONE_RE.search(text)
        or _EMAIL_RE.search(text)
        or any(stem in lowered for stem in _CONTACT_SIGNAL_STEMS)
    )


def _looks_like_header(text: str, lowered: str) -> bool:
    words = _TOKEN_RE.findall(text)
    if not words or len(words) > 12:
        return False
    if len(text) > 140:
        return False
    if text.endswith((".", "!", "?", ";")):
        return False
    if lowered in _NAV_TOKENS:
        return False
    if sum(1 for token in _NAV_TOKENS if token in lowered) >= 2:
        return False
    return True


def _title_tokens(value: str | None) -> set[str]:
    tokens: set[str] = set()
    for token in _TOKEN_RE.findall(_normalize_whitespace(value).casefold()):
        if len(token) < 4 or token.isdigit() or token in _TITLE_STOPWORDS:
            continue
        tokens.add(token)
    return tokens


def _match_count(text: str, stems: tuple[str, ...]) -> int:
    return sum(1 for stem in stems if stem in text)


def _normalize_whitespace(value: str | None) -> str:
    if not value:
        return ""
    return re.sub(r"\s+", " ", value).strip()


def _projected_length(current_length: int, candidate_text: str) -> int:
    separator = 1 if current_length else 0
    return current_length + separator + len(candidate_text)


def _trim_to_limit(text: str, max_chars: int) -> str:
    if len(text) <= max_chars:
        return text
    if max_chars <= 3:
        return text[:max_chars]
    return text[: max_chars - 3].rstrip() + "..."

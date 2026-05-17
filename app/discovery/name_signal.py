from __future__ import annotations

import re
from dataclasses import dataclass, field


NAME_SIGNAL_STATUS_NO_NAME = "no_name"
NAME_SIGNAL_STATUS_NO_MARKERS = "no_markers"
NAME_SIGNAL_STATUS_SECONDARY = "secondary_signal"
NAME_SIGNAL_STATUS_FALLBACK = "fallback_signal"

NAME_SIGNAL_VERDICT_NONE = "none"
NAME_SIGNAL_VERDICT_WEAK_REMOTE = "weak_remote_hint"
NAME_SIGNAL_VERDICT_STRONG_REMOTE = "strong_remote_hint"

COMPANY_TOKEN_STOPWORDS = frozenset(
    {
        "company",
        "group",
        "holding",
        "industrial",
        "logistic",
        "service",
        "trade",
        "trading",
        "группа",
        "компания",
        "логистика",
        "металл",
        "плюс",
        "ресурс",
        "сервис",
        "система",
        "строй",
        "технологии",
        "торг",
        "трейд",
        "центр",
    }
)

_NON_ALNUM_RE = re.compile(r"[^0-9a-zа-яё]+", flags=re.IGNORECASE)
_MULTISPACE_RE = re.compile(r"\s+")
_LEGAL_FORM_RE = re.compile(r"\b(ооо|ао|пао|зао|ип|оао|нпо|пк|пз|завод|филиал)\b", flags=re.IGNORECASE)


@dataclass(frozen=True)
class _RemoteMarker:
    prefix: str
    marker_label: str
    verdict: str
    risk_weight: int
    reason_code: str


@dataclass
class NameSignalResult:
    signal_status: str = NAME_SIGNAL_STATUS_NO_NAME
    source_name: str = ""
    verdict: str = NAME_SIGNAL_VERDICT_NONE
    risk_weight: int = 0
    matched_markers: list[str] = field(default_factory=list)
    reason_codes: list[str] = field(default_factory=list)


_REMOTE_MARKERS: tuple[_RemoteMarker, ...] = (
    _RemoteMarker("дальневост", "дальневост*", NAME_SIGNAL_VERDICT_STRONG_REMOTE, 3, "marker_dalnevost"),
    _RemoteMarker("сахалин", "сахалин*", NAME_SIGNAL_VERDICT_STRONG_REMOTE, 3, "marker_sakhalin"),
    _RemoteMarker("камчат", "камчат*", NAME_SIGNAL_VERDICT_STRONG_REMOTE, 3, "marker_kamchat"),
    _RemoteMarker("якут", "якут*", NAME_SIGNAL_VERDICT_STRONG_REMOTE, 3, "marker_yakut"),
    _RemoteMarker("примор", "примор*", NAME_SIGNAL_VERDICT_STRONG_REMOTE, 3, "marker_primor"),
    _RemoteMarker("хабаров", "хабаров*", NAME_SIGNAL_VERDICT_STRONG_REMOTE, 3, "marker_khabarov"),
    _RemoteMarker("магадан", "магадан*", NAME_SIGNAL_VERDICT_STRONG_REMOTE, 3, "marker_magadan"),
    _RemoteMarker("чукот", "чукот*", NAME_SIGNAL_VERDICT_STRONG_REMOTE, 3, "marker_chukot"),
    _RemoteMarker("сибир", "сибир*", NAME_SIGNAL_VERDICT_WEAK_REMOTE, 1, "marker_siberia"),
    _RemoteMarker("урал", "урал*", NAME_SIGNAL_VERDICT_WEAK_REMOTE, 1, "marker_ural"),
    _RemoteMarker("кузбасс", "кузбасс*", NAME_SIGNAL_VERDICT_WEAK_REMOTE, 1, "marker_kuzbass"),
    _RemoteMarker("алтай", "алтай*", NAME_SIGNAL_VERDICT_WEAK_REMOTE, 1, "marker_altai"),
    _RemoteMarker("байкал", "байкал*", NAME_SIGNAL_VERDICT_WEAK_REMOTE, 1, "marker_baikal"),
    _RemoteMarker("забайкал", "забайкал*", NAME_SIGNAL_VERDICT_WEAK_REMOTE, 1, "marker_zabaikal"),
)


def normalize_whitespace(value: str | None) -> str:
    if not value:
        return ""
    return _MULTISPACE_RE.sub(" ", str(value)).strip()


def normalize_company_name(value: str | None) -> str:
    text = normalize_whitespace(value)
    text = text.replace('"', " ").replace("«", " ").replace("»", " ")
    text = _LEGAL_FORM_RE.sub(" ", text)
    return normalize_whitespace(text)


def company_tokens(value: str | None) -> set[str]:
    text = normalize_company_name(value).lower().replace("ё", "е")
    tokens = _NON_ALNUM_RE.sub(" ", text).split()
    return {
        token
        for token in tokens
        if len(token) >= 4 and token not in COMPANY_TOKEN_STOPWORDS and not token.isdigit()
    }


def detect_name_signal(
    source_name: str | None,
    *,
    geo_match_status: str = "",
) -> NameSignalResult:
    cleaned_name = normalize_whitespace(source_name)
    if not cleaned_name:
        return NameSignalResult(
            signal_status=NAME_SIGNAL_STATUS_NO_NAME,
            source_name="",
            verdict=NAME_SIGNAL_VERDICT_NONE,
            risk_weight=0,
            matched_markers=[],
            reason_codes=["no_source_name"],
        )

    tokens = company_tokens(cleaned_name)
    matched_markers: list[_RemoteMarker] = []
    for marker in _REMOTE_MARKERS:
        if any(token.startswith(marker.prefix) for token in tokens):
            matched_markers.append(marker)

    if not matched_markers:
        return NameSignalResult(
            signal_status=NAME_SIGNAL_STATUS_NO_MARKERS,
            source_name=cleaned_name,
            verdict=NAME_SIGNAL_VERDICT_NONE,
            risk_weight=0,
            matched_markers=[],
            reason_codes=["no_remote_markers"],
        )

    has_strong_marker = any(marker.verdict == NAME_SIGNAL_VERDICT_STRONG_REMOTE for marker in matched_markers)
    verdict = NAME_SIGNAL_VERDICT_STRONG_REMOTE if has_strong_marker else NAME_SIGNAL_VERDICT_WEAK_REMOTE
    risk_weight = 3 if has_strong_marker else 1
    signal_status = NAME_SIGNAL_STATUS_SECONDARY if geo_match_status == "matched" else NAME_SIGNAL_STATUS_FALLBACK

    matched_marker_labels: list[str] = []
    reason_codes: list[str] = []
    for marker in matched_markers:
        if marker.marker_label not in matched_marker_labels:
            matched_marker_labels.append(marker.marker_label)
        if marker.reason_code not in reason_codes:
            reason_codes.append(marker.reason_code)
    reason_codes.append("geo_matched_secondary_only" if geo_match_status == "matched" else "geo_fallback_naming_hint")

    return NameSignalResult(
        signal_status=signal_status,
        source_name=cleaned_name,
        verdict=verdict,
        risk_weight=risk_weight,
        matched_markers=matched_marker_labels,
        reason_codes=reason_codes,
    )


__all__ = [
    "COMPANY_TOKEN_STOPWORDS",
    "NAME_SIGNAL_STATUS_FALLBACK",
    "NAME_SIGNAL_STATUS_NO_MARKERS",
    "NAME_SIGNAL_STATUS_NO_NAME",
    "NAME_SIGNAL_STATUS_SECONDARY",
    "NAME_SIGNAL_VERDICT_NONE",
    "NAME_SIGNAL_VERDICT_STRONG_REMOTE",
    "NAME_SIGNAL_VERDICT_WEAK_REMOTE",
    "NameSignalResult",
    "company_tokens",
    "detect_name_signal",
    "normalize_company_name",
    "normalize_whitespace",
]

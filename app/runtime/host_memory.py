from __future__ import annotations

from collections.abc import Iterable, Mapping
from typing import Any


HOST_MEMORY_LEDGER_LIMIT = 25
HOST_MEMORY_SIGNAL_HTTP_429 = "http_429"
HOST_MEMORY_SIGNAL_HTTP_403 = "http_403"
HOST_MEMORY_SIGNAL_BOT_GATE = "bot_gate"
HOST_MEMORY_SIGNAL_CHALLENGE = "challenge"
HOST_MEMORY_SIGNAL_COOLDOWN = "cooldown"
HOST_MEMORY_GOVERNOR_SIGNAL_TAGS = frozenset(
    {
        HOST_MEMORY_SIGNAL_HTTP_429,
        HOST_MEMORY_SIGNAL_HTTP_403,
        HOST_MEMORY_SIGNAL_BOT_GATE,
        HOST_MEMORY_SIGNAL_CHALLENGE,
        HOST_MEMORY_SIGNAL_COOLDOWN,
    }
)
HOST_MEMORY_SUCCESS_STATUSES = frozenset({"success"})
HOST_MEMORY_SUCCESS_BLOCK_CLASSES = frozenset({"SUCCESS"})
HOST_MEMORY_SUCCESS_ACCESS_STATES = frozenset({"completed_with_content", "recovered"})
_HOST_MEMORY_DIRECT_CONTOUR = "__direct__"


def normalize_host_memory_state(payload: Any) -> dict[str, Any]:
    root = dict(payload) if isinstance(payload, Mapping) else {}
    normalized: dict[str, Any] = {}
    for raw_host in sorted(root.keys(), key=lambda item: str(item).strip().lower()):
        host = _normalize_text(raw_host).lower()
        if not host:
            continue
        bucket = dict(root.get(raw_host)) if isinstance(root.get(raw_host), Mapping) else {}
        recent_attempts = bucket.get("recent_attempts")
        normalized_attempts = _normalize_recent_attempts(recent_attempts, host=host)
        first_event_at = _normalize_text(bucket.get("first_event_at"))
        last_event_at = _normalize_text(bucket.get("last_event_at"))
        if normalized_attempts:
            if not first_event_at:
                first_event_at = normalized_attempts[0]["ts"]
            if not last_event_at:
                last_event_at = normalized_attempts[-1]["ts"]
        normalized[host] = {
            "first_event_at": first_event_at,
            "last_event_at": last_event_at,
            "recent_attempts": normalized_attempts,
        }
    return normalized


def update_host_memory_from_event_payload(
    state: dict[str, Any],
    event_payload: Mapping[str, Any] | None,
    *,
    ts: str,
) -> bool:
    if not isinstance(event_payload, Mapping):
        return False
    entry = _normalize_attempt_entry(event_payload, host=_normalize_text(event_payload.get("host")).lower(), ts=ts)
    host = entry["host"]
    if not host or not entry["event_type"]:
        return False
    bucket = state.setdefault(
        host,
        {
            "first_event_at": entry["ts"],
            "last_event_at": entry["ts"],
            "recent_attempts": [],
        },
    )
    if not isinstance(bucket.get("recent_attempts"), list):
        bucket["recent_attempts"] = []
    if not _normalize_text(bucket.get("first_event_at")):
        bucket["first_event_at"] = entry["ts"]
    bucket["last_event_at"] = entry["ts"]
    recent_attempts = list(bucket["recent_attempts"])
    recent_attempts.append(entry)
    bucket["recent_attempts"] = recent_attempts[-HOST_MEMORY_LEDGER_LIMIT:]
    return True


def recent_host_proxy_outcomes(
    state: Mapping[str, Any] | None,
    host: str,
    *,
    signal_tags: Iterable[str] | None = None,
    proxy_label_or_id: str | None = None,
    limit: int | None = None,
) -> list[dict[str, Any]]:
    normalized_state = normalize_host_memory_state(state)
    bucket = normalized_state.get(_normalize_text(host).lower())
    if not bucket:
        return []
    requested_tags = _normalize_requested_signal_tags(signal_tags)
    requested_proxy = _normalize_text(proxy_label_or_id)
    normalized_limit = max(int(limit or 0), 0)
    matched: list[dict[str, Any]] = []
    for item in reversed(bucket["recent_attempts"]):
        if requested_proxy and item["proxy_label_or_id"] != requested_proxy:
            continue
        if requested_tags and requested_tags.isdisjoint(item["signal_tags"]):
            continue
        matched.append(dict(item))
        if normalized_limit and len(matched) >= normalized_limit:
            break
    return matched


def recent_host_proxy_labels(
    state: Mapping[str, Any] | None,
    host: str,
    *,
    signal_tags: Iterable[str] | None = None,
    limit: int | None = None,
) -> list[str]:
    seen: set[str] = set()
    labels: list[str] = []
    for item in recent_host_proxy_outcomes(state, host, signal_tags=signal_tags):
        proxy_label = item["proxy_label_or_id"]
        if not proxy_label or proxy_label in seen:
            continue
        seen.add(proxy_label)
        labels.append(proxy_label)
        if limit is not None and len(labels) >= max(int(limit), 0):
            break
    return labels


def recent_governor_signal_proxy_labels(
    state: Mapping[str, Any] | None,
    host: str,
    *,
    limit: int | None = None,
) -> list[str]:
    seen: set[str] = set()
    labels: list[str] = []
    for item in active_governor_signal_outcomes(state, host):
        proxy_label = item["proxy_label_or_id"]
        if not proxy_label or proxy_label in seen:
            continue
        seen.add(proxy_label)
        labels.append(proxy_label)
        if limit is not None and len(labels) >= max(int(limit), 0):
            break
    return labels


def active_governor_signal_outcomes(
    state: Mapping[str, Any] | None,
    host: str,
    *,
    limit: int | None = None,
) -> list[dict[str, Any]]:
    normalized_state = normalize_host_memory_state(state)
    bucket = normalized_state.get(_normalize_text(host).lower())
    if not bucket:
        return []
    normalized_limit = max(int(limit or 0), 0)
    cleared_contours: set[str] = set()
    matched: list[dict[str, Any]] = []
    for item in reversed(bucket["recent_attempts"]):
        contour_key = _attempt_contour_key(item)
        if _attempt_clears_governor_debt(item):
            cleared_contours.add(contour_key)
            continue
        if HOST_MEMORY_GOVERNOR_SIGNAL_TAGS.isdisjoint(item["signal_tags"]):
            continue
        if contour_key in cleared_contours:
            continue
        matched.append(dict(item))
        if normalized_limit and len(matched) >= normalized_limit:
            break
    return matched


def _normalize_recent_attempts(value: Any, *, host: str) -> list[dict[str, Any]]:
    if not isinstance(value, list):
        return []
    normalized_attempts: list[dict[str, Any]] = []
    for item in value[-HOST_MEMORY_LEDGER_LIMIT:]:
        if not isinstance(item, Mapping):
            continue
        normalized_attempts.append(_normalize_attempt_entry(item, host=host, ts=_normalize_text(item.get("ts"))))
    return normalized_attempts


def _normalize_attempt_entry(payload: Mapping[str, Any], *, host: str, ts: str) -> dict[str, Any]:
    normalized_host = _normalize_text(payload.get("host") or host).lower()
    normalized_ts = _normalize_text(ts or payload.get("ts"))
    status = _normalize_text(payload.get("status"))
    anti_bot_reason = _normalize_text(payload.get("anti_bot_reason"))
    http_status = _normalize_optional_int(payload.get("http_status"))
    cooldown_seconds = _normalize_non_negative_float(payload.get("cooldown_seconds"))
    interval_seconds = _normalize_optional_float(payload.get("interval_seconds"))
    challenge_detected = _normalize_bool(payload.get("challenge_detected"))
    signal_tags = _derive_signal_tags(
        status=status,
        anti_bot_reason=anti_bot_reason,
        http_status=http_status,
        challenge_detected=challenge_detected,
        cooldown_seconds=cooldown_seconds,
    )
    return {
        "ts": normalized_ts,
        "host": normalized_host,
        "event_type": _normalize_text(payload.get("event_type")),
        "source": _normalize_text(payload.get("source")),
        "status": status,
        "anti_bot_reason": anti_bot_reason,
        "block_class": _normalize_text(payload.get("block_class")),
        "http_status": http_status,
        "proxy_label_or_id": _normalize_text(payload.get("proxy_label_or_id") or payload.get("proxy_label")),
        "cooldown_seconds": cooldown_seconds,
        "interval_seconds": interval_seconds,
        "access_state": _normalize_text(payload.get("access_state")),
        "transport_selected": _normalize_text(payload.get("transport_selected")),
        "transport_final": _normalize_text(payload.get("transport_final")),
        "challenge_detected": challenge_detected,
        "blocked_by_policy": _normalize_bool(payload.get("blocked_by_policy")),
        "signal_tags": signal_tags,
    }


def _derive_signal_tags(
    *,
    status: str,
    anti_bot_reason: str,
    http_status: int | None,
    challenge_detected: bool,
    cooldown_seconds: float,
) -> list[str]:
    tags: list[str] = []
    normalized_status = status.lower()
    normalized_reason = anti_bot_reason.lower()
    if cooldown_seconds > 0:
        tags.append(HOST_MEMORY_SIGNAL_COOLDOWN)
    if (
        http_status == 429
        or normalized_status in {"http_429", "rate_limited"}
        or normalized_reason == "http_429"
    ):
        tags.append(HOST_MEMORY_SIGNAL_HTTP_429)
    if http_status == 403 or normalized_status == "http_403" or normalized_reason == "http_403":
        tags.append(HOST_MEMORY_SIGNAL_HTTP_403)
    if normalized_status == "bot_gate" or normalized_reason == "bot_gate":
        tags.append(HOST_MEMORY_SIGNAL_BOT_GATE)
    if challenge_detected or "challenge" in normalized_status or "challenge" in normalized_reason:
        tags.append(HOST_MEMORY_SIGNAL_CHALLENGE)
    return tags


def _normalize_requested_signal_tags(signal_tags: Iterable[str] | None) -> set[str]:
    if signal_tags is None:
        return set()
    normalized: set[str] = set()
    for item in signal_tags:
        value = _normalize_text(item)
        if value:
            normalized.add(value)
    return normalized


def _attempt_contour_key(item: Mapping[str, Any]) -> str:
    return _normalize_text(item.get("proxy_label_or_id")) or _HOST_MEMORY_DIRECT_CONTOUR


def _attempt_clears_governor_debt(item: Mapping[str, Any]) -> bool:
    status = _normalize_text(item.get("status")).lower()
    block_class = _normalize_text(item.get("block_class")).upper()
    access_state = _normalize_text(item.get("access_state")).lower()
    return (
        status in HOST_MEMORY_SUCCESS_STATUSES
        or block_class in HOST_MEMORY_SUCCESS_BLOCK_CLASSES
        or access_state in HOST_MEMORY_SUCCESS_ACCESS_STATES
    )


def _normalize_text(value: Any) -> str:
    return str(value or "").strip()


def _normalize_optional_int(value: Any) -> int | None:
    if value in (None, "") or isinstance(value, bool):
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _normalize_optional_float(value: Any) -> float | None:
    if value in (None, "") or isinstance(value, bool):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _normalize_non_negative_float(value: Any) -> float:
    normalized = _normalize_optional_float(value)
    if normalized is None:
        return 0.0
    return max(float(normalized), 0.0)


def _normalize_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if value in (0, 1):
        return bool(value)
    text = _normalize_text(value).lower()
    return text in {"1", "true", "yes"}


__all__ = [
    "HOST_MEMORY_GOVERNOR_SIGNAL_TAGS",
    "HOST_MEMORY_SIGNAL_BOT_GATE",
    "HOST_MEMORY_SIGNAL_CHALLENGE",
    "HOST_MEMORY_SIGNAL_COOLDOWN",
    "HOST_MEMORY_SIGNAL_HTTP_403",
    "HOST_MEMORY_SIGNAL_HTTP_429",
    "active_governor_signal_outcomes",
    "normalize_host_memory_state",
    "recent_governor_signal_proxy_labels",
    "recent_host_proxy_labels",
    "recent_host_proxy_outcomes",
    "update_host_memory_from_event_payload",
]

from __future__ import annotations

import os
import re
import time
from dataclasses import dataclass
from enum import Enum
from typing import Protocol

import requests


class CaptchaKind(str, Enum):
    TURNSTILE = "turnstile"
    RECAPTCHA_V2 = "recaptcha_v2"
    RECAPTCHA_V3 = "recaptcha_v3"
    HCAPTCHA = "hcaptcha"
    UNKNOWN = "unknown"


@dataclass(frozen=True)
class CaptchaChallenge:
    kind: CaptchaKind
    website_url: str
    website_key: str
    page_action: str = ""
    min_score: float = 0.0


@dataclass(frozen=True)
class CaptchaSolution:
    provider: str
    task_type: str
    token: str
    user_agent: str = ""
    task_id: int = 0


class CaptchaProvider(Protocol):
    def solve(self, challenge: CaptchaChallenge) -> CaptchaSolution:
        ...


_KIND_HINTS: dict[str, CaptchaKind] = {
    "turnstile": CaptchaKind.TURNSTILE,
    "cloudflare_turnstile": CaptchaKind.TURNSTILE,
    "cf_turnstile": CaptchaKind.TURNSTILE,
    "cloudflare": CaptchaKind.TURNSTILE,
    "recaptcha_v2": CaptchaKind.RECAPTCHA_V2,
    "recaptcha_v3": CaptchaKind.RECAPTCHA_V3,
    "recaptcha2": CaptchaKind.RECAPTCHA_V2,
    "recaptcha3": CaptchaKind.RECAPTCHA_V3,
    "hcaptcha": CaptchaKind.HCAPTCHA,
}


_CAPMONSTER_TASK_TYPE_CANDIDATES: dict[CaptchaKind, tuple[str, ...]] = {
    CaptchaKind.TURNSTILE: ("TurnstileTask", "TurnstileTaskProxyless"),
    CaptchaKind.RECAPTCHA_V2: ("RecaptchaV2Task", "RecaptchaV2TaskProxyless"),
    CaptchaKind.RECAPTCHA_V3: ("RecaptchaV3TaskProxyless",),
    CaptchaKind.HCAPTCHA: ("HCaptchaTask", "HCaptchaTaskProxyless"),
}


def parse_captcha_kind(value: str) -> CaptchaKind:
    raw = (value or "").strip().lower()
    if not raw:
        return CaptchaKind.UNKNOWN
    return _KIND_HINTS.get(raw, CaptchaKind.UNKNOWN)


def detect_captcha_kind(
    *,
    url: str = "",
    html: str = "",
    body_text: str = "",
    kind_hint: str = "",
) -> CaptchaKind:
    hint_kind = parse_captcha_kind(kind_hint)
    if hint_kind != CaptchaKind.UNKNOWN:
        return hint_kind

    blob = "\n".join([url or "", html or "", body_text or ""]).lower()

    if (
        "cf-turnstile" in blob
        or "challenges.cloudflare.com/turnstile" in blob
        or "name=\"cf-turnstile-response\"" in blob
        or "name='cf-turnstile-response'" in blob
        or "_cf_chl_opt" in blob
    ):
        return CaptchaKind.TURNSTILE

    if (
        "hcaptcha.com/1/api.js" in blob
        or "class=\"h-captcha" in blob
        or "class='h-captcha" in blob
        or "name=\"h-captcha-response\"" in blob
        or "name='h-captcha-response'" in blob
    ):
        return CaptchaKind.HCAPTCHA

    has_recaptcha = "recaptcha" in blob or "g-recaptcha" in blob or "grecaptcha" in blob
    is_v3 = "grecaptcha.execute" in blob or "api.js?render=" in blob
    if has_recaptcha and is_v3:
        return CaptchaKind.RECAPTCHA_V3
    if has_recaptcha:
        return CaptchaKind.RECAPTCHA_V2

    return CaptchaKind.UNKNOWN


def extract_sitekey_from_html(html: str, *, captcha_kind: CaptchaKind = CaptchaKind.UNKNOWN) -> str:
    if not html:
        return ""

    patterns: list[re.Pattern[str]] = [
        re.compile(r'data-sitekey=["\']([^"\']+)["\']', re.IGNORECASE),
        re.compile(r'sitekey[:=]["\']?([0-9A-Za-z_\-\.]+)["\']?', re.IGNORECASE),
        re.compile(r'[?&](?:k|sitekey|render)=([0-9A-Za-z_\-\.]+)', re.IGNORECASE),
        re.compile(r'cSiteKey["\']?\s*[:=]\s*["\']([^"\']+)["\']', re.IGNORECASE),
    ]

    for pattern in patterns:
        match = pattern.search(html)
        if not match:
            continue
        candidate = (match.group(1) or "").strip()
        if not candidate:
            continue
        if captcha_kind == CaptchaKind.RECAPTCHA_V3 and candidate.lower() == "explicit":
            continue
        return candidate
    return ""


class CapMonsterProvider:
    def __init__(
        self,
        *,
        api_key: str,
        create_task_url: str = "https://api.capmonster.cloud/createTask",
        get_result_url: str = "https://api.capmonster.cloud/getTaskResult",
        request_timeout_seconds: float = 15.0,
        poll_interval_seconds: float = 2.0,
        max_polls: int = 60,
    ) -> None:
        key = (api_key or "").strip()
        if not key:
            raise ValueError("CAPMONSTER_API_KEY is required for CapMonster provider")
        self.api_key = key
        self.create_task_url = create_task_url
        self.get_result_url = get_result_url
        self.request_timeout_seconds = request_timeout_seconds
        self.poll_interval_seconds = poll_interval_seconds
        self.max_polls = max_polls

    def solve(self, challenge: CaptchaChallenge) -> CaptchaSolution:
        candidates = _CAPMONSTER_TASK_TYPE_CANDIDATES.get(challenge.kind)
        if not candidates:
            raise ValueError(f"Unsupported captcha kind for CapMonster: {challenge.kind.value}")

        last_error = ""
        created_task_id = 0
        selected_task_type = ""
        for task_type in candidates:
            payload = {"clientKey": self.api_key, "task": self._build_task_payload(task_type, challenge)}
            response = requests.post(self.create_task_url, json=payload, timeout=self.request_timeout_seconds)
            response_data = response.json()
            error_id = int(response_data.get("errorId", 0) or 0)
            if error_id == 0 and response_data.get("taskId"):
                created_task_id = int(response_data["taskId"])
                selected_task_type = task_type
                break

            error_code = str(response_data.get("errorCode", ""))
            last_error = str(response_data)
            if error_code not in {"ERROR_NO_SUCH_METHOD"}:
                break

        if not created_task_id:
            raise RuntimeError(f"CapMonster createTask failed: {last_error}")

        token = ""
        user_agent = ""
        for _ in range(self.max_polls):
            time.sleep(self.poll_interval_seconds)
            poll_payload = {"clientKey": self.api_key, "taskId": created_task_id}
            poll_response = requests.post(self.get_result_url, json=poll_payload, timeout=self.request_timeout_seconds)
            poll_data = poll_response.json()
            error_id = int(poll_data.get("errorId", 0) or 0)
            if error_id != 0:
                raise RuntimeError(f"CapMonster getTaskResult failed: {poll_data}")
            if str(poll_data.get("status", "")).lower() != "ready":
                continue

            solution = poll_data.get("solution", {}) or {}
            token = self._extract_token(solution)
            user_agent = str(solution.get("userAgent", "") or "")
            if token:
                break

        if not token:
            raise TimeoutError("CapMonster timed out waiting for captcha token")

        return CaptchaSolution(
            provider="capmonster",
            task_type=selected_task_type,
            token=token,
            user_agent=user_agent,
            task_id=created_task_id,
        )

    def _build_task_payload(self, task_type: str, challenge: CaptchaChallenge) -> dict[str, object]:
        payload: dict[str, object] = {
            "type": task_type,
            "websiteURL": challenge.website_url,
            "websiteKey": challenge.website_key,
        }
        if challenge.page_action.strip():
            payload["pageAction"] = challenge.page_action.strip()
        if challenge.kind == CaptchaKind.RECAPTCHA_V3 and challenge.min_score > 0:
            payload["minScore"] = challenge.min_score
        return payload

    @staticmethod
    def _extract_token(solution: dict[str, object]) -> str:
        for key in ("token", "gRecaptchaResponse"):
            value = solution.get(key)
            if isinstance(value, str) and value.strip():
                return value.strip()
        return ""


def solve_captcha(
    challenge: CaptchaChallenge,
    *,
    provider_name: str = "",
    api_key: str = "",
) -> CaptchaSolution:
    selected_provider = (provider_name or os.getenv("CAPTCHA_PROVIDER", "capmonster")).strip().lower()
    if selected_provider in {"capmonster", "capmonster_cloud"}:
        key = (api_key or os.getenv("CAPMONSTER_API_KEY", "")).strip()
        provider = CapMonsterProvider(api_key=key)
        return provider.solve(challenge)
    raise ValueError(f"Unsupported captcha provider: {selected_provider}")


__all__ = [
    "CaptchaChallenge",
    "CaptchaKind",
    "CaptchaProvider",
    "CaptchaSolution",
    "CapMonsterProvider",
    "detect_captcha_kind",
    "extract_sitekey_from_html",
    "parse_captcha_kind",
    "solve_captcha",
]

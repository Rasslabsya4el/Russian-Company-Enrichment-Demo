from __future__ import annotations

import os
import re
from typing import Any
from urllib.parse import unquote, urljoin, urlparse

from bs4 import BeautifulSoup

from .antibot import (
    classify_fetch_attempt,
    route_is_high_value,
    resolve_transport_policy,
    TransportPolicyContext,
    TRANSPORT_REQUESTS,
    TRANSPORT_PLAYWRIGHT,
)
from .common import (
    CMS_SIGNATURES,
    DOCUMENT_EXTENSIONS,
    SITE_PROBE_ROUTE_HINTS,
    SPA_MARKERS,
    dedupe_preserve_order,
    guess_registered_domain,
    normalize_url,
    normalize_whitespace,
)
from .models import SiteProbe


class SiteProber:
    def __init__(self, client: Any) -> None:
        self.client = client
        self.max_routes_per_section = max(1, int(os.getenv("SITE_PROBE_MAX_ROUTES_PER_SECTION", "2")))
        self.max_route_checks = max(1, int(os.getenv("SITE_PROBE_MAX_ROUTE_CHECKS", "12")))

    def probe(self, site_url: str) -> SiteProbe:
        probe = SiteProbe(url=site_url, status="pending")
        normalized_url = normalize_url(site_url)
        if not normalized_url:
            probe.status = "invalid_url"
            probe.failure_reason = "invalid_url"
            probe.errors.append("Некорректный URL для probe")
            return probe

        outcome = self.client.request(normalized_url, source="site_probe", timeout=12)
        if not outcome.ok or not outcome.response:
            self._apply_failure_outcome(probe, outcome)
            return probe

        response = outcome.response
        probe.status = "success"
        probe.http_status = response.status_code
        probe.final_url = response.url
        probe.redirect_count = len(response.history or [])
        probe.content_type = normalize_whitespace(response.headers.get("Content-Type", "").lower())
        probe.encoding = normalize_whitespace((response.encoding or "").lower())

        html = response.text if "text" in probe.content_type or "<html" in response.text[:1000].lower() else ""
        probe.html_ok = "html" in probe.content_type or "<html" in html[:1000].lower()
        html_lower = html.lower()
        parsed_final = urlparse(response.url)
        origin = f"{parsed_final.scheme}://{parsed_final.netloc}"
        root_domain = guess_registered_domain(parsed_final.netloc)

        robots = self.client.request(urljoin(origin, "/robots.txt"), source="site_probe_aux", timeout=8)
        if robots.ok and robots.response and robots.response.status_code < 400:
            probe.robots_found = True
        sitemap = self.client.request(urljoin(origin, "/sitemap.xml"), source="site_probe_aux", timeout=8)
        if sitemap.ok and sitemap.response and sitemap.response.status_code < 400:
            probe.sitemap_found = True

        if not probe.html_ok:
            probe.site_class = "F"
            probe.worth_crawling = "false"
            probe.failure_reason = "non_html_content"
            probe.notes.append("главная страница не дала пригодный HTML")
            return probe

        soup = BeautifulSoup(html, "html.parser")
        text = normalize_whitespace(soup.get_text(" ", strip=True))
        probe.text_length = len(text)
        probe.cms_guess = self._guess_cms(html_lower, probe.final_url)

        internal_links: list[str] = []
        document_links: list[str] = []
        route_candidates = self._collect_obvious_route_candidates(
            response_url=response.url,
            origin=origin,
            root_domain=root_domain,
            soup=soup,
        )

        for anchor in soup.select("a[href]"):
            href = normalize_whitespace(anchor.get("href", ""))
            if not href or href.startswith("mailto:") or href.startswith("tel:") or href.startswith("#"):
                continue
            full = normalize_url(urljoin(response.url, href))
            if not full:
                continue
            lower_full = full.lower()
            if any(lower_full.endswith(ext) for ext in DOCUMENT_EXTENSIONS):
                document_links.append(full)
            if guess_registered_domain(urlparse(full).netloc) != root_domain:
                continue
            internal_links.append(full)

        probe.internal_links_count = len(dedupe_preserve_order(internal_links))
        probe.document_links_count = len(dedupe_preserve_order(document_links))
        probe.obvious_routes_attempted = [sample_url for sample_url, _section in route_candidates]

        for sample_url, section_name in route_candidates:
            sample = self.client.request(sample_url, source="site_probe_aux", timeout=10)
            if not sample.ok or not sample.response:
                continue
            sample_response = sample.response
            if sample_response.status_code >= 400:
                continue
            probe.sampled_urls.append(sample_url)
            probe.key_sections.append(section_name)
            sample_content_type = sample_response.headers.get("Content-Type", "").lower()
            sample_html = sample_response.text if "text" in sample_content_type or "<html" in sample_response.text[:1000].lower() else ""
            if sample_html:
                sample_soup = BeautifulSoup(sample_html, "html.parser")
                sample_text = normalize_whitespace(sample_soup.get_text(" ", strip=True))
                probe.text_length += int(len(sample_text) * 0.35)
                for anchor in sample_soup.select("a[href]"):
                    href = normalize_whitespace(anchor.get("href", ""))
                    if not href:
                        continue
                    full = normalize_url(urljoin(sample_response.url, href))
                    if full and any(full.lower().endswith(ext) for ext in DOCUMENT_EXTENSIONS):
                        probe.document_links_count += 1

        probe.key_sections = dedupe_preserve_order(probe.key_sections)
        probe.sampled_urls = dedupe_preserve_order(probe.sampled_urls)
        probe.obvious_routes_attempted = dedupe_preserve_order(probe.obvious_routes_attempted)

        spa_markers_found = sum(1 for marker in SPA_MARKERS if marker in html_lower)
        root_shell = spa_markers_found >= 1 and probe.text_length < 500
        mixed_js = spa_markers_found >= 1 and probe.text_length >= 500
        legacy_html = probe.cms_guess in {"bitrix", "aspnet"} or "windows-1251" in html_lower or response.encoding in {"cp1251", "windows-1251"}

        if root_shell:
            probe.site_class = "D"
            probe.browser_required_default = True
            probe.notes.append("homepage выглядит как JS-shell, контент в сыром HTML слабый")
        elif mixed_js:
            probe.site_class = "C"
            probe.browser_required_default = False
            probe.notes.append("сайт mixed: признаки JS есть, но контент в HTML тоже присутствует")
        elif legacy_html:
            probe.site_class = "B"
            probe.notes.append("старый или кривой серверный HTML, но requests-парсинг выглядит рабочим")
        elif probe.text_length >= 1200 and probe.internal_links_count >= 8:
            probe.site_class = "A"
            probe.notes.append("нормальный HTML-сайт с навигацией и текстом")
        elif probe.text_length >= 350 or probe.internal_links_count >= 3:
            probe.site_class = "B"
            probe.notes.append("HTML живой, но сайт бедный или частично сломан")
        else:
            probe.site_class = "F"
            probe.notes.append("сайт открылся, но полезного контента почти нет")

        if probe.site_class == "D":
            probe.worth_crawling = "true" if (probe.key_sections or probe.document_links_count > 0 or probe.internal_links_count >= 4) else "limited"
        elif probe.site_class in {"A", "B", "C"} and (probe.key_sections or probe.document_links_count > 0 or probe.internal_links_count >= 4):
            probe.worth_crawling = "true"
        elif probe.site_class in {"A", "B", "C"} and (probe.text_length >= 300 or probe.internal_links_count >= 1):
            probe.worth_crawling = "limited"
        elif probe.site_class == "E":
            probe.worth_crawling = "limited"
        else:
            probe.worth_crawling = "false"
        self._apply_policy_snapshot(
            probe,
            status="success",
            response=response,
            text=html,
            usable_content=probe.html_ok and not root_shell and probe.text_length >= 180,
            root_shell=root_shell,
            route_sections=["homepage", *probe.key_sections],
        )
        return probe

    def _guess_cms(self, html_lower: str, final_url: str) -> str:
        composite = html_lower + "\n" + final_url.lower()
        for signature, cms_name in CMS_SIGNATURES:
            if signature in composite:
                return cms_name
        return ""

    def _apply_policy_snapshot(
        self,
        probe: SiteProbe,
        *,
        status: str,
        response: Any,
        text: str,
        usable_content: bool,
        root_shell: bool,
        route_sections: list[str],
    ) -> None:
        host = normalize_whitespace(urlparse(probe.final_url or probe.url).netloc).lower()
        high_value_sections = dedupe_preserve_order(
            section_name
            for section_name in route_sections
            if route_is_high_value(route_family=section_name, section_name=section_name)
        )
        route_family = high_value_sections[0] if high_value_sections else (route_sections[0] if route_sections else "homepage")
        cooldown_active = normalize_whitespace(status).lower() == "cooldown_active"
        policy_decision = resolve_transport_policy(
            TransportPolicyContext(
                host=host,
                requested_mode=TRANSPORT_REQUESTS,
                route_family=route_family,
                section_name=route_family,
                status=status,
                response=response,
                response_text=text,
                usable_content=usable_content,
                attempt_no=1,
                current_transport=TRANSPORT_REQUESTS,
                browser_attempt=False,
                session_reused=False,
                cooldown_active=cooldown_active,
            )
        )
        normalized_symptoms = dedupe_preserve_order(list(policy_decision.symptoms.symptom_codes))
        escalation_reason = policy_decision.escalation_reason
        blocked_by_policy = policy_decision.blocked_by_policy or cooldown_active
        if not probe.challenge_detected:
            probe.challenge_detected = policy_decision.challenge_detected
        if (
            policy_decision.symptoms.challenge_page
            or policy_decision.symptoms.bot_gate
            or policy_decision.symptoms.rate_limited
            or policy_decision.symptoms.hard_block
            or policy_decision.symptoms.auth_required
        ):
            probe.anti_bot_detected = True
        if not escalation_reason and root_shell and not usable_content:
            escalation_reason = "empty_js_shell"
            if escalation_reason not in normalized_symptoms:
                normalized_symptoms.append(escalation_reason)
        if escalation_reason and not blocked_by_policy:
            probe.browser_required_default = probe.browser_required_default or (
                policy_decision.transport_final == TRANSPORT_PLAYWRIGHT
                or root_shell
                or policy_decision.symptoms.redirect_loop
            )

        probe.escalation_reason = escalation_reason
        probe.blocked_by_policy = blocked_by_policy
        probe.normalized_symptoms = normalized_symptoms
        probe.transport_selected = policy_decision.transport_selected
        probe.transport_final = policy_decision.transport_final
        probe.policy_hints = {
            "blocked_by_policy": blocked_by_policy,
            "browser_required_hint": bool(probe.browser_required_default),
            "cooldown_scope": policy_decision.cooldown_key,
            "escalation_candidate": bool(escalation_reason),
            "escalation_reason": escalation_reason,
            "high_value_sections": high_value_sections,
            "normalized_symptoms": normalized_symptoms,
            "route_policy_reason": policy_decision.route_policy.route_policy_reason,
            "transport_selected": policy_decision.transport_selected,
            "transport_final": policy_decision.transport_final,
            "retry_budget_scope": "host",
            "retryable": bool(policy_decision.retry_allowed),
        }

    def _collect_obvious_route_candidates(
        self,
        *,
        response_url: str,
        origin: str,
        root_domain: str,
        soup: BeautifulSoup,
    ) -> list[tuple[str, str]]:
        strong_real_by_section: dict[str, list[str]] = {}
        weak_real_by_section: dict[str, list[str]] = {}
        synthetic_by_section: dict[str, str] = {}
        seen_urls: set[str] = set()

        def normalize_candidate(url: str) -> str:
            normalized = normalize_url(url)
            if not normalized or normalized in seen_urls:
                return ""
            if guess_registered_domain(urlparse(normalized).netloc) != root_domain:
                return ""
            return normalized

        def register_real(url: str, section_name: str, *, strong: bool) -> None:
            normalized = normalize_candidate(url)
            if not normalized:
                return
            bucket = (strong_real_by_section if strong else weak_real_by_section).setdefault(section_name, [])
            if len(bucket) >= self.max_routes_per_section:
                return
            bucket.append(normalized)
            seen_urls.add(normalized)

        def register_synthetic(url: str, section_name: str) -> None:
            normalized = normalize_candidate(url)
            if not normalized or strong_real_by_section.get(section_name) or section_name in synthetic_by_section:
                return
            synthetic_by_section[section_name] = normalized
            seen_urls.add(normalized)

        def normalized_hint_slug(route_hint: str) -> str:
            normalized_hint = normalize_whitespace(unquote(route_hint)).lower().lstrip("/")
            if "=" in normalized_hint:
                normalized_hint = normalized_hint.split("=", 1)[0]
            return normalized_hint.rstrip("?")

        def slug_tokens(value: str) -> tuple[str, ...]:
            return tuple(token for token in re.split(r"[\W_]+", value, flags=re.UNICODE) if token)

        def has_contiguous_token_match(path_hint: str, hint_slug: str) -> bool:
            hint_tokens = slug_tokens(hint_slug)
            if not hint_tokens:
                return False
            for raw_segment in path_hint.split("/"):
                if not raw_segment:
                    continue
                segment_tokens = slug_tokens(raw_segment)
                if len(segment_tokens) < len(hint_tokens):
                    continue
                for start in range(len(segment_tokens) - len(hint_tokens) + 1):
                    if segment_tokens[start : start + len(hint_tokens)] == hint_tokens:
                        return True
            return False

        def is_strong_path_match(path_hint: str, route_hint: str) -> bool:
            normalized_hint = normalize_whitespace(unquote(route_hint)).lower()
            hint_slug = normalized_hint_slug(route_hint)
            if not normalized_hint or not hint_slug or not path_hint:
                return False

            if normalized_hint.startswith("/"):
                if path_hint == normalized_hint or path_hint.startswith(f"{normalized_hint}/"):
                    return True
                return has_contiguous_token_match(path_hint, hint_slug)

            return has_contiguous_token_match(path_hint, hint_slug)

        def is_weak_path_match(path_hint: str, route_hint: str) -> bool:
            hint_slug = normalized_hint_slug(route_hint)
            return bool(hint_slug) and hint_slug in path_hint

        section_order: list[str] = []
        canonical_hint_by_section: dict[str, str] = {}
        for route_hint, section_name in SITE_PROBE_ROUTE_HINTS:
            if section_name not in canonical_hint_by_section:
                canonical_hint_by_section[section_name] = route_hint
                section_order.append(section_name)

        for anchor in soup.select("a[href]"):
            href = normalize_whitespace(anchor.get("href", ""))
            if not href or href.startswith(("mailto:", "tel:", "#")):
                continue
            full = normalize_url(urljoin(response_url, href))
            if not full:
                continue
            text_hint = normalize_whitespace(anchor.get_text(" ", strip=True)).lower()
            path_hint = normalize_whitespace(unquote(urlparse(full).path)).lower()
            best_match: tuple[int, str] | None = None
            for route_hint, section_name in SITE_PROBE_ROUTE_HINTS:
                if is_strong_path_match(path_hint, route_hint):
                    best_match = (2, section_name)
                    break
                if is_weak_path_match(path_hint, route_hint) or (section_name == "contacts" and any(hint in text_hint for hint in ("контакт", "contact"))):
                    if best_match is None:
                        best_match = (1, section_name)
            if best_match:
                register_real(full, best_match[1], strong=best_match[0] == 2)

        for section_name in section_order:
            register_synthetic(urljoin(origin, canonical_hint_by_section[section_name]), section_name)

        ordered: list[tuple[str, str]] = []
        for section_name in section_order:
            section_count = 0
            for candidate_url in strong_real_by_section.get(section_name, []):
                if section_count >= self.max_routes_per_section:
                    break
                ordered.append((candidate_url, section_name))
                section_count += 1
                if len(ordered) >= self.max_route_checks:
                    return ordered
            synthetic_url = synthetic_by_section.get(section_name)
            if synthetic_url and section_count < self.max_routes_per_section:
                ordered.append((synthetic_url, section_name))
                section_count += 1
                if len(ordered) >= self.max_route_checks:
                    return ordered
            for candidate_url in weak_real_by_section.get(section_name, []):
                if section_count >= self.max_routes_per_section:
                    break
                ordered.append((candidate_url, section_name))
                section_count += 1
                if len(ordered) >= self.max_route_checks:
                    return ordered
        return ordered

    def _apply_failure_outcome(self, probe: SiteProbe, outcome: Any) -> None:
        status = str(getattr(outcome, "status", "") or "unknown_error")
        error = str(getattr(outcome, "error", "") or "")
        response = getattr(outcome, "response", None)
        probe.status = status
        probe.failure_reason = status or "unknown_error"
        probe.timeout_reason = self._extract_timeout_reason(status=status, error=error)
        if error:
            probe.errors.append(error)
        decision = classify_fetch_attempt(
            status=status,
            response=response,
            usable_content=False,
            browser_attempt=False,
            session_reused=False,
        )
        probe.block_class = decision.block_class
        probe.anti_bot_reason = decision.anti_bot_reason
        probe.challenge_detected = decision.challenge_detected
        probe.anti_bot_detected = status in {"bot_gate", "rate_limited", "http_403", "auth_required"}
        if probe.anti_bot_detected:
            probe.site_class = "E"
            probe.worth_crawling = "limited"
            probe.browser_required_default = True
            probe.notes.append("probe уперся в антибот или ограничение по IP")
        else:
            probe.site_class = "F"
            probe.worth_crawling = "false"
        self._apply_policy_snapshot(
            probe,
            status=status,
            response=response,
            text=getattr(response, "text", "") if response is not None else "",
            usable_content=False,
            root_shell=False,
            route_sections=["homepage"],
        )

    def _extract_timeout_reason(self, *, status: str, error: str) -> str:
        if status == "cooldown_active":
            return "cooldown_active"
        combined = normalize_whitespace(error).lower()
        timeout_markers = ("timed out", "timeout", "read timeout", "connect timeout", "истекло время ожидания")
        if any(marker in combined for marker in timeout_markers):
            return "request_timeout"
        return ""

from __future__ import annotations

from urllib.parse import urlparse

import company_enrichment_core as core

from .models import DomainCandidate, DomainResolution


def domain_tokens_from_value(value: str) -> set[str]:
    lowered = core.normalize_whitespace(value).lower()
    tokens = core.re.findall(r"[a-zа-яё0-9]{3,}", lowered, flags=core.re.IGNORECASE)
    return {
        token
        for token in tokens
        if len(token) >= 4 and token not in core.COMPANY_TOKEN_STOPWORDS and not token.isdigit()
    }


def build_domain_resolution(
    row: core.RowInput,
    source_results: dict[str, core.SourceResult],
    merged_contacts: dict[str, list[str]],
) -> DomainResolution:
    resolution = DomainResolution(inn=row.inn, company_name=row.company_name)
    company_name_tokens = core.company_tokens(row.company_name)
    buckets: dict[str, dict[str, object]] = {}

    def register(url: str, source: str, evidence: str, *, kind: str, weight: float) -> None:
        cleaned = core.sanitize_website_url(url)
        if not cleaned:
            return
        domain = core.guess_registered_domain(urlparse(cleaned).netloc)
        if not domain or domain in core.NON_CORPORATE_DOMAINS:
            return
        bucket = buckets.setdefault(
            domain,
            {
                "domain": domain,
                "urls": [],
                "sources": set(),
                "evidence": [],
                "website_sources": set(),
                "email_sources": set(),
                "weights": [],
                "from_input": False,
            },
        )
        bucket["urls"].append(cleaned)
        bucket["sources"].add(source)
        bucket["evidence"].append(evidence)
        bucket["weights"].append(weight)
        if kind == "website":
            bucket["website_sources"].add(source)
        elif kind == "email_domain":
            bucket["email_sources"].add(source)
        if source == "xlsx_input":
            bucket["from_input"] = True

    if row.xlsx_site:
        register(row.xlsx_site, "xlsx_input", "сайт пришел из исходной таблицы", kind="website", weight=0.75)

    for source_name, source in source_results.items():
        for item in source.websites:
            if item.masked:
                continue
            register(item.value, source_name, f"сайт найден в {source_name}", kind="website", weight=0.32)
        for item in source.emails:
            if item.masked:
                continue
            email = core.normalize_whitespace(item.value).lower()
            if "@" not in email:
                continue
            email_domain = core.guess_registered_domain(email.split("@", 1)[-1])
            if not email_domain or email_domain in core.GENERIC_EMAIL_DOMAINS or email_domain in core.NON_CORPORATE_DOMAINS:
                continue
            register(
                f"https://{email_domain}",
                source_name,
                f"домен собран из email {core.compact_text(email, 80)} в {source_name}",
                kind="email_domain",
                weight=0.24,
            )

    candidates: list[DomainCandidate] = []
    for domain, bucket in buckets.items():
        domain_tokens = domain_tokens_from_value(str(domain).replace(".", "-"))
        token_overlap = sorted(company_name_tokens & domain_tokens)
        confidence = sum(float(value) for value in bucket["weights"])
        if len(bucket["website_sources"]) >= 2:
            confidence += 0.2
            bucket["evidence"].append("один и тот же домен подтвержден несколькими источниками")
        if bucket["website_sources"] and bucket["email_sources"]:
            confidence += 0.18
            bucket["evidence"].append("домен подтвержден и сайтом, и корпоративной почтой")
        if token_overlap:
            confidence += min(0.16, 0.08 * len(token_overlap))
            bucket["evidence"].append("домен совпадает с токенами названия: " + ", ".join(token_overlap))
        if bucket["from_input"]:
            confidence += 0.08
        confidence = round(min(confidence, 0.99), 3)
        if not bucket["from_input"] and not bucket["website_sources"] and not token_overlap:
            status = "rejected"
        elif bucket["from_input"] or len(bucket["website_sources"]) >= 2 or (bucket["email_sources"] and token_overlap and confidence >= 0.55):
            status = "verified"
        elif confidence >= 0.28:
            status = "candidate"
        else:
            status = "rejected"

        preferred_url = ""
        for candidate_url in bucket["urls"]:
            if urlparse(candidate_url).scheme == "https":
                preferred_url = candidate_url
                break
        if not preferred_url:
            preferred_url = bucket["urls"][0]

        candidates.append(
            DomainCandidate(
                url=preferred_url,
                domain=str(domain),
                source=", ".join(sorted(bucket["sources"])),
                confidence=confidence,
                status=status,
                evidence=core.dedupe_preserve_order(bucket["evidence"])[:6],
            )
        )

    rank = {"verified": 0, "candidate": 1, "rejected": 2}
    candidates.sort(key=lambda item: (rank.get(item.status, 9), -item.confidence, item.url))
    kept_candidates = [item for item in candidates if item.status != "rejected"][:6]
    resolution.candidates = kept_candidates
    if kept_candidates:
        best = kept_candidates[0]
        resolution.status = best.status
        resolution.selected_primary_domain = best.url
        resolution.selected_primary_status = best.status
        if len([item for item in kept_candidates if item.status == "verified"]) > 1:
            resolution.notes.append("есть несколько verified-доменов, нужен контроль связанной структуры/бренда")
        if len(kept_candidates) > 1 and kept_candidates[1].confidence >= max(best.confidence - 0.08, 0):
            resolution.notes.append("есть близкий альтернативный домен, deep-check нужен для выбора primary")
    else:
        resolution.status = "not_found"
        resolution.notes.append("не удалось собрать достаточно сильного кандидата на корпоративный домен")
    return resolution


def choose_candidate_sites_from_resolution(resolution: DomainResolution) -> list[str]:
    if not resolution.candidates:
        return []
    ordered: list[str] = []
    for candidate in resolution.candidates:
        if candidate.status not in {"verified", "candidate"}:
            continue
        ordered.append(candidate.url)
    return core.dedupe_websites_preserve_order(ordered)[:5]


def choose_candidate_sites(
    row: core.RowInput,
    merged_contacts: dict[str, list[str]],
    domain_resolution: DomainResolution | None = None,
) -> list[str]:
    if domain_resolution and domain_resolution.candidates:
        return choose_candidate_sites_from_resolution(domain_resolution)
    candidates = list(merged_contacts["websites"])
    if row.xlsx_site:
        cleaned_input_site = core.sanitize_website_url(row.xlsx_site)
        if cleaned_input_site:
            candidates.insert(0, cleaned_input_site)
    filtered: list[str] = []
    seen_domains: set[str] = set()
    for candidate in candidates:
        candidate = core.sanitize_website_url(candidate)
        if not candidate:
            continue
        host = urlparse(candidate).netloc.lower()
        domain = core.guess_registered_domain(host)
        if not domain:
            continue
        if domain in seen_domains:
            continue
        seen_domains.add(domain)
        filtered.append(candidate)
    return filtered[:5]

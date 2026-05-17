from __future__ import annotations

from typing import Protocol

from app.site_intelligence import ContentRecord, RouteStrategy


class RowLike(Protocol):
    inn: str


class RouteFetcher(Protocol):
    def fetch(self, url: str, mode: str) -> tuple[object | None, str, list[str]]:
        ...


class HtmlNormalizer(Protocol):
    def normalize_html_record(
        self,
        *,
        company_id: str,
        site_url: str,
        route: RouteStrategy,
        response: object | None,
        fetch_status: str,
        notes: list[str],
    ) -> ContentRecord:
        ...


def collect_content_records_for_site(
    *,
    row: RowLike,
    candidate_site: str,
    site_strategies: list[RouteStrategy],
    fetcher: RouteFetcher,
    normalizer: HtmlNormalizer,
    max_routes: int = 4,
) -> list[ContentRecord]:
    records: list[ContentRecord] = []
    seen_route_patterns: set[str] = set()
    for route in site_strategies[:max_routes]:
        if route.route_pattern in seen_route_patterns:
            continue
        seen_route_patterns.add(route.route_pattern)
        response, fetch_status, fetch_notes = fetcher.fetch(route.route_pattern, route.mode)
        record = normalizer.normalize_html_record(
            company_id=row.inn,
            site_url=candidate_site,
            route=route,
            response=response,
            fetch_status=fetch_status,
            notes=fetch_notes,
        )
        records.append(record)
    return records


__all__ = ["collect_content_records_for_site"]

from __future__ import annotations

from dataclasses import dataclass
from typing import Sequence

import company_enrichment_core as core


@dataclass(frozen=True)
class ResolvedRowSelection:
    mode: str
    rows: list[core.RowInput]
    selected_ordinals: list[int]
    start_from: int | None
    end_at: int | None


def parse_ordinals(value: str) -> list[int]:
    if not value.strip():
        raise ValueError("--ordinals must contain at least one ordinal")
    raw_items = value.split(",")
    if not raw_items:
        raise ValueError("--ordinals must contain at least one ordinal")
    parsed: list[int] = []
    for raw_item in raw_items:
        item = raw_item.strip()
        if not item:
            raise ValueError("--ordinals must not contain empty items")
        try:
            parsed.append(int(item))
        except ValueError as exc:
            raise ValueError(f"--ordinals contains non-integer value: {item!r}") from exc
    return normalize_ordinals(parsed)


def normalize_ordinals(ordinals: Sequence[int]) -> list[int]:
    if not ordinals:
        raise ValueError("--ordinals must contain at least one ordinal")
    normalized: list[int] = []
    seen: set[int] = set()
    for ordinal in ordinals:
        if ordinal < 1:
            raise ValueError("--ordinals must be >= 1")
        if ordinal in seen:
            continue
        seen.add(ordinal)
        normalized.append(ordinal)
    return normalized


def select_rows_for_window(
    rows: list[core.RowInput],
    *,
    start_from: int,
    count: int | None,
) -> list[core.RowInput]:
    if start_from < 1:
        raise ValueError("--start-from must be >= 1")
    start_index = start_from - 1
    if start_index >= len(rows):
        return []
    selected = rows[start_index:]
    if count is not None:
        selected = selected[:count]
    return selected


def resolve_row_selection(
    rows: list[core.RowInput],
    *,
    start_from: int,
    count: int | None,
    ordinals: Sequence[int] | None = None,
) -> ResolvedRowSelection:
    if ordinals is not None:
        return _resolve_exact_ordinal_selection(rows, ordinals)
    selected = select_rows_for_window(rows, start_from=start_from, count=count)
    end_at = start_from + len(selected) - 1 if selected else None
    return ResolvedRowSelection(
        mode="window",
        rows=selected,
        selected_ordinals=[],
        start_from=start_from,
        end_at=end_at,
    )


def intersect_selection_by_inns(
    selection: ResolvedRowSelection,
    inns: Sequence[str],
) -> ResolvedRowSelection:
    normalized_inns = {core.normalize_whitespace(str(inn or "")) for inn in inns}
    normalized_inns.discard("")
    selected_rows = [
        row
        for row in selection.rows
        if core.normalize_whitespace(str(row.inn or "")) in normalized_inns
    ]
    selected_ordinals = list(selection.selected_ordinals)
    if selected_ordinals:
        selected_ordinal_set = {row.row_index - 1 for row in selected_rows}
        selected_ordinals = [
            ordinal for ordinal in selected_ordinals if ordinal in selected_ordinal_set
        ]
    return ResolvedRowSelection(
        mode=selection.mode,
        rows=selected_rows,
        selected_ordinals=selected_ordinals,
        start_from=selection.start_from,
        end_at=selection.end_at,
    )


def _resolve_exact_ordinal_selection(
    rows: list[core.RowInput],
    ordinals: Sequence[int],
) -> ResolvedRowSelection:
    normalized_ordinals = normalize_ordinals(ordinals)
    ordinal_to_row = {row.row_index - 1: row for row in rows}
    missing_ordinals = [ordinal for ordinal in normalized_ordinals if ordinal not in ordinal_to_row]
    if missing_ordinals:
        missing_text = ", ".join(str(ordinal) for ordinal in missing_ordinals)
        raise ValueError(f"--ordinals not found in XLSX: {missing_text}")
    selected_rows = [ordinal_to_row[ordinal] for ordinal in normalized_ordinals]
    return ResolvedRowSelection(
        mode="ordinals",
        rows=selected_rows,
        selected_ordinals=normalized_ordinals,
        start_from=None,
        end_at=None,
    )

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from pathlib import Path

import company_enrichment_core as core
from .base import BaseSource, mark_source_not_found


@dataclass(frozen=True)
class ListOrgOfflineIndexLoad:
    index: dict[str, core.ListOrgOfflineRow]
    status: str
    reason: str = ""


def _coerce_non_negative_int(value: object, default: int) -> int:
    try:
        return max(int(value), 0)
    except (TypeError, ValueError):
        return default


_LIST_ORG_OKVED_CODE_RE = re.compile(r"(?<!\d)(\d{2}(?:\.\d{1,2}){0,3})(?!\d)")


def _normalize_list_org_okved_label(value: str) -> str:
    label = core.normalize_whitespace(value)
    if not label:
        return ""

    label = re.sub(r"^[\s:;,\-]+", "", label)
    label = re.sub(
        r"^(?:основн(?:ой|ая|ые)?|дополнительн(?:ый|ая|ые)?)"
        r"(?:\s+вид(?:ы)?\s+деятельности)?(?:\s+по\s+оквэд(?:\s*ред\.?\s*2)?)?\s*:?\s*",
        "",
        label,
        flags=re.IGNORECASE,
    )
    label = re.sub(
        r"(?:[;,.]\s*)?(?:основн(?:ой|ая|ые)?|дополнительн(?:ый|ая|ые)?)"
        r"(?:\s+вид(?:ы)?\s+деятельности)?(?:\s+по\s+оквэд(?:\s*ред\.?\s*2)?)?\s*:?\s*$",
        "",
        label,
        flags=re.IGNORECASE,
    )
    return core.normalize_whitespace(label).strip(" -:;,.")


def _parse_list_org_okved_entries(value: object) -> list[core.OkvedEntry]:
    raw_text = core.normalize_whitespace(str(value or ""))
    if not raw_text:
        return []

    matches = list(_LIST_ORG_OKVED_CODE_RE.finditer(raw_text))
    if not matches:
        return []

    entries: list[core.OkvedEntry] = []
    seen: set[tuple[str, str]] = set()
    for index, match in enumerate(matches):
        code = match.group(1).strip()
        next_start = matches[index + 1].start() if index + 1 < len(matches) else len(raw_text)
        label = _normalize_list_org_okved_label(raw_text[match.end() : next_start])
        if not code or not label:
            continue

        dedupe_key = (code, label)
        if dedupe_key in seen:
            continue
        seen.add(dedupe_key)
        entries.append(core.OkvedEntry(code=code, label=label))

    return entries


def load_list_org_offline_index(path: Path) -> ListOrgOfflineIndexLoad:
    snapshot_path = path.resolve()
    if not path.exists():
        return ListOrgOfflineIndexLoad(
            index={},
            status="missing_file",
            reason=f"Не найден offline search.json для List-Org: {snapshot_path}",
        )
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return ListOrgOfflineIndexLoad(
            index={},
            status="invalid_json",
            reason=f"Offline search.json для List-Org не читается как корректный JSON: {snapshot_path}",
        )
    if not isinstance(payload, dict):
        return ListOrgOfflineIndexLoad(
            index={},
            status="invalid_payload",
            reason=f"Offline search.json для List-Org имеет неподдерживаемую верхнеуровневую структуру: {snapshot_path}",
        )
    requests_payload = payload.get("requests")
    if not isinstance(requests_payload, list):
        return ListOrgOfflineIndexLoad(
            index={},
            status="invalid_payload",
            reason=f"Offline search.json для List-Org не содержит список requests: {snapshot_path}",
        )

    index: dict[str, core.ListOrgOfflineRow] = {}
    for item in requests_payload:
        if not isinstance(item, dict):
            continue
        request_key = core.normalize_inn(item.get("request"))
        if not request_key:
            continue

        result_items = item.get("result") if isinstance(item.get("result"), list) else []
        selected: dict[str, object] | None = None
        for candidate in result_items:
            if not isinstance(candidate, dict):
                continue
            if core.normalize_inn(candidate.get("inn")) == request_key:
                selected = candidate
                break

        index[request_key] = core.ListOrgOfflineRow(
            request=request_key,
            result_count=_coerce_non_negative_int(item.get("result_count"), len(result_items)),
            search_count=_coerce_non_negative_int(item.get("search_count"), 0),
            entity=selected,
        )

    if not index:
        return ListOrgOfflineIndexLoad(
            index={},
            status="empty_index",
            reason=f"Offline search.json для List-Org не содержит пригодных записей request/result: {snapshot_path}",
        )

    return ListOrgOfflineIndexLoad(index=index, status="ready")


class ListOrgSource(BaseSource):
    source_name = "list_org"

    def __init__(self, client: core.RateLimitedHttpClient, data_path: Path) -> None:
        super().__init__(client)
        self.data_path = data_path
        self.offline_index_load = load_list_org_offline_index(data_path)
        self.index = self.offline_index_load.index

    def _offline_evidence_path(self) -> str:
        return str(self.data_path.resolve())

    def _append_offline_mode_note(self, result: core.SourceResult) -> None:
        result.notes.append(f"List-Org mode=offline_snapshot file={self._offline_evidence_path()}; live_requests=0")

    @staticmethod
    def _append_offline_stats_note(result: core.SourceResult, offline_row: core.ListOrgOfflineRow) -> None:
        result.notes.append(
            "List-Org offline snapshot stats: "
            f"search_count={offline_row.search_count}, result_count={offline_row.result_count}"
        )

    def _mark_not_configured(self, result: core.SourceResult) -> core.SourceResult:
        result.status = "not_configured"
        result.listing_url = self._offline_evidence_path()
        reason = core.normalize_whitespace(self.offline_index_load.reason) or (
            f"Offline search.json для List-Org недоступен: {self._offline_evidence_path()}"
        )
        result.notes.append(f"List-Org offline_snapshot status={self.offline_index_load.status}")
        result.notes.append(reason)
        result.errors.append(reason)
        for field_name in core.IMPORTANT_FIELDS:
            core.set_field_availability(result, field_name, "unknown", reason=reason)
        return result

    def _new_result(self) -> core.SourceResult:
        result = core.SourceResult(source=self.source_name, status="pending")
        result.listing_url = self._offline_evidence_path()
        self._append_offline_mode_note(result)
        return result

    def search(self, row: core.RowInput) -> core.SourceResult:
        result = self._new_result()
        if self.offline_index_load.status != "ready":
            return self._mark_not_configured(result)

        offline_row = self.index.get(row.inn)
        if not offline_row:
            mark_source_not_found(
                result,
                reason=f"Для ИНН {row.inn} в offline search.json нет записи List-Org",
            )
            core.finalize_source_availability(result)
            return result

        if not offline_row.entity:
            self._append_offline_stats_note(result, offline_row)
            mark_source_not_found(
                result,
                reason=(
                    f"Для ИНН {row.inn} в offline search.json есть запись List-Org, "
                    "но в ней нет точного entity match"
                ),
            )
            core.finalize_source_availability(result)
            return result

        entity = offline_row.entity
        if core.normalize_inn(entity.get("inn")) != row.inn:
            self._append_offline_stats_note(result, offline_row)
            result.status = "mismatch"
            result.notes.append(
                f"Offline-карточка List-Org для ИНН {row.inn} содержит другой ИНН; данные карточки отброшены"
            )
            core.finalize_source_availability(result)
            return result

        result.status = "success"
        result.company_name_found = core.normalize_whitespace(str(entity.get("name", "") or entity.get("ur_name", "")))
        evidence_path = result.listing_url or self._offline_evidence_path()

        phone_value = core.normalize_whitespace(str(entity.get("phone", "")))
        if phone_value:
            result.phones.append(core.ContactItem(value=phone_value, source_url=evidence_path, kind="phone"))

        for email_value in core.extract_emails(str(entity.get("email", ""))):
            result.emails.append(core.ContactItem(value=email_value, source_url=evidence_path, kind="email"))

        for website in core.split_list_org_www_candidates(str(entity.get("www", ""))):
            result.websites.append(core.ContactItem(value=website, source_url=evidence_path, kind="website"))

        address_value = core.normalize_whitespace(str(entity.get("ur_address", "")))
        if address_value:
            result.addresses.append(core.ContactItem(value=address_value, source_url=evidence_path, kind="address"))

        status_value = core.normalize_whitespace(str(entity.get("status", "")))
        legacy_okved_value = core.normalize_whitespace(str(entity.get("okved", "")))
        okved_entries = _parse_list_org_okved_entries(legacy_okved_value)
        if okved_entries:
            result.primary_okved = okved_entries[0]
            result.additional_okveds = okved_entries[1:]
        okved_value = "; ".join(entry.display for entry in okved_entries if entry.display)
        if not okved_value:
            okved_value = legacy_okved_value
        staff_value = entity.get("staff")
        boss_value = core.normalize_whitespace(str(entity.get("boss", "")))
        boss_inn_value = core.normalize_inn(entity.get("boss_inn"))
        summary_bits = [
            f"status={status_value}" if status_value else "",
            f"okved={okved_value}" if okved_value else "",
            f"staff={staff_value}" if staff_value not in (None, "") else "",
            f"boss={boss_value}" if boss_value else "",
            f"boss_inn={boss_inn_value}" if boss_inn_value else "",
        ]
        summary_text = ", ".join(bit for bit in summary_bits if bit)
        if summary_text:
            result.snippets.append(summary_text)
        self._append_offline_stats_note(result, offline_row)
        if boss_value or boss_inn_value:
            core.set_field_availability(
                result,
                "management",
                "open",
                reason="В offline-снимке List-Org есть данные о руководителе",
                open_count=1,
            )
        else:
            core.set_field_availability(
                result,
                "management",
                "unknown",
                reason="Offline-снимок List-Org не дал данных о руководителе по этой компании",
            )
        core.set_field_availability(
            result,
            "founders",
            "unknown",
            reason="Offline-снимок List-Org не мапит учредителей в shared contract",
        )

        result.phones = core.dedupe_contact_items(result.phones)
        result.emails = core.dedupe_contact_items(result.emails)
        result.websites = core.dedupe_contact_items(result.websites)
        result.addresses = core.dedupe_contact_items(result.addresses)
        core.finalize_source_availability(result)
        return result

from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from typing import Any

from app.documents.ocr import LOW_PRIORITY_OCR_QUEUE_FAMILY
from app.site_intelligence.site_authenticity import (
    LOW_PRIORITY_EXTRA_CHECK_QUEUE_FAMILY,
    LOW_PRIORITY_LLM_QUEUE_FAMILY,
)


AGGREGATOR_SITE_MAINLINE_QUEUE_FAMILY = "mainline_aggregator_site"
DEEP_PARSE_MAINLINE_QUEUE_FAMILY = "mainline_deep_parse"
FACTORY_SITE_MAINLINE_QUEUE_FAMILY = "mainline_factory_site"
CANDIDATE_SITE_STAGE_NAME = "candidate_site"
DEEP_PARSE_STAGE_NAME = "deep_parse"
FACTORY_SITE_STAGE_NAME = "factory_site"
LLM_STAGE_NAME = "llm"
OCR_STAGE_NAME = "ocr"
EXTRA_CHECK_STAGE_NAME = "extra_check"
LOW_PRIORITY_QUEUE_FAMILIES = frozenset(
    {
        LOW_PRIORITY_EXTRA_CHECK_QUEUE_FAMILY,
        LOW_PRIORITY_LLM_QUEUE_FAMILY,
        LOW_PRIORITY_OCR_QUEUE_FAMILY,
    }
)


@dataclass(frozen=True, slots=True)
class RuntimeQueueFamilyContour:
    mainline: tuple[str, ...]
    low_priority: tuple[str, ...] = ()

    def as_payload(self) -> dict[str, list[str]]:
        return {
            "mainline": list(self.mainline),
            "low_priority": list(self.low_priority),
        }


def _normalize_queue_family_names(values: Iterable[Any]) -> tuple[str, ...]:
    seen: set[str] = set()
    normalized: list[str] = []
    for item in values:
        family_name = str(item or "").strip()
        if not family_name or family_name in seen:
            continue
        seen.add(family_name)
        normalized.append(family_name)
    return tuple(normalized)


def normalize_queue_family_contour(payload: Any) -> RuntimeQueueFamilyContour:
    if not isinstance(payload, Mapping):
        return RuntimeQueueFamilyContour(())
    return RuntimeQueueFamilyContour(
        mainline=_normalize_queue_family_names(payload.get("mainline") or ()),
        low_priority=_normalize_queue_family_names(payload.get("low_priority") or ()),
    )


def build_aggregator_site_queue_family_contour() -> RuntimeQueueFamilyContour:
    return RuntimeQueueFamilyContour(
        mainline=(AGGREGATOR_SITE_MAINLINE_QUEUE_FAMILY,),
    )


def build_deep_parse_queue_family_contour() -> RuntimeQueueFamilyContour:
        return RuntimeQueueFamilyContour(
        mainline=(DEEP_PARSE_MAINLINE_QUEUE_FAMILY,),
        low_priority=_normalize_queue_family_names(
            (
                LOW_PRIORITY_OCR_QUEUE_FAMILY,
                LOW_PRIORITY_LLM_QUEUE_FAMILY,
                LOW_PRIORITY_EXTRA_CHECK_QUEUE_FAMILY,
            )
        ),
    )


def is_low_priority_queue_family(queue_family: str) -> bool:
    return str(queue_family or "").strip() in LOW_PRIORITY_QUEUE_FAMILIES


@dataclass(frozen=True, slots=True)
class RuntimeStageWorkerLane:
    stage_name: str
    queue_family: str
    runtime_identity: str
    priority_class: str
    worker_budget: int

    def as_payload(self) -> dict[str, object]:
        return {
            "stage_name": self.stage_name,
            "queue_family": self.queue_family,
            "runtime_identity": self.runtime_identity,
            "priority_class": self.priority_class,
            "worker_budget": self.worker_budget,
        }


@dataclass(frozen=True, slots=True)
class RuntimeDownstreamWorkerPoolContour:
    lanes: tuple[RuntimeStageWorkerLane, ...]

    def as_payload(self) -> dict[str, object]:
        mainline = _normalize_queue_family_names(
            lane.queue_family
            for lane in self.lanes
            if lane.priority_class == "mainline"
        )
        low_priority = _normalize_queue_family_names(
            lane.queue_family
            for lane in self.lanes
            if lane.priority_class == "low_priority"
        )
        return {
            "mainline": list(mainline),
            "low_priority": list(low_priority),
            "lanes": [lane.as_payload() for lane in self.lanes],
            "per_stage_budget_map": {
                lane.stage_name: lane.worker_budget
                for lane in self.lanes
            },
        }

    def per_stage_budget_map(self) -> dict[str, int]:
        return {
            lane.stage_name: lane.worker_budget
            for lane in self.lanes
        }


def build_downstream_worker_pool_contour(
    *,
    company_concurrency_cap: int,
    low_priority_budget: int = 1,
    candidate_site_concurrency: int | None = None,
    deep_parse_concurrency: int | None = None,
    factory_site_concurrency: int | None = None,
    ocr_concurrency: int | None = None,
    llm_concurrency: int | None = None,
    extra_check_concurrency: int | None = None,
) -> RuntimeDownstreamWorkerPoolContour:
    mainline_budget = max(int(company_concurrency_cap or 0), 1)
    low_priority_lane_budget = max(int(low_priority_budget or 0), 1)
    candidate_site_budget = _resolve_worker_budget(candidate_site_concurrency, default=mainline_budget)
    deep_parse_budget = _resolve_worker_budget(deep_parse_concurrency, default=mainline_budget)
    factory_site_budget = _resolve_worker_budget(factory_site_concurrency, default=mainline_budget)
    ocr_budget = _resolve_worker_budget(ocr_concurrency, default=low_priority_lane_budget)
    llm_budget = _resolve_worker_budget(llm_concurrency, default=low_priority_lane_budget)
    extra_check_budget = _resolve_worker_budget(extra_check_concurrency, default=low_priority_lane_budget)
    return RuntimeDownstreamWorkerPoolContour(
        lanes=(
            RuntimeStageWorkerLane(
                stage_name=CANDIDATE_SITE_STAGE_NAME,
                queue_family=AGGREGATOR_SITE_MAINLINE_QUEUE_FAMILY,
                runtime_identity="candidate_site_pool",
                priority_class="mainline",
                worker_budget=candidate_site_budget,
            ),
            RuntimeStageWorkerLane(
                stage_name=DEEP_PARSE_STAGE_NAME,
                queue_family=DEEP_PARSE_MAINLINE_QUEUE_FAMILY,
                runtime_identity="deep_parse_pool",
                priority_class="mainline",
                worker_budget=deep_parse_budget,
            ),
            RuntimeStageWorkerLane(
                stage_name=FACTORY_SITE_STAGE_NAME,
                queue_family=FACTORY_SITE_MAINLINE_QUEUE_FAMILY,
                runtime_identity="factory_site_pool",
                priority_class="mainline",
                worker_budget=factory_site_budget,
            ),
            RuntimeStageWorkerLane(
                stage_name=LLM_STAGE_NAME,
                queue_family=LOW_PRIORITY_LLM_QUEUE_FAMILY,
                runtime_identity="llm_pool",
                priority_class="low_priority",
                worker_budget=llm_budget,
            ),
            RuntimeStageWorkerLane(
                stage_name=OCR_STAGE_NAME,
                queue_family=LOW_PRIORITY_OCR_QUEUE_FAMILY,
                runtime_identity="ocr_pool",
                priority_class="low_priority",
                worker_budget=ocr_budget,
            ),
            RuntimeStageWorkerLane(
                stage_name=EXTRA_CHECK_STAGE_NAME,
                queue_family=LOW_PRIORITY_EXTRA_CHECK_QUEUE_FAMILY,
                runtime_identity="extra_check_pool",
                priority_class="low_priority",
                worker_budget=extra_check_budget,
            ),
        ),
    )


def _resolve_worker_budget(value: int | None, *, default: int) -> int:
    if value is None:
        return max(int(default or 0), 1)
    return max(int(value or 0), 1)


__all__ = [
    "AGGREGATOR_SITE_MAINLINE_QUEUE_FAMILY",
    "CANDIDATE_SITE_STAGE_NAME",
    "DEEP_PARSE_MAINLINE_QUEUE_FAMILY",
    "DEEP_PARSE_STAGE_NAME",
    "EXTRA_CHECK_STAGE_NAME",
    "FACTORY_SITE_MAINLINE_QUEUE_FAMILY",
    "FACTORY_SITE_STAGE_NAME",
    "LLM_STAGE_NAME",
    "LOW_PRIORITY_QUEUE_FAMILIES",
    "OCR_STAGE_NAME",
    "RuntimeDownstreamWorkerPoolContour",
    "RuntimeQueueFamilyContour",
    "RuntimeStageWorkerLane",
    "build_aggregator_site_queue_family_contour",
    "build_downstream_worker_pool_contour",
    "build_deep_parse_queue_family_contour",
    "is_low_priority_queue_family",
    "normalize_queue_family_contour",
]

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal, ROUND_HALF_UP
from typing import Any


USD_QUANT = Decimal("0.00000001")
TOKENS_PER_MILLION = Decimal("1000000")


@dataclass(frozen=True)
class ModelPricing:
    input_per_million_usd: Decimal
    cached_input_per_million_usd: Decimal
    output_per_million_usd: Decimal


@dataclass(frozen=True)
class UsageCostBreakdown:
    input_tokens: int | None
    output_tokens: int | None
    input_cost_usd: float | None
    output_cost_usd: float | None
    total_cost_usd: float | None
    cost_unknown: bool


MODEL_PRICING_USD: dict[str, ModelPricing] = {
    "gpt-5.4-mini": ModelPricing(
        input_per_million_usd=Decimal("0.75"),
        cached_input_per_million_usd=Decimal("0.075"),
        output_per_million_usd=Decimal("4.50"),
    ),
    "gpt-5.4-nano": ModelPricing(
        input_per_million_usd=Decimal("0.20"),
        cached_input_per_million_usd=Decimal("0.02"),
        output_per_million_usd=Decimal("1.25"),
    ),
}


def _normalize_token_count(value: Any) -> int | None:
    if value is None:
        return None
    try:
        normalized = int(value)
    except (TypeError, ValueError):
        return None
    return max(normalized, 0)


def _round_usd(value: Decimal) -> float:
    return float(value.quantize(USD_QUANT, rounding=ROUND_HALF_UP))


def calculate_usage_cost_usd(
    model: str,
    *,
    input_tokens: Any,
    output_tokens: Any,
    cached_input_tokens: Any = 0,
) -> UsageCostBreakdown:
    normalized_input_tokens = _normalize_token_count(input_tokens)
    normalized_output_tokens = _normalize_token_count(output_tokens)
    normalized_cached_tokens = _normalize_token_count(cached_input_tokens) or 0

    if normalized_input_tokens == 0 and normalized_output_tokens == 0:
        return UsageCostBreakdown(
            input_tokens=0,
            output_tokens=0,
            input_cost_usd=0.0,
            output_cost_usd=0.0,
            total_cost_usd=0.0,
            cost_unknown=False,
        )

    if normalized_input_tokens is None or normalized_output_tokens is None:
        return UsageCostBreakdown(
            input_tokens=normalized_input_tokens,
            output_tokens=normalized_output_tokens,
            input_cost_usd=None,
            output_cost_usd=None,
            total_cost_usd=None,
            cost_unknown=True,
        )

    pricing = MODEL_PRICING_USD.get(model.strip())
    if pricing is None:
        return UsageCostBreakdown(
            input_tokens=normalized_input_tokens,
            output_tokens=normalized_output_tokens,
            input_cost_usd=None,
            output_cost_usd=None,
            total_cost_usd=None,
            cost_unknown=True,
        )

    cached_tokens = min(normalized_cached_tokens, normalized_input_tokens)
    uncached_tokens = normalized_input_tokens - cached_tokens
    input_cost = (
        (Decimal(uncached_tokens) / TOKENS_PER_MILLION) * pricing.input_per_million_usd
        + (Decimal(cached_tokens) / TOKENS_PER_MILLION) * pricing.cached_input_per_million_usd
    )
    output_cost = (Decimal(normalized_output_tokens) / TOKENS_PER_MILLION) * pricing.output_per_million_usd
    total_cost = input_cost + output_cost

    return UsageCostBreakdown(
        input_tokens=normalized_input_tokens,
        output_tokens=normalized_output_tokens,
        input_cost_usd=_round_usd(input_cost),
        output_cost_usd=_round_usd(output_cost),
        total_cost_usd=_round_usd(total_cost),
        cost_unknown=False,
    )

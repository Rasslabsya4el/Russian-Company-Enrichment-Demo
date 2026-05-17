from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from .state import RuntimeStateSnapshot, build_runtime_state_payload, ordered_runtime_results


@dataclass(slots=True)
class RuntimeRunState:
    results: dict[str, dict[str, Any]] = field(default_factory=dict)
    summary: dict[str, Any] = field(default_factory=dict)
    host_stats: dict[str, Any] = field(default_factory=dict)
    host_memory: dict[str, Any] = field(default_factory=dict)
    run_metadata: dict[str, Any] = field(default_factory=dict)

    def restore_snapshot(self, snapshot: RuntimeStateSnapshot) -> None:
        self.results.clear()
        self.results.update(snapshot.results)
        self.summary.clear()
        self.summary.update(snapshot.summary)
        self.host_stats.clear()
        self.host_stats.update(snapshot.host_stats)
        self.host_memory.clear()
        self.host_memory.update(snapshot.host_memory)
        self.run_metadata.clear()
        self.run_metadata.update(snapshot.run_metadata)

    def reset(self) -> None:
        self.results.clear()
        self.summary.clear()
        self.host_stats.clear()
        self.host_memory.clear()
        self.run_metadata.clear()

    def ordered_results(self) -> list[dict[str, Any]]:
        return ordered_runtime_results(self.results)

    def build_payload(self, *, ordered_results: list[dict[str, Any]] | None = None) -> dict[str, Any]:
        ordered = self.ordered_results() if ordered_results is None else ordered_results
        return build_runtime_state_payload(
            ordered_results=ordered,
            summary=self.summary,
            host_stats=self.host_stats,
            host_memory=self.host_memory,
            run_metadata=self.run_metadata,
        )


__all__ = ["RuntimeRunState"]

from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from typing import Any

# Set by fixture 8, not chosen: 4.547e-13 was the largest accumulation over
# 3,600 trials up to 1,000 events, so this carries ~2,200x headroom.
EPSILON = 1e-9


@dataclass(frozen=True)
class Divergence:
    player_id: int
    scope: str
    expected: float
    actual: float
    event_count: int

    @property
    def amount(self) -> float:
        return self.expected - self.actual


def reconcile(
    events: Iterable[Mapping[str, Any]],
    actual_mu: Mapping[tuple[int, str], float | None],
    *,
    initial_mu: float,
    epsilon: float = EPSILON,
) -> list[Divergence]:
    """Sum ledger deltas per (player_id, scope); assert they equal stat movement."""
    grouped: dict[tuple[int, str], list[Mapping[str, Any]]] = {}
    for e in sorted(events, key=lambda e: e["occurred_at"]):
        pid = int(e["player_id"])
        if pid < 0:  # placeholder ids, skipped by the ledger writes too
            continue
        grouped.setdefault((pid, str(e["scope"])), []).append(e)

    out: list[Divergence] = []
    for (pid, scope), group in grouped.items():
        baseline = float(group[0]["mu_before"])
        delta_sum = sum(float(e["mu_after"]) - float(e["mu_before"]) for e in group)
        expected = baseline + delta_sum
        stored = actual_mu.get((pid, scope))
        actual = initial_mu if stored is None else float(stored)
        if abs(expected - actual) > epsilon:
            out.append(Divergence(pid, scope, expected, actual, len(group)))
    return out


__all__ = ["EPSILON", "Divergence", "reconcile"]

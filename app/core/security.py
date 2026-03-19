from __future__ import annotations

from secrets import compare_digest


def constant_time_equals(left: str, right: str) -> bool:
    return compare_digest(left.encode("utf-8"), right.encode("utf-8"))

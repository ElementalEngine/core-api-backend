from __future__ import annotations

import ast
import re
from pathlib import Path

CODES = {
    "INVALID_REQUEST",
    "UNAUTHORIZED",
    "FORBIDDEN",
    "NOT_FOUND",
    "CONFLICT",
    "RATE_LIMITED",
    "UNAVAILABLE",
    "INTERNAL",
}

# Every file Mite's routes answer from; Auth and Lady Justice keep their own codes.
GOVERNED = (
    "api/health.py",
    "core/dependencies.py",
    "core/errors.py",
    "features/civdata/router.py",
    "features/match_setups/router.py",
    "features/match_setups/vote_router.py",
    "features/matches/router_v2.py",
    "features/seasons/router.py",
    "features/stats/router_v2.py",
)
# A server set up wrong says which part, so the operator can fix it.
OPERATOR = re.compile(r"[A-Z_]+_(MISCONFIGURED|NOT_SEEDED)")


def _emitted_codes():
    app = Path(__file__).resolve().parents[2] / "app"
    found = []
    for rel in GOVERNED:
        path = app / rel
        assert path.exists(), f"missing governed file: {rel}"
        for node in ast.walk(ast.parse(path.read_text())):
            if not isinstance(node, ast.Call):
                continue
            for kw in node.keywords:
                if kw.arg == "code" and isinstance(kw.value, ast.Constant):
                    if isinstance(kw.value.value, str):
                        found.append((rel, node.lineno, kw.value.value))
    return found


def test_every_governed_code_is_in_the_closed_enum():
    outside = [
        f
        for f in _emitted_codes()
        if f[2] not in CODES and not OPERATOR.fullmatch(f[2])
    ]
    assert not outside, outside


def test_the_scan_actually_finds_the_emitters():
    # Without this the test above passes on a scan that finds nothing.
    codes = {c for _, _, c in _emitted_codes()}
    expected = {"INVALID_REQUEST", "NOT_FOUND", "FORBIDDEN", "UNAVAILABLE", "INTERNAL"}
    assert expected <= codes

"""On the surfaces D92 governs, every code is one of its eight.

D92 is scoped to C2 and C3 -- the matches and stats v2 routers, plus the
app-wide handlers in core/errors that answer for them. Auth, infractions
and civdata ship 26 feature-specific codes of their own, which C10 freezes
and this test deliberately does not police (correction 59).

A closed set is what lets a client branch exhaustively. It stays closed
only if something checks, because nothing else does: the envelope never
reaches the OpenAPI spec, so no generated type constrains it (section 4
item 96).

What should break this file: a v2 route inventing a ninth transport code.
"""

from __future__ import annotations

import ast
from pathlib import Path

D92_CODES = {
    "INVALID_REQUEST",
    "UNAUTHORIZED",
    "FORBIDDEN",
    "NOT_FOUND",
    "CONFLICT",
    "RATE_LIMITED",
    "UNAVAILABLE",
    "INTERNAL",
}

GOVERNED = (
    "core/errors.py",
    "features/matches/router_v2.py",
    "features/stats/router_v2.py",
)


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
    outside = [f for f in _emitted_codes() if f[2] not in D92_CODES]
    assert not outside, outside


def test_the_scan_actually_finds_the_emitters():
    # Without this the test above passes on a scan that finds nothing.
    codes = {c for _, _, c in _emitted_codes()}
    expected = {"INVALID_REQUEST", "NOT_FOUND", "FORBIDDEN", "UNAVAILABLE", "INTERNAL"}
    assert expected <= codes

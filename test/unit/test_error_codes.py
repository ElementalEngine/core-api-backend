"""Every error code emitted on a v2 surface is one of the eight in the envelope.

A ninth code reaching a client means a generated type does not cover it.

Governed by D92.
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

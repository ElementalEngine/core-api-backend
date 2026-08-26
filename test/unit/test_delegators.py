"""Every delegator forwards its own parameters, unchanged.

S6's carve produced eight wrong delegators; five sat on v1 routes with no
test, and four green gate runs said nothing because the files holding them
are mypy-quarantined. Coverage did not catch it; this shape did.

A delegator is a method whose whole body is `return [await] X.<same name>(...)`.
Name equality is the definition, not a heuristic: it is what separates a
forward from a repository method that happens to return a single call.

What should break this file: a carve that leaves a delegator forwarding
fewer arguments than it takes, in either position or keyword form.
"""

from __future__ import annotations

import ast
from pathlib import Path

Finding = tuple[str, str, str]

# Two wrong delegators and one correct one. Without a case the check can
# fail on, the suite passes on a checker that finds nothing at all -- D86
# Rule 1, and the shape of s6-close.md section 8's first quarantine check.
BROKEN_SOURCE = """
class Svc:
    async def packed(self, a, b):
        return await Other(self).packed()

    async def kwonly(self, *, civ_version, discord_id):
        return await self._r.kwonly(civ_version=civ_version)

    async def correct(self, a, *, b):
        return await Other(self).correct(a, b=b)
"""


def _forwarded_call(fn: ast.FunctionDef | ast.AsyncFunctionDef) -> ast.Call | None:
    body = [
        s
        for s in fn.body
        if not (isinstance(s, ast.Expr) and isinstance(s.value, ast.Constant))
    ]
    if len(body) != 1 or not isinstance(body[0], ast.Return):
        return None
    value = body[0].value
    if isinstance(value, ast.Await):
        value = value.value
    if not isinstance(value, ast.Call) or not isinstance(value.func, ast.Attribute):
        return None
    return value if value.func.attr == fn.name else None


def _check(name: str, source: str) -> list[Finding]:
    out: list[Finding] = []
    for cls in ast.walk(ast.parse(source)):
        if not isinstance(cls, ast.ClassDef):
            continue
        for fn in cls.body:
            if not isinstance(fn, (ast.FunctionDef, ast.AsyncFunctionDef)):
                continue
            call = _forwarded_call(fn)
            if call is None:
                continue
            where = f"{name}::{cls.name}.{fn.name}"

            want = [
                a.arg for a in fn.args.posonlyargs + fn.args.args if a.arg != "self"
            ]
            got = [a.id if isinstance(a, ast.Name) else "<expr>" for a in call.args]
            if want != got:
                out.append((where, f"positional {got}", f"signature {want}"))

            want_kw = sorted(a.arg for a in fn.args.kwonlyargs)
            got_kw = sorted(k.arg for k in call.keywords if k.arg is not None)
            if want_kw != got_kw:
                out.append((where, f"keyword {got_kw}", f"signature {want_kw}"))
    return out


def test_every_delegator_forwards_its_own_parameters():
    app = Path(__file__).resolve().parents[2] / "app"
    bad = [f for p in sorted(app.rglob("*.py")) for f in _check(p.name, p.read_text())]
    assert not bad, "\n".join(f"{w}: {g} != {s}" for w, g, s in bad)


def test_the_check_can_fail():
    found = {w.rsplit(".", 1)[-1] for w, _, _ in _check("broken.py", BROKEN_SOURCE)}
    assert found == {"packed", "kwonly"}, found

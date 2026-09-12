"""Regenerate `openapi.json`, or check the committed copy against the app.

The dump format must match CI's comparison exactly or the check is flaky:
`indent=2`, `sort_keys=True`, one trailing newline.

Governed by D98.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

# The repo is not installed as a package, so `app` is importable only with
# the repo root on sys.path -- running a file in scripts/ puts scripts/ there.
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from app.main import app

SPEC = Path("openapi.json")


def rendered() -> str:
    """The spec as CI expects to find it committed."""
    return json.dumps(app.openapi(), indent=2, sort_keys=True) + "\n"


def main() -> int:
    """Check with `--check`, otherwise write."""
    spec = rendered()
    if "--check" in sys.argv:
        if spec != SPEC.read_text(encoding="utf-8"):
            print("openapi.json is stale. Regenerate and commit it.")
            return 1
        print("spec matches")
        return 0
    SPEC.write_text(spec, encoding="utf-8")
    print("openapi.json regenerated")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

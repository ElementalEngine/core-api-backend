from __future__ import annotations

import re
from pathlib import Path

import pytest

# Every install reads a lock with --require-hashes, so a pin changed in a
# requirements file without rebuilding its lock would install the old version.
# These checks fail first.

ROOT = Path(__file__).resolve().parents[2]
PIN = re.compile(r"^([A-Za-z0-9][A-Za-z0-9._-]*)(?:\[[^\]]*\])?==([^\s;\\]+)")


def name(raw: str) -> str:
    return re.sub(r"[-_.]+", "-", raw).lower()


def pins(path: Path) -> dict[str, str]:
    """The name==version pins in a requirements file, following -r."""
    found: dict[str, str] = {}
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if line.startswith("-r "):
            found.update(pins(path.parent / line[3:].strip()))
            continue
        match = PIN.match(line)
        if match:
            found[name(match.group(1))] = match.group(2)
    return found


def locked(path: Path) -> dict[str, tuple[str, int]]:
    """Each locked package's version and how many hashes it carries."""
    entries: dict[str, tuple[str, int]] = {}
    current = None
    for line in path.read_text(encoding="utf-8").splitlines():
        match = PIN.match(line)
        if match:
            current = name(match.group(1))
            entries[current] = (match.group(2), 0)
        elif current and line.strip().startswith("--hash=sha256:"):
            version, hashes = entries[current]
            entries[current] = (version, hashes + 1)
    return entries


PAIRS = [
    ("requirements.txt", "requirements.lock"),
    ("requirements-dev.txt", "requirements-dev.lock"),
]


@pytest.mark.parametrize(("source", "lock"), PAIRS)
def test_every_pin_is_locked_at_its_version(source, lock):
    want = pins(ROOT / source)
    got = locked(ROOT / lock)
    assert want, f"no pins read from {source}"
    wrong = {
        pkg: (version, got.get(pkg, ("missing", 0))[0])
        for pkg, version in want.items()
        if got.get(pkg, ("missing", 0))[0] != version
    }
    assert not wrong, f"{lock} disagrees with {source} (pin, lock): {wrong}"


@pytest.mark.parametrize("lock", [lock for _, lock in PAIRS])
def test_every_locked_package_carries_a_hash(lock):
    entries = locked(ROOT / lock)
    assert entries
    bare = sorted(pkg for pkg, (_, hashes) in entries.items() if hashes == 0)
    assert not bare, f"{lock}: packages with no hash: {bare}"


def test_the_runtime_lock_matches_the_dev_lock():
    # Dev runs what prod runs: the runtime set is the dev set's, version for version.
    runtime = locked(ROOT / "requirements.lock")
    dev = locked(ROOT / "requirements-dev.lock")
    differ = {
        pkg: (version, dev.get(pkg, ("missing", 0))[0])
        for pkg, (version, _) in runtime.items()
        if dev.get(pkg, ("missing", 0))[0] != version
    }
    assert not differ, f"runtime and dev locks disagree (runtime, dev): {differ}"

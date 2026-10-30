#!/usr/bin/env bash
# v1, production's own code, against v2 on approve, on every
# validated match on dev and a read-only sample of production's. Every
# document the run writes is put back, and the dev cluster's fingerprint must
# be identical before and after. Dev box only; sudo reads production's .env.
#
#   bash scripts/differential_gate.sh
#
# v1 is built from production's HEAD into /tmp/v1-core when it is absent,
# which it is after a reboot.
set -euo pipefail
R="$(cd "$(dirname "$0")/.." && pwd)"
P=/opt/production/cpl/core-api-backend
V1=/tmp/v1-core
PORT=8002
H=scripts/differential_gate.py
PY=.venv/bin/python
WORK="$(mktemp -d)"
V1PID=""
cleanup() {
  if [ -n "$V1PID" ]; then kill "$V1PID" 2>/dev/null || true; fi
  rm -rf "$WORK"
}
trap cleanup EXIT
say() { printf '\n== %s\n' "$*"; }
cd "$R"

say "0. preflight"
if [ -n "$(git status --porcelain)" ]; then
  echo "STOP: working tree not clean"; git status --short; exit 1
fi
echo "v2: $(git rev-parse --short HEAD) on $(git rev-parse --abbrev-ref HEAD)"
if [ -n "$(git -C "$P" status --porcelain app)" ]; then
  echo "STOP: production's app/ differs from its HEAD"; exit 1
fi
PROD="$(git -C "$P" rev-parse --short HEAD)"
if [ ! -x "$V1/.venv/bin/python" ]; then
  echo "building v1 from production $PROD at $V1"
  rm -rf "$V1" && mkdir -p "$V1"
  git -C "$P" archive HEAD | tar -x -C "$V1"
  python3 -m venv "$V1/.venv"
  "$V1/.venv/bin/pip" install -q -r "$V1/requirements.txt"
fi
mkdir "$WORK/head"
git -C "$P" archive HEAD app | tar -x -C "$WORK/head"
if ! diff -rq --exclude=__pycache__ "$WORK/head/app" "$V1/app"; then
  echo "STOP: $V1 is not production's HEAD"; exit 1
fi
echo "v1: production $PROD, app/ identical"
if ! "$V1/.venv/bin/python" -I -c "import uvicorn, importlib.metadata as m; \
print('v1 venv: motor', m.version('motor'), 'trueskill', m.version('trueskill'))"; then
  echo "STOP: v1's venv cannot import its server"; exit 1
fi
if ss -ltn | grep -q "127.0.0.1:$PORT "; then echo "STOP: $PORT in use"; exit 1; fi

say "1. rating settings, production against v2"
sudo grep -E '^(TS_MU|TS_SIGMA|TS_BETA|TS_TAU|TS_DRAW_PROB|MIN_POINTS_FOR_SUBS)=' \
  "$P/.env" | "$PY" -I "$H" rating

say "2. production sample, read-only"
sudo grep -E '^(MONGO_URL|MONGO_URI)=' "$P/.env" \
  | "$PY" -I "$H" sample "$WORK/prod-sample.jsonl"

say "3. state before"
"$PY" scripts/reconcile.py
"$PY" -I "$H" fingerprint > "$WORK/before.json"
echo "fingerprint: $(md5sum < "$WORK/before.json" | cut -c1-12)"

say "4. start v1 on :$PORT"
"$PY" -I "$H" v1 "$V1" "$PORT" > "$WORK/v1.log" 2>&1 &
V1PID=$!
up=no
for _ in $(seq 1 60); do
  if grep -q "Application startup complete" "$WORK/v1.log"; then up=yes; break; fi
  if ! kill -0 "$V1PID" 2>/dev/null; then break; fi
  sleep 1
done
if [ "$up" != yes ]; then
  echo "STOP: v1 did not start"; tail -n 30 "$WORK/v1.log"; exit 1
fi
echo "v1 up, pid $V1PID"

say "5. differential: dev's matches, then production's sample"
set +e
"$PY" scripts/differential.py "http://127.0.0.1:$PORT"
DRC=$?
"$PY" scripts/differential.py "http://127.0.0.1:$PORT" --source "$WORK/prod-sample.jsonl"
SRC=$?
set -e

say "6. stop v1"
kill "$V1PID"; wait "$V1PID" 2>/dev/null || true; V1PID=""
echo "stopped"

say "7. state after: identical to before"
"$PY" scripts/reconcile.py
"$PY" -I "$H" fingerprint > "$WORK/after.json"
if ! cmp -s "$WORK/before.json" "$WORK/after.json"; then
  echo "STOP: the cluster changed"
  "$PY" -I "$H" fpdiff "$WORK/before.json" "$WORK/after.json"
  exit 1
fi
echo "fingerprint identical"
if [ "$DRC" != 0 ] || [ "$SRC" != 0 ]; then
  echo "STOP: the differential did not pass (dev $DRC, production sample $SRC)"; exit 1
fi
say "PASS"

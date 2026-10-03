#!/usr/bin/env bash
# End-to-end check: deploy the example app to a shared local "bucket", run two
# nodes, call through both, crash one, verify takeover, and use the generated
# Python client.  Usage: scripts/e2e.sh
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cargo build -q -p statex-cli --manifest-path "$ROOT/Cargo.toml"
S="$ROOT/target/debug/statex"
W="$(mktemp -d)"
export STATEX_STORE="$W/bucket" STATEX_APP=counter
PIDS=()
cleanup() { for p in "${PIDS[@]}"; do kill "$p" 2>/dev/null || true; done; rm -rf "$W"; }
trap cleanup EXIT

wait_up() { for _ in $(seq 1 120); do curl -sf "http://127.0.0.1:$1/healthz" >/dev/null && return; sleep 0.5; done; echo "node on $1 did not start"; exit 1; }
check() { if [ "$2" != "$3" ]; then echo "FAIL $1: expected $3, got $2"; exit 1; fi; echo "ok   $1"; }

(cd "$ROOT/examples/counter" && "$S" deploy)
"$S" node --listen 127.0.0.1:9201 --node-id n1 --data-dir "$W/n1" --lease-ttl 3 >"$W/n1.log" 2>&1 & PIDS+=($!)
"$S" node --listen 127.0.0.1:9202 --node-id n2 --data-dir "$W/n2" --lease-ttl 3 >"$W/n2.log" 2>&1 & PIDS+=($!)
N1=${PIDS[0]}
wait_up 9201; wait_up 9202

check "increment via n1" "$("$S" call counter alice increment -a by=5 --url http://127.0.0.1:9201)" 5
check "read via n2 (forwarded)" "$("$S" call counter alice get --url http://127.0.0.1:9202)" 5

kill -9 "$N1"
check "takeover after crash" "$("$S" call counter alice increment -a by=1 --url http://127.0.0.1:9202)" 6

(cd "$ROOT/examples/counter" && "$S" codegen python --from-url http://127.0.0.1:9202 --url http://127.0.0.1:9202 -o "$W/counter_client.py" >/dev/null)
OUT="$(cd "$W" && python3 - <<'PY'
from counter_client import CounterApp, MethodError, TxError
c = CounterApp()
assert c.counter("alice").increment(by=1) == 7
a = c.account("bob")
assert a.deposit(10) == 10
try:
    a.withdraw(99)
    raise SystemExit("expected MethodError")
except MethodError as e:
    assert e.error == TxError.insufficient_funds(10), e.error
print("python-ok")
PY
)"
check "generated python client" "$OUT" python-ok

# Ownership: the first deploy claimed `counter` for its owner.
if (cd "$ROOT/examples/counter" && "$S" deploy --owner intruder) >"$W/intruder.log" 2>&1; then
  echo "FAIL deploy by another owner was accepted"; exit 1
fi
check "deploy by another owner refused" "$(grep -c 'is owned by "examples"' "$W/intruder.log")" 1

# Monorepo workspace: scaffold, check, registry, compatibility against git.
M="$W/mono"; mkdir -p "$M"; cd "$M"
git init -q && git config user.email e2e@statex && git config user.name e2e
"$S" workspace init --statex "$ROOT" >/dev/null
"$S" new payments/shop --actor cart >/dev/null
check "app linked to workspace sdk" "$(grep -c "statex-guest = { path = \"$ROOT/crates/guest\"" apps/payments/shop/Cargo.toml)" 1
check "check --all" "$("$S" check --all | head -1)" "ok   payments/shop"
"$S" registry build >/dev/null && "$S" registry build --check >/dev/null
git add -A && git commit -qm init
sed -i.bak 's/increment: func(by: s64)/increment: func(by: s32)/' apps/payments/shop/wit/app.wit
if "$S" check --all --against HEAD >"$W/check.log" 2>&1; then echo "FAIL breaking change not detected"; exit 1; fi
check "breaking change detected" "$(grep -c 'parameters changed from (by: s64) to (by: s32)' "$W/check.log")" 1
if "$S" registry build --check >/dev/null 2>&1; then echo "FAIL stale registry not detected"; exit 1; fi
echo "ok   stale registry detected"
mkdir -p apps/growth/x && cp -R apps/payments/shop apps/growth/x/notes
if "$S" check --all >/dev/null 2>&1; then echo "FAIL app not under <team>/<app> passed"; exit 1; fi
echo "ok   app at the wrong path rejected"
echo "e2e passed"

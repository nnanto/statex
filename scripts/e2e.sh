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
cleanup() { s=$?; for p in "${PIDS[@]}"; do kill "$p" 2>/dev/null || true; done; if [ $s -ne 0 ] && [ -n "${KEEP:-}" ]; then echo "logs kept in $W"; else rm -rf "$W"; fi; }
trap cleanup EXIT

wait_up() { for _ in $(seq 1 120); do curl -sf "http://127.0.0.1:$1/healthz" >/dev/null && return; sleep 0.5; done; echo "node on $1 did not start"; exit 1; }
check() { if [ "$2" != "$3" ]; then echo "FAIL $1: expected $3, got $2"; exit 1; fi; echo "ok   $1"; }

(cd "$ROOT/examples/counter" && "$S" deploy)
"$S" node --listen 127.0.0.1:9201 --node-id n1 --data-dir "$W/n1" --lease-ttl 3 >"$W/n1.log" 2>&1 & PIDS+=($!)
"$S" node --listen 127.0.0.1:9202 --node-id n2 --data-dir "$W/n2" --lease-ttl 3 >"$W/n2.log" 2>&1 & PIDS+=($!)
N1=${PIDS[0]}
wait_up 9201; wait_up 9202
wait_app() { for port in 9201 9202; do for _ in $(seq 100); do "$S" schema --app "$1" --url http://127.0.0.1:$port >/dev/null 2>&1 && break; sleep 0.2; done; done; }

check "increment via n1" "$("$S" call counter alice increment -a by=5 --url http://127.0.0.1:9201)" 5
check "read via n2 (forwarded)" "$("$S" call counter alice get --url http://127.0.0.1:9202)" 5

(cd "$ROOT/examples/caller" && "$S" deploy >/dev/null)
wait_app caller
check "typed call into another app" "$("$S" call relay r1 bump -a key=alice -a by=2 --app caller --url http://127.0.0.1:9201)" 7
check "callee state after call" "$("$S" call counter alice get --url http://127.0.0.1:9202)" 7
check "call cycle rejected" "$("$S" call relay r1 ping '[["r2", "r1"]]' --app caller --url http://127.0.0.1:9201 2>&1 | grep -c 'call cycle')" 1
(cd "$ROOT/examples/python-caller" && "$S" deploy >/dev/null)
wait_app pycaller
check "python actor calls a rust actor" "$("$S" call relay p1 bump -a key=alice -a by=1 --app pycaller --url http://127.0.0.1:9202)" 8
check "python call cycle rejected" "$("$S" call relay p1 ping '[["p2", "p1"]]' --app pycaller --url http://127.0.0.1:9201 2>&1 | grep -c '"call cycle: pycaller/relay/p1 -> pycaller/relay/p2 -> pycaller/relay/p1"')" 1
check "python unit tests (mock host)" "$(cd "$ROOT/examples/python-caller" && "$S" test -q >/dev/null 2>&1 && echo ok)" ok
(cd "$ROOT/examples/python-counter" && "$S" deploy >/dev/null)
wait_app pycounter
"$S" call counter py1 remind -a delay-ms=300 --app pycounter --url http://127.0.0.1:9201 >/dev/null
for _ in $(seq 50); do [ "$("$S" call counter py1 reminders --app pycounter --url http://127.0.0.1:9202 2>/dev/null)" = 1 ] && break; sleep 0.2; done
check "python alarm fired" "$("$S" call counter py1 reminders --app pycounter --url http://127.0.0.1:9202)" 1

kill -9 "$N1"
check "takeover after crash" "$("$S" call counter alice increment -a by=1 --url http://127.0.0.1:9202)" 9

(cd "$ROOT/examples/counter" && "$S" codegen python --from-url http://127.0.0.1:9202 --url http://127.0.0.1:9202 -o "$W/counter_client.py" >/dev/null)
OUT="$(cd "$W" && python3 - <<'PY'
from counter_client import CounterApp, MethodError, TxError
c = CounterApp()
assert c.counter("alice").increment(by=1) == 10
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

# Monorepo workspace: scaffold, check, registry, compatibility against git.
M="$W/mono"; mkdir -p "$M"; cd "$M"
git init -q && git config user.email e2e@statex && git config user.name e2e
"$S" workspace init --statex "$ROOT" >/dev/null
"$S" new payments/shop --workspace --actor cart >/dev/null
SDK="$(sed -n 's/^statex-guest = { path = "\(.*\)" }$/\1/p' apps/payments/shop/Cargo.toml)"
check "app linked to workspace sdk" "$(cd apps/payments/shop && cd "$SDK" && pwd -P)" "$(cd "$ROOT/crates/guest" && pwd -P)"
check "check --all" "$("$S" check --all | head -1)" "ok   payments/shop"
"$S" registry build >/dev/null && "$S" registry build --check >/dev/null
git add -A && git commit -qm init
sed -i.bak 's/increment: func(by: s64)/increment: func(by: s32)/' apps/payments/shop/wit/app.wit
if "$S" check --all --against HEAD >"$W/check.log" 2>&1; then echo "FAIL breaking change not detected"; exit 1; fi
check "breaking change detected" "$(grep -c 'parameters changed from (by: s64) to (by: s32)' "$W/check.log")" 1
if "$S" registry build --check >/dev/null 2>&1; then echo "FAIL stale registry not detected"; exit 1; fi
echo "ok   stale registry detected"
mkdir -p apps/growth/x && cp -R apps/payments/shop apps/growth/x/notes
if "$S" check --all >"$W/duplicate.log" 2>&1; then echo "FAIL duplicate app name passed"; exit 1; fi
check "duplicate identity diagnosed" "$(grep -c 'app name "payments/shop" is used by both' "$W/duplicate.log")" 1
echo "ok   duplicate app name rejected"
sed -i.bak 's/name = "payments\/shop"/name = "notes"/' apps/growth/x/notes/statex.toml
"$S" check --all >"$W/unique.log"
check "nested app uses its manifest name" "$(grep -c '^ok   notes$' "$W/unique.log")" 1
echo "ok   unique app name at an independent nested path accepted"
echo "e2e passed"

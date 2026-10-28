#!/usr/bin/env bash
# Runs `tests/distributed/guest.rs` on node B of a control and two nodes, and
# fails unless it reports success within the timeout.
#
# Uses the sibling runtime build like the test runner does; set LUNATIC to use
# another binary.
set -euo pipefail

root=$(cd "$(dirname "$0")/../.." && pwd)
lunatic=${LUNATIC:-$root/../lunatic/target/release/lunatic}
timeout_secs=60
# Lines that mean a guest process failed, on any of the nodes. A panic alone
# isn't one: the guest catches some on purpose.
failures='failed, notifying|Error running wasm'

cd "$root"
cargo build --example distributed_checks
wasm=$root/target/wasm32-wasip1/debug/examples/distributed_checks.wasm

logs=$(mktemp -d)
pids=()
cleanup() {
    for pid in "${pids[@]}"; do
        kill "$pid" 2>/dev/null || true
    done
    wait 2>/dev/null || true
    rm -rf "$logs"
}
trap cleanup EXIT

start() {
    local name=$1
    shift
    # Otherwise lunatic may think it's running as a `cargo test` runner.
    env -u CARGO_MANIFEST_DIR RUST_LOG=info "$lunatic" "$@" >"$logs/$name.log" 2>&1 &
    pids+=($!)
}

# Waits for `pattern` in the log of `name`, and prints the matching line.
wait_for() {
    local name=$1 pattern=$2
    local deadline=$((SECONDS + timeout_secs))
    while ((SECONDS < deadline)); do
        if grep -Eq "$failures" "$logs"/*.log 2>/dev/null; then
            echo "A process failed while waiting for $name to print \"$pattern\":" >&2
            tail -n +1 "$logs"/*.log >&2
            exit 1
        fi
        if grep -m1 -F "$pattern" "$logs/$name.log" 2>/dev/null; then
            return
        fi
        sleep 0.2
    done
    echo "Timed out waiting for $name to print \"$pattern\":" >&2
    tail -n +1 "$logs"/*.log >&2
    exit 1
}

# Two free UDP ports, picked together so that the nodes don't get the same one.
read -r port_a port_b < <(python3 -c '
import socket
a, b = socket.socket(type=socket.SOCK_DGRAM), socket.socket(type=socket.SOCK_DGRAM)
a.bind(("127.0.0.1", 0)); b.bind(("127.0.0.1", 0))
print(a.getsockname()[1], b.getsockname()[1])')

start control control
control_url=$(wait_for control "Register URL: " | sed 's/.*Register URL: //')

start node_a node --bind-socket "127.0.0.1:$port_a" "$control_url"
node_a=${pids[-1]}
wait_for node_a "Registration successful" >/dev/null

start node_b node --bind-socket "127.0.0.1:$port_b" --wasm "$wasm" "$control_url"
wait_for node_b "stop node A" >/dev/null
# Crash it, so that it doesn't leave the cluster cleanly.
kill -9 "$node_a"
wait "$node_a" 2>/dev/null || true
wait_for node_b "distributed checks ok"

# The panics the guest caught must be the ones for these failures.
for expected in \
    'request\(\) failed: process [0-9]+ on node [0-9]+ does not exist' \
    'Err` value: Node does not exist' \
    'start_as\(\) failed on node [0-9]+: Node does not exist' \
    "request\\(\\) failed: node [0-9]+ isn't in the cluster, or left it before process 1 replied" \
    "deferred_request\\(\\) failed: node [0-9]+ isn't in the cluster, or left it before process [0-9]+ replied"; do
    if ! grep -Eq "$expected" "$logs/node_b.log"; then
        echo "node_b didn't print a panic matching \"$expected\":" >&2
        cat "$logs/node_b.log" >&2
        exit 1
    fi
done

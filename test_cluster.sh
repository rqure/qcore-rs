#!/usr/bin/env bash
set -euo pipefail

# Start all servers simultaneously and stop them all on Ctrl-C

# Prefer release binaries, fall back to debug if not found
BIN_DIR=${BIN_DIR:-./target/release}
if [ ! -x "$BIN_DIR/qcore-rs" ] || [ ! -x "$BIN_DIR/snapshot-tool" ]; then
    if [ -x ./target/debug/qcore-rs ] && [ -x ./target/debug/snapshot-tool ]; then
        BIN_DIR=./target/debug
    fi
fi

rm -rf ./data

"$BIN_DIR"/snapshot-tool factory-restore \
    --input base-topology.json \
    --machine qos-a \
    --force

# -------- cleanup handler --------
cleanup() {
    echo
    echo "Stopping cluster..."
    trap - INT TERM EXIT
    # Kill entire process group (all background jobs started by this script)
    kill -- -$$ 2>/dev/null || true
}
trap 'cleanup; exit 0' INT TERM
trap 'cleanup' EXIT

# -------- start nodes --------
start_node() {
    local name=$1; shift
    echo "Starting ${name}..."
    "$@" &
}

start_node qos-a "$BIN_DIR"/qcore-rs \
    --machine qos-a \
    --peer-addresses localhost:9101,localhost:9102 \
    --client-port 9100

start_node qos-b "$BIN_DIR"/qcore-rs \
    --machine qos-b \
    --peer-addresses localhost:9100,localhost:9102 \
    --client-port 9101

start_node qos-c "$BIN_DIR"/qcore-rs \
    --machine qos-c \
    --peer-addresses localhost:9100,localhost:9101 \
    --client-port 9102

echo "All nodes started."
echo "Press Ctrl-C to stop the cluster."

# -------- wait for first exit --------
# If any node dies, cleanup will kill the rest
wait -n
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

PIDS=()

cleanup() {
	echo
	echo "Stopping cluster..."
	# Make sure we don't re-enter cleanup
	trap - INT TERM EXIT
	if [ ${#PIDS[@]} -gt 0 ]; then
		kill "${PIDS[@]}" 2>/dev/null || true
		# Wait for processes to exit to avoid zombies
		wait "${PIDS[@]}" 2>/dev/null || true
	else
		# Fallback: kill any background jobs
		jobs -p | xargs -r kill 2>/dev/null || true
	fi
}

trap 'cleanup; exit 0' INT TERM
trap 'cleanup' EXIT

# Helper to start a node in background and track its PID
start_node() {
	local name=$1; shift
	echo "Starting ${name}..."
	"$@" &
	PIDS+=("$!")
}

start_node qos-a "$BIN_DIR"/qcore-rs \
	--machine qos-a \
	--peer-addresses localhost:9001,localhost:9002 \
	--peer-port 9000 \
	--client-port 9100

start_node qos-b "$BIN_DIR"/qcore-rs \
	--machine qos-b \
	--peer-addresses localhost:9000,localhost:9002 \
	--peer-port 9001 \
	--client-port 9101

start_node qos-c "$BIN_DIR"/qcore-rs \
	--machine qos-c \
	--peer-addresses localhost:9000,localhost:9001 \
	--peer-port 9002 \
	--client-port 9102

echo "All nodes started. PIDs: ${PIDS[*]}"
echo "Press Ctrl-C to stop the cluster."

# Keep the script running until any background process exits or Ctrl-C is pressed
wait
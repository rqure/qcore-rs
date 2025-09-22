#!/usr/bin/env bash
set -euo pipefail

# Start all servers simultaneously and stop them all on Ctrl-C

# Number of nodes to spawn (default: 3)
NUM_NODES=${1:-3}
BASE_PORT=9100

# Validate input
if ! [[ "$NUM_NODES" =~ ^[0-9]+$ ]] || [ "$NUM_NODES" -lt 1 ]; then
    echo "Usage: $0 [number_of_nodes]"
    echo "  number_of_nodes: positive integer (default: 3)"
    exit 1
fi

echo "Starting cluster with $NUM_NODES nodes..."

# Prefer release binaries, fall back to debug if not found
BIN_DIR=${BIN_DIR:-./target/release}
if [ ! -x "$BIN_DIR/qcore-rs" ] || [ ! -x "$BIN_DIR/snapshot-tool" ]; then
    if [ -x ./target/debug/qcore-rs ] && [ -x ./target/debug/snapshot-tool ]; then
        BIN_DIR=./target/debug
    fi
fi

rm -rf ./data

# Generate node name (e.g., qos-a for node 0, qos-b for node 1, etc.)
get_node_name() {
    local node_id=$1
    local letters="abcdefghijklmnopqrstuvwxyz"
    echo "qos-${letters:$node_id:1}"
}

# Generate peer addresses for a node (all other nodes)
get_peer_addresses() {
    local current_node=$1
    local peers=""
    for ((i=0; i<NUM_NODES; i++)); do
        if [ $i -ne $current_node ]; then
            if [ -n "$peers" ]; then
                peers="${peers},"
            fi
            local peer_name=$(get_node_name $i)
            peers="${peers}${peer_name}=localhost:$((BASE_PORT + i))"
        fi
    done
    echo "$peers"
}

# Use first node for factory-restore
FIRST_NODE=$(get_node_name 0)
"$BIN_DIR"/snapshot-tool factory-restore \
    --input base-topology.json \
    --machine "$FIRST_NODE" \
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

# Start all nodes dynamically
for ((i=0; i<NUM_NODES; i++)); do
    NODE_NAME=$(get_node_name $i)
    NODE_PORT=$((BASE_PORT + i))
    PEER_ADDRESSES=$(get_peer_addresses $i)
    
    start_node "$NODE_NAME" "$BIN_DIR"/qcore-rs \
        --machine "$NODE_NAME" \
        --peer-addresses "$PEER_ADDRESSES" \
        --port "$NODE_PORT"
done

echo "All $NUM_NODES nodes started."
echo "Press Ctrl-C to stop the cluster."

# -------- wait for first exit --------
# If any node dies, cleanup will kill the rest
wait -n
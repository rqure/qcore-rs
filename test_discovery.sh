#!/bin/bash

# Script to test mDNS discovery functionality
# This script starts two nodes and shows them discovering each other

set -e

echo "Building qcore-rs..."
cd /workspace/qcore-rs
cargo build --release

echo "Starting discovery test..."

# Check if schemas.yaml exists
if [ ! -f "schemas.yaml" ]; then
    echo "Error: schemas.yaml not found. Please ensure the schema file exists."
    exit 1
fi

# Start first node in background
echo "Starting node 1..."
RUST_LOG=info ./target/release/qcore-rs \
    --id 1 \
    --ws-addr 127.0.0.1:8080 \
    --config-file schemas.yaml \
    --enable-discovery \
    --min-nodes 1 \
    --auto-init \
    --discovery-timeout 10 &

NODE1_PID=$!
echo "Node 1 started with PID $NODE1_PID"

# Wait a moment for first node to start
sleep 2

# Start second node in background
echo "Starting node 2..."
RUST_LOG=info ./target/release/qcore-rs \
    --id 2 \
    --ws-addr 127.0.0.1:8081 \
    --config-file schemas.yaml \
    --enable-discovery \
    --min-nodes 1 \
    --auto-init \
    --discovery-timeout 10 &

NODE2_PID=$!
echo "Node 2 started with PID $NODE2_PID"

echo "Both nodes are running. Wait 15 seconds to see discovery in action..."
echo "Look for messages like 'Discovered node: X at Y' in the logs above."

# Wait for discovery to happen
sleep 15

echo "Test complete. Stopping nodes..."

# Clean shutdown
kill $NODE1_PID $NODE2_PID 2>/dev/null || true
wait $NODE1_PID $NODE2_PID 2>/dev/null || true

echo "Discovery test finished!"

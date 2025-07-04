#!/bin/bash

# test_cluster.sh - Simple test script for the qcore-rs cluster
# Usage: ./test_cluster.sh [port]
# Example: ./test_cluster.sh 8080

set -e

# Default port
DEFAULT_PORT=8080

# Colors for output
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

print_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

print_info() {
    echo -e "${YELLOW}[INFO]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Parse command line arguments
if [[ $# -eq 0 ]]; then
    PORT=$DEFAULT_PORT
elif [[ $# -eq 1 ]] && [[ $1 =~ ^[0-9]+$ ]]; then
    PORT=$1
else
    print_error "Usage: $0 [port]"
    print_error "Example: $0 8080"
    exit 1
fi

# Check if qcore-client binary exists
if [[ ! -f "target/debug/qcore-client" ]]; then
    print_error "qcore-client binary not found. Building..."
    cargo build --bin qcore-client
    if [[ $? -ne 0 ]]; then
        print_error "Failed to build qcore-client"
        exit 1
    fi
fi

print_info "Testing cluster node at port $PORT..."

# Test basic connectivity and get metrics
print_info "Getting cluster metrics..."
if ./target/debug/qcore-client --address "127.0.0.1:$PORT" metrics; then
    print_success "Successfully connected to cluster!"
else
    print_error "Failed to connect to cluster on port $PORT"
    exit 1
fi

echo ""
print_info "Testing entity creation..."

# Create a test entity
if ./target/debug/qcore-client --address "127.0.0.1:$PORT" create "TestEntity" "test1"; then
    print_success "Successfully created test entity!"
else
    print_error "Failed to create test entity"
fi

echo ""
print_info "Testing entity listing..."

# List entities
if ./target/debug/qcore-client --address "127.0.0.1:$PORT" list "TestEntity" --limit 5; then
    print_success "Successfully listed entities!"
else
    print_error "Failed to list entities"
fi

echo ""
print_success "Cluster test completed!"

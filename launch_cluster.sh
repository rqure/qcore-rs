#!/bin/bash

# launch_cluster.sh - Launch a qcore-rs cluster with N nodes
# Usage: ./launch_cluster.sh <number_of_nodes>
# Example: ./launch_cluster.sh 3

set -e

# Default values
DEFAULT_NODES=3
BASE_PORT=8080
BASE_NODE_ID=1

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Array to store PIDs of launched nodes
declare -a NODE_PIDS=()

# Function to print colored output
print_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

print_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Function to cleanup on exit
cleanup() {
    # Disable exit on error during cleanup
    set +e
    
    print_info "Shutting down cluster..."
    
    # Kill all node processes
    for pid in "${NODE_PIDS[@]}"; do
        if kill -0 "$pid" 2>/dev/null; then
            print_info "Stopping node with PID $pid"
            kill -TERM "$pid" 2>/dev/null
        fi
    done
    
    # Wait a bit for graceful shutdown
    sleep 2
    
    # Force kill if still running
    for pid in "${NODE_PIDS[@]}"; do
        if kill -0 "$pid" 2>/dev/null; then
            print_warning "Force killing node with PID $pid"
            kill -KILL "$pid" 2>/dev/null
        fi
    done
    
    print_success "All nodes stopped"
    
    # Re-enable exit on error
    set -e
}

# Function to build the project if needed
build_project() {
    if [[ ! -f "target/debug/qcore-rs" ]]; then
        print_info "Binary not found, building project..."
        cargo build
        if [[ $? -ne 0 ]]; then
            print_error "Failed to build project"
            exit 1
        fi
        print_success "Project built successfully"
    else
        print_info "Using existing binary: target/debug/qcore-rs"
    fi
}

# Function to check if port is available
check_port() {
    local port=$1
    if lsof -Pi :$port -sTCP:LISTEN -t >/dev/null 2>&1; then
        return 1  # Port is in use
    else
        return 0  # Port is available
    fi
}

# Function to find available port starting from base port
find_available_port() {
    local start_port=$1
    local port=$start_port
    
    while ! check_port $port; do
        ((port++))
        if [[ $port -gt $((start_port + 100)) ]]; then
            print_error "Could not find available port after trying 100 ports starting from $start_port"
            exit 1
        fi
    done
    
    echo $port
}

# Function to launch a single node
launch_node() {
    local node_id=$1
    local port=$2
    local data_dir="./cluster_data/node_$node_id"
    
    # Create data directory
    mkdir -p "$data_dir"
    
    # Check if port is available
    if ! check_port $port; then
        print_error "Port $port is already in use"
        exit 1
    fi
    
    print_info "Launching node $node_id on port $port..."
    
    # Launch the node in background
    ./target/debug/qcore-rs \
        --id $node_id \
        --ws-addr "127.0.0.1:$port" \
        --data-dir "$data_dir" \
        --enable-discovery \
        --min-nodes $NUM_NODES \
        --discovery-timeout 60 \
        --auto-init \
        > "./cluster_data/node_${node_id}.log" 2>&1 &
    
    local pid=$!
    NODE_PIDS+=($pid)
    
    print_success "Node $node_id launched with PID $pid on port $port"
    echo "  - Data directory: $data_dir"
    echo "  - Log file: ./cluster_data/node_${node_id}.log"
    echo "  - WebSocket URL: ws://127.0.0.1:$port"
}

# Function to wait for nodes to be ready
wait_for_nodes() {
    print_info "Waiting for nodes to start up and discover each other..."
    
    local max_attempts=30
    local attempt=0
    
    # Disable exit on error for this function
    set +e
    
    while [[ $attempt -lt $max_attempts ]]; do
        local ready_count=0
        
        for ((i=0; i<NUM_NODES; i++)); do
            local node_id=$((BASE_NODE_ID + i))
            local port=$((BASE_PORT + i))
            
            # Check if node is responding (simple port check)
            if timeout 1 bash -c "</dev/tcp/127.0.0.1/$port" 2>/dev/null; then
                ((ready_count++))
            fi
        done
        
        if [[ $ready_count -eq $NUM_NODES ]]; then
            print_success "All $NUM_NODES nodes are ready!"
            set -e  # Re-enable exit on error
            return 0
        fi
        
        echo -n "."
        sleep 1
        ((attempt++))
    done
    
    print_warning "Not all nodes became ready within timeout, but continuing..."
    set -e  # Re-enable exit on error
}

# Function to display cluster status
show_cluster_status() {
    echo ""
    echo "============================================"
    echo "           CLUSTER STATUS"
    echo "============================================"
    echo "Number of nodes: $NUM_NODES"
    echo "Base port: $BASE_PORT"
    echo ""
    
    for ((i=0; i<NUM_NODES; i++)); do
        local node_id=$((BASE_NODE_ID + i))
        local port=$((BASE_PORT + i))
        local pid=${NODE_PIDS[$i]}
        
        echo "Node $node_id:"
        echo "  - PID: $pid"
        echo "  - Port: $port"
        echo "  - WebSocket: ws://127.0.0.1:$port"
        echo "  - Data Dir: ./cluster_data/node_$node_id"
        echo "  - Log File: ./cluster_data/node_${node_id}.log"
        
        # Check if process is still running (disable set -e temporarily)
        set +e
        if kill -0 "$pid" 2>/dev/null; then
            echo -e "  - Status: ${GREEN}RUNNING${NC}"
        else
            echo -e "  - Status: ${RED}STOPPED${NC}"
        fi
        set -e
        echo ""
    done
    
    echo "============================================"
    echo ""
    echo "Useful commands:"
    echo "  - View node logs: tail -f ./cluster_data/node_X.log"
    echo "  - Test with client: ./target/debug/qcore-client --address 127.0.0.1:$BASE_PORT metrics"
    echo "  - Stop cluster: Press Ctrl+C"
    echo ""
}

# Main script starts here
main() {
    # Change to the directory where the script is located
    SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
    cd "$SCRIPT_DIR"
    
    print_info "Starting qcore-rs cluster launcher..."
    print_info "Working directory: $(pwd)"
    
    # Parse command line arguments
    if [[ $# -eq 0 ]]; then
        NUM_NODES=$DEFAULT_NODES
        print_info "No number specified, using default: $NUM_NODES nodes"
    elif [[ $# -eq 1 ]] && [[ $1 =~ ^[0-9]+$ ]] && [[ $1 -gt 0 ]]; then
        NUM_NODES=$1
        print_info "Launching cluster with $NUM_NODES nodes"
    else
        print_error "Usage: $0 [number_of_nodes]"
        print_error "Example: $0 3"
        exit 1
    fi
    
    # Validate number of nodes
    if [[ $NUM_NODES -lt 1 ]] || [[ $NUM_NODES -gt 10 ]]; then
        print_error "Number of nodes must be between 1 and 10"
        exit 1
    fi
    
    # Build project if needed
    build_project
    
    # Create cluster data directory
    mkdir -p "./cluster_data"
    
    # Set up signal handlers for cleanup
    trap cleanup EXIT INT TERM
    
    # Launch all nodes
    print_info "Launching $NUM_NODES nodes..."
    for ((i=0; i<NUM_NODES; i++)); do
        local node_id=$((BASE_NODE_ID + i))
        local port=$((BASE_PORT + i))
        
        launch_node $node_id $port
        
        # Small delay between launches to avoid race conditions
        sleep 1
    done
    
    # Wait for nodes to be ready
    wait_for_nodes
    
    # Show cluster status
    show_cluster_status
    
    # Keep script running and wait for user input
    print_info "Cluster is running. Press Ctrl+C to stop all nodes."
    print_info "Monitoring node health every 5 seconds..."
    
    # Monitor nodes and restart if needed
    while true; do
        sleep 5
        
        # Check if any nodes have died (disable set -e temporarily)
        set +e
        for ((i=0; i<NUM_NODES; i++)); do
            local pid=${NODE_PIDS[$i]}
            if ! kill -0 "$pid" 2>/dev/null; then
                local node_id=$((BASE_NODE_ID + i))
                print_warning "Node $node_id (PID $pid) has stopped unexpectedly"
                # Could add auto-restart logic here if desired
            fi
        done
        set -e
    done
}

# Run main function
main "$@"

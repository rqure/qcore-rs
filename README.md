# qcore-rs

A distributed data store built with Rust and OpenRaft, featuring automatic node discovery using mDNS.

## Features

- **Distributed Consensus**: Built on OpenRaft for reliable distributed consensus
- **Automatic Node Discovery**: Uses mDNS for automatic peer discovery on local networks
- **WebSocket API**: Real-time data access through WebSocket connections
- **Schema Management**: Flexible entity and field schema system
- **Data Types**: Support for various data types including strings, numbers, blobs, entity references, and lists

## Node Discovery

The system supports automatic node discovery using multicast DNS (mDNS). This allows nodes to find each other on local networks without manual configuration.

### Usage

Start a node with discovery enabled:

```bash
# Start first node
./qcore-rs --id 1 --ws-addr 127.0.0.1:8080 --enable-discovery --min-nodes 2 --auto-init

# Start second node  
./qcore-rs --id 2 --ws-addr 127.0.0.1:8081 --enable-discovery --min-nodes 2 --auto-init
```

### Discovery Options

- `--enable-discovery`: Enable mDNS-based node discovery
- `--min-nodes N`: Wait for at least N nodes before initializing cluster (default: 1)
- `--discovery-timeout T`: Timeout in seconds to wait for node discovery (default: 30)
- `--auto-init`: Automatically initialize cluster when minimum nodes are discovered

### How It Works

1. Each node registers itself as an mDNS service with type `_qcore._tcp.local.`
2. Nodes browse for other qcore services on the network
3. When a node is discovered, it's added to the cluster
4. If `--auto-init` is enabled and minimum nodes are reached, the cluster initializes automatically

## Testing Discovery

You can test the discovery functionality by running multiple instances:

```bash
# Build the project first
cargo build --release

# Terminal 1: Start first node
RUST_LOG=info ./target/release/qcore-rs --id 1 --ws-addr 127.0.0.1:8080 --config-file schemas.yaml --enable-discovery --min-nodes 1 --auto-init

# Terminal 2: Start second node  
RUST_LOG=info ./target/release/qcore-rs --id 2 --ws-addr 127.0.0.1:8081 --config-file schemas.yaml --enable-discovery --min-nodes 1 --auto-init
```

### Automated Test

Use the provided test script:

```bash
./test_discovery.sh
```

### Expected Output

You should see log messages indicating successful discovery and cluster initialization:

```
INFO qcore_rs::discovery - Registered mDNS service: qcore-node-1 on 172.18.0.2:8080
INFO qcore_rs::discovery - Discovered node: 2 at 172.18.0.2:8081
INFO qcore_rs - Discovered node 2 (1/1)
INFO qcore_rs - Minimum nodes reached, auto-initializing cluster...
INFO qcore_rs - Initializing cluster with nodes: {1: BasicNode { addr: "127.0.0.1:8080" }, 2: BasicNode { addr: "127.0.0.1:8081" }}
INFO qcore_rs - Cluster initialized successfully
INFO qcore_rs::websocket - New WebSocket connection from: 127.0.0.1:xxxxx
```
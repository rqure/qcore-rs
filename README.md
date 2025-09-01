# qcore-rs

A distributed data store built with Rust and OpenRaft, featuring automatic node discovery using mDNS.

## Features

- **Distributed Consensus**: Built on OpenRaft for reliable distributed consensus
- **Persistent Storage**: Automatic persistence of Raft logs and snapshots to disk
- **Node-Specific Data Directories**: Each node stores data in its own directory structure
- **Automatic Node Discovery**: Uses mDNS for automatic peer discovery on local networks
- **WebSocket API**: Real-time data access through WebSocket connections
- **Schema Management**: Flexible entity and field schema system
- **Data Types**: Support for various data types including strings, numbers, blobs, entity references, and lists
- **Command-Line Client**: Easy-to-use client tool for interacting with the cluster
- **Snapshot Management**: Automatic snapshot creation and cleanup to manage disk space

## Quick Start

### 1. Start a Cluster

Start multiple nodes with discovery enabled:

```bash
# Build the project first
cargo build --release

# Terminal 1: Start first node
RUST_LOG=info ./target/release/qcore-rs --id 1 --ws-addr 127.0.0.1:8080 --config-file schemas.yaml --enable-discovery --min-nodes 2 --auto-init --data-dir ./cluster_data

# Terminal 2: Start second node  
RUST_LOG=info ./target/release/qcore-rs --id 2 --ws-addr 127.0.0.1:8081 --config-file schemas.yaml --enable-discovery --min-nodes 2 --auto-init --data-dir ./cluster_data
```

### 2. Use the Client

Once the cluster is running, you can interact with it using the built-in client:

```bash
# Get cluster status
cargo run --bin qcore-client -- metrics

# Create a new entity
cargo run --bin qcore-client -- create User --name "John Doe"

# Write data to an entity (replace EntityId with actual ID from create command)
cargo run --bin qcore-client -- write "User$uuid" "age" "30"
cargo run --bin qcore-client -- write "User$uuid" "email" '"john.doe@example.com"'

# Read data from an entity
cargo run --bin qcore-client -- read "User$uuid" "age"
cargo run --bin qcore-client -- read "User$uuid" "email"

# Delete an entity
cargo run --bin qcore-client -- delete "User$uuid"
```

### 3. Run Demo

Use the provided demo scripts:

```bash
# Complete client functionality demo
./demo_client.sh

# Interactive client testing (requires running cluster)
./test_client.sh
```

## Client Commands

The `qcore-client` tool provides the following commands:

### Basic Operations

```bash
# Get cluster metrics and status
cargo run --bin qcore-client -- metrics

# Create a new entity
cargo run --bin qcore-client -- create <entity_type> --name <name> [--parent <parent_id>]

# Read a field from an entity
cargo run --bin qcore-client -- read <entity_id> <field>

# Write a value to an entity field
cargo run --bin qcore-client -- write <entity_id> <field> <value> [--behavior set|add|subtract]

# Delete an entity
cargo run --bin qcore-client -- delete <entity_id>

# Get schema information
cargo run --bin qcore-client -- schema <entity_type> [--complete]
```

### Value Formats

The client supports various data types:

- **Strings**: `"hello world"` or simple strings without quotes
- **Numbers**: `42` or `3.14`
- **Booleans**: `true` or `false`
- **Entity References**: `EntityType$id`
- **Entity Lists**: `["EntityType$id1", "EntityType$id2"]`

### Write Behaviors

- `set` (default): Replace the current value
- `add`: Add to the current numeric value
- `subtract`: Subtract from the current numeric value

### Examples

```bash
# Create entities
cargo run --bin qcore-client -- create User --name "Alice"
cargo run --bin qcore-client -- create Post --parent "User$123" --name "My Post"

# Write different data types
cargo run --bin qcore-client -- write "User$123" "age" "25"
cargo run --bin qcore-client -- write "User$123" "email" '"alice@example.com"'
cargo run --bin qcore-client -- write "User$123" "active" "true"

# Arithmetic operations
cargo run --bin qcore-client -- write "User$123" "score" "100"
cargo run --bin qcore-client -- write "User$123" "score" "10" --behavior add
cargo run --bin qcore-client -- write "User$123" "score" "5" --behavior subtract

# Read data
cargo run --bin qcore-client -- read "User$123" "age"
cargo run --bin qcore-client -- read "User$123" "email"
```

## Tools

qcore-rs includes several command-line tools for interacting with and managing the data store:

### qcore-client

The main client tool for performing CRUD operations on the data store.

```bash
# Get cluster metrics and status
cargo run --bin qcore-client -- metrics

# Create, read, write, and delete entities
cargo run --bin qcore-client -- create User --name "John Doe"
cargo run --bin qcore-client -- read "User$123" "name"
cargo run --bin qcore-client -- write "User$123" "age" "30"
cargo run --bin qcore-client -- delete "User$123"
```

### snapshot-tool

Tool for taking and restoring JSON snapshots of the data store.

```bash
# Take a snapshot
cargo run --bin snapshot-tool take --output backup.json

# Restore from snapshot
cargo run --bin snapshot-tool restore --input backup.json

# Factory restore (creates data files directly)
cargo run --bin snapshot-tool factory-restore --input backup.json --machine-id restored

# Validate a snapshot file
cargo run --bin snapshot-tool validate --input backup.json
```

### tree-tool

Display the hierarchical tree structure of the data store, similar to the Unix `tree` command.

```bash
# Display the complete tree structure
cargo run --bin tree-tool

# Show with entity types and IDs
cargo run --bin tree-tool --verbose

# Limit depth and start from specific entity
cargo run --bin tree-tool --max-depth 3 --start-from "Folder$123"
```

For detailed usage information, see [README_tree_tool.md](README_tree_tool.md).

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

## Important Notes

### Cluster Initialization

For the client to perform write operations (create, write, delete), the cluster must have an elected leader. This requires:

1. **Multi-node setup**: Start at least 2 nodes with the same `--min-nodes` value
2. **Auto-initialization**: Use `--auto-init` flag to automatically initialize when minimum nodes are discovered
3. **Wait for leader election**: Check `cargo run --bin qcore-client -- metrics` until you see a `Current Leader`

Single-node clusters remain in "Learner" state and cannot accept write operations. Read operations and cluster metrics work in any state.

### Example Working Setup

```bash
# Terminal 1
./target/release/qcore-rs --id 1 --ws-addr 127.0.0.1:8080 --config-file schemas.yaml --enable-discovery --min-nodes 2 --auto-init

# Terminal 2  
./target/release/qcore-rs --id 2 --ws-addr 127.0.0.1:8081 --config-file schemas.yaml --enable-discovery --min-nodes 2 --auto-init

# Wait for "Cluster initialized successfully" in logs, then:
cargo run --bin qcore-client -- metrics  # Should show Current Leader
cargo run --bin qcore-client -- create User "John Doe"  # Should work
```

## Discovery Options

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

## Persistent Storage

qcore-rs automatically persists all data to disk, ensuring durability across node restarts.

### Data Directory Structure

Each node creates its own data directory structure:

```
data_dir/
└── node_<id>/
    ├── logs/           # Raft log entries (one JSON file per entry)
    │   ├── 1.json
    │   ├── 2.json
    │   └── ...
    ├── snapshots/      # State machine snapshots
    │   ├── snapshot_1688123456.json
    │   └── ...
    ├── vote.json       # Current vote state
    └── committed.json  # Last committed log ID
```

### Configuration

Use the `--data-dir` parameter to specify where data should be stored:

```bash
# Store data in a custom directory
./target/release/qcore-rs --id 1 --ws-addr 127.0.0.1:8080 --data-dir ./my_cluster_data
```

#### Log Management Configuration

Control log file growth with these options:

```bash
# Limit log files and size
./target/release/qcore-rs --id 1 --ws-addr 127.0.0.1:8080 \
  --max-log-files 500 \
  --max-log-size-mb 50 \
  --log-cleanup-interval 50
```

**Log Management Options:**
- `--max-log-files`: Maximum number of log files to keep (default: 1000)
- `--max-log-size-mb`: Maximum total size of log files in MB (default: 100)
- `--log-cleanup-interval`: Check for cleanup every N log entries (default: 100)

### Recovery

When a node restarts, it automatically:

1. **Loads Raft state**: Restores vote and committed log information
2. **Replays log entries**: Recreates the state machine from persisted log entries
3. **Loads latest snapshot**: If available, loads the most recent snapshot to speed up recovery
4. **Rejoins cluster**: Reconnects with existing cluster members

### Snapshot Management

- Snapshots are automatically created during normal operation
- Old snapshots are cleaned up automatically (keeps 5 most recent by default)
- Snapshots include the complete state machine data for fast recovery

### Data Safety

- All writes are persisted before being acknowledged
- Log entries are stored before being applied to the state machine  
- Snapshots provide recovery points for large datasets
- Each node maintains its own copy of all data for fault tolerance
- **Bounded log growth**: Automatic cleanup prevents unbounded disk usage
- **Configurable retention**: Control how many logs to keep and maximum size
- **Size-based cleanup**: Removes oldest logs when size limits are exceeded
- **Count-based cleanup**: Maintains a maximum number of log files
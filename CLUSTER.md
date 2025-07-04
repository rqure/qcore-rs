# qcore-rs Cluster Launcher

This directory contains scripts to easily launch and test a qcore-rs cluster.

## Scripts

### `launch_cluster.sh`

Launches a qcore-rs cluster with N nodes where N is configurable.

**Usage:**
```bash
./launch_cluster.sh [number_of_nodes]
```

**Examples:**
```bash
# Launch a 3-node cluster (default)
./launch_cluster.sh

# Launch a 5-node cluster
./launch_cluster.sh 5
```

**Features:**
- Automatically builds the project if the binary doesn't exist
- Creates separate data directories for each node (`./cluster_data/node_X/`)
- Generates individual log files for each node (`./cluster_data/node_X.log`)
- Uses sequential ports starting from 8080 (8080, 8081, 8082, etc.)
- Enables mDNS discovery for automatic cluster formation
- Graceful shutdown when Ctrl+C is pressed
- Monitors node health and reports status
- Validates port availability before starting nodes

**Node Configuration:**
- Each node gets a unique ID (starting from 1)
- WebSocket addresses: `127.0.0.1:8080`, `127.0.0.1:8081`, etc.
- Data directories: `./cluster_data/node_1/`, `./cluster_data/node_2/`, etc.
- Log files: `./cluster_data/node_1.log`, `./cluster_data/node_2.log`, etc.

### `test_cluster.sh`

Simple test script to verify cluster functionality.

**Usage:**
```bash
./test_cluster.sh [port]
```

**Examples:**
```bash
# Test the first node (default port 8080)
./test_cluster.sh

# Test a specific node
./test_cluster.sh 8081
```

**What it tests:**
- Basic connectivity to the cluster
- Cluster metrics retrieval
- Entity creation
- Entity listing

## Quick Start

1. **Launch a 3-node cluster:**
   ```bash
   ./launch_cluster.sh 3
   ```

2. **In another terminal, test the cluster:**
   ```bash
   ./test_cluster.sh
   ```

3. **View logs from a specific node:**
   ```bash
   tail -f ./cluster_data/node_1.log
   ```

4. **Use the client directly:**
   ```bash
   ./target/debug/qcore-client --address 127.0.0.1:8080 metrics
   ./target/debug/qcore-client --address 127.0.0.1:8080 create "TestEntity" "test1"
   ./target/debug/qcore-client --address 127.0.0.1:8080 list "TestEntity"
   ```

5. **Stop the cluster:**
   Press `Ctrl+C` in the terminal running `launch_cluster.sh`

## Directory Structure

When running the cluster, the following structure is created:

```
qcore-rs/
├── launch_cluster.sh
├── test_cluster.sh
├── cluster_data/
│   ├── node_1/
│   │   └── [raft data files]
│   ├── node_2/
│   │   └── [raft data files]
│   ├── node_3/
│   │   └── [raft data files]
│   ├── node_1.log
│   ├── node_2.log
│   └── node_3.log
└── target/debug/
    ├── qcore-rs
    └── qcore-client
```

## Troubleshooting

### Port conflicts
If you get port conflict errors, the script will automatically find the next available port. You can also change the `BASE_PORT` variable in the script.

### Build failures
Make sure you have Rust installed and run:
```bash
cargo build
```

### Connection issues
- Ensure no firewall is blocking the ports
- Check that the nodes are actually running: `ps aux | grep qcore-rs`
- View the logs for error messages: `tail -f ./cluster_data/node_1.log`

### Cleanup
To clean up all data and logs:
```bash
rm -rf ./cluster_data/
```

## Advanced Usage

### Custom Configuration

You can modify the script variables at the top of `launch_cluster.sh`:

```bash
# Default values
DEFAULT_NODES=3
BASE_PORT=8080
BASE_NODE_ID=1
```

### Running with Custom Schema

If you have a custom `schemas.yaml` file, the nodes will automatically use it. Make sure it's in the same directory as the binary.

### Production Deployment

For production deployment, consider:
- Using systemd services instead of the bash script
- Configuring proper logging (syslog, etc.)
- Setting up monitoring and alerting
- Using a proper process manager
- Configuring firewalls and security
- Using persistent storage volumes

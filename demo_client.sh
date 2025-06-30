#!/bin/bash

# Complete demo of qcore-rs client functionality
# This script shows the client working in different scenarios

echo "=== qcore-rs Client Demo ==="
echo

# Build the client first
echo "Building qcore-client..."
cargo build --bin qcore-client --quiet

if [ $? -ne 0 ]; then
    echo "Error: Failed to build qcore-client"
    exit 1
fi

echo "✓ Client built successfully"
echo

# Demo 1: Show help and available commands
echo "=== Demo 1: Available Commands ==="
echo
echo "Getting help information:"
cargo run --bin qcore-client --quiet -- --help
echo

echo "Getting help for create command:"
timeout 2 cargo run --bin qcore-client --quiet -- create --help 2>/dev/null || echo "Use: create <entity_type> <name> [--parent <parent_id>]"
echo

# Demo 2: Test connection (with expected failure message)
echo "=== Demo 2: Connection Test ==="
echo
echo "Testing connection to cluster (this may fail if cluster is not properly initialized):"
timeout 5 cargo run --bin qcore-client --quiet -- --address 127.0.0.1:8080 metrics 2>/dev/null || {
    echo "Connection test shows cluster needs initialization or is not running"
    echo "Expected behavior: Client connects but operations fail until cluster has a leader"
}
echo

# Demo 3: Show the complete workflow that would work with a running cluster
echo "=== Demo 3: Complete Workflow (Commands) ==="
echo
echo "Here's how you would use the client with a running cluster:"
echo

echo "1. Check cluster status:"
echo "   cargo run --bin qcore-client -- metrics"
echo

echo "2. Create entities:"
echo "   cargo run --bin qcore-client -- create User \"John Doe\""
echo "   cargo run --bin qcore-client -- create Post \"My First Post\" --parent \"User\$uuid\""
echo

echo "3. Write data:"
echo "   cargo run --bin qcore-client -- write \"User\$uuid\" \"age\" \"30\""
echo "   cargo run --bin qcore-client -- write \"User\$uuid\" \"email\" '\"john@example.com\"'"
echo "   cargo run --bin qcore-client -- write \"User\$uuid\" \"active\" \"true\""
echo

echo "4. Read data:"
echo "   cargo run --bin qcore-client -- read \"User\$uuid\" \"age\""
echo "   cargo run --bin qcore-client -- read \"User\$uuid\" \"email\""
echo

echo "5. Update with arithmetic:"
echo "   cargo run --bin qcore-client -- write \"User\$uuid\" \"score\" \"100\""
echo "   cargo run --bin qcore-client -- write \"User\$uuid\" \"score\" \"10\" --behavior add"
echo "   cargo run --bin qcore-client -- write \"User\$uuid\" \"score\" \"5\" --behavior subtract"
echo

echo "6. Get schema information:"
echo "   cargo run --bin qcore-client -- schema User"
echo "   cargo run --bin qcore-client -- schema User --complete"
echo

echo "7. Cleanup:"
echo "   cargo run --bin qcore-client -- delete \"Post\$uuid\""
echo "   cargo run --bin qcore-client -- delete \"User\$uuid\""
echo

# Demo 4: Show data type examples
echo "=== Demo 4: Supported Data Types ==="
echo
echo "The client supports the following value formats:"
echo
echo "• Strings:"
echo "  Simple: hello"
echo "  Quoted: \"hello world\""
echo
echo "• Numbers:"
echo "  Integer: 42"
echo "  Float: 3.14"
echo
echo "• Booleans:"
echo "  true"
echo "  false"
echo
echo "• Entity References:"
echo "  User\$123456789"
echo "  Post\$987654321"
echo
echo "• Entity Lists:"
echo "  '[\"User\$123\", \"User\$456\"]'"
echo

# Demo 5: Error handling examples
echo "=== Demo 5: Error Handling ==="
echo
echo "The client provides clear error messages for:"
echo "• Connection failures"
echo "• Invalid entity IDs"
echo "• Type mismatches"
echo "• Missing entities"
echo "• Cluster not ready (no leader)"
echo

echo "=== Client Features Summary ==="
echo
echo "✓ WebSocket connection to cluster"
echo "✓ Full CRUD operations (Create, Read, Update, Delete)"
echo "✓ Multiple data types support"
echo "✓ Arithmetic operations (add, subtract)"
echo "✓ Schema introspection"
echo "✓ Cluster metrics and status"
echo "✓ Parent-child entity relationships"
echo "✓ Comprehensive error handling"
echo "✓ Command-line interface with help"
echo

echo "=== Setup Instructions ==="
echo
echo "To use the client with a running cluster:"
echo
echo "1. Start a multi-node cluster:"
echo "   Terminal 1: ./target/release/qcore-rs --id 1 --ws-addr 127.0.0.1:8080 --config-file schemas.yaml --enable-discovery --min-nodes 2 --auto-init"
echo "   Terminal 2: ./target/release/qcore-rs --id 2 --ws-addr 127.0.0.1:8081 --config-file schemas.yaml --enable-discovery --min-nodes 2 --auto-init"
echo
echo "2. Wait for cluster initialization (look for 'Cluster initialized successfully' in logs)"
echo
echo "3. Use the client:"
echo "   cargo run --bin qcore-client -- metrics"
echo

echo "=== Demo Complete ==="

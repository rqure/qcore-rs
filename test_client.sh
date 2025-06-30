#!/bin/bash

# Demo script for qcore-client
# This script demonstrates various client operations

# Set the cluster address (defaults to localhost:8080)
CLUSTER_ADDRESS=${1:-"127.0.0.1:8080"}

echo "=== qcore-rs Client Demo ==="
echo "Cluster Address: $CLUSTER_ADDRESS"
echo

# Function to run client command with error handling
run_client() {
    echo "Running: cargo run --bin qcore-client -- --address $CLUSTER_ADDRESS $@"
    cargo run --bin qcore-client -- --address "$CLUSTER_ADDRESS" "$@"
    echo
}

echo "1. Get cluster metrics"
run_client metrics

echo "2. Create a new entity"
ENTITY_ID=$(cargo run --bin qcore-client -- --address "$CLUSTER_ADDRESS" create User --name "John Doe" 2>/dev/null | grep "Created entity:" | cut -d' ' -f3)
if [ -n "$ENTITY_ID" ]; then
    echo "Created entity: $ENTITY_ID"
    echo
    
    echo "3. Write some data to the entity"
    run_client write "$ENTITY_ID" "age" "30"
    run_client write "$ENTITY_ID" "email" '"john.doe@example.com"'
    run_client write "$ENTITY_ID" "active" "true"
    
    echo "4. Read the data back"
    run_client read "$ENTITY_ID" "age"
    run_client read "$ENTITY_ID" "email"
    run_client read "$ENTITY_ID" "active"
    
    echo "5. Update the age using add behavior"
    run_client write "$ENTITY_ID" "age" "5" --behavior add
    
    echo "6. Read the updated age"
    run_client read "$ENTITY_ID" "age"
    
    echo "7. Create another entity as a child"
    CHILD_ID=$(cargo run --bin qcore-client -- --address "$CLUSTER_ADDRESS" create Post --parent "$ENTITY_ID" --name "My First Post" 2>/dev/null | grep "Created entity:" | cut -d' ' -f3)
    if [ -n "$CHILD_ID" ]; then
        echo "Created child entity: $CHILD_ID"
        echo
        
        echo "8. Write data to the child entity"
        run_client write "$CHILD_ID" "title" '"Hello World!"'
        run_client write "$CHILD_ID" "content" '"This is my first post using qcore-rs!"'
        
        echo "9. Read child entity data"
        run_client read "$CHILD_ID" "title"
        run_client read "$CHILD_ID" "content"
        
        echo "10. Clean up - delete entities"
        run_client delete "$CHILD_ID"
    fi
    
    run_client delete "$ENTITY_ID"
else
    echo "Failed to create entity. Make sure the cluster is running."
fi

echo "=== Demo Complete ==="
echo
echo "Available commands:"
echo "  cargo run --bin qcore-client -- metrics"
echo "  cargo run --bin qcore-client -- create <entity_type> --name <name> [--parent <parent_id>]"
echo "  cargo run --bin qcore-client -- read <entity_id> <field>"
echo "  cargo run --bin qcore-client -- write <entity_id> <field> <value> [--behavior set|add|subtract]"
echo "  cargo run --bin qcore-client -- delete <entity_id>"
echo "  cargo run --bin qcore-client -- schema <entity_type> [--complete]"
echo
echo "Value formats:"
echo "  Strings: '\"hello world\"' or just 'hello' (simple strings)"
echo "  Numbers: 42 or 3.14"
echo "  Booleans: true or false"
echo "  Entity References: EntityType\$id"
echo "  Entity Lists: '[\"EntityType\$id1\", \"EntityType\$id2\"]'"

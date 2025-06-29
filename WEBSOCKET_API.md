# QCore-RS WebSocket Implementation

This document describes the WebSocket implementation that replaces the previous HTTP-based communication in qcore-rs.

## Overview

The qcore-rs server now uses WebSocket connections for all communication, including:
- Raft consensus protocol messages
- Management API calls  
- Application API requests

## Starting the Server

```bash
cargo run -- --id 1 --ws-addr 127.0.0.1:8080 --config-file schemas.yaml
```

## WebSocket Message Format

All messages are JSON-encoded and follow this general structure:

```json
{
  "MessageType": {
    "id": "unique-request-id",
    "field1": "value1",
    "field2": "value2"
  }
}
```

## Message Types

### Management API

#### Initialize Cluster
```json
{
  "Init": {
    "id": "req-123",
    "nodes": []  // Empty for single-node cluster, or [(node_id, "address"), ...]
  }
}
```

Response:
```json
{
  "InitResponse": {
    "id": "req-123", 
    "response": "Ok" // or {"Err": "error message"}
  }
}
```

#### Add Learner
```json
{
  "AddLearner": {
    "id": "req-124",
    "node_id": 2,
    "node_addr": "127.0.0.1:8081"
  }
}
```

#### Change Membership
```json
{
  "ChangeMembership": {
    "id": "req-125",
    "members": [1, 2, 3]  // Set of node IDs
  }
}
```

#### Get Metrics
```json
{
  "GetMetrics": {
    "id": "req-126"
  }
}
```

### Application API

#### Perform Operation
```json
{
  "Perform": {
    "id": "req-127",
    "request": {
      "UpdateEntity": {
        "request": [
          {
            "Read": {
              "entity_id": {"typ": "User", "id": 123},
              "field_type": "Name",
              "value": null,
              "write_time": null,
              "writer_id": null
            }
          }
        ]
      }
    }
  }
}
```

### Raft Internal Messages

The system automatically handles Raft consensus messages between nodes:
- `RaftVote` / `RaftVoteResponse`
- `RaftAppend` / `RaftAppendResponse` 
- `RaftSnapshot` / `RaftSnapshotResponse`

## Example Client

A simple WebSocket client example is provided in `examples/websocket_client.rs`:

```bash
cargo run --example websocket_client
```

## Advantages of WebSocket

1. **Persistent Connections**: Eliminates connection overhead for frequent operations
2. **Bidirectional Communication**: Server can push updates to clients
3. **Lower Latency**: No HTTP header overhead
4. **Real-time Updates**: Clients can receive notifications in real-time
5. **Better Resource Usage**: Single connection per client instead of per request

## Error Handling

All WebSocket messages can return error responses:

```json
{
  "Error": {
    "id": "req-123",
    "error": "Description of what went wrong"
  }
}
```

## Dependencies

The WebSocket implementation uses:
- `tokio-tungstenite` for WebSocket protocol
- `futures-util` for async stream handling  
- `uuid` for generating unique request IDs
- `serde_json` for message serialization

## Testing

You can test the WebSocket server using any WebSocket client tool or the provided example client.

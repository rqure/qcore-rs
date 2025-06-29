# Persistent WebSocket Connections Implementation

## Overview

The network layer has been updated to maintain persistent WebSocket connections between Raft nodes instead of creating new connections for each RPC call. This provides significant performance benefits and reduces connection overhead.

## Key Components

### 1. PersistentConnection Struct

```rust
struct PersistentConnection {
    sender: Arc<Mutex<futures_util::stream::SplitSink<WsStream, tokio_tungstenite::tungstenite::Message>>>,
    pending_requests: Arc<Mutex<HashMap<String, oneshot::Sender<serde_json::Value>>>>,
}
```

- **sender**: Shared WebSocket sender for outgoing messages
- **pending_requests**: Tracks pending RPC requests awaiting responses

### 2. Connection Management

The `Network` struct maintains a connection pool:

```rust
pub struct Network {
    connections: Arc<RwLock<HashMap<NodeId, Arc<PersistentConnection>>>>,
}
```

#### Connection Lifecycle

1. **Creation**: Connections are created on-demand when first needed
2. **Reuse**: Subsequent requests to the same node reuse existing connections
3. **Cleanup**: Failed connections are automatically removed and recreated as needed

### 3. Async Message Handling

Each persistent connection spawns a background task that:
- Listens for incoming WebSocket messages
- Matches responses to pending requests using unique request IDs
- Delivers responses through oneshot channels
- Handles connection errors and cleanup

## Features

### Connection Pooling
- Maintains one persistent connection per target node
- Automatically creates connections when needed
- Removes and recreates failed connections

### Request-Response Matching
- Each RPC request gets a unique UUID
- Background task matches responses to pending requests
- 30-second timeout for request completion

### Error Handling
- Connection failures are properly categorized (Unreachable vs Network errors)
- Failed connections are removed from the pool
- Automatic retry with new connections

### Concurrency Safety
- All shared state is protected with appropriate locks (RwLock/Mutex)
- Multiple concurrent requests can use the same connection
- Background message handling doesn't block request sending

## Benefits

### Performance Improvements
1. **Eliminated Connection Overhead**: No TCP handshake + WebSocket upgrade for each request
2. **Reduced Latency**: Persistent connections mean faster message delivery
3. **Better Resource Usage**: Fewer file descriptors and network sockets

### Scalability Benefits
1. **Connection Reuse**: One connection per node pair instead of per request
2. **Concurrent Requests**: Multiple requests can share the same connection
3. **Efficient Multiplexing**: Request/response matching allows concurrent operations

### Reliability Features
1. **Automatic Recovery**: Failed connections are recreated automatically
2. **Timeout Protection**: Requests timeout after 30 seconds
3. **Error Propagation**: Connection issues are properly reported to Raft

## Usage

The persistent connection implementation is transparent to the Raft layer. The same `send_rpc` interface is maintained:

```rust
// This now uses persistent connections automatically
let response = network.send_rpc(target_id, target_node, "raft/vote", vote_request).await?;
```

## Configuration

Current configuration:
- **Request Timeout**: 30 seconds
- **Connection Pool**: Unlimited size (limited by number of nodes)
- **Retry Strategy**: Remove and recreate failed connections

## Monitoring

The implementation includes logging for:
- Connection creation and failures
- WebSocket message errors
- Request timeouts

## Future Enhancements

Potential improvements:
1. **Connection Health Checks**: Periodic ping/pong to verify connection health
2. **Configurable Timeouts**: Make request timeout configurable
3. **Connection Limits**: Optional maximum connections per node
4. **Metrics**: Expose connection pool statistics
5. **Graceful Shutdown**: Properly close connections on shutdown

## Comparison with Previous Implementation

| Aspect | Previous (HTTP-like) | New (Persistent) |
|--------|---------------------|------------------|
| Connection Model | New connection per request | Persistent connections |
| Latency | High (connection setup overhead) | Low (reuse existing connections) |
| Resource Usage | High (many short-lived connections) | Low (few long-lived connections) |
| Concurrency | Limited by connection pool | High (shared connections) |
| Error Recovery | Retry each request | Pool-level connection management |
| Implementation | Synchronous request/response | Async with background message handling |

This implementation provides the foundation for a high-performance, scalable Raft cluster communication layer.

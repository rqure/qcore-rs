# PeerService Implementation

This document describes the PeerService implementation that was extracted from the main.rs file to better organize peer connection management, leader election, and data synchronization.

## Overview

The PeerService is responsible for:

1. **Peer Connection Management**
   - Managing inbound peer connections (other nodes connecting to us)
   - Managing outbound peer connections (us connecting to other nodes)
   - Maintaining connection state and handling reconnections

2. **Leader Election**
   - Processing startup messages from peers
   - Determining leadership based on startup time and machine ID
   - Handling leader transitions and stepping down

3. **Data Synchronization**
   - Handling full sync requests and responses
   - Processing incremental sync requests
   - Managing availability state during sync operations

## Key Components

### PeerService Struct
```rust
#[derive(Clone)]
pub struct PeerService {
    app_state: Arc<AppState>,
}
```

### Main Methods

- `new(app_state: Arc<AppState>)` - Creates a new PeerService instance
- `start_inbound_server()` - Starts the WebSocket server for incoming peer connections
- `manage_outbound_connections()` - Manages connections to configured peer addresses
- `handle_peer_message()` - Processes different types of peer messages

### Message Types

The service handles four types of peer messages:

1. **Startup** - Announces machine ID and startup time for leader election
2. **FullSyncRequest** - Requests a complete snapshot from a peer
3. **FullSyncResponse** - Provides a complete snapshot to requesting peer
4. **SyncRequest** - Sends incremental changes to peers

## Integration

The PeerService is integrated into the main application by:

1. Creating a PeerService instance with the shared AppState
2. Spawning tasks for inbound server and outbound connection management
3. Using the service's methods instead of the old standalone functions

## Benefits

This refactoring provides:

- **Better code organization** - Peer-related functionality is now encapsulated
- **Easier testing** - The service can be tested independently
- **Improved maintainability** - Changes to peer logic are isolated
- **Cleaner main.rs** - The main function is more focused on orchestration

## Previous Implementation

The previous implementation had peer functionality scattered across multiple functions in main.rs:
- `handle_inbound_peer_connection`
- `handle_peer_message`
- `handle_outbound_peer_connection`
- `start_inbound_peer_server`
- `manage_outbound_peer_connections`

All of these have been consolidated into the PeerService structure.
mod persistance;
mod states;
mod clients;
mod peers;
mod misc;
mod store;

use qlib_rs::{et, ft, notification_channel, now, schoice, sread, sref, swrite, AuthConfig, AuthenticationResult, Cache, PushCondition, StoreMessage, StoreTrait, Snowflake, CelExecutor};
use qlib_rs::auth::{AuthorizationScope, get_scope};
use tokio::signal;
use tokio::net::{TcpListener, TcpStream};
use tokio_tungstenite::{accept_async, connect_async, tungstenite::Message};
use futures_util::{SinkExt, StreamExt};
use tokio::sync::{mpsc, Mutex};
use tracing::{info, warn, error, debug, instrument};
use anyhow::Result;
use std::collections::{HashSet};
use std::sync::Arc;
use std::vec;
use std::time::Duration;
use time;
use clap::Parser;

use crate::persistance::{SnapshotService, WalService, SnapshotTrait, WalTrait, WalConfig, SnapshotConfig};
use crate::states::{AppState, AppStateLocks, AvailabilityState, Config, LockRequest, PeerInfo, PeerMessage};
use crate::store::{StoreService, StoreHandle};

/// Consume and process requests from the store's write channel
async fn consume_write_channel(app_state: Arc<AppState>) -> Result<()> {
    info!("Starting write channel consumer");
    
    // Get a clone of the write channel receiver
    let receiver = {
        let mut locks = app_state.acquire_locks(LockRequest {
            store: true,
            ..Default::default()
        }).await;
        locks.store().inner().get_write_channel_receiver()
    };
    
    loop {
        // Wait for a batch of requests from the write channel without holding any store locks
        let requests = {
            let mut receiver_guard = receiver.lock().await;
            receiver_guard.recv().await
        };
        
        match requests {
            Some(mut requests) => {     
                let mut locks = app_state.acquire_locks(LockRequest {
                    core_state: true,
                    store: true,
                    connections: true,
                    ..Default::default()
                }).await;

                // Ensure the originator is set for all requests
                let current_machine = locks.core_state().config.machine.clone();
                requests.iter_mut().for_each(|req| req.try_set_originator(current_machine.clone()));
                
                // Write all requests to the WAL file - these requests have already been applied to the store
                for request in &requests {
                    if let Err(e) = write_request_to_wal(request, &mut locks, false).await {
                        error!(
                            error = %e,
                            "Failed to write request to WAL"
                        );
                    }
                }
                
                // Send batch of requests to peers for synchronization if we have any
                let requests_to_sync: Vec<qlib_rs::Request> = requests.iter()
                    .filter(|request| {
                        if let Some(originator) = request.originator() {
                            originator == &current_machine
                        } else {
                            false
                        }
                    })
                    .cloned()
                    .collect();
                
                if !requests_to_sync.is_empty() {                    
                    // Send to all connected outbound peers using PeerMessage
                    let peers_to_notify = locks.connections().connected_outbound_peers.clone();

                    // Create a batch sync message
                    let sync_message = PeerMessage::SyncRequest {
                        requests: requests_to_sync.clone(),
                    };
                    
                    // Serialize the sync message to JSON for transmission
                    match serde_json::to_string(&sync_message) {
                        Ok(message_json) => {
                            let message = Message::Text(message_json);
                            
                            for (peer_addr, sender) in &peers_to_notify {
                                if let Err(e) = sender.send(message.clone()) {
                                    warn!(
                                        peer_addr = %peer_addr,
                                        error = %e,
                                        "Failed to send sync requests to peer"
                                    );
                                } else {
                                    debug!(
                                        peer_addr = %peer_addr,
                                        count = requests_to_sync.len(),
                                        "Sent sync requests to peer"
                                    );
                                }
                            }
                        }
                        Err(e) => {
                            error!(
                                error = %e,
                                "Failed to serialize sync message for peer synchronization"
                            );
                        }
                    }
                }
            }
            None => {
                warn!("Write channel closed, stopping consumer");
                return Ok(());
            }
        }
    }
}

/// Handle miscellaneous periodic tasks that run every 10ms
async fn handle_misc_tasks(app_state: Arc<AppState>) -> Result<()> {
    info!("Starting miscellaneous tasks handler (10ms interval)");
    
    let mut interval = tokio::time::interval(Duration::from_millis(10));
    
    loop {
        interval.tick().await;

        // Process cache notifications
        {
            let permission_cache = locks.permission_cache();
            if let Some(cache) = permission_cache.as_mut() {
                cache.process_notifications();
            }
        }

        if is_leader {
            let (store, core) = locks.store.as_mut().zip(locks.core_state.as_ref()).unwrap();

            // Find us as a candidate
            let me_as_candidate = {
                let machine = &core.config.machine;

                let mut candidates = store.find_entities(
                    &et::candidate(), 
                    Some(format!("Name == 'qcore' && Parent->Name == '{}'", machine))).await?;

                candidates.pop()
            };

            // Update available list and current leader
            {
                let fault_tolerances = store.find_entities(&et::fault_tolerance(), None).await?;
                for ft_entity_id in fault_tolerances {
                    let ft_fields = store.perform_map(&mut vec![
                        sread!(ft_entity_id.clone(), ft::candidate_list()),
                        sread!(ft_entity_id.clone(), ft::available_list()),
                        sread!(ft_entity_id.clone(), ft::current_leader())
                    ]).await?;

                    let candidates = ft_fields
                        .get(&ft::candidate_list())
                        .unwrap()
                        .value()
                        .unwrap()
                        .expect_entity_list()?;

                    let mut available = Vec::new();
                    for candidate_id in candidates.iter() {
                        let candidate_fields = store.perform_map(&mut vec![
                            sread!(candidate_id.clone(), ft::make_me()),
                            sread!(candidate_id.clone(), ft::heartbeat()),
                            sread!(candidate_id.clone(), ft::death_detection_timeout()),
                        ]).await?;

                        let heartbeat_time = candidate_fields
                            .get(&ft::heartbeat())
                            .unwrap()
                            .write_time()
                            .unwrap();

                        let make_me = candidate_fields
                            .get(&ft::make_me())
                            .unwrap()
                            .value()
                            .unwrap()
                            .expect_choice()?;

                        let death_detection_timeout_millis = candidate_fields
                            .get(&ft::death_detection_timeout())
                            .unwrap()
                            .value()
                            .unwrap()
                            .expect_int()?;
                        
                        let death_detection_timeout_duration = time::Duration::milliseconds(death_detection_timeout_millis);

                        let desired_availability = match make_me {
                            1 => AvailabilityState::Available,
                            _ => AvailabilityState::Unavailable,
                        };

                        if desired_availability == AvailabilityState::Available && 
                           heartbeat_time + death_detection_timeout_duration > now() {
                            available.push(candidate_id.clone());
                        }
                    }

                    store.perform_mut(&mut vec![
                        swrite!(ft_entity_id.clone(), ft::available_list(), Some(qlib_rs::Value::EntityList(available.clone())), PushCondition::Changes),
                    ]).await?;

                    let mut handle_me_as_candidate = false;
                    if let Some(me_as_candidate) = &me_as_candidate {
                        // If we're not in the candidate list, we can't be leader
                        if candidates.contains(me_as_candidate) {
                            handle_me_as_candidate = true;

                            store.perform_mut(&mut vec![
                                swrite!(ft_entity_id.clone(), ft::current_leader(), sref!(Some(me_as_candidate.clone())), PushCondition::Changes)
                            ]).await?;
                        }
                    }

                    if !handle_me_as_candidate {
                        // Now we must promote an available candidate to leader
                        // if the current leader is no longer available.
                        // Note that we want to promote to the next available leader in the candidate list
                        // rather than the first available candidate.
                        let current_leader = ft_fields
                            .get(&ft::current_leader())
                            .unwrap()
                            .value()
                            .unwrap()
                            .expect_entity_reference()?;

                        if current_leader.is_none() {
                            store.perform_mut(&mut vec![
                                swrite!(ft_entity_id.clone(), ft::current_leader(), sref!(available.first().cloned()), PushCondition::Changes),
                            ]).await?;
                        }
                        else if let Some(current_leader) = current_leader {
                            if !available.contains(&current_leader) {
                                // Find the position of the current leader in the candidate list
                                let current_leader_idx = candidates.iter().position(|c| c.clone() == current_leader.clone());
                                
                                if let Some(current_idx) = current_leader_idx {
                                    // Find the next available candidate after the current leader in the candidate list
                                    let mut next_leader = None;
                                    
                                    // Start searching from the position after the current leader
                                    for i in (current_idx + 1)..candidates.len() {
                                        if available.contains(&candidates[i]) {
                                            next_leader = Some(candidates[i].clone());
                                            break;
                                        }
                                    }
                                    
                                    // If no leader found after current position, wrap around to the beginning
                                    if next_leader.is_none() {
                                        for i in 0..=current_idx {
                                            if available.contains(&candidates[i]) {
                                                next_leader = Some(candidates[i].clone());
                                                break;
                                            }
                                        }
                                    }
                                    
                                    store.perform_mut(&mut vec![
                                        swrite!(ft_entity_id.clone(), ft::current_leader(), sref!(next_leader), PushCondition::Changes),
                                    ]).await?;
                                } else {
                                    // Current leader not found in candidates list, just pick the first available
                                    store.perform_mut(&mut vec![
                                        swrite!(ft_entity_id.clone(), ft::current_leader(), sref!(available.first().cloned()), PushCondition::Changes),
                                    ]).await?;
                                }
                            }
                        }
                    }
                }
            }
        }

        tokio::task::yield_now().await;
    }
}

/// Handle heartbeat writing
async fn handle_heartbeat_writing(app_state: Arc<AppState>) -> Result<()> {
    info!("Starting heartbeat writer");
    
    let mut interval = tokio::time::interval(Duration::from_secs(1));
    
    loop {
        interval.tick().await;

        let mut locks = app_state.acquire_locks(LockRequest {
            store: true,
            core_state: true,
            ..Default::default()
        }).await;

        let (store, core) = locks.store.as_mut().zip(locks.core_state.as_ref()).unwrap();
        let machine = &core.config.machine;

        let candidates = store.find_entities(
            &et::candidate(), 
            Some(format!("Name == 'qcore' && Parent->Name == '{}'", machine))).await?;

        if let Some(candidate) = candidates.first() {
            store.perform_mut(&mut vec![
                swrite!(candidate.clone(), ft::heartbeat(), schoice!(0)),
                swrite!(candidate.clone(), ft::make_me(), schoice!(1), PushCondition::Changes)
            ]).await?;
        }

        tokio::task::yield_now().await;
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let config = Config::parse();

    // Initialize tracing with better structured logging
    tracing_subscriber::fmt()
        .with_env_filter(
            std::env::var("RUST_LOG")
                .unwrap_or_else(|_| "qcore_rs=debug,tokio=warn,tokio_tungstenite=warn".to_string())
        )
        .with_target(true)
        .with_thread_ids(true)
        .with_file(cfg!(debug_assertions))
        .with_line_number(cfg!(debug_assertions))
        .init();

    let machine_id = &config.machine;
    let peer_port = config.peer_port;
    let client_port = config.client_port;
    
    info!(
        machine_id = %machine_id,
        peer_port = peer_port,
        client_port = client_port,
        data_dir = %config.data_dir,
        "Starting QCore service"
    );

    // Create the store handle
    let store_handle = StoreService::spawn();

    // Create shared application state
    let app_state = Arc::new(AppState::new(config, store_handle)?);

    // Initialize the WAL file counter based on existing files
    {
        let mut locks = app_state.acquire_locks(LockRequest {
            core_state: true,
            ..Default::default()
        }).await;
        let wal_config = WalConfig {
            wal_dir: locks.core_state().get_wal_dir(),
            max_file_size: locks.core_state().config.wal_max_file_size * 1024 * 1024,
            max_files: locks.core_state().config.wal_max_files,
            snapshot_wal_interval: locks.core_state().config.snapshot_wal_interval,
            machine_id: locks.core_state().config.machine.clone(),
        };
        let wal_handle = WalService::spawn(wal_config, snapshot_handle, store_handle);
    }

    // Load the latest snapshot if available
    {
        let mut locks = app_state.acquire_locks(LockRequest {
            core_state: true,
            ..Default::default()
        }).await;

        let snapshot_config = SnapshotConfig {
            snapshots_dir: locks.core_state().get_snapshots_dir(),
            max_files: locks.core_state().config.snapshot_max_files,
        };
        let mut snapshot_manager = SnapshotService::new_default(snapshot_config);
        if let Some((snapshot, snapshot_counter)) = snapshot_manager.load_latest(&mut locks).await? {
            info!(
                snapshot_counter = snapshot_counter,
                "Restoring store from snapshot"
            );
            
            // Initialize the snapshot file counter to continue from the next number
            snapshot_manager.initialize_counter(&mut locks).await?;
            
            {
                let store_guard = locks.store();
                store_guard.inner_mut().disable_notifications();
                store_guard.inner_mut().restore_snapshot(snapshot);
                store_guard.inner_mut().enable_notifications();
            }
        } else {
            info!("No snapshot found, starting with empty store");
            
            // Initialize the snapshot file counter
            snapshot_manager.initialize_counter(&mut locks).await?;
        }
    }

    // Start the write channel consumer task
    let app_state_clone = Arc::clone(&app_state);
    let mut write_channel_task = tokio::spawn(async move {
        if let Err(e) = consume_write_channel(app_state_clone).await {
            error!(
                error = %e,
                "Write channel consumer failed"
            );
        }
    });

    // Start the peer WebSocket server task
    let app_state_clone = Arc::clone(&app_state);
    let mut peer_server_task = tokio::spawn(async move {
        if let Err(e) = start_inbound_peer_server(app_state_clone).await {
            error!(
                error = %e,
                "Peer server failed"
            );
        }
    });

    // Start the client WebSocket server task
    let app_state_clone = Arc::clone(&app_state);
    let mut client_server_task = tokio::spawn(async move {
        if let Err(e) = start_client_server(app_state_clone).await {
            error!(
                error = %e,
                "Client server failed"
            );
        }
    });

    // Start the outbound peer connection manager task
    let app_state_clone = Arc::clone(&app_state);
    let mut outbound_peer_task = tokio::spawn(async move {
        if let Err(e) = manage_outbound_peer_connections(app_state_clone).await {
            error!(
                error = %e,
                "Outbound peer connection manager failed"
            );
        }
    });

    // Start the misc tasks handler
    let app_state_clone = Arc::clone(&app_state);
    let mut misc_task = tokio::spawn(async move {
        if let Err(e) = handle_misc_tasks(app_state_clone).await {
            error!(
                error = %e,
                "Misc tasks handler failed"
            );
        }
    });

    // Start the heartbeat writer
    let app_state_clone = Arc::clone(&app_state);
    let mut heartbeat_task = tokio::spawn(async move {
        if let Err(e) = handle_heartbeat_writing(app_state_clone).await {
            error!(
                error = %e,
                "Heartbeat writer failed"
            );
        }
    });

    // Wait for either shutdown signal or any critical task to complete/fail
    tokio::select! {
        _ = signal::ctrl_c() => {
            warn!("Received shutdown signal, initiating graceful shutdown");
        }
        result = &mut write_channel_task => {
            match result {
                Ok(_) => error!("Write channel task exited unexpectedly"),
                Err(e) => error!(error = %e, "Write channel task failed"),
            }
            warn!("Critical task failure detected, initiating shutdown");
        }
        result = &mut peer_server_task => {
            match result {
                Ok(_) => error!("Peer server task exited unexpectedly"),
                Err(e) => error!(error = %e, "Peer server task failed"),
            }
            warn!("Critical task failure detected, initiating shutdown");
        }
        result = &mut client_server_task => {
            match result {
                Ok(_) => error!("Client server task exited unexpectedly"),
                Err(e) => error!(error = %e, "Client server task failed"),
            }
            warn!("Critical task failure detected, initiating shutdown");
        }
        result = &mut outbound_peer_task => {
            match result {
                Ok(_) => error!("Outbound peer task exited unexpectedly"),
                Err(e) => error!(error = %e, "Outbound peer task failed"),
            }
            warn!("Critical task failure detected, initiating shutdown");
        }
        result = &mut misc_task => {
            match result {
                Ok(_) => error!("Misc task exited unexpectedly"),
                Err(e) => error!(error = %e, "Misc task failed"),
            }
            warn!("Critical task failure detected, initiating shutdown");
        }
        result = &mut heartbeat_task => {
            match result {
                Ok(_) => error!("Heartbeat task exited unexpectedly"),
                Err(e) => error!(error = %e, "Heartbeat task failed"),
            }
            warn!("Critical task failure detected, initiating shutdown");
        }
    }

    let mut locks = app_state.acquire_locks(LockRequest {
        core_state: true,
        store: true,
        ..Default::default()
    }).await;

    // Take a final snapshot before shutting down
    info!("Taking final snapshot before shutdown");
    let snapshot = locks.store().inner().take_snapshot();
    
    match save_snapshot(&snapshot, &mut locks).await {
        Ok(snapshot_counter) => {
            info!(
                snapshot_counter = snapshot_counter,
                "Final snapshot saved successfully"
            );
            
            // Write a snapshot marker to the WAL to indicate the final snapshot point
            // This helps during replay to know that the state was snapshotted at shutdown
            let snapshot_request = qlib_rs::Request::Snapshot {
                snapshot_counter,
                timestamp: Some(now()),
                originator: Some({
                    let core = locks.core_state();
                    core.config.machine.clone()
                }),
            };
            
            if let Err(e) = write_request_to_wal(&snapshot_request, &mut locks, true).await {
                error!(error = %e, "Failed to write final snapshot marker to WAL");
            } else {
                info!("Final snapshot marker written to WAL");
            }
        }
        Err(e) => {
            error!(error = %e, "Failed to save final snapshot");
        }
    }

    // Abort all tasks
    info!("Stopping all background tasks");
    write_channel_task.abort();
    peer_server_task.abort();
    client_server_task.abort();
    outbound_peer_task.abort();
    misc_task.abort();
    heartbeat_task.abort();

    info!("QCore service shutdown complete");
    Ok(())
}

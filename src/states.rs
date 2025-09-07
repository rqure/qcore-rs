use qlib_rs::{Cache, CelExecutor, EntityId, NotificationSender, NotifyConfig};
use tokio_tungstenite::{tungstenite::Message};
use tokio::sync::{mpsc, Mutex, MutexGuard};
use tracing::{info, warn};
use clap::Parser;
use anyhow::Result;
use std::collections::{HashSet, HashMap};
use std::path::PathBuf;
use serde::{Serialize, Deserialize};
use time;
use crate::store::StoreHandle;

/// Application availability state
#[derive(Debug, Clone, PartialEq)]
pub enum AvailabilityState {
    /// Application is unavailable - attempting to sync with leader, clients are force disconnected
    Unavailable,
    /// Application is available - clients are allowed to connect and perform operations
    Available,
}

/// Helper structure for extracting data without holding locks
#[derive(Debug, Clone)]
pub struct StateSnapshot {
    pub config: Config,
    pub startup_time: u64,
    pub availability_state: AvailabilityState,
}

impl StateSnapshot {
    pub fn is_available(&self) -> bool {
        matches!(self.availability_state, AvailabilityState::Available)
    }
}

use crossbeam::channel::{unbounded, Receiver, Sender};
use std::thread;
use std::time::{Duration, Instant};
use tracing::{debug, warn};
use qlib_rs::Value;

use crate::store::StoreHandle;

/// Configuration for heartbeat service
#[derive(Debug, Clone)]
pub struct HeartbeatConfig {
    /// Machine identifier for this instance
    pub machine: String,
    /// Interval between heartbeat writes (in seconds)
    pub interval_secs: u64,
}

/// Heartbeat service commands
#[derive(Debug)]
pub enum HeartbeatCommand {
}

/// Handle for communicating with heartbeat service
#[derive(Debug, Clone)]
pub struct HeartbeatHandle {
    _sender: Sender<HeartbeatCommand>,
}

impl HeartbeatHandle {
}

/// Service that periodically writes heartbeat information to the store
pub struct HeartbeatService {
    config: HeartbeatConfig,
    store_handle: StoreHandle,
}

impl HeartbeatService {
    pub fn new(config: HeartbeatConfig, store_handle: StoreHandle) -> Self {
        Self {
            config,
            store_handle,
        }
    }

    /// Spawn the heartbeat service in a background thread
    pub fn spawn(config: HeartbeatConfig, store_handle: StoreHandle) -> HeartbeatHandle {
        let (sender, receiver) = unbounded();
        let mut service = Self::new(config, store_handle);

        thread::spawn(move || {
            service.run(receiver);
        });

        HeartbeatHandle { _sender: sender }
    }

    fn run(&mut self, _receiver: Receiver<HeartbeatCommand>) {
        let mut last_tick = Instant::now();
        let interval = Duration::from_secs(self.config.interval_secs);

        loop {
            let now = Instant::now();
            if now.duration_since(last_tick) >= interval {
                last_tick = now;
                self.write_heartbeat();
            }

            // Sleep briefly to avoid busy-waiting
            thread::sleep(Duration::from_millis(100));
        }
    }

    fn write_heartbeat(&mut self) {
        // Get required entity and field types
        let et_candidate = {
            if let Some(et) = self.store_handle.get_et().as_ref() {
                if let Some(candidate) = et.candidate {
                    candidate
                } else {
                    warn!("Entity type 'Candidate' not found in store");
                    return;
                }
            } else {
                warn!("Entity type 'Candidate' not found in store");
                return;
            }
        };

        let ft_heartbeat = {
            if let Some(ft) = self.store_handle.get_ft().as_ref() {
                if let Some(field) = ft.heartbeat {
                    field
                } else {
                    warn!("Field type 'Heartbeat' not found in store");
                    return;
                }
            } else {
                warn!("Field type 'Heartbeat' not found in store");
                return;
            }
        };

        let ft_make_me = {
            if let Some(ft) = self.store_handle.get_ft().as_ref() {
                if let Some(field) = ft.make_me {
                    field
                } else {
                    warn!("Field type 'MakeMe' not found in store");
                    return;
                }
            } else {
                warn!("Field type 'MakeMe' not found in store");
                return;
            }
        };

        let machine = &self.config.machine;

        // Find this machine's candidate entity
        let candidates = match self.store_handle.find_entities(
            et_candidate,
            Some(format!("Name == 'qcore' && Parent->Name == '{}'", machine)),
        ) {
            Ok(ents) => ents,
            Err(e) => {
                warn!("Failed to query candidate entity for heartbeat: {}", e);
                return;
            }
        };

        if let Some(candidate) = candidates.entities.first() {
            debug!("Writing heartbeat fields for candidate {:?}", candidate);
            
            // Update heartbeat timestamp
            if let Err(e) = self.store_handle.write(
                *candidate,
                vec![ft_heartbeat],
                Value::Choice(0),
                None,
                None,
                None,
                None,
            ) {
                warn!("Failed to write heartbeat field: {}", e);
            }
            
            // Update make_me field
            if let Err(e) = self.store_handle.write(
                *candidate,
                vec![ft_make_me],
                Value::Choice(1),
                None,
                None,
                Some(qlib_rs::PushCondition::Changes),
                None,
            ) {
                warn!("Failed to write make_me field: {}", e);
            }
        }
    }
}

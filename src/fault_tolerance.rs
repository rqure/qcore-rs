use crossbeam::channel::{bounded, Receiver, Sender};
use std::thread;
use std::time::{Duration, Instant};
use tracing::{debug, info, warn};
use qlib_rs::Value;

use crate::store_service::StoreHandle;

/// Configuration for fault tolerance service
#[derive(Debug, Clone)]
pub struct FaultToleranceConfig {
    /// Machine identifier for this instance
    pub machine: String,
    /// Whether this instance can be a leader
    pub is_leader: bool,
    /// Interval between fault tolerance checks (in milliseconds)
    pub interval_millis: u64,
}

/// Fault tolerance service commands
#[derive(Debug)]
pub enum FaultToleranceCommand {
    Stop,
    SetLeader { is_leader: bool },
}

/// Handle for communicating with fault tolerance service
#[derive(Debug, Clone)]
pub struct FaultToleranceHandle {
    sender: Sender<FaultToleranceCommand>,
}

impl FaultToleranceHandle {
    pub fn stop(&self) {
        let _ = self.sender.send(FaultToleranceCommand::Stop);
    }

    pub fn set_leader(&self, is_leader: bool) {
        let _ = self.sender.send(FaultToleranceCommand::SetLeader { is_leader });
    }
}

/// Service that manages fault tolerance and leader election
pub struct FaultToleranceService {
    config: FaultToleranceConfig,
    store_handle: StoreHandle,
    is_leader: bool,
}

impl FaultToleranceService {
    pub fn new(config: FaultToleranceConfig, store_handle: StoreHandle) -> Self {
        let is_leader = config.is_leader;
        Self {
            config,
            store_handle,
            is_leader,
        }
    }

    /// Spawn the fault tolerance service in a background thread
    pub fn spawn(config: FaultToleranceConfig, store_handle: StoreHandle) -> FaultToleranceHandle {
        let (sender, receiver) = bounded(100);
        let mut service = Self::new(config, store_handle);

        thread::spawn(move || {
            service.run(receiver);
        });

        FaultToleranceHandle { sender }
    }

    fn run(&mut self, receiver: Receiver<FaultToleranceCommand>) {
        let mut last_tick = Instant::now();
        let interval = Duration::from_millis(self.config.interval_millis);

        loop {
            // Check for commands
            match receiver.try_recv() {
                Ok(FaultToleranceCommand::Stop) => {
                    debug!("Fault tolerance service stopping");
                    break;
                }
                Ok(FaultToleranceCommand::SetLeader { is_leader }) => {
                    debug!("Fault tolerance service leader status changed: {}", is_leader);
                    self.is_leader = is_leader;
                }
                Err(_) => {}
            }

            let now = Instant::now();
            if now.duration_since(last_tick) >= interval {
                last_tick = now;
                self.manage_fault_tolerance();
            }

            // Sleep briefly to avoid busy-waiting
            thread::sleep(Duration::from_millis(10));
        }
    }

    fn manage_fault_tolerance(&mut self) {
        if !self.is_leader {
            return;
        }

        // Get required entity and field types
        let (
            et_candidate,
            et_fault_tolerance,
            ft_candidate_list,
            ft_available_list,
            ft_current_leader,
            ft_make_me,
            ft_heartbeat,
            ft_death_detection_timeout,
        ) = {
            let et_opt = self.store_handle.get_et();
            let et = match et_opt.as_ref() {
                Some(et) => et,
                None => {
                    warn!("Entity types not available");
                    return;
                }
            };

            let ft_opt = self.store_handle.get_ft();
            let ft = match ft_opt.as_ref() {
                Some(ft) => ft,
                None => {
                    warn!("Field types not available");
                    return;
                }
            };

            let et_candidate = match et.candidate {
                Some(candidate) => candidate,
                None => {
                    warn!("Entity type 'Candidate' not found");
                    return;
                }
            };

            let et_fault_tolerance = match et.fault_tolerance {
                Some(ft) => ft,
                None => {
                    warn!("Entity type 'FaultTolerance' not found");
                    return;
                }
            };

            let ft_candidate_list = match ft.candidate_list {
                Some(field) => field,
                None => {
                    warn!("Field type 'CandidateList' not found");
                    return;
                }
            };

            let ft_available_list = match ft.available_list {
                Some(field) => field,
                None => {
                    warn!("Field type 'AvailableList' not found");
                    return;
                }
            };

            let ft_current_leader = match ft.current_leader {
                Some(field) => field,
                None => {
                    warn!("Field type 'CurrentLeader' not found");
                    return;
                }
            };

            let ft_make_me = match ft.make_me {
                Some(field) => field,
                None => {
                    warn!("Field type 'MakeMe' not found");
                    return;
                }
            };

            let ft_heartbeat = match ft.heartbeat {
                Some(field) => field,
                None => {
                    warn!("Field type 'Heartbeat' not found");
                    return;
                }
            };

            let ft_death_detection_timeout = match ft.death_detection_timeout {
                Some(field) => field,
                None => {
                    warn!("Field type 'DeathDetectionTimeout' not found");
                    return;
                }
            };

            (
                et_candidate,
                et_fault_tolerance,
                ft_candidate_list,
                ft_available_list,
                ft_current_leader,
                ft_make_me,
                ft_heartbeat,
                ft_death_detection_timeout,
            )
        };

        let machine = &self.config.machine;

        // Find this machine's candidate entity
        let me_as_candidate = {
            let candidates_result = self.store_handle.find_entities(
                et_candidate,
                Some(format!("Name == 'qcore' && Parent->Name == '{}'", machine)),
            );

            match candidates_result {
                Ok(mut candidates) => candidates.entities.pop(),
                Err(e) => {
                    warn!("Failed to find candidate entity: {}", e);
                    return;
                }
            }
        };

        // Get all fault tolerance entities
        let fault_tolerances = match self.store_handle.find_entities(et_fault_tolerance, None) {
            Ok(entities) => entities,
            Err(e) => {
                warn!("Failed to find fault tolerance entities: {}", e);
                return;
            }
        };

        let now = qlib_rs::data::now();

        for ft_entity_id in &fault_tolerances.entities {
            // Read fault tolerance fields
            let candidate_list_result = self.store_handle.read(*ft_entity_id, vec![ft_candidate_list]);
            let available_list_result = self.store_handle.read(*ft_entity_id, vec![ft_available_list]);
            let current_leader_result = self.store_handle.read(*ft_entity_id, vec![ft_current_leader]);

            let candidates = match candidate_list_result {
                Ok(response) => {
                    if let Value::EntityList(list) = response.value {
                        list
                    } else {
                        warn!("Invalid candidate list format");
                        continue;
                    }
                }
                Err(e) => {
                    warn!("Failed to read candidate list: {}", e);
                    continue;
                }
            };

            if candidates.is_empty() {
                warn!("Failed to get candidate list");
                continue;
            }

            let current_available = match available_list_result {
                Ok(response) => {
                    if let Value::EntityList(list) = response.value {
                        list
                    } else {
                        Vec::new()
                    }
                }
                Err(_) => Vec::new(),
            };

            let current_leader = match current_leader_result {
                Ok(response) => {
                    if let Value::EntityReference(Some(id)) = response.value {
                        Some(id)
                    } else {
                        None
                    }
                }
                Err(_) => None,
            };

            // Check availability of each candidate
            let mut available = Vec::new();
            for candidate_id in &candidates {
                let make_me_result = self.store_handle.read(*candidate_id, vec![ft_make_me]);
                let heartbeat_result = self.store_handle.read(*candidate_id, vec![ft_heartbeat]);
                let timeout_result = self.store_handle.read(*candidate_id, vec![ft_death_detection_timeout]);

                let make_me = match make_me_result {
                    Ok(response) => {
                        if let Value::Choice(choice) = response.value {
                            choice
                        } else {
                            0
                        }
                    }
                    Err(_) => 0,
                };

                let heartbeat_time = match heartbeat_result {
                    Ok(response) => response.timestamp,
                    Err(_) => qlib_rs::data::epoch(),
                };

                let death_detection_timeout_millis = match timeout_result {
                    Ok(response) => {
                        if let Value::Int(timeout) = response.value {
                            timeout
                        } else {
                            5000
                        }
                    }
                    Err(_) => 5000,
                };

                // Check if candidate wants to be available and has recent heartbeat
                let death_detection_timeout =
                    time::Duration::milliseconds(death_detection_timeout_millis);
                if make_me == 1 && heartbeat_time + death_detection_timeout > now {
                    available.push(*candidate_id);
                }
            }

            // Update available list if changed
            if available != current_available {
                debug!(
                    "Updating available list for fault tolerance entity {:?}: {:?}",
                    ft_entity_id, available
                );
                if let Err(e) = self.store_handle.write(
                    *ft_entity_id,
                    vec![ft_available_list],
                    Value::EntityList(available.clone()),
                    None,
                    None,
                    None,
                    None,
                ) {
                    warn!("Failed to update available list: {}", e);
                    continue;
                }
            }

            // Handle leadership
            let mut handle_me_as_candidate = false;
            if let Some(me_as_candidate) = &me_as_candidate {
                // If we're in the candidate list, we can be leader
                if candidates.contains(me_as_candidate) && available.contains(me_as_candidate) {
                    handle_me_as_candidate = true;
                    if current_leader.is_none() || current_leader != Some(*me_as_candidate) {
                        info!(
                            "Setting current leader to {:?} for fault tolerance entity {:?}",
                            me_as_candidate, ft_entity_id
                        );
                        if let Err(e) = self.store_handle.write(
                            *ft_entity_id,
                            vec![ft_current_leader],
                            Value::EntityReference(Some(*me_as_candidate)),
                            None,
                            None,
                            None,
                            None,
                        ) {
                            warn!("Failed to set current leader: {}", e);
                        }
                    }
                }
            }

            if !handle_me_as_candidate {
                // Promote an available candidate to leader if needed
                if current_leader.is_none() {
                    if let Some(new_leader) = available.first() {
                        info!(
                            "Setting new leader {:?} for fault tolerance entity {:?} (no current leader)",
                            new_leader, ft_entity_id
                        );
                        if let Err(e) = self.store_handle.write(
                            *ft_entity_id,
                            vec![ft_current_leader],
                            Value::EntityReference(Some(*new_leader)),
                            None,
                            None,
                            None,
                            None,
                        ) {
                            warn!("Failed to set new leader: {}", e);
                        }
                    }
                } else if let Some(current_leader_id) = current_leader {
                    if !available.contains(&current_leader_id) {
                        // Current leader is not available, find next one
                        let next_leader = if let Some(current_idx) =
                            candidates.iter().position(|c| *c == current_leader_id)
                        {
                            // Try to find next available candidate in round-robin fashion
                            let mut found = None;
                            for i in 1..=candidates.len() {
                                let idx = (current_idx + i) % candidates.len();
                                if available.contains(&candidates[idx]) {
                                    found = Some(candidates[idx]);
                                    break;
                                }
                            }
                            found
                        } else {
                            available.first().copied()
                        };

                        if let Some(next_leader) = next_leader {
                            info!(
                                "Updating leader from {:?} to {:?} for fault tolerance entity {:?} (current leader unavailable)",
                                current_leader, next_leader, ft_entity_id
                            );
                            if let Err(e) = self.store_handle.write(
                                *ft_entity_id,
                                vec![ft_current_leader],
                                Value::EntityReference(Some(next_leader)),
                                None,
                                None,
                                None,
                                None,
                            ) {
                                warn!("Failed to update leader: {}", e);
                            }
                        }
                    }
                }
            }
        }
    }
}

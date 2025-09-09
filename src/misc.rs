use qlib_rs::{et, ft, now, schoice, sread, sref, swrite, PushCondition, Value};
use tokio::sync::{mpsc, oneshot};
use tracing::error;
use anyhow::Result;
use std::time::Duration;
use time;

use crate::Services;
use crate::peers::AvailabilityState;

/// Configuration for the misc service
#[derive(Debug, Clone)]
pub struct MiscConfig {
    /// Machine ID (unique identifier for this instance)
    pub machine: String,
}

impl From<&crate::Config> for MiscConfig {
    fn from(config: &crate::Config) -> Self {
        Self {
            machine: config.machine.clone(),
        }
    }
}

/// Misc service request types
#[derive(Debug)]
pub enum MiscRequest {
    SetServices {
        services: Services,
        response: oneshot::Sender<()>,
    },
}

/// Handle for communicating with misc service
#[derive(Debug, Clone)]
pub struct MiscHandle {
    sender: mpsc::UnboundedSender<MiscRequest>,
}

impl MiscHandle {
    /// Set services for dependencies
    pub async fn set_services(&self, services: Services) {
        let (response_tx, response_rx) = oneshot::channel();
        if self.sender.send(MiscRequest::SetServices {
            services,
            response: response_tx,
        }).is_ok() {
            let _ = response_rx.await;
        }
    }
}

pub struct MiscService {
    config: MiscConfig,
    services: Option<Services>,
}

impl MiscService {
    pub fn spawn(config: MiscConfig) -> MiscHandle {
        let (sender, mut receiver) = mpsc::unbounded_channel();
        
        tokio::spawn(async move {
            let mut service = Self {
                config,
                services: None,
            };
            
            let mut misc_interval = tokio::time::interval(Duration::from_millis(10));
            let mut heartbeat_interval = tokio::time::interval(Duration::from_secs(1));
            
            loop {
                tokio::select! {
                    // Handle service requests
                    request = receiver.recv() => {
                        match request {
                            Some(req) => service.handle_request(req).await,
                            None => break, // Channel closed
                        }
                    }
                    
                    // Handle misc tasks every 10ms
                    _ = misc_interval.tick() => {
                        if let Some(ref services) = service.services {
                            if let Err(e) = Self::handle_fault_tolerance_management(&service.config, services).await {
                                error!(error = %e, "Error in fault tolerance management");
                            }
                        }
                    }
                    
                    // Handle heartbeat every 1 second
                    _ = heartbeat_interval.tick() => {
                        if let Some(ref services) = service.services {
                            if let Err(e) = Self::write_heartbeat(&service.config, services).await {
                                error!(error = %e, "Error in heartbeat writing");
                            }
                        }
                    }
                }
            }
        });

        MiscHandle { sender }
    }
    
    async fn handle_request(&mut self, request: MiscRequest) {
        match request {
            MiscRequest::SetServices { services, response } => {
                self.services = Some(services);
                let _ = response.send(());
            }
        }
    }
    
    /// Handle fault tolerance and leader management when this instance is the leader
    async fn handle_fault_tolerance_management(
        config: &MiscConfig,
        services: &Services,
    ) -> Result<()> {
        let (is_leader, _) = services.peer_handle.get_leadership_info().await;
        
        if !is_leader {
            return Ok(());
        }
        
        // Find us as a candidate
        let me_as_candidate = {
            let machine = &config.machine;
            
            let candidates = services.store_handle.find_entities_paginated(
                &et::candidate(), 
                None,
                Some(format!("Name == 'qcore' && Parent->Name == '{}'", machine))
            ).await?;
            
            candidates.items.first().cloned()
        };
        
        // Update available list and current leader for all fault tolerance entities
        let fault_tolerances = services.store_handle.find_entities_paginated(
            &et::fault_tolerance(), 
            None,
            None
        ).await?;
        
        for ft_entity_id in fault_tolerances.items {
            let ft_fields = services.store_handle.perform_map(vec![
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
                let candidate_fields = services.store_handle.perform_map(vec![
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
            
            let mut requests = vec![
                swrite!(ft_entity_id.clone(), ft::available_list(), Some(Value::EntityList(available.clone())), PushCondition::Changes),
            ];
            
            let mut handle_me_as_candidate = false;
            if let Some(me_as_candidate) = &me_as_candidate {
                // If we're not in the candidate list, we can't be leader
                if candidates.contains(me_as_candidate) {
                    handle_me_as_candidate = true;
                    
                    requests.push(
                        swrite!(ft_entity_id.clone(), ft::current_leader(), sref!(Some(me_as_candidate.clone())), PushCondition::Changes)
                    );
                }
            }
            
            if !handle_me_as_candidate {
                // Promote an available candidate to leader if current leader is no longer available
                let current_leader = ft_fields
                    .get(&ft::current_leader())
                    .unwrap()
                    .value()
                    .unwrap()
                    .expect_entity_reference()?;
                
                if current_leader.is_none() {
                    requests.push(
                        swrite!(ft_entity_id.clone(), ft::current_leader(), sref!(available.first().cloned()), PushCondition::Changes)
                    );
                } else if let Some(current_leader) = current_leader {
                    if !available.contains(&current_leader) {
                        // Find the position of the current leader in the candidate list
                        let current_leader_idx = candidates.iter().position(|c| c.clone() == current_leader.clone());
                        
                        if let Some(current_idx) = current_leader_idx {
                            // Find the next available candidate after the current leader
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
                            
                            requests.push(
                                swrite!(ft_entity_id.clone(), ft::current_leader(), sref!(next_leader), PushCondition::Changes)
                            );
                        } else {
                            // Current leader not found in candidates list, just pick the first available
                            requests.push(
                                swrite!(ft_entity_id.clone(), ft::current_leader(), sref!(available.first().cloned()), PushCondition::Changes)
                            );
                        }
                    }
                }
            }
            
            services.store_handle.perform_mut(&mut requests).await?;
        }
        
        Ok(())
    }
    
    /// Handle heartbeat writing for this machine
    async fn write_heartbeat(
        config: &MiscConfig,
        services: &Services,
    ) -> Result<()> {
        let machine = &config.machine;
        
        let candidates = services.store_handle.find_entities_paginated(
            &et::candidate(), 
            None,
            Some(format!("Name == 'qcore' && Parent->Name == '{}'", machine))
        ).await?;
        
        if let Some(candidate) = candidates.items.first() {
            let mut requests = vec![
                swrite!(candidate.clone(), ft::heartbeat(), schoice!(0)),
                swrite!(candidate.clone(), ft::make_me(), schoice!(1), PushCondition::Changes)
            ];
            
            services.store_handle.perform_mut(&mut requests).await?;
        }
        
        Ok(())
    }
}

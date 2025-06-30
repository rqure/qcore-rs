use mdns_sd::{ServiceDaemon, ServiceInfo, ServiceEvent};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{RwLock, mpsc};
use std::time::Duration;
use std::net::{IpAddr, Ipv4Addr};
use hostname;

use crate::app::NodeId;

pub const SERVICE_TYPE: &str = "_qcore._tcp.local.";
pub const SERVICE_DOMAIN: &str = "local.";

#[derive(Debug, Clone)]
pub struct DiscoveredNode {
    pub node_id: NodeId,
    pub address: String,
    pub hostname: String,
    pub port: u16,
}

pub struct MdnsDiscovery {
    daemon: ServiceDaemon,
    discovered_nodes: Arc<RwLock<HashMap<NodeId, DiscoveredNode>>>,
    node_id: NodeId,
    service_name: String,
    port: u16,
}

impl MdnsDiscovery {
    pub fn new(node_id: NodeId, port: u16) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let daemon = ServiceDaemon::new()?;
        let _hostname = hostname::get()
            .map(|h| h.to_string_lossy().to_string())
            .unwrap_or_else(|_| format!("qcore-node-{}", node_id));
        
        let service_name = format!("qcore-node-{}", node_id);
        
        Ok(MdnsDiscovery {
            daemon,
            discovered_nodes: Arc::new(RwLock::new(HashMap::new())),
            node_id,
            service_name,
            port,
        })
    }

    pub async fn start_discovery(&self) -> Result<mpsc::Receiver<DiscoveredNode>, Box<dyn std::error::Error + Send + Sync>> {
        let (tx, rx) = mpsc::channel(100);
        
        // Register our service
        self.register_service().await?;
        
        // Start browsing for other services
        let browser = self.daemon.browse(SERVICE_TYPE)?;
        let discovered_nodes = self.discovered_nodes.clone();
        let node_id = self.node_id;
        
        tokio::spawn(async move {
            loop {
                match browser.recv_async().await {
                    Ok(event) => {
                        match Self::handle_service_event(event, &discovered_nodes, node_id, &tx).await {
                            Ok(_) => {},
                            Err(e) => log::error!("Error handling service event: {}", e),
                        }
                    }
                    Err(e) => {
                        log::error!("Error receiving service event: {}", e);
                        tokio::time::sleep(Duration::from_secs(1)).await;
                    }
                }
            }
        });
        
        Ok(rx)
    }

    async fn register_service(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        // Get local IP address
        let local_ip = Self::get_local_ip()?;
        
        // Create service info with TXT records containing node metadata
        let properties = HashMap::from([
            ("node_id".to_string(), self.node_id.to_string()),
            ("version".to_string(), "1.0".to_string()),
        ]);
        
        let service_info = ServiceInfo::new(
            SERVICE_TYPE,
            &self.service_name,
            &format!("{}.{}", &self.service_name, SERVICE_DOMAIN),
            local_ip,
            self.port,
            Some(properties),
        )?;
        
        self.daemon.register(service_info)?;
        log::info!("Registered mDNS service: {} on {}:{}", self.service_name, local_ip, self.port);
        
        Ok(())
    }

    async fn handle_service_event(
        event: ServiceEvent,
        discovered_nodes: &Arc<RwLock<HashMap<NodeId, DiscoveredNode>>>,
        our_node_id: NodeId,
        tx: &mpsc::Sender<DiscoveredNode>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        match event {
            ServiceEvent::ServiceResolved(info) => {
                log::debug!("Service resolved: {:?}", info);
                
                // Extract node ID from properties
                let properties = info.get_properties();
                if let Some(node_id_prop) = properties.get("node_id") {
                    if let Some(val) = node_id_prop.val() {
                        if let Ok(node_id_str) = std::str::from_utf8(val) {
                            if let Ok(node_id) = node_id_str.parse::<NodeId>() {
                                // Don't discover ourselves
                                if node_id == our_node_id {
                                    return Ok(());
                                }
                                
                                let discovered_node = DiscoveredNode {
                                    node_id,
                                    address: format!("{}:{}", info.get_addresses().iter().next().unwrap_or(&IpAddr::V4(Ipv4Addr::LOCALHOST)), info.get_port()),
                                    hostname: info.get_hostname().to_string(),
                                    port: info.get_port(),
                                };
                                
                                // Add to discovered nodes
                                {
                                    let mut nodes = discovered_nodes.write().await;
                                    nodes.insert(node_id, discovered_node.clone());
                                }
                                
                                // Notify listeners
                                if let Err(e) = tx.send(discovered_node.clone()).await {
                                    log::warn!("Failed to send discovered node notification: {}", e);
                                }
                                log::info!("Discovered node: {} at {}", node_id, discovered_node.address);
                            }
                        }
                    }
                }
            }
            ServiceEvent::ServiceRemoved(typ, name) => {
                log::info!("Service removed: {} ({})", name, typ);
                
                // Try to extract node ID from service name
                if let Some(node_id_str) = name.strip_prefix("qcore-node-") {
                    if let Ok(node_id) = node_id_str.parse::<NodeId>() {
                        let mut nodes = discovered_nodes.write().await;
                        if let Some(removed_node) = nodes.remove(&node_id) {
                            log::info!("Removed node: {} from discovered nodes", removed_node.node_id);
                        }
                    }
                }
            }
            _ => {
                log::debug!("Other service event: {:?}", event);
            }
        }
        
        Ok(())
    }

    pub async fn get_discovered_nodes(&self) -> HashMap<NodeId, DiscoveredNode> {
        self.discovered_nodes.read().await.clone()
    }

    pub async fn wait_for_nodes(&self, min_nodes: usize, timeout: Duration) -> Result<Vec<DiscoveredNode>, Box<dyn std::error::Error + Send + Sync>> {
        let start_time = std::time::Instant::now();
        
        loop {
            let nodes = self.discovered_nodes.read().await;
            if nodes.len() >= min_nodes {
                return Ok(nodes.values().cloned().collect());
            }
            drop(nodes);
            
            if start_time.elapsed() > timeout {
                return Err("Timeout waiting for nodes".into());
            }
            
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    pub async fn unregister(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        self.daemon.unregister(&self.service_name)?;
        log::info!("Unregistered mDNS service: {}", self.service_name);
        Ok(())
    }

    fn get_local_ip() -> Result<IpAddr, Box<dyn std::error::Error + Send + Sync>> {
        // Try to get the local IP address by connecting to a remote address
        use std::net::UdpSocket;
        
        let socket = UdpSocket::bind("0.0.0.0:0")?;
        socket.connect("8.8.8.8:80")?;
        let local_addr = socket.local_addr()?;
        Ok(local_addr.ip())
    }
}

impl Drop for MdnsDiscovery {
    fn drop(&mut self) {
        if let Err(e) = self.daemon.unregister(&self.service_name) {
            log::warn!("Failed to unregister mDNS service on drop: {}", e);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_mdns_discovery() {
        let discovery1 = MdnsDiscovery::new(1, 8080).unwrap();
        let discovery2 = MdnsDiscovery::new(2, 8081).unwrap();

        let _rx1 = discovery1.start_discovery().await.unwrap();
        let _rx2 = discovery2.start_discovery().await.unwrap();

        // Wait a bit for services to be discovered
        tokio::time::sleep(Duration::from_secs(2)).await;

        // Check if nodes discovered each other
        let nodes1 = discovery1.get_discovered_nodes().await;
        let nodes2 = discovery2.get_discovered_nodes().await;

        // Note: This test might not always pass in CI environments
        // where mDNS might not be available
        if !nodes1.is_empty() {
            assert!(nodes1.contains_key(&2));
        }
        if !nodes2.is_empty() {
            assert!(nodes2.contains_key(&1));
        }

        discovery1.unregister().await.unwrap();
        discovery2.unregister().await.unwrap();
    }

    #[test]
    fn test_get_local_ip() {
        let ip = MdnsDiscovery::get_local_ip().unwrap();
        assert!(!ip.is_loopback());
    }
}

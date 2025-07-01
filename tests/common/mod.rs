use std::sync::Arc;
use std::time::Duration;
use tokio::time::sleep;
use qcore_rs::{app::App, network::Network, store::{LogStore, StateMachineStore}};
use openraft::Config;

pub struct ClusterNode {
    pub id: u64,
    pub ws_addr: String,
    pub app: Arc<App>,
    pub _server_handle: tokio::task::JoinHandle<std::io::Result<()>>,
}

impl Clone for ClusterNode {
    fn clone(&self) -> Self {
        ClusterNode {
            id: self.id,
            ws_addr: self.ws_addr.clone(),
            app: self.app.clone(),
            _server_handle: tokio::spawn(async { Ok(()) }), // Dummy handle for testing
        }
    }
}

/// Set up a test cluster with the specified number of nodes
pub async fn setup_test_cluster(node_count: u64) -> Vec<ClusterNode> {
    let mut nodes = Vec::new();
    let mut node_addresses = Vec::new();
    
    // First, collect all addresses for the cluster configuration
    for i in 1..=node_count {
        let ws_addr = format!("127.0.0.1:{}", 8000 + i);
        node_addresses.push((i, ws_addr.clone()));
    }
    
    // Start each node
    for (node_id, ws_addr) in node_addresses {
        let node = start_node_with_cluster_config(node_id, ws_addr.clone(), &[(node_id, ws_addr.clone())]).await;
        nodes.push(node);
    }
    
    // Initialize the cluster with the first node
    if !nodes.is_empty() {
        let first_node = &nodes[0];
        let init_nodes: Vec<(u64, String)> = nodes.iter()
            .map(|n| (n.id, n.ws_addr.clone()))
            .collect();
            
        match first_node.app.raft.initialize(
            init_nodes.into_iter()
                .map(|(id, addr)| (id, openraft::BasicNode { addr }))
                .collect::<std::collections::BTreeMap<u64, openraft::BasicNode>>()
        ).await {
            Ok(_) => println!("✅ Cluster initialized successfully"),
            Err(e) => println!("❌ Failed to initialize cluster: {}", e),
        }
        
        // Wait a bit for initialization to complete
        sleep(Duration::from_millis(1000)).await;
    }
    
    nodes
}

/// Start a single node for testing
pub async fn start_single_node(node_id: u64) -> ClusterNode {
    let ws_addr = format!("127.0.0.1:{}", 8000 + node_id);
    let node = start_node_with_cluster_config(node_id, ws_addr.clone(), &[(node_id, ws_addr.clone())]).await;
    
    // Initialize as single-node cluster
    match node.app.raft.initialize(
        [(node_id, openraft::BasicNode { addr: ws_addr.clone() })]
            .into_iter()
            .collect::<std::collections::BTreeMap<u64, openraft::BasicNode>>()
    ).await {
        Ok(_) => println!("✅ Single node {} initialized successfully", node_id),
        Err(e) => println!("❌ Failed to initialize single node {}: {}", node_id, e),
    }
    
    node
}

/// Start a node with cluster configuration
async fn start_node_with_cluster_config(
    node_id: u64, 
    ws_addr: String,
    _cluster_nodes: &[(u64, String)]
) -> ClusterNode {
    // Create temporary directory for this node
    let temp_dir = std::env::temp_dir().join(format!("qcore_test_node_{}", node_id));
    std::fs::create_dir_all(&temp_dir).expect("Failed to create temp directory");
    
    // Create Raft configuration
    let config = Arc::new(Config {
        heartbeat_interval: 250,
        election_timeout_min: 299,
        election_timeout_max: 500,
        ..Default::default()
    });
    
    // Create stores
    let log_store = LogStore::new_for_node_with_config(
        temp_dir.join("log"),
        node_id,
        qcore_rs::store::LogStoreConfig::default(),
    ).expect("Failed to create log store");
    
    let state_machine_store = Arc::new(StateMachineStore::new_for_node(
        temp_dir.join("state_machine"),
        node_id,
    ).expect("Failed to create state machine store"));
    
    // Create network
    let network = Network::default();
    
    // Create Raft instance
    let raft = openraft::Raft::new(
        node_id,
        config.clone(),
        network.clone(),
        log_store.clone(),
        state_machine_store.clone(),
    ).await.expect("Failed to create Raft instance");
    
    // Create app
    let app = Arc::new(App {
        id: node_id,
        addr: ws_addr.clone(),
        raft,
        state_machine_store,
        network,
    });
    
    // Start WebSocket server
    let app_clone = app.clone();
    let ws_addr_clone = ws_addr.clone();
    let server_handle = tokio::spawn(async move {
        qcore_rs::websocket::start_websocket_server(ws_addr_clone, app_clone).await
    });
    
    // Give the server time to start
    sleep(Duration::from_millis(100)).await;
    
    ClusterNode {
        id: node_id,
        ws_addr,
        app,
        _server_handle: server_handle,
    }
}

/// Wait for a leader to be elected in the cluster
pub async fn wait_for_leader(nodes: &[ClusterNode]) -> Option<&ClusterNode> {
    let max_attempts = 20;
    let sleep_duration = Duration::from_millis(500);
    
    for _attempt in 0..max_attempts {
        for node in nodes {
            let metrics = node.app.raft.metrics().borrow().clone();
            if let Some(leader_id) = metrics.current_leader {
                if leader_id == node.id {
                    println!("✅ Leader elected: node {}", node.id);
                    return Some(node);
                }
            }
        }
        sleep(sleep_duration).await;
    }
    
    println!("❌ No leader elected after waiting");
    None
}

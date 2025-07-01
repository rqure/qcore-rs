use std::time::Duration;
use tokio::time::sleep;
use qlib_rs::{
    Context, EntitySchema, EntityType, FieldSchema, FieldType, 
    NotifyConfig, StoreProxy, Single, Value, Request, AdjustBehavior, PushCondition
};

mod common;
use common::{setup_test_cluster, wait_for_leader, start_single_node};

/// Test basic read/write operations work correctly
#[tokio::test]
async fn test_basic_read_write_operations() {
    let nodes = setup_test_cluster(3).await;
    
    // Wait for leader election
    sleep(Duration::from_millis(2000)).await;
    let leader = wait_for_leader(&nodes).await.expect("No leader elected");
    
    // Connect to leader
    let proxy = StoreProxy::connect(&format!("ws://{}", leader.ws_addr)).await.unwrap();
    let ctx = Context {};
    
    // Create a schema
    let entity_type = EntityType::from("TestEntity");
    let mut schema = EntitySchema::<Single>::new(entity_type.clone(), None);
    schema.fields.insert(
        FieldType::from("Name"), 
        FieldSchema::String {
            field_type: FieldType::from("Name"),
            default_value: "".to_string(),
            rank: 0,
            read_permission: None,
            write_permission: None,
        }
    );
    
    proxy.set_entity_schema(&ctx, &schema).await.unwrap();
    
    // Create an entity
    let entity = proxy.create_entity(&ctx, &entity_type, None, "TestEntity1").await.unwrap();
    println!("Created entity: {:?}", entity);
    
    // Test reads work from all nodes
    for node in &nodes {
        let node_proxy = StoreProxy::connect(&format!("ws://{}", node.ws_addr)).await.unwrap();
        let exists = node_proxy.entity_exists(&ctx, &entity.entity_id).await.unwrap();
        assert!(exists, "Entity should exist when queried from node {}", node.id);
    }
    
    println!("✅ Basic read/write operations test passed");
}

/// Test that reads work during leader failover
#[tokio::test]
async fn test_reads_during_failover() {
    let nodes = setup_test_cluster(3).await;
    
    // Wait for leader election
    sleep(Duration::from_millis(2000)).await;
    let leader = wait_for_leader(&nodes).await.expect("No leader elected");
    
    // Connect to leader and set up data
    let proxy = StoreProxy::connect(&format!("ws://{}", leader.ws_addr)).await.unwrap();
    let ctx = Context {};
    
    // Create schema and entity
    let entity_type = EntityType::from("FailoverTest");
    let mut schema = EntitySchema::<Single>::new(entity_type.clone(), None);
    schema.fields.insert(
        FieldType::from("Name"), 
        FieldSchema::String {
            field_type: FieldType::from("Name"),
            default_value: "".to_string(),
            rank: 0,
            read_permission: None,
            write_permission: None,
        }
    );
    
    proxy.set_entity_schema(&ctx, &schema).await.unwrap();
    let entity = proxy.create_entity(&ctx, &entity_type, None, "FailoverEntity").await.unwrap();
    
    // Ensure data is replicated
    sleep(Duration::from_millis(1000)).await;
    
    // Stop the leader
    println!("Stopping leader node {}", leader.id);
    leader.app.raft.shutdown().await.unwrap();
    
    // Wait for new leader election
    sleep(Duration::from_millis(3000)).await;
    
    // Try reads from remaining nodes
    let remaining_nodes: Vec<_> = nodes.iter().filter(|n| n.id != leader.id).collect();
    let mut successful_reads = 0;
    
    for node in &remaining_nodes {
        match StoreProxy::connect(&format!("ws://{}", node.ws_addr)).await {
            Ok(node_proxy) => {
                match node_proxy.entity_exists(&ctx, &entity.entity_id).await {
                    Ok(exists) => {
                        if exists {
                            successful_reads += 1;
                            println!("✅ Successful read from node {} after failover", node.id);
                        } else {
                            println!("❌ Entity not found on node {} after failover", node.id);
                        }
                    }
                    Err(e) => {
                        println!("❌ Read failed from node {} after failover: {}", node.id, e);
                    }
                }
            }
            Err(e) => {
                println!("❌ Connection failed to node {} after failover: {}", node.id, e);
            }
        }
    }
    
    assert!(successful_reads >= 1, "At least one node should be able to serve reads after failover");
    println!("✅ Reads during failover test passed ({}/2 nodes responding)", successful_reads);
}

/// Test that writes work after leader failover
#[tokio::test]
async fn test_writes_after_failover() {
    let nodes = setup_test_cluster(3).await;
    
    // Wait for leader election
    sleep(Duration::from_millis(2000)).await;
    let leader = wait_for_leader(&nodes).await.expect("No leader elected");
    
    // Connect to leader and set up schema
    let proxy = StoreProxy::connect(&format!("ws://{}", leader.ws_addr)).await.unwrap();
    let ctx = Context {};
    
    let entity_type = EntityType::from("WriteFailoverTest");
    let mut schema = EntitySchema::<Single>::new(entity_type.clone(), None);
    schema.fields.insert(
        FieldType::from("Name"), 
        FieldSchema::String {
            field_type: FieldType::from("Name"),
            default_value: "".to_string(),
            rank: 0,
            read_permission: None,
            write_permission: None,
        }
    );
    
    proxy.set_entity_schema(&ctx, &schema).await.unwrap();
    
    // Stop the leader
    println!("Stopping leader node {}", leader.id);
    leader.app.raft.shutdown().await.unwrap();
    
    // Wait for new leader election
    sleep(Duration::from_millis(3000)).await;
    
    // Try to create an entity using the new leader
    let remaining_nodes: Vec<_> = nodes.iter().filter(|n| n.id != leader.id).collect();
    let mut write_successful = false;
    
    for node in &remaining_nodes {
        match StoreProxy::connect(&format!("ws://{}", node.ws_addr)).await {
            Ok(node_proxy) => {
                match node_proxy.create_entity(&ctx, &entity_type, None, "PostFailoverEntity").await {
                    Ok(entity) => {
                        println!("✅ Successful write to node {} after failover: {:?}", node.id, entity);
                        write_successful = true;
                        break;
                    }
                    Err(e) => {
                        println!("❌ Write failed to node {} after failover: {}", node.id, e);
                    }
                }
            }
            Err(e) => {
                println!("❌ Connection failed to node {} after failover: {}", node.id, e);
            }
        }
    }
    
    assert!(write_successful, "At least one node should accept writes after failover");
    println!("✅ Writes after failover test passed");
}

/// Test notification delivery during normal operations
#[tokio::test]
async fn test_notifications_normal_operation() {
    let node = start_single_node(1).await;
    
    // Connect and set up
    let proxy = StoreProxy::connect(&format!("ws://{}", node.ws_addr)).await.unwrap();
    let ctx = Context {};
    
    // Set up schema
    let entity_type = EntityType::from("NotificationTest");
    let mut schema = EntitySchema::<Single>::new(entity_type.clone(), None);
    schema.fields.insert(
        FieldType::from("Value"), 
        FieldSchema::Int {
            field_type: FieldType::from("Value"),
            default_value: 0,
            rank: 0,
            read_permission: None,
            write_permission: None,
        }
    );
    
    proxy.set_entity_schema(&ctx, &schema).await.unwrap();
    
    // Create entity
    let entity = proxy.create_entity(&ctx, &entity_type, None, "NotificationEntity").await.unwrap();
    
    // Register for notifications
    let config = NotifyConfig::EntityId {
        entity_id: entity.entity_id.clone(),
        field_type: FieldType::from("Value"),
        trigger_on_change: true,
        context: vec![],
    };
    
    let token = proxy.register_notification(&ctx, config).await.unwrap();
    
    // Subscribe to notifications
    let mut notifications = proxy.subscribe_notifications().await;
    
    // Perform a write operation
    let mut requests = vec![
        Request::Write {
            entity_id: entity.entity_id.clone(),
            field_type: FieldType::from("Value"),
            value: Some(Value::Int(42)),
            push_condition: PushCondition::Always,
            adjust_behavior: AdjustBehavior::Set,
            write_time: None,
            writer_id: None,
        }
    ];
    
    proxy.perform(&ctx, &mut requests).await.unwrap();
    
    // Wait for notification
    let notification_received = tokio::time::timeout(
        Duration::from_secs(5),
        notifications.recv()
    ).await;
    
    match notification_received {
        Ok(Some(notification)) => {
            println!("✅ Received notification: {:?}", notification);
            assert_eq!(notification.entity_id, entity.entity_id);
            assert_eq!(notification.field_type, FieldType::from("Value"));
            assert_eq!(notification.current_value, Value::Int(42));
        }
        Ok(None) => {
            panic!("❌ Notification channel closed unexpectedly");
        }
        Err(_) => {
            panic!("❌ Timeout waiting for notification");
        }
    }
    
    // Cleanup
    proxy.unregister_notification(&ctx, token).await.unwrap();
    
    println!("✅ Notifications normal operation test passed");
}

/// Test cluster resilience with multiple failovers
#[tokio::test]
async fn test_cluster_resilience() {
    let nodes = setup_test_cluster(5).await; // Start with 5 nodes for better resilience
    
    // Wait for leader election
    sleep(Duration::from_millis(2000)).await;
    let leader = wait_for_leader(&nodes).await.expect("No leader elected");
    
    // Connect to leader and set up data
    let proxy = StoreProxy::connect(&format!("ws://{}", leader.ws_addr)).await.unwrap();
    let ctx = Context {};
    
    // Set up schema
    let entity_type = EntityType::from("ResilienceTest");
    let mut schema = EntitySchema::<Single>::new(entity_type.clone(), None);
    schema.fields.insert(
        FieldType::from("Counter"), 
        FieldSchema::Int {
            field_type: FieldType::from("Counter"),
            default_value: 0,
            rank: 0,
            read_permission: None,
            write_permission: None,
        }
    );
    
    proxy.set_entity_schema(&ctx, &schema).await.unwrap();
    let entity = proxy.create_entity(&ctx, &entity_type, None, "ResilienceEntity").await.unwrap();
    
    // Ensure data is replicated
    sleep(Duration::from_millis(1000)).await;
    
    // Simulate multiple failovers
    let mut active_nodes = nodes.clone();
    let mut counter = 0;
    
    for failover in 0..2 {
        if active_nodes.len() <= 2 {
            break; // Need at least 2 nodes to maintain quorum
        }
        
        // Find current leader
        let current_leader = wait_for_leader(&active_nodes).await;
        if let Some(leader) = current_leader {
            println!("Failover {}: Stopping leader node {}", failover + 1, leader.id);
            let leader_id = leader.id;
            leader.app.raft.shutdown().await.unwrap();
            active_nodes.retain(|n| n.id != leader_id);
        }
        
        // Wait for new leader election
        sleep(Duration::from_millis(3000)).await;
        
        // Try to perform a write with remaining nodes
        let mut write_successful = false;
        for node in &active_nodes {
            match StoreProxy::connect(&format!("ws://{}", node.ws_addr)).await {
                Ok(node_proxy) => {
                    counter += 1;
                    let mut requests = vec![
                        Request::Write {
                            entity_id: entity.entity_id.clone(),
                            field_type: FieldType::from("Counter"),
                            value: Some(Value::Int(counter)),
                            push_condition: PushCondition::Always,
                            adjust_behavior: AdjustBehavior::Set,
                            write_time: None,
                            writer_id: None,
                        }
                    ];
                    
                    match node_proxy.perform(&ctx, &mut requests).await {
                        Ok(_) => {
                            println!("✅ Write successful on node {} after failover {}", node.id, failover + 1);
                            write_successful = true;
                            break;
                        }
                        Err(e) => {
                            println!("❌ Write failed on node {} after failover {}: {}", node.id, failover + 1, e);
                        }
                    }
                }
                Err(e) => {
                    println!("❌ Connection failed to node {} after failover {}: {}", node.id, failover + 1, e);
                }
            }
        }
        
        assert!(write_successful, "Cluster should remain operational after failover {}", failover + 1);
    }
    
    println!("✅ Cluster resilience test passed");
}

/// Test that data consistency is maintained across nodes
#[tokio::test]
async fn test_data_consistency() {
    let nodes = setup_test_cluster(3).await;
    
    // Wait for leader election
    sleep(Duration::from_millis(2000)).await;
    let leader = wait_for_leader(&nodes).await.expect("No leader elected");
    
    // Connect to leader
    let proxy = StoreProxy::connect(&format!("ws://{}", leader.ws_addr)).await.unwrap();
    let ctx = Context {};
    
    // Set up schema
    let entity_type = EntityType::from("ConsistencyTest");
    let mut schema = EntitySchema::<Single>::new(entity_type.clone(), None);
    schema.fields.insert(
        FieldType::from("Value"), 
        FieldSchema::String {
            field_type: FieldType::from("Value"),
            default_value: "".to_string(),
            rank: 0,
            read_permission: None,
            write_permission: None,
        }
    );
    
    proxy.set_entity_schema(&ctx, &schema).await.unwrap();
    
    // Create multiple entities
    let mut entities = Vec::new();
    for i in 0..5 {
        let entity = proxy.create_entity(&ctx, &entity_type, None, &format!("Entity{}", i)).await.unwrap();
        entities.push(entity);
    }
    
    // Wait for replication
    sleep(Duration::from_millis(2000)).await;
    
    // Verify all entities exist on all nodes
    for node in &nodes {
        let node_proxy = StoreProxy::connect(&format!("ws://{}", node.ws_addr)).await.unwrap();
        
        for entity in &entities {
            let exists = node_proxy.entity_exists(&ctx, &entity.entity_id).await.unwrap();
            assert!(exists, "Entity {:?} should exist on node {}", entity.entity_id, node.id);
        }
    }
    
    // Test find_entities returns consistent results
    for node in &nodes {
        let node_proxy = StoreProxy::connect(&format!("ws://{}", node.ws_addr)).await.unwrap();
        let found_entities = node_proxy.find_entities(&ctx, &entity_type, None, None).await.unwrap();
        
        assert_eq!(found_entities.items.len(), entities.len(), 
                  "Node {} should return {} entities, got {}", 
                  node.id, entities.len(), found_entities.items.len());
    }
    
    println!("✅ Data consistency test passed");
}

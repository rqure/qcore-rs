use qlib_rs::{Context, StoreProxy, EntityType};
use std::time::Duration;
use tokio::time::sleep;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // Example WebSocket client for qcore-rs
    println!("🚀 Starting WebSocket client example");
    
    // Connect to qcore-rs server
    let proxy = StoreProxy::connect("ws://127.0.0.1:8001").await?;
    println!("✅ Connected to qcore-rs server");
    
    // Create a test entity type
    let entity_type = EntityType("user".to_string());
    
    let context = Context {};
    
    // Create an entity
    let entity_id = proxy.create_entity(
        &context,
        &entity_type,
        None, // parent_id
        "test_entity", // name
    ).await?;
    println!("✅ Created entity with ID: {:?}", entity_id);
    
    // Wait a bit
    sleep(Duration::from_secs(1)).await;
    
    println!("🎉 WebSocket client example completed successfully");
    
    Ok(())
}

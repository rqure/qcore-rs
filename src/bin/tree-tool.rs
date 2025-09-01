use anyhow::{Context, Result};
use clap::Parser;
use qlib_rs::{StoreProxy, EntityId, EntityType, FieldType, Value};
use std::pin::Pin;
use std::boxed::Box;
use std::future::Future;

/// Command-line tool for displaying the tree structure of the QCore data store
#[derive(Parser)]
#[command(name = "tree-tool", about = "Display the tree structure of the QCore data store")]
struct Config {
    /// QCore service URL (WebSocket endpoint for client connections)
    #[arg(long, default_value = "ws://localhost:9100")]
    core_url: String,

    /// Username for authentication (can be set via QCORE_USERNAME env var)
    #[arg(long, default_value = "admin")]
    username: String,

    /// Password for authentication (can be set via QCORE_PASSWORD env var)
    #[arg(long, default_value = "admin123")]
    password: String,

    /// Maximum depth to traverse (0 = unlimited)
    #[arg(long, default_value_t = 0)]
    max_depth: usize,

    /// Show only entity types instead of names
    #[arg(long)]
    show_types: bool,

    /// Show entity IDs
    #[arg(long)]
    show_ids: bool,

    /// Start from a specific entity ID instead of Root
    #[arg(long)]
    start_from: Option<String>,

    /// Show detailed information (name, type, ID)
    #[arg(long, short)]
    verbose: bool,
}

/// Represents a node in the tree structure
#[derive(Debug, Clone)]
struct TreeNode {
    entity_id: EntityId,
    entity_type: String,
    name: String,
    children: Vec<TreeNode>,
}

#[tokio::main]
async fn main() -> Result<()> {
    let config = Config::parse();

    // Get credentials from environment if available
    let username = std::env::var("QCORE_USERNAME").unwrap_or(config.username.clone());
    let password = std::env::var("QCORE_PASSWORD").unwrap_or(config.password.clone());

    println!("Connecting to QCore service at {}...", config.core_url);
    
    // Connect to the Core service with authentication
    let mut store = StoreProxy::connect_and_authenticate(&config.core_url, &username, &password).await
        .with_context(|| format!("Failed to connect to Core service at {}", config.core_url))?;

    println!("Connected successfully. Building tree structure...");

    // Determine the starting entity
    let root_entity_id = if let Some(start_id) = &config.start_from {
        EntityId::try_from(start_id.as_str())
            .map_err(|e| anyhow::anyhow!("Invalid entity ID format '{}': {}", start_id, e))?
    } else {
        // Find the Root entity
        find_root_entity(&mut store).await
            .context("Failed to find Root entity")?
    };

    // Build the tree structure
    let tree = build_tree(&mut store, root_entity_id, config.max_depth, 0).await
        .context("Failed to build tree structure")?;

    // Print the tree
    println!("\nTree structure:");
    print_tree(&tree, "", true, &config);

    Ok(())
}

/// Find the Root entity in the data store
async fn find_root_entity(store: &mut StoreProxy) -> Result<EntityId> {
    // Look for entities of type "Root"
    let root_type = EntityType::from("Root");
    let entities = store.find_entities(&root_type, None).await
        .context("Failed to find Root entities")?;

    if entities.is_empty() {
        return Err(anyhow::anyhow!("No Root entity found in the data store"));
    }

    if entities.len() > 1 {
        println!("Warning: Multiple Root entities found, using the first one");
    }

    Ok(entities[0].clone())
}

/// Build the tree structure recursively
fn build_tree(
    store: &mut StoreProxy, 
    entity_id: EntityId, 
    max_depth: usize, 
    current_depth: usize
) -> Pin<Box<dyn Future<Output = Result<TreeNode>> + '_>> {
    Box::pin(async move {
        // Check depth limit
        if max_depth > 0 && current_depth >= max_depth {
            return Ok(TreeNode {
                entity_id: entity_id.clone(),
                entity_type: "...".to_string(),
                name: "...".to_string(),
                children: vec![],
            });
        }

        // Get entity type
        let entity_type = get_entity_type(store, &entity_id).await
            .with_context(|| format!("Failed to get entity type for {}", entity_id.get_id()))?;

        // Get entity name
        let name = get_entity_name(store, &entity_id).await
            .with_context(|| format!("Failed to get entity name for {}", entity_id.get_id()))?;

        // Get children
        let children_ids = get_entity_children(store, &entity_id).await
            .with_context(|| format!("Failed to get children for {}", entity_id.get_id()))?;

        // Recursively build child nodes
        let mut children = Vec::new();
        for child_id in children_ids {
            match build_tree(store, child_id, max_depth, current_depth + 1).await {
                Ok(child_node) => children.push(child_node),
                Err(e) => {
                    eprintln!("Warning: Failed to build tree for child entity: {}", e);
                    // Continue with other children instead of failing completely
                }
            }
        }

        Ok(TreeNode {
            entity_id,
            entity_type,
            name,
            children,
        })
    })
}

/// Get the entity type for a given entity ID
async fn get_entity_type(_store: &mut StoreProxy, entity_id: &EntityId) -> Result<String> {
    // The entity type is stored in the entity ID itself
    Ok(entity_id.get_type().as_ref().to_string())
}

/// Get the name of an entity
async fn get_entity_name(store: &mut StoreProxy, entity_id: &EntityId) -> Result<String> {
    let mut requests = vec![qlib_rs::sread!(entity_id.clone(), FieldType::from("Name"))];
    store.perform(&mut requests).await?;
    
    if let Some(request) = requests.first() {
        if let Some(Value::String(name)) = request.value() {
            return Ok(name.clone());
        }
    }
    
    Ok("Unnamed".to_string())
}

/// Get the children of an entity
async fn get_entity_children(store: &mut StoreProxy, entity_id: &EntityId) -> Result<Vec<EntityId>> {
    let mut requests = vec![qlib_rs::sread!(entity_id.clone(), FieldType::from("Children"))];
    store.perform(&mut requests).await?;
    
    if let Some(request) = requests.first() {
        if let Some(Value::EntityList(children)) = request.value() {
            return Ok(children.clone());
        }
    }
    
    Ok(vec![])
}

/// Print the tree structure using ASCII tree characters
fn print_tree(node: &TreeNode, prefix: &str, is_last: bool, config: &Config) {
    // Determine what to display
    let display_text = if config.verbose {
        format!("{} ({}) [{}]", node.name, node.entity_type, node.entity_id.get_id())
    } else if config.show_types {
        node.entity_type.clone()
    } else if config.show_ids {
        format!("{} [{}]", node.name, node.entity_id.get_id())
    } else {
        node.name.clone()
    };

    // Print current node
    let connector = if is_last { "└── " } else { "├── " };
    println!("{}{}{}", prefix, connector, display_text);

    // Print children
    let child_prefix = if is_last {
        format!("{}    ", prefix)
    } else {
        format!("{}│   ", prefix)
    };

    for (i, child) in node.children.iter().enumerate() {
        let is_last_child = i == node.children.len() - 1;
        print_tree(child, &child_prefix, is_last_child, config);
    }
}

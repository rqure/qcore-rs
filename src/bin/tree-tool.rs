use anyhow::{Context, Result};
use clap::Parser;
use qlib_rs::{ft, EntityId, EntityType, StoreProxy, Value};
use tracing::{info, warn};

/// Command-line tool for displaying the tree structure of the QCore data store
#[derive(Parser)]
#[command(name = "tree-tool", about = "Display the tree structure of the QCore data store")]
struct Config {
    /// QCore service URL (WebSocket endpoint for client connections)
    #[arg(long, default_value = "localhost:9100")]
    core_url: String,



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

fn main() -> Result<()> {
    // Initialize tracing for CLI tools
    tracing_subscriber::fmt()
        .with_env_filter(
            std::env::var("RUST_LOG")
                .unwrap_or_else(|_| "tree_tool=info".to_string())
        )
        .with_target(false)
        .without_time()
        .init();

    let config = Config::parse();
    
    info!(
        core_url = %config.core_url,
        max_depth = config.max_depth,
        show_types = config.show_types,
        show_ids = config.show_ids,
        verbose = config.verbose,
        start_from = ?config.start_from,
        "Starting tree tool"
    );

    info!(core_url = %config.core_url, "Connecting to QCore service");
    
    // Connect to the Core service
    let store = StoreProxy::connect(&config.core_url)
        .with_context(|| format!("Failed to connect to Core service at {}", config.core_url))?;

    info!("Connected successfully, building tree structure");

    // Determine the starting entity
    let root_entity_id = if let Some(start_id) = &config.start_from {
        // Parse entity ID from string format "type:id"
        let parts: Vec<&str> = start_id.split(':').collect();
        if parts.len() != 2 {
            return Err(anyhow::anyhow!("Invalid entity ID format '{}'. Expected format: 'type_id:entity_id'", start_id));
        }
        let type_id: u32 = parts[0].parse()
            .map_err(|e| anyhow::anyhow!("Invalid entity type ID '{}': {}", parts[0], e))?;
        let entity_id: u32 = parts[1].parse()
            .map_err(|e| anyhow::anyhow!("Invalid entity ID '{}': {}", parts[1], e))?;
        EntityId::new(EntityType(type_id), entity_id)
    } else {
        // Find the Root entity
        find_root_entity(&store)
            .context("Failed to find Root entity")?
    };

    // Build the tree structure
    let tree = build_tree(&store, root_entity_id, config.max_depth, 0)
        .context("Failed to build tree structure")?;

    // Print the tree
    info!("Tree structure built successfully");
    println!("\nTree structure:");
    print_tree(&tree, "", true, &config);

    Ok(())
}

/// Find the Root entity in the data store
fn find_root_entity(store: &StoreProxy) -> Result<EntityId> {
    // Look for entities of type "Root"
    let root_type = store.get_entity_type("Root")
        .context("Failed to get Root entity type")?;
    let entities = store.find_entities(root_type, None)
        .context("Failed to find Root entities")?;

    if entities.is_empty() {
        return Err(anyhow::anyhow!("No Root entity found in the data store"));
    }

    if entities.len() > 1 {
        warn!("Multiple Root entities found, using the first one");
    }

    Ok(entities[0])
}

/// Build the tree structure recursively
fn build_tree(
    store: &StoreProxy, 
    entity_id: EntityId, 
    max_depth: usize, 
    current_depth: usize
) -> Result<TreeNode> {
    // Check depth limit
    if max_depth > 0 && current_depth >= max_depth {
        return Ok(TreeNode {
            entity_id,
            entity_type: "...".to_string(),
            name: "...".to_string(),
            children: vec![],
        });
    }

    // Get entity type
    let entity_type = get_entity_type(store, entity_id)
        .with_context(|| format!("Failed to get entity type for {:?}", entity_id))?;

    // Get entity name
    let name = get_entity_name(store, entity_id)
        .with_context(|| format!("Failed to get entity name for {:?}", entity_id))?;

    // Get children
    let children_ids = get_entity_children(store, entity_id)
        .with_context(|| format!("Failed to get children for {:?}", entity_id))?;

    // Recursively build child nodes
    let mut children = Vec::new();
    for child_id in children_ids {
        let child_id_str = format!("{}", child_id.0);
        match build_tree(store, child_id, max_depth, current_depth + 1) {
            Ok(child_node) => children.push(child_node),
            Err(e) => {
                warn!(
                    child_id = %child_id_str,
                    error = %e,
                    "Failed to build tree for child entity"
                );
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
}

/// Get the entity type for a given entity ID
fn get_entity_type(store: &StoreProxy, entity_id: EntityId) -> Result<String> {
    // Extract the entity type from the entity ID and resolve it to a string
    let entity_type = entity_id.extract_type();
    store.resolve_entity_type(entity_type)
        .with_context(|| format!("Failed to resolve entity type {:?}", entity_type))
}

/// Get the name of an entity
fn get_entity_name(store: &StoreProxy, entity_id: EntityId) -> Result<String> {
    let name_ft = store.get_field_type(ft::NAME)
        .context("Failed to get Name field type")?;
    
    match store.read(entity_id, &[name_ft]) {
        Ok((Value::String(name), _, _)) => Ok(name),
        _ => Ok("Unnamed".to_string()),
    }
}

/// Get the children of an entity
fn get_entity_children(store: &StoreProxy, entity_id: EntityId) -> Result<Vec<EntityId>> {
    let children_ft = store.get_field_type(ft::CHILDREN)
        .context("Failed to get Children field type")?;
    
    match store.read(entity_id, &[children_ft]) {
        Ok((Value::EntityList(children), _, _)) => Ok(children),
        _ => Ok(vec![]),
    }
}

/// Print the tree structure using ASCII tree characters
fn print_tree(node: &TreeNode, prefix: &str, is_last: bool, config: &Config) {
    // Helper function to format entity ID
    let format_entity_id = || format!("{}", node.entity_id.0);
    
    // Determine what to display
    let display_text = if config.verbose {
        format!("{} ({}) [{}]", node.name, node.entity_type, format_entity_id())
    } else if config.show_types {
        node.entity_type.clone()
    } else if config.show_ids {
        format!("{} [{}]", node.name, format_entity_id())
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

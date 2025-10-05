use anyhow::{Context, Result};
use clap::Parser;
use qlib_rs::{EntityId, EntityType, FieldType, StoreProxy, Value, Timestamp, Notification, NotifyConfig};
use tracing::{info, debug};
use base64::{Engine as _, engine::general_purpose};
use crossbeam::channel::{Receiver, Sender};
use rustyline::{
    completion::{Completer, Pair},
    error::ReadlineError,
    highlight::{Highlighter, MatchingBracketHighlighter},
    hint::{Hinter, HistoryHinter},
    history::FileHistory,
    validate::{Validator, ValidationContext, ValidationResult},
    Context as RustyContext, Editor, Helper, Result as RustyResult,
};
use std::borrow::Cow;
use std::collections::HashMap;

/// QCore CLI - Interactive command-line interface for QCore data store
/// Similar to redis-cli, supports both interactive and command execution modes
#[derive(Parser)]
#[command(name = "qcore-cli", about = "QCore command-line interface", version)]
struct Config {
    /// QCore service URL (TCP endpoint for client connections)
    #[arg(short = 'h', long = "host", default_value = "localhost")]
    host: String,

    /// QCore service port
    #[arg(short = 'p', long = "port", default_value = "9100")]
    port: u16,

    /// Execute a single command and exit
    #[arg(short = 'c', long = "command")]
    command: Option<String>,

    /// Repeat command N times (use with -c)
    #[arg(short = 'r', long = "repeat", default_value = "1")]
    repeat: usize,

    /// Interval between commands in milliseconds (use with -r)
    #[arg(short = 'i', long = "interval", default_value = "0")]
    interval: u64,

    /// Output format (human, json, csv)
    #[arg(short = 'f', long = "format", default_value = "human")]
    format: OutputFormat,

    /// Enable verbose output
    #[arg(short = 'v', long = "verbose")]
    verbose: bool,

    /// Execute commands from file
    #[arg(long = "eval")]
    eval_file: Option<std::path::PathBuf>,

    /// Show raw RESP protocol messages (debug mode)
    #[arg(long = "raw")]
    raw: bool,

    /// No color output
    #[arg(long = "no-color")]
    no_color: bool,
}

#[derive(Debug, Clone, clap::ValueEnum)]
enum OutputFormat {
    /// Human-readable format with colors
    Human,
    /// JSON output
    Json,
    /// CSV output
    Csv,
    /// Raw RESP protocol output
    Raw,
}

/// Color codes for terminal output
struct Colors {
    reset: &'static str,
    bold: &'static str,
    dim: &'static str,
    red: &'static str,
    green: &'static str,
    yellow: &'static str,
    blue: &'static str,
    magenta: &'static str,
    cyan: &'static str,
}

impl Colors {
    fn new(enabled: bool) -> Self {
        if enabled {
            Colors {
                reset: "\x1b[0m",
                bold: "\x1b[1m",
                dim: "\x1b[2m",
                red: "\x1b[31m",
                green: "\x1b[32m",
                yellow: "\x1b[33m",
                blue: "\x1b[34m",
                magenta: "\x1b[35m",
                cyan: "\x1b[36m",
            }
        } else {
            Colors {
                reset: "",
                bold: "",
                dim: "",
                red: "",
                green: "",
                yellow: "",
                blue: "",
                magenta: "",
                cyan: "",
            }
        }
    }
}

fn main() -> Result<()> {
    let config = Config::parse();

    // Initialize tracing
    let log_level = if config.verbose {
        "qcore_cli=debug"
    } else {
        "qcore_cli=info"
    };

    tracing_subscriber::fmt()
        .with_env_filter(log_level)
        .with_target(false)
        .without_time()
        .init();

    let address = format!("{}:{}", config.host, config.port);
    
    // Connect to QCore service
    debug!("Connecting to {}", address);
    let store = StoreProxy::connect(&address)
        .with_context(|| format!("Failed to connect to QCore service at {}", address))?;
    
    info!("Connected to {}", address);

    let colors = Colors::new(!config.no_color);

    // Execute from file
    if let Some(file_path) = &config.eval_file {
        return execute_file(&store, file_path, &config, &colors);
    }

    // Execute single command
    if let Some(cmd) = &config.command {
        let mut notifications: HashMap<NotifyConfig, (Sender<Notification>, Receiver<Notification>)> = HashMap::new();
        for i in 0..config.repeat {
            if i > 0 && config.interval > 0 {
                std::thread::sleep(std::time::Duration::from_millis(config.interval));
            }
            execute_command(&store, cmd, &config, &colors, &mut notifications)?;
        }
        return Ok(());
    }

    // Interactive mode
    run_interactive(&store, &config, &colors)
}

fn run_interactive(store: &StoreProxy, config: &Config, colors: &Colors) -> Result<()> {
    println!("{}QCore CLI v0.1.0{}", colors.bold, colors.reset);
    println!("Type {}HELP{} for help, {}EXIT{} or {}QUIT{} to quit", 
        colors.cyan, colors.reset, colors.cyan, colors.reset, colors.cyan, colors.reset);
    println!();

    // Track registered notifications: config -> (sender, receiver)
    let mut notifications: HashMap<NotifyConfig, (Sender<Notification>, Receiver<Notification>)> = HashMap::new();

    // Setup rustyline editor
    let mut rl = Editor::<QCoreHelper, FileHistory>::new()
        .context("Failed to create readline editor")?;
    rl.set_helper(Some(QCoreHelper::new()));
    
    // Load history from file
    let history_file = std::env::var("HOME")
        .ok()
        .map(|home| std::path::PathBuf::from(home).join(".qcore_history"))
        .unwrap_or_else(|| std::path::PathBuf::from(".qcore_history"));
    
    if rl.load_history(&history_file).is_err() {
        debug!("No previous history found");
    }

    loop {
        let prompt = format!("{}qcore>{} ", colors.green, colors.reset);
        let readline = rl.readline(&prompt);

        match readline {
            Ok(line) => {
                let input = line.trim();
                if input.is_empty() {
                    continue;
                }

                // Add to history
                let _ = rl.add_history_entry(input);

                // Handle special commands
                match input.to_uppercase().as_str() {
                    "EXIT" | "QUIT" => {
                        println!("Goodbye!");
                        break;
                    }
                    "HELP" => {
                        print_help(colors);
                        continue;
                    }
                    "CLEAR" | "CLS" => {
                        print!("\x1b[2J\x1b[1;1H");
                        continue;
                    }
                    "HISTORY" => {
                        for (i, entry) in rl.history().iter().enumerate() {
                            println!("{}{:4}{} {}", colors.dim, i + 1, colors.reset, entry);
                        }
                        continue;
                    }
                    _ => {}
                }

                // Execute command
                match execute_command(store, input, config, colors, &mut notifications) {
                    Ok(_) => {
                    }
                    Err(e) => {
                        eprintln!("{}Error:{} {}", colors.red, colors.reset, e);
                    }
                }
            }
            Err(ReadlineError::Interrupted) => {
                println!("^C");
                continue;
            }
            Err(ReadlineError::Eof) => {
                println!("^D");
                break;
            }
            Err(err) => {
                eprintln!("{}Error:{} {}", colors.red, colors.reset, err);
                break;
            }
        }
    }

    // Save history
    let _ = rl.save_history(&history_file);

    Ok(())
}

fn execute_file(store: &StoreProxy, file_path: &std::path::PathBuf, config: &Config, colors: &Colors) -> Result<()> {
    let mut notifications: HashMap<NotifyConfig, (Sender<Notification>, Receiver<Notification>)> = HashMap::new();
    let content = std::fs::read_to_string(file_path)
        .with_context(|| format!("Failed to read file: {}", file_path.display()))?;

    for (line_num, line) in content.lines().enumerate() {
        let line = line.trim();
        
        // Skip empty lines and comments
        if line.is_empty() || line.starts_with('#') {
            continue;
        }

        if config.verbose {
            println!("{}[{}]{} {}", colors.dim, line_num + 1, colors.reset, line);
        }

        execute_command(store, line, config, colors, &mut notifications)?;
    }

    Ok(())
}

fn execute_command(store: &StoreProxy, input: &str, config: &Config, colors: &Colors, notifications: &mut HashMap<NotifyConfig, (Sender<Notification>, Receiver<Notification>)>) -> Result<()> {
    let parts: Vec<&str> = input.split_whitespace().collect();
    if parts.is_empty() {
        return Ok(());
    }

    let cmd = parts[0].to_uppercase();
    let args = &parts[1..];

    let start = std::time::Instant::now();
    
    let result = match cmd.as_str() {
        "PING" => cmd_ping(colors),
        "GET" => cmd_get(store, args, colors),
        "SET" => cmd_set(store, args, colors),
        "CREATE" => cmd_create(store, args, colors),
        "DEL" | "DELETE" => cmd_delete(store, args, colors),
        "EXISTS" => cmd_exists(store, args, colors),
        "GETTYPE" => cmd_gettype(store, args, colors),
        "RESTYPE" => cmd_restype(store, args, colors),
        "GETFLD" => cmd_getfld(store, args, colors),
        "RESFLD" => cmd_resfld(store, args, colors),
        "FIND" => cmd_find(store, args, colors),
        "FINDPAG" => cmd_findpag(store, args, colors),
        "FINDEX" => cmd_findex(store, args, colors),
        "TYPES" => cmd_types(store, colors),
        "TYPEPAG" => cmd_typepag(store, args, colors),
        "GETSCH" => cmd_getsch(store, args, colors),
        "GETCSCH" => cmd_getcsch(store, args, colors),
        "SETSCH" => cmd_setsch(store, args, colors),
        "GETFSCH" => cmd_getfsch(store, args, colors),
        "SETFSCH" => cmd_setfsch(store, args, colors),
        "FEXISTS" => cmd_fexists(store, args, colors),
        "RESOLVE" => cmd_resolve(store, args, colors),
        "SNAP" => cmd_snap(store, colors),
        "MACHINE" => cmd_machine(store, colors),
        "INFO" => cmd_info(store, colors),
        "LISTEN" => cmd_listen(store, args, colors, notifications),
        "UNLISTEN" => cmd_unlisten(store, args, colors, notifications),
        "POLL" => cmd_poll(store, args, colors, notifications),
        _ => {
            eprintln!("{}Unknown command:{} {}", colors.red, colors.reset, cmd);
            eprintln!("Type {}HELP{} for available commands", colors.cyan, colors.reset);
            return Ok(());
        }
    };

    let elapsed = start.elapsed();

    match result {
        Ok(_) => {
            if config.verbose {
                println!("{}({}){}", colors.dim, format_duration(&elapsed), colors.reset);
            }
        }
        Err(e) => {
            eprintln!("{}Error:{} {}", colors.red, colors.reset, e);
        }
    }

    Ok(())
}

fn cmd_ping(colors: &Colors) -> Result<()> {
    println!("{}PONG{}", colors.green, colors.reset);
    Ok(())
}

fn cmd_get(store: &StoreProxy, args: &[&str], colors: &Colors) -> Result<()> {
    if args.is_empty() {
        return Err(anyhow::anyhow!("Usage: GET <entity_id> [field_path...]"));
    }

    let entity_id = parse_entity_id(args[0])?;
    
    if args.len() == 1 {
        // Get all fields from complete schema
        let entity_type = entity_id.extract_type();
        let schema = store.get_complete_entity_schema(entity_type)?;
        
        for (field_type, _field_schema) in schema.fields.iter() {
            let field_path = vec![*field_type];
            match store.read(entity_id, &field_path) {
                Ok((value, timestamp, writer_id)) => {
                    let field_name = store.resolve_field_type(*field_type)
                        .unwrap_or_else(|_| format!("{}", field_type.0));
                    println!("{}{}{}: {}", colors.cyan, field_name, colors.reset, format_value_with_store(&value, colors, Some(store)));
                    
                    if writer_id.is_some() || timestamp.unix_timestamp() > 0 {
                        println!("{}  Timestamp:{} {}", colors.dim, colors.reset, format_timestamp(&timestamp));
                        if let Some(writer) = writer_id {
                            println!("{}  Writer:{} {}", colors.dim, colors.reset, writer.0);
                        }
                    }
                    println!();
                }
                Err(e) => {
                    let field_name = store.resolve_field_type(*field_type)
                        .unwrap_or_else(|_| format!("{}", field_type.0));
                    println!("{}{}{}: {}Error:{} {}", colors.cyan, field_name, colors.reset, colors.red, colors.reset, e);
                }
            }
        }
    } else {
        // Get specified fields
        for field_arg in &args[1..] {
            let field_path = parse_field_path(store, field_arg)?;
            match store.read(entity_id, &field_path) {
                Ok((value, timestamp, writer_id)) => {
                    println!("{}{}{}: {}", colors.cyan, field_arg, colors.reset, format_value_with_store(&value, colors, Some(store)));
                    
                    if writer_id.is_some() || timestamp.unix_timestamp() > 0 {
                        println!("{}  Timestamp:{} {}", colors.dim, colors.reset, format_timestamp(&timestamp));
                        if let Some(writer) = writer_id {
                            println!("{}  Writer:{} {}", colors.dim, colors.reset, writer.0);
                        }
                    }
                    println!();
                }
                Err(e) => {
                    println!("{}{}{}: {}Error:{} {}", colors.cyan, field_arg, colors.reset, colors.red, colors.reset, e);
                }
            }
        }
    }

    Ok(())
}

fn cmd_set(store: &StoreProxy, args: &[&str], colors: &Colors) -> Result<()> {
    if args.len() < 3 {
        return Err(anyhow::anyhow!("Usage: SET <entity_id> <field_path> <value>"));
    }

    let entity_id = parse_entity_id(args[0])?;
    let field_path = parse_field_path(store, args[1])?;
    let value = parse_value(args[2])?;

    store.write(entity_id, &field_path, value, None, None, None, None)?;

    println!("{}OK{}", colors.green, colors.reset);
    Ok(())
}

fn cmd_create(store: &StoreProxy, args: &[&str], colors: &Colors) -> Result<()> {
    if args.len() < 2 {
        return Err(anyhow::anyhow!("Usage: CREATE <entity_type> <name> [parent_id]"));
    }

    let entity_type = store.get_entity_type(args[0])?;
    let name = args[1];
    let parent_id = if args.len() > 2 {
        Some(parse_entity_id(args[2])?)
    } else {
        None
    };

    let entity_id = store.create_entity(entity_type, parent_id, name)?;

    let name_display = get_entity_name(store, entity_id).unwrap_or_default();
    if !name_display.is_empty() {
        println!("{}{}{} ({})", colors.cyan, entity_id.0, colors.reset, name_display);
    } else {
        println!("{}{}{}", colors.cyan, entity_id.0, colors.reset);
    }
    Ok(())
}

fn cmd_delete(store: &StoreProxy, args: &[&str], colors: &Colors) -> Result<()> {
    if args.is_empty() {
        return Err(anyhow::anyhow!("Usage: DELETE <entity_id>"));
    }

    let entity_id = parse_entity_id(args[0])?;
    store.delete_entity(entity_id)?;

    println!("{}OK{}", colors.green, colors.reset);
    Ok(())
}

fn cmd_exists(store: &StoreProxy, args: &[&str], colors: &Colors) -> Result<()> {
    if args.is_empty() {
        return Err(anyhow::anyhow!("Usage: EXISTS <entity_id>"));
    }

    let entity_id = parse_entity_id(args[0])?;
    let exists = store.entity_exists(entity_id);

    println!("{}{}{}", colors.cyan, if exists { "1" } else { "0" }, colors.reset);
    Ok(())
}

fn cmd_gettype(store: &StoreProxy, args: &[&str], colors: &Colors) -> Result<()> {
    if args.is_empty() {
        return Err(anyhow::anyhow!("Usage: GETTYPE <name>"));
    }

    let entity_type = store.get_entity_type(args[0])?;
    println!("{}{}{}", colors.cyan, entity_type.0, colors.reset);
    Ok(())
}

fn cmd_restype(store: &StoreProxy, args: &[&str], colors: &Colors) -> Result<()> {
    if args.is_empty() {
        return Err(anyhow::anyhow!("Usage: RESTYPE <type_id>"));
    }

    let type_id: u32 = args[0].parse()
        .context("Invalid type ID")?;
    let name = store.resolve_entity_type(EntityType(type_id))?;
    
    println!("{}\"{}\"{}", colors.yellow, name, colors.reset);
    Ok(())
}

fn cmd_getfld(store: &StoreProxy, args: &[&str], colors: &Colors) -> Result<()> {
    if args.is_empty() {
        return Err(anyhow::anyhow!("Usage: GETFLD <name>"));
    }

    let field_type = store.get_field_type(args[0])?;
    println!("{}{}{}", colors.cyan, field_type.0, colors.reset);
    Ok(())
}

fn cmd_resfld(store: &StoreProxy, args: &[&str], colors: &Colors) -> Result<()> {
    if args.is_empty() {
        return Err(anyhow::anyhow!("Usage: RESFLD <field_id>"));
    }

    let field_id: u64 = args[0].parse()
        .context("Invalid field ID")?;
    let name = store.resolve_field_type(FieldType(field_id))?;
    
    println!("{}\"{}\"{}", colors.yellow, name, colors.reset);
    Ok(())
}

fn cmd_find(store: &StoreProxy, args: &[&str], colors: &Colors) -> Result<()> {
    if args.is_empty() {
        return Err(anyhow::anyhow!("Usage: FIND <entity_type> [filter]"));
    }

    let entity_type = store.get_entity_type(args[0])?;
    let filter = if args.len() > 1 {
        Some(args[1..].join(" "))
    } else {
        None
    };

    let entities = store.find_entities(entity_type, filter.as_deref())?;

    for (i, entity_id) in entities.iter().enumerate() {
        let name_display = get_entity_name(store, *entity_id).unwrap_or_default();
        if !name_display.is_empty() {
            println!("{}{}{} {} ({})", colors.dim, i + 1, colors.reset, entity_id.0, name_display);
        } else {
            println!("{}{}{} {}", colors.dim, i + 1, colors.reset, entity_id.0);
        }
    }
    
    println!();
    println!("{}({} entities){}", colors.dim, entities.len(), colors.reset);

    Ok(())
}

fn cmd_types(store: &StoreProxy, colors: &Colors) -> Result<()> {
    let types = store.get_entity_types()?;

    for entity_type in types.iter() {
        let name = store.resolve_entity_type(*entity_type)?;
        println!("{}{}{} - {}", colors.cyan, entity_type.0, colors.reset, name);
    }
    
    println!();
    println!("{}({} types){}", colors.dim, types.len(), colors.reset);

    Ok(())
}

fn cmd_findpag(store: &StoreProxy, args: &[&str], colors: &Colors) -> Result<()> {
    if args.is_empty() {
        return Err(anyhow::anyhow!("Usage: FINDPAG <entity_type> [limit] [cursor] [filter]"));
    }

    let entity_type = store.get_entity_type(args[0])?;
    
    let (page_opts, filter_start) = if args.len() >= 3 && args[1].parse::<usize>().is_ok() {
        let limit: usize = args[1].parse().context("Invalid limit")?;
        let cursor: usize = args[2].parse().context("Invalid cursor")?;
        (Some(qlib_rs::PageOpts { limit, cursor: Some(cursor) }), 3)
    } else if args.len() >= 2 && args[1].parse::<usize>().is_ok() {
        let limit: usize = args[1].parse().context("Invalid limit")?;
        (Some(qlib_rs::PageOpts { limit, cursor: None }), 2)
    } else {
        (None, 1)
    };

    let filter = if args.len() > filter_start {
        Some(args[filter_start..].join(" "))
    } else {
        None
    };

    let result = store.find_entities_paginated(entity_type, page_opts.as_ref(), filter.as_deref())?;

    for (i, entity_id) in result.items.iter().enumerate() {
        let name_display = get_entity_name(store, *entity_id).unwrap_or_default();
        if !name_display.is_empty() {
            println!("{}{}{} {} ({})", colors.dim, i + 1, colors.reset, entity_id.0, name_display);
        } else {
            println!("{}{}{} {}", colors.dim, i + 1, colors.reset, entity_id.0);
        }
    }
    
    println!();
    println!("{}({} of {} entities){}", colors.dim, result.items.len(), result.total, colors.reset);
    if let Some(next) = result.next_cursor {
        println!("{}Next cursor: {}{}", colors.dim, next, colors.reset);
    }

    Ok(())
}

fn cmd_findex(store: &StoreProxy, args: &[&str], colors: &Colors) -> Result<()> {
    if args.is_empty() {
        return Err(anyhow::anyhow!("Usage: FINDEX <entity_type> [limit] [cursor] [filter]"));
    }

    let entity_type = store.get_entity_type(args[0])?;
    
    let (page_opts, filter_start) = if args.len() >= 3 && args[1].parse::<usize>().is_ok() {
        let limit: usize = args[1].parse().context("Invalid limit")?;
        let cursor: usize = args[2].parse().context("Invalid cursor")?;
        (Some(qlib_rs::PageOpts { limit, cursor: Some(cursor) }), 3)
    } else if args.len() >= 2 && args[1].parse::<usize>().is_ok() {
        let limit: usize = args[1].parse().context("Invalid limit")?;
        (Some(qlib_rs::PageOpts { limit, cursor: None }), 2)
    } else {
        (None, 1)
    };

    let filter = if args.len() > filter_start {
        Some(args[filter_start..].join(" "))
    } else {
        None
    };

    let result = store.find_entities_exact(entity_type, page_opts.as_ref(), filter.as_deref())?;

    for (i, entity_id) in result.items.iter().enumerate() {
        let name_display = get_entity_name(store, *entity_id).unwrap_or_default();
        if !name_display.is_empty() {
            println!("{}{}{} {} ({})", colors.dim, i + 1, colors.reset, entity_id.0, name_display);
        } else {
            println!("{}{}{} {}", colors.dim, i + 1, colors.reset, entity_id.0);
        }
    }
    
    println!();
    println!("{}({} of {} entities - exact match only){}", colors.dim, result.items.len(), result.total, colors.reset);
    if let Some(next) = result.next_cursor {
        println!("{}Next cursor: {}{}", colors.dim, next, colors.reset);
    }

    Ok(())
}

fn cmd_typepag(store: &StoreProxy, args: &[&str], colors: &Colors) -> Result<()> {
    let page_opts = if args.len() >= 2 {
        let limit: usize = args[0].parse().context("Invalid limit")?;
        let cursor: usize = args[1].parse().context("Invalid cursor")?;
        Some(qlib_rs::PageOpts { limit, cursor: Some(cursor) })
    } else if args.len() == 1 {
        let limit: usize = args[0].parse().context("Invalid limit")?;
        Some(qlib_rs::PageOpts { limit, cursor: None })
    } else {
        None
    };

    let result = store.get_entity_types_paginated(page_opts.as_ref())?;

    for entity_type in result.items.iter() {
        let name = store.resolve_entity_type(*entity_type)?;
        println!("{}{}{} - {}", colors.cyan, entity_type.0, colors.reset, name);
    }
    
    println!();
    println!("{}({} of {} types){}", colors.dim, result.items.len(), result.total, colors.reset);
    if let Some(next) = result.next_cursor {
        println!("{}Next cursor: {}{}", colors.dim, next, colors.reset);
    }

    Ok(())
}

fn cmd_getsch(store: &StoreProxy, args: &[&str], colors: &Colors) -> Result<()> {
    if args.is_empty() {
        return Err(anyhow::anyhow!("Usage: GETSCH <entity_type>"));
    }

    let entity_type = store.get_entity_type(args[0])?;
    let schema = store.get_entity_schema(entity_type)?;

    println!("{}Entity Type:{} {}", colors.bold, colors.reset, args[0]);
    println!("{}Inherits:{} {}", colors.dim, colors.reset, 
        if schema.inherit.is_empty() {
            "None".to_string()
        } else {
            schema.inherit.iter()
                .map(|et| store.resolve_entity_type(*et).unwrap_or_else(|_| format!("{}", et.0)))
                .collect::<Vec<_>>()
                .join(", ")
        });
    
    println!();
    println!("{}Fields:{}", colors.bold, colors.reset);
    
    for (field_type, field_schema) in schema.fields.iter() {
        let field_name = store.resolve_field_type(*field_type).unwrap_or_else(|_| format!("{}", field_type.0));
        let variant_name = match field_schema {
            qlib_rs::FieldSchema::Blob { .. } => "Blob",
            qlib_rs::FieldSchema::Bool { .. } => "Bool",
            qlib_rs::FieldSchema::Choice { .. } => "Choice",
            qlib_rs::FieldSchema::EntityList { .. } => "EntityList",
            qlib_rs::FieldSchema::EntityReference { .. } => "EntityReference",
            qlib_rs::FieldSchema::Float { .. } => "Float",
            qlib_rs::FieldSchema::Int { .. } => "Int",
            qlib_rs::FieldSchema::String { .. } => "String",
            qlib_rs::FieldSchema::Timestamp { .. } => "Timestamp",
        };
        println!("  {}{}{}", colors.cyan, field_name, colors.reset);
        println!("    {}Type:{} {}", colors.dim, colors.reset, variant_name);
        let default = field_schema.default_value();
        println!("    {}Default:{} {}", colors.dim, colors.reset, format_value(&default, colors));
        println!("    {}Rank:{} {}", colors.dim, colors.reset, field_schema.rank());
    }

    Ok(())
}

fn cmd_getcsch(store: &StoreProxy, args: &[&str], colors: &Colors) -> Result<()> {
    if args.is_empty() {
        return Err(anyhow::anyhow!("Usage: GETCSCH <entity_type>"));
    }

    let entity_type = store.get_entity_type(args[0])?;
    let schema = store.get_complete_entity_schema(entity_type)?;

    println!("{}Complete Entity Schema:{} {}", colors.bold, colors.reset, args[0]);
    println!("{}Inherits:{} {}", colors.dim, colors.reset, if schema.inherit.is_empty() {
            "None".to_string()
        } else {
            schema.inherit.iter()
                .map(|et| store.resolve_entity_type(*et).unwrap_or_else(|_| format!("{}", et.0)))
                .collect::<Vec<_>>()
                .join(", ")
        });
    
    println!();
    println!("{}All Fields (including inherited):{}", colors.bold, colors.reset);
    
    for (field_type, field_schema) in schema.fields.iter() {
        let field_name = store.resolve_field_type(*field_type).unwrap_or_else(|_| format!("{}", field_type.0));
        let variant_name = match field_schema {
            qlib_rs::FieldSchema::Blob { .. } => "Blob",
            qlib_rs::FieldSchema::Bool { .. } => "Bool",
            qlib_rs::FieldSchema::Choice { .. } => "Choice",
            qlib_rs::FieldSchema::EntityList { .. } => "EntityList",
            qlib_rs::FieldSchema::EntityReference { .. } => "EntityReference",
            qlib_rs::FieldSchema::Float { .. } => "Float",
            qlib_rs::FieldSchema::Int { .. } => "Int",
            qlib_rs::FieldSchema::String { .. } => "String",
            qlib_rs::FieldSchema::Timestamp { .. } => "Timestamp",
        };
        println!("  {}{}{}", colors.cyan, field_name, colors.reset);
        println!("    {}Type:{} {}", colors.dim, colors.reset, variant_name);
        let default = field_schema.default_value();
        println!("    {}Default:{} {}", colors.dim, colors.reset, format_value(&default, colors));
        println!("    {}Rank:{} {}", colors.dim, colors.reset, field_schema.rank());
    }

    Ok(())
}

fn cmd_setsch(store: &StoreProxy, args: &[&str], colors: &Colors) -> Result<()> {
    if args.is_empty() {
        return Err(anyhow::anyhow!("Usage: SETSCH <json_file_path>"));
    }

    let json_content = std::fs::read_to_string(args[0])
        .context("Failed to read schema file")?;
    
    let schema: qlib_rs::EntitySchema<qlib_rs::Single, String, String> = serde_json::from_str(&json_content)
        .context("Failed to parse schema JSON")?;

    store.update_schema(schema)?;
    
    println!("{}Schema updated{}", colors.green, colors.reset);
    Ok(())
}

fn cmd_getfsch(store: &StoreProxy, args: &[&str], colors: &Colors) -> Result<()> {
    if args.len() < 2 {
        return Err(anyhow::anyhow!("Usage: GETFSCH <entity_type> <field_name>"));
    }

    let entity_type = store.get_entity_type(args[0])?;
    let field_type = store.get_field_type(args[1])?;
    let field_schema = store.get_field_schema(entity_type, field_type)?;

    let variant_name = match &field_schema {
        qlib_rs::FieldSchema::Blob { .. } => "Blob",
        qlib_rs::FieldSchema::Bool { .. } => "Bool",
        qlib_rs::FieldSchema::Choice { .. } => "Choice",
        qlib_rs::FieldSchema::EntityList { .. } => "EntityList",
        qlib_rs::FieldSchema::EntityReference { .. } => "EntityReference",
        qlib_rs::FieldSchema::Float { .. } => "Float",
        qlib_rs::FieldSchema::Int { .. } => "Int",
        qlib_rs::FieldSchema::String { .. } => "String",
        qlib_rs::FieldSchema::Timestamp { .. } => "Timestamp",
    };

    println!("{}Field:{} {} ({})", colors.bold, colors.reset, args[1], field_type.0);
    println!("{}Type:{} {}", colors.bold, colors.reset, variant_name);
    let default = field_schema.default_value();
    println!("{}Default:{} {}", colors.dim, colors.reset, format_value(&default, colors));
    println!("{}Rank:{} {}", colors.dim, colors.reset, field_schema.rank());
    Ok(())
}

fn cmd_setfsch(store: &StoreProxy, args: &[&str], colors: &Colors) -> Result<()> {
    if args.len() < 3 {
        return Err(anyhow::anyhow!("Usage: SETFSCH <entity_type> <field_name> <value_type> [reference_type]"));
    }

    let entity_type = store.get_entity_type(args[0])?;
    let field_type = store.get_field_type(args[1])?;
    
    let field_schema = match args[2].to_uppercase().as_str() {
        "BLOB" | "BINARY" => qlib_rs::FieldSchema::Blob { 
            field_type, 
            default_value: Vec::new(), 
            rank: 0, 
            storage_scope: qlib_rs::StorageScope::Runtime 
        },
        "BOOL" | "BOOLEAN" => qlib_rs::FieldSchema::Bool { 
            field_type, 
            default_value: false, 
            rank: 0, 
            storage_scope: qlib_rs::StorageScope::Runtime 
        },
        "INT" | "INTEGER" => qlib_rs::FieldSchema::Int { 
            field_type, 
            default_value: 0, 
            rank: 0, 
            storage_scope: qlib_rs::StorageScope::Runtime 
        },
        "FLOAT" | "DOUBLE" => qlib_rs::FieldSchema::Float { 
            field_type, 
            default_value: 0.0, 
            rank: 0, 
            storage_scope: qlib_rs::StorageScope::Runtime 
        },
        "STRING" | "STR" => qlib_rs::FieldSchema::String { 
            field_type, 
            default_value: String::new(), 
            rank: 0, 
            storage_scope: qlib_rs::StorageScope::Runtime 
        },
        "TIMESTAMP" | "TIME" => qlib_rs::FieldSchema::Timestamp { 
            field_type, 
            default_value: qlib_rs::Timestamp::UNIX_EPOCH, 
            rank: 0, 
            storage_scope: qlib_rs::StorageScope::Runtime 
        },
        "REFERENCE" | "REF" | "ENTITYREFERENCE" => {
            qlib_rs::FieldSchema::EntityReference { 
                field_type, 
                default_value: None, 
                rank: 0, 
                storage_scope: qlib_rs::StorageScope::Runtime 
            }
        },
        "ENTITYLIST" | "REFLIST" | "REFERENCELIST" => {
            qlib_rs::FieldSchema::EntityList { 
                field_type, 
                default_value: Vec::new(), 
                rank: 0, 
                storage_scope: qlib_rs::StorageScope::Runtime 
            }
        },
        "CHOICE" => qlib_rs::FieldSchema::Choice { 
            field_type, 
            default_value: 0, 
            rank: 0, 
            choices: Vec::new(), 
            storage_scope: qlib_rs::StorageScope::Runtime 
        },
        _ => return Err(anyhow::anyhow!("Unknown value type: {}", args[2])),
    };

    store.set_field_schema(entity_type, field_type, field_schema)?;
    
    println!("{}Field schema updated{}", colors.green, colors.reset);
    Ok(())
}

fn cmd_fexists(store: &StoreProxy, args: &[&str], colors: &Colors) -> Result<()> {
    if args.len() < 2 {
        return Err(anyhow::anyhow!("Usage: FEXISTS <entity_type> <field_name>"));
    }

    let entity_type = store.get_entity_type(args[0])?;
    let field_type = store.get_field_type(args[1])?;
    let exists = store.field_exists(entity_type, field_type);

    println!("{}{}{}", colors.cyan, if exists { "1" } else { "0" }, colors.reset);
    Ok(())
}

fn cmd_resolve(store: &StoreProxy, args: &[&str], colors: &Colors) -> Result<()> {
    if args.len() < 2 {
        return Err(anyhow::anyhow!("Usage: RESOLVE <entity_id> <field1> [field2...]"));
    }

    let entity_id = parse_entity_id(args[0])?;
    let mut field_path = Vec::new();
    for field_name in &args[1..] {
        let field_type = store.get_field_type(field_name)?;
        field_path.push(field_type);
    }

    let (resolved_entity, final_field) = store.resolve_indirection(entity_id, &field_path)?;
    let final_field_name = store.resolve_field_type(final_field)?;

    println!("{}Entity:{} {}", colors.cyan, colors.reset, resolved_entity.0);
    println!("{}Field:{} {} ({})", colors.cyan, colors.reset, final_field_name, final_field.0);
    Ok(())
}

fn cmd_snap(store: &StoreProxy, colors: &Colors) -> Result<()> {
    let snapshot = store.take_snapshot();
    println!("{}Snapshot taken{}", colors.green, colors.reset);
    println!("{}Schemas:{} {}", colors.dim, colors.reset, snapshot.schemas.len());
    println!("{}Entities:{} {}", colors.dim, colors.reset, snapshot.entities.len());
    Ok(())
}

fn cmd_machine(store: &StoreProxy, colors: &Colors) -> Result<()> {
    let machine_id = store.machine_info()?;
    println!("{}\"{}\"{}", colors.yellow, machine_id, colors.reset);
    Ok(())
}

fn cmd_info(store: &StoreProxy, colors: &Colors) -> Result<()> {
    let types = store.get_entity_types()?;
    
    println!("{}# Server{}", colors.bold, colors.reset);
    println!("qcore_version:0.1.0");
    println!("rust_version:{}", option_env!("CARGO_PKG_RUST_VERSION").unwrap_or("unknown"));
    
    println!();
    println!("{}# Stats{}", colors.bold, colors.reset);
    println!("entity_types:{}", types.len());
    
    let mut total_entities = 0;
    for entity_type in types.iter() {
        let entities = store.find_entities(*entity_type, None)?;
        total_entities += entities.len();
    }
    println!("total_entities:{}", total_entities);

    Ok(())
}

fn cmd_listen(store: &StoreProxy, args: &[&str], colors: &Colors, notifications: &mut HashMap<NotifyConfig, (Sender<Notification>, Receiver<Notification>)>) -> Result<()> {
    if args.len() < 2 {
        return Err(anyhow::anyhow!("Usage: LISTEN <target> <field> [CHANGE] [context_fields...]\n  target: @<entity_id>, <entity_id>, or <entity_type>\n  field: field name\n  CHANGE: trigger only on value changes (default: trigger on all writes)\n  context_fields: additional fields to include in notifications"));
    }

    let target = args[0];
    let field_name = args[1];
    
    // Parse optional CHANGE flag and context fields
    let mut trigger_on_change = false;
    let mut context_start_idx = 2;
    
    if args.len() > 2 && args[2].eq_ignore_ascii_case("CHANGE") {
        trigger_on_change = true;
        context_start_idx = 3;
    }
    
    let field_type = store.get_field_type(field_name)?;
    
    // Parse context fields
    let mut context = Vec::new();
    for arg in &args[context_start_idx..] {
        let field_path = parse_field_path(store, arg)?;
        context.push(field_path);
    }

    let config = if target.starts_with('@') {
        // Entity ID with @ prefix
        let entity_id_str = &target[1..];
        let entity_id = parse_entity_id(entity_id_str)?;
        NotifyConfig::EntityId {
            entity_id,
            field_type,
            trigger_on_change,
            context,
        }
    } else if target.parse::<u64>().is_ok() {
        // Entity ID as plain number
        let entity_id = parse_entity_id(target)?;
        NotifyConfig::EntityId {
            entity_id,
            field_type,
            trigger_on_change,
            context,
        }
    } else {
        // Entity type name
        let entity_type = store.get_entity_type(target)?;
        NotifyConfig::EntityType {
            entity_type,
            field_type,
            trigger_on_change,
            context,
        }
    };

    // Check if already listening
    if notifications.contains_key(&config) {
        println!("{}Already listening for this configuration{}", colors.yellow, colors.reset);
        return Ok(());
    }

    // Create channel
    let (sender, receiver) = crossbeam::channel::unbounded();

    // Register notification
    store.register_notification(config.clone(), sender.clone())?;

    // Store sender and receiver
    notifications.insert(config, (sender, receiver));

    println!("{}Listening for notifications{}", colors.green, colors.reset);
    Ok(())
}

fn cmd_unlisten(store: &StoreProxy, args: &[&str], colors: &Colors, notifications: &mut HashMap<NotifyConfig, (Sender<Notification>, Receiver<Notification>)>) -> Result<()> {
    if args.len() < 2 {
        return Err(anyhow::anyhow!("Usage: UNLISTEN <target> <field> [CHANGE] [context_fields...]\n  target: @<entity_id>, <entity_id>, or <entity_type>\n  field: field name\n  CHANGE: trigger only on value changes (default: trigger on all writes)\n  context_fields: additional fields to include in notifications"));
    }

    let target = args[0];
    let field_name = args[1];
    
    // Parse optional CHANGE flag and context fields
    let mut trigger_on_change = false;
    let mut context_start_idx = 2;
    
    if args.len() > 2 && args[2].eq_ignore_ascii_case("CHANGE") {
        trigger_on_change = true;
        context_start_idx = 3;
    }
    
    let field_type = store.get_field_type(field_name)?;
    
    // Parse context fields
    let mut context = Vec::new();
    for arg in &args[context_start_idx..] {
        let field_path = parse_field_path(store, arg)?;
        context.push(field_path);
    }

    let config = if target.starts_with('@') {
        // Entity ID with @ prefix
        let entity_id_str = &target[1..];
        let entity_id = parse_entity_id(entity_id_str)?;
        NotifyConfig::EntityId {
            entity_id,
            field_type,
            trigger_on_change,
            context,
        }
    } else if target.parse::<u64>().is_ok() {
        // Entity ID as plain number
        let entity_id = parse_entity_id(target)?;
        NotifyConfig::EntityId {
            entity_id,
            field_type,
            trigger_on_change,
            context,
        }
    } else {
        // Entity type name
        let entity_type = store.get_entity_type(target)?;
        NotifyConfig::EntityType {
            entity_type,
            field_type,
            trigger_on_change,
            context,
        }
    };

    // Check if listening
    if let Some((sender, _receiver)) = notifications.remove(&config) {
        // Unregister notification
        let _ = store.unregister_notification(&config, &sender);
        println!("{}Stopped listening{}", colors.green, colors.reset);
    } else {
        println!("{}Not currently listening for this configuration{}", colors.yellow, colors.reset);
    }

    Ok(())
}

fn cmd_poll(store: &StoreProxy, args: &[&str], colors: &Colors, notifications: &HashMap<NotifyConfig, (Sender<Notification>, Receiver<Notification>)>) -> Result<()> {
    let interval_ms = if args.is_empty() {
        100
    } else {
        args[0].parse().context("Invalid interval (milliseconds)")?
    };

    println!("{}Polling for notifications every {}ms. Press Ctrl+C to stop.{}", colors.green, interval_ms, colors.reset);
    println!();

    // Set up Ctrl+C handler using a flag
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};
    
    let running = Arc::new(AtomicBool::new(true));
    let r = running.clone();
    
    let handler = move || {
        r.store(false, Ordering::SeqCst);
    };
    
    let handle = unsafe {
        signal_hook::low_level::register(signal_hook::consts::SIGINT, handler)
    }.context("Failed to register signal handler")?;

    while running.load(Ordering::SeqCst) {
        // Process any pending notifications from the store
        let _ = store.process_notifications();
        
        // Process any pending notifications from local receivers
        process_notifications(notifications, colors, store);
        
        // Sleep for the specified interval
        std::thread::sleep(std::time::Duration::from_millis(interval_ms));
    }
    
    // Unregister the signal handler
    signal_hook::low_level::unregister(handle);
    
    println!();
    println!("{}Stopped polling{}", colors.green, colors.reset);
    
    Ok(())
}

fn process_notifications(notifications: &HashMap<NotifyConfig, (Sender<Notification>, Receiver<Notification>)>, colors: &Colors, store: &StoreProxy) {
    for (_config, (_sender, receiver)) in notifications.iter() {
        // Try to receive notifications without blocking
        while let Ok(notification) = receiver.try_recv() {
            let formatted = format_notification(&notification, colors, store);
            println!("{}NOTIFY:{} {}", colors.magenta, colors.reset, formatted);
        }
    }
}

fn format_notification(notification: &Notification, colors: &Colors, store: &StoreProxy) -> String {
    // Resolve entity type name
    let entity_type = notification.current.entity_id.extract_type();
    let entity_type_name = store.resolve_entity_type(entity_type)
        .unwrap_or_else(|_| format!("Unknown({})", entity_type.0));
    
    // Resolve field name (resolve entire field path)
    let field_name = if !notification.current.field_path.is_empty() {
        let field_names: Vec<String> = notification.current.field_path.iter()
            .map(|ft| store.resolve_field_type(*ft)
                .unwrap_or_else(|_| format!("Unknown({})", ft.0)))
            .collect();
        field_names.join("->")
    } else {
        "unknown".to_string()
    };
    
    // Format timestamp
    let timestamp = notification.current.timestamp
        .as_ref()
        .map(|ts| format_timestamp(ts))
        .unwrap_or_else(|| "unknown".to_string());
    
    // Format old and new values
    let prev_val = notification.previous.value.as_ref()
        .map(|v| format_value_with_store(v, colors, Some(store)))
        .unwrap_or_else(|| format!("{}null{}", colors.dim, colors.reset));
    let curr_val = notification.current.value.as_ref()
        .map(|v| format_value_with_store(v, colors, Some(store)))
        .unwrap_or_else(|| format!("{}null{}", colors.dim, colors.reset));
    
    let mut result = format!("{} {} {}\n    {}\n    {} -> {}", 
        entity_type_name,
        notification.current.entity_id.0,
        field_name,
        timestamp,
        prev_val,
        curr_val
    );
    
    // Add context fields
    for (field_path, info) in &notification.context {
        if !field_path.is_empty() {
            let context_field_name = field_path.iter()
                .map(|ft| store.resolve_field_type(*ft)
                    .unwrap_or_else(|_| format!("Unknown({})", ft.0)))
                .collect::<Vec<String>>()
                .join("->");
            let context_value = info.value.as_ref()
                .map(|v| format_value_with_store(v, colors, Some(store)))
                .unwrap_or_else(|| format!("{}null{}", colors.dim, colors.reset));
            result.push_str(&format!("\n    {}: {}", context_field_name, context_value));
        }
    }
    
    result
}

fn print_help(colors: &Colors) {
    println!("{}Available Commands:{}", colors.bold, colors.reset);
    println!();
    
    let commands = [
        ("PING", "Test connection"),
        ("GET <entity_id> [field...]", "Get field value(s) (all fields if none specified)"),
        ("SET <entity_id> <field> <value>", "Set field value"),
        ("CREATE <type> <name> [parent]", "Create new entity"),
        ("DELETE <entity_id>", "Delete entity"),
        ("EXISTS <entity_id>", "Check if entity exists"),
        ("GETTYPE <name>", "Get entity type ID"),
        ("RESTYPE <id>", "Resolve entity type name"),
        ("GETFLD <name>", "Get field type ID"),
        ("RESFLD <id>", "Resolve field type name"),
        ("FIND <type> [filter]", "Find entities"),
        ("FINDPAG <type> [limit] [cursor] [filter]", "Find entities with pagination"),
        ("FINDEX <type> [limit] [cursor] [filter]", "Find entities (exact match, no inheritance)"),
        ("TYPES", "List all entity types"),
        ("TYPEPAG [limit] [cursor]", "List entity types with pagination"),
        ("GETSCH <type>", "Get entity schema"),
        ("GETCSCH <type>", "Get complete entity schema (with inheritance)"),
        ("SETSCH <json_file>", "Update entity schema from JSON file"),
        ("GETFSCH <type> <field>", "Get field schema"),
        ("SETFSCH <type> <field> <value_type>", "Set field schema"),
        ("FEXISTS <type> <field>", "Check if field exists in schema"),
        ("RESOLVE <entity_id> <field1> [field2...]", "Resolve field indirection"),
        ("LISTEN <target> <field> [CHANGE] [ctx...]", "Listen for field changes"),
        ("UNLISTEN <target> <field> [CHANGE] [ctx...]", "Stop listening for field changes"),
        ("POLL [interval_ms]", "Poll for notifications continuously"),
        ("SNAP", "Take snapshot"),
        ("MACHINE", "Get machine ID/name"),
        ("INFO", "Server information"),
        ("HELP", "Show this help"),
        ("CLEAR", "Clear screen"),
        ("HISTORY", "Show command history"),
        ("EXIT/QUIT", "Exit CLI"),
    ];

    for (cmd, desc) in commands.iter() {
        println!("  {}{:40}{} {}", colors.cyan, cmd, colors.reset, desc);
    }

    println!();
    println!("{}Examples:{}", colors.bold, colors.reset);
    println!("  {}GET 12345 Name{}", colors.dim, colors.reset);
    println!("  {}GET 12345 Name Age{}", colors.dim, colors.reset);
    println!("  {}GET 12345{} (get all fields)", colors.dim, colors.reset);
    println!("  {}SET 12345 Name \"John Doe\"{}", colors.dim, colors.reset);
    println!("  {}CREATE User \"john@example.com\"{}", colors.dim, colors.reset);
    println!("  {}FIND User \"Age > 25\"{}", colors.dim, colors.reset);
    println!("  {}LISTEN @12345 Name{}", colors.dim, colors.reset);
    println!("  {}LISTEN 12345 Name{}", colors.dim, colors.reset);
    println!("  {}LISTEN User Email CHANGE{}", colors.dim, colors.reset);
    println!("  {}LISTEN @12345 Status Age Name{}", colors.dim, colors.reset);
    println!("  {}POLL{}", colors.dim, colors.reset);
    println!("  {}POLL 500{}", colors.dim, colors.reset);
}

// Helper functions

fn get_entity_name(store: &StoreProxy, entity_id: EntityId) -> Result<String> {
    // Try to read the Name field
    if let Ok(name_field) = store.get_field_type("Name") {
        if let Ok((value, _, _)) = store.read(entity_id, &[name_field]) {
            if let Value::String(s) = value {
                return Ok(s.as_str().to_string());
            }
        }
    }
    Ok(String::new())
}

fn parse_entity_id(s: &str) -> Result<EntityId> {
    let id: u64 = s.parse()
        .context("Invalid entity ID")?;
    Ok(EntityId(id))
}

fn parse_field_path(store: &StoreProxy, s: &str) -> Result<Vec<FieldType>> {
    let parts: Vec<&str> = s.split("->").collect();
    let mut field_types = Vec::new();
    
    for part in parts {
        let field_type = store.get_field_type(part)?;
        field_types.push(field_type);
    }
    
    Ok(field_types)
}

fn parse_value(s: &str) -> Result<Value> {
    // Try to parse as different types
    
    // Boolean
    if s.eq_ignore_ascii_case("true") {
        return Ok(Value::Bool(true));
    }
    if s.eq_ignore_ascii_case("false") {
        return Ok(Value::Bool(false));
    }
    
    // Null
    if s.eq_ignore_ascii_case("null") || s.eq_ignore_ascii_case("nil") {
        return Ok(Value::EntityReference(None));
    }
    
    // Integer
    if let Ok(i) = s.parse::<i64>() {
        return Ok(Value::Int(i));
    }
    
    // Float
    if let Ok(f) = s.parse::<f64>() {
        return Ok(Value::Float(f));
    }
    
    // Entity reference (starts with @)
    if s.starts_with('@') {
        let id: u64 = s[1..].parse()
            .context("Invalid entity reference")?;
        return Ok(Value::EntityReference(Some(EntityId(id))));
    }
    
    // String (remove quotes if present)
    let s = if (s.starts_with('"') && s.ends_with('"')) || (s.starts_with('\'') && s.ends_with('\'')) {
        &s[1..s.len()-1]
    } else {
        s
    };
    
    Ok(Value::String(s.to_string().into()))
}

fn format_value(value: &Value, colors: &Colors) -> String {
    format_value_with_store(value, colors, None)
}

fn format_value_with_store(value: &Value, colors: &Colors, store: Option<&StoreProxy>) -> String {
    match value {
        Value::String(s) => format!("{}\"{}\"{}", colors.yellow, s.as_str(), colors.reset),
        Value::Int(i) => format!("{}{}{}", colors.cyan, i, colors.reset),
        Value::Float(f) => format!("{}{}{}", colors.cyan, f, colors.reset),
        Value::Bool(b) => format!("{}{}{}", colors.cyan, b, colors.reset),
        Value::EntityReference(Some(e)) => {
            if let Some(store) = store {
                if let Ok(name) = get_entity_name(store, *e) {
                    if !name.is_empty() {
                        return format!("{}@{}{} ({})", colors.magenta, e.0, colors.reset, name);
                    }
                }
            }
            format!("{}@{}{}", colors.magenta, e.0, colors.reset)
        }
        Value::EntityReference(None) => format!("{}null{}", colors.dim, colors.reset),
        Value::EntityList(list) => {
            let items: Vec<String> = list.iter()
                .map(|e| {
                    if let Some(store) = store {
                        if let Ok(name) = get_entity_name(store, *e) {
                            if !name.is_empty() {
                                return format!("@{} ({})", e.0, name);
                            }
                        }
                    }
                    format!("@{}", e.0)
                })
                .collect();
            format!("{}[{}]{}", colors.magenta, items.join(", "), colors.reset)
        }
        Value::Choice(c) => format!("{}{}{}", colors.cyan, c, colors.reset),
        Value::Timestamp(ts) => format!("{}{}{}", colors.cyan, format_timestamp(ts), colors.reset),
        Value::Blob(data) => {
            if data.len() <= 32 {
                format!("{}blob[{}]: {}{}", 
                    colors.blue, data.len(), 
                    general_purpose::STANDARD.encode(data.as_slice()),
                    colors.reset)
            } else {
                format!("{}blob[{}]: {}...{}", 
                    colors.blue, data.len(),
                    general_purpose::STANDARD.encode(&data.as_slice()[..32]),
                    colors.reset)
            }
        }
    }
}

fn format_timestamp(ts: &Timestamp) -> String {
    ts.format(&time::format_description::well_known::Rfc3339)
        .unwrap_or_else(|_| format!("{}", ts.unix_timestamp()))
}

fn format_duration(d: &std::time::Duration) -> String {
    let nanos = d.as_nanos();
    
    if nanos < 1_000 {
        format!("{}ns", nanos)
    } else if nanos < 1_000_000 {
        format!("{:.1}μs", nanos as f64 / 1_000.0)
    } else if nanos < 1_000_000_000 {
        format!("{:.1}ms", nanos as f64 / 1_000_000.0)
    } else {
        format!("{:.2}s", d.as_secs_f64())
    }
}

// Rustyline helper for tab completion, hints, and history
struct QCoreHelper {
    commands: Vec<String>,
    hinter: HistoryHinter,
    highlighter: MatchingBracketHighlighter,
}

impl QCoreHelper {
    fn new() -> Self {
        let commands = vec![
            "PING", "GET", "SET", "CREATE", "DELETE", "DEL", "EXISTS",
            "GETTYPE", "RESTYPE", "GETFLD", "RESFLD", "FIND", "TYPES",
            "GETSCH", "LISTEN", "UNLISTEN", "POLL", "SNAP", "MACHINE", "INFO", "HELP", "CLEAR", "CLS", "HISTORY",
            "EXIT", "QUIT"
        ].into_iter().map(String::from).collect();

        QCoreHelper {
            commands,
            hinter: HistoryHinter::new(),
            highlighter: MatchingBracketHighlighter::new(),
        }
    }
}

impl Completer for QCoreHelper {
    type Candidate = Pair;

    fn complete(
        &self,
        line: &str,
        pos: usize,
        _ctx: &RustyContext<'_>,
    ) -> RustyResult<(usize, Vec<Pair>)> {
        let line_up_to_pos = &line[..pos];
        
        // Find the start of the current word
        let start = line_up_to_pos.rfind(' ').map(|i| i + 1).unwrap_or(0);
        let word = &line_up_to_pos[start..];
        
        // If we're completing the first word (command)
        if !line_up_to_pos[..start].trim().is_empty() {
            return Ok((start, vec![]));
        }
        
        let word_upper = word.to_uppercase();
        let matches: Vec<Pair> = self.commands.iter()
            .filter(|cmd| cmd.starts_with(&word_upper))
            .map(|cmd| Pair {
                display: cmd.clone(),
                replacement: cmd.clone(),
            })
            .collect();

        Ok((start, matches))
    }
}

impl Hinter for QCoreHelper {
    type Hint = String;

    fn hint(&self, line: &str, pos: usize, ctx: &RustyContext<'_>) -> Option<String> {
        self.hinter.hint(line, pos, ctx)
    }
}

impl Highlighter for QCoreHelper {
    fn highlight<'l>(&self, line: &'l str, pos: usize) -> Cow<'l, str> {
        self.highlighter.highlight(line, pos)
    }

    fn highlight_char(&self, line: &str, pos: usize, forced: bool) -> bool {
        self.highlighter.highlight_char(line, pos, forced)
    }
}

impl Validator for QCoreHelper {
    fn validate(&self, _ctx: &mut ValidationContext) -> RustyResult<ValidationResult> {
        Ok(ValidationResult::Valid(None))
    }
}

impl Helper for QCoreHelper {}

use anyhow::{Context, Result};
use clap::Parser;
use qlib_rs::{EntityId, EntityType, FieldType, StoreProxy, Value, Timestamp};
use tracing::{info, debug};
use base64::{Engine as _, engine::general_purpose};
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
        for i in 0..config.repeat {
            if i > 0 && config.interval > 0 {
                std::thread::sleep(std::time::Duration::from_millis(config.interval));
            }
            execute_command(&store, cmd, &config, &colors)?;
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
                match execute_command(store, input, config, colors) {
                    Ok(_) => {}
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

        execute_command(store, line, config, colors)?;
    }

    Ok(())
}

fn execute_command(store: &StoreProxy, input: &str, config: &Config, colors: &Colors) -> Result<()> {
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
        "TYPES" => cmd_types(store, colors),
        "GETSCH" => cmd_getsch(store, args, colors),
        "SNAP" => cmd_snap(store, colors),
        "INFO" => cmd_info(store, colors),
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
    if args.len() < 2 {
        return Err(anyhow::anyhow!("Usage: GET <entity_id> <field_path>"));
    }

    let entity_id = parse_entity_id(args[0])?;
    let field_path = parse_field_path(store, args[1])?;

    let (value, timestamp, writer_id) = store.read(entity_id, &field_path)?;

    println!("{}", format_value(&value, colors));
    
    if writer_id.is_some() || timestamp.unix_timestamp() > 0 {
        println!("{}  Timestamp:{} {}", colors.dim, colors.reset, format_timestamp(&timestamp));
        if let Some(writer) = writer_id {
            println!("{}  Writer:{} {}", colors.dim, colors.reset, writer.0);
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

    println!("{}{}{}", colors.cyan, entity_id.0, colors.reset);
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
        println!("{}{}{} {}", colors.dim, i + 1, colors.reset, entity_id.0);
    }
    
    println!();
    println!("{}({} entities){}", colors.dim, entities.len(), colors.reset);

    Ok(())
}

fn cmd_types(store: &StoreProxy, colors: &Colors) -> Result<()> {
    let types = store.get_entity_types()?;

    for entity_type in types.iter() {
        let name = store.resolve_entity_type(*entity_type)?;
        println!("{}{:6}{} {}", colors.cyan, entity_type.0, colors.reset, name);
    }
    
    println!();
    println!("{}({} types){}", colors.dim, types.len(), colors.reset);

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

fn cmd_snap(store: &StoreProxy, colors: &Colors) -> Result<()> {
    let snapshot = store.take_snapshot();
    println!("{}Snapshot taken{}", colors.green, colors.reset);
    println!("{}Schemas:{} {}", colors.dim, colors.reset, snapshot.schemas.len());
    println!("{}Entities:{} {}", colors.dim, colors.reset, snapshot.entities.len());
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

fn print_help(colors: &Colors) {
    println!("{}Available Commands:{}", colors.bold, colors.reset);
    println!();
    
    let commands = [
        ("PING", "Test connection"),
        ("GET <entity_id> <field>", "Get field value"),
        ("SET <entity_id> <field> <value>", "Set field value"),
        ("CREATE <type> <name> [parent]", "Create new entity"),
        ("DELETE <entity_id>", "Delete entity"),
        ("EXISTS <entity_id>", "Check if entity exists"),
        ("GETTYPE <name>", "Get entity type ID"),
        ("RESTYPE <id>", "Resolve entity type name"),
        ("GETFLD <name>", "Get field type ID"),
        ("RESFLD <id>", "Resolve field type name"),
        ("FIND <type> [filter]", "Find entities"),
        ("TYPES", "List all entity types"),
        ("GETSCH <type>", "Get entity schema"),
        ("SNAP", "Take snapshot"),
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
    println!("  {}SET 12345 Name \"John Doe\"{}", colors.dim, colors.reset);
    println!("  {}CREATE User \"john@example.com\"{}", colors.dim, colors.reset);
    println!("  {}FIND User \"Age > 25\"{}", colors.dim, colors.reset);
}

// Helper functions

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
    match value {
        Value::String(s) => format!("{}\"{}\"{}", colors.yellow, s.as_str(), colors.reset),
        Value::Int(i) => format!("{}{}{}", colors.cyan, i, colors.reset),
        Value::Float(f) => format!("{}{}{}", colors.cyan, f, colors.reset),
        Value::Bool(b) => format!("{}{}{}", colors.cyan, b, colors.reset),
        Value::EntityReference(Some(e)) => format!("{}@{}{}", colors.magenta, e.0, colors.reset),
        Value::EntityReference(None) => format!("{}null{}", colors.dim, colors.reset),
        Value::EntityList(list) => {
            let items: Vec<String> = list.iter()
                .map(|e| format!("@{}", e.0))
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
            "GETSCH", "SNAP", "INFO", "HELP", "CLEAR", "CLS", "HISTORY",
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

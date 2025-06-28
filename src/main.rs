use clap::Parser;
use qlib_rs::{Context, EntitySchema, FieldSchema, FieldType, Single, Value};
use std::fs::File;
use std::io::Read;
use std::path::PathBuf;
use serde::{Deserialize, Serialize};

mod api;
mod app;
mod management;
mod network;
mod raft;
mod store;

use std::sync::Arc;

use actix_web::{
    HttpServer,
    middleware::{self, Logger},
    web::Data,
};
use openraft::Config;

use crate::{app::App, network::Network, store::{LogStore, StateMachineStore}};

#[derive(Parser, Clone, Debug)]
#[clap(author, version, about, long_about = None)]
pub struct Opt {
    #[clap(long)]
    pub id: u64,

    #[clap(long)]
    pub http_addr: String,

    #[clap(long, help = "Path to the YAML schema configuration file")]
    pub config_file: Option<PathBuf>,
}

#[derive(Debug, Serialize, Deserialize)]
struct YamlFieldSchema {
    default_value: YamlValue,
    rank: i64,
    read_permission: Option<String>,
    write_permission: Option<String>,
    choices: Option<Vec<String>>,
}

#[derive(Debug, Serialize, Deserialize)]
enum YamlValue {
    Bool(bool),
    Int(i64),
    Float(f64),
    String(String),
    #[serde(rename = "EntityReference")]
    EntityReference(Option<String>),
    #[serde(rename = "EntityList")]
    EntityList(Vec<String>),
    #[serde(rename = "Choice")]
    Choice(i64),
}

#[derive(Debug, Serialize, Deserialize)]
struct YamlEntitySchema {
    entity_type: String,
    inherit: Option<String>,
    fields: std::collections::HashMap<String, YamlFieldSchema>,
}

#[derive(Debug, Serialize, Deserialize)]
struct YamlSchemaConfig {
    schemas: Vec<YamlEntitySchema>,
}

impl From<YamlValue> for Value {
    fn from(value: YamlValue) -> Self {
        match value {
            YamlValue::Bool(b) => Value::Bool(b),
            YamlValue::Int(i) => Value::Int(i),
            YamlValue::Float(f) => Value::Float(f),
            YamlValue::String(s) => Value::String(s),
            YamlValue::EntityReference(e) => Value::EntityReference(e.and_then(|id| qlib_rs::EntityId::try_from(id.as_str()).ok())),
            YamlValue::EntityList(list) => Value::EntityList(list.into_iter()
                .filter_map(|id| qlib_rs::EntityId::try_from(id.as_str()).ok())
                .collect()),
            YamlValue::Choice(c) => Value::Choice(c),
        }
    }
}

fn load_schemas_from_yaml(path: &PathBuf) -> Result<Vec<EntitySchema<Single>>, Box<dyn std::error::Error>> {
    let mut file = File::open(path)?;
    let mut contents = String::new();
    file.read_to_string(&mut contents)?;
    
    let config: YamlSchemaConfig = serde_yaml::from_str(&contents)?;
    
    let mut schemas = Vec::new();
    
    for yaml_schema in config.schemas {
        let mut schema = EntitySchema::<Single>::new(
            yaml_schema.entity_type.clone(),
            match yaml_schema.inherit {
                Some(parent) => Some(parent.into()),
                None => None,
            }
        );
        
        for (field_name, yaml_field) in yaml_schema.fields {
            let field_type: FieldType = field_name.clone().into();
            
            schema.fields.insert(field_name.into(), FieldSchema {
                field_type,
                default_value: yaml_field.default_value.into(),
                rank: yaml_field.rank,
                read_permission: yaml_field.read_permission.and_then(|id| qlib_rs::EntityId::try_from(id.as_str()).ok()),
                write_permission: yaml_field.write_permission.and_then(|id| qlib_rs::EntityId::try_from(id.as_str()).ok()),
                choices: yaml_field.choices,
            });
        }
        
        schemas.push(schema);
    }
    
    Ok(schemas)
}

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    // Initialize logging
    env_logger::init_from_env(env_logger::Env::new().default_filter_or("info"));
    
    let options = Opt::parse();
    let node_id = options.id;
    let http_addr = options.http_addr;
    
    // Create a configuration for the raft instance.
    let config = Config {
        heartbeat_interval: 500,
        election_timeout_min: 1500,
        election_timeout_max: 3000,
        ..Default::default()
    };

    let config = Arc::new(config.validate().unwrap());

    // Create a instance of where the Raft logs will be stored.
    let log_store = LogStore::default();
    // Create a instance of where the Raft data will be stored.
    let state_machine_store = Arc::new(StateMachineStore::default());

    {
        let mut store = state_machine_store.state_machine.write().await;

        let ctx = Context {};
        
        // Load schemas from YAML if config file is provided, otherwise use defaults
        let schemas = if let Some(config_path) = options.config_file {
            match load_schemas_from_yaml(&config_path) {
                Ok(schemas) => {
                    log::info!("Successfully loaded {} schemas from config file", schemas.len());
                    schemas
                },
                Err(err) => {
                    log::error!("Failed to load schemas from config file: {}", err);
                }
            }
        } else {
            log::error!("No config file provided, using default schemas");
        }
        
        // Apply the schemas to the store
        for schema in schemas {
            log::info!("Setting entity schema: {}", schema.entity_type);
            store.data.set_entity_schema(&ctx, &schema).expect("Failed to set entity schema");
        }
    }

    // Create the network layer that will connect and communicate the raft instances and
    // will be used in conjunction with the store created above.
    let network = Network {};

    // Create a local raft instance.
    let raft = openraft::Raft::new(
        node_id,
        config.clone(),
        network,
        log_store.clone(),
        state_machine_store.clone(),
    )
    .await
    .unwrap();

    // Create an application that will store all the instances created above, this will
    // later be used on the actix-web services.
    let app_data = Data::new(App {
        id: node_id,
        addr: http_addr.clone(),
        raft,
        state_machine_store,
    });

    // Start the actix-web server.
    let server = HttpServer::new(move || {
        actix_web::App::new()
            .wrap(Logger::default())
            .wrap(Logger::new("%a %{User-Agent}i"))
            .wrap(middleware::Compress::default())
            .app_data(app_data.clone())
            // raft internal RPC
            .service(raft::append)
            .service(raft::snapshot)
            .service(raft::vote)
            // admin API
            .service(management::init)
            .service(management::add_learner)
            .service(management::change_membership)
            .service(management::metrics)
            // application API
            .service(api::perform)
    });

    let x = server.bind(http_addr)?;

    x.run().await
}

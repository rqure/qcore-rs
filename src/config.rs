use std::{fs::File, path::PathBuf};
use std::io::Read;
use qlib_rs::{Context, EntitySchema, FieldSchema, FieldType, Single, Value};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Serialize, Deserialize)]
pub struct YamlFieldSchema {
    default_value: YamlValue,
    rank: i64,
    read_permission: Option<String>,
    write_permission: Option<String>,
    choices: Option<Vec<String>>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(untagged)]
#[allow(non_snake_case)]
pub enum YamlValue {
    Bool(bool),
    Int(i64),
    Float(f64),
    String { String: String },
    EntityReference { EntityReference: Option<String> },
    EntityList { EntityList: Vec<String> },
    Choice { Choice: i64 },
    Blob { Blob: Vec<u8> },
}

#[derive(Debug, Serialize, Deserialize)]
pub struct YamlEntitySchema {
    entity_type: String,
    inherit: Option<String>,
    fields: std::collections::HashMap<String, YamlFieldSchema>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct YamlSchemaConfig {
    schemas: Vec<YamlEntitySchema>,
    tree: Option<Vec<YamlEntityTreeNode>>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct YamlEntityTreeNode {
    entity_type: String,
    name: String,
    children: Option<Vec<YamlEntityTreeNode>>,
    attributes: Option<HashMap<String, YamlValue>>,
}

impl From<YamlValue> for Value {
    fn from(value: YamlValue) -> Self {
        match value {
            YamlValue::Bool(b) => Value::Bool(b),
            YamlValue::Int(i) => Value::Int(i),
            YamlValue::Float(f) => Value::Float(f),
            YamlValue::String { String: s } => Value::String(s),
            YamlValue::EntityReference { EntityReference: e } => Value::EntityReference(e.and_then(|id| qlib_rs::EntityId::try_from(id.as_str()).ok())),
            YamlValue::EntityList { EntityList: list } => Value::EntityList(list.into_iter()
                .filter_map(|id| qlib_rs::EntityId::try_from(id.as_str()).ok())
                .collect()),
            YamlValue::Choice { Choice: c } => Value::Choice(c),
            YamlValue::Blob { Blob: b } => Value::Blob(b),
        }
    }
}

impl YamlFieldSchema {
    fn to_field_schema(&self, field_type: FieldType) -> Result<FieldSchema, Box<dyn std::error::Error>> {
        let read_permission = self.read_permission.as_ref()
            .and_then(|id| qlib_rs::EntityId::try_from(id.as_str()).ok());
        let write_permission = self.write_permission.as_ref()
            .and_then(|id| qlib_rs::EntityId::try_from(id.as_str()).ok());

        let field_schema = match &self.default_value {
            YamlValue::Bool(b) => FieldSchema::Bool {
                field_type,
                default_value: *b,
                rank: self.rank,
                read_permission,
                write_permission,
            },
            YamlValue::Int(i) => FieldSchema::Int {
                field_type,
                default_value: *i,
                rank: self.rank,
                read_permission,
                write_permission,
            },
            YamlValue::Float(f) => FieldSchema::Float {
                field_type,
                default_value: *f,
                rank: self.rank,
                read_permission,
                write_permission,
            },
            YamlValue::String { String: s } => FieldSchema::String {
                field_type,
                default_value: s.clone(),
                rank: self.rank,
                read_permission,
                write_permission,
            },
            YamlValue::EntityReference { EntityReference: e } => FieldSchema::EntityReference {
                field_type,
                default_value: e.as_ref().and_then(|id| qlib_rs::EntityId::try_from(id.as_str()).ok()),
                rank: self.rank,
                read_permission,
                write_permission,
            },
            YamlValue::EntityList { EntityList: list } => FieldSchema::EntityList {
                field_type,
                default_value: list.iter()
                    .filter_map(|id| qlib_rs::EntityId::try_from(id.as_str()).ok())
                    .collect(),
                rank: self.rank,
                read_permission,
                write_permission,
            },
            YamlValue::Choice { Choice: c } => FieldSchema::Choice {
                field_type,
                default_value: *c,
                rank: self.rank,
                read_permission,
                write_permission,
                choices: self.choices.clone().unwrap_or_default(),
            },
            YamlValue::Blob { Blob: b } => FieldSchema::Blob {
                field_type,
                default_value: b.clone(),
                rank: self.rank,
                read_permission,
                write_permission,
            },
        };

        Ok(field_schema)
    }
}

pub fn load_schemas_from_yaml(path: &PathBuf) -> Result<(Vec<EntitySchema<Single>>, Option<Vec<YamlEntityTreeNode>>), Box<dyn std::error::Error>> {
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
            let field_schema = yaml_field.to_field_schema(field_type)?;
            schema.fields.insert(field_name.into(), field_schema);
        }
        
        schemas.push(schema);
    }
    
    // Sort schemas by dependency order - base schemas (no inheritance) first
    schemas.sort_by(|a, b| {
        match (&a.inherit, &b.inherit) {
            (None, None) => std::cmp::Ordering::Equal,
            (None, Some(_)) => std::cmp::Ordering::Less,    // Base schemas first
            (Some(_), None) => std::cmp::Ordering::Greater, // Inherited schemas later
            (Some(_), Some(_)) => std::cmp::Ordering::Equal, // Keep relative order for inherited schemas
        }
    });
    
    Ok((schemas, config.tree))
}

/// Create entities based on the tree definition
pub async fn create_entity_tree(
    store: &mut qlib_rs::Store,
    ctx: &Context,
    tree_nodes: &Vec<YamlEntityTreeNode>,
    parent_id: Option<qlib_rs::EntityId>
) -> Result<Vec<qlib_rs::EntityId>, Box<dyn std::error::Error>> {
    let mut created_entities = Vec::new();
    let mut work_queue = Vec::new();
    
    // Initialize work queue with root nodes
    for node in tree_nodes {
        work_queue.push((node, parent_id.clone()));
    }
    
    // Process nodes iteratively to avoid async recursion issues
    while let Some((node, current_parent_id)) = work_queue.pop() {
        // Create the entity
        let entity = store.create_entity(ctx, &node.entity_type.clone().into(), current_parent_id, &node.name)?;
        let entity_id = entity.entity_id;
        
        // Set additional attributes if specified
        if let Some(attrs) = &node.attributes {
            let mut requests = Vec::new();
            for (field_name, value) in attrs {
                requests.push(qlib_rs::Request::Write {
                    entity_id: entity_id.clone(),
                    field_type: field_name.clone().into(),
                    value: Some(value.clone().into()),
                    push_condition: qlib_rs::PushCondition::Always,
                    adjust_behavior: qlib_rs::AdjustBehavior::Set,
                    write_time: None,
                    writer_id: None,
                });
            }
            if !requests.is_empty() {
                store.perform(ctx, &mut requests)?;
            }
        }
        
        // Add children to work queue for processing
        if let Some(children) = &node.children {
            for child in children {
                work_queue.push((child, Some(entity_id.clone())));
            }
        }
        
        created_entities.push(entity_id);
    }
    
    Ok(created_entities)
}
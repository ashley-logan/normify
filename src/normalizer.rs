use anyhow::Result;
use indexmap::IndexMap;
use polars::{frame::DataFrame, prelude::DataType};
use serde_json::{Map, Value};
use uuid::Uuid;

type Row = Map<String, Value>; // row type is a map of field name to data element

#[derive(Debug)]
pub struct TableData {
    pub records: Vec<Row>, // represents the records as a vector of rows
    schema: IndexMap<String, DataType>,
}
#[derive(Debug)]
pub struct Relationship {
    parent: Option<String>,   // holds the table name of the parent table
    child: Option<String>,    // holds the table name of the child table
    fk_field: Option<String>, // holds the field name of the foreign key
}

#[derive(Debug)]
pub struct NormifyContext {
    pub tables: IndexMap<String, TableData>,
    relationships: Vec<Relationship>,
}

#[derive(Debug)]
pub struct NormfiedData {
    dataframes: IndexMap<String, DataFrame>, // maps dataframe name to DataFrame
    relationships: IndexMap<String, Relationship>, // map dataframe name to Relationship struct
}

impl TableData {
    pub fn new() -> Self {
        TableData {
            records: Vec::new(),
            schema: IndexMap::new(),
        }
    }
    pub fn append_row(&mut self, row: Row) {
        self.records.push(row);
    }
}

impl NormifyContext {
    pub fn new() -> Self {
        NormifyContext {
            tables: IndexMap::new(),
            relationships: Vec::new(),
        }
    }

    pub fn parse_object(
        &mut self,
        table_name: &str,
        obj: &Map<String, Value>,
        id: Option<&Value>,
        id_field_name: Option<&str>,
    ) -> Result<()> {
        println!("Creating table {}", table_name);
        if let (Some(curr_id), Some(id_field)) = (id, id_field_name) {}

        let mut new_row: Row = Map::new();
        let curr_id = Value::String(Uuid::new_v4().to_string());
        new_row.insert("id".to_string(), curr_id.clone());

        if let (Some(pid), Some(ptable)) = (parent_id, parent_table) {
            new_row.insert(format!("{}_id", ptable), pid.clone());
        }

        for (k, v) in obj {
            println!("processing item {}", k);
            // process each value in the object
            match v {
                // if value is primitive type
                Value::Null => {
                    new_row.insert(k.to_string(), Value::Null);
                }
                Value::Bool(b) => {
                    new_row.insert(k.to_string(), Value::Bool(b.clone()));
                }
                Value::Number(n) => {
                    new_row.insert(k.to_string(), Value::Number(n.clone()));
                }
                Value::String(s) => {
                    new_row.insert(k.to_string(), Value::String(s.to_string()));
                }
                Value::Object(child) => {
                    println!("object {} is a new table", k);
                    let child_id: Value = Value::String(Uuid::new_v4().to_string());
                    let child_field: String = format!("{}_id", k);
                    let new_table_name: String = format!("{}_table", k);
                    new_row.insert(child_field, child_id);
                    // recursive call to self with table name as the key
                    self.parse_object(
                        new_table_name.as_str(), // creates a new table name based on the key of the child object
                        child,                   // child object becomes target object
                        Some(child_id),          // current id becomes parent id
                        Some(table_name),        // current table name becomes parent table name
                    )?;
                }
                Value::Array(arr) => {
                    println!("key {}'s value is an array", k);
                    if arr.iter().all(|ele| ele.is_object()) {
                        self.parse_objects_array(
                            table_name,
                            Some(k.to_string()),
                            Some(&curr_id),
                            &arr,
                        )?;
                    } else {
                        self.parse_values_array(&mut new_row, k.to_string(), arr.to_vec())?;
                    }
                }
            };
        }
        let curr_table: &mut TableData = self
            .tables
            .entry(table_name.to_string())
            .or_insert_with(TableData::new);
        curr_table.append_row(new_row);
        Ok(())
    }

    pub fn parse_values_array(
        &mut self,
        curr_row: &mut Row,
        key: String,
        arr: Vec<Value>,
    ) -> Result<()> {
        if arr.is_empty() {
            curr_row.insert(key, Value::Null);
        } else if arr.iter().any(|ele| ele.is_object()) {
            panic!("found an object in an array of values with key {}", key);
        } else {
            curr_row.insert(key, Value::Array(arr));
        }
        Ok(())
    }

    pub fn parse_objects_array(
        &mut self,
        table_name: &str,
        key: Option<String>,
        row_id: Option<&Value>,
        arr: &Vec<Value>,
    ) -> Result<()> {
        if key.is_none() {
            for obj in arr {
                self.parse_object(table_name, obj.as_object().unwrap(), None, None)?;
            }
        } else {
            let new_table_name: String = format!("{}_table", key.unwrap());
            for obj in arr {
                self.parse_object(
                    new_table_name.as_str(),
                    obj.as_object().unwrap(),
                    row_id,
                    Some(table_name),
                )?;
            }
        }
        Ok(())
    }

    pub fn from_value(payload: Value) -> Result<NormifyContext> {
        let mut context: NormifyContext = NormifyContext::new();
        match payload {
            Value::Object(obj) => {
                context.parse_object("ROOT", &obj, None, None)?;
            }
            Value::Array(arr) => {
                context.parse_objects_array("ROOT", None, None, &arr)?;
            }
            _ => {
                panic!("Neither Object nor Array found at root of JSON");
            }
        }
        Ok(context)
    }
}

use serde_json::{Error, Map, Value};
use std::collections::HashMap;
use uuid::Uuid;

type NestedRecords = Vec<Map<String, Value>>; // a table as a list of rows
type UnnestedRecords = HashMap<String, Vec<Value>>; // a table as a map of column names to columns

pub struct TableData {
    rows: NestedRecords,
}

impl TableData {
    pub fn new() -> Self {
        TableData { rows: vec![] }
    }
    pub fn transform(&self) -> UnnestedRecords {
        let mut table: UnnestedRecords = HashMap::new();
        for row in &self.rows {
            for (key, value) in row {
                table
                    .entry(key.to_string())
                    .or_insert(vec![])
                    .push(value.clone());
            }
        }
        table
    }

    pub fn append_row(&mut self, key: String, value: Value) {
        let mut new_row = Map::new();
        new_row.insert(key, value);
        self.rows.push(new_row);
    }
}
#[derive(Debug)]
pub struct Normalizer {
    // a map of table names to unformatted tables
    tables: HashMap<String, UnnestedRecords>, // a map of table names to properly formatted tables
}

impl Normalizer {
    pub fn new() -> Self {
        Normalizer {
            tables: HashMap::new(),
        }
    }

    pub fn append_table(&mut self, table: &TableData, table_name: &str) {
        self.tables
            .insert(table_name.to_string(), table.transform());
    }

    // pub fn append_row(&mut self, row: Map<String, Value>, table_name: &str) {
    //     self.tables
    //         .entry(table_name.to_string())
    //         .or_insert(HashMap::new())
    //         .push(row);

    //     if let Some(table) = self.row_wise_tables.get_mut(table_name) {
    //         // if table already exists in tables, add row to the table
    //         table.push(row);
    //     } else {
    //         // else create a new table with the first row and insert it
    //         self.row_wise_tables
    //             .insert(table_name.to_string(), vec![row]);
    //     }
    // }
    pub fn parse_object(
        &mut self,
        table_name: &str,
        obj: &Map<String, Value>,
        parent_id: Option<&str>,
        parent_table: Option<&str>,
    ) -> Result<(), Error> {
        println!("Creating table {}", table_name);
        let mut new_table = TableData::new();
        let new_id = Uuid::new_v4().to_string();
        new_table.append_row("id".to_string(), Value::String(new_id.to_string()));

        if let (Some(pid), Some(ptable)) = (parent_id, parent_table) {
            new_table.append_row(format!("{}_id", ptable), Value::String(pid.to_string()));
        }
        for (k, v) in obj {
            // process each value in the object
            match v {
                // if value is primitive type
                Value::Null => {
                    new_table.append_row(k.to_string(), Value::Null);
                }
                Value::Bool(b) => {
                    new_table.append_row(k.to_string(), Value::Bool(b.clone()));
                }
                Value::Number(n) => {
                    new_table.append_row(k.to_string(), Value::Number(n.clone()));
                }
                Value::String(s) => {
                    new_table.append_row(k.to_string(), Value::String(s.to_string()));
                }
                Value::Object(child) => {
                    println!("object {} is a new table", k);
                    // recursive call to self with table name as the key
                    self.parse_object(
                        format!("{}_table", k).as_str(), // creates a new table name based on the key of the child object
                        child,                           // child object becomes target object
                        Some(new_id.as_str()),           // current id becomes parent id
                        Some(table_name), // current table name becomes parent table name
                    )?;
                }
                Value::Array(arr) => {
                    println!("key {}'s value is an array", k);
                    self.parse_array(
                        table_name,
                        &mut new_table,
                        k.clone(),
                        arr,
                        parent_id,
                        parent_table,
                    )?;
                }
            };
            self.append_table(&new_table, table_name);
        }
        Ok(())
    }

    pub fn parse_array(
        &mut self,
        table_name: &str,
        curr_table: &mut TableData,
        key: String,
        arr: &Vec<Value>,
        parent_id: Option<&str>,
        parent_table: Option<&str>,
    ) -> Result<(), Error> {
        if arr.is_empty() {
            return Ok(());
        }
        if arr.iter().all(|item| item.is_object()) {
            println!(
                "{} is an array of objects, creating table name {}",
                key, table_name
            );
            let new_name: String = format!("{}_table", key);
            // if array is an array of objects, process each
            for value in arr {
                self.parse_object(
                    new_name.as_str(),
                    value.as_object().unwrap(),
                    parent_id,
                    parent_table,
                )?;
            }
        } else {
            // else insert the array as the value for the row
            curr_table.append_row(key, Value::Array(arr.to_owned()));
        }
        Ok(())
    }

    pub fn normify_json(&mut self, payload: Value) -> Result<(), Error> {
        match payload {
            Value::Object(obj) => {
                self.parse_object("root", &obj, None, None)?;
            }
            Value::Array(arr) => {
                self.parse_array(
                    "root",
                    &mut TableData::new(),
                    "entry-array".to_string(),
                    &arr,
                    None,
                    None,
                )?;
            }
            _ => {
                panic!("Neither Object nor Array found at root of JSON");
            }
        }
        Ok(())
    }

    pub fn get_tables(self) -> HashMap<String, UnnestedRecords> {
        self.tables
    }
}

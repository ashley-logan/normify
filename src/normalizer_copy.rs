use crate::dtype::Dtype;
use anyhow::Result;
use indexmap::IndexMap;
use polars::prelude::*;
use serde_json::{Map, Value};
use uuid::Uuid;

type Row = IndexMap<String, Dtype>; // row type is a map of field name to data element

#[derive(Debug)]
pub struct TableData {
    pub records: Vec<Row>, // represents the records as a vector of rows
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
    pub relationships: Vec<Relationship>,
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
        parent_id: Option<&Dtype>,
        parent_table: Option<&str>,
    ) -> Result<()> {
        println!("Creating table {}", table_name);

        let mut new_row: Row = IndexMap::new();
        let curr_id: Dtype = Uuid::new_v4().to_string().into();
        new_row.insert("id".to_string(), curr_id.clone());

        if let (Some(pid), Some(ptable)) = (parent_id, parent_table) {
            new_row.insert(format!("{}_id", ptable), pid.clone());
        }

        for (k, v) in obj {
            println!("processing item {}", k);
            if let Value::Object(child) = v {
                let new_table: String = format!("{}_table", k);
                self.parse_object(new_table.as_str(), child, Some(&curr_id), Some(table_name))?;
            } else if let Value::Array(arr) = v
                && arr.iter().all(Value::is_object)
            {
                self.parse_objects_array(table_name, Some(k.to_string()), Some(&curr_id), arr)?;
            } else {
                new_row.insert(k.to_string(), v.into());
            }
        }
        let curr_table: &mut TableData = self
            .tables
            .entry(table_name.to_string())
            .or_insert_with(TableData::new);
        curr_table.append_row(new_row);
        Ok(())
    }

    pub fn parse_objects_array(
        &mut self,
        table_name: &str,
        key: Option<String>,
        row_id: Option<&Dtype>,
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

use crate::dtype::Dtype;
use anyhow::Result;
use indexmap::{IndexMap, map::Iter};
use serde_json::{Map, Value};
use std::iter::zip;
use uuid::Uuid;

#[derive(Debug)]
pub struct TableData {
    columns: IndexMap<String, Vec<Dtype>>,
}

#[derive(Debug)]
pub struct Normifier {
    tables: IndexMap<String, TableData>,
    // relations: Vec<Relationship>,
}

impl TableData {
    pub fn new() -> Self {
        Self {
            columns: IndexMap::new(),
        }
    }
    pub fn extend_column(&mut self, col_name: String, col_data: Dtype) {
        self.columns
            .entry(col_name)
            .or_insert_with(Vec::new)
            .push(col_data);
    }

    pub fn iter_columns<'a>(&'a self) -> Iter<'a, String, Vec<Dtype>> {
        self.columns.iter()
    }

    pub fn iter_items<'a>(
        &'a self,
    ) -> std::iter::Flatten<indexmap::map::Values<'a, String, Vec<Dtype>>> {
        self.columns.values().flatten()
    }

    pub fn clean_nulls(&mut self) {
        // keeps all columns that have at least one non-null value
        self.columns.retain(|_, v| v.iter().any(|d| !d.is_null()));
    }
}

impl Normifier {
    pub fn new() -> Self {
        Self {
            tables: IndexMap::new(),
        }
    }

    pub fn update_table(&mut self, table_name: String, record: IndexMap<String, Dtype>) {
        let table: &mut TableData = self.tables.entry(table_name).or_insert_with(TableData::new);
        for (field, data) in record {
            table.extend_column(field, data);
        }
    }
    pub fn iter_tables<'a>(&'a self) -> Iter<'a, String, TableData> {
        self.tables.iter()
    }

    pub fn parse_object(
        &mut self,
        t_name: &String,
        obj: &Map<String, Value>,
        p_id: Option<&String>,
        pt_name: Option<&String>,
    ) -> Result<()> {
        // TODO log table name
        let mut this_record: IndexMap<String, Dtype> = IndexMap::new();
        let this_id = Uuid::now_v7().to_string();
        // this_table.extend_column("id".to_string(), this_id.clone().into());
        this_record.insert("id".to_string(), this_id.clone().into());

        if let (Some(pname), Some(pid)) = (pt_name, p_id) {
            // this_table.extend_column(format!("{}_id", pname), pid.to_string().into());
            this_record.insert(format!("{}_id", pname), pid.to_owned().into());
        }

        for (k, v) in obj {
            match v {
                Value::Array(arr) => {
                    if arr.iter().all(Value::is_object) {
                        let child_table: String = format!("{}_table", k);
                        self.parse_object_array(&child_table, arr, Some(t_name), Some(&this_id))?
                    } else {
                        this_record.insert(k.to_string(), v.to_owned().into());
                    }
                }
                Value::Object(child) => {
                    let new_tname: String = format!("{}_table", k);
                    self.parse_object(&new_tname, child, Some(&this_id), Some(t_name))?;
                }
                _ => {
                    this_record.insert(k.to_string(), v.to_owned().into());
                } // _ => this_table.extend_column(k.to_string(), v.to_owned()),
            }
        }
        self.update_table(t_name.to_owned(), this_record);
        Ok(())
    }

    pub fn parse_object_array(
        &mut self,
        t_name: &String,
        arr: &Vec<Value>,
        p_name: Option<&String>,
        row_id: Option<&String>,
    ) -> Result<()> {
        for obj in arr {
            self.parse_object(t_name, obj.as_object().unwrap(), row_id, p_name)?;
        }
        Ok(())
    }

    pub fn from_value(payload: Value) -> Result<Normifier> {
        let mut norm_context: Normifier = Normifier::new();
        let root_name: &String = &String::from("root_table");
        match payload {
            Value::Object(root_obj) => {
                norm_context.parse_object(root_name, &root_obj, None, None)?;
            }
            Value::Array(arr) => {
                norm_context.parse_object_array(root_name, &arr, None, None)?;
            }
            _ => {
                panic!("Neither Object nor Array found at root of JSON");
            }
        }
        norm_context
            .tables
            .iter_mut()
            .for_each(|(_, t)| t.clean_nulls());
        Ok(norm_context)
    }
}

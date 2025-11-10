use crate::dtype::Dtype;
use anyhow::Result;
use indexmap::{IndexMap, map::Iter};
use serde_json::{Map, Value};
use uuid::Uuid;

#[derive(Debug)]
pub struct TableData {
    pub columns: IndexMap<String, Vec<Dtype>>,
}

#[derive(Debug)]
pub struct Normifier {
    pub tables: IndexMap<String, TableData>,
    // relations: Vec<Relationship>,
}

impl TableData {
    pub fn new() -> Self {
        Self {
            columns: IndexMap::new(),
        }
    }
    pub fn extend_column(&mut self, col_name: String, col_data: Dtype) {
        // pushes a value into its appropriate column vector or creates a new vector
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
        // removes columns that contain entirely null values
        self.columns.retain(|_, v| v.iter().any(|d| !d.is_null()));
    }
}

impl Normifier {
    pub fn new() -> Self {
        Self {
            tables: IndexMap::new(),
        }
    }

    pub fn add_record(&mut self, table_name: String, record: IndexMap<String, Dtype>) {
        // inserts a row of data into its corresponding table
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
        // creates a new index map to hold a row of data
        let mut this_record: IndexMap<String, Dtype> = IndexMap::new();
        // creates a new random id for this row
        let this_id = Uuid::now_v7().to_string();
        // this_table.extend_column("id".to_string(), this_id.clone().into());
        this_record.insert("id".to_string(), this_id.clone().into());

        if let (Some(pname), Some(pid)) = (pt_name, p_id) {
            // if the table this row belongs to has a parent table, insert the parent id as a foreign key
            this_record.insert(format!("{}_id", pname), pid.to_owned().into());
        }

        for (k, v) in obj {
            // iterate through each property and its value
            match v {
                Value::Array(arr) => {
                    // if the value is an array, this signifies the possible creation of a new table,
                    // where the current table has a one-to-many relationship with the new table
                    if arr.iter().all(Value::is_object) {
                        // if every item is an object, this value becomes a new table
                        // new table name created from property name
                        let child_table: String = format!("{}_table", k);
                        self.parse_object_array(&child_table, arr, Some(t_name), Some(&this_id))?
                    } else {
                        // if the array is an array of json primitives, just insert the array into the row container
                        this_record.insert(k.to_string(), Dtype::from_value(v.to_owned()));
                    }
                }
                Value::Object(child) => {
                    // if the value is an object, this is a new table
                    // the current table has a one-to-one relationship with the new table
                    let new_tname: String = format!("{}_table", k);
                    self.parse_object(&new_tname, child, Some(&this_id), Some(t_name))?;
                }
                _ => {
                    // if the type if non-nested, just insert it into the row container
                    this_record.insert(k.to_string(), Dtype::from_value(v.to_owned()));
                } // _ => this_table.extend_column(k.to_string(), v.to_owned()),
            }
        }
        // transform and add the row container to the current table
        self.add_record(t_name.to_owned(), this_record);
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
            // parse each object in the array
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

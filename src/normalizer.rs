use crate::dtype::Dtype;
use anyhow::Result;
use arrow_array::{Array, ArrowPrimitiveType, builder::ArrayBuilder};
use indexmap::IndexMap;
use serde_json::{Map, Number, Value};
use std::cell::RefCell;
use std::mem;
use uuid::Uuid;

#[derive(Debug)]
pub struct TableData {
    columns: IndexMap<String, Vec<Dtype>>,
    relationship: Relationship,
}

#[derive(Debug)]
pub struct Normifier {
    tables: RefCell<IndexMap<String, TableData>>,
    relations: RefCell<IndexMap<String, Relationship>>,
}

pub enum RelationType {
    OneToMany,
    OneToOne,
    ManyToMany,
}

#[derive(Debug)]
pub struct Relationship {
    parent: Option<String>, // holds the table name of the parent table
    child: Option<String>,  // holds the table name of the child table
}
impl Relationship {
    pub fn new() -> Self {
        Self {
            parent: None,
            child: None,
        }
    }

    pub fn add_parent(&mut self, parent: String) -> Option<String> {
        let old_parent = mem::replace(&mut self.parent, Some(parent));
        old_parent
    }

    pub fn add_child(&mut self, child: String) -> Option<String> {
        let old_child = mem::replace(&mut self.child, Some(child));
        old_child
    }

    pub fn add_relation(&mut self, parent: String, child: String) -> Option<(String, String)> {
        let (p, c) = (self.add_parent(parent), self.add_child(child));
        p.zip(c)
    }
}

impl TableData {
    pub fn new() -> Self {
        Self {
            columns: IndexMap::new(),
            relationship: Relationship::new(),
        }
    }
    pub fn extend_column(&mut self, col_name: String, col_data: Value) {
        self.columns
            .entry(col_name)
            .or_insert_with(Vec::new)
            .push(col_data.into());
    }
}

impl Normifier {
    pub fn new() -> Self {
        Self {
            tables: RefCell::new(IndexMap::new()),
            relations: RefCell::new(IndexMap::new()),
        }
    }
    pub fn parse_object(
        &self,
        t_name: &String,
        obj: &Map<String, Value>,
        p_id: Option<&String>,
        pt_name: Option<&String>,
    ) -> Result<()> {
        // TODO log table name
        let mut tbls = self.tables.borrow_mut();
        let this_table = tbls.entry(t_name.clone()).or_insert_with(TableData::new); // get mutable reference to working table
        let this_id = Uuid::new_v4().to_string();
        this_table.extend_column("id".to_string(), this_id.clone().into());

        if let (Some(pname), Some(pid)) = (pt_name, p_id) {
            this_table.extend_column(format!("{}_id", pname), pid.to_string().into());
        }

        for (k, v) in obj {
            match v {
                Value::Array(arr) => {
                    if arr.iter().all(Value::is_object) {
                        let child_table: String = format!("{}_table", k);
                        this_table
                            .relationship
                            .add_relation(t_name.to_owned(), child_table.clone());
                        self.parse_object_array(
                            child_table,
                            Some(k.to_string()),
                            &arr,
                            t_name.to_owned(),
                            Some(this_id.clone()),
                        )?
                    } else {
                        this_table.extend_column(k.to_string(), v.to_owned());
                    }
                }
                Value::Object(child) => {
                    let new_tname: String = format!("{}_table", k);
                    this_table
                        .relationship
                        .add_relation(t_name.to_owned(), new_tname.to_owned());
                    self.parse_object(&new_tname, child, Some(&this_id), Some(t_name))?;
                }
                _ => this_table.extend_column(k.to_string(), v.to_owned()),
            }
        }
        Ok(())
    }

    pub fn parse_object_array(
        &self,
        t_name: String,
        key: Option<String>,
        arr: &Vec<Value>,
        p_name: String,
        row_id: Option<String>,
    ) -> Result<()> {
        if let Some(k) = key {
            for obj in arr {
                self.parse_object(t_name, obj.as_object().unwrap(), row_id, Some(p_name))?;
            }
        } else {
            for obj in arr {
                self.parse_object(t_name, obj.as_object().unwrap(), None, None)?;
            }
        }
        Ok(())
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

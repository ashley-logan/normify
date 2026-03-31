use crate::error::{NormError, Result};
use crate::models::Database;
use serde_json::Value;
use std::io::Read;

pub struct Normifier {
    root: Value,
    root_tname: String,
    database: Database,
}

impl Normifier {
    pub fn new() -> Self {
        Self {
            root: Value::Null,
            root_tname: String::from("root_table"),
            database: Database::new(),
        }
    }

    pub fn new_with_config(
        root: Option<Value>,
        root_tname: Option<String>,
        database: Option<Database>,
    ) -> Self {
        Self {
            root: root.unwrap_or(Value::Null),
            root_tname: root_tname.unwrap_or(String::from("root_table")),
            database: database.unwrap_or_else(Database::new),
        }
    }

    pub fn set_root(&mut self, root: Value) {
        self.root = root;
    }

    pub fn set_root_tbl_name(&mut self, root_tname: String) {
        self.root_tname = root_tname;
    }

    pub fn set_database(&mut self, database: Database) {
        self.database = database;
    }

    pub fn get_database<'a>(&'a self) -> &'a Database {
        &self.database
    }

    pub fn extract_database(mut self, replace_with: Option<Database>) -> Database {
        let db: Database = self.database;
        self.database = replace_with.unwrap_or_else(Database::new);
        db
    }

    pub fn get_root<'a>(&'a self) -> &'a Value {
        &self.root
    }

    pub fn normify_root(&mut self) -> Result<()> {
        match &self.root {
            Value::Array(arr) => {
                self.database
                    .parse_object_array(&self.root_tname, arr, None, None)?;
            }
            Value::Object(obj) => {
                self.database.parse_obj(&self.root_tname, obj, None, None)?;
            }
            _ => return Err(NormError::Parse),
        }
        self.database.remove_null_cols();
        Ok(())
    }

    pub fn normify_from_reader(&mut self, reader: impl Read) -> Result<()> {
        self.root = serde_json::from_reader(reader)?;
        self.normify_root()
    }

    pub fn normify_from_str(&mut self, str: &str) -> Result<()> {
        self.root = serde_json::from_str(str)?;
        self.normify_root()
    }
}

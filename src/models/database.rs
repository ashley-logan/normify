use crate::models::{ColumnType, Table};
use indexmap::IndexMap;

pub struct Database {
    tables: IndexMap<String, Table>,
}

impl Database {
    pub fn new() -> Self {
        Self {
            tables: IndexMap::new(),
        }
    }

    pub fn get_mut_table(&mut self, name: &String) -> Option<&mut Table> {
        self.tables.get_mut(name)
    }

    pub fn get_mut_table_or_create(&mut self, name: &String) -> &mut Table {
        if let Some(tbl) = self.get_mut_table(name) {
            tbl
        } else {
            self.add_table(name.to_string());
            self.get_mut_table(name).unwrap()
        }
    }

    pub fn add_table(&mut self, name: String) {
        self.tables.insert(name, Table::new());
    }

    pub fn replace_table(&mut self, name: String, table: Table) {
        self.tables.insert(name, table);
    }

    pub fn drop_table(&mut self, name: &String) -> bool {
        self.tables.swap_remove(name).is_some()
    }
}

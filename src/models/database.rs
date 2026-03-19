use crate::models::{ArrayTrait, Table};
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

    pub fn insert_table(&mut self, name: String, table: Table) {
        self.tables.insert(name, table);
    }

    pub fn drop_table(&mut self, name: &String) -> bool {
        self.tables.swap_remove(name).is_some()
    }
}

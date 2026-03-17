use crate::models::{DataColumn, IdColumn};
use indexmap::IndexMap;

pub struct Table {
    pub(crate) id_column: IdColumn,
    pub(crate) columns: IndexMap<String, DataColumn>,
    pub(crate) fk_columns: IndexMap<String, IdColumn>,
}

impl Table {
    pub fn new() -> Self {
        Self {
            id_column: IdColumn::new(),
            columns: IndexMap::new(),
            fk_columns: IndexMap::new(),
        }
    }

    pub fn insert_col(&mut self, field: String, col: DataColumn) {
        self.columns.insert(field, col);
    }

    pub fn insert_fk(&mut self, parent_name: String) -> String {
        // insert a new foreign key column into the table and return the column's name
        let fk_field: String = format!("{}_id", parent_name);
        self.fk_columns.insert(fk_field.clone(), IdColumn::new());
        fk_field
    }

    pub fn new_id(&mut self) -> u64 {
        self.id_column.auto_insert()
    }
}

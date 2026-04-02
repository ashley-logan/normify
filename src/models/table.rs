use std::ops::Index;

use crate::error::{NormError, Result};
use crate::models::{ColumnType, IdColumn, Item, ItemTrait, NestedArray, NormArray};
use indexmap::IndexMap;

pub struct Table {
    pub id_col: IdColumn,
    pub data_cols: IndexMap<String, Box<dyn ColumnType>>,
    pub fk_cols: IndexMap<String, IdColumn>,
}

impl Table {
    pub fn new() -> Self {
        Self {
            id_col: IdColumn::new(),
            data_cols: IndexMap::new(),
            fk_cols: IndexMap::new(),
        }
    }

    pub fn insert_col(&mut self, field: String, col: Box<dyn ColumnType>) {
        self.data_cols.insert(field, col);
    }

    pub fn num_cols(&self) -> usize {
        1 + self.num_data_cols() + self.num_fk_cols()
    }

    pub fn num_data_cols(&self) -> usize {
        self.data_cols.len()
    }
    pub fn num_fk_cols(&self) -> usize {
        self.fk_cols.len()
    }

    pub fn num_rows(&self) -> usize {
        self.id_col.len()
    }

    pub fn replace_col(&mut self, field: &String, new_data: Box<dyn ColumnType>) {
        if self.data_cols.contains_key(field) {
            self.data_cols[field] = new_data;
        }
    }

    pub fn get_mut_or_insert(
        &mut self,
        field: &String,
        default: Box<dyn ColumnType>,
    ) -> &mut Box<dyn ColumnType> {
        self.data_cols.entry(field.to_string()).or_insert(default)
    }

    pub fn get_mut_or_panic(&mut self, field: &str) -> &mut Box<dyn ColumnType> {
        self.data_cols.get_mut(field).unwrap()
    }

    pub fn append_list<T: ItemTrait + 'static>(
        &mut self,
        field: &String,
        list: NormArray<T>,
    ) -> Result<()> {
        if let Some(col) = self.get_mut_col(field) {
            if let Some(ucol) = col.as_unknown() {
                let mut new_col: NestedArray<T> = NestedArray::new_with_nulls(ucol.count_nulls());
                new_col.push_arr(list);
                self.data_cols.insert(field.to_string(), Box::new(new_col));
            } else if let Some(n_col) = col.as_any_mut().downcast_mut::<NestedArray<T>>() {
                n_col.push_arr(list);
            } else {
                return Err(NormError::Build);
            }
        } else {
            let mut new_col: NestedArray<T> = NestedArray::new_with_nulls(self.num_rows() - 1);
            new_col.push_arr(list);
            self.insert_col(field.to_string(), Box::new(new_col));
        }
        Ok(())
    }

    pub fn append_item<T: ItemTrait + 'static>(
        &mut self,
        field: &String,
        item: Item<T>,
    ) -> Result<()> {
        if let Some(col) = self.get_mut_col(field) {
            if let Some(ucol) = col.as_unknown() {
                let mut new_col: NormArray<T> = NormArray::new_with_nulls(ucol.count_nulls());
                new_col.push_item(item);
                let _ = self.data_cols.insert(field.to_string(), Box::new(new_col));
            } else if let Some(n_col) = col.as_any_mut().downcast_mut::<NormArray<T>>() {
                n_col.push_item(item);
            } else if let Some(s_col) = col.as_string_column() {
                s_col.push_item(item.inner_to_string());
            } else {
                // fallback to string representation
                let _ = col;
                let (ind, name, old_col) = self.data_cols.shift_remove_full(field).unwrap();
                let new_col = old_col.into_string_super();
                self.data_cols.shift_insert(ind, name, new_col);
            }
        } else {
            let mut new_col: NormArray<T> = NormArray::new_with_nulls(self.num_rows() - 1);
            new_col.push_item(item);
            self.insert_col(field.to_string(), Box::new(new_col));
        }
        Ok(())
    }

    pub fn append_null(&mut self, field: &str) -> Result<()> {
        if let Some(col) = self.get_mut_col(field) {
            col.push_null();
            Ok(())
        } else {
            Err(NormError::Insert)
        }
    }

    pub fn insert_fk(&mut self, parent_name: String, parent_id: u64) -> String {
        // insert a new foreign key column into the table and return the column's name
        let fk_field: String = format!("{}_id", parent_name);
        self.fk_cols
            .entry(fk_field.clone())
            .or_insert_with(IdColumn::new)
            .man_insert(parent_id);
        fk_field
    }

    pub fn drop(&mut self, name: &str) {
        self.data_cols.swap_remove(name);
    }

    pub fn new_id(&mut self) -> u64 {
        self.id_col.auto_insert2()
    }

    pub fn get_col(&self, field: &str) -> Option<&Box<dyn ColumnType>> {
        self.data_cols.get(field)
    }

    pub fn get_id_col(&self) -> &IdColumn {
        &self.id_col
    }

    pub fn get_fk_col(&self, field: &str) -> Option<&IdColumn> {
        self.fk_cols.get(field)
    }
    pub fn get_mut_col(&mut self, field: &str) -> Option<&mut Box<dyn ColumnType>> {
        self.data_cols.get_mut(field)
    }

    pub fn remove_unknown_cols(&mut self) {
        let mut remove: Vec<String> = vec![];
        for (k, c) in self.data_cols.iter() {
            if c.is_unknown() {
                println!("Column: {} was removed due to undetermined type", k);
                remove.push(k.clone());
            }
        }
        for name in remove {
            self.drop(name.as_str());
        }
    }

    pub fn iter_data_cols(&self) -> indexmap::map::Iter<'_, String, Box<dyn ColumnType>> {
        self.data_cols.iter()
    }

    pub fn iter_cols(&self) -> IterCols<'_> {
        IterCols {
            table: self,
            index: 0,
        }
    }
}

pub struct IterRows<'a> {
    table: &'a Table,
    index: usize,
}

pub struct IterCols<'a> {
    table: &'a Table,
    index: usize,
}

impl<'a> Iterator for IterCols<'a> {
    type Item = (&'a str, &'a dyn ColumnType);

    fn next(&mut self) -> Option<Self::Item> {
        if self.index >= self.table.num_cols() {
            None
        } else if self.index == 0 {
            let col: &IdColumn = self.table.get_id_col();
            self.index += 1;
            Some(("ID", col))
        } else if self.index <= self.table.num_fk_cols() {
            let r: Option<(&String, &IdColumn)> = self.table.fk_cols.get_index(self.index - 1);
            match r {
                Some((k, v)) => Some((k.as_str(), v)),
                _ => None,
            }
        } else {
            let r: Option<(&String, &Box<dyn ColumnType>)> = self
                .table
                .data_cols
                .get_index(self.index - 1 - self.table.num_fk_cols());
            match r {
                Some((k, v)) => Some((k.as_str(), v.as_ref())),
                _ => None,
            }
        }
    }
}

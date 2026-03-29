use crate::error::{NormError, Result};
use crate::models::{
    ColumnType, IdColumn, Item, ItemTrait, NestedArray, NormArray, SimpleArrayType, UnknownArray,
};
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

    pub fn replace_col(
        &mut self,
        field: &String,
        new_data: Box<dyn ColumnType>,
    ) -> Option<Box<dyn ColumnType>> {
        if self.data_cols.contains_key(field) {
            self.data_cols.insert(field.clone(), new_data)
        } else {
            Option::None
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

    pub fn append_list<T: SimpleArrayType + 'static>(
        &mut self,
        field: &String,
        list: T,
    ) -> Result<()> {
        if let Some(col) = self.get_mut_col(field) {
            if let Some(ucol) = col.as_unknown() {
                let mut new_col: NestedArray<T> = ucol.clone().into_nested();
                new_col.push_arr(list);
                self.replace_col(field, Box::new(new_col));
            } else if let Some(n_col) = col.as_any_mut().downcast_mut::<NestedArray<T>>() {
                n_col.push_arr(list);
            } else {
                return Err(NormError::Build);
            }
        } else {
            self.insert_col(field.to_string(), Box::new(NestedArray::from(list)));
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
                let new_col: NormArray<T> = ucol.clone().into();
                self.replace_col(field, Box::new(new_col));
            } else if let Some(n_col) = col.as_any_mut().downcast_mut::<NormArray<T>>() {
                n_col.push_item(item);
            } else {
                return Err(NormError::Build);
            }
        } else {
            self.insert_col(field.to_string(), Box::new(NormArray::from(item)));
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
        self.id_col.auto_insert()
    }

    pub fn get_col(&self, field: &str) -> Option<&Box<dyn ColumnType>> {
        self.data_cols.get(field)
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
}

use crate::models::{ArrayTrait, DataColumn, ListArray, IdColumn, Item, ItemTrait, NormArray, UnknownArray, NestedTrait};
use crate::error::{NormError, Result};
use crate::models::type_aliases::*;
use indexmap::IndexMap;

// pub struct Table {
//     pub(crate) id_column: IdColumn,
//     pub(crate) columns: IndexMap<String, DataColumn>,
//     pub(crate) fk_columns: IndexMap<String, IdColumn>,
// }

pub struct Table {
    pub id_col: IdColumn,
    pub data_cols: IndexMap<String, Box<dyn ArrayTrait>>,
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

    // pub fn insert_col(&mut self, field: String, col: Box<dyn ArrayTrait>) {
    //     self.columns.insert(field, col);
    // }

    pub fn insert_col<T: ItemTrait>(&mut self, field: String, arr: NormArray<T>) {
        self.data_cols.insert(field, Box::new(arr));
    }

    pub fn insert(&mut self, field: String, col: Box<dyn ArrayTrait>) {
        self.data_cols.insert(field, col);
    }

    pub fn get_mut_or_insert(&mut self, field: &String, default: Box<dyn ArrayTrait>) -> &mut Box<dyn ArrayTrait> {
        self.data_cols.entry(field.to_string()).or_insert(default)
    }

    pub fn col_push_item<T: ItemTrait>(&mut self, field: &String, item: Item<T>) -> Result<()> {
        if let Some(arr) = self.data_cols.get(field) {
            let mut norm_arr = arr.as_any_mut().downcast_mut::<NormArray<T>>().ok_or(NormError::Insert)?;
            norm_arr.push_item(item);
        } else {
            let mut norm_arr: NormArray<T> = NormArray::from_item(item);
            self.data_cols.insert(field.to_string(), Box::new(norm_arr));
        }
        Ok(())
    }

    pub fn col_push_list<T: ArrayTrait>(&mut self, field: &String, list: T) -> Result<()> {
        if let Some(arr) = self.data_cols.get(field) {
            let mut list_arr = arr.as_any_mut().downcast_mut::<ListArray<T>>().ok_or(NormError::Insert)?;
            list_arr.push_arr(list);
        } else {
            let mut list_arr: ListArray<T> = ListArray::from_arr(list);
            self.data_cols.insert(field.to_string(), Box::new(list_arr));
        }
        Ok(())
    }


    pub fn col_push_null(&mut self, field: &String) -> Result<()> {
        if let Some(arr) = self.data_cols.get(field) { // if column exists...

            if arr.count_data() > 0 { // if column has non-null entries...
                

                if let Some(b_nested) = arr.as_any_mut().downcast_mut::<ListArray<NormArray<bool>>>() {
                    b_nested.push_arr(NormArray::new());
                } else if let Some(s_nested) = arr.as_any_mut().downcast_mut::<ListArray<NormArray<String>>>() {
                    s_nested.push_arr(NormArray::new());
                }

                // try downcasting as each NormArray type and pushing Item<T>::Null
                if let Some(b_arr) = arr.as_any_mut().downcast_mut::<NormArray<bool>>() {
                    b_arr.push_null();
                } else if let Some(s_arr) = arr.as_any_mut().downcast_mut::<NormArray<String>>() {
                    s_arr.push_null();
                } else if let Some(i_arr) = arr.as_any_mut().downcast_mut::<NormArray<i64>>() {
                    i_arr.push_null();
                } else if let Some(u_arr) = arr.as_any_mut().downcast_mut::<NormArray<u64>>() {
                    u_arr.push_null();
                } else if let Some(f_arr) = arr.as_any_mut().downcast_mut::<NormArray<f64>>() {
                    f_arr.push_null();
                } else {
                    return Err(NormError::Insert)
                }
            }
            // columns exists but is populated with only null values
            arr.as_any_mut().downcast_mut::<UnknownArray>().ok_or(NormError::Insert)?.add_null();
        } else {
            // column doesn't exist yet, create temporary unknown column
            self.data_cols.insert(field.to_string(), Box::new(UnknownArray::new_with_null()));
        }
        Ok(())
       
    }

    pub fn get_as_array<T: ItemTrait>(self, field: &String) -> Option<&NormArray<T>> {
        if let Some(arr) = self.data_cols.get(field) {
            arr.as_any().downcast_ref::<NormArray<T>>()
        } else { Option::None }
    }

    pub fn insert_fk(&mut self, parent_name: String, parent_id: u64) -> String {
        // insert a new foreign key column into the table and return the column's name
        let fk_field: String = format!("{}_id", parent_name);
        self.fk_cols.entry(fk_field).or_insert_with(IdColumn::new).man_insert(parent_id);
        fk_field
    }

    pub fn new_id(&mut self) -> u64 {
        self.id_col.auto_insert()
    }

    pub fn get_mut_col(&mut self, field: &String) -> Option<&Box<dyn ArrayTrait>> {
        self.data_cols.get(field)
    }
    }
}

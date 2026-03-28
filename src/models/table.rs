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

    // pub fn replace_unknown_col(&mut self, field: &String, new_data: Box<dyn ColumnType>) -> Result<()> {
    //     if let Some(u_col) = self.data_cols
    //         .get(field.as_str())
    //         .and_then(|x| x.as_any().downcast_ref::<UnknownArray>())
    //     {
    //         u_col.into_list_col(list)
    //         self.replace_col(field, new_data);
    //     } else {
    //         return Err(NormError::Build);
    //     }
    //     Ok(())
    // }

    pub fn get_mut_or_insert(
        &mut self,
        field: &String,
        default: Box<dyn ColumnType>,
    ) -> &mut Box<dyn ColumnType> {
        self.data_cols.entry(field.to_string()).or_insert(default)
    }

    pub fn append_list<T: SimpleArrayType>(&mut self, field: &String, list: T) -> Result<()> {
        if let Some(col) = self.get_mut_col(field) {
            if let Some(ucol) = col.as_unknown() {
                let mut new_col: NestedArray<T> = ucol.clone().into();
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

    pub fn append_item<T: ItemTrait>(&mut self, field: &String, item: Item<T>) -> Result<()> {
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

    // pub fn col_push_null(&mut self, field: &String) -> Result<()> {
    //     if let Some(arr) = self.data_cols.get(field) {
    //         // if column exists...

    //         if arr.count_data() > 0 {
    //             // if column has non-null entries...

    //             if let Some(b_nested) = arr
    //                 .as_any_mut()
    //                 .downcast_mut::<NestedArray<NormArray<bool>>>()
    //             {
    //                 b_nested.push_arr(NormArray::new());
    //             } else if let Some(s_nested) = arr
    //                 .as_any_mut()
    //                 .downcast_mut::<NestedArray<NormArray<String>>>()
    //             {
    //                 s_nested.push_arr(NormArray::new());
    //             }

    //             // try downcasting as each NormArray type and pushing Item<T>::Null
    //             if let Some(b_arr) = arr.as_any_mut().downcast_mut::<NormArray<bool>>() {
    //                 b_arr.push_null();
    //             } else if let Some(s_arr) = arr.as_any_mut().downcast_mut::<NormArray<String>>() {
    //                 s_arr.push_null();
    //             } else if let Some(i_arr) = arr.as_any_mut().downcast_mut::<NormArray<i64>>() {
    //                 i_arr.push_null();
    //             } else if let Some(u_arr) = arr.as_any_mut().downcast_mut::<NormArray<u64>>() {
    //                 u_arr.push_null();
    //             } else if let Some(f_arr) = arr.as_any_mut().downcast_mut::<NormArray<f64>>() {
    //                 f_arr.push_null();
    //             } else {
    //                 return Err(NormError::Insert);
    //             }
    //         }
    //         // columns exists but is populated with only null values
    //         arr.as_any_mut()
    //             .downcast_mut::<UnknownArray>()
    //             .ok_or(NormError::Insert)?
    //             .add_null();
    //     } else {
    //         // column doesn't exist yet, create temporary unknown column
    //         self.data_cols
    //             .insert(field.to_string(), Box::new(UnknownArray::new_with_null()));
    //     }
    //     Ok(())
    // }

    // pub fn get_as_array<T: ItemTrait>(self, field: &String) -> Option<&NormArray<T>> {
    //     if let Some(arr) = self.data_cols.get(field) {
    //         arr.as_any().downcast_ref::<NormArray<T>>()
    //     } else {
    //         Option::None
    //     }
    // }
    // pub fn get_as_mut_array<T: ItemTrait>(self, field: &String) -> Option<&mut NormArray<T>> {
    //     if let Some(arr) = self.data_cols.get(field) {
    //         arr.as_any_mut().downcast_mut::<NormArray<T>>()
    //     } else {
    //         Option::None
    //     }
    // }

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
}

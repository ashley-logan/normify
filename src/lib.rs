mod database_builder;
mod error;
mod helpers;
mod models;
mod normalizer;
mod trait_impl;
pub use database_builder::DataBase;
pub use helpers::normalize_arr;
pub use normalizer::Normifier;
pub use serde_json::{Map, Value};

pub use crate::error::NormError;
pub use crate::models::{
    Database, IdColumn, Item, ItemTrait, ListArray, NormArray, Table, type_aliases::*,
};

type ObjectIter = &Map<String, Value>;

fn parse_obj(
    db: &mut Database,
    table_name: &String,
    obj: ObjectIter,
    parent_id: Option<u64>,
    parent_tname: Option<&String>,
) {
    let mut curr_table = db.get_mut_table(table_name).ok_or(NormError::Build)?; // curr_table = mutable reference to Table: table_name

    let curr_id: u64 = curr_table.new_id(); // push auto generated id for new row and store in curr_id

    if let (Some(pid), Some(pname)) = (parent_id, parent_tname) {
        // if curr_table is a child of Table: parent_name, then push parent id for foreign key column
        // foreign key column is created if needed
        curr_table.insert_fk(parent_name.to_string(), pid);
    }

    for (k, v) in obj {
        let mut maybe_col = curr_table.get_mut_col(k);

        match v {
            Value::Bool(b) => {
                // Bool variant => push Data(bool)
                curr_table.col_push_item(k, Item::Data(*b))?;
            }
            Value::String(s) => {
                // String variant => push Data(String)
                curr_table.col_push_item(k, Item::Data(s.to_string()))?;
            }
            Value::Null => {
                // Null variant => push
                curr_table.col_push_null(k)?;
            }
            Value::Number(n) => {
                if let Some(i) = n.as_i64() {
                    curr_table.col_push_item(k, Item::Data(i))?;
                } else if let Some(u) = n.as_u64() {
                    curr_table.col_push_item(k, Item::Data(u))?;
                } else if let Some(f) = n.as_f64() {
                    curr_table.col_push_item(k, Item::Data(f))?;
                } else {
                    return Err(NormError::Convert);
                }
            }
            Value::Array(arr) => {
                if arr.iter().all(Value::is_object) {
                    let child_name: String = format!("{}_table", k);
                    db.insert_table(child_name.clone(), Table::new());

                    for child_obj in arr {
                        parse_obj(
                            db,
                            &child_name,
                            child_obj.as_object().ok_or(NormError::Build)?,
                            Some(curr_id),
                            Some(table_name),
                        );
                    }
                } else {
                    let n_arr = normalize_arr(arr);

                    if let Some(int_arr) = n_arr.as_any().downcast_ref::<NormArray<i64>>() {
                        let c: &mut ListArray<i64> = curr_table
                            .get_mut_or_insert(k, Box::new(ListArray::new::<NormArray<i64>>()))
                            .as_any_mut()
                            .downcast_mut()
                            .unwrap();
                        c.push_arr(int_arr.clone());
                    } else if let Some(uint_arr) = n_arr.as_any().downcast_ref::<NormArray<u64>>() {
                        let c: &mut ListArray<u64> = curr_table
                            .get_mut_or_insert(k, Box::new(ListArray::new::<NormArray<u64>>()))
                            .as_any_mut()
                            .downcast_mut()
                            .unwrap();
                        c.push_arr(uint_arr.clone());
                    } else if let Some(float_arr) = n_arr.as_any().downcast_ref::<NormArray<f64>>()
                    {
                        let c: &mut ListArray<f64> = curr_table
                            .get_mut_or_insert(k, Box::new(ListArray::new::<NormArray<f64>>()))
                            .as_any_mut()
                            .downcast_mut()
                            .unwrap();
                        c.push_arr(float_arr.clone());
                    } else if let Some(bool_arr) = n_arr.as_any().downcast_ref::<NormArray<bool>>()
                    {
                        let c: &mut ListArray<bool> = curr_table
                            .get_mut_or_insert(k, Box::new(ListArray::new::<NormArray<bool>>()))
                            .as_any_mut()
                            .downcast_mut()
                            .unwrap();
                        c.push_arr(bool_arr.clone());
                    } else if let Some(string_arr) =
                        n_arr.as_any().downcast_ref::<NormArray<String>>()
                    {
                        let c: &mut ListArray<String> = curr_table
                            .get_mut_or_insert(k, Box::new(ListArray::new::<NormArray<String>>()))
                            .as_any_mut()
                            .downcast_mut()
                            .unwrap();
                        c.push_arr(string_arr.clone());
                    } else {
                    }
                }
            }
            Value::Object(child_obj) => {
                // if the value is an object, this is a new table
                // the current table has a one-to-one relationship with the new table
                let child_name: String = format!("{}_table", k);
                db.insert_table(child_name.clone(), Table::new());
                parse_obj(db, &child_name, child_obj, Some(curr_id), Some(table_name));
            }
        }
    }
}

pub fn from_value(root_value: Value) -> Result<Normifier, NormError> {
    let mut norm_context: Normifier = Normifier::new();
    let root_name: String = String::from("root_table");
    norm_context.process_root(root_value, root_name)?;
    norm_context.clean_normifier();
    Ok(norm_context)
}

pub fn from_text(content: &str) -> Result<Normifier, NormError> {
    let mut norm_context: Normifier = Normifier::new();
    let root_value: serde_json::Value = serde_json::from_str(content)?;
    let root_name: String = String::from("root_table");
    norm_context.process_root(root_value, root_name)?;
    norm_context.clean_normifier();
    Ok(norm_context)
}

pub fn from_value_with_name(root_value: Value, root_name: &str) -> Result<Normifier, NormError> {
    let mut norm_context: Normifier = Normifier::new();
    norm_context.process_root(root_value, root_name.to_string())?;
    norm_context.clean_normifier();
    Ok(norm_context)
}
pub fn from_text_with_name(content: &str, root_name: &str) -> Result<Normifier, NormError> {
    let mut norm_context: Normifier = Normifier::new();
    let root_value: serde_json::Value = serde_json::from_str(content)?;
    norm_context.process_root(root_value, root_name.to_string())?;
    norm_context.clean_normifier();
    Ok(norm_context)
}

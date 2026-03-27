mod database_builder;
mod error;
mod helpers;
mod models;
mod normalizer;
mod trait_impl;
pub use database_builder::DataBase;
pub use helpers::normalize_arr;
// pub use normalizer::Normifier;
pub use serde_json::{Map, Value};

pub use crate::error::NormError;
use crate::models::UnknownArray;
pub use crate::models::{Database, IdColumn, Item, ItemTrait, NormArray, Table, type_aliases::*};

type ObjectIter = &'static Map<String, Value>;

fn parse_obj(
    db: &mut Database,
    table_name: &String,
    obj: ObjectIter,
    parent_id: Option<u64>,
    parent_tname: Option<&String>,
) {
    let mut curr_table: &mut Table = db.get_mut_table_or_create(table_name); // curr_table = mutable reference to Table: table_name

    let curr_id: u64 = curr_table.new_id(); // push auto generated id for new row and store in curr_id

    if let (Some(pid), Some(pname)) = (parent_id, parent_tname) {
        // if curr_table is a child of Table=parent_name, then push parent id for foreign key column
        // foreign key column is created if needed
        curr_table.insert_fk(pname.to_string(), pid);
    }

    for (k, v) in obj {
        // let mut maybe_col = curr_table.get_mut_col(k);

        match v {
            // match on Value variant
            Value::Bool(b) => {
                // Bool variant => push Data(bool)
                curr_table.append_item(k, Item::Data(*b));
            }
            Value::String(s) => {
                // String variant => push Data(String)
                curr_table.append_item(k, Item::Data(s.to_string()));
            }
            Value::Null => {
                // Null variant => push Item::Null
                curr_table.col_push_null(k);
            }
            Value::Number(n) => {
                // Number variant => try converting to (impl ItemTrait) number types
                if let Some(i) = n.as_i64() {
                    // try i64
                    curr_table.append_item(k, Item::Data(i));
                } else if let Some(u) = n.as_u64() {
                    // try u64
                    curr_table.append_item(k, Item::Data(u));
                } else if let Some(f) = n.as_f64() {
                    // fallback to f64
                    curr_table.append_item(k, Item::Data(f));
                } else {
                    // number must be > u64::MAX, for now raise error until implemented
                    todo!("handle number that doesn't fit in column")
                }
            }
            Value::Array(arr) => {
                // Array variant => check inner Value variants

                if arr.iter().all(Value::is_object) {
                    // array of objects implies a new child table
                    let child_name: String = format!("{}_table", k);

                    for child_obj in arr {
                        // parse each object in array, with curr_table of the caller as the parent_table of the call
                        parse_obj(
                            db,
                            &child_name,
                            child_obj.as_object().ok_or(NormError::Build)?,
                            Some(curr_id),
                            Some(table_name),
                        );
                    }
                } else {
                    // column is a ListArray
                    let n_arr = normalize_arr(arr)?; // homogenize array and convert innner types to Item<T>

                    // try downcasting as each array type after normalizing


                    if let Some(bool_arr) = n_arr.as_bool_column()
                    
                    if let Some(int_arr) = n_arr.as_any().downcast_ref::<NormArray<i64>>() {
                        curr_table.append_list(k, int_arr.to_owned());
                        // entry is NormArray<i64>
                    } else if let Some(uint_arr) = n_arr.as_any().downcast_ref::<NormArray<u64>>() {
                        // entry is NormArray<u64>
                        curr_table.append_list(k, uint_arr.to_owned());
                    } else if let Some(float_arr) = n_arr.as_any().downcast_ref::<NormArray<f64>>()
                    {
                        // entry is NormArray<f64>
                        curr_table.append_list(k, float_arr.to_owned());
                    } else if let Some(bool_arr) = n_arr.as_any().downcast_ref::<NormArray<bool>>()
                    {
                        // entry is NormArray<bool>
                        curr_table.append_list(k, bool_arr.to_owned());
                    } else if let Some(string_arr) =
                        n_arr.as_any().downcast_ref::<NormArray<String>>()
                    {
                        // entry is NormArray<String>
                        curr_table.append_list(k, string_arr.to_owned());
                    } else if let Some(null_arr) = n_arr.as_any().downcast_ref::<UnknownArray>() {
                        curr_table.append_null(k);
                        todo!()
                    }
                }
            }
            Value::Object(child_obj) => {
                // if the value is an object, this is a new table
                // the current table has a one-to-one relationship with the new table
                let child_name: String = format!("{}_table", k);
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

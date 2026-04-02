use crate::error::Result;
use crate::models::{ColumnType, Item, NormArray, Table, UnknownArray};
use serde_json::Value;

// takes a slice of values and returns an owned vectored with all Value::Array items flattened
pub fn flatten(arr: &[Value]) -> Vec<Value> {
    fn flatten_array(this_arr: &[Value], flat_arr: &mut Vec<Value>) {
        for x in this_arr {
            match x {
                Value::Array(inner_arr) => flatten_array(inner_arr, flat_arr),
                _ => flat_arr.push(x.clone()),
            }
        }
    }
    let mut flat_arr: Vec<Value> = Vec::new();
    flatten_array(arr, &mut flat_arr);
    flat_arr
}

// resulting array necessarilly upcasted as dyn ColumnType
// if all values cannot be cast to single type then values are represented as strings -> NormArray<String> as Box<dyn ColumnType>
pub fn normalize_arr(arr: &[Value]) -> Result<Box<dyn ColumnType>> {
    if arr.iter().any(Value::is_object) {
        // array cannot contain object Value variants
        return Err(crate::error::NormError::Convert);
    }

    if arr.is_empty() {
        // UknownArray represents undetermined type
        return Ok(Box::new(UnknownArray::new()));
    }

    let flat_arr: Vec<Value> = flatten(arr); // get flattened array

    if flat_arr.iter().all(|x| x.as_str().is_some() || x.is_null()) {
        // every Value can be cast to Item<String>
        Ok(Box::new(
            flat_arr
                .iter()
                .map(|x| {
                    if let Some(s) = x.as_str() {
                        Item::Data(s.to_string())
                    } else {
                        Item::Null
                    }
                })
                .collect::<NormArray<String>>(), // can be downcasted to NormArray<String>
        ))
    } else if flat_arr
        .iter()
        .all(|x| x.as_bool().is_some() || x.is_null())
    {
        // every Value can be cast to Item<Bool>
        Ok(Box::new(
            flat_arr
                .iter()
                .map(|x| {
                    if let Some(b) = x.as_bool() {
                        Item::Data(b)
                    } else {
                        Item::Null
                    }
                })
                .collect::<NormArray<bool>>(), // can be downcasted to NormArray<bool>
        ))
    } else if flat_arr.iter().all(|x| x.as_i64().is_some() || x.is_null()) {
        // every Value can be cast to Item<i64>
        Ok(Box::new(
            flat_arr
                .iter()
                .map(|x| {
                    if let Some(i) = x.as_i64() {
                        Item::Data(i)
                    } else {
                        Item::Null
                    }
                })
                .collect::<NormArray<i64>>(), // can be downcasted to NormArray<i64>
        ))
    } else if flat_arr.iter().all(|x| x.as_u64().is_some() || x.is_null()) {
        // every Value can be cast to Item<u64>
        Ok(Box::new(
            flat_arr
                .iter()
                .map(|x| {
                    if let Some(u) = x.as_u64() {
                        Item::Data(u)
                    } else {
                        Item::Null
                    }
                })
                .collect::<NormArray<u64>>(), // can be downcasted to NormArray<u64>
        ))
    } else if flat_arr.iter().all(|x| x.as_f64().is_some() || x.is_null()) {
        // every Value can be cast to Item<f64>
        Ok(Box::new(
            flat_arr
                .iter()
                .map(|x| {
                    if let Some(f) = x.as_f64() {
                        Item::Data(f)
                    } else {
                        Item::Null
                    }
                })
                .collect::<NormArray<f64>>(), // can be downcasted to NormArray<f64>
        ))
    } else {
        // heterogenous array, represent all values as strings
        Ok(Box::new(
            flat_arr
                .iter()
                .map(|x| {
                    if x.is_null() {
                        Item::Null
                    } else {
                        Item::Data(x.to_string())
                    }
                })
                .collect::<NormArray<String>>(), // can be downcasted to NormArray<String>
        ))
    }
}

pub(crate) fn pad_columns(tbl: &mut Table) {
    let n = tbl.num_rows();
    for (_, col) in tbl.data_cols.iter_mut() {
        for _ in 0..n - col.len() {
            col.push_null();
        }
    }
}

use crate::error::Result;
use crate::models::{ArrayTrait, Item, ListArray, NormArray, NormErrorm, UnknownArray};
use indexmap::IndexMap;
use serde_json::Value;
use std::any::Any;

// converts a NON_NESTED vector of Values (not Value::Object, Value::Array) to some NormArray or UnknownArray
// resulting array necessarilly upcasted as dyn ArrayTrait
// if all values cannot be cast to single type then values are represented as strings -> NormArray<String> as Box<dyn ArrayTrait>
pub fn normalize_arr(arr: &Vec<Value>) -> Result<Box<dyn ArrayTrait>> {
    if arr.iter().any(|x| x.is_array() || x.is_object()) {
        // array cannot contain nested Value variants
        return Err(crate::NormError::Convert);
    }

    if arr.is_empty() {
        // UknownArray represents undetermined type
        return Ok(Box::new(UnknownArray::new()));
    }

    if arr.iter().all(|x| x.as_str().is_some() || x.is_null()) {
        // every Value can be cast to Item<String>
        Ok(Box::new(
            arr.iter()
                .map(|x| {
                    if let Some(s) = x.as_str() {
                        Item::Data(s.to_string())
                    } else {
                        Item::Null
                    }
                })
                .collect(), // can be downcasted to NormArray<String>
        ))
    } else if arr.iter().all(|x| x.as_bool().is_some() || x.is_null()) {
        // every Value can be cast to Item<Bool>
        Ok(Box::new(
            arr.iter()
                .map(|x| {
                    if let Some(b) = x.as_bool() {
                        Item::Data(b)
                    } else {
                        Item::Null
                    }
                })
                .collect(), // can be downcasted to NormArray<bool>
        ))
    } else if arr.iter().all(|x| x.as_i64().is_some() || x.is_null()) {
        // every Value can be cast to Item<i64>
        Ok(Box::new(
            arr.iter()
                .map(|x| {
                    if let Some(i) = x.as_i64() {
                        Item::Data(i)
                    } else {
                        Item::Null
                    }
                })
                .collect(), // can be downcasted to NormArray<i64>
        ))
    } else if arr.iter().all(|x| x.as_u64().is_some() || x.is_null()) {
        // every Value can be cast to Item<u64>
        Ok(Box::new(
            arr.iter()
                .map(|x| {
                    if let Some(u) = x.as_u64() {
                        Item::Data(u)
                    } else {
                        Item::Null
                    }
                })
                .collect(), // can be downcasted to NormArray<u64>
        ))
    } else if arr.iter().all(|x| x.as_f64().is_some() || x.is_null()) {
        // every Value can be cast to Item<f64>
        Ok(Box::new(
            arr.iter()
                .map(|x| {
                    if let Some(f) = x.as_f64() {
                        Item::Data(f)
                    } else {
                        Item::Null
                    }
                })
                .collect(), // can be downcasted to NormArray<f64>
        ))
    } else {
        // heterogenous array, represent all values as strings
        Ok(Box::new(
            arr.iter()
                .map(|x| {
                    if x.is_null() {
                        Item::Null
                    } else {
                        Item::Data(x.to_string())
                    }
                })
                .collect(), // can be downcasted to NormArray<String>
        ))
    }
}

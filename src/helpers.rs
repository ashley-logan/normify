use crate::models::{ArrayTrait, Item, ListArray, NormArray, NormError};
use indexmap::IndexMap;
use serde_json::Value;
use std::any::Any;

pub fn normalize_arr(arr: &Vec<Value>) -> Box<dyn ArrayTrait> {
    if arr.iter().all(|x| x.as_str().is_some() || x.is_null()) {
        Box::new(
            arr.iter()
                .map(|x| {
                    if let Some(s) = x.as_str() {
                        Item::Data(s.to_string())
                    } else {
                        Item::Null
                    }
                })
                .collect(),
        )
    } else if arr.iter().all(|x| x.as_bool().is_some() || x.is_null()) {
        Box::new(
            arr.iter()
                .map(|x| {
                    if let Some(b) = x.as_bool() {
                        Item::Data(b)
                    } else {
                        Item::Null
                    }
                })
                .collect(),
        )
    } else if arr.iter().all(|x| x.as_i64().is_some() || x.is_null()) {
        Box::new(
            arr.iter()
                .map(|x| {
                    if let Some(i) = x.as_i64() {
                        Item::Data(i)
                    } else {
                        Item::Null
                    }
                })
                .collect(),
        )
    } else if arr.iter().all(|x| x.as_u64().is_some() || x.is_null()) {
        Box::new(
            arr.iter()
                .map(|x| {
                    if let Some(u) = x.as_u64() {
                        Item::Data(u)
                    } else {
                        Item::Null
                    }
                })
                .collect(),
        )
    } else if arr.iter().all(|x| x.as_f64().is_some() || x.is_null()) {
        Box::new(
            arr.iter()
                .map(|x| {
                    if let Some(f) = x.as_f64() {
                        Item::Data(f)
                    } else {
                        Item::Null
                    }
                })
                .collect(),
        )
    } else {
        Box::new(
            arr.iter()
                .map(|x| {
                    if x.is_null() {
                        Item::Null
                    } else {
                        Item::Data(x.to_string())
                    }
                })
                .collect(),
        )
    }
}

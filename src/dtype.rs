use std::{convert::Infallible, fmt::Display};

use derive_more::{From, TryInto, TryIntoError};
use polars::prelude::DataType;
use serde_json::Value;

#[derive(Debug, Clone, From, TryInto)]
pub enum Dtype {
    String(String),
    Float(f64),
    UInt(u64),
    Int(i64),
    Bool(bool),
    #[try_into(ignore)]
    Array(Vec<Dtype>),
    #[try_into(ignore)]
    Null,
}

impl Display for Dtype {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::String(s) => write!(f, "{}", s),
            Self::Float(fl) => write!(f, "{}", fl),
            Self::UInt(u) => write!(f, "{}", u),
            Self::Int(i) => write!(f, "{}", i),
            _ => Err(std::fmt::Error),
        }
    }
}

impl Dtype {
    pub fn is_float(&self) -> bool {
        matches!(&self, Dtype::Float(_))
    }

    pub fn is_uint(&self) -> bool {
        matches!(&self, Dtype::UInt(_))
    }

    pub fn is_int(&self) -> bool {
        matches!(&self, Dtype::Int(_))
    }

    pub fn is_string(&self) -> bool {
        matches!(&self, Dtype::String(_))
    }

    pub fn is_bool(&self) -> bool {
        matches!(&self, Dtype::Bool(_))
    }

    pub fn is_null(&self) -> bool {
        matches!(&self, Dtype::Null)
    }

    pub fn is_array(&self) -> bool {
        matches!(&self, Dtype::Array(_))
    }

    pub fn into_vec(self) -> Option<Vec<Self>> {
        match self {
            Self::Array(arr) => Some(arr),
            _ => None,
        }
    }

    pub fn array_is_type(arr: &Vec<Dtype>, check: fn(&Self) -> bool) -> bool {
        arr.iter().all(|i| check(i) || i.is_null())
    }

    pub fn cast_array_as<T>(arr: Vec<Self>) -> Vec<Option<T>>
    where
        Self: TryInto<T>,
    {
        arr.into_iter().map(|i| i.try_into().ok()).collect()
    }

    pub fn stringify_array<T>(arr: Vec<Dtype>) -> Vec<Option<String>> {
        arr.into_iter()
            .map(|i| {
                if !i.is_null() {
                    Some(i.try_into().unwrap().to_string())
                } else {
                    None
                }
            })
            .collect()
    }

    pub fn from_value(value: Value) -> Self {
        match value {
            Value::String(s) => Dtype::String(s.to_owned()),
            Value::Null => Dtype::Null,
            Value::Array(arr) => Dtype::Array(
                arr.into_iter()
                    .map(Dtype::from_value)
                    .collect::<Vec<Dtype>>(),
            ),
            Value::Bool(b) => Dtype::Bool(b.to_owned()),
            Value::Number(n) => {
                if n.is_u64() {
                    Dtype::UInt(n.as_u64().unwrap())
                } else if n.is_i64() {
                    Dtype::Int(n.as_i64().unwrap())
                } else if n.is_f64() {
                    Dtype::Float(n.as_f64().unwrap())
                } else {
                    Dtype::Null
                }
            }
            Value::Object(_) => {
                panic!("Cannot convert type Value::Object into Dtype");
            }
        }
    }

    fn get_array_type(&self) -> Option<DataType> {
        if let Dtype::Array(arr) = &self {
            match arr {
                v if v.iter().all(Dtype::is_string) => Some(DataType::String),
                v if v.iter().all(Dtype::is_null) => Some(DataType::Null),
                v if v.iter().all(Dtype::is_bool) => Some(DataType::Boolean),
                v if v.iter().all(Dtype::is_uint) => Some(DataType::UInt64),
                v if v.iter().all(Dtype::is_int) => Some(DataType::Int64),
                v if v.iter().all(Dtype::is_float) => Some(DataType::Float64),
                _ => None, // array doesn't contain homogenous data
            }
        } else {
            None
        }
    }

    // pub fn get_polars_type(&self) -> DataType {
    //     match self {
    //         Dtype::String(_) => DataType::String,
    //         Dtype::Null => DataType::Null,
    //         Dtype::Bool(_) => DataType::Boolean,
    //         Dtype::UInt(_) => DataType::UInt64,
    //         Dtype::Int(_) => DataType::Int64,
    //         Dtype::Float(_) => DataType::Float64,
    //         Dtype::Array(_) => todo!("get array type or transform"),
    //     }
    // }

    // pub fn normalize_arr(self) -> Self {
    //     // converts all elements in an array to the same type
    //     // defaults to string conversion if array is not already normalized
    //     if let Dtype::Array(arr) = &self {
    //         match self.get_array_type() {
    //             Some(_) => self,
    //             None => Dtype::Array(
    //                 arr.iter()
    //                     .map(Dtype::to_string)
    //                     .map(Dtype::from)
    //                     .collect::<Vec<Dtype>>(),
    //             ),
    //         }
    //     } else {
    //         panic!("normalize array can only be called on variant Array");
    //     }
    // }
}

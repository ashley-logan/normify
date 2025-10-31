use derive_more::{Display, From};
use polars::prelude::DataType;
use serde_json::Value;

#[derive(Debug, Clone, From)]
pub enum Dtype {
    String(String),
    Float(f64),
    UInt(u64),
    Int(i64),
    Bool(bool),
    #[from(skip)]
    Array(Vec<Dtype>),
    #[from(skip)]
    Null,
}

impl From<Value> for Dtype {
    fn from(value: Value) -> Self {
        match value {
            Value::String(s) => Dtype::String(s.to_owned()),
            Value::Null => Dtype::Null,
            Value::Array(arr) => {
                Dtype::Array(arr.into_iter().map(Dtype::from).collect::<Vec<Dtype>>())
            }
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

    fn get_array_type(&self) -> Option<DataType> {
        if let Dtype::Array(arr) = &self {
            match arr {
                v if v.iter().all(Dtype::is_string) => Some(DataType::String),
                v if v.iter().all(Dtype::is_null) => Some(DataType::Null),
                v if v.iter().all(Dtype::is_bool) => Some(DataType::Boolean),
                v if v.iter().all(Dtype::is_uint) => Some(DataType::UInt64),
                v if v.iter().all(Dtype::is_int) => Some(DataType::Int64),
                v if v.iter().all(Dtype::is_float) => Some(DataType::Float64),
                v if v.iter().all(Dtype::is_array) => {
                    panic!("Cannot convert a nested array into a polars datatype");
                } // TODO

                _ => None, // array doesn't contain homogenous data
            }
        } else {
            panic!("Cannot call get array type on non-Array variant")
        }
    }

    pub fn get_polars_type(&self) -> DataType {
        match self {
            Dtype::String(_) => DataType::String,
            Dtype::Null => DataType::Null,
            Dtype::Bool(_) => DataType::Boolean,
            Dtype::UInt(_) => DataType::UInt64,
            Dtype::Int(_) => DataType::Int64,
            Dtype::Float(_) => DataType::Float64,
            Dtype::Array(_) => todo!("get array type or transform"),
        }
    }

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

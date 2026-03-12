use crate::error::{NormError, Result};
use derive_more::From;
use serde_json::Value;
use std::fmt::Display;

#[derive(Debug, Clone, From)]
pub enum NormValue {
    // wrapper for json primitives
    String(String),
    Float(f64),
    UInt(u64),
    Int(i64),
    Bool(bool),
    Array(Vec<NormValue>),
    Null,
}

impl Display for NormValue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Null => write!(f, "null"),
            Self::Bool(b) => write!(f, "{}", b),
            Self::String(s) => write!(f, "{}", s),
            Self::Float(fl) => write!(f, "{}", fl),
            Self::UInt(u) => write!(f, "{}", u),
            Self::Int(i) => write!(f, "{}", i),
            // if Array, write each element individually
            Self::Array(a) => a
                .iter()
                .map(|x| write!(f, "{}, ", x.to_string()))
                .collect::<std::fmt::Result>(),
        }
    }
}

impl NormValue {
    pub fn is_float(&self) -> bool {
        matches!(&self, NormValue::Float(_))
    }

    pub fn is_uint(&self) -> bool {
        matches!(&self, NormValue::UInt(_))
    }

    pub fn is_int(&self) -> bool {
        matches!(&self, NormValue::Int(_))
    }

    pub fn is_string(&self) -> bool {
        matches!(&self, NormValue::String(_))
    }

    pub fn is_bool(&self) -> bool {
        matches!(&self, NormValue::Bool(_))
    }

    pub fn is_null(&self) -> bool {
        matches!(&self, NormValue::Null)
    }

    pub fn is_array(&self) -> bool {
        matches!(&self, NormValue::Array(_))
    }

    // extract vector from Array type (moves vector)
    pub fn into_vec(self) -> Result<Vec<Self>, NormError> {
        match self {
            Self::Array(arr) => Ok(arr),
            _ => Err(NormError::Convert),
        }
    }

    // extract a slice from Array type (no ownership transfer)
    pub fn get_slice<'a>(&'a self) -> Result<&'a [NormValue], NormError> {
        if let Self::Array(arr) = self {
            Ok(arr.as_slice())
        } else {
            Err(NormError::Convert)
        }
    }

    // returns true if all elements pass the specified type check function or are null
    pub fn array_is_type(arr: &Vec<NormValue>, check: fn(&Self) -> bool) -> bool {
        arr.iter().all(|i| check(i) || i.is_null())
    }

    // cast each NormValue object in an array
    pub fn cast_to_option<T>(values: Vec<NormValue>) -> Vec<Option<T>>
    where
        Self: TryInto<T>,
    {
        values
            .into_iter()
            .map(|x| {
                if !x.is_null() {
                    x.try_into().ok()
                } else {
                    None
                }
            })
            .collect()
    }

    pub fn from_value(value: Value) -> Result<Self, NormError> {
        let r: NormValue = match value {
            Value::String(s) => NormValue::String(s.to_owned()),
            Value::Null => NormValue::Null,
            Value::Array(mut arr) => {
                if arr.is_empty() {
                    NormValue::Array(vec![])
                } else {
                    NormValue::Array(
                        arr.into_iter()
                            .map(NormValue::from_value)
                            .collect::<Result<Vec<NormValue>, NormError>>()?,
                    )
                }
            }
            Value::Bool(b) => NormValue::Bool(b.to_owned()),
            Value::Number(n) => {
                if let Some(i) = n.as_i64() {
                    NormValue::Int(i)
                } else if let Some(u) = n.as_u64() {
                    NormValue::UInt(u)
                } else {
                    NormValue::Float(n.as_f64().unwrap())
                }
            }
            Value::Object(_) => return Err(NormError::Convert),
        };
        Ok(r)
    }
}

fn normify_array(arr: Vec<Value>) -> Vec<NormValue> {
    todo!()
}

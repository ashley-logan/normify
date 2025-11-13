use crate::error;
use derive_more::From;
use serde_json::Value;
use std::fmt::Display;

#[derive(Debug, Clone, From)]
pub enum Dtype {
    String(String),
    Float(f64),
    UInt(u64),
    Int(i64),
    Bool(bool),
    Array(Vec<Dtype>),
    Null,
}

impl Display for Dtype {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Null => write!(f, "null"),
            Self::Bool(b) => write!(f, "{}", b),
            Self::String(s) => write!(f, "{}", s),
            Self::Float(fl) => write!(f, "{}", fl),
            Self::UInt(u) => write!(f, "{}", u),
            Self::Int(i) => write!(f, "{}", i),
            Self::Array(a) => a
                .iter()
                .map(|x| write!(f, "{}, ", x.to_string()))
                .collect::<std::fmt::Result>(),
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

    pub fn get_slice<'a>(&'a self) -> Option<&'a [Dtype]> {
        if let Self::Array(arr) = self {
            Some(arr.as_slice())
        } else {
            None
        }
    }

    pub fn array_is_type(arr: &Vec<Dtype>, check: fn(&Self) -> bool) -> bool {
        arr.iter().all(|i| check(i) || i.is_null())
    }

    pub fn cast_to_option<T>(values: Vec<Dtype>) -> Vec<Option<T>>
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

    pub fn from_value(value: Value) -> Self {
        match value {
            Value::String(s) => Dtype::String(s.to_owned()),
            Value::Null => Dtype::Null,
            Value::Array(arr) => {
                if arr.is_empty() {
                    Dtype::Array(vec![Dtype::Null])
                } else {
                    Dtype::Array(
                        arr.into_iter()
                            .map(Dtype::from_value)
                            .collect::<Vec<Dtype>>(),
                    )
                }
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

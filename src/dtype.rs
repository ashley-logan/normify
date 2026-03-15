use crate::trait_impl::{NullType, PrimitiveType};
use crate::{
    error::{NormError, Result},
    impl_insert,
};
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
    Null,
}

pub trait NormType: Sized {
    fn into_norm(&self) -> NormValue;
}

pub struct NormArray<T: NormType> {
    items: Vec<T>,
}

pub struct NestedArray<T: NormType> {
    lists: Vec<NormArray<T>>
}


impl<T: NormType> NormArray<T> {
    pub(crate) fn new() -> Self {
        Self {
            items: Vec::<T>::new(),
        }
    }

    pub fn push(&mut self, item: T) {
        self.items.push(item)
    }
}

impl<T: NormType> NestedArray<T> {
    pub(crate) fn new() -> Self {
        Self {
            lists: vec![]
        }
    }

    pub fn push(&mut self, list: NormArray<T>) {
        self.lists.push(list)
    }
}


pub enum DataColumn {
    FloatColumn(NormArray<f64>),
    IntColumn(NormArray<i64>),
    UintColumn(NormArray<u64>),
    BoolColumn(NormArray<bool>),
    StringColumn(NormArray<String>),
    FloatListColumn(NestedArray<f64>),
    IntListColumn(NestedArray<i64>),
    UintListColumn(NestedArray<u64>),
    BoolListColumn(NestedArray<bool>),
    StringListColumn(NestedArray<String>)

}



impl DataColumn {
    pub(crate) fn new_from_norm(value: Box<dyn PrimitiveType>) -> Self {
        match value {
            NormValue::Float(f) => Self::
        }
    }
    impl_insert!(insert_int, IntColumn, i64);
    impl_insert!(insert_uint, UintColumn, u64);
    impl_insert!(insert_float, FloatColumn, f64);
    impl_insert!(insert_bool, BoolColumn, bool);
    impl_insert!(insert_string, StringColumn, String);
    // list inserts
    impl_insert!(insert_float_list, FloatListColumn, NormArray<f64>);
    impl_insert!(insert_int_list, IntListColumn, NormArray<i64>);
    impl_insert!(insert_uint_list, UintListColumn, NormArray<u64>);
    impl_insert!(insert_bool_list, BoolListColumn, NormArray<bool>);
    impl_insert!(insert_string_list, StringListColumn, NormArray<String>);
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
        }
    }
}

impl NormType for f64 {
    fn into_norm(&self) -> NormValue {
        NormValue::Float(self.to_owned())
    }
}

impl NormType for i64 {
    fn into_norm(&self) -> NormValue {
        NormValue::Int(self.to_owned())
    }
}

impl NormType for u64 {
    fn into_norm(&self) -> NormValue {
        NormValue::UInt(self.to_owned())
    }
}

impl NormType for bool {
    fn into_norm(&self) -> NormValue {
        NormValue::Bool(self.to_owned())
    }
}

impl NormType for String {
    fn into_norm(&self) -> NormValue {
        NormValue::String(self.to_owned())
    }
}

impl NormType for NullType {
    fn into_norm(&self) -> NormValue {
        NormValue::Null
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

    // // returns true if all elements pass the specified type check function or are null
    // pub fn array_is_type(arr: &Vec<NormValue>, check: fn(&Self) -> bool) -> bool {
    //     arr.iter().all(|i| check(i) || i.is_null())
    // }

    // cast each NormValue object in an array
    // pub fn cast_to_option<T>(values: Vec<NormValue>) -> Vec<Option<T>>
    // where
    //     Self: TryInto<T>,
    // {
    //     values
    //         .into_iter()
    //         .map(|x| {
    //             if !x.is_null() {
    //                 x.try_into().ok()
    //             } else {
    //                 None
    //             }
    //         })
    //         .collect()
    // }

    pub(crate) fn from_value(value: Value) -> Result<Self> {
        let r: NormValue = match value {
            Value::String(s) => s.into_norm(),
            Value::Null => NormValue::Null,
            Value::Bool(b) => b.into_norm(),
            Value::Number(n) => {
                if let Some(f) = n.as_f64() {
                    f.into_norm()
                } else if let Some(i) = n.as_i64() {
                    i.into_norm()
                } else if let Some(u) = n.as_u64() {
                    u.into_norm()
                } else {
                    return Err(NormError::Convert);
                }
            }
            _ => return Err(NormError::Convert),
        };
        Ok(r)
    }
}

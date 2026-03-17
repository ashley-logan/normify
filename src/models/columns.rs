use crate::error::{NormError, Result};
use crate::models::traits::{ArrayTrait, NormType};
use crate::models::{Item, NestedArray, NormArray, NullMarker};
use derive_more::From;

#[derive(From)]
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
    StringListColumn(NestedArray<String>),
    UnknownColumn(Vec<NullMarker>),
}

macro_rules! impl_push {
    ($func:ident, $variant:ident, $ty:ty) => {
        pub fn $func(&mut self, item: $ty) {
            match self {
                Self::$variant(arr) => arr.push(Item::Data(item)),
                _ => (),
            }
        }
    };
}

impl DataColumn {
    // primitive inserts
    impl_push!(float_push, FloatColumn, f64);
    impl_push!(int_push, IntColumn, i64);
    impl_push!(uint_push, UintColumn, u64);
    impl_push!(bool_push, BoolColumn, bool);
    impl_push!(string_push, StringColumn, String);
    pub fn null_push(&mut self) {
        match self {
            Self::FloatColumn(arr) => arr.push(Item::Null),
            Self::IntColumn(arr) => arr.push(Item::Null),
            Self::UintColumn(arr) => arr.push(Item::Null),
            Self::BoolColumn(arr) => arr.push(Item::Null),
            Self::StringColumn(arr) => arr.push(Item::Null),
            Self::UnknownColumn(v) => v.push(NullMarker),
            _ => (),
        }
    }
    // // list inserts
    // impl_insert!(insert_float_list, FloatListColumn, NormArray<f64>);
    // impl_insert!(insert_int_list, IntListColumn, NormArray<i64>);
    // impl_insert!(insert_uint_list, UintListColumn, NormArray<u64>);
    // impl_insert!(insert_bool_list, BoolListColumn, NormArray<bool>);
    // impl_insert!(insert_string_list, StringListColumn, NormArray<String>);
}

use indexmap::IndexSet;
pub struct IdColumn(IndexSet<u64>);

impl IdColumn {
    pub(crate) fn new() -> Self {
        Self(IndexSet::new())
    }

    pub(crate) fn man_insert(&mut self, id: u64) -> bool {
        self.0.insert(id)
    }

    pub(crate) fn auto_insert(&mut self) -> u64 {
        let next_id: u64 = (self.0.len() + 1) as u64;
        self.0.insert(next_id);
        next_id
    }
}

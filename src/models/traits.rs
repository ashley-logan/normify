// macro_rules! impl_normtype {
//     ($ty:ty) => {
//         impl NormType for $ty {
//             fn get_size(self) -> usize {
//                 size_of::<$ty>()
//             }
//         }
//     };
// }
macro_rules! concrete_cast {
    ($ref_method:ident, $move_method:ident, $ty:ty, $body:stmt) => {
        fn $ref_method(&mut self) -> Option<&mut $ty> {
            $body
        }

        fn $move_method(self: Box<Self>) -> Option<Box<$ty>> {
            $body
        }
    };
}

use serde_json::Value;
pub trait ItemTrait: PartialEq + Into<Value> {
    fn as_serde_value(self) -> Value;
    fn as_item<T>(self) -> Item<T>;
}

// impl_normtype!(f64);
// impl_normtype!(i64);
// impl_normtype!(u64);
// impl_normtype!(bool);
// impl_normtype!(String);

use std::any::{Any}

use crate::models::{NestedArray, NormArray, UnknownArray, type_aliases::*};
pub trait ColumnType {
    // object safe, dyn compatible
    fn as_any(&self) -> &dyn Any;
    fn as_any_mut(&mut self) -> &mut dyn Any;
    fn into_any(self: Box<Self>) -> Box<dyn Any>;
    fn len(&self) -> usize;
    fn count_data(&self) -> usize;
    fn count_nulls(&self) -> usize;
    fn is_unknown(&self) -> bool {
        false
    }
    fn is_nested(&self) -> bool {
        false
    }

    fn as_unknown(&mut self) -> Option<&mut UnknownArray> {
        None
    }

    fn as_unknown_nested(&mut self) -> Option<&mut NestedArray<UnknownArray>> {
        None
    }

    concrete_cast!(as_bool_column, into_bool_column, BoolColumn, None);
    concrete_cast!(
        as_bool_list_column,
        into_bool_list_column,
        BoolListColumn,
        None
    );
    concrete_cast!(as_string_column, into_string_column, StringColumn, None);
    concrete_cast!(
        as_string_list_column,
        into_string_list_column,
        StringListColumn,
        None
    );
    concrete_cast!(as_int_column, into_int_column, IntColumn, None);
    concrete_cast!(
        as_int_list_column,
        into_int_list_column,
        IntListColumn,
        None
    );
    concrete_cast!(as_uint_column, into_uint_column, UintColumn, None);
    concrete_cast!(
        as_uint_list_column,
        into_uint_list_column,
        UintListColumn,
        None
    );
    concrete_cast!(as_float_column, into_float_column, FloatColumn, None);
    concrete_cast!(
        as_float_list_column,
        into_float_list_column,
        FloatListColumn,
        None
    );
}

pub trait SimpleArrayType: ColumnType {
    // unsafe, not dyn compatible
    fn new() -> Self;
    fn is_known(&self) -> bool;
    fn is_unknown(&self) -> bool {
        !self.is_known()
    }
    fn push_null(&mut self);
}

pub trait NestedArrayType: ColumnType {
    fn new() -> Self;
    fn push_empty(&mut self);
}

macro_rules! concrete_cast {
    ($ref_method:ident, $move_method:ident, $is_method:ident, $ty:ty, $body:stmt, $body2:stmt) => {
        fn $ref_method(&mut self) -> Option<&mut $ty> {
            $body
        }

        fn $move_method(self: Box<Self>) -> Option<Box<$ty>> {
            $body
        }

        fn $is_method(&self) -> bool {
            $body2
        }
    };
}

use crate::Item;
use crate::error::*;
use std::fmt::Display;
use std::ops::{Index, IndexMut};
pub trait ItemTrait: PartialEq + Sized + Display + 'static {
    type Fallback: From<Self> + TryInto<Self> + ItemTrait;
    fn into_fallback(self) -> Self::Fallback {
        self.into()
    }
    fn item(self) -> Item<Self> {
        Item::Data(self)
    }
    fn try_from_fallback(value: Self::Fallback) -> Result<Self> {
        match value.try_into() {
            Ok(i) => Ok(i),
            Err(_) => Err(NormError::Convert),
        }
    }
}

use crate::{
    NormArray,
    models::{UnknownArray, type_aliases::*},
};
use std::{any::Any, fmt::Write};
pub trait ColumnType: Index<usize> {
    // object safe, dyn compatible
    fn as_any(&self) -> &dyn Any;
    fn as_any_mut(&mut self) -> &mut dyn Any;
    fn into_any(self: Box<Self>) -> Box<dyn Any>;

    fn write_list_fmt(&self, limit: Option<usize>, buf: &mut dyn Write);
    fn write_col_fmt(&self, limit: Option<usize>, buf: &mut dyn Write);

    fn print_list_fmt(&self, limit: Option<usize>) {
        let mut buf = String::new();
        self.write_list_fmt(limit, &mut buf);
        print!("{}", buf);
    }
    fn print_col_fmt(&self, limit: Option<usize>) {
        let mut buf = String::new();
        self.write_col_fmt(limit, &mut buf);
        print!("{}", buf);
    }

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

    fn push_null(&mut self);

    fn into_string_super(self: Box<Self>) -> Box<NormArray<String>>;

    concrete_cast!(
        as_bool_column,
        into_bool_column,
        is_bool_column,
        BoolColumn,
        None,
        false
    );
    concrete_cast!(
        as_bool_list_column,
        into_bool_list_column,
        is_bool_list_column,
        BoolListColumn,
        None,
        false
    );
    concrete_cast!(
        as_string_column,
        into_string_column,
        is_string_column,
        StringColumn,
        None,
        false
    );
    concrete_cast!(
        as_string_list_column,
        into_string_list_column,
        is_string_list_column,
        StringListColumn,
        None,
        false
    );
    concrete_cast!(
        as_int_column,
        into_int_column,
        is_int_column,
        IntColumn,
        None,
        false
    );
    concrete_cast!(
        as_int_list_column,
        into_int_list_column,
        is_int_list_column,
        IntListColumn,
        None,
        false
    );
    concrete_cast!(
        as_uint_column,
        into_uint_column,
        is_uint_column,
        UintColumn,
        None,
        false
    );
    concrete_cast!(
        as_uint_list_column,
        into_uint_list_column,
        is_uint_list_column,
        UintListColumn,
        None,
        false
    );
    concrete_cast!(
        as_float_column,
        into_float_column,
        is_float_column,
        FloatColumn,
        None,
        false
    );
    concrete_cast!(
        as_float_list_column,
        into_float_list_column,
        is_float_list_column,
        FloatListColumn,
        None,
        false
    );
}

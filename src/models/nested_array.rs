use crate::{impl_concrete_cast, models::{ColumnType, ItemTrait, SimpleArrayType, UnknownArray, type_aliases::*}};
use std::any::Any;

#[derive(Clone)]
pub struct NestedArray<T: SimpleArrayType> {
    sub_arrays: Vec<T>,
}

impl<T: SimpleArrayType + 'static> ColumnType for NestedArray<T> {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }

    fn into_any(self: Box<Self>) -> Box<dyn Any> {
        self
    }

    fn len(&self) -> usize {
        self.sub_arrays.len()
    }

    fn count_data(&self) -> usize {
        self.sub_arrays
            .iter()
            .filter(|&x| x.count_data() > 0)
            .count()
    }

    fn count_nulls(&self) -> usize {
        self.sub_arrays
            .iter()
            .filter(|&x| x.count_data() == 0)
            .count()
    }
    fn is_nested(&self) -> bool {
        true
    }
    

    fn push_null(&mut self) {
        self.sub_arrays.push(T::new());
    }

    impl_concrete_cast!(as_bool_list_column, into_bool_list_column, is_bool_list_column, BoolListColumn);
    impl_concrete_cast!(as_string_list_column, into_string_list_column, is_string_list_column, StringListColumn);
    impl_concrete_cast!(as_int_list_column, into_int_list_column, is_int_list_column, IntListColumn);
    impl_concrete_cast!(as_uint_list_column, into_uint_list_column, is_uint_list_column, UintListColumn);
    impl_concrete_cast!(as_float_list_column, into_float_list_column, is_float_list_column, FloatListColumn);

    
}

impl<T: SimpleArrayType> From<T> for NestedArray<T> {
    fn from(value: T) -> Self {
        Self {
            sub_arrays: vec![value],
        }
    }
}

impl<T: SimpleArrayType> From<UnknownArray> for NestedArray<T> {
    fn from(value: UnknownArray) -> Self {
        let mut arr: NestedArray<T> = NestedArray::new();
        for _ in 0..value.count_nulls() {
            arr.push_empty();
        }
        arr
    }
}

impl<T: SimpleArrayType> NestedArray<T> {
    pub fn new() -> Self {
        Self {
            sub_arrays: Vec::new(),
        }
    }

    pub fn from_arr(arr: T) -> Self {
        Self {
            sub_arrays: vec![arr],
        }
    }

    pub fn push_arr(&mut self, arr: T) {
        self.sub_arrays.push(arr)
    }

    pub fn push_empty(&mut self) {
        self.sub_arrays.push(T::new());
    }
\}

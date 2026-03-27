use crate::models::{ColumnType, ItemTrait, SimpleArrayType, UnknownArray, type_aliases::*};
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
    fn is_unknown(&self) -> bool {
        self.as_any().is::<NestedArray<UnknownArray>>()
    }

    fn as_unknown_nested(&mut self) -> Option<&mut NestedArray<UnknownArray>> {
        self.as_any_mut()
            .downcast_mut::<NestedArray<UnknownArray>>()
    }

    fn as_bool_list_column(&mut self) -> Option<&mut BoolListColumn> {
        self.as_any_mut().downcast_mut()
    }

    fn as_string_list_column(&mut self) -> Option<&mut StringListColumn> {
        self.as_any_mut().downcast_mut()
    }

    fn as_int_list_column(&mut self) -> Option<&mut IntListColumn> {
        self.as_any_mut().downcast_mut()
    }

    fn as_uint_list_column(&mut self) -> Option<&mut UintListColumn> {
        self.as_any_mut().downcast_mut()
    }

    fn as_float_list_column(&mut self) -> Option<&mut FloatListColumn> {
        self.as_any_mut().downcast_mut()
    }
}

impl<T: SimpleArrayType> From<T> for NestedArray<T> {
    fn from(value: T) -> Self {
        Self {
            sub_arrays: vec![value],
        }
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

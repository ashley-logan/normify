use crate::models::{ColumnType, Item, ItemTrait, NestedArray, NormArray};
use std::any::Any;

#[derive(Clone, Copy)]
pub struct UnknownArray(usize);

impl ColumnType for UnknownArray {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }

    fn into_any(self: Box<Self>) -> Box<dyn Any> {
        self
    }

    fn into_string_super(self: Box<Self>) -> Box<NormArray<String>> {
        let mut arr: NormArray<String> = NormArray::new();
        let item: Item<String> = Item::Null;
        for _ in 0..self.0 {
            arr.push_item(item.clone().inner_to_string());
        }
        Box::new(arr)
    }

    fn write_list_fmt(&self, limit: Option<usize>, buf: &mut dyn std::fmt::Write) {
        let _ = writeln!(buf, "UNKNOWN");
    }

    fn write_col_fmt(&self, limit: Option<usize>, buf: &mut dyn std::fmt::Write) {
        let _ = writeln!(buf, "UNKNOWN");
    }

    fn len(&self) -> usize {
        self.0
    }

    fn count_data(&self) -> usize {
        0
    }

    fn count_nulls(&self) -> usize {
        self.0
    }

    fn push_null(&mut self) {
        self.0 += 1;
    }

    fn is_unknown(&self) -> bool {
        true
    }
    fn as_unknown(&mut self) -> Option<&mut super::UnknownArray> {
        Some(self)
    }
}

impl UnknownArray {
    pub fn new() -> Self {
        Self(0)
    }
    pub fn into_norm<T: ItemTrait>(self) -> NormArray<T> {
        let mut arr: NormArray<T> = NormArray::new();
        for _ in 0..self.0 {
            arr.push_null();
        }
        arr
    }

    pub fn into_nested<T: ItemTrait>(self) -> NestedArray<T> {
        let mut arr: NestedArray<T> = NestedArray::new();
        for _ in 0..self.0 {
            arr.push_empty();
        }
        arr
    }
}

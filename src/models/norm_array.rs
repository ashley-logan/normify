use crate::impl_concrete_cast;
use crate::models::{ColumnType, ItemTrait, UnknownArray};
use crate::models::{Item, type_aliases::*};
use std::fmt::Write;

#[derive(Clone)]
pub struct NormArray<T: ItemTrait> {
    items: Vec<Item<T>>,
}

use std::any::Any;
impl<T: ItemTrait + 'static> ColumnType for NormArray<T> {
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
        for item in self.into_iter() {
            arr.push_item(item.inner_to_string());
        }
        Box::new(arr)
    }

    fn write_col_fmt(&self, limit: Option<usize>, buf: &mut dyn Write) {
        let mut lim: usize = limit.unwrap_or(self.len());
        if lim > self.len() {
            lim = self.len();
        }
        writeln!(buf, "").unwrap();
        for i in 0..lim {
            writeln!(buf, "{}", self.items[i]).unwrap();
        }
        writeln!(buf, "").unwrap();
    }

    fn write_list_fmt(&self, limit: Option<usize>, buf: &mut dyn Write) {
        let mut lim: usize = limit.unwrap_or(self.len());
        if lim > self.len() {
            lim = self.len();
        }
        write!(buf, "[  ").unwrap();
        for i in 0..lim {
            if i == lim - 1 {
                write!(buf, "{}  ]", self.items[i]).unwrap();
                break;
            }
            write!(buf, "{}, ", self.items[i]).unwrap();
        }
    }

    fn len(&self) -> usize {
        self.items.len()
    }

    fn count_data(&self) -> usize {
        self.items
            .iter()
            .filter(|&x| matches!(x, Item::Data(_)))
            .count()
    }

    fn count_nulls(&self) -> usize {
        self.items
            .iter()
            .filter(|&x| matches!(x, Item::Null))
            .count()
    }

    fn push_null(&mut self) {
        self.items.push(Item::Null);
    }

    impl_concrete_cast!(as_bool_column, into_bool_column, is_bool_column, BoolColumn);
    impl_concrete_cast!(
        as_string_column,
        into_string_column,
        is_string_column,
        StringColumn
    );
    impl_concrete_cast!(as_int_column, into_int_column, is_int_column, IntColumn);
    impl_concrete_cast!(as_uint_column, into_uint_column, is_uint_column, UintColumn);
    impl_concrete_cast!(
        as_float_column,
        into_float_column,
        is_float_column,
        FloatColumn
    );
}

impl<T: ItemTrait> From<Item<T>> for NormArray<T> {
    fn from(value: Item<T>) -> Self {
        Self { items: vec![value] }
    }
}

impl<T: ItemTrait> From<UnknownArray> for NormArray<T> {
    fn from(value: UnknownArray) -> Self {
        let mut arr: NormArray<T> = NormArray::new();
        for _ in 0..value.count_nulls() {
            arr.push_null();
        }
        arr
    }
}
// impl_ColumnType_normarray!(f64);
// impl_ColumnType_normarray!(i64);
// impl_ColumnType_normarray!(u64);
// impl_ColumnType_normarray!(bool);
// impl_ColumnType_normarray!(String);

impl<T: ItemTrait> NormArray<T> {
    pub(crate) fn new() -> Self {
        Self {
            items: Vec::<Item<T>>::new(),
        }
    }

    pub(crate) fn new_with_nulls(null_count: usize) -> Self {
        let mut arr = Self::new();
        for _ in 0..null_count {
            arr.push_null();
        }
        arr
    }

    pub(crate) fn from_item(item: Item<T>) -> Self {
        Self { items: vec![item] }
    }

    pub(crate) fn from_prim(prim: T) -> Self {
        Self {
            items: vec![Item::Data(prim)],
        }
    }

    // pub(crate) fn from_vec_values<T>(v: Vec<serde_json::Value>) -> Result<Self<T>>
    // where
    //     T: ItemTrait,
    //     serde_json::Value: TryInto<T>,
    //     T: TryFrom<serde_json::Value>,
    // {
    //     let mut item_v: Vec<Item<T>> = vec![];
    //     let mut arr: NormArray<T> = NormArray::new();
    //     for val in v {
    //         if matches!(val, serde_json::Value::Null) {
    //             arr.push_null();
    //         } else if let Ok(prim) = val.try_into::<T>() {
    //             arr.push_prim(prim);
    //         } else {
    //             return Err(NormError::Convert);
    //         }
    //     }
    //     Ok(arr)
    // }

    pub fn push_item(&mut self, item: Item<T>) {
        self.items.push(item)
    }

    pub fn push_null(&mut self) {
        self.items.push(Item::Null)
    }

    pub fn push_prim(&mut self, prim: T) {
        self.items.push(Item::Data(prim))
    }

    pub fn contains(&self, item: &Item<T>) -> bool {
        self.items.contains(item)
    }

    pub fn is_empty(&self) -> bool {
        self.items.is_empty()
    }
}

impl<T: ItemTrait> FromIterator<Item<T>> for NormArray<T> {
    fn from_iter<I: IntoIterator<Item = Item<T>>>(iter: I) -> Self {
        let mut arr = NormArray::new();
        for item in iter {
            arr.push_item(item);
        }
        arr
    }
}

impl<T: ItemTrait> FromIterator<T> for NormArray<T> {
    fn from_iter<I: IntoIterator<Item = T>>(iter: I) -> Self {
        let mut arr = NormArray::new();
        for prim in iter {
            arr.push_prim(prim);
        }
        arr
    }
}

impl<T: ItemTrait> IntoIterator for NormArray<T> {
    type Item = Item<T>;
    type IntoIter = std::vec::IntoIter<Self::Item>;

    fn into_iter(self) -> Self::IntoIter {
        self.items.into_iter()
    }
}

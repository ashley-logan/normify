use crate::models::{ColumnType, ItemTrait, , SimpleArrayType};
use crate::models::{Item, type_aliases::*};
use crate::{NormError};


macro_rules! impl_try_cast {
    ($ref_method:ident, $move_method:ident, $ty:ty) => {
        fn $ref_method(&mut self) -> Option<&mut $ty> {
            self.as_any_mut().downcast_mut::<$ty>()
        }

        fn $move_method(self: Box<Self>) -> Option<Box<$ty>> {
            self.into_any().downcast::<$ty>().ok()
        }
    };
}

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


    impl_try_cast!(as_bool_column, into_bool_column, BoolColumn);
    impl_try_cast!(as_string_column, into_string_column, StringColumn);
    impl_try_cast!(as_int_column, into_int_column, IntColumn);
    impl_try_cast!(as_uint_column, into_uint_column, UintColumn);
    impl_try_cast!(as_float_column, into_float_column, FloatColumn);

}

impl<T: ItemTrait> SimpleArrayType for NormArray<T> {
    fn new() -> Self {
        Self { items: Vec::new() }
    }
    fn is_known(&self) -> bool {
        true
    }
    fn push_null(&mut self) {
        self.items.push(Item::Null);
    }
}

impl<T: ItemTrait> From<Item<T>> for NormArray<T> {
    fn from(value: Item<T>) -> Self {
        Self { items: vec![value] }
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
            arr.push(item);
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

impl<T: ItemTrait> From<Vec<NullMarker>> for NormArray<T> {
    fn from(v: Vec<NullMarker>) -> Self {
        let mut arr = Self::new();
        for _ in v {
            arr.push(Item::Null);
        }
        arr
    }
}

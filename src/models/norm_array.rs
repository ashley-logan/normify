use uuid::serde;

use crate::NormError;
use crate::models::{ArrayTrait, ItemTrait};
use crate::models::{Item, NullMarker};

// macro_rules! impl_arraytrait_normarray {
//     (
//         $ty:ty
//     ) => {
//         impl ArrayTrait for NormArray<$ty> {
//             fn len(&self) -> usize {
//                 self.items.len()
//             }

//             fn count_data(&self) -> usize {
//                 self.items
//                     .iter()
//                     .filter(|&x| matches!(x, Item::Data(_)))
//                     .count()
//             }

//             fn count_nulls(&self) -> usize {
//                 self.items
//                     .iter()
//                     .filter(|x| matches!(x, Item::Null))
//                     .count()
//             }
//         }
//     };
// }

#[derive(Clone)]
pub struct NormArray<T: ItemTrait> {
    items: Vec<Item<T>>,
}

#[derive(Clone)]
pub(crate) struct UnknownArray {
    nulls: Vec<NullMarker>,
}

use std::any::Any;
impl<T: ItemTrait> ArrayTrait for NormArray<T> {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
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
}

impl ArrayTrait for UnknownArray {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }

    fn len(&self) -> usize {
        self.nulls.len()
    }

    fn count_data(&self) -> usize {
        0
    }

    fn count_nulls(&self) -> usize {
        self.nulls.len()
    }
}

impl UnknownArray {
    pub fn new() -> Self {
        Self { nulls: vec![] }
    }

    pub fn add_null(&mut self) {
        self.nulls.push(NullMarker)
    }

    pub fn new_with_null() -> Self {
        Self {
            nulls: vec![NullMarker],
        }
    }

    pub fn into_column<T: ItemTrait>(self, item: Item<T>) -> NormArray<T> {
        let mut arr: NormArray<T> = NormArray::new();
        for _ in self.len() {
            arr.push_null();
        }
        arr.push_item(item);
        arr
    }
}

// impl_arraytrait_normarray!(f64);
// impl_arraytrait_normarray!(i64);
// impl_arraytrait_normarray!(u64);
// impl_arraytrait_normarray!(bool);
// impl_arraytrait_normarray!(String);

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

    pub(crate) fn from_vec_values<T>(v: Vec<serde_json::Value>) -> Result<Self<T>>
    where
        T: ItemTrait,
        serde_json::Value: TryInto<T>,
        T: TryFrom<serde_json::Value>,
    {
        let mut item_v: Vec<Item<T>> = vec![];
        let mut arr: NormArray<T> = NormArray::new();
        for val in v {
            if matches!(val, serde_json::Value::Null) {
                arr.push_null();
            } else if let Ok(prim) = val.try_into::<T>() {
                arr.push_prim(prim);
            } else {
                return Err(NormError::Convert);
            }
        }
        Ok(arr)
    }

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

    pub fn from_nulls(null_arr: UnknownArray) -> Self {}
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

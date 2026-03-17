use crate::models::norm_array::NormArray;
use crate::models::traits::NormType;
use crate::models::{Item, ListType};

macro_rules! impl_arraytrait_nestedarray {
    ($ty:ty) => {
        impl ArrayTrait for NestedArray<$ty> {
            type ItemInner = $ty;
            fn add_item(&mut self, item: Item<$ty>) {
                self.lists.push(NormArray::from_item(item));
            }
        }
    };
}

pub struct NestedArray<T: NormType> {
    lists: Vec<NormArray<T>>,
}

impl_arraytrait_nestedarray!(f64);
impl_arraytrait_nestedarray!(i64);
impl_arraytrait_nestedarray!(u64);
impl_arraytrait_nestedarray!(bool);
impl_arraytrait_nestedarray!(String);

impl<T: NormType> NestedArray<T> {
    pub(crate) fn new() -> Self {
        Self { lists: vec![] }
    }

    pub(crate) fn from(arr: NormArray<T>) -> Self {
        Self { lists: vec![arr] }
    }

    pub fn push(&mut self, list: NormArray<T>) {
        self.lists.push(list)
    }

    pub fn len(&self) -> usize {
        self.lists.len()
    }
}

impl<T: NormType> FromIterator<NormArray<T>> for NestedArray<T> {
    fn from_iter<I: IntoIterator<Item = NormArray<T>>>(iter: I) -> Self {
        let mut arr = NestedArray::new();
        for item in iter {
            arr.push(item);
        }
        arr
    }
}
